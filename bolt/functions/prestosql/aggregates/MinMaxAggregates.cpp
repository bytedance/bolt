/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <folly/Likely.h>
#include <limits>
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/AggregationHook.h"
#include "bolt/functions/lib/CheckNestedNulls.h"
#include "bolt/functions/lib/aggregates/SimpleNumericAggregate.h"
#include "bolt/functions/lib/aggregates/SingleValueAccumulator.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/functions/prestosql/aggregates/Compare.h"
using namespace bytedance::bolt::functions::aggregate;
namespace bytedance::bolt::aggregate::prestosql {

namespace {

template <typename T>
struct MinMaxTrait : public std::numeric_limits<T> {};

template <typename T>
class MinMaxAggregate : public SimpleNumericAggregate<T, T, T> {
  using BaseAggregate = SimpleNumericAggregate<T, T, T>;

 public:
  explicit MinMaxAggregate(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(T);
  }

  int32_t accumulatorAlignmentSize() const override {
    return 1;
  }

  bool supportsToIntermediate() const override {
    return true;
  }

  FLATTEN void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& input = args[0];
    if (rows.isAllSelected()) {
      result = input;
      return;
    }

    auto* pool = BaseAggregate::allocator_->pool();

    result = BaseVector::create(input->type(), rows.size(), pool);

    // Set result to NULL for rows that are masked out.
    {
      BufferPtr nulls = allocateNulls(rows.size(), pool, bits::kNull);
      rows.clearNulls(nulls);
      result->setNulls(nulls);
    }

    result->copy(input.get(), rows, nullptr, false);
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    BaseAggregate::template doExtractValues<T>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<T>(group);
        });
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    BaseAggregate::template doExtractValues<T>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<T>(group);
        });
  }
};

/// Override 'accumulatorAlignmentSize' for UnscaledLongDecimal values as it
/// uses int128_t type. Some CPUs don't support misaligned access to int128_t
/// type.
template <>
inline int32_t MinMaxAggregate<int128_t>::accumulatorAlignmentSize() const {
  return static_cast<int32_t>(sizeof(int128_t));
}

// Truncate timestamps to milliseconds precision.
template <>
void MinMaxAggregate<Timestamp>::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<Timestamp>(
      groups, numGroups, result, [&](char* group) {
        auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
        return Timestamp::fromMillis(ts.toMillis());
      });
}

template <typename T>
class MaxAggregate : public MinMaxAggregate<T> {
  using BaseAggregate = SimpleNumericAggregate<T, T, T>;

 public:
  explicit MaxAggregate(TypePtr resultType) : MinMaxAggregate<T>(resultType) {}

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) final {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<T>(groups[i]) = kInitialValue_;
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    // Re-enable pushdown for TIMESTAMP after
    // https://github.com/facebookincubator/bolt/issues/6297 is fixed.
    // Decimal is not supported to pushdown. The reason for that is the file can
    // have different precision/scale compared to table schema and this forces
    // us to do a slow per-row division, which make the benefit very small and
    // does not worth the extra complexity added.
    if (args[0]->typeKind() == TypeKind::TIMESTAMP ||
        args[0]->type()->isDecimal()) {
      mayPushdown = false;
    }
    if (mayPushdown && args[0]->isLazy()) {
      BaseAggregate::template pushdown<MinMaxHook<T, false>>(
          groups, rows, args[0]);
      return;
    }
    BaseAggregate::template updateGroups<true, T>(
        groups,
        rows,
        args[0],
        [](T& result, T value) {
          if (SimpleVector<T>::comparePrimitiveAsc(result, value) == -1) {
            result = value;
          }
        },
        mayPushdown);
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    BaseAggregate::updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) {
          if (SimpleVector<T>::comparePrimitiveAsc(result, value) == -1) {
            result = value;
          }
        },
        [](T& result, T value, int /* unused */) { result = value; },
        mayPushdown,
        kInitialValue_);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

 private:
  static const T kInitialValue_;
};

template <typename T>
const T MaxAggregate<T>::kInitialValue_ = MinMaxTrait<T>::lowest();

template <typename T>
class MinAggregate : public MinMaxAggregate<T> {
  using BaseAggregate = SimpleNumericAggregate<T, T, T>;

 public:
  explicit MinAggregate(TypePtr resultType) : MinMaxAggregate<T>(resultType) {}

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) final {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<T>(groups[i]) = kInitialValue_;
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    // Re-enable pushdown for TIMESTAMP after
    // https://github.com/facebookincubator/bolt/issues/6297 is fixed.
    // Decimal is not supported to pushdown. The reason for that is the file can
    // have different precision/scale compared to table schema and this forces
    // us to do a slow per-row division, which make the benefit very small and
    // does not worth the extra complexity added.
    if (args[0]->typeKind() == TypeKind::TIMESTAMP ||
        args[0]->type()->isDecimal()) {
      mayPushdown = false;
    }
    if (mayPushdown && args[0]->isLazy()) {
      BaseAggregate::template pushdown<MinMaxHook<T, true>>(
          groups, rows, args[0]);
      return;
    }
    BaseAggregate::template updateGroups<true, T>(
        groups,
        rows,
        args[0],
        [](T& result, T value) {
          if (SimpleVector<T>::comparePrimitiveAsc(result, value) == 1) {
            result = value;
          }
        },
        mayPushdown);
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    BaseAggregate::updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) {
          if (SimpleVector<T>::comparePrimitiveAsc(result, value) == 1) {
            result = value;
          }
        },
        [](T& result, T value, int /* unused */) { result = value; },
        mayPushdown,
        kInitialValue_);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

 private:
  static const T kInitialValue_;
};

template <typename T>
const T MinAggregate<T>::kInitialValue_ = MinMaxTrait<T>::max();
template <>
const double MinAggregate<double>::kInitialValue_ =
    MinMaxTrait<double>::quiet_NaN();
template <>
const float MinAggregate<float>::kInitialValue_ =
    MinMaxTrait<float>::quiet_NaN();

template <typename AccumulatorType>
class NonNumericMinMaxAggregateBase : public exec::Aggregate {
 public:
  explicit NonNumericMinMaxAggregateBase(
      const TypePtr& resultType,
      bool throwOnNestedNulls)
      : exec::Aggregate(resultType),
#ifdef SPARK_COMPATIBLE
        throwOnNestedNulls_(false)
#else
        throwOnNestedNulls_(throwOnNestedNulls)
#endif
  {
  }

  bool isFixedSize() const override {
    return false;
  }

  virtual bool supportAccumulatorSerde() const override {
    return AccumulatorType::supportSerde;
  }

  virtual uint32_t getAccumulatorSerializeSize(char* group) const override {
    return value<AccumulatorType>(group)->getSerializeSize();
  }

  virtual char* serializeAccumulator(char* group, char* dst) const override {
    return value<AccumulatorType>(group)->serialize(dst);
  }

  virtual char* deserializeAccumulator(char* group, char* src) const override {
    return value<AccumulatorType>(group)->deserialize(src);
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) AccumulatorType();
    }
  }

  bool supportsToIntermediate() const override {
    return true;
  }

  FLATTEN void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& input = args[0];

    if (throwOnNestedNulls_) {
      DecodedVector decoded(*input, rows, true);
      auto indices = decoded.indices();
      rows.applyToSelected([&](vector_size_t i) {
        bolt::functions::checkNestedNulls(
            decoded, indices, i, throwOnNestedNulls_);
      });
    }

    if (rows.isAllSelected()) {
      result = input;
      return;
    }

    auto* pool = allocator_->pool();

    // Set result to NULL for rows that are masked out.
    BufferPtr nulls = allocateNulls(rows.size(), pool, bits::kNull);
    rows.clearNulls(nulls);

    BufferPtr indices = allocateIndices(rows.size(), pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    std::iota(rawIndices, rawIndices + rows.size(), 0);

    result = BaseVector::wrapInDictionary(nulls, indices, rows.size(), input);
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    BOLT_CHECK(result);
    (*result)->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if ((*result)->mayHaveNulls()) {
      BufferPtr& nulls = (*result)->mutableNulls((*result)->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    bool exactSize = (numGroups == 1) ? true : false;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<AccumulatorType>(group);
      if (!accumulator->hasValue()) {
        (*result)->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::clearBit(rawNulls, i);
        }
        accumulator->read(*result, i, exactSize);
      }
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    // partial and final aggregations are the same
    extractValues(groups, numGroups, result);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<AccumulatorType>(group)->destroy(allocator_);
    }
  }

 protected:
  template <bool throwOnNestedNulls = false>
  void doUpdate(
      char** groups,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      std::function<bool(int32_t)> compareTest) {
    DecodedVector decoded(*arg, rows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping() && decoded.isNullAt(0)) {
      // nothing to do; all values are nulls
      return;
    }

    rows.applyToSelected([&](vector_size_t i) {
      if constexpr (throwOnNestedNulls) {
        if (bolt::functions::checkNestedNulls(decoded, indices, i, true)) {
          return;
        }
      } else if (decoded.isNullAt(i)) {
        return;
      }

      auto accumulator = value<AccumulatorType>(groups[i]);
      if (!accumulator->hasValue() ||
          compareTest(compare(accumulator, decoded, i))) {
        accumulator->write(baseVector, indices[i], allocator_);
      }
    });
  }

  template <bool throwOnNestedNulls = false>
  void doUpdateSingleGroup(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      std::function<bool(int32_t)> compareTest) {
    DecodedVector decoded(*arg, rows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping()) {
      if constexpr (throwOnNestedNulls) {
        if (bolt::functions::checkNestedNulls(decoded, indices, 0, true)) {
          return;
        }
      } else if (decoded.isNullAt(0)) {
        return;
      }

      auto accumulator = value<AccumulatorType>(group);
      if (!accumulator->hasValue() ||
          compareTest(compare(accumulator, decoded, 0))) {
        accumulator->write(baseVector, indices[0], allocator_);
      }
      return;
    }

    auto accumulator = value<AccumulatorType>(group);
    rows.applyToSelected([&](vector_size_t i) {
      if constexpr (throwOnNestedNulls) {
        if (bolt::functions::checkNestedNulls(decoded, indices, i, true)) {
          return;
        }
      } else if (decoded.isNullAt(i)) {
        return;
      }

      if (!accumulator->hasValue() ||
          compareTest(compare(accumulator, decoded, i))) {
        accumulator->write(baseVector, indices[i], allocator_);
      }
    });
  }

 protected:
  const bool throwOnNestedNulls_;
};

template <typename AccumlatorType>
class NonNumericMaxAggregate
    : public NonNumericMinMaxAggregateBase<AccumlatorType> {
 public:
  explicit NonNumericMaxAggregate(
      const TypePtr& resultType,
      bool throwOnNestedNulls)
      : NonNumericMinMaxAggregateBase<AccumlatorType>(
            resultType,
            throwOnNestedNulls) {}

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    if (this->throwOnNestedNulls_) {
      this->template doUpdate<true>(
          groups, rows, args[0], [](int32_t compareResult) {
            return compareResult < 0;
          });
    } else {
      this->doUpdate(groups, rows, args[0], [](int32_t compareResult) {
        return compareResult < 0;
      });
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    if (this->throwOnNestedNulls_) {
      this->template doUpdateSingleGroup<true>(
          group, rows, args[0], [](int32_t compareResult) {
            return compareResult < 0;
          });
    } else {
      this->doUpdateSingleGroup(
          group, rows, args[0], [](int32_t compareResult) {
            return compareResult < 0;
          });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }
};

template <typename AccumlatorType>
class NonNumericMinAggregate
    : public NonNumericMinMaxAggregateBase<AccumlatorType> {
 public:
  explicit NonNumericMinAggregate(
      const TypePtr& resultType,
      bool throwOnNestedNulls)
      : NonNumericMinMaxAggregateBase<AccumlatorType>(
            resultType,
            throwOnNestedNulls) {}

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    if (this->throwOnNestedNulls_) {
      this->template doUpdate<true>(
          groups, rows, args[0], [](int32_t compareResult) {
            return compareResult > 0;
          });
    } else {
      this->doUpdate(groups, rows, args[0], [](int32_t compareResult) {
        return compareResult > 0;
      });
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) final {
    if (this->throwOnNestedNulls_) {
      this->template doUpdateSingleGroup<true>(
          group, rows, args[0], [](int32_t compareResult) {
            return compareResult > 0;
          });
    } else {
      this->doUpdateSingleGroup(
          group, rows, args[0], [](int32_t compareResult) {
            return compareResult > 0;
          });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) final {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }
};

std::pair<vector_size_t*, vector_size_t*> rawOffsetAndSizes(
    ArrayVector& arrayVector) {
  return {
      arrayVector.offsets()->asMutable<vector_size_t>(),
      arrayVector.sizes()->asMutable<vector_size_t>()};
}

/// @tparam V Type of value.
/// @tparam Compare Type of comparator for T.
template <typename T, typename Compare>
struct MinMaxNAccumulator {
  int64_t n{0};
  std::vector<T, StlAllocator<T>> heapValues;

  explicit MinMaxNAccumulator(HashStringAllocator* allocator)
      : heapValues{StlAllocator<T>(allocator)} {}

  int64_t getN() const {
    return n;
  }

  size_t size() const {
    return heapValues.size();
  }

  void checkAndSetN(DecodedVector& decodedN, vector_size_t row) {
    // Skip null N.
    if (decodedN.isNullAt(row)) {
      return;
    }

    const auto newN = decodedN.valueAt<int64_t>(row);
    BOLT_USER_CHECK_GT(
        newN, 0, "second argument of max/min must be a positive integer");

    BOLT_USER_CHECK_LE(
        newN,
        10'000,
        "second argument of max/min must be less than or equal to 10000");

    if (n) {
      BOLT_USER_CHECK_EQ(
          newN,
          n,
          "second argument of max/min must be a constant for all rows in a group");
    } else {
      n = newN;
    }
  }

  void compareAndAdd(T value, Compare& comparator) {
    if (heapValues.size() < n) {
      heapValues.push_back(value);
      std::push_heap(heapValues.begin(), heapValues.end(), comparator);
    } else {
      const auto& topValue = heapValues.front();
      if (comparator(value, topValue)) {
        std::pop_heap(heapValues.begin(), heapValues.end(), comparator);
        heapValues.back() = value;
        std::push_heap(heapValues.begin(), heapValues.end(), comparator);
      }
    }
  }

  /// Copy all values from 'topValues' into 'rawValues' buffer. The heap remains
  /// unchanged after the call.
  void extractValues(T* rawValues, vector_size_t offset, Compare& comparator) {
    std::sort_heap(heapValues.begin(), heapValues.end(), comparator);
    for (int64_t i = heapValues.size() - 1; i >= 0; --i) {
      rawValues[offset + i] = heapValues[i];
    }
    std::make_heap(heapValues.begin(), heapValues.end(), comparator);
  }
};

template <typename T, typename Compare>
class MinMaxNAggregateBase : public exec::Aggregate {
 protected:
  explicit MinMaxNAggregateBase(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = MinMaxNAccumulator<T, Compare>;

  bool isFixedSize() const override {
    return false;
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (const vector_size_t i : indices) {
      auto group = groups[i];
      new (group + offset_) AccumulatorType(allocator_);
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    decodedValue_.decode(*args[0], rows);
    decodedN_.decode(*args[1], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedValue_.isNullAt(i) || decodedN_.isNullAt(i)) {
        return;
      }

      auto* group = groups[i];

      auto* accumulator = value(group);
      accumulator->checkAndSetN(decodedN_, i);

      auto tracker = trackRowSize(group);
      addRawInput(group, i);
    });
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto results = decodeIntermediateResults(args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedIntermediates_.isNullAt(i)) {
        addIntermediateResults(groups[i], i, results);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    decodedValue_.decode(*args[0], rows);

    auto* accumulator = value(group);
    validateN(args[1], rows, accumulator);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t i) {
      // Skip null value or N.
      if (!decodedValue_.isNullAt(i) && !decodedN_.isNullAt(i)) {
        addRawInput(group, i);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto results = decodeIntermediateResults(args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedIntermediates_.isNullAt(i)) {
        addIntermediateResults(group, i, results);
      }
    });
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    auto valuesArray = (*result)->as<ArrayVector>();
    valuesArray->resize(numGroups);

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valuesArray->elements();
    values->resize(numValues);

    auto* rawValues = values->asFlatVector<T>()->mutableRawValues();

    auto [rawOffsets, rawSizes] = rawOffsetAndSizes(*valuesArray);

    extractValues(groups, numGroups, rawOffsets, rawSizes, rawValues, nullptr);
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    auto rowVector = (*result)->as<RowVector>();
    rowVector->resize(numGroups);

    auto nVector = rowVector->childAt(0);
    nVector->resize(numGroups);

    auto valuesArray = rowVector->childAt(1)->as<ArrayVector>();
    valuesArray->resize(numGroups);

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valuesArray->elements();
    values->resize(numValues);

    auto* rawNs = nVector->as<FlatVector<int64_t>>()->mutableRawValues();
    auto* rawValues = values->asFlatVector<T>()->mutableRawValues();

    auto [rawOffsets, rawSizes] = rawOffsetAndSizes(*valuesArray);

    extractValues(groups, numGroups, rawOffsets, rawSizes, rawValues, rawNs);
  }

  void destroy(folly::Range<char**> groups) override {
    destroyAccumulators<AccumulatorType>(groups);
  }

 private:
  inline AccumulatorType* value(char* group) {
    return reinterpret_cast<AccumulatorType*>(group + Aggregate::offset_);
  }

  void extractValues(
      char** groups,
      int32_t numGroups,
      vector_size_t* rawOffsets,
      vector_size_t* rawSizes,
      T* rawValues,
      int64_t* rawNs) {
    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];

      if (!isNull(group)) {
        auto* accumulator = value(group);
        const vector_size_t size = accumulator->size();

        rawOffsets[i] = offset;
        rawSizes[i] = size;

        if (rawNs != nullptr) {
          rawNs[i] = accumulator->n;
        }
        accumulator->extractValues(rawValues, offset, comparator_);

        offset += size;
      }
    }
  }

  FLATTEN void addRawInput(char* group, vector_size_t index) {
    clearNull(group);

    auto* accumulator = value(group);

    accumulator->compareAndAdd(decodedValue_.valueAt<T>(index), comparator_);
  }

  struct IntermediateResult {
    const ArrayVector* valueArray;
    const FlatVector<T>* flatValues;
  };

  FLATTEN void addIntermediateResults(
      char* group,
      vector_size_t index,
      IntermediateResult& result) {
    clearNull(group);

    auto* accumulator = value(group);

    const auto decodedIndex = decodedIntermediates_.index(index);

    accumulator->checkAndSetN(decodedN_, decodedIndex);

    const auto* valueArray = result.valueArray;
    const auto* values = result.flatValues;
    auto* rawValues = values->rawValues();

    const auto numValues = valueArray->sizeAt(decodedIndex);
    const auto valueOffset = valueArray->offsetAt(decodedIndex);

    auto tracker = trackRowSize(group);
    for (auto i = 0; i < numValues; ++i) {
      const auto v = rawValues[valueOffset + i];
      accumulator->compareAndAdd(v, comparator_);
    }
  }

  IntermediateResult decodeIntermediateResults(
      const VectorPtr& arg,
      const SelectivityVector& rows) {
    decodedIntermediates_.decode(*arg, rows);

    auto baseRowVector =
        dynamic_cast<const RowVector*>(decodedIntermediates_.base());

    decodedN_.decode(*baseRowVector->childAt(0), rows);
    decodedValue_.decode(*baseRowVector->childAt(1), rows);

    IntermediateResult result{};
    result.valueArray = decodedValue_.base()->template as<ArrayVector>();
    result.flatValues =
        result.valueArray->elements()->template as<FlatVector<T>>();

    return result;
  }

  /// Return total number of values in all accumulators of specified 'groups'.
  /// Set null flags in 'result'.
  vector_size_t countValuesAndSetResultNulls(
      char** groups,
      int32_t numGroups,
      VectorPtr& result) {
    vector_size_t numValues = 0;

    uint64_t* rawNulls = getRawNulls(result.get());

    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      auto* accumulator = value(group);

      if (isNull(group)) {
        result->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        numValues += accumulator->size();
      }
    }

    return numValues;
  }

  void validateN(
      const VectorPtr& arg,
      const SelectivityVector& rows,
      AccumulatorType* accumulator) {
    decodedN_.decode(*arg, rows);
    if (decodedN_.isConstantMapping()) {
      accumulator->checkAndSetN(decodedN_, rows.begin());
    } else {
      rows.applyToSelected(
          [&](auto row) { accumulator->checkAndSetN(decodedN_, row); });
    }
  }

  Compare comparator_;
  DecodedVector decodedValue_;
  DecodedVector decodedN_;
  DecodedVector decodedIntermediates_;
};

template <typename T>
class MinNAggregate : public MinMaxNAggregateBase<T, std::less<T>> {
 public:
  explicit MinNAggregate(const TypePtr& resultType)
      : MinMaxNAggregateBase<T, std::less<T>>(resultType) {}
};

template <typename T>
class MaxNAggregate : public MinMaxNAggregateBase<T, std::greater<T>> {
 public:
  explicit MaxNAggregate(const TypePtr& resultType)
      : MinMaxNAggregateBase<T, std::greater<T>>(resultType) {}
};

template <
    template <typename T>
    class TNumeric,
    template <typename T>
    class TNonNumeric,
    template <typename T>
    class TNumericN>
exec::AggregateRegistrationResult registerMinMax(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .orderableTypeVariable("T")
                           .returnType("T")
                           .intermediateType("T")
                           .argumentType("T")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("UNKNOWN")
                           .intermediateType("UNKNOWN")
                           .argumentType("UNKNOWN")
                           .build());

#ifdef SPARK_COMPATIBLE
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("K")
                           .typeVariable("V")
                           .argumentType("MAP(K,V)")
                           .returnType("MAP(K,V)")
                           .intermediateType("MAP(K,V)")
                           .build());
#endif

  for (const auto& type :
       {"tinyint", "integer", "smallint", "bigint", "real", "double"}) {
    // T, bigint -> row(array(T), bigint) -> array(T)
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType(fmt::format("array({})", type))
            .intermediateType(fmt::format("row(bigint, array({}))", type))
            .argumentType(type)
            .argumentType("bigint")
            .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        const bool nAgg = !resultType->equivalent(*argTypes[0]);
        const bool throwOnNestedNulls = bolt::exec::isRawInput(step);

        if (nAgg) {
          // We have either 2 arguments: T, bigint (partial aggregation)
          // or one argument: row(bigint, array(T)) (intermediate or final
          // aggregation). Extract T.
          const auto& inputType = argTypes.size() == 2
              ? argTypes[0]
              : argTypes[0]->childAt(1)->childAt(0);

          switch (inputType->kind()) {
            case TypeKind::TINYINT:
              return std::make_unique<TNumericN<int8_t>>(resultType);
            case TypeKind::SMALLINT:
              return std::make_unique<TNumericN<int16_t>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<TNumericN<int32_t>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<TNumericN<int64_t>>(resultType);
            case TypeKind::REAL:
              return std::make_unique<TNumericN<float>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<TNumericN<double>>(resultType);
            case TypeKind::TIMESTAMP:
              return std::make_unique<TNumericN<Timestamp>>(resultType);
            case TypeKind::HUGEINT:
              return std::make_unique<TNumericN<int128_t>>(resultType);
            default:
              BOLT_CHECK(
                  false,
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          auto inputType = argTypes[0];
          switch (inputType->kind()) {
            case TypeKind::BOOLEAN:
              return std::make_unique<TNumeric<bool>>(resultType);
            case TypeKind::TINYINT:
              return std::make_unique<TNumeric<int8_t>>(resultType);
            case TypeKind::SMALLINT:
              return std::make_unique<TNumeric<int16_t>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<TNumeric<int32_t>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<TNumeric<int64_t>>(resultType);
            case TypeKind::REAL:
              return std::make_unique<TNumeric<float>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<TNumeric<double>>(resultType);
            case TypeKind::TIMESTAMP:
              return std::make_unique<TNumeric<Timestamp>>(resultType);
            case TypeKind::HUGEINT:
              return std::make_unique<TNumeric<int128_t>>(resultType);
            case TypeKind::VARBINARY:
              [[fallthrough]];
            case TypeKind::VARCHAR:
              return std::make_unique<TNonNumeric<SingleVarcharAccumulator>>(
                  inputType, false);
            case TypeKind::ARRAY:
              [[fallthrough]];
            case TypeKind::MAP:
              [[fallthrough]];
            case TypeKind::ROW:
              return std::make_unique<TNonNumeric<SingleValueAccumulator>>(
                  inputType, throwOnNestedNulls);
            case TypeKind::UNKNOWN:
              return std::make_unique<TNonNumeric<SingleVarcharAccumulator>>(
                  inputType, false);
            default:
              BOLT_CHECK(
                  false,
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerMinMaxAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerMinMax<MinAggregate, NonNumericMinAggregate, MinNAggregate>(
      prefix + kMin, withCompanionFunctions, overwrite);
  registerMinMax<MaxAggregate, NonNumericMaxAggregate, MaxNAggregate>(
      prefix + kMax, withCompanionFunctions, overwrite);
}

} // namespace bytedance::bolt::aggregate::prestosql
