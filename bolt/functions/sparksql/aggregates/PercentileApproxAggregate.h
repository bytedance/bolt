/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#pragma once

#include "bolt/common/base/IOUtils.h"
#include "bolt/common/base/Macros.h"
#include "bolt/common/base/RandomUtil.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/lib/GreenwaldKhanna.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"

#include <iostream>
#include <vector>
namespace bytedance::bolt::functions::aggregate::sparksql {

static constexpr int64_t kDefaultAccuracy = 10000;

template <typename T>
using GKSummary = functions::gk::GKQuantileSummaries<T>;

// Accumulator to buffer large count values in addition to the GreenwaldKhanna
// summaries itself.
template <typename T>
struct GKAccumulator {
  explicit GKAccumulator(HashStringAllocator* allocator)
      : accuracy_(kDefaultAccuracy),
        summary_(
            allocator->pool(),
            functions::gk::kDefaultRelativeError,
            // min size to trigger compress
            functions::gk::kDefaultCompressThreshold,
            // maximum number of elements to store in the head buffer
            functions::gk::kDefaultHeadSize) {
    summary_.setCompressed(true);
    allocator_ = allocator;
  }

  void setAccuracy(int32_t accuracy) {
    BOLT_USER_CHECK(
        accuracy > 0 && accuracy <= 2147483647,
        "The accuracy provided must be a literal between (0, 2147483647] (current value = {}})",
        accuracy);
    accuracy_ = accuracy;
    summary_.setAccuracy(accuracy);
  }

  void append(T value) {
    summary_.insert(value);
  }

  void append(const GKSummary<T>& summary) {
    summary_.merge(summary);
  }

  const GKSummary<T>& getSummary() const {
    return summary_;
  }

  void compress() {
    summary_.compress();
  }

  void reset() {
    summary_.reset();
  }

 private:
  int32_t accuracy_;
  HashStringAllocator* allocator_;
  GKSummary<T> summary_;
};

enum IntermediateTypeChildIndex {
  kPercentiles = 0,
  kPercentilesIsArray = 1,
  kAccuracy = 2,
  kTypePalceHolder = 3,
  kSerialized = 4,
};

class PercentileApproxAggregateBase : public exec::Aggregate {
 public:
  PercentileApproxAggregateBase(bool hasAccuracy, const TypePtr& resultType);

 protected:
  struct Percentiles {
    std::vector<double> values;
    bool isArray;
  };

  void decodePercentileAndAccuracy(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      size_t& argIndex);

  static void extractPercentiles(
      const ArrayVector* arrays,
      vector_size_t indexInBaseVector,
      const double*& data,
      vector_size_t& len,
      std::vector<bool>& isNull);

  void checkSetPercentile(const SelectivityVector& rows, const BaseVector& vec);

  void checkSetPercentile(
      bool isArray,
      const double* data,
      vector_size_t len,
      const std::vector<bool>& isNull);

  void checkSetAccuracy();

  void checkSetAccuracy(int64_t accuracy);

  const bool hasAccuracy_;
  std::optional<Percentiles> percentiles_;
  int32_t accuracy_{kDefaultAccuracy};

 private:
  DecodedVector decodedAccuracy_;
};

template <typename T>
class PercentileApproxAggregate : public PercentileApproxAggregateBase {
 public:
  PercentileApproxAggregate(bool hasAccuracy, const TypePtr& resultType)
      : PercentileApproxAggregateBase(hasAccuracy, resultType) {}

  ~PercentileApproxAggregate() override = default;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(GKAccumulator<T>);
  }

  bool isFixedSize() const override {
    return false;
  }

  bool accumulatorUsesExternalMemory() const override {
    return true;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) GKAccumulator<T>(allocator_);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<GKAccumulator<T>>(group)->~GKAccumulator<T>();
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    finalize(groups, numGroups);

    BOLT_USER_CHECK(result);
    // When all inputs are nulls or masked out, percentiles_ can be
    // uninitialized. The result should be nulls in this case.
    if (!percentiles_.has_value()) {
      *result = BaseVector::createNullConstant(
          (*result)->type(), numGroups, (*result)->pool());
      return;
    }

    if (percentiles_ && percentiles_->isArray) {
      std::vector<double>& percentiles = percentiles_->values;
      auto arrayResult = (*result)->asUnchecked<ArrayVector>();
      vector_size_t elementsCount = 0;
      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        auto accumulator = value<GKAccumulator<T>>(group);
        if (!accumulator->getSummary().empty()) {
          elementsCount += percentiles.size();
        }
      }
      arrayResult->elements()->resize(elementsCount);
      elementsCount = 0;
      auto rawValues =
          arrayResult->elements()->asFlatVector<T>()->mutableRawValues();
      extract(
          groups,
          numGroups,
          arrayResult,
          [&](const GKSummary<T>& digest,
              ArrayVector* result,
              vector_size_t index) {
            digest.query(percentiles, rawValues + elementsCount);
            result->setOffsetAndSize(index, elementsCount, percentiles.size());
            elementsCount += percentiles.size();
          });
    } else {
      extract(
          groups,
          numGroups,
          (*result)->asFlatVector<T>(),
          [&](const GKSummary<T>& digest,
              FlatVector<T>* result,
              vector_size_t index) {
            BOLT_USER_DCHECK_EQ(percentiles_->values.size(), 1);
            result->set(
                index,
                static_cast<T>(digest.query(percentiles_->values.back())));
          });
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    BOLT_USER_CHECK(result);
    auto rowResult = (*result)->as<RowVector>();
    BOLT_USER_CHECK(rowResult);
    auto pool = rowResult->pool();
    // Carry the declared element type (e.g. DECIMAL) for the type-placeholder
    // field. CppToType<T> would yield the physical native type (e.g. HUGEINT)
    // and drop decimal precision/scale, producing a vector whose runtime type
    // disagrees with the intermediate schema.
    const auto& placeholderType =
        rowResult->type()->asRow().childAt(kTypePalceHolder);

    // percentiles_ can be uninitialized during an intermediate aggregation step
    // when all input intermediate states are nulls. Result should be nulls in
    // this case.
    if (!percentiles_) {
      rowResult->ensureWritable(SelectivityVector{numGroups});
      // rowResult->childAt(i) for i = kPercentiles, kPercentilesIsArray, and
      // kAccuracy are expected to be constant in addIntermediateResults.
      rowResult->childAt(kPercentiles) =
          BaseVector::createNullConstant(ARRAY(DOUBLE()), numGroups, pool);
      rowResult->childAt(kPercentilesIsArray) =
          BaseVector::createNullConstant(BOOLEAN(), numGroups, pool);
      rowResult->childAt(kAccuracy) =
          BaseVector::createNullConstant(INTEGER(), numGroups, pool);
      rowResult->childAt(kTypePalceHolder) =
          BaseVector::createNullConstant(placeholderType, numGroups, pool);
      auto rawNulls = rowResult->mutableRawNulls();
      bits::fillBits(rawNulls, 0, rowResult->size(), bits::kNull);
      return;
    }
    auto& values = percentiles_->values;
    auto size = values.size();
    auto elements =
        BaseVector::create<FlatVector<double>>(DOUBLE(), size, pool);
    std::copy(values.begin(), values.end(), elements->mutableRawValues());
    auto array = std::make_shared<ArrayVector>(
        pool,
        ARRAY(DOUBLE()),
        nullptr,
        1,
        AlignedBuffer::allocate<vector_size_t>(1, pool, 0),
        AlignedBuffer::allocate<vector_size_t>(1, pool, size),
        std::move(elements));
    rowResult->childAt(kPercentiles) =
        BaseVector::wrapInConstant(numGroups, 0, std::move(array));
    rowResult->childAt(kPercentilesIsArray) =
        std::make_shared<ConstantVector<bool>>(
            pool, numGroups, false, BOOLEAN(), bool(percentiles_->isArray));
    rowResult->childAt(kAccuracy) = std::make_shared<ConstantVector<int32_t>>(
        pool, numGroups, accuracy_ <= 0, INTEGER(), int32_t(accuracy_));
    rowResult->childAt(kTypePalceHolder) = std::make_shared<ConstantVector<T>>(
        pool, numGroups, false, placeholderType, T());
    auto serializedSummary =
        rowResult->childAt(kSerialized)->asFlatVector<StringView>();

    rowResult->resize(numGroups);
    serializedSummary->resize(numGroups);
    bool spillExtract = this->isSpillExtract();
    for (int i = 0; i < numGroups; ++i) {
      auto accumulator = value<GKAccumulator<T>>(groups[i]);
      if (!spillExtract) {
        accumulator->compress();
      }
      auto& summary = accumulator->getSummary();
      if (summary.empty()) {
        rowResult->setNull(i, true);
      } else {
        auto serializedByteSize = summary.serializedByteSize();
        auto buffer =
            serializedSummary->getRawStringBufferWithSpace(serializedByteSize);
        rowResult->setNull(i, false);
        summary.serialize(buffer);
        StringView serialized = StringView(buffer, serializedByteSize);
        serializedSummary->setNoCopy(i, serialized);
        // summary cannot be reset, because of groups will be used after
        // extractAccumulators in GroupingSet
      }
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

    if (decodedValue_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedValue_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedValue_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedValue_.size(),
                rows.end()));
        nulls = decodedValue_.nulls();
      }
      rows.applyToSelected(
          [&](auto row) {
            auto accumulator = initRawAccumulator(groups[row]);
            accumulator->append(decodedValue_.valueAt<T>(row));
          },
          nulls);
    } else {
      rows.applyToSelected([&](auto row) {
        auto accumulator = initRawAccumulator(groups[row]);
        accumulator->append(decodedValue_.valueAt<T>(row));
      });
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediate<false>(groups, rows, args);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

    auto tracker = trackRowSize(group);
    auto accumulator = initRawAccumulator(group);

    if (decodedValue_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedValue_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedValue_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedValue_.size(),
                rows.end()));

        nulls = decodedValue_.nulls();
      }
      rows.applyToSelected(
          [&](auto row) { accumulator->append(decodedValue_.valueAt<T>(row)); },
          nulls);
    } else {
      rows.applyToSelected([&](auto row) {
        accumulator->append(decodedValue_.valueAt<T>(row));
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediate<true>(group, rows, args);
  }

 private:
  void finalize(char** groups, int32_t numGroups) {
    for (auto i = 0; i < numGroups; ++i) {
      value<GKAccumulator<T>>(groups[i])->compress();
    }
  }

  template <typename VectorType, typename ExtractFunc>
  void extract(
      char** groups,
      int32_t numGroups,
      VectorType* result,
      ExtractFunc extractFunction) {
    BOLT_USER_CHECK(result);
    result->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (result->mayHaveNulls()) {
      BufferPtr& nulls = result->mutableNulls(result->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<GKAccumulator<T>>(group);
      accumulator->compress();
      if (accumulator->getSummary().empty()) {
        result->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::setNull(rawNulls, i, false);
        }
        extractFunction(accumulator->getSummary(), result, i);
      }
    }
  }

  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    size_t argIndex = 0;
    decodedValue_.decode(*args[argIndex++], rows, true);
    decodePercentileAndAccuracy(rows, args, argIndex);
    BOLT_USER_CHECK_EQ(argIndex, args.size());
  }

  GKAccumulator<T>* initRawAccumulator(char* group) {
    auto accumulator = value<GKAccumulator<T>>(group);
    accumulator->setAccuracy(accuracy_);
    return accumulator;
  }

  template <bool kSingleGroup>
  void addIntermediate(
      std::conditional_t<kSingleGroup, char*, char**> group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    // The input encoding of intermediate type is not preserved since
    // Gluten's shuffle write will always flatten row vector.
    // So we always skip validateIntermediateInputs here and ignore
    // validateIntermediateInputs_
    addIntermediateImpl<kSingleGroup, false>(group, rows, args);
  }

  DecodedVector decodedValue_;

 private:
  template <bool kSingleGroup, bool checkIntermediateInputs>
  void addIntermediateImpl(
      std::conditional_t<kSingleGroup, char*, char**> group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    auto rowVec = decoded.base()->as<RowVector>();
    if (rowVec == nullptr) {
      return;
    }
    // checkIntermediateInputs always false?
    // rowVec maybe null ptr when input is constant null
    if constexpr (checkIntermediateInputs) {
      BOLT_USER_CHECK(rowVec);
      for (int i = kPercentiles; i <= kAccuracy; ++i) {
        BOLT_USER_CHECK(rowVec->childAt(i)->isConstantEncoding());
      }
      BOLT_USER_CHECK(rowVec->childAt(kSerialized)->isFlatEncoding());
    } else {
      BOLT_USER_CHECK(rowVec);
    }

    const SelectivityVector* baseRows = &rows;
    SelectivityVector innerRows{rowVec->size(), false};
    if (!decoded.isIdentityMapping()) {
      if (decoded.isConstantMapping()) {
        innerRows.setValid(decoded.index(0), true);
        innerRows.updateBounds();
      } else {
        bolt::translateToInnerRows(
            rows, decoded.indices(), decoded.nulls(&rows), innerRows);
      }
      baseRows = &innerRows;
    }

    DecodedVector percentiles(*rowVec->childAt(kPercentiles), *baseRows);
    auto percentileIsArray =
        rowVec->childAt(kPercentilesIsArray)->asUnchecked<SimpleVector<bool>>();
    auto accuracy =
        rowVec->childAt(kAccuracy)->asUnchecked<SimpleVector<int32_t>>();
    auto serializedSummary =
        rowVec->childAt(kSerialized)->asUnchecked<SimpleVector<StringView>>();
    GKAccumulator<T>* accumulator = nullptr;
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      int i = decoded.index(row);
      if (percentileIsArray->isNullAt(i)) {
        return;
      }
      if (serializedSummary->valueAt(i).size() <= 0) {
        return;
      }
      if (!accumulator) {
        int indexInBaseVector = percentiles.index(i);
        auto percentilesBase = percentiles.base()->asUnchecked<ArrayVector>();
        auto percentileBaseElements =
            percentilesBase->elements()->asFlatVector<double>();
        if constexpr (checkIntermediateInputs) {
          BOLT_USER_CHECK(percentileBaseElements);
          BOLT_USER_CHECK(
              !percentilesBase->isNullAt(indexInBaseVector),
              "Percentage value must not be null");
        }

        bool isArray = percentileIsArray->valueAt(i);
        const double* data;
        vector_size_t len;
        std::vector<bool> isNull;
        extractPercentiles(
            percentilesBase, indexInBaseVector, data, len, isNull);
        checkSetPercentile(isArray, data, len, isNull);

        if (!accuracy->isNullAt(i)) {
          checkSetAccuracy(accuracy->valueAt(i));
        }
      }
      if constexpr (kSingleGroup) {
        if (!accumulator) {
          accumulator = initRawAccumulator(group);
        }
      } else {
        accumulator = initRawAccumulator(group[row]);
      }

      if constexpr (checkIntermediateInputs) {
        BOLT_USER_CHECK(!serializedSummary->isNullAt(i));
      }
      BOLT_USER_CHECK(
          serializedSummary != nullptr, "serializedSummary is null");
      GKSummary<T> summary(
          pool_,
          functions::gk::kDefaultRelativeError,
          functions::gk::kDefaultCompressThreshold,
          functions::gk::kDefaultHeadSize);
      summary.deserialize(
          serializedSummary->valueAt(i).data(),
          serializedSummary->valueAt(i).size());
      summary.compress();
      if constexpr (kSingleGroup) {
        auto tracker = trackRowSize(group);
      } else {
        auto tracker = trackRowSize(group[row]);
      }
      accumulator->append(summary);
    });
  }
};

template <typename T>
struct PercentileApproxAggregateFactory {
  static std::unique_ptr<exec::Aggregate> create(
      bool hasAccuracy,
      const TypePtr& resultType);
};

extern template struct PercentileApproxAggregateFactory<int8_t>;
extern template struct PercentileApproxAggregateFactory<int16_t>;
extern template struct PercentileApproxAggregateFactory<int32_t>;
extern template struct PercentileApproxAggregateFactory<int64_t>;
extern template struct PercentileApproxAggregateFactory<int128_t>;
extern template struct PercentileApproxAggregateFactory<float>;
extern template struct PercentileApproxAggregateFactory<double>;
extern template struct PercentileApproxAggregateFactory<Timestamp>;

} // namespace bytedance::bolt::functions::aggregate::sparksql
