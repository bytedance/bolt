/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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
#include <boost/sort/pdqsort/pdqsort.hpp>

#include <folly/Likely.h>
#include <folly/container/F14Map.h>
#include <cmath>
#include <exception>
#include <functional>
#include <type_traits>
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/Strings.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/type/Conversions.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorEncoding.h"
namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

template <typename T>
struct custom_equal_to {
  bool operator()(const T& x, const T& y) const {
    if constexpr (std::is_floating_point<T>::value) {
      bool leftIsNan = std::isnan(x);
      bool rightIsNan = std::isnan(y);
      if (FOLLY_UNLIKELY(leftIsNan && rightIsNan)) {
        return true;
      }
    }
    return x == y;
  }
};

template <typename T>
struct custom_hash {
 private:
  std::hash<T> hasher_;

 public:
  size_t operator()(const T& val) const {
    if constexpr (std::is_same_v<T, float>) {
      const static float Nan = std::nan("");
      if (FOLLY_UNLIKELY(std::isnan(val))) {
        return hasher_(Nan);
      }
    }
    if constexpr (std::is_same_v<T, double>) {
      const static double Nan = std::nan("");
      if (FOLLY_UNLIKELY(std::isnan(val))) {
        return hasher_(Nan);
      }
    }
    return hasher_(val);
  }
};

// Accumulator to buffer large count values in addition to the KLL
// sketch itself.
template <
    typename T,
    typename Hash = custom_hash<T>,
    typename EqualTo = custom_equal_to<T>>
struct Accumulator {
  using ValueMap = folly::F14FastMap<
      T,
      int64_t,
      custom_hash<T>,
      custom_equal_to<T>,
      AlignedStlAllocator<std::pair<const T, int64_t>, 16>>;
  ValueMap countMap_;
  std::vector<T> keys_;
  std::vector<int64_t> counts_;
  int128_t powerOfScale = 0;
  bool sort_ = false;

  explicit Accumulator(int128_t powerOfScale, HashStringAllocator* allocator)
      : countMap_{AlignedStlAllocator<std::pair<const T, int64_t>, 16>(
            allocator)},
        powerOfScale(powerOfScale) {}

  void append(T value, HashStringAllocator* /*allocator*/) {
    countMap_[value]++;
  }

  void
  appendWithCount(T value, int64_t count, HashStringAllocator* /*allocator*/) {
    countMap_[value] += count;
  }

  size_t size() const {
    return countMap_.size();
  }

  void sort() {
    if (sort_) {
      return;
    }
    sort_ = true;
    keys_.reserve(countMap_.size());
    counts_.resize(countMap_.size());
    if constexpr (std::is_same_v<T, StringView>) {
      bool nullOutput = false;
      for (const auto& [key, _] : countMap_) {
        try {
          util::Converter<TypeKind::DOUBLE, void, util::DefaultCastPolicy>::
              cast(key, &nullOutput);
        } catch (const std::exception&) {
          nullOutput = true;
        }
        if (!nullOutput) {
          keys_.emplace_back(key);
        }
      }
    } else {
      for (const auto& [key, _] : countMap_) {
        keys_.emplace_back(key);
      }
    }
    if constexpr (std::is_same_v<T, StringView>) {
      boost::sort::pdqsort(
          keys_.begin(), keys_.end(), [&](StringView left, StringView right) {
            bool nullOutput = false;
            double leftVal = util::Converter<
                TypeKind::DOUBLE,
                void,
                util::DefaultCastPolicy>::cast(left, &nullOutput);
            double rightVal = util::Converter<
                TypeKind::DOUBLE,
                void,
                util::DefaultCastPolicy>::cast(right, &nullOutput);
            return leftVal < rightVal;
          });
    } else {
      boost::sort::pdqsort(
          keys_.begin(), keys_.end(), [](const T& left, const T& right) {
            if constexpr (std::is_floating_point<T>::value) {
              if (FOLLY_UNLIKELY(std::isnan(left) && std::isnan(right))) {
                return false; // both NaN, equal
              }
              if (FOLLY_UNLIKELY(std::isnan(left))) {
                return false; // left is NaN, left > right
              }
              if (FOLLY_UNLIKELY(std::isnan(right))) {
                return true; // right is NaN, left < right
              }
            }
            return left < right;
          });
    }
    counts_[0] = countMap_[keys_[0]];
    for (size_t i = 1; i < countMap_.size(); ++i) {
      // The find function must be used instead of operator[] because operator[]
      // creates a new key-value pair for a non-existent key (countMap_ thus
      // changed).
      auto it = countMap_.find(keys_[i]);
      BOLT_CHECK(it != countMap_.end(), "Can't find {} in countMap", keys_[i]);
      counts_[i] = it->second + counts_[i - 1];
    }
  }

  int64_t binarySearchCount(int start, int end, int64_t value) {
    auto iter =
        lower_bound(counts_.begin() + start, counts_.begin() + end, value);
    return iter - counts_.begin() - start;
  }

  double estimatePercentile(double level) {
    sort();
    int64_t maxPosition = counts_.back() - 1;
    double position = static_cast<double>(maxPosition * level);
    int64_t lower = floor(position);
    int64_t higher = ceil(position);
    int64_t lowerIndex = binarySearchCount(0, keys_.size(), lower + 1);
    int64_t higherIndex = binarySearchCount(0, keys_.size(), higher + 1);
    T lowerKey = keys_[lowerIndex];
    bool nullOutput;
    double lowerValue =
        util::Converter<TypeKind::DOUBLE, void, util::DefaultCastPolicy>::cast(
            lowerKey, &nullOutput) /
        powerOfScale;
    if (lowerIndex == higherIndex) {
      return lowerValue;
    }

    T higherKey = keys_[higherIndex];
    if (higherKey == lowerKey) {
      return lowerValue;
    }
    double higherValue =
        util::Converter<TypeKind::DOUBLE, void, util::DefaultCastPolicy>::cast(
            higherKey, &nullOutput) /
        powerOfScale;

    return static_cast<double>(
        (higher - position) * lowerValue + (position - lower) * higherValue);
  }

  void extractValues(
      FlatVector<T>* keys,
      FlatVector<int64_t>* counts,
      vector_size_t offset) const {
    auto index = offset;
    for (const auto& [value, count] : countMap_) {
      keys->set(index, value);
      counts->set(index, count);
      ++index;
    }
  }

  void estimatePercentiles(
      const std::vector<double>& percentiles,
      double* output) {
    for (size_t i = 0; i < percentiles.size(); ++i) {
      output[i] = estimatePercentile(percentiles[i]);
    }
  }

  void freeSortData() {
    keys_.clear();
    counts_.clear();
  }

  void free(HashStringAllocator& allocator) {
    using UT = decltype(countMap_);
    countMap_.~UT();
  }
};

struct StringViewAccumulator {
  /// A map of unique StringViews pointing to storage managed by 'strings'.
  Accumulator<StringView> base;

  /// Stores unique non-null non-inline strings.
  bytedance::bolt::aggregate::prestosql::Strings strings;

  explicit StringViewAccumulator(
      int128_t powerOfScale,
      HashStringAllocator* allocator)
      : base{powerOfScale, allocator} {}

  size_t size() const {
    return base.size();
  }

  void append(StringView value, HashStringAllocator* allocator) {
    auto storeValue = store(value, allocator);
    bool nullOutput = false;
    try {
      util::Converter<TypeKind::DOUBLE, void, util::DefaultCastPolicy>::cast(
          storeValue, &nullOutput);
    } catch (const std::exception&) {
      nullOutput = true;
    }
    if (!nullOutput) {
      base.countMap_[storeValue]++;
    }
  }

  void appendWithCount(
      StringView value,
      int64_t count,
      HashStringAllocator* allocator) {
    auto storeValue = store(value, allocator);
    bool nullOutput = false;
    try {
      util::Converter<TypeKind::DOUBLE, void, util::DefaultCastPolicy>::cast(
          storeValue, &nullOutput);
    } catch (const std::exception&) {
      nullOutput = true;
    }
    if (!nullOutput) {
      base.countMap_[storeValue] += count;
    }
  }

  StringView store(StringView value, HashStringAllocator* allocator) {
    if (!value.isInline()) {
      auto it = base.countMap_.find(value);
      if (it != base.countMap_.end()) {
        value = it->first;
      } else {
        value = strings.append(value, *allocator);
      }
    }
    return value;
  }

  void extractValues(
      FlatVector<StringView>* keys,
      FlatVector<int64_t>* counts,
      vector_size_t offset) const {
    base.extractValues(keys, counts, offset);
  }

  double estimatePercentile(double level) {
    return base.estimatePercentile(level);
  }

  void estimatePercentiles(
      const std::vector<double>& percentiles,
      double* output) {
    base.estimatePercentiles(percentiles, output);
  }

  void freeSortData() {
    base.freeSortData();
  }
};

template <typename T>
struct AccumulatorTypeTraits {
  using AccumulatorType = Accumulator<T>;
};

template <>
struct AccumulatorTypeTraits<StringView> {
  using AccumulatorType = StringViewAccumulator;
};

// Combines a partial aggregation represented by the key-value pair at row in
// mapKeys and mapValues into groupMap.
template <typename T, typename Accumulator>
FOLLY_ALWAYS_INLINE void addToFinalAggregation(
    const FlatVector<T>* mapKeys,
    const FlatVector<int64_t>* mapValues,
    vector_size_t index,
    const vector_size_t* rawSizes,
    const vector_size_t* rawOffsets,
    Accumulator* accumulator,
    HashStringAllocator* allocator) {
  auto size = rawSizes[index];
  auto offset = rawOffsets[index];
  for (int i = 0; i < size; ++i) {
    accumulator->appendWithCount(
        mapKeys->valueAt(offset + i),
        mapValues->valueAt(offset + i),
        allocator);
  }
}

enum IntermediateTypeChildIndex {
  kPercentiles = 0,
  kPercentilesIsArray = 1,
  kItems = 2,
  kCounts = 3,
};

template <typename U>
inline void checkWeight(U weight) {
  BOLT_USER_CHECK(
      1 <= weight, "Percentile: weight must be positive, got {}", weight);
}

template <typename T, typename U>
class PercentileAggregate : public exec::Aggregate {
 public:
  PercentileAggregate(
      bool hasWeight,
      const TypePtr& resultType,
      int128_t powerOfScale = 0)
      : exec::Aggregate(resultType),
        hasWeight_{hasWeight},
        powerOfScale_(powerOfScale) {}

  using AccumulatorType = typename AccumulatorTypeTraits<T>::AccumulatorType;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) AccumulatorType(powerOfScale_, allocator_);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<AccumulatorType>(group)->~AccumulatorType();
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    BOLT_CHECK(result);
    // When all inputs are nulls or masked out, percentiles_ can be
    // uninitialized. The result should be nulls in this case.
    if (!percentiles_.has_value()) {
      *result = BaseVector::createNullConstant(
          (*result)->type(), numGroups, (*result)->pool());
      return;
    }
    if (percentiles_ && percentiles_->isArray) {
      auto arrayResult = (*result)->asUnchecked<ArrayVector>();
      vector_size_t elementsCount = 0;
      vector_size_t percentileSize = percentiles_->values.size();
      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        auto accumulator = value<AccumulatorType>(group);
        if (accumulator->size() > 0) {
          elementsCount += percentileSize;
        }
      }
      arrayResult->elements()->resize(elementsCount);
      elementsCount = 0;
      auto rawValues =
          arrayResult->elements()->asFlatVector<double>()->mutableRawValues();
      extract(
          groups,
          numGroups,
          arrayResult,
          [&](AccumulatorType* accumulator,
              ArrayVector* result,
              vector_size_t index) {
            accumulator->estimatePercentiles(
                percentiles_->values, rawValues + elementsCount);
            result->setOffsetAndSize(index, elementsCount, percentileSize);
            elementsCount += percentileSize;
          });
    } else {
      extract(
          groups,
          numGroups,
          (*result)->asFlatVector<double>(),
          [&](AccumulatorType* accumulator,
              FlatVector<double>* result,
              vector_size_t index) {
            BOLT_DCHECK_EQ(percentiles_->values.size(), 1);
            result->set(
                index,
                accumulator->estimatePercentile(percentiles_->values.back()));
          });
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    BOLT_CHECK(result);
    auto rowResult = (*result)->as<RowVector>();
    BOLT_CHECK(rowResult);
    auto pool = rowResult->pool();

    // percentiles_ can be uninitialized during an intermediate aggregation step
    // when all input intermediate states are nulls. Result should be nulls in
    // this case.
    if (!percentiles_) {
      rowResult->ensureWritable(SelectivityVector{numGroups});
      // rowResult->childAt(i) for i = kPercentiles, kPercentilesIsArray
      rowResult->childAt(kPercentiles) =
          BaseVector::createNullConstant(ARRAY(DOUBLE()), numGroups, pool);
      rowResult->childAt(kPercentilesIsArray) =
          BaseVector::createNullConstant(BOOLEAN(), numGroups, pool);
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
    auto items = rowResult->childAt(kItems)->as<ArrayVector>();
    auto counts = rowResult->childAt(kCounts)->as<ArrayVector>();
    rowResult->resize(numGroups);
    items->resize(numGroups);
    counts->resize(numGroups);

    auto itemsElements = items->elements()->asFlatVector<T>();
    auto countsElements = counts->elements()->asFlatVector<int64_t>();
    auto itemsCount = countAllGroups(groups, numGroups);
    BOLT_CHECK_LE(itemsCount, std::numeric_limits<vector_size_t>::max());
    itemsElements->resize(itemsCount);
    itemsElements->resetNulls();
    countsElements->resize(itemsCount);
    countsElements->resetNulls();

    itemsCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto accumulator = value<const AccumulatorType>(groups[i]);
      vector_size_t size = accumulator->size();
      if (size == 0) {
        rowResult->setNull(i, true);
      } else {
        rowResult->setNull(i, false);
        items->setOffsetAndSize(i, itemsCount, size);
        counts->setOffsetAndSize(i, itemsCount, size);
        accumulator->extractValues(itemsElements, countsElements, itemsCount);
        itemsCount += size;
      }
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

    if (hasWeight_) {
      if (constantWeight_) {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row)) {
            return;
          }

          auto tracker = trackRowSize(groups[row]);
          auto accumulator = initRawAccumulator(groups[row]);
          auto value = decodedValue_.valueAt<T>(row);
          accumulator->appendWithCount(value, weight_, allocator_);
        });
      } else {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row) || decodedWeight_.isNullAt(row)) {
            return;
          }

          auto tracker = trackRowSize(groups[row]);
          auto accumulator = initRawAccumulator(groups[row]);
          auto value = decodedValue_.valueAt<T>(row);
          auto weight = decodedWeight_.valueAt<U>(row);
          checkWeight(weight);
          accumulator->appendWithCount(
              value, static_cast<int64_t>(weight), allocator_);
        });
      }
    } else {
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
              accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
            },
            nulls);
      } else {
        rows.applyToSelected([&](auto row) {
          auto accumulator = initRawAccumulator(groups[row]);
          accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
        });
      }
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

    if (hasWeight_) {
      if (constantWeight_) {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row)) {
            return;
          }

          auto value = decodedValue_.valueAt<T>(row);
          accumulator->appendWithCount(value, weight_, allocator_);
        });
      } else {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row) || decodedWeight_.isNullAt(row)) {
            return;
          }

          auto value = decodedValue_.valueAt<T>(row);
          auto weight = decodedWeight_.valueAt<U>(row);
          checkWeight(weight);
          accumulator->appendWithCount(
              value, static_cast<int64_t>(weight), allocator_);
        });
      }
    } else {
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
              accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
            },
            nulls);
      } else {
        rows.applyToSelected([&](auto row) {
          accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
        });
      }
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
  template <typename VectorType, typename ExtractFunc>
  void extract(
      char** groups,
      int32_t numGroups,
      VectorType* result,
      ExtractFunc extractFunction) {
    BOLT_CHECK(result);
    result->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (result->mayHaveNulls()) {
      BufferPtr& nulls = result->mutableNulls(result->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<AccumulatorType>(group);
      if (accumulator->size() == 0) {
        result->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::setNull(rawNulls, i, false);
        }
        extractFunction(accumulator, result, i);
      }
      accumulator->freeSortData();
    }
  }

  vector_size_t countAllGroups(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<AccumulatorType>(groups[i])->size();
    }
    return size;
  }

  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    size_t argIndex = 0;
    decodedValue_.decode(*args[argIndex++], rows, true);
    checkSetPercentile(rows, *args[argIndex++]);
    if (hasWeight_) {
      if (args[argIndex]->encoding() == VectorEncoding::Simple::CONSTANT &&
          args[argIndex]->isNullAt(0) == false) {
        auto weight = (args[argIndex++])->as<ConstantVector<U>>()->valueAt(0);
        checkWeight(weight);
        constantWeight_ = true;
        weight_ = static_cast<int64_t>(weight);
      } else {
        decodedWeight_.decode(*args[argIndex++], rows, true);
      }
    }
    BOLT_CHECK_EQ(argIndex, args.size());
  }

  /// Extract percentile info: the raw data, the length and the null-ness from
  /// top-level ArrayVector.
  static void extractPercentiles(
      const ArrayVector* arrays,
      vector_size_t indexInBaseVector,
      const double*& data,
      vector_size_t& len,
      std::vector<bool>& isNull) {
    auto elements = arrays->elements()->asFlatVector<double>();
    auto offset = arrays->offsetAt(indexInBaseVector);
    data = elements->rawValues() + offset;
    len = arrays->sizeAt(indexInBaseVector);
    isNull.resize(len);
    for (auto index = offset; index < offset + len; index++) {
      isNull[index - offset] = elements->isNullAt(index);
    }
  }

  void checkSetPercentile(
      const SelectivityVector& rows,
      const BaseVector& vec) {
    DecodedVector decoded(vec, rows);
    BOLT_USER_CHECK(
        decoded.isConstantMapping(),
        "Percentile argument must be constant for all input rows");
    bool isArray;
    const double* data;
    vector_size_t len;
    std::vector<bool> isNull;
    auto indexInBaseVector = decoded.index(0);
    if (decoded.base()->typeKind() == TypeKind::DOUBLE) {
      isArray = false;
      auto baseVector = decoded.base();
      data = baseVector->asUnchecked<ConstantVector<double>>()->rawValues() +
          indexInBaseVector;
      len = 1;
      isNull = {baseVector->isNullAt(indexInBaseVector)};
    } else if (decoded.base()->typeKind() == TypeKind::ARRAY) {
      isArray = true;
      auto arrays = decoded.base()->asUnchecked<ArrayVector>();
      BOLT_USER_CHECK(
          arrays->elements()->isFlatEncoding(),
          "Only flat encoding is allowed for percentile array elements");
      extractPercentiles(arrays, indexInBaseVector, data, len, isNull);
    } else {
      BOLT_USER_FAIL(
          "Incorrect type for percentile: {}", decoded.base()->typeKind());
    }
    checkSetPercentile(isArray, data, len, isNull);
  }

  void checkSetPercentile(
      bool isArray,
      const double* data,
      vector_size_t len,
      const std::vector<bool>& isNull) {
    if (!percentiles_) {
      BOLT_USER_CHECK_GT(len, 0, "Percentile cannot be empty");
      percentiles_ = {
          .values = std::vector<double>(len),
          .isArray = isArray,
      };
      for (vector_size_t i = 0; i < len; ++i) {
        BOLT_USER_CHECK(!isNull[i], "Percentile cannot be null");
        BOLT_USER_CHECK_GE(data[i], 0, "Percentile must be between 0 and 1");
        BOLT_USER_CHECK_LE(data[i], 1, "Percentile must be between 0 and 1");
        percentiles_->values[i] = data[i];
      }
    } else {
      BOLT_USER_CHECK_EQ(
          isArray,
          percentiles_->isArray,
          "Percentile argument must be constant for all input rows");
      BOLT_USER_CHECK_EQ(
          len,
          percentiles_->values.size(),
          "Percentile argument must be constant for all input rows");
      for (vector_size_t i = 0; i < len; ++i) {
        BOLT_USER_CHECK_EQ(
            data[i],
            percentiles_->values[i],
            "Percentile argument must be constant for all input rows");
      }
    }
  }

  AccumulatorType* initRawAccumulator(char* group) {
    auto accumulator = value<AccumulatorType>(group);
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

  struct Percentiles {
    std::vector<double> values;
    bool isArray;
  };

  const bool hasWeight_;
  bool constantWeight_ = false;
  int64_t weight_;
  int powerOfScale_;
  std::optional<Percentiles> percentiles_;
  DecodedVector decodedValue_;
  DecodedVector decodedWeight_;
  DecodedVector decodedIntermediate_;

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
    // rowVec maybe null ptr when input is constant nulll
    auto indices = decoded.indices();
    if constexpr (checkIntermediateInputs) {
      BOLT_USER_CHECK(rowVec);
      for (int i = kPercentiles; i <= kPercentilesIsArray; ++i) {
        BOLT_USER_CHECK(rowVec->childAt(i)->isConstantEncoding());
      }
      for (int i = kItems; i <= kCounts; ++i) {
        BOLT_USER_CHECK(
            rowVec->childAt(i)->encoding() == VectorEncoding::Simple::ARRAY);
      }
    } else {
      BOLT_CHECK(rowVec);
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
    auto items = rowVec->childAt(kItems)->asUnchecked<ArrayVector>();
    auto counts = rowVec->childAt(kCounts)->asUnchecked<ArrayVector>();
    auto itemsElements = items->elements()->asFlatVector<T>();
    auto countsElements = counts->elements()->asFlatVector<int64_t>();
    if constexpr (checkIntermediateInputs) {
      BOLT_USER_CHECK(itemsElements);
      BOLT_USER_CHECK(countsElements);
    } else {
      BOLT_CHECK(itemsElements);
      BOLT_CHECK(countsElements);
    }
    auto rawSizes = items->rawSizes();
    auto rawOffsets = items->rawOffsets();

    AccumulatorType* accumulator = nullptr;
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      int i = decoded.index(row);
      if (percentileIsArray->isNullAt(i)) {
        return;
      }
      if (!accumulator) {
        int indexInBaseVector = percentiles.index(i);
        auto percentilesBase = percentiles.base()->asUnchecked<ArrayVector>();
        auto percentileBaseElements =
            percentilesBase->elements()->asFlatVector<double>();
        if constexpr (checkIntermediateInputs) {
          BOLT_USER_CHECK(percentileBaseElements);
          BOLT_USER_CHECK(!percentilesBase->isNullAt(indexInBaseVector));
        }

        bool isArray = percentileIsArray->valueAt(i);
        const double* data;
        vector_size_t len;
        std::vector<bool> isNull;
        extractPercentiles(
            percentilesBase, indexInBaseVector, data, len, isNull);
        checkSetPercentile(isArray, data, len, isNull);
      }
      if constexpr (kSingleGroup) {
        if (!accumulator) {
          accumulator = initRawAccumulator(group);
        }
      } else {
        accumulator = initRawAccumulator(group[row]);
      }

      if constexpr (checkIntermediateInputs) {
        BOLT_USER_CHECK(!(items->isNullAt(i) || counts->isNullAt(i)));
      }
      if constexpr (kSingleGroup) {
        auto tracker = trackRowSize(group);
      } else {
        auto tracker = trackRowSize(group[row]);
      }
      addToFinalAggregation<T, AccumulatorType>(
          itemsElements,
          countsElements,
          indices[row],
          rawSizes,
          rawOffsets,
          accumulator,
          allocator_);
    });
  }
};

bool validPercentileType(const Type& type) {
  if (type.kind() == TypeKind::DOUBLE) {
    return true;
  }
  if (type.kind() != TypeKind::ARRAY) {
    return false;
  }
  return type.as<TypeKind::ARRAY>().elementType()->kind() == TypeKind::DOUBLE;
}

void addDecimalSignatures(
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType =
      "row(array(double), boolean, array(DECIMAL(a_precision, a_scale)), array(bigint))";
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("double")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(double)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .build());
  for (const auto& weightType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("double")
                             .intermediateType(intermediateType)
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("double")
                             .argumentType(weightType)
                             .build());
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("array(double)")
                             .intermediateType(intermediateType)
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("array(double)")
                             .argumentType(weightType)
                             .build());
  }
}

void addSignatures(
    const std::string& inputType,
    const std::string& percentileType,
    const std::string& returnType,
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType = fmt::format(
      "row(array(double), boolean, array({0}), array(bigint))", inputType);
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .build());
  for (const auto& weightType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(returnType)
                             .intermediateType(intermediateType)
                             .argumentType(inputType)
                             .argumentType(percentileType)
                             .argumentType(weightType)
                             .build());
  }
}

template <TypeKind Kind>
std::unique_ptr<exec::Aggregate> getPercentileFuncPtr(
    const TypeKind& weightType,
    bool hasWeight,
    const TypePtr& resultType,
    int128_t powerOfScale) {
  using T = typename TypeTraits<Kind>::NativeType;
  switch (weightType) {
    case TypeKind::TINYINT:
      return std::make_unique<
          PercentileAggregate<T, TypeTraits<TypeKind::TINYINT>::NativeType>>(
          hasWeight, resultType, powerOfScale);
    case TypeKind::SMALLINT:
      return std::make_unique<
          PercentileAggregate<T, TypeTraits<TypeKind::SMALLINT>::NativeType>>(
          hasWeight, resultType, powerOfScale);
    case TypeKind::INTEGER:
      return std::make_unique<
          PercentileAggregate<T, TypeTraits<TypeKind::INTEGER>::NativeType>>(
          hasWeight, resultType, powerOfScale);
    case TypeKind::BIGINT:
      return std::make_unique<
          PercentileAggregate<T, TypeTraits<TypeKind::BIGINT>::NativeType>>(
          hasWeight, resultType, powerOfScale);
    default:
      BOLT_USER_FAIL("Unsupported weight type for percentile aggregation");
  }
}

exec::AggregateRegistrationResult registerPercentile(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "hugeint",
        "real",
        "double",
        "varchar"}) {
    addSignatures(inputType, "double", "double", signatures);
    addSignatures(inputType, "array(double)", "array(double)", signatures);
  }
  addDecimalSignatures(signatures);
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        TypeKind weightType = TypeKind::BIGINT;
        auto hasWeight = argTypes.size() >= 3;

        if (isRawInput) {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              2 + hasWeight,
              "Wrong number of arguments passed to {}",
              name);
          if (hasWeight) {
            if (argTypes[2]->kind() != TypeKind::BIGINT &&
                argTypes[2]->kind() != TypeKind::INTEGER &&
                argTypes[2]->kind() != TypeKind::SMALLINT &&
                argTypes[2]->kind() != TypeKind::TINYINT) {
              BOLT_USER_FAIL(
                  "The type of the weight argument of {} must be integer type",
                  name);
            }
            weightType = argTypes[2]->kind();
          }
          BOLT_USER_CHECK(
              validPercentileType(*argTypes[1]),
              "The type of the percentile argument of {} must be DOUBLE or ARRAY(DOUBLE)",
              name);
        } else {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "The type of partial result for {} must be ROW",
              name);
        }

        TypePtr type;
        if (isRawInput) {
          type = argTypes[0];
        } else {
          type = argTypes[0]->asRow().childAt(kItems);
          type = type->as<TypeKind::ARRAY>().elementType();
        }

        int scale = 0;
        if (type->isLongDecimal() || type->isShortDecimal()) {
          scale = getDecimalPrecisionScale(*type).second;
        }
        auto powerOfScale = DecimalUtil::kPowersOfTen[scale];

        switch (type->kind()) {
          case TypeKind::TINYINT:
            return getPercentileFuncPtr<TypeKind::TINYINT>(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::SMALLINT:
            return getPercentileFuncPtr<TypeKind::SMALLINT>(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::INTEGER:
            return getPercentileFuncPtr<TypeKind::INTEGER>(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::BIGINT:
            return getPercentileFuncPtr<TypeKind::BIGINT>(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::HUGEINT:
            return getPercentileFuncPtr<TypeKind::HUGEINT>(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::REAL:
            return getPercentileFuncPtr<TypeKind::REAL>(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::DOUBLE:
            return getPercentileFuncPtr<TypeKind::DOUBLE>(
                weightType, hasWeight, resultType, powerOfScale);
          case TypeKind::VARCHAR:
            return getPercentileFuncPtr<TypeKind::VARCHAR>(
                weightType, hasWeight, resultType, powerOfScale);
          default:
            BOLT_USER_FAIL(
                "Unsupported input type for percentile aggregation {}",
                type->toString());
        }
      },
      /*registerCompanionFunctions*/ true);
}

} // namespace

void registerPercentileAggregate(const std::string& prefix) {
  registerPercentile(prefix + "percentile");
}

} // namespace bytedance::bolt::functions::aggregate::sparksql