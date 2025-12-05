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

#include <boost/sort/pdqsort/pdqsort.hpp>

#include <exception>
#include <type_traits>
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/Strings.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/type/Conversions.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorEncoding.h"
#include "folly/container/F14Map.h"
namespace bytedance::bolt::aggregate::prestosql {
/**
 * percentile(expr, pc) -> double or double array
 *                      Returns the percentile(s) of expr at pc (range: [0,1])
 *   expr can be tinyint/smallint/integer/bigint
 *   pc can be a double or double array
 *   IntermediateType is row(map(bigint, bigint), array(bigint))
 */
namespace {
// Accumulator to buffer large count values in addition to the KLL
// sketch itself.
template <typename T>
struct Accumulator {
  using ValueMap = folly::F14FastMap<
      T,
      int64_t,
      std::hash<T>,
      std::equal_to<T>,
      AlignedStlAllocator<std::pair<const T, int64_t>, 16>>;
  ValueMap countMap_;
  std::vector<T> keys_;
  std::vector<int64_t> counts_;
  bool sort_ = false;
  explicit Accumulator(HashStringAllocator* allocator)
      : countMap_{
            AlignedStlAllocator<std::pair<const T, int64_t>, 16>(allocator)} {}

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
    for (const auto& [key, _] : countMap_) {
      keys_.emplace_back(key);
    }
    boost::sort::pdqsort(keys_.begin(), keys_.end());
    counts_[0] = countMap_[keys_[0]];
    for (size_t i = 1; i < countMap_.size(); ++i) {
      counts_[i] = countMap_[keys_[i]] + counts_[i - 1];
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
            lowerKey, &nullOutput);
    if (lowerIndex == higherIndex) {
      return lowerValue;
    }

    T higherKey = keys_[higherIndex];
    if (higherKey == lowerKey) {
      return lowerValue;
    }
    double higherValue =
        util::Converter<TypeKind::DOUBLE, void, util::DefaultCastPolicy>::cast(
            higherKey, &nullOutput);

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

template <typename T>
struct AccumulatorTypeTraits {
  using AccumulatorType = Accumulator<T>;
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
  kItemWithCountMap = 0,
  kPercentiles = 1,
};

template <typename T, typename sourceT>
class HiveUDAFPercentileAggregate : public exec::Aggregate {
 public:
  HiveUDAFPercentileAggregate(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = typename AccumulatorTypeTraits<T>::AccumulatorType;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
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
      new (group + offset_) AccumulatorType(allocator_);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<AccumulatorType>(group)->~AccumulatorType();
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BOLT_CHECK(result);
    // When all inputs are nulls or masked out, percentiles_ can be
    // uninitialized. The result should be nulls in this case.
    if (!percentiles_.has_value()) {
      *result = BaseVector::createNullConstant(
          (*result)->type(), numGroups, (*result)->pool());
      return;
    }
    if (resultType_->kind() == TypeKind::ARRAY) {
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

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BOLT_CHECK(result);
    auto rowResult = (*result)->as<RowVector>();
    BOLT_CHECK(rowResult);
    auto pool = rowResult->pool();

    // percentiles_ can be uninitialized during an intermediate aggregation step
    // when all input intermediate states are nulls. Result should be nulls in
    // this case.
    if (!percentiles_) {
      rowResult->ensureWritable(SelectivityVector{numGroups});
      rowResult->childAt(kPercentiles) =
          BaseVector::createNullConstant(ARRAY(DOUBLE()), numGroups, pool);
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
    auto itemWithCount = rowResult->childAt(kItemWithCountMap)->as<MapVector>();
    rowResult->resize(numGroups);
    itemWithCount->resize(numGroups);

    auto itemsElements = itemWithCount->mapKeys()->asFlatVector<T>();
    auto countsElements = itemWithCount->mapValues()->asFlatVector<int64_t>();
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
        itemWithCount->setOffsetAndSize(i, itemsCount, size);
        accumulator->extractValues(itemsElements, countsElements, itemsCount);
        itemsCount += size;
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    if (decodedValue_.mayHaveNulls()) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row)) {
          return;
        }
        auto accumulator = initRawAccumulator(groups[row]);
        accumulator->append(decodedValue_.valueAt<sourceT>(row), allocator_);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        auto accumulator = initRawAccumulator(groups[row]);
        accumulator->append(decodedValue_.valueAt<sourceT>(row), allocator_);
      });
    }
  }

  void addIntermediateResults(
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
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row)) {
          return;
        }
        accumulator->append(decodedValue_.valueAt<sourceT>(row), allocator_);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        accumulator->append(decodedValue_.valueAt<sourceT>(row), allocator_);
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
          bits::clearBit(rawNulls, i);
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
    const double* data;
    vector_size_t len;
    std::vector<bool> isNull;
    auto indexInBaseVector = decoded.index(0);
    if (decoded.base()->typeKind() == TypeKind::DOUBLE) {
      auto baseVector = decoded.base();
      data = baseVector->asUnchecked<ConstantVector<double>>()->rawValues() +
          indexInBaseVector;
      len = 1;
      isNull = {baseVector->isNullAt(indexInBaseVector)};
    } else if (decoded.base()->typeKind() == TypeKind::ARRAY) {
      auto arrays = decoded.base()->asUnchecked<ArrayVector>();
      BOLT_USER_CHECK(
          arrays->elements()->isFlatEncoding(),
          "Only flat encoding is allowed for percentile array elements");
      extractPercentiles(arrays, indexInBaseVector, data, len, isNull);
    } else {
      BOLT_USER_FAIL(
          "Incorrect type for percentile: {}", decoded.base()->typeKind());
    }
    checkSetPercentile(data, len, isNull);
  }

  void checkSetPercentile(
      const double* data,
      vector_size_t len,
      const std::vector<bool>& isNull) {
    if (!percentiles_) {
      BOLT_USER_CHECK_GT(len, 0, "Percentile cannot be empty");
      percentiles_ = {
          .values = std::vector<double>(len),
      };
      for (vector_size_t i = 0; i < len; ++i) {
        BOLT_USER_CHECK(!isNull[i], "Percentile cannot be null");
        BOLT_USER_CHECK_GE(data[i], 0, "Percentile must be between 0 and 1");
        BOLT_USER_CHECK_LE(data[i], 1, "Percentile must be between 0 and 1");
        percentiles_->values[i] = data[i];
      }
    } else {
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
  };

  std::optional<Percentiles> percentiles_;
  DecodedVector decodedValue_;
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
      BOLT_USER_CHECK(rowVec->childAt(kPercentiles)->isConstantEncoding());
      BOLT_USER_CHECK(
          rowVec->childAt(kItemWithCountMap)->encoding() ==
          VectorEncoding::Simple::MAP);
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
    auto itemWithCount =
        rowVec->childAt(kItemWithCountMap)->asUnchecked<MapVector>();
    auto itemsElements = itemWithCount->mapKeys()->asFlatVector<T>();
    auto countsElements = itemWithCount->mapValues()->asFlatVector<int64_t>();
    if constexpr (checkIntermediateInputs) {
      BOLT_USER_CHECK(itemsElements);
      BOLT_USER_CHECK(countsElements);
    } else {
      BOLT_CHECK(itemsElements);
      BOLT_CHECK(countsElements);
    }
    auto rawSizes = itemWithCount->rawSizes();
    auto rawOffsets = itemWithCount->rawOffsets();

    AccumulatorType* accumulator = nullptr;
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      int i = decoded.index(row);
      if (!accumulator) {
        int indexInBaseVector = percentiles.index(i);
        auto percentilesBase = percentiles.base()->asUnchecked<ArrayVector>();
        auto percentileBaseElements =
            percentilesBase->elements()->asFlatVector<double>();
        if constexpr (checkIntermediateInputs) {
          BOLT_USER_CHECK(percentileBaseElements);
          BOLT_USER_CHECK(!percentilesBase->isNullAt(indexInBaseVector));
        }

        const double* data;
        vector_size_t len;
        std::vector<bool> isNull;
        extractPercentiles(
            percentilesBase, indexInBaseVector, data, len, isNull);
        checkSetPercentile(data, len, isNull);
      }
      if constexpr (kSingleGroup) {
        if (!accumulator) {
          accumulator = initRawAccumulator(group);
        }
      } else {
        accumulator = initRawAccumulator(group[row]);
      }

      if constexpr (checkIntermediateInputs) {
        BOLT_USER_CHECK(!(itemWithCount->isNullAt(i)));
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

void addSignatures(
    const std::string& inputType,
    const std::string& percentileType,
    const std::string& returnType,
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType = "row(map(bigint, bigint), array(double))";
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .build());
}

exec::AggregateRegistrationResult registerHiveUDAFPercentile(
    const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"}) {
    addSignatures(inputType, "double", "double", signatures);
    addSignatures(inputType, "array(double)", "array(double)", signatures);
  }
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        bool isRawInput = exec::isRawInput(step);
        // When is raw input,
        // input type should be (integer, double) or (integer, array(double)).
        // Else input type is Row<Map<bigint, bigint>, double/array<double>>.
        if (isRawInput) {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              2,
              "The number of hive.udf.percentile aggregation's input type should be 2 vs {}",
              argTypes.size());
          BOLT_USER_CHECK(
              validPercentileType(*argTypes[1]),
              "The type of the percentile argument of {} must be DOUBLE or ARRAY(DOUBLE)",
              name);
        } else {
          BOLT_USER_CHECK(
              argTypes.size() == 1 && argTypes[0]->kind() == TypeKind::ROW,
              "The type of partial result for {} must be ROW",
              name);
        }

        TypePtr type;
        if (isRawInput) {
          type = argTypes[0];
        } else {
          type = argTypes[0]->asRow().childAt(kItemWithCountMap);
          type = type->asMap().keyType();
        }

        if (type->isLongDecimal() || type->isShortDecimal()) {
          BOLT_USER_FAIL(
              "Unsupported input type for percentile aggregation {}",
              type->toString());
        }

        switch (type->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<
                HiveUDAFPercentileAggregate<int64_t, int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<
                HiveUDAFPercentileAggregate<int64_t, int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<
                HiveUDAFPercentileAggregate<int64_t, int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<
                HiveUDAFPercentileAggregate<int64_t, int64_t>>(resultType);
          default:
            BOLT_USER_FAIL(
                "Unsupported input type for hive.udf.percentile aggregation {}",
                type->toString());
        }
      },
      /*registerCompanionFunctions*/ true);
}

} // namespace

void registerHiveUDAFPercentileAggregate(const std::string& prefix) {
  registerHiveUDAFPercentile(prefix + "percentile");
}

} // namespace bytedance::bolt::aggregate::prestosql
