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

#include "bolt/exec/Aggregate.h"
#include "bolt/exec/Strings.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/type/Conversions.h"
#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"

#include <fmt/format.h>
namespace bytedance::bolt::aggregate::prestosql {
namespace {

using PairType = std::pair<const StringView, int64_t>;
using MapAllocatorType = AlignedStlAllocator<PairType, 16>;

struct MapSumAccumulator {
  MapSumAccumulator(HashStringAllocator* alloc)
      : map_(MapAllocatorType(alloc)) {}

  using MapType = folly::F14FastMap<
      const StringView,
      int64_t,
      std::hash<StringView>,
      std::equal_to<StringView>,
      MapAllocatorType>;

  Strings strings_;
  MapType map_;
};

template <bool isDecimal, TypeKind kind>
class MapSumAggregate : public exec::Aggregate {
 public:
  MapSumAggregate(TypePtr resultType, TypePtr valueType)
      : exec::Aggregate(resultType), valueType_(valueType) {
    if constexpr (isDecimal) {
      const auto precisionScale = getDecimalPrecisionScale(*valueType);
      scaleFactor_ = DecimalUtil::getPowersOfTen(precisionScale.second);
    }
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(MapSumAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(MapSumAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) MapSumAccumulator(allocator_);
    }
    setAllNulls(groups, indices);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto accumulator = value<MapSumAccumulator>(group);
      accumulator->strings_.free(*allocator_);
      accumulator->~MapSumAccumulator();
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addInputImpl<false, false, ValueType>(groups, rows, args);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addInputImpl<true, false, ValueType>(group, rows, args);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto mapVector = (*result)->as<MapVector>();
    BOLT_CHECK(mapVector);
    mapVector->resize(numGroups);
    auto mapKeys = mapVector->mapKeys();
    auto mapValues = mapVector->mapValues();
    auto numElements = countElements(groups, numGroups);
    mapKeys->resize(numElements);
    mapValues->resize(numElements);
    auto keys = mapKeys->as<FlatVector<StringView>>();
    auto values = mapValues->as<FlatVector<int64_t>>();
    auto* rawNulls = getRawNulls(mapVector);
    vector_size_t offset = 0;

    auto* rawOffsets = mapVector->offsets()->asMutable<vector_size_t>();
    auto* rawSizes = mapVector->sizes()->asMutable<vector_size_t>();

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];

      if (isNull(group)) {
        // aggregate_map_sum should return an empty map for empty groups instead
        // of NULLs based on empirical results from Spark Java.
        clearNull(rawNulls, i);
        rawOffsets[i] = 0;
        rawSizes[i] = 0;
        continue;
      }

      clearNull(rawNulls, i);

      auto accumulator = value<MapSumAccumulator>(group);
      auto mapSize = accumulator->map_.size();
      if (mapSize) {
        auto index = 0;
        for (const auto& [key, value] : accumulator->map_) {
          // LOG(ERROR) << fmt::format("extract key: {}, val: {}", key, value);
          keys->set(offset + index, key);
          values->set(offset + index, value);
          ++index;
        }
        mapVector->setOffsetAndSize(i, offset, mapSize);
        offset += mapSize;
      } else {
        mapVector->setOffsetAndSize(i, 0, 0);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown = false) override {
    addInputImpl<false, true, int64_t>(groups, rows, args);
  }

  void addSingleGroupIntermediateResults(
      char* groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown = false) override {
    addInputImpl<true, true, int64_t>(groups, rows, args);
  }

 private:
  using ValueType = typename TypeTraits<kind>::NativeType;

  static_assert(
      !isDecimal || kind == TypeKind::BIGINT || kind == TypeKind::HUGEINT);

  int64_t plus(int64_t a, int64_t b) {
    int64_t result;
    auto isOverflow = __builtin_add_overflow(a, b, &result);
    if (isOverflow) {
      result = a > 0 ? std::numeric_limits<int64_t>::max()
                     : std::numeric_limits<int64_t>::min();
    }
    return result;
  }

  int64_t decimalToLong(ValueType val) {
    auto integralPart = val / scaleFactor_;
    if (integralPart > std::numeric_limits<int64_t>::max()) {
      return std::numeric_limits<int64_t>::max();
    } else if (integralPart < std::numeric_limits<int64_t>::min()) {
      return std::numeric_limits<int64_t>::min();
    }
    return integralPart;
  }

  int64_t toLong(ValueType val) {
    if constexpr (isDecimal) {
      return decimalToLong(val);
    } else {
      return util::Converter<TypeKind::BIGINT, void, util::TruncateCastPolicy>::
          cast(val, nullptr);
    }
  }

  template <bool isSingleGroup, bool isIntermediate, typename TValue>
  void addInputImpl(
      std::conditional_t<isSingleGroup, char*, char**> groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->as<MapVector>();

    BOLT_CHECK_NOT_NULL(mapVector);

    const SelectivityVector* baseRows = &rows;
    SelectivityVector innerRows{mapVector->size(), false};
    if (!decodedMaps_.isIdentityMapping()) {
      if (decodedMaps_.isConstantMapping()) {
        innerRows.setValid(decodedMaps_.index(0), true);
        innerRows.updateBounds();
      } else {
        bolt::translateToInnerRows(
            rows, decodedMaps_.indices(), decodedMaps_.nulls(), innerRows);
      }
      baseRows = &innerRows;
    }

    decodedKeys_.decode(*mapVector->mapKeys(), *baseRows);
    decodedValues_.decode(*mapVector->mapValues(), *baseRows);
    rows.applyToSelected([&](vector_size_t row) {
      char* group;
      if constexpr (isSingleGroup) {
        group = groups;
      } else {
        group = groups[row];
      }
      auto accumulator = value<MapSumAccumulator>(group);

      auto decodedRow = decodedMaps_.index(row);

      if (LIKELY(!decodedMaps_.isNullAt(row))) {
        clearNull(group);
        auto offset = mapVector->offsetAt(decodedRow);
        auto size = mapVector->sizeAt(decodedRow);
        auto tracker = trackRowSize(group);
        for (auto i = offset; i < size + offset; ++i) {
          if (UNLIKELY(decodedValues_.isNullAt(i))) {
            continue;
          }
          auto key = decodedKeys_.valueAt<StringView>(i);
          auto val = decodedValues_.valueAt<TValue>(i);
          int64_t longVal;
          if constexpr (isIntermediate) {
            longVal = val;
          } else {
            longVal = toLong(val);
          }
          update(key, longVal, group);
        }
      }
    });
  }

  void update(StringView key, int64_t val, char* group) {
    // LOG(ERROR) << fmt::format("update key: {} value: {}", key, val);
    auto accumulator = value<MapSumAccumulator>(group);
    auto& map = accumulator->map_;
    auto i = map.find(key);
    if (LIKELY(i != map.end())) {
      // aggregate_map_sum does not handle overflow
#ifdef SPARK_COMPATIBLE
      i->second += val;
#else
      i->second = plus(i->second, val);
#endif
      return;
    }
    auto tracker = trackRowSize(group);
    if (!key.isInline()) {
      key = accumulator->strings_.append(key, *allocator_);
    }
    map.insert({key, val});
  }

  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<MapSumAccumulator>(groups[i])->map_.size();
    }
    return size;
  }

  DecodedVector decodedMaps_;
  DecodedVector decodedKeys_;
  DecodedVector decodedValues_;
  TypePtr valueType_;
  int128_t scaleFactor_;
};

std::unique_ptr<exec::Aggregate> constructImpl(
    TypePtr resultType,
    TypePtr valueType) {
  auto isDecimal = valueType->isDecimal();
  auto kind = valueType->kind();
  if (isDecimal) {
    switch (kind) {
      case TypeKind::BIGINT:
        return std::make_unique<MapSumAggregate<true, TypeKind::BIGINT>>(
            resultType, valueType);
      case TypeKind::HUGEINT:
        return std::make_unique<MapSumAggregate<true, TypeKind::HUGEINT>>(
            resultType, valueType);
      default:
        BOLT_FAIL(
            fmt::format("invalid decimal type: {}", valueType->toString()));
    }
  } else {
    switch (kind) {
      case TypeKind::TINYINT:
        return std::make_unique<MapSumAggregate<false, TypeKind::TINYINT>>(
            resultType, valueType);
      case TypeKind::SMALLINT:
        return std::make_unique<MapSumAggregate<false, TypeKind::SMALLINT>>(
            resultType, valueType);
      case TypeKind::INTEGER:
        return std::make_unique<MapSumAggregate<false, TypeKind::INTEGER>>(
            resultType, valueType);
      case TypeKind::BIGINT:
        return std::make_unique<MapSumAggregate<false, TypeKind::BIGINT>>(
            resultType, valueType);
      case TypeKind::REAL:
        return std::make_unique<MapSumAggregate<false, TypeKind::REAL>>(
            resultType, valueType);
      case TypeKind::DOUBLE:
        return std::make_unique<MapSumAggregate<false, TypeKind::DOUBLE>>(
            resultType, valueType);
      default:
        BOLT_FAIL(fmt::format(
            "aggregate_map_sum: invalid value type: {}",
            valueType->toString()));
    }
  }
}

exec::AggregateRegistrationResult registerMapSum(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  auto signature = exec::AggregateFunctionSignatureBuilder()
                       .typeVariable("V")
                       .argumentType("map(varchar, V)")
                       .intermediateType("map(varchar, bigint)")
                       .returnType("map(varchar, bigint)")
                       .build();
  return exec::registerAggregateFunction(
      name,
      {std::move(signature)},
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /* config */) -> std::unique_ptr<exec::Aggregate> {
        BOLT_USER_CHECK_EQ(argTypes.size(), 1);
        auto type = argTypes[0];
        auto errorMsg = fmt::format(
            "the argument of {} must be map(varchar, integer type/float type/decimal)",
            name);
        BOLT_USER_CHECK(type->isMap(), errorMsg);
        auto keyType = type->childAt(0);
        BOLT_USER_CHECK(type->childAt(0)->isVarchar(), errorMsg);
        auto valueType = type->childAt(1);
        BOLT_USER_CHECK(
            valueType->isTinyint() || valueType->isSmallint() ||
                valueType->isInteger() || valueType->isBigint() ||
                valueType->isDouble() || valueType->isReal() ||
                valueType->isDecimal(),
            errorMsg);
        return constructImpl(resultType, valueType);
      },
      withCompanionFunctions,
      overwrite);
}

}; // namespace

void registerMapSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerMapSum(prefix + kMapSum, withCompanionFunctions, overwrite);
}

}; // namespace bytedance::bolt::aggregate::prestosql
