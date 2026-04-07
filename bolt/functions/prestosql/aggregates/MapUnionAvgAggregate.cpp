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

#include <functional>
#include <utility>
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/lib/CheckedArithmeticImpl.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::aggregate::prestosql {

namespace {

template <typename K, typename S>
class MapUnionAvgAggregate : public exec::Aggregate {
 public:
  explicit MapUnionAvgAggregate(TypePtr resultType)
      : Aggregate(std::move(resultType)) {}

  using Accumulator = folly::F14FastMap<
      K,
      // pair of sum and count
      std::pair<S, int64_t>,
      std::hash<K>,
      std::equal_to<K>,
      AlignedStlAllocator<std::pair<const K, std::pair<S, int64_t>>, 16>>;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(Accumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto index : indices) {
      new (groups[index] + offset_) Accumulator{
          AlignedStlAllocator<std::pair<const K, std::pair<S, int64_t>>, 16>(
              allocator_)};
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto mapVector = (*result)->as<MapVector>();
    BOLT_CHECK(mapVector);
    mapVector->resize(numGroups);

    auto mapKeys = mapVector->mapKeys()->as<FlatVector<K>>();
    auto mapValues = mapVector->mapValues()->as<FlatVector<S>>();

    auto numElements = countElements(groups, numGroups);
    mapKeys->resize(numElements);
    mapValues->resize(numElements);

    auto rawNulls = mapVector->mutableRawNulls();
    vector_size_t index = 0;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        bits::setNull(rawNulls, i, true);
        mapVector->setOffsetAndSize(i, 0, 0);
      } else {
        clearNull(rawNulls, i);

        auto groupMap = value<Accumulator>(group);
        auto mapSize = groupMap->size();
        for (auto it = groupMap->begin(); it != groupMap->end(); ++it) {
          mapKeys->set(index, it->first);
          auto sumCountPair = it->second;
          if constexpr (std::is_same_v<S, double> || std::is_same_v<S, float>) {
            mapValues->set(index, sumCountPair.first / sumCountPair.second);
          } else {
            BOLT_CHECK(sumCountPair.second != 0, "division by zero");
            mapValues->set(
                index, sumCountPair.first / (double)sumCountPair.second);
          }
          ++index;
        }
        mapVector->setOffsetAndSize(i, index - mapSize, mapSize);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    // partial results are stored in a row vector:
    // [map(key, sum), array(count)]
    auto rowVector = (*result)->as<RowVector>();
    auto mapVector = rowVector->childAt(0)->as<MapVector>();
    auto countArrayVector = rowVector->childAt(1)->as<ArrayVector>();
    rowVector->resize(numGroups);
    mapVector->resize(numGroups);
    countArrayVector->resize(numGroups);

    auto keyVector = mapVector->mapKeys()->as<FlatVector<K>>();
    auto sumVector = mapVector->mapValues()->asFlatVector<S>();
    auto countArrayVectorElements = countArrayVector->elements();
    auto numElements = countElements(groups, numGroups);

    keyVector->resize(numElements);
    sumVector->resize(numElements);
    countArrayVectorElements->resize(numElements);
    auto rowRawNulls = rowVector->mutableRawNulls();
    auto mapVectorRawNulls = mapVector->mutableRawNulls();
    auto countVectorRawNulls = countArrayVector->mutableRawNulls();

    K* rawKeys = keyVector->mutableRawValues();
    S* rawSums = sumVector->mutableRawValues();
    auto counts = countArrayVectorElements->asUnchecked<FlatVector<int64_t>>()
                      ->mutableRawValues();
    vector_size_t index = 0;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        bits::setNull(mapVectorRawNulls, i, true);
        bits::setNull(countVectorRawNulls, i, true);
        bits::setNull(rowRawNulls, i, true);
        mapVector->setOffsetAndSize(i, 0, 0);
        countArrayVector->setOffsetAndSize(i, 0, 0);
      } else {
        clearNull(mapVectorRawNulls, i);
        clearNull(countVectorRawNulls, i);
        clearNull(rowRawNulls, i);
        auto groupMap = value<Accumulator>(group);
        auto mapSize = groupMap->size();
        for (auto it = groupMap->begin(); it != groupMap->end(); ++it) {
          rawKeys[index] = it->first;
          auto sumCountPair = it->second;
          rawSums[index] = sumCountPair.first;
          counts[index] = sumCountPair.second;
          ++index;
        }
        mapVector->setOffsetAndSize(i, index - mapSize, mapSize);
        countArrayVector->setOffsetAndSize(i, index - mapSize, mapSize);
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();
    auto mapKeys = mapVector->mapKeys()->template as<SimpleVector<K>>();
    auto mapValues = mapVector->mapValues()->template as<SimpleVector<S>>();

    rows.applyToSelected([&](auto row) {
      if (!decodedMaps_.isNullAt(row)) {
        clearNull(groups[row]);
        auto groupMap = value<Accumulator>(groups[row]);
        addRawInputMap(*groupMap, mapVector, mapKeys, mapValues, row);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();
    auto mapKeys = mapVector->mapKeys()->template as<SimpleVector<K>>();
    auto mapValues = mapVector->mapValues()->template as<SimpleVector<S>>();

    auto groupMap = value<Accumulator>(group);

    rows.applyToSelected([&](auto row) {
      if (!decodedMaps_.isNullAt(row)) {
        clearNull(group);
        addRawInputMap(*groupMap, mapVector, mapKeys, mapValues, row);
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    std::function<char*(vector_size_t)> groupSupplier =
        [groups](vector_size_t row) { return groups[row]; };
    addIntermediateInputMap(rows, args, groupSupplier);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    std::function<char*(vector_size_t)> groupSupplier =
        [group](vector_size_t row) { return group; };
    addIntermediateInputMap(rows, args, groupSupplier);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto groupMap = value<Accumulator>(group);
      std::destroy_at(groupMap);
    }
  }

 private:
  void addRawInputMap(
      Accumulator& groupMap,
      const MapVector* mapVector,
      const SimpleVector<K>* mapKeys,
      const SimpleVector<S>* mapValues,
      vector_size_t row) const {
    auto decodedRow = decodedMaps_.index(row);
    auto offset = mapVector->offsetAt(decodedRow);
    auto size = mapVector->sizeAt(decodedRow);
    for (auto i = 0; i < size; ++i) {
      // Ignore null map keys.
      if (!mapKeys->isNullAt(offset + i)) {
        auto key = mapKeys->valueAt(offset + i);
        if (mapValues->isNullAt(offset + i)) {
          continue;
        } else {
          auto value = mapValues->valueAt(offset + i);
          std::pair<S, int64_t>& sumCountPair = groupMap[key];
          sumCountPair.second += 1;
          if constexpr (std::is_same_v<S, double> || std::is_same_v<S, float>) {
            sumCountPair.first += value;
          } else {
            sumCountPair.first =
                functions::checkedPlus<S>(sumCountPair.first, value);
          }
        }
      }
    }
  }

  void addIntermediateInputMap(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      std::function<char*(vector_size_t)> groupSupplier) {
    decodedPartial_.decode(*args[0], rows);
    auto rowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto mapVector = rowVector->childAt(0)->as<MapVector>();
    auto countArrayVector = rowVector->childAt(1)->as<ArrayVector>();

    auto mapKeys = mapVector->mapKeys()->template as<SimpleVector<K>>();
    auto mapValues = mapVector->mapValues()->template as<SimpleVector<S>>();
    auto counts =
        countArrayVector->elements()->template as<SimpleVector<int64_t>>();
    rows.applyToSelected([&](auto row) {
      if (!decodedPartial_.isNullAt(row)) {
        char* group = groupSupplier(row);
        clearNull(group);
        updateNonNullValue(group, mapVector, mapKeys, mapValues, counts, row);
      }
    });
  }

  template <bool tableHasNulls = true>
  inline void updateNonNullValue(
      char* group,
      const MapVector* mapVector,
      const SimpleVector<K>* mapKeys,
      const SimpleVector<S>* mapValues,
      const SimpleVector<int64_t>* counts,
      vector_size_t row) const {
    auto decodedRow = decodedPartial_.index(row);
    auto offset = mapVector->offsetAt(decodedRow);
    auto size = mapVector->sizeAt(decodedRow);
    auto groupMap = value<Accumulator>(group);

    for (auto i = 0; i < size; ++i) {
      // Ignore null map keys.
      if (!mapKeys->isNullAt(offset + i)) {
        auto key = mapKeys->valueAt(offset + i);
        if (mapValues->isNullAt(offset + i)) {
          continue;
        } else {
          auto value = mapValues->valueAt(offset + i);
          auto count = counts->valueAt(offset + i);
          std::pair<S, int64_t>& sumCountPair = (*groupMap)[key];
          sumCountPair.second += count;
          if constexpr (std::is_same_v<S, double> || std::is_same_v<S, float>) {
            sumCountPair.first += value;
          } else {
            sumCountPair.first =
                functions::checkedPlus<S>(sumCountPair.first, value);
          }
        }
      }
    }
  }

  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<Accumulator>(groups[i])->size();
    }
    return size;
  }

  DecodedVector decodedMaps_;
  DecodedVector decodedPartial_;
};

template <typename K>
std::unique_ptr<exec::Aggregate> createMapUnionAvgAggregate(
    TypeKind valueKind,
    const TypePtr& resultType) {
  switch (valueKind) {
    case TypeKind::TINYINT:
      return std::make_unique<MapUnionAvgAggregate<K, int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<MapUnionAvgAggregate<K, int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<MapUnionAvgAggregate<K, int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<MapUnionAvgAggregate<K, int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<MapUnionAvgAggregate<K, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<MapUnionAvgAggregate<K, double>>(resultType);
    default:
      BOLT_UNREACHABLE();
  }
}

bool registerMapUnionAvg(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (auto keyType : {"tinyint", "smallint", "integer", "bigint"}) {
    for (auto valueType :
         {"tinyint", "smallint", "integer", "bigint", "double", "real"}) {
      auto mapType = fmt::format("map({},{})", keyType, valueType);
      // [map(key, sum), array(count)]
      auto intermediateType =
          fmt::format("row(map({},{}),array(bigint))", keyType, valueType);
      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType(mapType)
                               .intermediateType(intermediateType)
                               .argumentType(mapType)
                               .build());
    }
  }

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(argTypes.size(), 1);
        BOLT_CHECK(argTypes[0]->isMap() || argTypes[0]->isRow());
        bool isRawInput = exec::isRawInput(step);
        TypeKind keyType;
        TypeKind valueTypeKind;
        auto& mapType = isRawInput ? argTypes[0]->asMap()
                                   : argTypes[0]->childAt(0)->asMap();
        keyType = mapType.keyType()->kind();
        valueTypeKind = mapType.valueType()->kind();
        switch (keyType) {
          case TypeKind::TINYINT:
            return createMapUnionAvgAggregate<int8_t>(
                valueTypeKind, resultType);
          case TypeKind::SMALLINT:
            return createMapUnionAvgAggregate<int16_t>(
                valueTypeKind, resultType);
          case TypeKind::INTEGER:
            return createMapUnionAvgAggregate<int32_t>(
                valueTypeKind, resultType);
          case TypeKind::BIGINT:
            return createMapUnionAvgAggregate<int64_t>(
                valueTypeKind, resultType);
          default:
            BOLT_UNREACHABLE();
        }
      });

  return true;
}

} // namespace

void registerMapUnionAvgAggregate(const std::string& prefix) {
  registerMapUnionAvg(prefix + kMapUnionAvg);
}

} // namespace bytedance::bolt::aggregate::prestosql
