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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/expression/VectorFunction.h"
#include "bolt/expression/VectorReaders.h"
#include "bolt/expression/VectorWriters.h"

// The description of the map function in Spark
// https://kontext.tech/article/586/spark-sql-map-functions
//
// Example:
// Select map(1,'a',2,'b',3,'c');
// map(1, a, 2, b, 3, c)
//
// Result:
// {1:"a",2:"b",3:"c"}
namespace bytedance::bolt::functions::sparksql {
namespace {

enum MapKeyDedupPolicy { EXCEPTION, LAST_WIN, FIRST_WIN };

const std::string DuplicateKeyExceptionInfo =
    "Duplicate map key was found, please check the input data. If you want "
    "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
    "FIRST_WIN/LAST_WIN so that the key inserted at first/last takes precedence.";

template <MapKeyDedupPolicy Policy>
class MapFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_USER_CHECK(
        args.size() >= 2 && args.size() % 2 == 0,
        "Map function must take an even number of arguments");
    auto mapSize = args.size() / 2;

    auto keyType = outputType->asMap().keyType();
    auto valueType = outputType->asMap().valueType();
    bool isAllKeyConstant = true;
    exec::DecodedArgs decodedArgs(rows, args, context);
    // Check key and value types
    for (auto i = 0; i < mapSize; i++) {
      auto key = decodedArgs.at(i * 2)->base();
      isAllKeyConstant &= key->isConstantEncoding();
      BOLT_USER_CHECK(
          key->type()->equivalent(*keyType),
          "All the key arguments in Map function must be the same!");
      BOLT_USER_CHECK(
          decodedArgs.at(i * 2 + 1)->base()->type()->equivalent(*valueType),
          "All the value arguments in Map function must be the same!");
    }

    context.ensureWritable(rows, outputType, result);
    if (keyType->isPrimitiveType()) {
      if (isAllKeyConstant) {
        BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            implForConstantPrimitiveKey,
            keyType->kind(),
            rows,
            mapSize,
            args,
            decodedArgs,
            result);
      } else {
        BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            implForPrimitiveKey,
            keyType->kind(),
            rows,
            mapSize,
            args,
            decodedArgs,
            result);
      }
      return;
    }

    exec::VectorWriter<Map<Any, Any>> resultWriter;
    auto mapResult = result->as<MapVector>();
    resultWriter.init(*mapResult);

    std::vector<std::unique_ptr<DecodedVector>> keyDecoders;
    std::vector<std::unique_ptr<DecodedVector>> valueDecoders;
    std::vector<std::unique_ptr<exec::VectorReader<Any>>> keyReaders;
    std::vector<std::unique_ptr<exec::VectorReader<Any>>> valueReaders;

    for (auto i = 0; i < mapSize; ++i) {
      keyDecoders.emplace_back(std::make_unique<DecodedVector>());
      keyDecoders.back()->decode(*args[i * 2], rows);
      keyReaders.emplace_back(
          std::make_unique<exec::VectorReader<Any>>(keyDecoders.back().get()));
      valueDecoders.emplace_back(std::make_unique<DecodedVector>());
      valueDecoders.back()->decode(*args[i * 2 + 1], rows);
      valueReaders.emplace_back(std::make_unique<exec::VectorReader<Any>>(
          valueDecoders.back().get()));
    }

    // Check map dup key and create result
    rows.applyToSelected([&](vector_size_t row) {
      resultWriter.setOffset(row);
      auto& mapWriter = resultWriter.current();

      std::unordered_set<exec::GenericView> keys;

      // Only used when Policy == LAST_WIN
      std::unordered_map<exec::GenericView, vector_size_t> lastIndex;
      if constexpr (Policy == LAST_WIN) {
        lastIndex.reserve(mapSize);
      }

      for (auto i = 0; i < mapSize; ++i) {
        const auto& keyReader = *keyReaders[i];
        const auto& valueReader = *valueReaders[i];

        BOLT_USER_CHECK(keyReader.isSet(row), "Cannot use null as map key!");

        exec::GenericView key = keyReader[row];

        if constexpr (Policy == LAST_WIN) {
          lastIndex.insert_or_assign(key, i);
          continue;
        }

        std::optional<exec::GenericView> value;

        if (valueReader.isSet(row)) {
          value.emplace(valueReader[row]);
        } else {
          value = std::nullopt;
        }

        if constexpr (Policy == EXCEPTION) {
          BOLT_USER_CHECK(keys.insert(key).second, DuplicateKeyExceptionInfo);
          if (value.has_value()) {
            auto [keyWriter, valueWriter] = mapWriter.add_item();
            keyWriter.copy_from(key);
            valueWriter.copy_from(*value);
          } else {
            mapWriter.add_null().copy_from(key); // value is null
          }
        } else if constexpr (Policy == FIRST_WIN) {
          if (keys.insert(key).second) {
            if (value.has_value()) {
              auto [keyWriter, valueWriter] = mapWriter.add_item();
              keyWriter.copy_from(key);
              valueWriter.copy_from(*value);
            } else {
              mapWriter.add_null().copy_from(key); // value is null
            }
          }
        } else {
          BOLT_UNREACHABLE();
        }
      }

      if constexpr (Policy == LAST_WIN) {
        for (auto i = 0; i < mapSize; ++i) {
          const auto& keyReader = *keyReaders[i];
          const auto& valueReader = *valueReaders[i];

          exec::GenericView key = keyReader[row];
          if (lastIndex.at(key) != i) {
            continue;
          }

          if (valueReader.isSet(row)) {
            auto [keyWriter, valueWriter] = mapWriter.add_item();
            keyWriter.copy_from(key);
            valueWriter.copy_from(valueReader[row]);
          } else {
            mapWriter.add_null().copy_from(key); // value is null
          }
        }
      }

      resultWriter.commit();
    });

    resultWriter.finish();
  }

 private:
  template <TypeKind Kind>
  void implForPrimitiveKey(
      const SelectivityVector& rows,
      vector_size_t mapSize,
      std::vector<VectorPtr>& args,
      exec::DecodedArgs& decodedArgs,
      VectorPtr& result) const {
    using NativeType = typename TypeTraits<Kind>::NativeType;
    auto mapResult = result->as<MapVector>();
    auto rawSizes = mapResult->mutableSizes(rows.end())->asMutable<int32_t>();
    auto rawOffsets =
        mapResult->mutableOffsets(rows.end())->asMutable<int32_t>();

    auto& keysResult = mapResult->mapKeys();
    auto& valuesResult = mapResult->mapValues();
    vector_size_t offset = keysResult->size();

    std::vector<const SimpleVector<NativeType>*> argsKeySimpleVector;
    argsKeySimpleVector.reserve(mapSize);
    for (vector_size_t i = 0; i < mapSize; i++) {
      argsKeySimpleVector.emplace_back(
          decodedArgs.at(i * 2)->base()->as<SimpleVector<NativeType>>());
    }
    std::vector<SelectivityVector> argsSelectivity(
        rows.end(), SelectivityVector(mapSize, false));
    folly::F14FastMap<NativeType, vector_size_t> keyToArgsIndex;
    keyToArgsIndex.reserve(mapSize);

    rows.applyToSelected([&](vector_size_t row) {
      for (vector_size_t i = 0; i < mapSize; i++) {
        auto decodedkey = decodedArgs.at(i * 2);
        BOLT_USER_CHECK(
            !decodedkey->isNullAt(row), "Cannot use null as map key!");
        auto key = argsKeySimpleVector[i];
        auto keyRow = decodedkey->index(row);
        if constexpr (Policy == EXCEPTION) {
          BOLT_USER_CHECK(
              keyToArgsIndex.insert({key->valueAt(keyRow), i}).second,
              DuplicateKeyExceptionInfo);
        } else if constexpr (Policy == FIRST_WIN) {
          keyToArgsIndex.insert({key->valueAt(keyRow), i});
        } else if constexpr (Policy == LAST_WIN) {
          keyToArgsIndex.insert_or_assign(key->valueAt(keyRow), i);
        } else {
          BOLT_UNREACHABLE();
        }
      }

      rawSizes[row] = keyToArgsIndex.size();
      rawOffsets[row] = offset;
      offset += keyToArgsIndex.size();
      auto& argsSelec = argsSelectivity[row];
      for (const auto& [_, argsIndex] : keyToArgsIndex) {
        argsSelec.setValid(argsIndex, true);
      }
      argsSelec.updateBounds();
      keyToArgsIndex.clear();
    });

    auto resultKeySize = offset;
    keysResult->resize(resultKeySize);
    valuesResult->resize(resultKeySize);
    SelectivityVector targetRows(resultKeySize, false);
    // Number of elements in the nth row of the result map.
    std::vector<vector_size_t> targetIdx(rows.size(), 0);
    std::vector<vector_size_t> toSourceRow(resultKeySize);

    // Copy key-value from the args to the result map column by column
    for (vector_size_t i = 0; i < mapSize; i++) {
      rows.applyToSelected([&](vector_size_t row) {
        const auto mapOffset = rawOffsets[row];
        if (argsSelectivity[row].isValid(i)) {
          targetRows.setValid(mapOffset + targetIdx[row], true);
          toSourceRow[mapOffset + targetIdx[row]] = row;
          targetIdx[row]++;
        }
      });
      targetRows.updateBounds();
      keysResult->copy(
          args[i * 2].get(), targetRows, toSourceRow.data(), false);
      valuesResult->copy(
          args[i * 2 + 1].get(), targetRows, toSourceRow.data(), false);
      targetRows.clearAll();
    }
  }

  template <TypeKind Kind>
  void implForConstantPrimitiveKey(
      const SelectivityVector& rows,
      vector_size_t mapSize,
      std::vector<VectorPtr>& args,
      exec::DecodedArgs& decodedArgs,
      VectorPtr& result) const {
    using NativeType = typename TypeTraits<Kind>::NativeType;
    auto mapResult = result->as<MapVector>();
    auto rawSizes = mapResult->mutableSizes(rows.end())->asMutable<int32_t>();
    auto rawOffsets =
        mapResult->mutableOffsets(rows.end())->asMutable<int32_t>();

    auto& keysResult = mapResult->mapKeys();
    auto& valuesResult = mapResult->mapValues();
    vector_size_t offset = keysResult->size();

    folly::F14FastMap<NativeType, vector_size_t> lastIndex;
    lastIndex.reserve(mapSize);
    folly::F14FastSet<NativeType> seen;
    seen.reserve(mapSize);
    std::vector<NativeType> argKeys;
    argKeys.reserve(mapSize);
    std::vector<vector_size_t> selectedArgs;
    selectedArgs.reserve(mapSize);

    // 1. For constant keys, only one time-consuming duplication is required.
    for (vector_size_t i = 0; i < mapSize; i++) {
      auto key =
          decodedArgs.at(i * 2)->base()->as<ConstantVector<NativeType>>();
      BOLT_USER_CHECK(!key->isNullAt(0), "Cannot use null as map key!");
      argKeys.emplace_back(key->valueAtFast(0));
      if constexpr (Policy == EXCEPTION) {
        BOLT_USER_CHECK(
            seen.insert(argKeys.back()).second, DuplicateKeyExceptionInfo);
        selectedArgs.emplace_back(i);
      } else if constexpr (Policy == FIRST_WIN) {
        if (seen.insert(argKeys.back()).second) {
          selectedArgs.emplace_back(i);
        }
      } else if constexpr (Policy == LAST_WIN) {
        lastIndex.insert_or_assign(argKeys.back(), i);
      } else {
        BOLT_UNREACHABLE();
      }
    }

    if constexpr (Policy == LAST_WIN) {
      for (vector_size_t i = 0; i < mapSize; i++) {
        if (lastIndex.at(argKeys[i]) == i) {
          selectedArgs.emplace_back(i);
        }
      }
    }

    const auto mapSizeAfterDedup = selectedArgs.size();
    rows.applyToSelected([&](vector_size_t row) {
      rawSizes[row] = mapSizeAfterDedup;
      rawOffsets[row] = offset;
      offset += mapSizeAfterDedup;
    });

    auto resultKeySize = offset;
    keysResult->resize(resultKeySize);
    valuesResult->resize(resultKeySize);
    auto flatKeysResult = keysResult->as<FlatVector<NativeType>>();
    BOLT_CHECK_NOT_NULL(flatKeysResult);
    auto rawKeysResult = flatKeysResult->mutableRawValues();

    auto beginRow = rows.begin();
    SelectivityVector targetRows(resultKeySize, false);
    vector_size_t targetIdx{0};
    std::vector<vector_size_t> toSourceRow(resultKeySize);

    // 2. Copy value and bool type keys to the result map column by column.
    // For other types of keys, copy first line from the args to the result
    // map, and then directly operate the `rawKeysResult` to memcpy the value of
    // each line.
    for (auto argsIndex : selectedArgs) {
      rows.applyToSelected([&](vector_size_t row) {
        const auto mapIndex = rawOffsets[row] + targetIdx;
        targetRows.setValid(mapIndex, true);
        toSourceRow[mapIndex] = row;
      });
      targetRows.updateBounds();
      auto resultKeyIndex = rawOffsets[beginRow] + targetIdx;
      if constexpr (Kind == TypeKind::BOOLEAN) {
        flatKeysResult->copy(
            args[argsIndex * 2].get(), targetRows, toSourceRow.data(), false);
      } else if constexpr (
          Kind == TypeKind::VARBINARY || Kind == TypeKind::VARCHAR) {
        flatKeysResult->set(resultKeyIndex, argKeys[argsIndex]);
      } else {
        rawKeysResult[resultKeyIndex] = argKeys[argsIndex];
      }
      valuesResult->copy(
          args[argsIndex * 2 + 1].get(), targetRows, toSourceRow.data(), false);
      targetRows.clearAll();
      targetIdx++;
    }
    if constexpr (Kind == TypeKind::BOOLEAN) {
      return;
    }

    rows.applyToSelected([&](vector_size_t row) {
      if (row == beginRow) {
        return;
      }
      auto resultKeyIndex = rawOffsets[row];
      memcpy(
          rawKeysResult + resultKeyIndex,
          rawKeysResult + resultKeyIndex - mapSizeAfterDedup,
          mapSizeAfterDedup * sizeof(NativeType));
    });
  }
};

std::unique_ptr<exec::VectorFunction> createMapFunction(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  const std::string mapKeyDedupPolicy = config.sparkMapKeyDedupPolicy();

  if (mapKeyDedupPolicy == "FIRST_WIN") {
    return std::make_unique<MapFunction<FIRST_WIN>>();
  } else if (mapKeyDedupPolicy == "LAST_WIN") {
    return std::make_unique<MapFunction<LAST_WIN>>();
  } else if (mapKeyDedupPolicy == "EXCEPTION") {
    return std::make_unique<MapFunction<EXCEPTION>>();
  } else {
    BOLT_FAIL("Unknown mapKeyDedupPolicy: {}", mapKeyDedupPolicy);
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // For the purpose of testing we introduce up to 6 inputs
  // array(K), array(V) -> map(K,V)
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  auto builder = exec::FunctionSignatureBuilder()
                     .knownTypeVariable("K")
                     .typeVariable("V")
                     .argumentType("K")
                     .argumentType("V")
                     .argumentType("any")
                     .variableArity()
                     .returnType("map(K,V)");
  signatures.push_back(builder.build());
  return signatures;
}
} // namespace

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(udf_map, signatures(), createMapFunction);

} // namespace bytedance::bolt::functions::sparksql
