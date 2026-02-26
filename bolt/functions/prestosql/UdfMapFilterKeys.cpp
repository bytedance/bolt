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

#include <folly/container/F14Set.h>
#include <functions/lib/RowsTranslationUtil.h>
#include <type/Type.h>
#include <vector/ComplexVector.h>
#include <vector/ConstantVector.h>
#include <vector/DecodedVector.h>
#include <vector/SimpleVector.h>
#include <vector/TypeAliases.h>
#include <iostream>
#include <memory>
#include "Buffer.h"
#include "bolt/functions/Udf.h"
#include "bolt/functions/lib/CheckedArithmetic.h"
#include "bolt/functions/lib/LambdaFunctionUtil.h"
#include "bolt/type/Conversions.h"
namespace bytedance::bolt::functions {

/**
 * map_filter_keys(map[k, v], array[k]) -> map[k, v]
 *           return new map where keys exist in array
 */
class UdfMapFilterKeys : public exec::VectorFunction {
 public:
  // todo: consider whether null behavior is correct or not.
  bool isDefaultNullBehavior() const override {
    return false;
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    const std::vector<std::string> types = {
        "boolean",
        "tinyint",
        "smallint",
        "integer",
        "bigint",
        "double",
        "real",
        "varchar",
        "timestamp",
        "date",
    };
    // map(K,V), array(K) -> map(K,V)
    for (const auto& keyType : types) {
      for (const auto& valueType : types) {
        signatures.emplace_back(
            exec::FunctionSignatureBuilder()
                .returnType(fmt::format("map({}, {})", keyType, valueType))
                .argumentType(fmt::format("map({}, {})", keyType, valueType))
                .argumentType(fmt::format("array({})", keyType))
                .build());
      }
    }
    return signatures;
  }

  template <typename T>
  vector_size_t applyArrayTypedSet(
      const SelectivityVector& rows,
      BufferPtr& resultOffsets,
      BufferPtr& resultSizes,
      BufferPtr& selectedIndices,
      const MapVectorPtr orignlaMapVector,
      const VectorPtr& mapKeys,
      const VectorPtr& orignalArrayVector,
      const exec::EvalCtx& context) const {
    const auto* inputOffsets = orignlaMapVector->rawOffsets();
    const auto* inputSizes = orignlaMapVector->rawSizes();

    auto* pool = context.pool();
    resultSizes = allocateSizes(rows.end(), pool);
    resultOffsets = allocateOffsets(rows.end(), pool);
    auto* rawResultSizes = resultSizes->asMutable<vector_size_t>();
    auto* rawResultOffsets = resultOffsets->asMutable<vector_size_t>();

    const auto numElements = mapKeys->size();
    selectedIndices = allocateIndices(numElements, pool);
    auto* rawSelectedIndices = selectedIndices->asMutable<vector_size_t>();
    vector_size_t numSelected = 0;

    auto elementToTopLevelRows = getElementToTopLevelRows(
        numElements, rows, orignlaMapVector.get(), pool);

    exec::LocalDecodedVector arrayDecoder(context, *orignalArrayVector, rows);
    auto& decodedArray = *arrayDecoder.get();

    // fast path for seocnd constant input
    if (decodedArray.isConstantMapping()) {
      // construct constant element set
      auto arrayVector = decodedArray.base()->as<ArrayVector>();
      auto constantIdx = decodedArray.index(0);
      auto constantArrayOffset = arrayVector->offsetAt(constantIdx);
      auto constantArraySize = arrayVector->sizeAt(constantIdx);
      auto constantElements = arrayVector->elements()->asFlatVector<T>();

      folly::F14FastSet<T> elementSet;
      for (int i = constantArrayOffset;
           i < constantArrayOffset + constantArraySize;
           i++) {
        if (!constantElements->containsNullAt(i)) {
          elementSet.insert(constantElements->valueAt(i));
        }
      }
      auto simpleKeyVector = mapKeys->as<SimpleVector<T>>();
      BOLT_CHECK_NOT_NULL(
          simpleKeyVector,
          fmt::format("mapKeys encoding: {}", mapKeys->encoding()));
      // filter keys
      rows.applyToSelected([&](vector_size_t row) {
        rawResultOffsets[row] = numSelected;
        // filter keys
        for (auto j = inputOffsets[row];
             j < inputOffsets[row] + inputSizes[row];
             j++) {
          if (simpleKeyVector->hashValueAt(j) &&
              elementSet.contains(simpleKeyVector->valueAt(j))) {
            ++rawResultSizes[row];
            rawSelectedIndices[numSelected] = j;
            ++numSelected;
          }
        }
      });
    } else {
      auto flatArray = flattenArray(rows, orignalArrayVector, decodedArray);
      auto arrayElements = flatArray->elements()->as<SimpleVector<T>>();
      rows.applyToSelected([&](vector_size_t row) {
        folly::F14FastSet<T> elementSet;
        for (auto i = flatArray->offsetAt(row);
             i < flatArray->offsetAt(row) + flatArray->sizeAt(row);
             i++) {
          if (!arrayElements->containsNullAt(i)) {
            elementSet.insert(arrayElements->valueAt(i));
          }
        }
        rawResultOffsets[row] = numSelected;
        auto simpleKeyVector = mapKeys->as<SimpleVector<T>>();
        BOLT_CHECK_NOT_NULL(
            simpleKeyVector,
            fmt::format("mapKeys encoding: {}", mapKeys->encoding()));
        // filter keys
        for (auto j = inputOffsets[row];
             j < inputOffsets[row] + inputSizes[row];
             j++) {
          if (simpleKeyVector->hashValueAt(j) &&
              elementSet.contains(simpleKeyVector->valueAt(j))) {
            ++rawResultSizes[row];
            rawSelectedIndices[numSelected] = j;
            ++numSelected;
          }
        }
      });
    }
    selectedIndices->setSize(numSelected * sizeof(vector_size_t));
    return numSelected;
  }

 private:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 2);
    exec::LocalDecodedVector mapDecoder(context, *args[0], rows);
    auto& decodedMap = *mapDecoder.get();

    auto flatMap = flattenMap(rows, args[0], decodedMap);

    VectorPtr keys = flatMap->mapKeys();
    VectorPtr values = flatMap->mapValues();

    exec::LocalDecodedVector arrayDecoder(context, *args[1], rows);
    auto& decodedArray = *arrayDecoder.get();

    // result map vector resultSizes
    BufferPtr resultSizes;
    BufferPtr resultOffsets;
    BufferPtr selectedIndices;
    auto numSelected = 0;
    switch (keys->typeKind()) {
      case TypeKind::HUGEINT:
        numSelected = applyArrayTypedSet<int128_t>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::BIGINT:
        numSelected = applyArrayTypedSet<int64_t>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::INTEGER:
        numSelected = applyArrayTypedSet<int32_t>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::SMALLINT:
        numSelected = applyArrayTypedSet<int16_t>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::TINYINT:
        numSelected = applyArrayTypedSet<int8_t>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::REAL:
        numSelected = applyArrayTypedSet<float>(
            rows,
            resultOffsets,
            resultSizes,

            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
        numSelected = applyArrayTypedSet<double>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::BOOLEAN:
        numSelected = applyArrayTypedSet<bool>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::TIMESTAMP:
        numSelected = applyArrayTypedSet<Timestamp>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        numSelected = applyArrayTypedSet<StringView>(
            rows,
            resultOffsets,
            resultSizes,
            selectedIndices,
            flatMap,
            keys,
            args[1],
            context);
        break;
      default:
        BOLT_UNSUPPORTED(
            "Unsupported key type for map_filter_keys: {}",
            mapTypeKindToName(keys->typeKind()));
    }
    auto wrappedKeys = numSelected
        ? BaseVector::wrapInDictionary(
              BufferPtr(nullptr), selectedIndices, numSelected, std::move(keys))
        : nullptr;
    auto wrappedValues = numSelected ? BaseVector::wrapInDictionary(
                                           BufferPtr(nullptr),
                                           selectedIndices,
                                           numSelected,
                                           std::move(values))
                                     : nullptr;
    // Set nulls for rows not present in 'rows'.
    BufferPtr newNulls = addNullsForUnselectedRows(flatMap, rows);
    auto localResult = std::make_shared<MapVector>(
        flatMap->pool(),
        outputType,
        std::move(newNulls),
        rows.end(),
        std::move(resultOffsets),
        std::move(resultSizes),
        wrappedKeys,
        wrappedValues);
    context.moveOrCopyResult(localResult, rows, result);
  }
};

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_map_filter_keys,
    UdfMapFilterKeys::signatures(),
    std::make_unique<UdfMapFilterKeys>());

} // namespace bytedance::bolt::functions
