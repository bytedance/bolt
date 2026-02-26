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

#include <folly/container/F14Map.h>
#include "bolt/expression/Expr.h"
#include "bolt/expression/VectorFunction.h"
namespace bytedance::bolt::functions {
/**
 * update(map(),map(),boolean) -> map()
 * update(map(),map()) -> map()
 */

class MapUpdateFunction : public exec::VectorFunction {
  bool isDefaultNullBehavior() const override {
    return false;
  }

 public:
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
                .argumentType(fmt::format("map({}, {})", keyType, valueType))
                .build());
        signatures.emplace_back(
            exec::FunctionSignatureBuilder()
                .returnType(fmt::format("map({}, {})", keyType, valueType))
                .argumentType(fmt::format("map({}, {})", keyType, valueType))
                .argumentType(fmt::format("map({}, {})", keyType, valueType))
                .argumentType("boolean")
                .build());
      }
    }
    return signatures;
  }

  template <TypeKind Kind>
  void applyMapUpdate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result,
      const bool insertNew) const {
    using T = typename TypeTraits<Kind>::NativeType;
    exec::DecodedArgs decodedArgs(rows, args, context);
    vector_size_t maxSize = 0;
    for (auto i = 0; i < 2; i++) {
      auto decodedArg = decodedArgs.at(i);
      auto inputMap = decodedArg->base()->as<MapVector>();
      auto rawSizes = inputMap->rawSizes();
      rows.applyToSelected([&](vector_size_t row) {
        maxSize += rawSizes[decodedArg->index(row)];
      });
    }
    auto keyType = outputType->asMap().keyType();
    auto valueType = outputType->asMap().valueType();

    auto* pool = context.pool();
    auto updatedKeys = BaseVector::create(keyType, maxSize, pool);
    auto updatedValues = BaseVector::create(valueType, maxSize, pool);
    BufferPtr nulls = allocateNulls(rows.size(), pool);
    auto mutableNulls = nulls->asMutable<uint64_t>();

    // Initialize offsets and sizes to 0 so that canonicalize() will
    // work also for sparse 'rows'.
    BufferPtr offsets = allocateOffsets(rows.end(), pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();

    BufferPtr sizes = allocateSizes(rows.end(), pool);
    auto rawSizes = sizes->asMutable<vector_size_t>();

    vector_size_t offset = 0;
    rows.applyToSelected([&](vector_size_t row) {
      rawOffsets[row] = offset;
      auto firstDecodeArg = decodedArgs.at(0);
      if (firstDecodeArg->isNullAt(row)) {
        bits::setNull(mutableNulls, row);
      } else {
        auto firstIndex = firstDecodeArg->index(row);
        auto firstMap =
            const_cast<MapVector*>(firstDecodeArg->base()->as<MapVector>());
        auto firstOffset = firstMap->offsetAt(firstIndex);
        auto firstSize = firstMap->sizeAt(firstIndex);
        auto firstMapValues = firstMap->mapKeys();

        auto secondDecodeArg = decodedArgs.at(1);
        auto secondIndex = secondDecodeArg->index(row);
        auto secondMap =
            const_cast<MapVector*>(secondDecodeArg->base()->as<MapVector>());
        auto secondOffset = secondMap->offsetAt(secondIndex);
        auto secondSize = secondMap->sizeAt(secondIndex);
        auto secondMapValues = secondMap->mapKeys();
        auto simplefirstKeyVector = firstMapValues->as<SimpleVector<T>>();
        BOLT_CHECK_NOT_NULL(
            simplefirstKeyVector,
            fmt::format(
                "firstMapValues encoding: {}", firstMapValues->encoding()));
        auto simpleSecondKeyVector = secondMapValues->as<SimpleVector<T>>();
        BOLT_CHECK_NOT_NULL(
            simpleSecondKeyVector,
            fmt::format(
                "secondMapValues encoding: {}", secondMapValues->encoding()));

        folly::F14FastMap<T, vector_size_t> elementMap;
        for (auto i = firstOffset; i < firstOffset + firstSize; i++) {
          elementMap.insert(
              {simplefirstKeyVector->valueAt(i), offset + i - firstOffset});
        }

        folly::F14FastMap<T, vector_size_t> elementSecondMap;
        for (auto i = secondOffset; i < secondOffset + secondSize; i++) {
          elementSecondMap.insert({simpleSecondKeyVector->valueAt(i), i});
        }
        updatedKeys->copy(
            firstMap->mapKeys().get(), offset, firstOffset, firstSize);
        updatedValues->copy(
            firstMap->mapValues().get(), offset, firstOffset, firstSize);
        offset = offset + firstSize;
        for (const auto& key : elementSecondMap) {
          auto it = elementMap.find(key.first);
          if (it != elementMap.end()) {
            updatedValues->copy(
                secondMap->mapValues().get(),
                elementMap.at(key.first),
                elementSecondMap.at(key.first),
                1);
          } else if (insertNew) {
            updatedKeys->copy(
                secondMap->mapKeys().get(),
                offset,
                elementSecondMap.at(key.first),
                1);
            updatedValues->copy(
                secondMap->mapValues().get(),
                offset,
                elementSecondMap.at(key.first),
                1);
            offset++;
          }
        }
        rawSizes[row] = offset - rawOffsets[row];
      }
    });
    auto updatedMap = std::make_shared<MapVector>(
        pool,
        outputType,
        nulls,
        rows.end(),
        offsets,
        sizes,
        updatedKeys,
        updatedValues);
    MapVector::canonicalize(updatedMap, true);
    context.moveOrCopyResult(updatedMap, rows, result);
  };

 private:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_USER_CHECK(
        args.size() <= 3, "There should be no more than 3 input arguments.");
    BOLT_USER_CHECK(
        args.size() >= 2, "There should be at least 2 input arguments.");
    auto mapType = args[0]->type();
    BOLT_USER_CHECK(
        mapType->kind() == TypeKind::MAP,
        "The first input argument should be a map.");
    BOLT_USER_CHECK(
        mapType->kindEquals(args[1]->type()),
        "The second input argument should be a map.");
    BOLT_CHECK(mapType->kindEquals(outputType));
    if (args.size() == 3)
      BOLT_USER_CHECK(
          !args[2]->isNullAt(0), "The third argument cannot be NULL.");
    bool insertNew = args.size() == 3
        ? args[2]->as<SimpleVector<bool>>()->valueAt(0)
        : false;
    auto keyType = outputType->asMap().keyType();
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        applyMapUpdate,
        keyType->kind(),
        rows,
        args,
        outputType,
        context,
        result,
        insertNew);
  }
};
BOLT_DECLARE_VECTOR_FUNCTION(
    udf_map_update,
    MapUpdateFunction::signatures(),
    std::make_unique<MapUpdateFunction>());
} // namespace bytedance::bolt::functions
