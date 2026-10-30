/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <algorithm>

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/lib/RowsTranslationUtil.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/LazyVector.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

struct DecodedMap {
  DecodedMap(
      const MapVector* map,
      DecodedVector& parent,
      const SelectivityVector& rows,
      exec::EvalCtx& context)
      : keys(context), values(context) {
    const auto flatOrConstant = [](const VectorPtr& vector) {
      return vector->isFlatEncoding() || vector->isConstantEncoding();
    };
    if (flatOrConstant(map->mapKeys()) && flatOrConstant(map->mapValues())) {
      keys.get()->decode(*map->mapKeys());
      values.get()->decode(*map->mapValues());
    } else {
      auto elementRows = toElementRows(
          map->mapKeys()->size(),
          rows,
          map,
          parent.isIdentityMapping() ? nullptr : parent.indices());
      if (isLazyNotLoaded(*map->mapKeys())) {
        LazyVector::ensureLoadedRows(map->mapKeys(), elementRows);
      }
      if (isLazyNotLoaded(*map->mapValues())) {
        LazyVector::ensureLoadedRows(map->mapValues(), elementRows);
      }
      keys.get()->decode(*map->mapKeys(), elementRows);
      values.get()->decode(*map->mapValues(), elementRows);
    }
  }

  exec::LocalDecodedVector keys;
  exec::LocalDecodedVector values;
};

int32_t compareKeys(
    const DecodedVector& left,
    vector_size_t leftIndex,
    const DecodedVector& right,
    vector_size_t rightIndex) {
  const bool leftNull = left.isNullAt(leftIndex);
  const bool rightNull = right.isNullAt(rightIndex);
  if (leftNull || rightNull) {
    return leftNull == rightNull ? 0 : leftNull ? -1 : 1;
  }
  return left.valueAt<StringView>(leftIndex).compare(
      right.valueAt<StringView>(rightIndex));
}

bool mapEqual(
    const MapVector* left,
    vector_size_t leftRow,
    const MapVector* right,
    vector_size_t rightRow,
    const DecodedMap& leftData,
    const DecodedMap& rightData) {
  const auto size = left->rawSizes()[leftRow];
  if (size == 0) {
    return true;
  }
  if (size == 1) {
    const auto leftEntry = left->rawOffsets()[leftRow];
    const auto rightEntry = right->rawOffsets()[rightRow];
    return !leftData.values->isNullAt(leftEntry) &&
        !rightData.values->isNullAt(rightEntry) &&
        compareKeys(*leftData.keys, leftEntry, *rightData.keys, rightEntry) ==
        0 &&
        leftData.values->valueAt<int32_t>(leftEntry) ==
        rightData.values->valueAt<int32_t>(rightEntry);
  }

  const auto leftIndices = left->sortedKeyIndices(leftRow);
  const auto rightIndices = right->sortedKeyIndices(rightRow);
  size_t leftPos = 0;
  size_t rightPos = 0;
  // Flink keeps the last value for duplicate keys and visits only left keys.
  while (leftPos < leftIndices.size()) {
    const auto leftKey = leftIndices[leftPos];
    auto leftValue = leftKey;
    do {
      leftValue = std::max(leftValue, leftIndices[leftPos++]);
    } while (
        leftPos < leftIndices.size() &&
        compareKeys(
            *leftData.keys, leftKey, *leftData.keys, leftIndices[leftPos]) ==
            0);

    int32_t keyComparison = -1;
    while (rightPos < rightIndices.size()) {
      keyComparison = compareKeys(
          *leftData.keys, leftKey, *rightData.keys, rightIndices[rightPos]);
      if (keyComparison <= 0) {
        break;
      }
      ++rightPos;
    }
    if (rightPos == rightIndices.size() || keyComparison != 0) {
      return false;
    }

    const auto rightKey = rightIndices[rightPos];
    auto rightValue = rightKey;
    do {
      rightValue = std::max(rightValue, rightIndices[rightPos++]);
    } while (rightPos < rightIndices.size() &&
             compareKeys(
                 *rightData.keys,
                 rightKey,
                 *rightData.keys,
                 rightIndices[rightPos]) == 0);

    if (leftData.values->isNullAt(leftValue) ||
        rightData.values->isNullAt(rightValue) ||
        leftData.values->valueAt<int32_t>(leftValue) !=
            rightData.values->valueAt<int32_t>(rightValue)) {
      return false;
    }
  }
  return true;
}

class FlinkMapEqual final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decoded(rows, args, context);
    auto* left = decoded.at(0);
    auto* right = decoded.at(1);
    const auto* leftMap = left->base()->asUnchecked<MapVector>();
    const auto* rightMap = right->base()->asUnchecked<MapVector>();
    context.ensureWritable(rows, outputType, result);
    auto* output = result->asFlatVector<bool>();
    exec::LocalSelectivityVector equalSizeRows(context);
    SelectivityVector* matchingRows = nullptr;
    rows.applyToSelected([&](vector_size_t row) {
      if (leftMap->sizeAt(left->index(row)) !=
          rightMap->sizeAt(right->index(row))) {
        output->set(row, false);
        if (!matchingRows) {
          matchingRows = equalSizeRows.get(rows);
        }
        matchingRows->setValid(row, false);
      }
    });
    if (matchingRows) {
      matchingRows->updateBounds();
      if (!matchingRows->hasSelections()) {
        return;
      }
    }
    const auto& compareRows = matchingRows ? *matchingRows : rows;
    const DecodedMap leftData(leftMap, *left, compareRows, context);
    const DecodedMap rightData(rightMap, *right, compareRows, context);
    compareRows.applyToSelected([&](vector_size_t row) {
      output->set(
          row,
          mapEqual(
              leftMap,
              left->index(row),
              rightMap,
              right->index(row),
              leftData,
              rightData));
    });
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> mapEqualSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("map(varchar,integer)")
              .argumentType("map(varchar,integer)")
              .build()};
}

} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    flink_map_equal,
    mapEqualSignatures(),
    std::make_unique<FlinkMapEqual>());

} // namespace bytedance::bolt::functions::flinksql
