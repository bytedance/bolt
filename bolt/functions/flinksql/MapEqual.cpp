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
#include <cmath>
#include <numeric>

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/InlineFlatten.h"
#include "bolt/functions/flinksql/Comparisons.h"
#include "bolt/functions/lib/RowsTranslationUtil.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/LazyVector.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

template <typename T>
int32_t compareFloatingKeys(T left, T right) {
  if (left == right) {
    // Java boxed keys distinguish signed zeros, unlike numeric values.
    return std::signbit(right) - std::signbit(left);
  }
  if (std::isnan(left) || std::isnan(right)) {
    return std::isnan(left) - std::isnan(right);
  }
  return left < right ? -1 : 1;
}

struct ScalarComparison {
  using Key = int32_t (*)(
      const DecodedVector&,
      vector_size_t,
      const DecodedVector&,
      vector_size_t);
  using Value = bool (*)(
      const DecodedVector&,
      vector_size_t,
      const DecodedVector&,
      vector_size_t);
  Key key = nullptr;
  Value value = nullptr;
};

template <typename T>
FLATTEN int32_t compareScalarKeys(
    const DecodedVector& left,
    vector_size_t li,
    const DecodedVector& right,
    vector_size_t ri) {
  const bool ln = left.isNullAt(li);
  const bool rn = right.isNullAt(ri);
  if (ln || rn) {
    return ln == rn ? 0 : ln ? -1 : 1;
  }
  const auto l = left.valueAt<T>(li);
  const auto r = right.valueAt<T>(ri);
  if constexpr (std::is_floating_point_v<T>) {
    return compareFloatingKeys(l, r);
  } else if constexpr (std::is_same_v<T, StringView>) {
    return l.compare(r);
  } else {
    return l == r ? 0 : l < r ? -1 : 1;
  }
}

template <typename T>
FLATTEN bool equalScalarValues(
    const DecodedVector& left,
    vector_size_t li,
    const DecodedVector& right,
    vector_size_t ri) {
  return !left.isNullAt(li) && !right.isNullAt(ri) &&
      left.valueAt<T>(li) == right.valueAt<T>(ri);
}

template <typename T>
ScalarComparison scalarComparison() {
  return {compareScalarKeys<T>, equalScalarValues<T>};
}

ScalarComparison scalarComparison(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return scalarComparison<bool>();
    case TypeKind::TINYINT:
      return scalarComparison<int8_t>();
    case TypeKind::SMALLINT:
      return scalarComparison<int16_t>();
    case TypeKind::INTEGER:
      return scalarComparison<int32_t>();
    case TypeKind::BIGINT:
      return scalarComparison<int64_t>();
    case TypeKind::HUGEINT:
      return scalarComparison<int128_t>();
    case TypeKind::REAL:
      return scalarComparison<float>();
    case TypeKind::DOUBLE:
      return scalarComparison<double>();
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return scalarComparison<StringView>();
    case TypeKind::TIMESTAMP:
      return scalarComparison<Timestamp>();
    default:
      return {};
  }
}

// Cache child decoding once per batch, including selected nested elements.
struct DecodedValue {
  DecodedValue(
      const VectorPtr& vector,
      const SelectivityVector& rows,
      exec::EvalCtx& context)
      : decoded(context), scalar(scalarComparison(vector->typeKind())) {
    if (vector->type()->isPrimitiveType() &&
        (vector->isFlatEncoding() || vector->isConstantEncoding())) {
      decoded.get()->decode(*vector);
      return;
    }
    if (!rows.hasSelections()) {
      return;
    }
    if (isLazyNotLoaded(*vector)) {
      LazyVector::ensureLoadedRows(vector, rows);
    }
    if (vector->isFlatEncoding() || vector->isConstantEncoding()) {
      decoded.get()->decode(*vector);
    } else {
      decoded.get()->decode(*vector, rows);
    }
    const auto* base = decoded->base();
    if (base->type()->isPrimitiveType()) {
      return;
    }
    SelectivityVector baseRows(base->size(), false);
    rows.applyToSelected([&](vector_size_t row) {
      if (!decoded->isNullAt(row)) {
        baseRows.setValid(decoded->index(row), true);
      }
    });
    baseRows.updateBounds();
    switch (base->typeKind()) {
      case TypeKind::ROW:
        for (const auto& child : base->asUnchecked<RowVector>()->children()) {
          children.emplace_back(
              std::make_unique<DecodedValue>(child, baseRows, context));
        }
        break;
      case TypeKind::ARRAY: {
        const auto* array = base->asUnchecked<ArrayVector>();
        const auto elements =
            toElementRows(array->elements()->size(), baseRows, array);
        children.emplace_back(std::make_unique<DecodedValue>(
            array->elements(), elements, context));
        break;
      }
      case TypeKind::MAP: {
        const auto* map = base->asUnchecked<MapVector>();
        const auto elements =
            toElementRows(map->mapKeys()->size(), baseRows, map);
        children.emplace_back(
            std::make_unique<DecodedValue>(map->mapKeys(), elements, context));
        children.emplace_back(std::make_unique<DecodedValue>(
            map->mapValues(), elements, context));
        break;
      }
      default:
        BOLT_UNREACHABLE();
    }
  }

  exec::LocalDecodedVector decoded;
  const ScalarComparison scalar;
  std::vector<std::unique_ptr<DecodedValue>> children;
};

std::vector<vector_size_t> sortedKeys(
    const MapVector* map,
    vector_size_t row,
    const DecodedVector& keys,
    ScalarComparison::Key compare) {
  if (map->mapKeys()->typeKind() != TypeKind::REAL &&
      map->mapKeys()->typeKind() != TypeKind::DOUBLE) {
    return map->sortedKeyIndices(row);
  }
  std::vector<vector_size_t> indices(map->sizeAt(row));
  std::iota(indices.begin(), indices.end(), map->offsetAt(row));
  std::sort(indices.begin(), indices.end(), [&](auto left, auto right) {
    return compare(keys, left, keys, right) < 0;
  });
  return indices;
}

struct GenericKeyCompare {
  ScalarComparison::Key compare;

  FOLLY_ALWAYS_INLINE int32_t operator()(
      const DecodedVector& left,
      vector_size_t li,
      const DecodedVector& right,
      vector_size_t ri) const {
    return compare(left, li, right, ri);
  }

  auto sortedIndices(
      const MapVector* map,
      vector_size_t row,
      const DecodedVector& keys) const {
    return sortedKeys(map, row, keys, compare);
  }
};

struct StringKeyCompare {
  FOLLY_ALWAYS_INLINE int32_t operator()(
      const DecodedVector& left,
      vector_size_t li,
      const DecodedVector& right,
      vector_size_t ri) const {
    const bool ln = left.isNullAt(li);
    const bool rn = right.isNullAt(ri);
    if (ln || rn) {
      return ln == rn ? 0 : ln ? -1 : 1;
    }
    return left.valueAt<StringView>(li).compare(right.valueAt<StringView>(ri));
  }

  auto sortedIndices(
      const MapVector* map,
      vector_size_t row,
      const DecodedVector&) const {
    return map->sortedKeyIndices(row);
  }
};

template <typename TKeyCompare, typename TValueEqual>
FLATTEN inline bool mapEqual(
    const MapVector* left,
    vector_size_t leftRow,
    const MapVector* right,
    vector_size_t rightRow,
    const DecodedVector& leftKeys,
    const DecodedVector& rightKeys,
    TKeyCompare keyCompare,
    TValueEqual valueEqual) {
  const auto size = left->sizeAt(leftRow);
  if (size != right->sizeAt(rightRow)) {
    return false;
  }
  if (size == 0) {
    return true;
  }
  if (size == 1) {
    const auto leftEntry = left->offsetAt(leftRow);
    const auto rightEntry = right->offsetAt(rightRow);
    return keyCompare(leftKeys, leftEntry, rightKeys, rightEntry) == 0 &&
        valueEqual(leftEntry, rightEntry);
  }

  const auto leftIndices = keyCompare.sortedIndices(left, leftRow, leftKeys);
  const auto rightIndices =
      keyCompare.sortedIndices(right, rightRow, rightKeys);
  size_t leftPos = 0;
  size_t rightPos = 0;
  // Flink keeps the last value for duplicate keys and visits only left keys.
  while (leftPos < leftIndices.size()) {
    const auto leftKey = leftIndices[leftPos];
    auto leftValue = leftKey;
    do {
      leftValue = std::max(leftValue, leftIndices[leftPos++]);
    } while (leftPos < leftIndices.size() &&
             keyCompare(leftKeys, leftKey, leftKeys, leftIndices[leftPos]) ==
                 0);

    int32_t keyComparison = -1;
    while (rightPos < rightIndices.size()) {
      keyComparison =
          keyCompare(leftKeys, leftKey, rightKeys, rightIndices[rightPos]);
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
             keyCompare(
                 rightKeys, rightKey, rightKeys, rightIndices[rightPos]) == 0);
    if (!valueEqual(leftValue, rightValue)) {
      return false;
    }
  }
  return true;
}

bool equalValues(
    const DecodedValue& left,
    vector_size_t leftIndex,
    const DecodedValue& right,
    vector_size_t rightIndex) {
  if (left.scalar.value) {
    return left.scalar.value(
        *left.decoded, leftIndex, *right.decoded, rightIndex);
  }
  if (left.decoded->isNullAt(leftIndex) ||
      right.decoded->isNullAt(rightIndex)) {
    return false;
  }
  switch (left.decoded->base()->typeKind()) {
    case TypeKind::ROW: {
      const auto leftRow = left.decoded->index(leftIndex);
      const auto rightRow = right.decoded->index(rightIndex);
      for (size_t i = 0; i < left.children.size(); ++i) {
        const auto& leftChild = *left.children[i];
        const auto& rightChild = *right.children[i];
        const bool leftNull = leftChild.decoded->isNullAt(leftRow);
        const bool rightNull = rightChild.decoded->isNullAt(rightRow);
        if (leftNull || rightNull) {
          if (leftNull != rightNull) {
            return false;
          }
        } else if (!equalValues(leftChild, leftRow, rightChild, rightRow)) {
          return false;
        }
      }
      return true;
    }
    case TypeKind::ARRAY: {
      const auto* leftArray = left.decoded->base()->asUnchecked<ArrayVector>();
      const auto* rightArray =
          right.decoded->base()->asUnchecked<ArrayVector>();
      const auto leftRow = left.decoded->index(leftIndex);
      const auto rightRow = right.decoded->index(rightIndex);
      if (leftArray->sizeAt(leftRow) != rightArray->sizeAt(rightRow)) {
        return false;
      }
      for (vector_size_t i = 0; i < leftArray->sizeAt(leftRow); ++i) {
        if (!equalValues(
                *left.children[0],
                leftArray->offsetAt(leftRow) + i,
                *right.children[0],
                rightArray->offsetAt(rightRow) + i)) {
          return false;
        }
      }
      return true;
    }
    case TypeKind::MAP: {
      const auto* leftMap = left.decoded->base()->asUnchecked<MapVector>();
      const auto* rightMap = right.decoded->base()->asUnchecked<MapVector>();
      const auto leftRow = left.decoded->index(leftIndex);
      const auto rightRow = right.decoded->index(rightIndex);
      if (leftMap->sizeAt(leftRow) != rightMap->sizeAt(rightRow)) {
        return false;
      }
      if (leftMap->sizeAt(leftRow) == 0) {
        return true;
      }
      return mapEqual(
          left.decoded->base()->asUnchecked<MapVector>(),
          left.decoded->index(leftIndex),
          right.decoded->base()->asUnchecked<MapVector>(),
          right.decoded->index(rightIndex),
          *left.children[0]->decoded,
          *right.children[0]->decoded,
          GenericKeyCompare{left.children[0]->scalar.key},
          [&](auto leftEntry, auto rightEntry) {
            return equalValues(
                *left.children[1], leftEntry, *right.children[1], rightEntry);
          });
    }
    default:
      return left.decoded->base()->equalValueAt(
          right.decoded->base(),
          left.decoded->index(leftIndex),
          right.decoded->index(rightIndex));
  }
}

template <bool negate, bool stringInteger>
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
        output->set(row, negate);
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
    const auto elementRows = [&](const MapVector* map, DecodedVector& parent) {
      const auto decodedWithoutRows = [](const VectorPtr& vector) {
        return vector->type()->isPrimitiveType() &&
            (vector->isFlatEncoding() || vector->isConstantEncoding());
      };
      if (decodedWithoutRows(map->mapKeys()) &&
          decodedWithoutRows(map->mapValues())) {
        return SelectivityVector(0);
      }
      return toElementRows(
          map->mapKeys()->size(),
          compareRows,
          map,
          parent.isIdentityMapping() ? nullptr : parent.indices());
    };
    const auto leftElements = elementRows(leftMap, *left);
    const auto rightElements = elementRows(rightMap, *right);
    const DecodedValue leftKeys(leftMap->mapKeys(), leftElements, context);
    const DecodedValue rightKeys(rightMap->mapKeys(), rightElements, context);
    const DecodedValue leftValues(leftMap->mapValues(), leftElements, context);
    const DecodedValue rightValues(
        rightMap->mapValues(), rightElements, context);
    const auto compareRowsWith = [&](auto keyCompare, auto valueEqual) {
      compareRows.applyToSelected([&](vector_size_t row) {
        // Empty maps have no decoded child rows.
        output->set(
            row,
            negate !=
                (leftMap->sizeAt(left->index(row)) == 0 ||
                 mapEqual(
                     leftMap,
                     left->index(row),
                     rightMap,
                     right->index(row),
                     *leftKeys.decoded,
                     *rightKeys.decoded,
                     keyCompare,
                     valueEqual)));
      });
    };
    if constexpr (stringInteger) {
      compareRowsWith(StringKeyCompare{}, [&](auto li, auto ri) {
        return !leftValues.decoded->isNullAt(li) &&
            !rightValues.decoded->isNullAt(ri) &&
            leftValues.decoded->valueAt<int32_t>(li) ==
            rightValues.decoded->valueAt<int32_t>(ri);
      });
    } else {
      compareRowsWith(
          GenericKeyCompare{leftKeys.scalar.key}, [&](auto li, auto ri) {
            return equalValues(leftValues, li, rightValues, ri);
          });
    }
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> mapEqualSignatures() {
  return {exec::FunctionSignatureBuilder()
              .typeVariable("K")
              .typeVariable("V")
              .returnType("boolean")
              .argumentType("map(K,V)")
              .argumentType("map(K,V)")
              .build()};
}

template <bool negate>
std::shared_ptr<exec::VectorFunction> makeMapEqual(
    const std::string&,
    const std::vector<exec::VectorFunctionArg>& inputs,
    const core::QueryConfig&) {
  BOLT_USER_CHECK(
      detail::supportsMapEquality(*inputs[0].type),
      "Unsupported Flink MAP equality type: {}",
      inputs[0].type->toString());
  const auto& type = inputs[0].type;
  if (type->childAt(0)->isVarchar() && type->childAt(1)->isInteger()) {
    return std::make_shared<FlinkMapEqual<negate, true>>();
  }
  return std::make_shared<FlinkMapEqual<negate, false>>();
}

} // namespace

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    flink_map_equal,
    mapEqualSignatures(),
    makeMapEqual<false>);
BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    flink_map_not_equal,
    mapEqualSignatures(),
    makeMapEqual<true>);

} // namespace bytedance::bolt::functions::flinksql
