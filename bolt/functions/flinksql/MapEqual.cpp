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

#include <boost/sort/pdqsort/pdqsort.hpp>
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

template <typename F>
decltype(auto) dispatchScalar(TypeKind kind, F&& function) {
  auto dispatch = [&]<TypeKind Kind>() -> decltype(auto) {
    using T = std::conditional_t<
        Kind == TypeKind::UNKNOWN,
        void,
        typename TypeTraits<Kind>::NativeType>;
    return function.template operator()<T>();
  };
  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      dispatch.template operator(),
      kind < TypeKind::ARRAY ? kind : TypeKind::UNKNOWN);
}

ScalarComparison scalarComparison(TypeKind kind) {
  return dispatchScalar(kind, []<typename T>() {
    if constexpr (std::is_void_v<T>) {
      return ScalarComparison{};
    } else {
      return scalarComparison<T>();
    }
  });
}

bool canDecodeWithoutRows(const VectorPtr& vector) {
  return vector->type()->isPrimitiveType() &&
      (vector->isFlatEncoding() ||
       (vector->isConstantEncoding() && !isLazyNotLoaded(*vector)));
}

// Cache child decoding once per batch, including selected nested elements.
struct DecodedValue {
  DecodedValue(
      const VectorPtr& vector,
      const SelectivityVector& rows,
      exec::EvalCtx& context)
      : decoded(context), scalar(scalarComparison(vector->typeKind())) {
    if (canDecodeWithoutRows(vector)) {
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
    if (!baseRows.hasSelections()) {
      return;
    }
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
  // Each nesting level retains its scratch indices for this batch.
  mutable std::vector<vector_size_t> indices;
};

template <typename TKeyCompare>
FOLLY_NOINLINE const std::vector<vector_size_t>& sortedKeys(
    const MapVector* map,
    vector_size_t row,
    const DecodedValue& keys,
    TKeyCompare compare) {
  auto& indices = keys.indices;
  indices.resize(map->sizeAt(row));
  std::iota(indices.begin(), indices.end(), map->offsetAt(row));
  if (map->mapKeys()->typeKind() != TypeKind::REAL &&
      map->mapKeys()->typeKind() != TypeKind::DOUBLE) {
    if (!map->hasSortedKeys()) {
      map->mapKeys()->sortIndices(indices, CompareFlags());
    }
  } else {
    boost::sort::pdqsort(
        indices.begin(), indices.end(), [&](auto left, auto right) {
          return compare(*keys.decoded, left, *keys.decoded, right) < 0;
        });
  }
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
};

template <typename T>
struct ScalarKeyCompare {
  FOLLY_ALWAYS_INLINE int32_t operator()(
      const DecodedVector& left,
      vector_size_t li,
      const DecodedVector& right,
      vector_size_t ri) const {
    return compareScalarKeys<T>(left, li, right, ri);
  }
};

template <bool checkSizes = true, typename TKeyCompare, typename TValueEqual>
FLATTEN inline bool mapEqual(
    const MapVector* left,
    vector_size_t leftRow,
    const MapVector* right,
    vector_size_t rightRow,
    const DecodedValue& leftKeyValues,
    const DecodedValue& rightKeyValues,
    TKeyCompare keyCompare,
    TValueEqual valueEqual) {
  const auto size = left->sizeAt(leftRow);
  if constexpr (checkSizes) {
    if (size != right->sizeAt(rightRow)) {
      return false;
    }
    if (size == 0) {
      return true;
    }
  }
  const auto& leftKeys = *leftKeyValues.decoded;
  const auto& rightKeys = *rightKeyValues.decoded;
  if (size == 1) {
    const auto leftEntry = left->offsetAt(leftRow);
    const auto rightEntry = right->offsetAt(rightRow);
    return keyCompare(leftKeys, leftEntry, rightKeys, rightEntry) == 0 &&
        valueEqual(leftEntry, rightEntry);
  }

  const auto& leftIndices =
      sortedKeys(left, leftRow, leftKeyValues, keyCompare);
  const auto& rightIndices =
      sortedKeys(right, rightRow, rightKeyValues, keyCompare);
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

FOLLY_NOINLINE bool equalValues(
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
      return mapEqual(
          leftMap,
          leftRow,
          rightMap,
          rightRow,
          *left.children[0],
          *right.children[0],
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

template <typename K, typename V>
void compareRows(
    const SelectivityVector& rows,
    const MapVector* leftMap,
    const MapVector* rightMap,
    const DecodedVector& left,
    const DecodedVector& right,
    const DecodedValue& leftKeys,
    const DecodedValue& rightKeys,
    const DecodedValue& leftValues,
    const DecodedValue& rightValues,
    bool negate,
    FlatVector<bool>* output) {
  rows.applyToSelected([&](vector_size_t row) {
    output->set(
        row,
        negate !=
            (leftMap->sizeAt(left.index(row)) == 0 ||
             mapEqual<false>(
                 leftMap,
                 left.index(row),
                 rightMap,
                 right.index(row),
                 leftKeys,
                 rightKeys,
                 ScalarKeyCompare<K>{},
                 [&](auto li, auto ri) {
                   if constexpr (std::is_void_v<V>) {
                     return equalValues(leftValues, li, rightValues, ri);
                   } else {
                     return equalScalarValues<V>(
                         *leftValues.decoded, li, *rightValues.decoded, ri);
                   }
                 })));
  });
}

using CompareRows = decltype(&compareRows<int32_t, int32_t>);

class FlinkMapEqual final : public exec::VectorFunction {
 public:
  explicit FlinkMapEqual(CompareRows compare, bool negate)
      : compare_(compare), negate_(negate) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const bool negate = negate_;
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
      if (canDecodeWithoutRows(map->mapKeys()) &&
          canDecodeWithoutRows(map->mapValues())) {
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
    compare_(
        compareRows,
        leftMap,
        rightMap,
        *left,
        *right,
        leftKeys,
        rightKeys,
        leftValues,
        rightValues,
        negate,
        output);
  }

 private:
  const CompareRows compare_;
  const bool negate_;
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
  const auto compare = dispatchScalar(
      inputs[0].type->childAt(0)->kind(), [&]<typename K>() -> CompareRows {
        if constexpr (std::is_void_v<K>) {
          BOLT_UNREACHABLE();
        } else {
          return dispatchScalar(
              inputs[0].type->childAt(1)->kind(),
              []<typename V>() -> CompareRows { return compareRows<K, V>; });
        }
      });
  return std::make_shared<FlinkMapEqual>(compare, negate);
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
