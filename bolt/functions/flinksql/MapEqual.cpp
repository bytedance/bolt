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
 *
 * Flink complex equality and inequality functions.
 */

#include <boost/sort/pdqsort/pdqsort.hpp>
#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <numeric>
#include <unordered_map>

#include "bolt/core/QueryConfig.h"
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

// Generic comparisons use Java's elementwise ARRAY and fieldwise ROW rules.
// Binary comparisons consume canonical BinaryArrayData/BinaryRowData values.
// MAP still matches keys; ROWs read from a BinaryMapData are NestedRowData and
// compare fieldwise, while their ARRAY fields compare as binary sections.
// Callers must establish the Java representation before logical transport.
// For mixed subtrees, preserve original bytes as VARBINARY at each proven
// binary comparison boundary, or evaluate in Java. Original noncanonical
// sections also require their bytes; logical vectors cannot reconstruct them.
enum class ComparisonMode { kGeneric, kBinary };

template <bool sqlKeys, typename T>
int32_t compareFloatingKeys(T left, T right) {
  if (left == right) {
    if constexpr (sqlKeys) {
      return 0;
    } else {
      // Java boxed keys distinguish signed zeros, unlike numeric values.
      return std::signbit(right) - std::signbit(left);
    }
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

template <bool sqlKeys, typename T>
FLATTEN FOLLY_ALWAYS_INLINE int32_t compareScalarKeys(
    const DecodedVector& left,
    vector_size_t li,
    const DecodedVector& right,
    vector_size_t ri) {
  const bool ln = left.isNullAt(li);
  const bool rn = right.isNullAt(ri);
  if (ln || rn) {
    return ln == rn ? 0 : ln ? -1 : 1;
  }
  if constexpr (std::is_same_v<T, StringView>) {
    return left.data<StringView>()[left.index(li)].compare(
        right.data<StringView>()[right.index(ri)]);
  } else {
    const auto l = left.valueAt<T>(li);
    const auto r = right.valueAt<T>(ri);
    if constexpr (std::is_floating_point_v<T>) {
      return compareFloatingKeys<sqlKeys>(l, r);
    } else {
      return l == r ? 0 : l < r ? -1 : 1;
    }
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

template <bool sqlKeys, typename T>
ScalarComparison scalarComparison() {
  return {
      compareScalarKeys<(sqlKeys && std::is_floating_point_v<T>), T>,
      equalScalarValues<T>};
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

template <bool sqlKeys>
ScalarComparison scalarComparison(TypeKind kind) {
  return dispatchScalar(kind, []<typename T>() {
    if constexpr (std::is_void_v<T>) {
      return ScalarComparison{};
    } else {
      return scalarComparison<sqlKeys, T>();
    }
  });
}

bool canDecodeWithoutRows(const VectorPtr& vector) {
  return vector->type()->isPrimitiveType() &&
      (vector->isFlatEncoding() ||
       (vector->isConstantEncoding() && !isLazyNotLoaded(*vector)));
}

struct DecodedValue;

uint32_t typeDepth(const Type& type) {
  uint32_t depth = 0;
  for (uint32_t i = 0; i < type.size(); ++i) {
    depth = std::max(depth, 1 + typeDepth(*type.childAt(i)));
  }
  return depth;
}

struct LazyInput {
  VectorPtr vector;
  SelectivityVector rows;
  DecodedValue* target;
};

struct LazyLoad {
  LazyLoad(const LazyVector* lazy, bool allRows)
      : rows(lazy->size(), allRows), depth(typeDepth(*lazy->type())) {}

  SelectivityVector rows;
  const uint32_t depth;
  std::vector<LazyInput> inputs;
};

using LazyLoads = std::unordered_map<const LazyVector*, LazyLoad>;

// Cache child decoding once per batch, including selected nested elements.
struct DecodedValue {
  template <bool sqlKeys>
  DecodedValue(
      const VectorPtr& vector,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      std::unique_ptr<LazyLoads>& lazyLoads,
      std::bool_constant<sqlKeys> policy)
      : decoded(context),
        scalar(scalarComparison<sqlKeys>(vector->typeKind())) {
    if (canDecodeWithoutRows(vector)) {
      decoded.get()->decode(*vector);
    } else if (rows.hasSelections()) {
      initialize(vector, rows, context, lazyLoads, policy);
    }
  }

  template <bool sqlKeys>
  void initialize(
      const VectorPtr& vector,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      std::unique_ptr<LazyLoads>& lazyLoads,
      std::bool_constant<sqlKeys> policy) {
    if (vector->isFlatEncoding() || vector->isConstantEncoding()) {
      decoded.get()->decode(*vector, false);
    } else {
      decoded.get()->decode(*vector, rows, false);
    }
    const auto* base = decoded->base();
    if (base->isLazy()) {
      if (rows.testSelected(
              [&](vector_size_t row) { return decoded->isNullAt(row); })) {
        return;
      }
      // Merge aliases before the one-shot load. Later conditional branches
      // may access any row of this child vector.
      if (!lazyLoads) {
        lazyLoads = std::make_unique<LazyLoads>();
      }
      auto& load = lazyLoads
                       ->try_emplace(
                           base->asUnchecked<LazyVector>(),
                           base->asUnchecked<LazyVector>(),
                           !context.isFinalSelection())
                       .first->second;
      rows.applyToSelected([&](vector_size_t row) {
        if (!decoded->isNullAt(row)) {
          load.rows.setValid(decoded->index(row), true);
        }
      });
      load.inputs.push_back({vector, rows, this});
      return;
    }
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
          children.emplace_back(std::make_unique<DecodedValue>(
              child, baseRows, context, lazyLoads, policy));
        }
        break;
      case TypeKind::ARRAY: {
        const auto* array = base->asUnchecked<ArrayVector>();
        const auto elements =
            toElementRows(array->elements()->size(), baseRows, array);
        children.emplace_back(std::make_unique<DecodedValue>(
            array->elements(), elements, context, lazyLoads, policy));
        break;
      }
      case TypeKind::MAP: {
        const auto* map = base->asUnchecked<MapVector>();
        const auto elements =
            toElementRows(map->mapKeys()->size(), baseRows, map);
        children.emplace_back(std::make_unique<DecodedValue>(
            map->mapKeys(), elements, context, lazyLoads, policy));
        children.emplace_back(std::make_unique<DecodedValue>(
            map->mapValues(), elements, context, lazyLoads, policy));
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

template <bool sqlKeys>
FOLLY_NOINLINE void loadLazyValues(
    std::unique_ptr<LazyLoads> loads,
    exec::EvalCtx& context,
    std::bool_constant<sqlKeys> policy) {
  while (!loads->empty()) {
    uint32_t depth = 0;
    for (const auto& [lazy, load] : *loads) {
      depth = std::max(depth, load.depth);
    }
    // Containers can reveal aliases of their children, so finish decoding
    // every parent type before loading a shallower type.
    LazyLoads current;
    std::array<size_t, static_cast<size_t>(TypeKind::ROW) + 1> peers{};
    for (auto it = loads->begin(); it != loads->end();) {
      if (it->second.depth == depth) {
        ++peers[static_cast<size_t>(it->first->typeKind())];
        current.insert(loads->extract(it++));
      } else {
        ++it;
      }
    }
    for (auto& [lazy, load] : current) {
      // Same-kind peer loaders may return wrappers over each other, whose
      // mappings are unavailable until after the one-shot load.
      if (peers[static_cast<size_t>(lazy->typeKind())] > 1) {
        load.rows.setAll();
      }
      load.rows.updateBounds();
      std::vector<vector_size_t> selected;
      selected.reserve(load.rows.countSelected());
      load.rows.applyToSelected(
          [&](vector_size_t row) { selected.push_back(row); });
      lazy->load(RowSet(selected), nullptr);
    }
    for (const auto& [lazy, load] : current) {
      for (const auto& input : load.inputs) {
        input.vector->loadedVector();
        input.target->initialize(
            input.vector, input.rows, context, loads, policy);
      }
    }
  }
}

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

  bool isNaN(const DecodedVector& keys, vector_size_t index) const {
    if (keys.isNullAt(index)) {
      return false;
    }
    switch (keys.base()->typeKind()) {
      case TypeKind::REAL:
        return std::isnan(keys.valueAt<float>(index));
      case TypeKind::DOUBLE:
        return std::isnan(keys.valueAt<double>(index));
      default:
        return false;
    }
  }
};

template <bool sqlKeys, typename T>
struct ScalarKeyCompare {
  FOLLY_ALWAYS_INLINE int32_t operator()(
      const DecodedVector& left,
      vector_size_t li,
      const DecodedVector& right,
      vector_size_t ri) const {
    return compareScalarKeys<sqlKeys, T>(left, li, right, ri);
  }

  bool isNaN(const DecodedVector& keys, vector_size_t index) const {
    if constexpr (std::is_floating_point_v<T>) {
      return !keys.isNullAt(index) && std::isnan(keys.valueAt<T>(index));
    } else {
      return false;
    }
  }
};

template <
    bool sqlKeys,
    bool checkSizes = true,
    typename TKeyCompare,
    typename TValueEqual>
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
    if constexpr (sqlKeys) {
      if (keyCompare.isNaN(leftKeys, leftEntry)) {
        return false;
      }
    }
    return keyCompare(leftKeys, leftEntry, rightKeys, rightEntry) == 0 &&
        valueEqual(leftEntry, rightEntry);
  }

  const auto& leftIndices =
      sortedKeys(left, leftRow, leftKeyValues, keyCompare);
  const auto& rightIndices =
      sortedKeys(right, rightRow, rightKeyValues, keyCompare);
  if constexpr (sqlKeys) {
    // NaNs sort last, but never match a key under SQL equality.
    if (keyCompare.isNaN(leftKeys, leftIndices.back())) {
      return false;
    }
  }
  size_t leftPos = 0;
  size_t rightPos = 0;
  // The last physical entry supplies the value for a duplicate key.
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
      if (sqlKeys || keyComparison <= 0) {
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
  return !sqlKeys || rightPos == rightIndices.size();
}

template <typename T>
bool equalCanonicalFloating(T left, T right) {
  using Bits = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;
  // Canonical encodings use one NaN representation and retain signed zero.
  return (std::isnan(left) && std::isnan(right)) ||
      std::bit_cast<Bits>(left) == std::bit_cast<Bits>(right);
}

// Equality of a deterministic Flink binary encoding can be evaluated without
// allocating that encoding. This requires canonical NaNs, zeroed null slots
// and padding, and INSERT RowKind throughout the subtree.
FOLLY_NOINLINE bool equalCanonicalValues(
    const DecodedValue& left,
    vector_size_t leftIndex,
    const DecodedValue& right,
    vector_size_t rightIndex) {
  const bool leftNull = left.decoded->isNullAt(leftIndex);
  const bool rightNull = right.decoded->isNullAt(rightIndex);
  if (leftNull || rightNull) {
    return leftNull == rightNull;
  }
  const auto kind = left.decoded->base()->typeKind();
  if (kind == TypeKind::REAL) {
    return equalCanonicalFloating(
        left.decoded->valueAt<float>(leftIndex),
        right.decoded->valueAt<float>(rightIndex));
  }
  if (kind == TypeKind::DOUBLE) {
    return equalCanonicalFloating(
        left.decoded->valueAt<double>(leftIndex),
        right.decoded->valueAt<double>(rightIndex));
  }
  if (left.scalar.value) {
    return left.scalar.value(
        *left.decoded, leftIndex, *right.decoded, rightIndex);
  }
  const auto leftRow = left.decoded->index(leftIndex);
  const auto rightRow = right.decoded->index(rightIndex);
  if (kind == TypeKind::ROW) {
    for (size_t i = 0; i < left.children.size(); ++i) {
      if (!equalCanonicalValues(
              *left.children[i], leftRow, *right.children[i], rightRow)) {
        return false;
      }
    }
    return true;
  }
  const auto* leftArray = left.decoded->base()->asUnchecked<ArrayVectorBase>();
  const auto* rightArray =
      right.decoded->base()->asUnchecked<ArrayVectorBase>();
  const auto size = leftArray->sizeAt(leftRow);
  if (size != rightArray->sizeAt(rightRow)) {
    return false;
  }
  // MAP entries inside a binary section retain physical order and duplicates.
  // Both the key and value arrays participate in the byte comparison.
  for (size_t child = 0; child < left.children.size(); ++child) {
    for (vector_size_t i = 0; i < size; ++i) {
      if (!equalCanonicalValues(
              *left.children[child],
              leftArray->offsetAt(leftRow) + i,
              *right.children[child],
              rightArray->offsetAt(rightRow) + i)) {
        return false;
      }
    }
  }
  return true;
}

template <bool sqlKeys, ComparisonMode mode>
FLATTEN FOLLY_NOINLINE bool equalValues(
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
      // Logical vectors omit RowKind. Callers establish matching kinds at
      // every fieldwise ROW boundary before logical vector transport.
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
        } else if (!equalValues<sqlKeys, mode>(
                       leftChild, leftRow, rightChild, rightRow)) {
          return false;
        }
      }
      return true;
    }
    case TypeKind::ARRAY: {
      if constexpr (mode == ComparisonMode::kBinary) {
        return equalCanonicalValues(left, leftIndex, right, rightIndex);
      }
      const auto* leftArray = left.decoded->base()->asUnchecked<ArrayVector>();
      const auto* rightArray =
          right.decoded->base()->asUnchecked<ArrayVector>();
      const auto leftRow = left.decoded->index(leftIndex);
      const auto rightRow = right.decoded->index(rightIndex);
      if (leftArray->sizeAt(leftRow) != rightArray->sizeAt(rightRow)) {
        return false;
      }
      for (vector_size_t i = 0; i < leftArray->sizeAt(leftRow); ++i) {
        if (!equalValues<sqlKeys, mode>(
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
      return mapEqual<sqlKeys>(
          leftMap,
          leftRow,
          rightMap,
          rightRow,
          *left.children[0],
          *right.children[0],
          GenericKeyCompare{left.children[0]->scalar.key},
          [&](auto leftEntry, auto rightEntry) {
            return equalValues<sqlKeys, mode>(
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

template <
    bool sqlKeys,
    typename K,
    typename V,
    ComparisonMode mode = ComparisonMode::kGeneric>
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
            mapEqual<sqlKeys, false>(
                leftMap,
                left.index(row),
                rightMap,
                right.index(row),
                leftKeys,
                rightKeys,
                ScalarKeyCompare<(sqlKeys && std::is_floating_point_v<K>), K>{},
                [&](auto li, auto ri) {
                  if constexpr (std::is_void_v<V>) {
                    return equalValues<sqlKeys, mode>(
                        leftValues, li, rightValues, ri);
                  } else {
                    return equalScalarValues<V>(
                        *leftValues.decoded, li, *rightValues.decoded, ri);
                  }
                }));
  });
}

using CompareRows = decltype(&compareRows<false, int32_t, int32_t>);

// Resolve unequal sizes and empty containers before decoding their children.
const SelectivityVector& filterContainerRows(
    const SelectivityVector& rows,
    const DecodedVector& left,
    const DecodedVector& right,
    bool negate,
    FlatVector<bool>* output,
    exec::LocalSelectivityVector& remainingRows) {
  const auto* leftContainer = left.base()->asUnchecked<ArrayVectorBase>();
  const auto* rightContainer = right.base()->asUnchecked<ArrayVectorBase>();
  SelectivityVector* selected = nullptr;
  rows.applyToSelected([&](vector_size_t row) {
    const auto leftSize = leftContainer->sizeAt(left.index(row));
    const auto rightSize = rightContainer->sizeAt(right.index(row));
    if (leftSize != rightSize || leftSize == 0) {
      output->set(row, negate != (leftSize == rightSize));
      if (!selected) {
        selected = remainingRows.get(rows);
      }
      selected->setValid(row, false);
    }
  });
  if (selected) {
    selected->updateBounds();
  }
  return selected ? *selected : rows;
}

template <bool sqlKeys>
class FlinkMapEqual final : public exec::VectorFunction {
 public:
  FlinkMapEqual(CompareRows compare, bool negate)
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
    const auto& compareRows =
        filterContainerRows(rows, *left, *right, negate, output, equalSizeRows);
    if (!compareRows.hasSelections()) {
      return;
    }
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
    constexpr std::bool_constant<sqlKeys> policy;
    std::unique_ptr<LazyLoads> lazyLoads;
    DecodedValue leftKeys(
        leftMap->mapKeys(), leftElements, context, lazyLoads, policy);
    DecodedValue rightKeys(
        rightMap->mapKeys(), rightElements, context, lazyLoads, policy);
    DecodedValue leftValues(
        leftMap->mapValues(), leftElements, context, lazyLoads, policy);
    DecodedValue rightValues(
        rightMap->mapValues(), rightElements, context, lazyLoads, policy);
    if (lazyLoads) {
      loadLazyValues(std::move(lazyLoads), context, policy);
    }
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

template <bool sqlKeys, ComparisonMode mode>
class FlinkComplexEqual final : public exec::VectorFunction {
 public:
  explicit FlinkComplexEqual(bool negate) : negate_(negate) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, outputType, result);
    auto* output = result->asFlatVector<bool>();
    exec::LocalSelectivityVector equalSizeRows(context);
    const SelectivityVector* compareRows = &rows;
    if (args[0]->typeKind() == TypeKind::ARRAY) {
      exec::DecodedArgs decoded(rows, args, context);
      compareRows = &filterContainerRows(
          rows, *decoded.at(0), *decoded.at(1), negate_, output, equalSizeRows);
      if (!compareRows->hasSelections()) {
        return;
      }
    }
    constexpr std::bool_constant<sqlKeys> policy;
    std::unique_ptr<LazyLoads> lazyLoads;
    DecodedValue left(args[0], *compareRows, context, lazyLoads, policy);
    DecodedValue right(args[1], *compareRows, context, lazyLoads, policy);
    if (lazyLoads) {
      loadLazyValues(std::move(lazyLoads), context, policy);
    }
    compareRows->applyToSelected([&](vector_size_t row) {
      const bool equal = mode == ComparisonMode::kBinary &&
              args[0]->typeKind() == TypeKind::ROW
          ? equalCanonicalValues(left, row, right, row)
          : equalValues<sqlKeys, mode>(left, row, right, row);
      output->set(row, negate_ != equal);
    });
  }

 private:
  const bool negate_;
};

template <bool sqlKeys, ComparisonMode mode>
std::shared_ptr<exec::VectorFunction> makeTypedMapEqual(
    const TypePtr& type,
    bool negate) {
  const auto compare = dispatchScalar(
      type->childAt(0)->kind(), [&]<typename K>() -> CompareRows {
        if constexpr (std::is_void_v<K>) {
          BOLT_UNREACHABLE();
        } else {
          return dispatchScalar(
              type->childAt(1)->kind(), []<typename V>() -> CompareRows {
                if constexpr (std::is_void_v<V>) {
                  return compareRows<sqlKeys, K, V, mode>;
                } else {
                  return compareRows<sqlKeys, K, V>;
                }
              });
        }
      });
  return std::make_shared<FlinkMapEqual<sqlKeys>>(compare, negate);
}

template <bool negate, ComparisonMode mode>
std::shared_ptr<exec::VectorFunction> makeComplexEqual(
    const std::string&,
    const std::vector<exec::VectorFunctionArg>& inputs,
    const core::QueryConfig& config) {
  const auto& type = *inputs[0].type;
  BOLT_USER_CHECK(
      (type.isMap() || type.isArray() || type.isRow() || type.isVarbinary()) &&
          detail::supportsMapValueEquality(type),
      "Unsupported Flink complex equality type: {}",
      type.toString());
  const auto make =
      [&]<bool sqlKeys>() -> std::shared_ptr<exec::VectorFunction> {
    if (inputs[0].type->isMap()) {
      return makeTypedMapEqual<sqlKeys, mode>(inputs[0].type, negate);
    }
    return std::make_shared<FlinkComplexEqual<sqlKeys, mode>>(negate);
  };
  const auto keyMode = config.flinkMapEqualityMode();
  if (keyMode == "sql_keys") {
    return make.template operator()<true>();
  }
  BOLT_USER_CHECK_EQ(
      keyMode, "java_map", "Unsupported Flink MAP equality mode: {}", keyMode);
  return make.template operator()<false>();
}

} // namespace

void registerComparisonFunctions(const std::string& prefix) {
  const std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("boolean")
          .argumentType("T")
          .argumentType("T")
          .build()};
  for (const bool negate : {false, true}) {
    const auto name =
        prefix + (negate ? "flink_complex_not_equal" : "flink_complex_equal");
    const auto generic = negate
        ? makeComplexEqual<true, ComparisonMode::kGeneric>
        : makeComplexEqual<false, ComparisonMode::kGeneric>;
    exec::registerStatefulVectorFunction(name, signatures, generic);
    exec::registerStatefulVectorFunction(
        name + "_generic", signatures, generic);
    exec::registerStatefulVectorFunction(
        name + "_binary",
        signatures,
        negate ? makeComplexEqual<true, ComparisonMode::kBinary>
               : makeComplexEqual<false, ComparisonMode::kBinary>);
  }
}

} // namespace bytedance::bolt::functions::flinksql
