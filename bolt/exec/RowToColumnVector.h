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

#pragma once

#include <folly/container/F14Set.h>
#include <cstdint>
#include <vector>

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/type/StringView.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/TypeAliases.h"
#include "bolt/vector/VectorTypeUtils.h"
namespace bytedance::bolt::exec {

namespace {

FOLLY_ALWAYS_INLINE void extractString(
    StringView value,
    FlatVector<StringView>* values,
    vector_size_t index) {
  if (value.isInline()) {
    values->set(index, value);
    return;
  }
  auto rawBuffer = values->getRawStringBufferWithSpace(value.size());
  std::memcpy(rawBuffer, value.data(), value.size());
  values->setNoCopy(index, StringView(rawBuffer, value.size()));
}

template <bool useRowNumbers>
FOLLY_ALWAYS_INLINE void extractComplexType(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  auto nullByte = column.nullByte();
  auto nullMask = column.nullMask();
  auto offset = column.offset();

  BOLT_DCHECK_LE(numRows + resultOffset, result->size());
  for (int i = 0; i < numRows; ++i) {
    const char* row;
    if constexpr (useRowNumbers) {
      auto rowNumber = rowNumbers[i];
      row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
    } else {
      row = rows[i];
    }
    auto resultIndex = resultOffset + i;
    if (!row || RowContainer::isNullAt(row, nullByte, nullMask)) {
      result->setNull(resultIndex, true);
    } else {
      auto sv = RowContainer::valueAt<std::string_view>(row, offset);
      ByteRange range{
          reinterpret_cast<uint8_t*>(const_cast<char*>(sv.data())),
          static_cast<int32_t>(sv.size()),
          0};
      ByteInputStream stream(std::vector<ByteRange>{range});
      ContainerRowSerde::deserialize(stream, resultIndex, result.get());
    }
  }
}

template <bool useRowNumbers, typename T>
FOLLY_ALWAYS_INLINE void extractValuesWithNulls(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    int32_t resultOffset,
    FlatVector<T>* FOLLY_NONNULL result) {
  auto maxRows = numRows + resultOffset;
  BOLT_DCHECK_LE(maxRows, result->size());

  BufferPtr& nullBuffer = result->mutableNulls(maxRows);
  auto nulls = nullBuffer->asMutable<uint64_t>();
  BufferPtr valuesBuffer = result->mutableValues(maxRows);
  auto values = valuesBuffer->asMutableRange<T>();
  for (int32_t i = 0; i < numRows; ++i) {
    const char* row;
    if constexpr (useRowNumbers) {
      auto rowNumber = rowNumbers[i];
      row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
    } else {
      row = rows[i];
    }
    auto resultIndex = resultOffset + i;
    if (row == nullptr || RowContainer::isNullAt(row, nullByte, nullMask)) {
      bits::setNull(nulls, resultIndex, true);
    } else {
      bits::setNull(nulls, resultIndex, false);
      if constexpr (std::is_same_v<T, StringView>) {
        extractString(
            RowContainer::valueAt<StringView>(row, offset),
            result,
            resultIndex);
      } else {
        values[resultIndex] = RowContainer::valueAt<T>(row, offset);
      }
    }
  }
}

template <bool useRowNumbers, typename T>
FOLLY_ALWAYS_INLINE void extractValuesNoNulls(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    int32_t offset,
    int32_t resultOffset,
    FlatVector<T>* FOLLY_NONNULL result) {
  auto maxRows = numRows + resultOffset;
  BOLT_DCHECK_LE(maxRows, result->size());
  BufferPtr valuesBuffer = result->mutableValues(maxRows);
  auto values = valuesBuffer->asMutableRange<T>();

  for (int32_t i = 0; i < numRows; ++i) {
    const char* row;
    if constexpr (useRowNumbers) {
      auto rowNumber = rowNumbers[i];
      row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
    } else {
      row = rows[i];
    }
    auto resultIndex = resultOffset + i;
    if (row == nullptr) {
      result->setNull(resultIndex, true);
    } else {
      result->setNull(resultIndex, false);
      if constexpr (std::is_same_v<T, StringView>) {
        extractString(
            RowContainer::valueAt<StringView>(row, offset),
            result,
            resultIndex);
      } else {
        values[resultIndex] = RowContainer::valueAt<T>(row, offset);
      }
    }
  }
}

template <bool useRowNumbers, TypeKind Kind>
FOLLY_ALWAYS_INLINE void extractColumnTypedInternal(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  // Resize the result vector before all copies.
  result->resize(numRows + resultOffset);

  using T = typename KindToFlatVector<Kind>::HashRowType;
  auto* flatResult = result->as<FlatVector<T>>();
  auto nullMask = column.nullMask();
  auto offset = column.offset();

  if (!nullMask) {
    extractValuesNoNulls<useRowNumbers, T>(
        rows, rowNumbers, numRows, offset, resultOffset, flatResult);
  } else {
    extractValuesWithNulls<useRowNumbers, T>(
        rows,
        rowNumbers,
        numRows,
        offset,
        column.nullByte(),
        nullMask,
        resultOffset,
        flatResult);
  }
}

template <>
FOLLY_ALWAYS_INLINE void extractColumnTypedInternal<false, TypeKind::ROW>(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  result->resize(numRows + resultOffset);
  extractComplexType<false>(
      rows, rowNumbers, numRows, column, resultOffset, result);
}

template <>
FOLLY_ALWAYS_INLINE void extractColumnTypedInternal<true, TypeKind::ROW>(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  result->resize(numRows + resultOffset);
  extractComplexType<true>(
      rows, rowNumbers, numRows, column, resultOffset, result);
}

template <>
FOLLY_ALWAYS_INLINE void extractColumnTypedInternal<false, TypeKind::MAP>(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  result->resize(numRows + resultOffset);
  extractComplexType<false>(
      rows, rowNumbers, numRows, column, resultOffset, result);
}

template <>
FOLLY_ALWAYS_INLINE void extractColumnTypedInternal<true, TypeKind::MAP>(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  result->resize(numRows + resultOffset);
  extractComplexType<true>(
      rows, rowNumbers, numRows, column, resultOffset, result);
}

template <>
FOLLY_ALWAYS_INLINE void extractColumnTypedInternal<false, TypeKind::ARRAY>(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  result->resize(numRows + resultOffset);
  extractComplexType<false>(
      rows, rowNumbers, numRows, column, resultOffset, result);
}

template <>
FOLLY_ALWAYS_INLINE void extractColumnTypedInternal<true, TypeKind::ARRAY>(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  result->resize(numRows + resultOffset);
  extractComplexType<true>(
      rows, rowNumbers, numRows, column, resultOffset, result);
}

template <TypeKind Kind>
FOLLY_ALWAYS_INLINE void extractColumnTyped(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  BOLT_CHECK(result != nullptr, "In extractColumnTyped, result is null");
  if (rowNumbers.size() > 0) {
    extractColumnTypedInternal<true, Kind>(
        rows, rowNumbers, rowNumbers.size(), column, resultOffset, result);
  } else {
    extractColumnTypedInternal<false, Kind>(
        rows, rowNumbers, numRows, column, resultOffset, result);
  }
}

template <>
FOLLY_ALWAYS_INLINE void extractColumnTyped<TypeKind::OPAQUE>(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  BOLT_NYI("Unsupported type TypeKind::OPAQUE for extractColumnTyped");
}

} // namespace

FOLLY_ALWAYS_INLINE void rowToColumnVector(
    char** rows,
    folly::Range<const vector_size_t*> rowNumbers,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  // If the caller pre-allocated a LazyComplexVector, redirect writes into its
  // inner FlatVector<StringView>. The RowContainer stored lazy columns as
  // VARBINARY StringView bytes, so writing into the inner bytes vector yields
  // a correctly-populated LazyComplexVector for the caller.
  bool isLazyComplex =
      result->encoding() == VectorEncoding::Simple::LAZY_COMPLEX;
  const auto& inner = isLazyComplex
      ? result->asUnchecked<LazyComplexVector>()->encoded()
      : result;
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      rowNumbers,
      rowNumbers.size(),
      column,
      resultOffset,
      inner);
}

FOLLY_ALWAYS_INLINE void rowToColumnVector(
    char** rows,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result) {
  // If the caller pre-allocated a LazyComplexVector, redirect writes into its
  // inner FlatVector<StringView>. The RowContainer stored lazy columns as
  // VARBINARY StringView bytes, so writing into the inner bytes vector yields
  // a correctly-populated LazyComplexVector for the caller.
  bool isLazyComplex =
      result->encoding() == VectorEncoding::Simple::LAZY_COMPLEX;
  const auto& inner = isLazyComplex
      ? result->asUnchecked<LazyComplexVector>()->encoded()
      : result;
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      folly::Range<const vector_size_t*>(),
      numRows,
      column,
      resultOffset,
      inner);
}

FOLLY_ALWAYS_INLINE void rowToColumnVector(
    char** rows,
    int32_t numRows,
    const std::vector<RowColumn>& columns,
    int32_t resultOffset,
    const RowVectorPtr& result,
    const std::vector<IdentityProjection>& columnMap) {
  if (!columnMap.empty()) {
    for (const auto& columnProjection : columnMap) {
      rowToColumnVector(
          rows,
          numRows,
          columns[columnProjection.inputChannel],
          resultOffset,
          result->childAt(columnProjection.outputChannel));
    }
  } else {
    for (vector_size_t i = 0; i < columns.size(); i++) {
      rowToColumnVector(
          rows, numRows, columns[i], resultOffset, result->childAt(i));
    }
  }
}

} // namespace bytedance::bolt::exec
