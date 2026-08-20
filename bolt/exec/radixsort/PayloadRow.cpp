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

#include "bolt/exec/radixsort/PayloadRow.h"

#include <folly/small_vector.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/process/ProcessBase.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"
#include "bolt/vector/VariantVector.h"

namespace bytedance::bolt::exec::radixsort {

namespace {
struct VariableColumnWriter {
  const PayloadRowColumnLayout* column;
  const BaseVector* vector;
  const DecodedVector* decoded;
  const uint64_t* complexSizes;
};
} // namespace

struct PayloadRowWriter::Impl {
  BufferPtr complexSizes;
  std::vector<std::optional<DecodedVector>> decoded;
  folly::small_vector<VariableColumnWriter, 32> variableColumns;
};

namespace {

bool isComplex(const Type& type) {
  return type.kind() == TypeKind::ARRAY || type.kind() == TypeKind::MAP ||
      type.kind() == TypeKind::ROW;
}

bool isSupportedType(const Type& type, bool nested) {
  if (type.isDecimal()) {
    return true;
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
    case TypeKind::UNKNOWN:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return true;
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      for (uint32_t child = 0; child < type.size(); ++child) {
        if (!isSupportedType(*type.childAt(child), true)) {
          return false;
        }
      }
      return true;
    case TypeKind::VARIANT:
      return nested;
    default:
      return false;
  }
}

std::optional<uint32_t> slotWidth(const Type& type) {
  if (type.isShortDecimal()) {
    return sizeof(int64_t);
  }
  if (type.isLongDecimal()) {
    return sizeof(int128_t);
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::HUGEINT:
      return sizeof(int128_t);
    case TypeKind::TIMESTAMP:
      return sizeof(Timestamp);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(StringView);
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      return sizeof(PayloadVarlenRef);
    case TypeKind::UNKNOWN:
      return 0;
    default:
      return std::nullopt;
  }
}

} // namespace

bool PayloadRowLayout::supports(const Type& type) {
  return isSupportedType(type, false);
}

std::shared_ptr<const PayloadRowLayout> PayloadRowLayout::create(
    const RowTypePtr& rowType) {
  if (rowType->size() == 0) {
    return nullptr;
  }

  const auto nullBytes = static_cast<uint32_t>((rowType->size() + 7) / 8);
  bool hasVariableFields = false;
  bool supportedTypes = true;
  std::string unsupportedType;
  for (uint32_t column = 0; column < rowType->size(); ++column) {
    const auto& type = rowType->childAt(column);
    if (!supports(*type)) {
      supportedTypes = false;
      unsupportedType = type->toString();
    }
    hasVariableFields |= type->kind() == TypeKind::VARCHAR ||
        type->kind() == TypeKind::VARBINARY || isComplex(*type);
  }
  BOLT_CHECK(
      supportedTypes,
      "Payload row layout is not implemented for ",
      unsupportedType);

  std::optional<uint64_t> variableSizeOffset;
  uint64_t rowWidth = nullBytes;
  if (hasVariableFields) {
    variableSizeOffset = rowWidth;
    rowWidth += sizeof(uint64_t);
  }

  std::vector<PayloadRowColumnLayout> columns;
  columns.reserve(rowType->size());
  bool supportedSlots = true;
  std::string unsupportedSlotType;
  bool rowWidthValid = true;
  for (uint32_t column = 0; column < rowType->size(); ++column) {
    const auto& type = rowType->childAt(column);
    const auto width = slotWidth(*type);
    if (!width.has_value()) {
      supportedSlots = false;
      unsupportedSlotType = type->toString();
      continue;
    }
    columns.push_back(PayloadRowColumnLayout{
        type,
        rowWidth,
        *width,
        column / 8,
        static_cast<uint8_t>(uint8_t{1} << (column % 8)),
        type->kind() == TypeKind::VARCHAR ||
            type->kind() == TypeKind::VARBINARY || isComplex(*type),
        isComplex(*type)});
    auto next = checkedAdd<uint64_t>(rowWidth, *width);
    rowWidthValid &= next.has_value();
    if (next.has_value()) {
      rowWidth = *next;
    }
  }
  BOLT_CHECK(
      supportedSlots,
      "Payload row slot is not implemented for ",
      unsupportedSlotType);
  BOLT_CHECK(rowWidthValid, "Payload row row width overflows");

  return std::shared_ptr<const PayloadRowLayout>(new PayloadRowLayout(
      rowType, std::move(columns), nullBytes, variableSizeOffset, rowWidth));
}

namespace {

void prepareResult(
    const PayloadRowLayout& layout,
    vector_size_t size,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  if (result != nullptr && result->pool() == pool &&
      result->type()->equivalent(*layout.rowType())) {
    VectorPtr reusable(std::move(result));
    BaseVector::prepareForReuse(reusable, size);
    result = std::static_pointer_cast<RowVector>(std::move(reusable));
    for (auto& child : result->children()) {
      child->resize(size);
    }
    return;
  }

  std::vector<VectorPtr> children;
  children.reserve(layout.columns().size());
  for (const auto& column : layout.columns()) {
    children.push_back(BaseVector::create(column.type, size, pool));
  }
  result = std::make_shared<RowVector>(
      pool, layout.rowType(), nullptr, size, std::move(children));
}

bool isNull(const char* row, const PayloadRowColumnLayout& column) {
  return (static_cast<uint8_t>(row[column.nullByte]) & column.nullMask) == 0;
}

template <typename T, bool MayHaveNulls>
void gatherFixedColumn(
    const PayloadRowColumnLayout& column,
    std::span<char* const> rows,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<T>>();
  auto* values = flat->mutableRawValues();
  auto* nulls = MayHaveNulls || flat->rawNulls() != nullptr
      ? flat->mutableRawNulls()
      : nullptr;
  if (nulls != nullptr) {
    bits::clearAllNull(nulls, rows.size());
  }
  if constexpr (MayHaveNulls) {
    constexpr vector_size_t kRowsPerWord = 64;
    const auto size = static_cast<vector_size_t>(rows.size());
    for (vector_size_t begin = 0; begin < size; begin += kRowsPerWord) {
      const auto end = std::min(size, begin + kRowsPerWord);
      uint64_t validityWord = 0;
      for (vector_size_t row = begin; row < end; ++row) {
        const bool null = isNull(rows[row], column);
        validityWord |= static_cast<uint64_t>(!null) << (row - begin);
        values[row] = loadUnaligned<T>(rows[row] + column.offset);
      }
      nulls[begin / kRowsPerWord] = validityWord;
    }
    return;
  }
  if constexpr (std::is_same_v<T, int64_t>) {
    constexpr vector_size_t kBatchSize = xsimd::batch<int64_t>::size;
    if (rows.size() >= kBatchSize && process::hasAvx2()) {
      const auto baseAddress =
          reinterpret_cast<intptr_t>(rows.front() + column.offset);
      const auto* base =
          reinterpret_cast<const int64_t*>(rows.front() + column.offset);
      vector_size_t row = 0;
      constexpr vector_size_t kStep = 4 * kBatchSize;
      for (; row + kStep <= rows.size(); row += kStep) {
        int64_t indices[kStep];
        for (vector_size_t lane = 0; lane < kStep; ++lane) {
          const auto address =
              reinterpret_cast<intptr_t>(rows[row + lane] + column.offset);
          indices[lane] = address - baseAddress;
        }
        simd::gather<int64_t, int64_t, 1>(base, indices)
            .store_unaligned(reinterpret_cast<int64_t*>(values + row));
        simd::gather<int64_t, int64_t, 1>(base, indices + kBatchSize)
            .store_unaligned(
                reinterpret_cast<int64_t*>(values + row + kBatchSize));
        simd::gather<int64_t, int64_t, 1>(base, indices + 2 * kBatchSize)
            .store_unaligned(
                reinterpret_cast<int64_t*>(values + row + 2 * kBatchSize));
        simd::gather<int64_t, int64_t, 1>(base, indices + 3 * kBatchSize)
            .store_unaligned(
                reinterpret_cast<int64_t*>(values + row + 3 * kBatchSize));
      }
      for (; row + kBatchSize <= rows.size(); row += kBatchSize) {
        int64_t indices[kBatchSize];
        for (vector_size_t lane = 0; lane < kBatchSize; ++lane) {
          const auto address =
              reinterpret_cast<intptr_t>(rows[row + lane] + column.offset);
          indices[lane] = address - baseAddress;
        }
        simd::gather<int64_t, int64_t, 1>(base, indices)
            .store_unaligned(reinterpret_cast<int64_t*>(values + row));
      }
      for (; row < rows.size(); ++row) {
        values[row] = loadUnaligned<T>(rows[row] + column.offset);
      }
      return;
    }
  }
  for (vector_size_t row = 0; row < rows.size(); ++row) {
    values[row] = loadUnaligned<T>(rows[row] + column.offset);
  }
}

struct Fixed64GatherState {
  const PayloadRowColumnLayout* column;
  void* values;
  uint64_t* nulls;
  bool isDouble;
  bool mayHaveNulls;
};

bool isFixed64Column(const PayloadRowColumnLayout& column) {
  return column.type->isShortDecimal() ||
      column.type->kind() == TypeKind::BIGINT ||
      column.type->kind() == TypeKind::DOUBLE;
}

void gatherFixed64Columns(
    std::span<char* const> rows,
    folly::small_vector<Fixed64GatherState, 32>& states) {
  constexpr vector_size_t kBatchSize = xsimd::batch<int64_t>::size;
  constexpr vector_size_t kStep = 4 * kBatchSize;
  const auto rowBaseAddress = reinterpret_cast<intptr_t>(rows.front());
  vector_size_t row = 0;
  for (; row + kStep <= rows.size(); row += kStep) {
    int64_t indices[kStep];
    for (vector_size_t lane = 0; lane < kStep; ++lane) {
      indices[lane] =
          reinterpret_cast<intptr_t>(rows[row + lane]) - rowBaseAddress;
    }
    for (const auto& state : states) {
      if (state.isDouble) {
        const auto* base = reinterpret_cast<const double*>(
            rows.front() + state.column->offset);
        auto* values = static_cast<double*>(state.values);
        simd::gather<double, int64_t, 1>(base, indices)
            .store_unaligned(values + row);
        simd::gather<double, int64_t, 1>(base, indices + kBatchSize)
            .store_unaligned(values + row + kBatchSize);
        simd::gather<double, int64_t, 1>(base, indices + 2 * kBatchSize)
            .store_unaligned(values + row + 2 * kBatchSize);
        simd::gather<double, int64_t, 1>(base, indices + 3 * kBatchSize)
            .store_unaligned(values + row + 3 * kBatchSize);
      } else {
        const auto* base = reinterpret_cast<const int64_t*>(
            rows.front() + state.column->offset);
        auto* values = static_cast<int64_t*>(state.values);
        simd::gather<int64_t, int64_t, 1>(base, indices)
            .store_unaligned(values + row);
        simd::gather<int64_t, int64_t, 1>(base, indices + kBatchSize)
            .store_unaligned(values + row + kBatchSize);
        simd::gather<int64_t, int64_t, 1>(base, indices + 2 * kBatchSize)
            .store_unaligned(values + row + 2 * kBatchSize);
        simd::gather<int64_t, int64_t, 1>(base, indices + 3 * kBatchSize)
            .store_unaligned(values + row + 3 * kBatchSize);
      }
    }
  }
  for (const auto& state : states) {
    if (state.isDouble) {
      auto* values = static_cast<double*>(state.values);
      for (auto tail = row; tail < rows.size(); ++tail) {
        values[tail] = loadUnaligned<double>(rows[tail] + state.column->offset);
      }
    } else {
      auto* values = static_cast<int64_t*>(state.values);
      for (auto tail = row; tail < rows.size(); ++tail) {
        values[tail] =
            loadUnaligned<int64_t>(rows[tail] + state.column->offset);
      }
    }
    if (!state.mayHaveNulls) {
      continue;
    }
    constexpr vector_size_t kRowsPerWord = 64;
    for (vector_size_t begin = 0; begin < rows.size(); begin += kRowsPerWord) {
      const auto end =
          std::min<vector_size_t>(rows.size(), begin + kRowsPerWord);
      uint64_t validityWord = 0;
      for (vector_size_t nullRow = begin; nullRow < end; ++nullRow) {
        validityWord |=
            static_cast<uint64_t>(!isNull(rows[nullRow], *state.column))
            << (nullRow - begin);
      }
      state.nulls[begin / kRowsPerWord] = validityWord;
    }
  }
}

template <bool MayHaveNulls>
void gatherFixedBooleanColumn(
    const PayloadRowColumnLayout& column,
    std::span<char* const> rows,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  auto* values = flat->template mutableRawValues<uint64_t>();
  auto* nulls = MayHaveNulls || flat->rawNulls() != nullptr
      ? flat->mutableRawNulls()
      : nullptr;
  if (nulls != nullptr) {
    bits::clearAllNull(nulls, rows.size());
  }
  for (vector_size_t row = 0; row < rows.size(); ++row) {
    if constexpr (MayHaveNulls) {
      bits::setNull(nulls, row, isNull(rows[row], column));
    }
    const auto value = loadUnaligned<uint8_t>(rows[row] + column.offset);
    bits::setBit(values, row, value != 0);
  }
}

template <bool MayHaveNulls>
void gatherFixedTimestampColumn(
    const PayloadRowColumnLayout& column,
    std::span<char* const> rows,
    const VectorPtr& result) {
  auto* flat = result->asUnchecked<FlatVector<Timestamp>>();
  auto* values = flat->mutableRawValues();
  auto* nulls = MayHaveNulls || flat->rawNulls() != nullptr
      ? flat->mutableRawNulls()
      : nullptr;
  if (nulls != nullptr) {
    bits::clearAllNull(nulls, rows.size());
  }
  for (vector_size_t row = 0; row < rows.size(); ++row) {
    if constexpr (MayHaveNulls) {
      bits::setNull(nulls, row, isNull(rows[row], column));
    }
    const auto seconds = loadUnaligned<int64_t>(rows[row] + column.offset);
    const auto nanos =
        loadUnaligned<uint64_t>(rows[row] + column.offset + sizeof(int64_t));
    values[row] = Timestamp(seconds, nanos);
  }
}

template <bool MayHaveNulls>
void gatherFixedColumn(
    const PayloadRowColumnLayout& column,
    std::span<char* const> rows,
    const VectorPtr& result) {
  if (column.type->isShortDecimal()) {
    gatherFixedColumn<int64_t, MayHaveNulls>(column, rows, result);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    gatherFixedColumn<int128_t, MayHaveNulls>(column, rows, result);
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      gatherFixedBooleanColumn<MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::TINYINT:
      gatherFixedColumn<int8_t, MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::SMALLINT:
      gatherFixedColumn<int16_t, MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::INTEGER:
      gatherFixedColumn<int32_t, MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::BIGINT:
      gatherFixedColumn<int64_t, MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::REAL:
      gatherFixedColumn<float, MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::DOUBLE:
      gatherFixedColumn<double, MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::TIMESTAMP:
      gatherFixedTimestampColumn<MayHaveNulls>(column, rows, result);
      return;
    case TypeKind::UNKNOWN:
      for (vector_size_t row = 0; row < rows.size(); ++row) {
        result->setNull(row, true);
      }
      return;
    default:
      BOLT_FAIL(
          "Sort fixed payload gather is not implemented for {}",
          column.type->toString());
  }
}

struct StringGatherState {
  const PayloadRowColumnLayout* column;
  FlatVector<StringView>* result;
  StringView* values;
  uint64_t* nulls;
  uint64_t stringBytes{0};
  char* output{nullptr};
  char* outputEnd{nullptr};
};

template <bool MayHaveNulls>
void gatherStringColumns(
    std::span<char* const> rows,
    std::vector<StringGatherState>& states) {
  for (auto& state : states) {
    state.values = state.result->mutableRawValues();
    state.nulls = MayHaveNulls || state.result->rawNulls() != nullptr
        ? state.result->mutableRawNulls()
        : nullptr;
    if (state.nulls != nullptr) {
      bits::clearAllNull(state.nulls, rows.size());
    }
  }

  constexpr vector_size_t kRowsPerTile = 32;
  for (vector_size_t begin = 0; begin < rows.size(); begin += kRowsPerTile) {
    const auto end = std::min<vector_size_t>(rows.size(), begin + kRowsPerTile);
    for (auto& state : states) {
      const auto& column = *state.column;
      for (vector_size_t row = begin; row < end; ++row) {
        if constexpr (MayHaveNulls) {
          if (isNull(rows[row], column)) {
            bits::setNull(state.nulls, row, true);
            continue;
          }
        }
        const auto value = loadUnaligned<StringView>(rows[row] + column.offset);
        state.values[row] = value;
        if (!value.isInline()) {
          state.stringBytes += value.size();
        }
      }
    }
  }

  for (auto& state : states) {
    state.output = state.stringBytes == 0
        ? nullptr
        : state.result->getRawStringBufferWithSpace(state.stringBytes, true);
    state.outputEnd =
        state.output == nullptr ? nullptr : state.output + state.stringBytes;
  }

  for (vector_size_t begin = 0; begin < rows.size(); begin += kRowsPerTile) {
    const auto end = std::min<vector_size_t>(rows.size(), begin + kRowsPerTile);
    for (auto& state : states) {
      for (vector_size_t row = begin; row < end; ++row) {
        if constexpr (MayHaveNulls) {
          if (bits::isBitNull(state.nulls, row)) {
            continue;
          }
        }
        const auto value = state.values[row];
        if (value.isInline()) {
          continue;
        }
        std::memcpy(state.output, value.data(), value.size());
        state.values[row] =
            StringView(state.output, static_cast<int32_t>(value.size()));
        state.output += value.size();
      }
    }
  }
}

void gatherStringColumn(
    const PayloadRowColumnLayout& column,
    std::span<char* const> rows,
    FlatVector<StringView>* flat) {
  uint64_t stringBytes = 0;
  bool hasNulls = false;
  for (const auto* row : rows) {
    if (isNull(row, column)) {
      hasNulls = true;
      continue;
    }
    const auto value = loadUnaligned<StringView>(row + column.offset);
    if (!value.isInline()) {
      stringBytes += value.size();
    }
  }

  auto* values = flat->mutableRawValues();
  auto* nulls = hasNulls || flat->rawNulls() != nullptr
      ? flat->mutableRawNulls()
      : nullptr;
  if (nulls != nullptr) {
    bits::clearAllNull(nulls, rows.size());
  }
  char* output = stringBytes == 0
      ? nullptr
      : flat->getRawStringBufferWithSpace(stringBytes, true);
  const auto* outputEnd = output == nullptr ? nullptr : output + stringBytes;

  for (vector_size_t row = 0; row < rows.size(); ++row) {
    if (hasNulls && isNull(rows[row], column)) {
      bits::setNull(nulls, row, true);
      continue;
    }
    const auto value = loadUnaligned<StringView>(rows[row] + column.offset);
    if (value.isInline()) {
      values[row] = value;
      continue;
    }
    std::memcpy(output, value.data(), value.size());
    values[row] = StringView(output, static_cast<int32_t>(value.size()));
    output += value.size();
  }
}

void gatherComplexColumn(
    const PayloadRowColumnLayout& column,
    std::span<char* const> rows,
    const VectorPtr& result) {
  BOLT_DCHECK(column.complex);
  for (vector_size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
    const auto* row = rows[rowIndex];
    if (isNull(row, column)) {
      result->setNull(rowIndex, true);
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(row + column.offset);
    if (value.size == 0) {
      result->setNull(rowIndex, false);
      continue;
    }
    std::vector<ByteRange> ranges{ByteRange{
        reinterpret_cast<uint8_t*>(value.data),
        static_cast<int32_t>(value.size),
        0}};
    ByteInputStream input(std::move(ranges));
    exec::ContainerRowSerde::deserialize(input, rowIndex, result.get(), true);
  }
}

void gatherImpl(
    const PayloadRowLayout& layout,
    std::span<char* const> rows,
    std::span<const uint8_t> mayHaveNulls,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  BOLT_CHECK_NOT_NULL(pool, "Payload row output memory pool must not be null");
  const auto size = static_cast<vector_size_t>(rows.size());
  prepareResult(layout, size, pool, result);

  if (!layout.hasVariableFields()) {
    folly::small_vector<Fixed64GatherState, 32> fixed64Columns;
    if (process::hasAvx2() && rows.size() >= xsimd::batch<int64_t>::size) {
      for (uint32_t columnIndex = 0; columnIndex < layout.columns().size();
           ++columnIndex) {
        const auto& column = layout.columns()[columnIndex];
        if (!isFixed64Column(column)) {
          continue;
        }
        auto* child = result->childAt(columnIndex).get();
        uint64_t* nulls = nullptr;
        if (mayHaveNulls[columnIndex] || child->rawNulls() != nullptr) {
          nulls = child->mutableRawNulls();
          bits::clearAllNull(nulls, rows.size());
        }
        if (column.type->kind() == TypeKind::DOUBLE) {
          fixed64Columns.push_back(Fixed64GatherState{
              &column,
              child->asUnchecked<FlatVector<double>>()->mutableRawValues(),
              nulls,
              true,
              mayHaveNulls[columnIndex] != 0});
        } else {
          fixed64Columns.push_back(Fixed64GatherState{
              &column,
              child->asUnchecked<FlatVector<int64_t>>()->mutableRawValues(),
              nulls,
              false,
              mayHaveNulls[columnIndex] != 0});
        }
      }
    }
    if (fixed64Columns.size() > 1) {
      gatherFixed64Columns(rows, fixed64Columns);
    } else {
      fixed64Columns.clear();
    }
    for (uint32_t columnIndex = 0; columnIndex < layout.columns().size();
         ++columnIndex) {
      if (!fixed64Columns.empty() &&
          isFixed64Column(layout.columns()[columnIndex])) {
        continue;
      }
      if (mayHaveNulls[columnIndex]) {
        gatherFixedColumn<true>(
            layout.columns()[columnIndex], rows, result->childAt(columnIndex));
      } else {
        gatherFixedColumn<false>(
            layout.columns()[columnIndex], rows, result->childAt(columnIndex));
      }
    }
    return;
  }

  uint32_t nonNullStringColumnCount = 0;
  uint32_t nullableStringColumnCount = 0;
  for (uint32_t columnIndex = 0; columnIndex < layout.columns().size();
       ++columnIndex) {
    const auto& column = layout.columns()[columnIndex];
    if (column.variable && !column.complex) {
      if (mayHaveNulls[columnIndex]) {
        ++nullableStringColumnCount;
      } else {
        ++nonNullStringColumnCount;
      }
    }
  }
  const auto stringColumnCount =
      nonNullStringColumnCount + nullableStringColumnCount;
  std::vector<StringGatherState> nonNullStringColumns;
  std::vector<StringGatherState> nullableStringColumns;
  if (stringColumnCount > 1) {
    nonNullStringColumns.reserve(nonNullStringColumnCount);
    nullableStringColumns.reserve(nullableStringColumnCount);
  }
  for (uint32_t columnIndex = 0; columnIndex < layout.columns().size();
       ++columnIndex) {
    const auto& column = layout.columns()[columnIndex];
    auto& child = result->childAt(columnIndex);
    if (!column.complex) {
      if (column.variable) {
        if (stringColumnCount == 1) {
          gatherStringColumn(
              column, rows, child->asUnchecked<FlatVector<StringView>>());
          continue;
        }
        auto& states = mayHaveNulls[columnIndex] ? nullableStringColumns
                                                 : nonNullStringColumns;
        states.push_back(StringGatherState{
            &column,
            child->asUnchecked<FlatVector<StringView>>(),
            nullptr,
            nullptr});
        continue;
      }
      if (mayHaveNulls[columnIndex]) {
        gatherFixedColumn<true>(column, rows, child);
      } else {
        gatherFixedColumn<false>(column, rows, child);
      }
      continue;
    }

    gatherComplexColumn(column, rows, child);
  }
  if (stringColumnCount > 1) {
    gatherStringColumns<false>(rows, nonNullStringColumns);
    gatherStringColumns<true>(rows, nullableStringColumns);
  }
}

} // namespace

void PayloadRowReader::gather(
    const PayloadRowLayout& layout,
    std::span<char* const> rows,
    memory::MemoryPool* pool,
    RowVectorPtr& result,
    std::span<const uint8_t> mayHaveNulls) {
  if (!mayHaveNulls.empty()) {
    gatherImpl(layout, rows, mayHaveNulls, pool, result);
    return;
  }
  std::vector<uint8_t> inferredMayHaveNulls(layout.columns().size(), 0);
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    uint8_t hasNull = 0;
    for (const auto* row : rows) {
      hasNull |= static_cast<uint8_t>(isNull(row, layout.columns()[column]));
    }
    inferredMayHaveNulls[column] = hasNull;
  }
  gatherImpl(layout, rows, inferredMayHaveNulls, pool, result);
}

namespace {

class BoundedPayloadStreamArena final : public StreamArena {
 public:
  BoundedPayloadStreamArena() : StreamArena(nullptr) {}

  void newRange(int32_t, ByteRange*, ByteRange*) override {
    BOLT_FAIL("Sort payload contiguous output is too small");
  }
};

namespace payload_size {

class RowSizes {
 public:
  const BufferPtr& heapSizes() const {
    return heapSizes_;
  }

  const uint64_t* complexSizes(uint32_t ordinal) const {
    return complexSizes_ + static_cast<uint64_t>(ordinal) * size_;
  }

  vector_size_t size_{0};
  BufferPtr heapSizes_;
  const uint64_t* complexSizes_{nullptr};
};

FOLLY_ALWAYS_INLINE void addSize(uint64_t& size, uint64_t increment) {
  size += increment;
}

int32_t checkedComplexSerializedSize(uint64_t size) {
  BOLT_CHECK_LE(
      size,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "A single complex sort payload serialization exceeds ByteRange "
      "capacity: {} bytes",
      size);
  return static_cast<int32_t>(size);
}

uint64_t nullBitmapBytes(vector_size_t size) {
  return bits::nwords(static_cast<uint64_t>(size)) * sizeof(uint64_t);
}

std::optional<uint64_t> fixedSerializedSize(const Type& type) {
  if (type.kind() == TypeKind::UNKNOWN) {
    return sizeof(UnknownValue);
  }
  return type.isFixedWidth() ? std::optional<uint64_t>{type.cppSizeInBytes()}
                             : std::nullopt;
}

struct RangeSizeMetadata {
  std::optional<uint64_t> fixedElementSize;
  bool mayHaveNulls{true};
};

RangeSizeMetadata rangeSizeMetadata(const BaseVector& elements) {
  return RangeSizeMetadata{
      fixedSerializedSize(*elements.type()), elements.mayHaveNulls()};
}

uint64_t serializedSizeForPayload(const BaseVector& vector, vector_size_t row);

uint64_t serializedRangeSizeForPayload(
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    const RangeSizeMetadata& metadata) {
  uint64_t size = sizeof(int32_t) + nullBitmapBytes(count);
  if (metadata.fixedElementSize.has_value()) {
    if (!metadata.mayHaveNulls) {
      const auto valuesSize =
          static_cast<uint64_t>(count) * *metadata.fixedElementSize;
      addSize(size, valuesSize);
      return size;
    }
    for (vector_size_t element = 0; element < count; ++element) {
      if (!elements.isNullAt(offset + element)) {
        addSize(size, *metadata.fixedElementSize);
      }
    }
    return size;
  }
  for (vector_size_t element = 0; element < count; ++element) {
    const auto index = offset + element;
    if (!elements.isNullAt(index)) {
      addSize(size, serializedSizeForPayload(elements, index));
    }
  }
  return size;
}

uint64_t serializedArraySizeForPayload(
    const ArrayVector& array,
    vector_size_t row,
    const RangeSizeMetadata& elementMetadata) {
  return serializedRangeSizeForPayload(
      *array.elements(),
      array.offsetAt(row),
      array.sizeAt(row),
      elementMetadata);
}

uint64_t serializedArraySizeForPayload(
    const ArrayVector& array,
    vector_size_t row) {
  return serializedArraySizeForPayload(
      array, row, rangeSizeMetadata(*array.elements()));
}

uint64_t serializedMapSizeForPayload(
    const MapVector& map,
    vector_size_t row,
    const RangeSizeMetadata& keyMetadata,
    const RangeSizeMetadata& valueMetadata) {
  const auto offset = map.offsetAt(row);
  const auto count = map.sizeAt(row);
  uint64_t size =
      serializedRangeSizeForPayload(*map.mapKeys(), offset, count, keyMetadata);
  addSize(
      size,
      serializedRangeSizeForPayload(
          *map.mapValues(), offset, count, valueMetadata));
  return size;
}

uint64_t serializedMapSizeForPayload(const MapVector& map, vector_size_t row) {
  return serializedMapSizeForPayload(
      map,
      row,
      rangeSizeMetadata(*map.mapKeys()),
      rangeSizeMetadata(*map.mapValues()));
}

uint64_t serializedSizeForPayload(const BaseVector& vector, vector_size_t row) {
  const auto fixedSize = fixedSerializedSize(*vector.type());
  if (fixedSize.has_value()) {
    return *fixedSize;
  }

  switch (vector.typeKind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const auto value =
          vector.asUnchecked<SimpleVector<StringView>>()->valueAt(row);
      return sizeof(int32_t) + value.size();
    }
    case TypeKind::VARIANT: {
      const auto* variant =
          vector.wrappedVector()->asUnchecked<VariantVector>();
      const auto value = variant->valueAt(vector.wrappedIndex(row));
      return 2 * sizeof(int32_t) + value.value.size() + value.metadata.size();
    }
    case TypeKind::ARRAY: {
      const auto* array = vector.wrappedVector()->asUnchecked<ArrayVector>();
      const auto wrappedRow = vector.wrappedIndex(row);
      return serializedArraySizeForPayload(*array, wrappedRow);
    }
    case TypeKind::MAP: {
      const auto* map = vector.wrappedVector()->asUnchecked<MapVector>();
      const auto wrappedRow = vector.wrappedIndex(row);
      return serializedMapSizeForPayload(*map, wrappedRow);
    }
    case TypeKind::ROW: {
      const auto* rowVector = vector.wrappedVector()->asUnchecked<RowVector>();
      const auto wrappedRow = vector.wrappedIndex(row);
      uint64_t size = nullBitmapBytes(rowVector->type()->size());
      for (uint32_t child = 0; child < rowVector->childrenSize(); ++child) {
        const auto& childVector = rowVector->childAt(child);
        if (childVector == nullptr || childVector->isNullAt(wrappedRow)) {
          continue;
        }
        addSize(size, serializedSizeForPayload(*childVector, wrappedRow));
      }
      return size;
    }
    default:
      BOLT_FAIL(
          "Payload serialized size is not implemented for ",
          vector.type()->toString());
  }
}

void measurePayloadRows(
    const RowVector& input,
    const PayloadRowLayout& layout,
    memory::MemoryPool* pool,
    BufferPtr reusableHeapSizes,
    BufferPtr& complexSizeScratch,
    RowSizes& sizes) {
  sizes = RowSizes{};

  const auto rowCount = input.size();

  sizes.size_ = rowCount;
  if (rowCount == 0 || !layout.hasVariableFields()) {
    return;
  }

  const auto complexColumnCount = std::count_if(
      layout.columns().begin(), layout.columns().end(), [](const auto& column) {
        return column.complex;
      });
  auto* rawHeapSizes =
      prepareReusableBuffer<uint64_t>(reusableHeapSizes, rowCount, pool);
  std::fill(rawHeapSizes, rawHeapSizes + rowCount, uint64_t{0});
  sizes.heapSizes_ = std::move(reusableHeapSizes);
  if (complexColumnCount > 0) {
    const auto complexSizeCount = checkedMultiply<size_t>(
        static_cast<size_t>(rowCount), complexColumnCount);
    BOLT_CHECK(
        complexSizeCount.has_value(),
        "Payload row complex size scratch count overflows");
    auto* rawComplexSizes = prepareReusableBuffer<uint64_t>(
        complexSizeScratch, *complexSizeCount, pool);
    std::fill(
        rawComplexSizes, rawComplexSizes + *complexSizeCount, uint64_t{0});
    sizes.complexSizes_ = rawComplexSizes;
  }
  const auto addVariableBytes = [&](vector_size_t row, uint64_t bytes) {
    addSize(rawHeapSizes[row], bytes);
  };

  uint32_t complexOrdinal = 0;
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    const auto& metadata = layout.columns()[column];
    if (!metadata.variable) {
      continue;
    }
    const auto& vector = *input.childAt(column);

    if (metadata.complex) {
      auto* rawComplexSizes =
          const_cast<uint64_t*>(sizes.complexSizes(complexOrdinal++));
      const auto addComplexBytes = [&](vector_size_t row, uint64_t bytes) {
        checkedComplexSerializedSize(bytes);
        rawComplexSizes[row] = bytes;
        addVariableBytes(row, bytes);
      };
      switch (metadata.type->kind()) {
        case TypeKind::ARRAY: {
          const auto* array =
              vector.wrappedVector()->asUnchecked<ArrayVector>();
          const auto elementMetadata = rangeSizeMetadata(*array->elements());
          if (!vector.mayHaveNulls()) {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              addComplexBytes(
                  row,
                  serializedArraySizeForPayload(
                      *array, vector.wrappedIndex(row), elementMetadata));
            }
          } else {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              if (!vector.isNullAt(row)) {
                addComplexBytes(
                    row,
                    serializedArraySizeForPayload(
                        *array, vector.wrappedIndex(row), elementMetadata));
              }
            }
          }
          break;
        }
        case TypeKind::MAP: {
          const auto* map = vector.wrappedVector()->asUnchecked<MapVector>();
          const auto keyMetadata = rangeSizeMetadata(*map->mapKeys());
          const auto valueMetadata = rangeSizeMetadata(*map->mapValues());
          if (!vector.mayHaveNulls()) {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              addComplexBytes(
                  row,
                  serializedMapSizeForPayload(
                      *map,
                      vector.wrappedIndex(row),
                      keyMetadata,
                      valueMetadata));
            }
          } else {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              if (!vector.isNullAt(row)) {
                addComplexBytes(
                    row,
                    serializedMapSizeForPayload(
                        *map,
                        vector.wrappedIndex(row),
                        keyMetadata,
                        valueMetadata));
              }
            }
          }
          break;
        }
        case TypeKind::ROW: {
          const auto* rowVector =
              vector.wrappedVector()->asUnchecked<RowVector>();
          for (vector_size_t row = 0; row < rowCount; ++row) {
            if (vector.isNullAt(row)) {
              continue;
            }
            const auto wrappedRow = vector.wrappedIndex(row);
            uint64_t size = nullBitmapBytes(rowVector->type()->size());
            for (uint32_t child = 0; child < rowVector->childrenSize();
                 ++child) {
              const auto& childVector = rowVector->childAt(child);
              if (childVector == nullptr || childVector->isNullAt(wrappedRow)) {
                continue;
              }
              addSize(size, serializedSizeForPayload(*childVector, wrappedRow));
            }
            addComplexBytes(row, size);
          }
          break;
        }
        default:
          BOLT_FAIL(
              "Payload complex size is not implemented for ",
              metadata.type->toString());
      }
      continue;
    }

    DecodedVector decoded(vector);
    for (vector_size_t row = 0; row < rowCount; ++row) {
      if (decoded.isNullAt(row)) {
        continue;
      }
      const auto value = decoded.valueAt<StringView>(row);
      if (!value.isInline()) {
        addVariableBytes(row, value.size());
      }
    }
  }
}

} // namespace payload_size

void setNull(char* row, const PayloadRowColumnLayout& column) {
  row[column.nullByte] = static_cast<char>(
      static_cast<uint8_t>(row[column.nullByte]) &
      static_cast<uint8_t>(~column.nullMask));
}

bool shouldClearPayloadSlots(const RowVector& input) {
  for (const auto& vector : input.children()) {
    if (vector->encoding() != VectorEncoding::Simple::FLAT ||
        vector->typeKind() == TypeKind::UNKNOWN ||
        vector->rawNulls() != nullptr) {
      return true;
    }
  }
  return false;
}

template <typename T>
void writeFixedFlatColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  const auto* flat = vector.asUnchecked<FlatVector<T>>();
  const auto* values = flat->rawValues();
  const auto* nulls = flat->rawNulls();
  for (vector_size_t row = begin; row < end; ++row) {
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      setNull(rows[row], column);
    } else {
      storeUnaligned<T>(rows[row] + column.offset, values[row]);
    }
  }
}

template <>
void writeFixedFlatColumn<bool>(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  const auto* flat = vector.asUnchecked<FlatVector<bool>>();
  const auto* nulls = flat->rawNulls();
  for (vector_size_t row = begin; row < end; ++row) {
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      setNull(rows[row], column);
    } else {
      storeUnaligned<uint8_t>(
          rows[row] + column.offset,
          static_cast<uint8_t>(flat->valueAtFast(row)));
    }
  }
}

void writeFixedFlatColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  if (column.type->isShortDecimal()) {
    writeFixedFlatColumn<int64_t>(column, vector, rows, begin, end);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    writeFixedFlatColumn<int128_t>(column, vector, rows, begin, end);
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      writeFixedFlatColumn<bool>(column, vector, rows, begin, end);
      return;
    case TypeKind::TINYINT:
      writeFixedFlatColumn<int8_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::SMALLINT:
      writeFixedFlatColumn<int16_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::INTEGER:
      writeFixedFlatColumn<int32_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::BIGINT:
      writeFixedFlatColumn<int64_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::REAL:
      writeFixedFlatColumn<float>(column, vector, rows, begin, end);
      return;
    case TypeKind::DOUBLE:
      writeFixedFlatColumn<double>(column, vector, rows, begin, end);
      return;
    case TypeKind::TIMESTAMP:
      writeFixedFlatColumn<Timestamp>(column, vector, rows, begin, end);
      return;
    case TypeKind::UNKNOWN:
      for (vector_size_t row = begin; row < end; ++row) {
        setNull(rows[row], column);
      }
      return;
    default:
      BOLT_FAIL(
          "Sort fixed payload fast path is not implemented for ",
          column.type->toString());
  }
}

void writeFlatStringValue(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t* heapRemaining) {
  const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
  const auto* values = flat->rawValues();
  const auto* nulls = flat->rawNulls();
  if (nulls != nullptr && bits::isBitNull(nulls, row)) {
    setNull(payloadRow, column);
    return;
  }
  const auto value = values[row];
  auto* slot = payloadRow + column.offset;
  if (value.isInline()) {
    storeUnaligned<StringView>(slot, value);
    return;
  }
  std::memcpy(heapCursor, value.data(), value.size());
  storeUnaligned<StringView>(
      slot, StringView(heapCursor, static_cast<int32_t>(value.size())));
  heapCursor += value.size();
  if (heapRemaining != nullptr) {
    *heapRemaining -= value.size();
  }
}

template <typename T>
void writeFixedDecodedColumn(
    const PayloadRowColumnLayout& column,
    const DecodedVector& decoded,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  if (decoded.isConstantMapping()) {
    if (decoded.isNullAt(0)) {
      for (vector_size_t row = begin; row < end; ++row) {
        setNull(rows[row], column);
      }
      return;
    }
    const auto value = decoded.valueAt<T>(0);
    for (vector_size_t row = begin; row < end; ++row) {
      storeUnaligned<T>(rows[row] + column.offset, value);
    }
    return;
  }
  for (vector_size_t row = begin; row < end; ++row) {
    if (decoded.isNullAt(row)) {
      setNull(rows[row], column);
    } else {
      storeUnaligned<T>(rows[row] + column.offset, decoded.valueAt<T>(row));
    }
  }
}

void writeFixedDecodedColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  DecodedVector decoded(vector);
  if (column.type->isShortDecimal()) {
    writeFixedDecodedColumn<int64_t>(column, decoded, rows, begin, end);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    writeFixedDecodedColumn<int128_t>(column, decoded, rows, begin, end);
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      for (vector_size_t row = begin; row < end; ++row) {
        if (decoded.isNullAt(row)) {
          setNull(rows[row], column);
        } else {
          storeUnaligned<uint8_t>(
              rows[row] + column.offset,
              static_cast<uint8_t>(decoded.valueAt<bool>(row)));
        }
      }
      return;
    case TypeKind::TINYINT:
      writeFixedDecodedColumn<int8_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::SMALLINT:
      writeFixedDecodedColumn<int16_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::INTEGER:
      writeFixedDecodedColumn<int32_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::BIGINT:
      writeFixedDecodedColumn<int64_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::REAL:
      writeFixedDecodedColumn<float>(column, decoded, rows, begin, end);
      return;
    case TypeKind::DOUBLE:
      writeFixedDecodedColumn<double>(column, decoded, rows, begin, end);
      return;
    case TypeKind::TIMESTAMP:
      writeFixedDecodedColumn<Timestamp>(column, decoded, rows, begin, end);
      return;
    case TypeKind::UNKNOWN:
      for (vector_size_t row = begin; row < end; ++row) {
        setNull(rows[row], column);
      }
      return;
    default:
      BOLT_FAIL(
          "Sort fixed payload column is not implemented for ",
          column.type->toString());
  }
}

void writeFixedDecodedColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t size) {
  writeFixedDecodedColumn(column, vector, rows, 0, size);
}

void writeStringDecodedValue(
    const PayloadRowColumnLayout& column,
    const DecodedVector& decoded,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t* heapRemaining) {
  if (decoded.isNullAt(row)) {
    setNull(payloadRow, column);
    return;
  }
  const auto value = decoded.valueAt<StringView>(row);
  auto* slot = payloadRow + column.offset;
  if (value.isInline()) {
    storeUnaligned<StringView>(slot, value);
    return;
  }
  std::memcpy(heapCursor, value.data(), value.size());
  storeUnaligned<StringView>(
      slot, StringView(heapCursor, static_cast<int32_t>(value.size())));
  heapCursor += value.size();
  if (heapRemaining != nullptr) {
    *heapRemaining -= value.size();
  }
}

void writeComplexDecodedValue(
    const PayloadRowColumnLayout& column,
    const DecodedVector& decoded,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t& heapRemaining,
    uint64_t serializedSize,
    ByteOutputStream& stream) {
  if (decoded.isNullAt(row)) {
    setNull(payloadRow, column);
    return;
  }
  if (serializedSize == 0) {
    storeUnaligned<PayloadVarlenRef>(
        payloadRow + column.offset, PayloadVarlenRef{0, nullptr});
    return;
  }
  const auto decodedRow = decoded.index(row);
  stream.setRange(
      ByteRange{
          reinterpret_cast<uint8_t*>(heapCursor),
          static_cast<int32_t>(serializedSize),
          0},
      0);
  exec::ContainerRowSerde::serialize(
      *decoded.base(),
      decodedRow,
      stream,
      exec::ContainerRowSerdeOptions{.isKey = false});
  const auto valueSize = stream.size();
  BOLT_DCHECK_EQ(valueSize, serializedSize);
  storeUnaligned<PayloadVarlenRef>(
      payloadRow + column.offset,
      PayloadVarlenRef{valueSize, valueSize == 0 ? nullptr : heapCursor});
  if (valueSize > 0) {
    heapCursor += valueSize;
  }
  heapRemaining -= valueSize;
}

void writeFixedColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    const DecodedVector* decoded,
    char* const* rows,
    vector_size_t size) {
  if (decoded == nullptr) {
    writeFixedFlatColumn(column, vector, rows, 0, size);
  } else {
    writeFixedDecodedColumn(column, vector, rows, size);
  }
}

void writeVariableValue(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    const DecodedVector* decoded,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t& heapRemaining,
    uint64_t complexSize,
    ByteOutputStream& stream) {
  if (column.complex) {
    writeComplexDecodedValue(
        column,
        *decoded,
        row,
        payloadRow,
        heapCursor,
        heapRemaining,
        complexSize,
        stream);
    return;
  }
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    writeFlatStringValue(
        column, vector, row, payloadRow, heapCursor, &heapRemaining);
  } else {
    writeStringDecodedValue(
        column, *decoded, row, payloadRow, heapCursor, &heapRemaining);
  }
}

void appendRows(
    const RowVector& input,
    RadixSortRunStorage& arena,
    const payload_size::RowSizes* sizes,
    std::vector<std::optional<DecodedVector>>& decoded,
    folly::small_vector<VariableColumnWriter, 32>& variableColumns,
    PayloadRowBatch& batch) {
  const auto& layout = arena.payloadLayout();
  BOLT_CHECK_NOT_NULL(
      layout, "Radix sort run storage does not have a payload layout");
  const auto hasVariableFields = layout->hasVariableFields();
  const uint64_t* rawHeapSizes = nullptr;
  if (hasVariableFields) {
    rawHeapSizes = sizes->heapSizes() == nullptr
        ? nullptr
        : sizes->heapSizes()->as<uint64_t>();
    arena.allocatePayloadRowBatch(
        std::span<const uint64_t>(rawHeapSizes, input.size()),
        sizes->heapSizes(),
        batch);
  } else {
    arena.allocateFixedPayloadRowBatch(input.size(), batch);
  }
  if (input.size() == 0) {
    return;
  }

  auto** rows = batch.rows()->asMutable<char*>();
  const auto clearPayloadSlots = shouldClearPayloadSlots(input);
  for (vector_size_t row = 0; row < input.size(); ++row) {
    auto* fixed = rows[row];
    if (clearPayloadSlots) {
      std::memset(fixed, 0, layout->rowWidth());
    }
    std::memset(fixed, 0xff, layout->nullBytes());
    if (hasVariableFields) {
      storeUnaligned<uint64_t>(
          fixed + *layout->variableSizeOffset(), rawHeapSizes[row]);
    }
  }

  decoded.resize(input.childrenSize());
  for (uint32_t column = 0; column < input.childrenSize(); ++column) {
    if (input.childAt(column)->encoding() != VectorEncoding::Simple::FLAT) {
      if (decoded[column].has_value()) {
        decoded[column]->decode(*input.childAt(column));
      } else {
        decoded[column].emplace(*input.childAt(column));
      }
    } else {
      decoded[column].reset();
    }
  }

  variableColumns.clear();
  uint32_t complexOrdinal = 0;
  for (uint32_t column = 0; column < layout->columns().size(); ++column) {
    const auto& metadata = layout->columns()[column];
    if (!metadata.variable) {
      writeFixedColumn(
          metadata,
          *input.childAt(column),
          decoded[column].has_value() ? &*decoded[column] : nullptr,
          rows,
          input.size());
    } else {
      variableColumns.push_back(VariableColumnWriter{
          &metadata,
          input.childAt(column).get(),
          decoded[column].has_value() ? &*decoded[column] : nullptr,
          metadata.complex ? sizes->complexSizes(complexOrdinal++) : nullptr});
    }
  }

  if (!hasVariableFields) {
    return;
  }

  BoundedPayloadStreamArena streamArena;
  ByteOutputStream stream(&streamArena, false, false);
  for (vector_size_t row = 0; row < input.size(); ++row) {
    auto* payloadRow = rows[row];
    auto* heapCursor = batch.heapAt(row);
    uint64_t heapRemaining = rawHeapSizes[row];
    for (const auto& writer : variableColumns) {
      writeVariableValue(
          *writer.column,
          *writer.vector,
          writer.decoded,
          row,
          payloadRow,
          heapCursor,
          heapRemaining,
          writer.complexSizes == nullptr ? 0 : writer.complexSizes[row],
          stream);
    }
  }
}

} // namespace

PayloadRowWriter::PayloadRowWriter() : impl_(std::make_unique<Impl>()) {}

PayloadRowWriter::~PayloadRowWriter() = default;

void PayloadRowWriter::clear() {
  impl_->complexSizes.reset();
  std::vector<std::optional<DecodedVector>>{}.swap(impl_->decoded);
  folly::small_vector<VariableColumnWriter, 32>{}.swap(impl_->variableColumns);
}

void PayloadRowWriter::append(
    const RowVector& input,
    RadixSortRunStorage& arena,
    PayloadRowBatch& batch) {
  const auto& layout = arena.payloadLayout();
  if (!layout->hasVariableFields()) {
    return appendRows(
        input, arena, nullptr, impl_->decoded, impl_->variableColumns, batch);
  }
  payload_size::RowSizes sizes;
  auto reusableHeapSizes = std::move(batch.heapSizes_);
  payload_size::measurePayloadRows(
      input,
      *layout,
      arena.pool(),
      std::move(reusableHeapSizes),
      impl_->complexSizes,
      sizes);
  appendRows(
      input, arena, &sizes, impl_->decoded, impl_->variableColumns, batch);
}

} // namespace bytedance::bolt::exec::radixsort
