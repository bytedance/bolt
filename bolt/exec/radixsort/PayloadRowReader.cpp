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

#include "bolt/exec/radixsort/PayloadRowReader.h"

#include <folly/small_vector.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/process/ProcessBase.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort {
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

template <typename T>
void setValue(const VectorPtr& vector, vector_size_t row, const char* slot) {
  vector->asUnchecked<FlatVector<T>>()->set(row, loadUnaligned<T>(slot));
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

void readValue(
    const PayloadRowColumnLayout& column,
    const char* row,
    vector_size_t rowIndex,
    const VectorPtr& result,
    char*& stringOutput,
    const char* stringOutputEnd) {
  const auto* slot = row + column.offset;
  if (column.type->isShortDecimal()) {
    setValue<int64_t>(result, rowIndex, slot);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    setValue<int128_t>(result, rowIndex, slot);
    return;
  }

  switch (column.type->kind()) {
    case TypeKind::BOOLEAN: {
      const auto value = loadUnaligned<uint8_t>(slot);
      result->asUnchecked<FlatVector<bool>>()->set(rowIndex, value != 0);
      return;
    }
    case TypeKind::TINYINT:
      setValue<int8_t>(result, rowIndex, slot);
      return;
    case TypeKind::SMALLINT:
      setValue<int16_t>(result, rowIndex, slot);
      return;
    case TypeKind::INTEGER:
      setValue<int32_t>(result, rowIndex, slot);
      return;
    case TypeKind::BIGINT:
      setValue<int64_t>(result, rowIndex, slot);
      return;
    case TypeKind::REAL:
      setValue<float>(result, rowIndex, slot);
      return;
    case TypeKind::DOUBLE:
      setValue<double>(result, rowIndex, slot);
      return;
    case TypeKind::TIMESTAMP: {
      const auto seconds = loadUnaligned<int64_t>(slot);
      const auto nanos = loadUnaligned<uint64_t>(slot + sizeof(int64_t));
      result->asUnchecked<FlatVector<Timestamp>>()->set(
          rowIndex, Timestamp(seconds, nanos));
      return;
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const auto value = loadUnaligned<StringView>(slot);
      auto* flat = result->asUnchecked<FlatVector<StringView>>();
      if (value.isInline()) {
        flat->setNoCopy(rowIndex, value);
        return;
      }
      std::memcpy(stringOutput, value.data(), value.size());
      flat->setNoCopy(
          rowIndex,
          StringView(stringOutput, static_cast<int32_t>(value.size())));
      stringOutput += value.size();
      return;
    }
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW: {
      const auto value = loadUnaligned<PayloadVarlenRef>(slot);
      if (value.size == 0) {
        result->setNull(rowIndex, false);
        return;
      }
      std::vector<ByteRange> ranges{ByteRange{
          reinterpret_cast<uint8_t*>(value.data),
          static_cast<int32_t>(value.size),
          0}};
      ByteInputStream input(std::move(ranges));
      exec::ContainerRowSerde::deserialize(input, rowIndex, result.get(), true);
      return;
    }
    case TypeKind::UNKNOWN:
      BOLT_FAIL("UNKNOWN sort payload values must be null");
    default:
      BOLT_FAIL(
          "Payload row gather is not implemented for {}",
          column.type->toString());
  }
}

void gatherImpl(
    const PayloadRowLayout& layout,
    std::span<char* const> rows,
    std::span<const uint8_t> mayHaveNulls,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  BOLT_CHECK_NOT_NULL(pool, "Payload row output memory pool must not be null");
  BOLT_CHECK_LE(
      rows.size(),
      static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()),
      "Payload row output row count exceeds vector range");
  const auto size = static_cast<vector_size_t>(rows.size());
  BOLT_CHECK_EQ(
      mayHaveNulls.size(),
      layout.columns().size(),
      "Payload row nullability does not match layout");
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

    uint64_t stringBytes = 0;
    char* stringOutput = nullptr;
    if (stringBytes > 0) {
      stringOutput = child->asUnchecked<FlatVector<StringView>>()
                         ->getRawStringBufferWithSpace(stringBytes, true);
    }
    const auto* stringOutputEnd =
        stringOutput == nullptr ? nullptr : stringOutput + stringBytes;

    for (vector_size_t rowIndex = 0; rowIndex < size; ++rowIndex) {
      const auto* row = rows[rowIndex];
      if (isNull(row, column)) {
        child->setNull(rowIndex, true);
        continue;
      }
      readValue(column, row, rowIndex, child, stringOutput, stringOutputEnd);
    }
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
    std::span<const uint8_t> mayHaveNulls,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  gatherImpl(layout, rows, mayHaveNulls, pool, result);
}

void PayloadRowReader::gather(
    const PayloadRowLayout& layout,
    std::span<char* const> rows,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  std::vector<uint8_t> mayHaveNulls(layout.columns().size(), 0);
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    uint8_t hasNull = 0;
    for (const auto* row : rows) {
      hasNull |= static_cast<uint8_t>(isNull(row, layout.columns()[column]));
    }
    mayHaveNulls[column] = hasNull;
  }
  gather(layout, rows, mayHaveNulls, pool, result);
}

void PayloadRowReader::gather(
    const PayloadRowLayout& layout,
    const PayloadRowBatch& batch,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  if (batch.size() == 0) {
    gather(layout, std::span<char* const>{}, pool, result);
    return;
  }
  if (batch.rows() == nullptr) {
    result.reset();
    BOLT_FAIL("Payload row batch rows must not be null");
  }
  gather(
      layout,
      std::span<char* const>(
          batch.rows()->as<char*>(), static_cast<uint64_t>(batch.size())),
      pool,
      result);
}

} // namespace bytedance::bolt::exec::radixsort
