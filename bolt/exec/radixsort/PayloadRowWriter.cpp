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

#include "bolt/exec/radixsort/PayloadRowWriter.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/PayloadRowWriterInternal.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

template <typename T>
T valueAt(const BaseVector& vector, vector_size_t row) {
  const auto* base = vector.wrappedVector();
  return base->asUnchecked<SimpleVector<T>>()->valueAt(
      vector.wrappedIndex(row));
}

void setNotNull(char* row, const PayloadRowColumnLayout& column) {
  row[column.nullByte] = static_cast<char>(
      static_cast<uint8_t>(row[column.nullByte]) | column.nullMask);
}

void setNull(char* row, const PayloadRowColumnLayout& column) {
  row[column.nullByte] = static_cast<char>(
      static_cast<uint8_t>(row[column.nullByte]) &
      static_cast<uint8_t>(~column.nullMask));
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

void writeFlatStringColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    char** heapCursors,
    vector_size_t begin,
    vector_size_t end) {
  const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
  const auto* values = flat->rawValues();
  const auto* nulls = flat->rawNulls();
  for (vector_size_t row = begin; row < end; ++row) {
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      setNull(rows[row], column);
      continue;
    }
    const auto value = values[row];
    auto* slot = rows[row] + column.offset;
    if (value.isInline()) {
      storeUnaligned<StringView>(slot, value);
      continue;
    }
    auto*& heap = heapCursors[row - begin];
    std::memcpy(heap, value.data(), value.size());
    storeUnaligned<StringView>(
        slot, StringView(heap, static_cast<int32_t>(value.size())));
    heap += value.size();
  }
}

void writeFlatScalar(
    const RowVector& input,
    const PayloadRowLayout& layout,
    const uint64_t* heapSizes,
    PayloadRowBatch& batch) {
  auto** rows = batch.rows()->asMutable<char*>();
  const bool initializeSlots = layout.hasVariableFields() ||
      std::any_of(input.children().begin(),
                  input.children().end(),
                  [](const auto& vector) {
                    return vector->typeKind() == TypeKind::UNKNOWN ||
                        vector->rawNulls() != nullptr;
                  });
  for (vector_size_t row = 0; row < input.size(); ++row) {
    if (initializeSlots) {
      std::memset(rows[row], 0, layout.rowWidth());
    }
    std::memset(rows[row], 0xff, layout.nullBytes());
    if (layout.variableSizeOffset().has_value()) {
      storeUnaligned<uint64_t>(
          rows[row] + *layout.variableSizeOffset(), heapSizes[row]);
    }
  }

  constexpr vector_size_t kRowsPerTile = 64;
  for (vector_size_t begin = 0; begin < input.size(); begin += kRowsPerTile) {
    const auto end = std::min(input.size(), begin + kRowsPerTile);
    std::array<char*, kRowsPerTile> heapCursors{};
    if (layout.hasVariableFields()) {
      for (vector_size_t row = begin; row < end; ++row) {
        heapCursors[row - begin] = batch.heapAt(row);
      }
    }
    for (uint32_t column = 0; column < layout.columns().size(); ++column) {
      const auto& metadata = layout.columns()[column];
      if (metadata.variable) {
        writeFlatStringColumn(
            metadata,
            *input.childAt(column),
            rows,
            heapCursors.data(),
            begin,
            end);
      } else {
        writeFixedFlatColumn(
            metadata, *input.childAt(column), rows, begin, end);
      }
    }
  }
}

template <typename T>
void writeFixedDecodedColumn(
    const PayloadRowColumnLayout& column,
    const DecodedVector& decoded,
    char* const* rows,
    vector_size_t size) {
  if (decoded.isConstantMapping()) {
    if (decoded.isNullAt(0)) {
      for (vector_size_t row = 0; row < size; ++row) {
        setNull(rows[row], column);
      }
      return;
    }
    const auto value = decoded.valueAt<T>(0);
    for (vector_size_t row = 0; row < size; ++row) {
      storeUnaligned<T>(rows[row] + column.offset, value);
    }
    return;
  }
  for (vector_size_t row = 0; row < size; ++row) {
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
    vector_size_t size) {
  DecodedVector decoded(vector);
  if (column.type->isShortDecimal()) {
    writeFixedDecodedColumn<int64_t>(column, decoded, rows, size);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    writeFixedDecodedColumn<int128_t>(column, decoded, rows, size);
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      for (vector_size_t row = 0; row < size; ++row) {
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
      writeFixedDecodedColumn<int8_t>(column, decoded, rows, size);
      return;
    case TypeKind::SMALLINT:
      writeFixedDecodedColumn<int16_t>(column, decoded, rows, size);
      return;
    case TypeKind::INTEGER:
      writeFixedDecodedColumn<int32_t>(column, decoded, rows, size);
      return;
    case TypeKind::BIGINT:
      writeFixedDecodedColumn<int64_t>(column, decoded, rows, size);
      return;
    case TypeKind::REAL:
      writeFixedDecodedColumn<float>(column, decoded, rows, size);
      return;
    case TypeKind::DOUBLE:
      writeFixedDecodedColumn<double>(column, decoded, rows, size);
      return;
    case TypeKind::TIMESTAMP:
      writeFixedDecodedColumn<Timestamp>(column, decoded, rows, size);
      return;
    case TypeKind::UNKNOWN:
      for (vector_size_t row = 0; row < size; ++row) {
        setNull(rows[row], column);
      }
      return;
    default:
      BOLT_FAIL(
          "Sort fixed payload column is not implemented for ",
          column.type->toString());
  }
}

void writeStringDecodedColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    char** heapCursors,
    uint64_t* heapRemaining,
    vector_size_t size) {
  DecodedVector decoded(vector);
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      setNull(rows[row], column);
      continue;
    }
    const auto value = decoded.valueAt<StringView>(row);
    auto* slot = rows[row] + column.offset;
    if (value.isInline()) {
      storeUnaligned<StringView>(slot, value);
      continue;
    }
    std::memcpy(heapCursors[row], value.data(), value.size());
    storeUnaligned<StringView>(
        slot, StringView(heapCursors[row], static_cast<int32_t>(value.size())));
    heapCursors[row] += value.size();
    heapRemaining[row] -= value.size();
  }
}

void writeComplexDecodedColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    char** heapCursors,
    uint64_t* heapRemaining,
    vector_size_t size) {
  DecodedVector decoded(vector);
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      setNull(rows[row], column);
      continue;
    }
    const auto decodedRow = decoded.index(row);
    const auto valueSize = exec::ContainerRowSerde::serializedSize(
        *decoded.base(),
        decodedRow,
        exec::ContainerRowSerdeOptions{.isKey = false});
    if (valueSize > 0) {
      exec::ContainerRowSerde::serializeTo(
          *decoded.base(),
          decodedRow,
          heapCursors[row],
          valueSize,
          exec::ContainerRowSerdeOptions{.isKey = false});
    }
    storeUnaligned<PayloadVarlenRef>(
        rows[row] + column.offset,
        PayloadVarlenRef{
            valueSize, valueSize == 0 ? nullptr : heapCursors[row]});
    if (valueSize > 0) {
      heapCursors[row] += valueSize;
    }
    heapRemaining[row] -= valueSize;
  }
}

} // namespace

void PayloadRowWriter::append(
    const RowVector& input,
    RadixSortRunStorage& arena,
    PayloadRowBatch& batch) {
  batch = PayloadRowBatch{};
  const auto& layout = arena.payloadLayout();
  BOLT_CHECK_NOT_NULL(
      layout, "Radix sort run storage does not have a payload layout");
  PayloadRowSizes sizes;
  measure(input, *layout, arena.pool(), sizes);
  append(input, arena, sizes, batch);
}

void PayloadRowWriter::appendFixedOnly(
    const RowVector& input,
    RadixSortRunStorage& arena,
    PayloadRowBatch& batch) {
  batch = PayloadRowBatch{};
  const auto& layout = arena.payloadLayout();
  BOLT_CHECK_NOT_NULL(
      layout, "Radix sort run storage does not have a payload layout");
  BOLT_CHECK(
      !layout->hasVariableFields(),
      "Payload row fixed-only append requires a fixed-only layout");
  validatePayloadRowInput(*layout, input);
  arena.allocateFixedPayloadRowBatch(input.size(), batch);
  if (input.size() == 0) {
    return;
  }
  if (canUseFlatScalarFastPath(*layout, input)) {
    return writeFlatScalar(input, *layout, nullptr, batch);
  }
  auto** rows = batch.rows_->asMutable<char*>();
  for (vector_size_t row = 0; row < input.size(); ++row) {
    auto* fixed = rows[row];
    std::memset(fixed, 0, layout->rowWidth());
    std::memset(fixed, 0xff, layout->nullBytes());
  }
  for (uint32_t column = 0; column < layout->columns().size(); ++column) {
    writeFixedDecodedColumn(
        layout->columns()[column], *input.childAt(column), rows, input.size());
  }
}

void PayloadRowWriter::append(
    const RowVector& input,
    RadixSortRunStorage& arena,
    const PayloadRowSizes& sizes,
    PayloadRowBatch& batch) {
  batch = PayloadRowBatch{};
  const auto& layout = arena.payloadLayout();
  BOLT_CHECK_NOT_NULL(
      layout, "Radix sort run storage does not have a payload layout");
  if (!layout->hasVariableFields()) {
    return appendFixedOnly(input, arena, batch);
  }
  validatePayloadRowInput(*layout, input);
  BOLT_CHECK(
      sizes.size() == input.size() &&
          (input.size() == 0 || sizes.heapSizes_ != nullptr),
      "Payload row input sizes do not match input");
  const auto* rawHeapSizes =
      sizes.heapSizes_ == nullptr ? nullptr : sizes.heapSizes_->as<uint64_t>();
  arena.allocatePayloadRowBatch(
      std::span<const uint64_t>(rawHeapSizes, input.size()), batch);
  if (input.size() == 0) {
    return;
  }

  if (canUseFlatScalarFastPath(*layout, input)) {
    return writeFlatScalar(input, *layout, rawHeapSizes, batch);
  }

  auto** rows = batch.rows_->asMutable<char*>();
  auto** heapCursors = batch.heaps_->asMutable<char*>();
  auto* heapRemaining = batch.heapSizes_->asMutable<uint64_t>();
  for (vector_size_t row = 0; row < input.size(); ++row) {
    auto* fixed = rows[row];
    std::memset(fixed, 0, layout->rowWidth());
    std::memset(fixed, 0xff, layout->nullBytes());
    if (layout->variableSizeOffset().has_value()) {
      storeUnaligned<uint64_t>(
          fixed + *layout->variableSizeOffset(), rawHeapSizes[row]);
    }
  }

  for (uint32_t column = 0; column < layout->columns().size(); ++column) {
    const auto& metadata = layout->columns()[column];
    const auto& vector = *input.childAt(column);
    if (metadata.complex) {
      writeComplexDecodedColumn(
          metadata, vector, rows, heapCursors, heapRemaining, input.size());
    } else if (metadata.variable) {
      writeStringDecodedColumn(
          metadata, vector, rows, heapCursors, heapRemaining, input.size());
    } else if (vector.encoding() == VectorEncoding::Simple::FLAT) {
      writeFixedFlatColumn(metadata, vector, rows, 0, input.size());
    } else {
      writeFixedDecodedColumn(metadata, vector, rows, input.size());
    }
  }

  for (vector_size_t row = 0; row < input.size(); ++row) {
    if (heapCursors[row] != nullptr) {
      heapCursors[row] -= rawHeapSizes[row];
    }
    heapRemaining[row] = rawHeapSizes[row];
  }
}

} // namespace bytedance::bolt::exec::radixsort
