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
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"
#include "bolt/vector/VariantVector.h"

namespace bytedance::bolt::exec::radixsort {
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
  vector_size_t size() const {
    return size_;
  }

  const BufferPtr& heapSizes() const {
    return heapSizes_;
  }

  vector_size_t size_{0};
  BufferPtr heapSizes_;
};

FOLLY_ALWAYS_INLINE void addSize(uint64_t& size, uint64_t increment) {
  auto next = checkedAdd<uint64_t>(size, increment);
  BOLT_CHECK(next.has_value(), "Payload serialized size overflows");
  size = *next;
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
          checkedMultiply<uint64_t>(count, *metadata.fixedElementSize);
      BOLT_CHECK(valuesSize.has_value(), "Payload serialized size overflows");
      addSize(size, *valuesSize);
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
    RowSizes& sizes) {
  sizes = RowSizes{};
  BOLT_CHECK_NOT_NULL(
      pool, "Payload row size pass memory pool must not be null");

  const auto rowCount = input.size();
  const auto fixedRowBytes = layout.rowWidth();
  auto fixedBytes = checkedMultiply<uint64_t>(rowCount, fixedRowBytes);
  BOLT_CHECK(fixedBytes.has_value(), "Payload row input fixed bytes overflow");

  sizes.size_ = rowCount;
  if (rowCount == 0 || !layout.hasVariableFields()) {
    return;
  }

  sizes.heapSizes_ =
      AlignedBuffer::allocate<uint64_t>(rowCount, pool, uint64_t{0});
  auto* rawHeapSizes = sizes.heapSizes_->asMutable<uint64_t>();
  const auto addVariableBytes = [&](vector_size_t row, uint64_t bytes) {
    addSize(rawHeapSizes[row], bytes);
  };

  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    const auto& metadata = layout.columns()[column];
    if (!metadata.variable) {
      continue;
    }
    const auto& vector = *input.childAt(column);

    if (metadata.complex) {
      switch (metadata.type->kind()) {
        case TypeKind::ARRAY: {
          const auto* array =
              vector.wrappedVector()->asUnchecked<ArrayVector>();
          const auto elementMetadata = rangeSizeMetadata(*array->elements());
          if (!vector.mayHaveNulls()) {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              addVariableBytes(
                  row,
                  serializedArraySizeForPayload(
                      *array, vector.wrappedIndex(row), elementMetadata));
            }
          } else {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              if (!vector.isNullAt(row)) {
                addVariableBytes(
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
              addVariableBytes(
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
                addVariableBytes(
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
            addVariableBytes(row, size);
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
    ByteOutputStream& stream) {
  if (decoded.isNullAt(row)) {
    setNull(payloadRow, column);
    return;
  }
  const auto decodedRow = decoded.index(row);
  stream.setRange(
      ByteRange{
          reinterpret_cast<uint8_t*>(heapCursor),
          static_cast<int32_t>(heapRemaining),
          0},
      0);
  exec::ContainerRowSerde::serialize(
      *decoded.base(),
      decodedRow,
      stream,
      exec::ContainerRowSerdeOptions{.isKey = false});
  const auto valueSize = stream.size();
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
    ByteOutputStream& stream) {
  if (column.complex) {
    BOLT_DCHECK_NOT_NULL(decoded);
    writeComplexDecodedValue(
        column, *decoded, row, payloadRow, heapCursor, heapRemaining, stream);
    return;
  }
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    writeFlatStringValue(
        column, vector, row, payloadRow, heapCursor, &heapRemaining);
  } else {
    BOLT_DCHECK_NOT_NULL(decoded);
    writeStringDecodedValue(
        column, *decoded, row, payloadRow, heapCursor, &heapRemaining);
  }
}

struct VariableColumnWriter {
  const PayloadRowColumnLayout* column;
  const BaseVector* vector;
  const DecodedVector* decoded;
};

void appendRows(
    const RowVector& input,
    RadixSortRunStorage& arena,
    const payload_size::RowSizes* sizes,
    PayloadRowBatch& batch) {
  batch = PayloadRowBatch{};
  const auto& layout = arena.payloadLayout();
  BOLT_CHECK_NOT_NULL(
      layout, "Radix sort run storage does not have a payload layout");
  const auto hasVariableFields = layout->hasVariableFields();
  const uint64_t* rawHeapSizes = nullptr;
  if (hasVariableFields) {
    BOLT_CHECK_NOT_NULL(sizes, "Variable sort payload requires measured sizes");
    BOLT_CHECK(
        sizes->size() == input.size() &&
            (input.size() == 0 || sizes->heapSizes() != nullptr),
        "Payload row input sizes do not match input");
    rawHeapSizes = sizes->heapSizes() == nullptr
        ? nullptr
        : sizes->heapSizes()->as<uint64_t>();
    arena.allocatePayloadRowBatch(
        std::span<const uint64_t>(rawHeapSizes, input.size()), batch);
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

  std::vector<std::unique_ptr<DecodedVector>> decoded(input.childrenSize());
  for (uint32_t column = 0; column < input.childrenSize(); ++column) {
    if (input.childAt(column)->encoding() != VectorEncoding::Simple::FLAT) {
      decoded[column] = std::make_unique<DecodedVector>(*input.childAt(column));
    }
  }

  std::vector<VariableColumnWriter> variableColumns;
  if (hasVariableFields) {
    variableColumns.reserve(layout->columns().size());
  }
  for (uint32_t column = 0; column < layout->columns().size(); ++column) {
    const auto& metadata = layout->columns()[column];
    if (!metadata.variable) {
      writeFixedColumn(
          metadata,
          *input.childAt(column),
          decoded[column].get(),
          rows,
          input.size());
    } else {
      variableColumns.push_back(VariableColumnWriter{
          &metadata, input.childAt(column).get(), decoded[column].get()});
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
          stream);
    }
  }
}

} // namespace

void PayloadRowWriter::append(
    const RowVector& input,
    RadixSortRunStorage& arena,
    PayloadRowBatch& batch) {
  const auto& layout = arena.payloadLayout();
  BOLT_CHECK_NOT_NULL(
      layout, "Radix sort run storage does not have a payload layout");
  if (!layout->hasVariableFields()) {
    return appendRows(input, arena, nullptr, batch);
  }
  payload_size::RowSizes sizes;
  payload_size::measurePayloadRows(input, *layout, arena.pool(), sizes);
  appendRows(input, arena, &sizes, batch);
}

} // namespace bytedance::bolt::exec::radixsort
