/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/row/CompactRow.h"

#include <limits>

#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::row {
namespace {
constexpr size_t kSizeBytes = sizeof(int32_t);
constexpr size_t kMaxSerializedSize = std::numeric_limits<int32_t>::max();
using TRowSize = uint32_t;

size_t nullBytes(size_t size) {
  return size / 8 + (size % 8 != 0);
}

int32_t checkedSerializedSize(size_t size) {
  BOLT_USER_CHECK_LE(
      size,
      kMaxSerializedSize,
      "CompactRow serialized value exceeds the maximum supported size of {} "
      "bytes",
      kMaxSerializedSize);
  return static_cast<int32_t>(size);
}

void checkAvailable(
    std::string_view data,
    size_t row,
    size_t offset,
    size_t bytes,
    std::string_view description) {
  BOLT_CHECK(
      offset <= data.size() && bytes <= data.size() - offset,
      "Invalid CompactRow: {} needs {} bytes at offset {} in row {} with size "
      "{}",
      description,
      bytes,
      offset,
      row,
      data.size());
}

void checkAvailable(
    const std::vector<std::string_view>& data,
    size_t row,
    size_t offset,
    size_t bytes,
    std::string_view description) {
  checkAvailable(data[row], row, offset, bytes, description);
}

void writeInt32(char* buffer, int32_t n) {
  ::memcpy(buffer, &n, kSizeBytes);
}

int32_t readInt32(const char* buffer) {
  int32_t n;
  ::memcpy(&n, buffer, kSizeBytes);
  return n;
}

FOLLY_ALWAYS_INLINE void writeFixedWidth(
    const char* rawData,
    vector_size_t index,
    size_t valueBytes,
    char* buffer,
    size_t& offset) {
  ::memcpy(buffer + offset, rawData + index * valueBytes, valueBytes);
  offset += valueBytes;
}

FOLLY_ALWAYS_INLINE void
writeTimestamp(const Timestamp& timestamp, char* buffer, size_t& offset) {
  // Write micros(int64_t) for timestamp value.
  const auto timeUs = timestamp.toMicros();
  ::memcpy(buffer + offset, &timeUs, sizeof(int64_t));
  offset += sizeof(int64_t);
}

FOLLY_ALWAYS_INLINE void
writeString(const StringView& value, char* buffer, size_t& offset) {
  writeInt32(buffer + offset, value.size());
  if (!value.empty()) {
    ::memcpy(buffer + offset + kSizeBytes, value.data(), value.size());
  }
  offset += kSizeBytes + value.size();
}

// Serialize the child vector of a row type within a range of consecutive rows.
// Write the serialized data at offsets of buffer row by row.
// Update offsets with the actual serialized size.
template <TypeKind kind>
void serializeTyped(
    const raw_vector<vector_size_t>& rows,
    uint32_t childIdx,
    const DecodedVector& decoded,
    size_t valueBytes,
    const raw_vector<uint8_t*>& nulls,
    char* buffer,
    std::vector<size_t>& offsets) {
  const auto* rawData = decoded.data<char>();
  if (!decoded.mayHaveNulls()) {
    for (auto i = 0; i < rows.size(); ++i) {
      writeFixedWidth(
          rawData, decoded.index(rows[i]), valueBytes, buffer, offsets[i]);
    }
  } else {
    for (auto i = 0; i < rows.size(); ++i) {
      if (decoded.isNullAt(rows[i])) {
        bits::setBit(nulls[i], childIdx, true);
        offsets[i] += valueBytes;
      } else {
        writeFixedWidth(
            rawData, decoded.index(rows[i]), valueBytes, buffer, offsets[i]);
      }
    }
  }
}

template <>
void serializeTyped<TypeKind::UNKNOWN>(
    const raw_vector<vector_size_t>& rows,
    uint32_t childIdx,
    const DecodedVector& /* unused */,
    size_t /* unused */,
    const raw_vector<uint8_t*>& nulls,
    char* /* unused */,
    std::vector<size_t>& /* unused */) {
  for (auto i = 0; i < rows.size(); ++i) {
    bits::setBit(nulls[i], childIdx, true);
  }
}

template <>
void serializeTyped<TypeKind::BOOLEAN>(
    const raw_vector<vector_size_t>& rows,
    uint32_t childIdx,
    const DecodedVector& decoded,
    size_t /* unused */,
    const raw_vector<uint8_t*>& nulls,
    char* buffer,
    std::vector<size_t>& offsets) {
  auto* byte = reinterpret_cast<bool*>(buffer);
  if (!decoded.mayHaveNulls()) {
    for (auto i = 0; i < rows.size(); ++i) {
      byte[offsets[i]++] = decoded.valueAt<bool>(rows[i]);
    }
  } else {
    for (auto i = 0; i < rows.size(); ++i) {
      if (decoded.isNullAt(rows[i])) {
        bits::setBit(nulls[i], childIdx, true);
        offsets[i] += 1;
      } else {
        // Write 1 byte for bool type.
        byte[offsets[i]++] = decoded.valueAt<bool>(rows[i]);
      }
    }
  }
}

template <>
void serializeTyped<TypeKind::TIMESTAMP>(
    const raw_vector<vector_size_t>& rows,
    uint32_t childIdx,
    const DecodedVector& decoded,
    size_t /* unused */,
    const raw_vector<uint8_t*>& nulls,
    char* buffer,
    std::vector<size_t>& offsets) {
  const auto* rawData = decoded.data<Timestamp>();
  if (!decoded.mayHaveNulls()) {
    for (auto i = 0; i < rows.size(); ++i) {
      const auto index = decoded.index(rows[i]);
      writeTimestamp(rawData[index], buffer, offsets[i]);
    }
  } else {
    for (auto i = 0; i < rows.size(); ++i) {
      if (decoded.isNullAt(rows[i])) {
        bits::setBit(nulls[i], childIdx, true);
        offsets[i] += sizeof(int64_t);
      } else {
        const auto index = decoded.index(rows[i]);
        writeTimestamp(rawData[index], buffer, offsets[i]);
      }
    }
  }
}

template <>
void serializeTyped<TypeKind::VARCHAR>(
    const raw_vector<vector_size_t>& rows,
    uint32_t childIdx,
    const DecodedVector& decoded,
    size_t valueBytes,
    const raw_vector<uint8_t*>& nulls,
    char* buffer,
    std::vector<size_t>& offsets) {
  if (!decoded.mayHaveNulls()) {
    for (auto i = 0; i < rows.size(); ++i) {
      writeString(decoded.valueAt<StringView>(rows[i]), buffer, offsets[i]);
    }
  } else {
    for (auto i = 0; i < rows.size(); ++i) {
      if (decoded.isNullAt(rows[i])) {
        bits::setBit(nulls[i], childIdx, true);
      } else {
        writeString(decoded.valueAt<StringView>(rows[i]), buffer, offsets[i]);
      }
    }
  }
}

template <>
void serializeTyped<TypeKind::VARBINARY>(
    const raw_vector<vector_size_t>& rows,
    uint32_t childIdx,
    const DecodedVector& decoded,
    size_t valueBytes,
    const raw_vector<uint8_t*>& nulls,
    char* buffer,
    std::vector<size_t>& offsets) {
  serializeTyped<TypeKind::VARCHAR>(
      rows, childIdx, decoded, valueBytes, nulls, buffer, offsets);
}
} // namespace

CompactRow::CompactRow(const RowVectorPtr& vector)
    : typeKind_{vector->typeKind()}, decoded_{*vector} {
  initialize(vector->type());
}

CompactRow::CompactRow(const VectorPtr& vector)
    : typeKind_{vector->typeKind()}, decoded_{*vector} {
  initialize(vector->type());
}

void CompactRow::initialize(const TypePtr& type) {
  auto base = decoded_.base();
  switch (typeKind_) {
    case TypeKind::ARRAY: {
      auto arrayBase = base->as<ArrayVector>();
      children_.push_back(CompactRow(arrayBase->elements()));
      childIsFixedWidth_.push_back(
          arrayBase->elements()->type()->isFixedWidth());
      break;
    }
    case TypeKind::MAP: {
      auto mapBase = base->as<MapVector>();
      children_.push_back(CompactRow(mapBase->mapKeys()));
      children_.push_back(CompactRow(mapBase->mapValues()));
      childIsFixedWidth_.push_back(mapBase->mapKeys()->type()->isFixedWidth());
      childIsFixedWidth_.push_back(
          mapBase->mapValues()->type()->isFixedWidth());
      break;
    }
    case TypeKind::ROW: {
      auto rowBase = base->as<RowVector>();
      for (const auto& child : rowBase->children()) {
        children_.push_back(CompactRow(child));
        childIsFixedWidth_.push_back(child->type()->isFixedWidth());
      }

      rowNullBytes_ = nullBytes(type->size());
      break;
    }
    case TypeKind::BOOLEAN:
      valueBytes_ = 1;
      fixedWidthTypeKind_ = true;
      break;
    case TypeKind::TINYINT:
      [[fallthrough]];
    case TypeKind::SMALLINT:
      [[fallthrough]];
    case TypeKind::INTEGER:
      [[fallthrough]];
    case TypeKind::BIGINT:
      [[fallthrough]];
    case TypeKind::HUGEINT:
      [[fallthrough]];
    case TypeKind::REAL:
      [[fallthrough]];
    case TypeKind::DOUBLE:
      valueBytes_ = type->cppSizeInBytes();
      fixedWidthTypeKind_ = true;
      supportsBulkCopy_ = decoded_.isIdentityMapping();
      break;
    case TypeKind::TIMESTAMP:
      valueBytes_ = sizeof(int64_t);
      fixedWidthTypeKind_ = true;
      break;
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY:
      // Nothing to do.
      break;
    case TypeKind::UNKNOWN:
      // UNKNOWN values are always nulls, hence, do not take up space.
      valueBytes_ = 0;
      fixedWidthTypeKind_ = true;
      supportsBulkCopy_ = true;
      break;
    default:
      BOLT_UNSUPPORTED("Unsupported type: {}", type->toString());
  }
}

namespace {
std::optional<int32_t> fixedValueSize(const TypePtr& type) {
  if (type->isTimestamp()) {
    return sizeof(int64_t);
  }
  if (type->isFixedWidth()) {
    return type->cppSizeInBytes();
  }
  return std::nullopt;
}
} // namespace

// static
std::optional<int32_t> CompactRow::fixedRowSize(const RowTypePtr& rowType) {
  const size_t numFields = rowType->size();
  const size_t nullLength = nullBytes(numFields);

  size_t size = nullLength;
  for (const auto& child : rowType->children()) {
    if (auto valueBytes = fixedValueSize(child)) {
      size += valueBytes.value();
    } else {
      return std::nullopt;
    }
  }

  return checkedSerializedSize(size);
}

int32_t CompactRow::rowSize(vector_size_t index) const {
  return rowRowSize(index);
}

int32_t CompactRow::rowRowSize(vector_size_t index) const {
  auto childIndex = decoded_.index(index);

  const auto numFields = children_.size();
  size_t size = rowNullBytes_;

  for (auto i = 0; i < numFields; ++i) {
    if (childIsFixedWidth_[i]) {
      size += children_[i].valueBytes_;
    } else if (!children_[i].isNullAt(childIndex)) {
      size += children_[i].variableWidthRowSize(childIndex);
    }
  }

  return checkedSerializedSize(size);
}

int32_t CompactRow::serializeRow(vector_size_t index, char* buffer) const {
  auto childIndex = decoded_.index(index);

  size_t valuesOffset = rowNullBytes_;

  auto* nulls = reinterpret_cast<uint8_t*>(buffer);

  for (auto i = 0; i < children_.size(); ++i) {
    auto& child = children_[i];

    // Write null bit. Advance offset if 'fixed-width'.
    if (child.isNullAt(childIndex)) {
      bits::setBit(nulls, i, true);
      if (childIsFixedWidth_[i]) {
        valuesOffset += child.valueBytes_;
      }
      continue;
    }

    if (childIsFixedWidth_[i]) {
      // Write fixed-width value.
      if (child.valueBytes_ > 0) {
        child.serializeFixedWidth(childIndex, buffer + valuesOffset);
      }
      valuesOffset += child.valueBytes_;
    } else {
      // Write non-null variable-width value.
      auto size =
          child.serializeVariableWidth(childIndex, buffer + valuesOffset);
      valuesOffset += size;
    }
  }

  return checkedSerializedSize(valuesOffset);
}

void CompactRow::serializeRow(
    vector_size_t offset,
    vector_size_t size,
    char* buffer,
    const size_t* bufferOffsets) const {
  raw_vector<vector_size_t> rows(size);
  raw_vector<uint8_t*> nulls(size);
  if (decoded_.isIdentityMapping()) {
    std::iota(rows.begin(), rows.end(), offset);
  } else {
    for (auto i = 0; i < size; ++i) {
      rows[i] = decoded_.index(offset + i);
    }
  }

  // After serializing each column, the 'offsets' are updated accordingly.
  std::vector<size_t> offsets(size);
  auto* const base = reinterpret_cast<uint8_t*>(buffer);
  for (auto i = 0; i < size; ++i) {
    nulls[i] = base + bufferOffsets[i];
    offsets[i] = bufferOffsets[i] + rowNullBytes_;
  }

  // Fixed-width and varchar/varbinary types are serialized using the vectorized
  // API 'serializedTyped'. Other data types are serialized row-by-row.
  for (auto childIdx = 0; childIdx < children_.size(); ++childIdx) {
    auto& child = children_[childIdx];
    if (childIsFixedWidth_[childIdx] ||
        child.typeKind_ == TypeKind::VARBINARY ||
        child.typeKind_ == TypeKind::VARCHAR) {
      BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          serializeTyped,
          child.typeKind_,
          rows,
          childIdx,
          child.decoded_,
          child.valueBytes_,
          nulls,
          buffer,
          offsets);
    } else {
      const bool mayHaveNulls = child.decoded_.mayHaveNulls();
      for (auto i = 0; i < rows.size(); ++i) {
        if (mayHaveNulls && child.isNullAt(rows[i])) {
          bits::setBit(nulls[i], childIdx, true);
        } else {
          // Write non-null variable-width value.
          offsets[i] +=
              child.serializeVariableWidth(rows[i], buffer + offsets[i]);
        }
      }
    }
  }
}

bool CompactRow::isNullAt(vector_size_t index) const {
  return decoded_.isNullAt(index);
}

int32_t CompactRow::variableWidthRowSize(vector_size_t index) const {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      return checkedSerializedSize(kSizeBytes + value.size());
    }
    case TypeKind::ARRAY:
      return arrayRowSize(index);
    case TypeKind::MAP:
      return mapRowSize(index);
    case TypeKind::ROW:
      return rowRowSize(index);
    default:
      BOLT_UNREACHABLE(
          "Unexpected type kind: {}", mapTypeKindToName(typeKind_));
  };
}

int32_t CompactRow::arrayRowSize(vector_size_t index) const {
  auto baseIndex = decoded_.index(index);

  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]);
}

int32_t CompactRow::arrayRowSize(
    const CompactRow& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth) const {
  BOLT_DCHECK_GE(size, 0);
  const size_t numNullBytes = nullBytes(size);

  // array size | null bits | elements

  // 4 bytes for number of elements, some bytes for null flags.
  size_t rowSize = sizeof(int32_t) + numNullBytes;
  if (fixedWidth) {
    rowSize += static_cast<size_t>(size) * elements.valueBytes();
    return checkedSerializedSize(rowSize);
  }

  if (size == 0) {
    return checkedSerializedSize(rowSize);
  }

  // If element type is a complex type, then add 4 bytes for overall serialized
  // size of the array + 4 bytes per element for offset of the serialized
  // element.
  // size | nulls | serialized size | serialized offset 1 | serialized offset 2
  // |...| element 1 | element 2 |...

  if (!(elements.typeKind_ == TypeKind::VARCHAR ||
        elements.typeKind_ == TypeKind::VARBINARY)) {
    // 4 bytes for the overall serialized size + 4 bytes for the offset of each
    // element.
    rowSize += sizeof(int32_t);
    rowSize += static_cast<size_t>(size) * sizeof(int32_t);
  }

  for (auto i = 0; i < size; ++i) {
    if (!elements.isNullAt(offset + i)) {
      rowSize += elements.variableWidthRowSize(offset + i);
    }
  }

  return checkedSerializedSize(rowSize);
}

int32_t CompactRow::serializeArray(vector_size_t index, char* buffer) const {
  auto baseIndex = decoded_.index(index);

  // For complex-type elements:
  // array size | null bits | serialized size | offset e1 | offset e2 |... | e1
  // | e2 |...
  //
  // 'serialized size' is the number of bytes starting after null bits and to
  // the end of the array. Offsets are specified relative to position right
  // after 'serialized size'.
  //
  // For fixed-width or string element type:
  // array size | null bite | e1 | e2 |...

  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return serializeAsArray(
      children_[0], offset, size, childIsFixedWidth_[0], buffer);
}

int32_t CompactRow::serializeAsArray(
    const CompactRow& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth,
    char* buffer) const {
  // For complex-type elements:
  // array size | null bits | serialized size | offset e1 | offset e2 |... | e1
  // | e2 |...
  //
  // For fixed-width and string element types:
  // array size | null bits | e1 | e2 |...

  BOLT_DCHECK_GE(size, 0);
  const size_t numNullBytes = nullBytes(size);
  const size_t nullsOffset = kSizeBytes;
  size_t elementsOffset = nullsOffset + numNullBytes;

  // Validate the complete fixed-width layout before writing.
  const auto fixedSerializedSize = fixedWidth
      ? checkedSerializedSize(
            elementsOffset + static_cast<size_t>(size) * elements.valueBytes_)
      : 0;

  // Write array size and null flags.
  writeInt32(buffer, size);

  auto* rawNulls = reinterpret_cast<uint8_t*>(buffer + nullsOffset);

  if (elements.supportsBulkCopy_) {
    BOLT_DCHECK(fixedWidth);
    if (elements.decoded_.mayHaveNulls()) {
      for (auto i = 0; i < size; ++i) {
        if (elements.isNullAt(offset + i)) {
          bits::setBit(rawNulls, i, true);
        }
      }
    }
    elements.serializeFixedWidth(offset, size, buffer + elementsOffset);
    return fixedSerializedSize;
  }

  if (fixedWidth) {
    for (auto i = 0; i < size; ++i) {
      if (elements.isNullAt(offset + i)) {
        bits::setBit(rawNulls, i, true);
      } else {
        elements.serializeFixedWidth(offset + i, buffer + elementsOffset);
      }
      elementsOffset += elements.valueBytes_;
    }
    return fixedSerializedSize;
  } else if (
      elements.typeKind_ == TypeKind::VARCHAR ||
      elements.typeKind_ == TypeKind::VARBINARY) {
    for (auto i = 0; i < size; ++i) {
      if (elements.isNullAt(offset + i)) {
        bits::setBit(rawNulls, i, true);
      } else {
        auto serializedBytes = elements.serializeVariableWidth(
            offset + i, buffer + elementsOffset);
        elementsOffset += serializedBytes;
      }
    }
  } else {
    if (size > 0) {
      // Leave room for serialized size and offsets.
      elementsOffset += kSizeBytes;
      const size_t baseOffset = elementsOffset;
      elementsOffset += static_cast<size_t>(size) * kSizeBytes;
      checkedSerializedSize(elementsOffset);

      for (auto i = 0; i < size; ++i) {
        if (elements.isNullAt(offset + i)) {
          bits::setBit(rawNulls, i, true);
        } else {
          writeInt32(
              buffer + baseOffset + i * kSizeBytes,
              checkedSerializedSize(elementsOffset - baseOffset));

          auto serializedBytes = elements.serializeVariableWidth(
              offset + i, buffer + elementsOffset);

          elementsOffset += serializedBytes;
        }
      }

      writeInt32(
          buffer + baseOffset - kSizeBytes,
          checkedSerializedSize(elementsOffset - baseOffset));
    }
  }

  return checkedSerializedSize(elementsOffset);
}

int32_t CompactRow::mapRowSize(vector_size_t index) const {
  auto baseIndex = decoded_.index(index);

  //  <keys array> | <values array>

  auto mapBase = decoded_.base()->asUnchecked<MapVector>();
  auto offset = mapBase->offsetAt(baseIndex);
  auto size = mapBase->sizeAt(baseIndex);

  size_t rowSize =
      arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]);
  rowSize += arrayRowSize(children_[1], offset, size, childIsFixedWidth_[1]);
  return checkedSerializedSize(rowSize);
}

int32_t CompactRow::serializeMap(vector_size_t index, char* buffer) const {
  auto baseIndex = decoded_.index(index);

  //  <keys array> | <values array>

  auto mapBase = decoded_.base()->asUnchecked<MapVector>();
  auto offset = mapBase->offsetAt(baseIndex);
  auto size = mapBase->sizeAt(baseIndex);

  auto keysSerializedBytes = serializeAsArray(
      children_[0], offset, size, childIsFixedWidth_[0], buffer);

  auto valuesSerializedBytes = serializeAsArray(
      children_[1],
      offset,
      size,
      childIsFixedWidth_[1],
      buffer + keysSerializedBytes);

  size_t serializedBytes = keysSerializedBytes;
  serializedBytes += valuesSerializedBytes;
  return checkedSerializedSize(serializedBytes);
}

int32_t CompactRow::serialize(vector_size_t index, char* buffer) const {
  return serializeRow(index, buffer);
}

void CompactRow::serialize(
    vector_size_t offset,
    vector_size_t size,
    const size_t* bufferOffsets,
    char* buffer) const {
  serializeRow(offset, size, buffer, bufferOffsets);
}

void CompactRow::serializeFixedWidth(vector_size_t index, char* buffer) const {
  BOLT_DCHECK(fixedWidthTypeKind_);
  switch (typeKind_) {
    case TypeKind::BOOLEAN:
      *reinterpret_cast<bool*>(buffer) = decoded_.valueAt<bool>(index);
      break;
    case TypeKind::TIMESTAMP: {
      auto micros = decoded_.valueAt<Timestamp>(index).toMicros();
      ::memcpy(buffer, &micros, sizeof(int64_t));
      break;
    }
    default:
      ::memcpy(
          buffer,
          decoded_.data<char>() + decoded_.index(index) * valueBytes_,
          valueBytes_);
  }
}

void CompactRow::serializeFixedWidth(
    vector_size_t offset,
    vector_size_t size,
    char* buffer) const {
  BOLT_DCHECK(supportsBulkCopy_);
  // decoded_.data<char>() can be null if all values are null.
  if (decoded_.data<char>()) {
    ::memcpy(
        buffer,
        decoded_.data<char>() + decoded_.index(offset) * valueBytes_,
        valueBytes_ * size);
  }
}

int32_t CompactRow::serializeVariableWidth(vector_size_t index, char* buffer)
    const {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      const auto serializedBytes =
          checkedSerializedSize(kSizeBytes + static_cast<size_t>(value.size()));
      writeInt32(buffer, value.size());
      if (!value.empty()) {
        ::memcpy(buffer + kSizeBytes, value.data(), value.size());
      }
      return serializedBytes;
    }
    case TypeKind::ARRAY:
      return serializeArray(index, buffer);
    case TypeKind::MAP:
      return serializeMap(index, buffer);
    case TypeKind::ROW:
      return serializeRow(index, buffer);
    default:
      BOLT_UNREACHABLE(
          "Unexpected type kind: {}", mapTypeKindToName(typeKind_));
  };
}

namespace {

// Reads single fixed-width value from buffer into flatVector[index].
template <typename T>
void readFixedWidthValue(
    bool isNull,
    const char* buffer,
    FlatVector<T>* flatVector,
    vector_size_t index) {
  if (isNull) {
    flatVector->setNull(index, true);
  } else if constexpr (std::is_same_v<T, Timestamp>) {
    int64_t micros;
    ::memcpy(&micros, buffer, sizeof(int64_t));
    flatVector->set(index, Timestamp::fromMicros(micros));
  } else {
    T value;
    ::memcpy(&value, buffer, sizeof(T));
    flatVector->set(index, value);
  }
}

template <typename T>
int32_t valueSize() {
  if constexpr (std::is_same_v<T, Timestamp>) {
    return sizeof(int64_t);
  } else {
    return sizeof(T);
  }
}

// Deserializes one fixed-width value from each 'row' in 'data'.
// Each value starts at data[row].data() + offsets[row].
//
// @param nulls Null flags for the values.
// @param offsets Offsets in 'data' for the serialized values. Not used if value
// is null.
template <TypeKind Kind>
VectorPtr deserializeFixedWidth(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    const std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;

  const auto numRows = data.size();
  auto flatVector = BaseVector::create<FlatVector<T>>(type, numRows, pool);

  auto* rawNulls = nulls->as<uint64_t>();

  for (auto i = 0; i < numRows; ++i) {
    const bool isNull = bits::isBitNull(rawNulls, i);
    if (!isNull) {
      checkAvailable(data, i, offsets[i], valueSize<T>(), "fixed-width value");
    }
    const char* buffer = isNull ? nullptr : data[i].data() + offsets[i];
    readFixedWidthValue<T>(isNull, buffer, flatVector.get(), i);
  }

  return flatVector;
}

vector_size_t totalSize(const vector_size_t* rawSizes, size_t numRows) {
  size_t total = 0;
  for (auto i = 0; i < numRows; ++i) {
    BOLT_DCHECK_GE(rawSizes[i], 0);
    total += rawSizes[i];
  }
  BOLT_DCHECK_LE(total, std::numeric_limits<vector_size_t>::max());
  return static_cast<vector_size_t>(total);
}

const uint8_t* readNulls(const char* buffer) {
  return reinterpret_cast<const uint8_t*>(buffer);
}

// Deserializes multiple fixed-width values from each 'row' in 'data'.
// Each set of values starts at data[row].data() + offsets[row] and contains
// null flags followed by values. The number of values is provided in
// sizes[row].
// nulls | v1 | v2 | v3 |...
// Advances offsets past the last value.
template <TypeKind Kind>
VectorPtr deserializeFixedWidthArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;

  const int32_t valueBytes = valueSize<T>();

  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  auto flatVector = BaseVector::create<FlatVector<T>>(type, total, pool);

  vector_size_t index = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      const auto numNullBytes = nullBytes(size);
      auto* rawElementNulls = readNulls(data[i].data() + offsets[i]);

      offsets[i] += numNullBytes;

      for (auto j = 0; j < size; ++j) {
        readFixedWidthValue<T>(
            bits::isBitSet(rawElementNulls, j),
            data[i].data() + offsets[i],
            flatVector.get(),
            index);

        offsets[i] += valueBytes;
        ++index;
      }
    }
  }

  return flatVector;
}

size_t readString(
    std::string_view data,
    size_t offset,
    FlatVector<StringView>* flatVector,
    vector_size_t index,
    size_t row) {
  checkAvailable(data, row, offset, kSizeBytes, "string length");
  const int32_t size = readInt32(data.data() + offset);
  BOLT_CHECK_GE(
      size,
      0,
      "Invalid CompactRow: negative string size {} at row {}, offset {}",
      size,
      row,
      offset);
  checkAvailable(
      data,
      row,
      offset + kSizeBytes,
      static_cast<size_t>(size),
      "string payload");
  StringView value(data.data() + offset + kSizeBytes, size);
  flatVector->set(index, value);
  return kSizeBytes + size;
}

VectorPtr deserializeUnknowns(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  return BaseVector::createNullConstant(UNKNOWN(), data.size(), pool);
}

// Deserializes one string from each 'row' in 'data'.
// Each strings starts at data[row].data() + offsets[row].
// string size | <string bytes>
// Advances the offsets past the strings.
VectorPtr deserializeStrings(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto flatVector =
      BaseVector::create<FlatVector<StringView>>(type, numRows, pool);

  auto* rawNulls = nulls->as<uint64_t>();

  for (auto i = 0; i < numRows; ++i) {
    if (bits::isBitNull(rawNulls, i)) {
      flatVector->setNull(i, true);
    } else {
      offsets[i] += readString(data[i], offsets[i], flatVector.get(), i, i);
    }
  }

  return flatVector;
}

VectorPtr deserializeUnknownArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();
  const auto total = totalSize(rawSizes, numRows);

  for (auto i = 0; i < numRows; ++i) {
    const auto numNullBytes = nullBytes(rawSizes[i]);
    offsets[i] += numNullBytes;
  }

  return BaseVector::createNullConstant(UNKNOWN(), total, pool);
}

// Deserializes multiple strings from each 'row' in 'data'.
// Each set of strings starts at data[row].data() + offsets[row] and contains
// null flags followed by the strings. The number of strings is provided in
// sizes[row].
// nulls | size-of-s1 | <s1> | size-of-s2 | <s2> |...
// Advances offsets past the last string.
VectorPtr deserializeStringArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  auto flatVector =
      BaseVector::create<FlatVector<StringView>>(type, total, pool);

  vector_size_t index = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      const auto numNullBytes = nullBytes(size);
      auto* rawElementNulls = readNulls(data[i].data() + offsets[i]);

      offsets[i] += numNullBytes;

      for (auto j = 0; j < size; ++j) {
        if (bits::isBitSet(rawElementNulls, j)) {
          flatVector->setNull(index++, true);
        } else {
          offsets[i] +=
              readString(data[i], offsets[i], flatVector.get(), index, i);
          ++index;
        }
      }
    }
  }

  return flatVector;
}

VectorPtr deserialize(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool);

// Deserializes multiple arrays from each 'row' in 'data'.
// Each set of arrays starts at data[row].data() + offsets[row] and contains
// null flags followed by the arrays. The number of arrays is provided in
// sizes[row].
// nulls | serializes size | offset-of-a1 | offset-of-a2 |...
// |size-of-a1 | nulls-of-a1-elements | <a1 elements> |...
//
// Advances offsets past the last array.
VectorPtr deserializeComplexArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  BufferPtr nulls = allocateNulls(total, pool);
  auto* rawNulls = nulls->asMutable<uint64_t>();

  std::vector<std::string_view> nestedData(total);
  std::vector<size_t> nestedOffsets(total, 0);

  vector_size_t nestedIndex = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      // Read nulls.
      const auto numNullBytes = nullBytes(size);
      auto* rawElementNulls = readNulls(data[i].data() + offsets[i]);
      offsets[i] += numNullBytes;

      // Read serialized size.
      const int32_t serializedSize = readInt32(data[i].data() + offsets[i]);
      BOLT_CHECK_GE(
          serializedSize,
          0,
          "Invalid CompactRow: negative complex array serialized size {} at "
          "row {}, offset {}",
          serializedSize,
          i,
          offsets[i]);
      offsets[i] += kSizeBytes;

      // Read offsets of individual elements.
      const auto offsetTableBytes = static_cast<size_t>(size) * kSizeBytes;
      BOLT_CHECK_GE(
          static_cast<size_t>(serializedSize),
          offsetTableBytes,
          "Invalid CompactRow: complex array serialized size {} is smaller "
          "than its {}-byte offset table at row {}",
          serializedSize,
          offsetTableBytes,
          i);
      checkAvailable(
          data, i, offsets[i], serializedSize, "complex array payload");
      auto buffer = data[i].data() + offsets[i];
      vector_size_t previousIndex = -1;
      int32_t previousOffset = 0;
      for (auto j = 0; j < size; ++j) {
        if (bits::isBitSet(rawElementNulls, j)) {
          bits::setNull(rawNulls, nestedIndex);
        } else {
          const int32_t nestedOffset = readInt32(buffer + j * kSizeBytes);
          BOLT_CHECK_GE(
              nestedOffset,
              static_cast<int32_t>(offsetTableBytes),
              "Invalid CompactRow: complex element offset {} points into its "
              "offset table at row {}, element {}",
              nestedOffset,
              i,
              j);
          BOLT_CHECK_LE(
              nestedOffset,
              serializedSize,
              "Invalid CompactRow: complex element offset {} exceeds payload "
              "size {} at row {}, element {}",
              nestedOffset,
              serializedSize,
              i,
              j);
          if (previousIndex >= 0) {
            BOLT_CHECK_GE(
                nestedOffset,
                previousOffset,
                "Invalid CompactRow: complex element offset {} precedes "
                "offset {} at row {}, element {}",
                nestedOffset,
                previousOffset,
                i,
                j);
            nestedData[previousIndex] =
                data[i].substr(0, offsets[i] + nestedOffset);
          }
          nestedOffsets[nestedIndex] = offsets[i] + nestedOffset;
          previousIndex = nestedIndex;
          previousOffset = nestedOffset;
        }
        ++nestedIndex;
      }
      if (previousIndex >= 0) {
        nestedData[previousIndex] =
            data[i].substr(0, offsets[i] + serializedSize);
      }

      offsets[i] += serializedSize;
    }
  }

  return deserialize(type, nestedData, nulls, nestedOffsets, pool);
}

// Deserializes one array from each 'row' in 'data'.
// Each array starts at data[row].data() + offsets[row].
// size | element nulls | serialized size (if complex type elements)
// | element offsets (if complex type elements) | e1 | e2 | e3 |...
//
// Advances the offsets past the arrays.
ArrayVectorPtr deserializeArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();

  auto* rawNulls = nulls->as<uint64_t>();

  BufferPtr arrayOffsets = allocateOffsets(numRows, pool);
  auto* rawArrayOffsets = arrayOffsets->asMutable<vector_size_t>();

  BufferPtr arraySizes = allocateSizes(numRows, pool);
  auto* rawArraySizes = arraySizes->asMutable<vector_size_t>();

  size_t arrayOffset = 0;
  const auto& elementType = type->childAt(0);

  for (auto i = 0; i < numRows; ++i) {
    if (!bits::isBitNull(rawNulls, i)) {
      // Read array size.
      checkAvailable(data, i, offsets[i], kSizeBytes, "array size");
      const int32_t size = readInt32(data[i].data() + offsets[i]);
      BOLT_CHECK_GE(
          size,
          0,
          "Invalid CompactRow: negative array size {} at row {}, offset {}",
          size,
          i,
          offsets[i]);
      offsets[i] += kSizeBytes;

      BOLT_CHECK_LE(
          static_cast<size_t>(size),
          static_cast<size_t>(std::numeric_limits<vector_size_t>::max()) -
              arrayOffset,
          "Invalid CompactRow: total array element count exceeds {} at row {}",
          std::numeric_limits<vector_size_t>::max(),
          i);
      rawArrayOffsets[i] = static_cast<vector_size_t>(arrayOffset);
      rawArraySizes[i] = size;
      arrayOffset += size;

      const auto numNullBytes = nullBytes(size);
      size_t minimumPayloadBytes = numNullBytes;
      if (!elementType->isUnKnown() && elementType->isFixedWidth()) {
        minimumPayloadBytes +=
            static_cast<size_t>(size) * fixedValueSize(elementType).value();
      } else if (
          size > 0 && elementType->kind() != TypeKind::VARCHAR &&
          elementType->kind() != TypeKind::VARBINARY &&
          !elementType->isUnKnown()) {
        minimumPayloadBytes +=
            kSizeBytes + static_cast<size_t>(size) * kSizeBytes;
      }
      checkAvailable(data, i, offsets[i], minimumPayloadBytes, "array payload");
    }
  }

  VectorPtr elements;
  if (elementType->isUnKnown()) {
    elements =
        deserializeUnknownArrays(elementType, data, arraySizes, offsets, pool);
  } else if (elementType->isFixedWidth()) {
    elements = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        deserializeFixedWidthArrays,
        elementType->kind(),
        elementType,
        data,
        arraySizes,
        offsets,
        pool);
  } else {
    switch (elementType->kind()) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        elements = deserializeStringArrays(
            elementType, data, arraySizes, offsets, pool);
        break;
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::ROW:
        elements = deserializeComplexArrays(
            elementType, data, arraySizes, offsets, pool);
        break;
      default:
        BOLT_UNREACHABLE("{}", elementType->toString());
    }
  }

  return std::make_shared<ArrayVector>(
      pool, type, nulls, numRows, arrayOffsets, arraySizes, elements);
}

// Deserializes one map from each 'row' in 'data'.
// Each map starts at data[row].data() + offsets[row].
// array-of-keys | array-of-values
// Advances the offsets past the maps.
VectorPtr deserializeMaps(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  auto arrayOfKeysType = ARRAY(type->childAt(0));
  auto arrayOfValuesType = ARRAY(type->childAt(1));
  auto arrayOfKeys =
      deserializeArrays(arrayOfKeysType, data, nulls, offsets, pool);
  auto arrayOfValues =
      deserializeArrays(arrayOfValuesType, data, nulls, offsets, pool);

  const auto* rawKeySizes = arrayOfKeys->rawSizes();
  const auto* rawValueSizes = arrayOfValues->rawSizes();
  for (auto i = 0; i < data.size(); ++i) {
    BOLT_CHECK_EQ(
        rawKeySizes[i],
        rawValueSizes[i],
        "Invalid CompactRow: map has {} keys but {} values at row {}",
        rawKeySizes[i],
        rawValueSizes[i],
        i);
  }

  return std::make_shared<MapVector>(
      pool,
      type,
      nulls,
      data.size(),
      arrayOfKeys->offsets(),
      arrayOfKeys->sizes(),
      arrayOfKeys->elements(),
      arrayOfValues->elements());
}

RowVectorPtr deserializeRows(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool);

// Switches on 'type' and calls type-specific deserialize method to deserialize
// one value from each 'row' in 'data' starting at the specified offset.
// Each value starts at data[row].data() + offsets[row].
// If 'type' is variable-width, advances 'offsets' past deserialized values.
// If 'type' is fixed-width, offsets remain unmodified.
VectorPtr deserialize(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto typeKind = type->kind();

  if (typeKind == TypeKind::UNKNOWN) {
    return deserializeUnknowns(type, data, nulls, offsets, pool);
  }

  if (type->isFixedWidth()) {
    return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        deserializeFixedWidth, typeKind, type, data, nulls, offsets, pool);
  }
  switch (typeKind) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return deserializeStrings(type, data, nulls, offsets, pool);
      break;
    case TypeKind::ARRAY:
      return deserializeArrays(type, data, nulls, offsets, pool);
      break;
    case TypeKind::MAP:
      return deserializeMaps(type, data, nulls, offsets, pool);
      break;
    case TypeKind::ROW:
      return deserializeRows(type, data, nulls, offsets, pool);
      break;
    default:
      BOLT_UNREACHABLE("{}", type->toString());
  }
}

// Deserializes one struct from each 'row' in 'data'.
// nulls | field1 | field2 |...
RowVectorPtr deserializeRows(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  const size_t numFields = type->size();

  std::vector<VectorPtr> fields;

  auto* rawNulls = nulls != nullptr ? nulls->as<uint64_t>() : nullptr;
  const size_t nullLength = nullBytes(numFields);
  if (nullLength > 0) {
    for (auto row = 0; row < numRows; ++row) {
      const auto isTopLevelNull =
          rawNulls != nullptr && bits::isBitNull(rawNulls, row);
      if (!isTopLevelNull) {
        checkAvailable(data, row, offsets[row], nullLength, "row null bitmap");
      }
    }
  }

  std::vector<BufferPtr> fieldNulls;
  fieldNulls.reserve(numFields);
  for (auto i = 0; i < numFields; ++i) {
    fieldNulls.emplace_back(allocateNulls(numRows, pool));
    auto* rawFieldNulls = fieldNulls.back()->asMutable<uint8_t>();
    for (auto row = 0; row < numRows; ++row) {
      const auto isTopLevelNull =
          rawNulls != nullptr && bits::isBitNull(rawNulls, row);
      const auto isNull = isTopLevelNull ||
          bits::isBitSet(readNulls(data[row].data() + offsets[row]), i);
      bits::setBit(rawFieldNulls, row, !isNull);
    }
  }

  for (auto row = 0; row < numRows; ++row) {
    if (rawNulls != nullptr && bits::isBitNull(rawNulls, row)) {
      continue;
    }
    offsets[row] += nullLength;
  }

  for (auto i = 0; i < numFields; ++i) {
    const auto& child = type->childAt(i);
    auto field = deserialize(child, data, fieldNulls[i], offsets, pool);
    // If 'field' is fixed-width, advance offsets for rows where top-level
    // struct is not null.
    if (auto numBytes = fixedValueSize(child)) {
      const auto* rawFieldNulls = fieldNulls[i]->as<uint64_t>();
      for (auto row = 0; row < numRows; ++row) {
        const auto isTopLevelNull =
            rawNulls != nullptr && bits::isBitNull(rawNulls, row);
        if (!isTopLevelNull) {
          if (bits::isBitNull(rawFieldNulls, row)) {
            checkAvailable(
                data,
                row,
                offsets[row],
                numBytes.value(),
                "null fixed-width row field");
          }
          offsets[row] += numBytes.value();
        }
      }
    }
    fields.emplace_back(std::move(field));
  }

  return std::make_shared<RowVector>(
      pool, type, nulls, numRows, std::move(fields));
}

} // namespace

// static
RowVectorPtr CompactRow::deserialize(
    const std::vector<std::string_view>& data,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  std::vector<size_t> offsets(numRows, 0);

  return deserializeRows(rowType, data, nullptr, offsets, pool);
}

} // namespace bytedance::bolt::row
