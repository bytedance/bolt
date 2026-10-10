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

#include "bolt/dwio/lance/NativeLanceLegacyStruct.h"

#include <limits>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLancePackedStruct.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using ArrayEncoding = ::lance::encodings::ArrayEncoding;
using ColumnMetadata = ::lance::file::v2::ColumnMetadata;

const ArrayEncoding& unwrapNoNullEncoding(
    const ArrayEncoding& encoding,
    std::string_view role) {
  if (encoding.array_encoding_case() != ArrayEncoding::kNullable) {
    return encoding;
  }
  BOLT_CHECK_EQ(
      encoding.nullable().nullability_case(),
      ::lance::encodings::Nullable::kNoNulls,
      "Native Lance fixed-layout reader requires non-null {} encoding",
      role);
  BOLT_CHECK(encoding.nullable().no_nulls().has_values());
  return encoding.nullable().no_nulls().values();
}

std::pair<std::string_view, uint32_t> fixedSizeListType(
    std::string_view logicalType) {
  constexpr auto kPrefix = std::string_view("fixed_size_list:");
  BOLT_CHECK_EQ(logicalType.rfind(kPrefix, 0), 0);
  const auto dimensionStart = logicalType.rfind(':');
  BOLT_CHECK_GT(dimensionStart, kPrefix.size());
  const auto dimension =
      std::stoul(std::string(logicalType.substr(dimensionStart + 1)));
  BOLT_CHECK_GT(dimension, 0);
  BOLT_CHECK_LE(dimension, std::numeric_limits<uint32_t>::max());
  return {
      logicalType.substr(kPrefix.size(), dimensionStart - kPrefix.size()),
      static_cast<uint32_t>(dimension)};
}

uint32_t fixedSizeBinaryWidth(std::string_view logicalType) {
  constexpr auto kPrefix = std::string_view("fixed_size_binary:");
  BOLT_CHECK_EQ(logicalType.rfind(kPrefix, 0), 0);
  const auto width =
      std::stoul(std::string(logicalType.substr(kPrefix.size())));
  BOLT_CHECK_GT(width, 0);
  BOLT_CHECK_LE(width, std::numeric_limits<uint32_t>::max());
  return static_cast<uint32_t>(width);
}

uint64_t fixedEncodingBitWidth(const ArrayEncoding& encoding) {
  switch (encoding.array_encoding_case()) {
    case ArrayEncoding::kFlat:
      return encoding.flat().bits_per_value();
    case ArrayEncoding::kBitpacked:
      return encoding.bitpacked().compressed_bits_per_value();
    case ArrayEncoding::kFixedSizeBinary:
      return static_cast<uint64_t>(encoding.fixed_size_binary().byte_width()) *
          8;
    case ArrayEncoding::kFixedSizeList:
      BOLT_CHECK(encoding.fixed_size_list().has_items());
      return static_cast<uint64_t>(encoding.fixed_size_list().dimension()) *
          fixedEncodingBitWidth(encoding.fixed_size_list().items());
    case ArrayEncoding::kNullable:
      return fixedEncodingBitWidth(
          unwrapNoNullEncoding(encoding, "packed child"));
    default:
      BOLT_UNSUPPORTED(
          "Encoding {} does not have a fixed bit width",
          static_cast<int>(encoding.array_encoding_case()));
  }
}

void setFixedSizeBinaryViews(
    const BufferPtr& values,
    uint32_t byteWidth,
    uint64_t count,
    VectorPtr& result) {
  auto* output = result->asFlatVector<StringView>();
  output->addStringBuffer(values);
  for (uint64_t i = 0; i < count; ++i) {
    output->setNoCopy(
        i,
        StringView(
            values->as<char>() + i * byteWidth,
            static_cast<int32_t>(byteWidth)));
  }
}

VectorPtr decodeFixedWidthLogical(
    const TypePtr& type,
    std::string_view logicalType,
    uint64_t bitsPerValue,
    const BufferPtr& values,
    uint64_t count,
    memory::MemoryPool& pool) {
  if (type->kind() == TypeKind::ARRAY) {
    const auto [itemLogicalType, dimension] = fixedSizeListType(logicalType);
    BOLT_CHECK_EQ(bitsPerValue % dimension, 0);
    auto child = decodeFixedWidthLogical(
        type->childAt(0),
        itemLogicalType,
        bitsPerValue / dimension,
        values,
        count * dimension,
        pool);
    auto offsets = allocateOffsets(count, &pool);
    auto sizes = allocateSizes(count, &pool);
    for (uint64_t row = 0; row < count; ++row) {
      offsets->asMutable<vector_size_t>()[row] = row * dimension;
      sizes->asMutable<vector_size_t>()[row] = dimension;
    }
    return std::make_shared<ArrayVector>(
        &pool,
        type,
        nullptr,
        count,
        std::move(offsets),
        std::move(sizes),
        std::move(child));
  }
  if (logicalType.rfind("fixed_size_binary:", 0) == 0 ||
      logicalType == "lance.bfloat16") {
    const auto byteWidth =
        logicalType == "lance.bfloat16" ? 2 : fixedSizeBinaryWidth(logicalType);
    BOLT_CHECK_EQ(bitsPerValue, byteWidth * 8);
    auto result = BaseVector::create(type, count, &pool);
    setFixedSizeBinaryViews(values, byteWidth, count, result);
    return result;
  }
  auto result = BaseVector::create(type, count, &pool);
  if (type->kind() == TypeKind::BOOLEAN) {
    BOLT_CHECK_EQ(bitsPerValue, 1);
    decodeLegacyBitmapValues(values->as<char>(), 0, count, 0, result);
  } else {
    decodeLegacyFixedWidthValues(
        type, logicalType, bitsPerValue, values->as<char>(), count, 0, result);
  }
  return result;
}

BufferPtr readFlatRange(
    const ::lance::encodings::Flat& flat,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t byteStart,
    uint64_t byteLength,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  BOLT_CHECK(flat.has_buffer());
  const auto descriptor = metadata.resolveBuffer(flat.buffer(), column, page);
  const auto compressed = flat.has_compression() &&
      !flat.compression().scheme().empty() &&
      flat.compression().scheme() != "none";
  if (!compressed) {
    return read(descriptor.offset + byteStart, byteLength);
  }
  return readCompressed(
      flat.compression().scheme(),
      descriptor.offset,
      descriptor.length,
      byteStart,
      byteLength);
}

VectorPtr decodeFixedSizePage(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  if (type->kind() != TypeKind::ARRAY) {
    if (isLegacyBinaryEncoding(encoding)) {
      return decodeLegacyBinaryPage(
          type,
          encoding,
          column,
          page,
          metadata,
          localStart,
          localCount,
          pool,
          read,
          readCompressed);
    }
    const auto* values = &encoding;
    if (encoding.array_encoding_case() == ArrayEncoding::kNullable &&
        encoding.nullable().nullability_case() !=
            ::lance::encodings::Nullable::kAllNulls) {
      values = encoding.nullable().nullability_case() ==
              ::lance::encodings::Nullable::kNoNulls
          ? &encoding.nullable().no_nulls().values()
          : &encoding.nullable().some_nulls().values();
    }
    if (values->array_encoding_case() == ArrayEncoding::kDictionary) {
      return decodeLegacyDictionaryPage(
          type,
          logicalType,
          *values,
          column,
          page,
          metadata,
          localStart,
          localCount,
          pool,
          read,
          readCompressed);
    }
    return decodeLegacyPrimitivePage(
        type,
        logicalType,
        encoding,
        column,
        page,
        metadata,
        localStart,
        localCount,
        pool,
        read,
        readCompressed);
  }

  const ArrayEncoding* fixedEncoding = &encoding;
  const ::lance::encodings::Nullable* nullable = nullptr;
  if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
    nullable = &encoding.nullable();
    if (nullable->nullability_case() ==
        ::lance::encodings::Nullable::kAllNulls) {
      return BaseVector::createNullConstant(type, localCount, &pool);
    }
    fixedEncoding =
        nullable->nullability_case() == ::lance::encodings::Nullable::kNoNulls
        ? &nullable->no_nulls().values()
        : &nullable->some_nulls().values();
  }
  BOLT_CHECK_EQ(
      fixedEncoding->array_encoding_case(), ArrayEncoding::kFixedSizeList);
  const auto& fixed = fixedEncoding->fixed_size_list();
  BOLT_CHECK_GT(fixed.dimension(), 0);
  BOLT_CHECK(fixed.has_items());
  const auto [itemLogicalType, dimension] = fixedSizeListType(logicalType);
  BOLT_CHECK_EQ(dimension, fixed.dimension());
  const auto firstItem = localStart * fixed.dimension();
  const auto numItems = localCount * fixed.dimension();
  auto elements = decodeFixedSizePage(
      type->childAt(0),
      itemLogicalType,
      fixed.items(),
      column,
      page,
      metadata,
      firstItem,
      numItems,
      pool,
      read,
      readCompressed);
  auto offsets = allocateOffsets(localCount, &pool);
  auto sizes = allocateSizes(localCount, &pool);
  for (uint64_t i = 0; i < localCount; ++i) {
    offsets->asMutable<vector_size_t>()[i] = i * fixed.dimension();
    sizes->asMutable<vector_size_t>()[i] = fixed.dimension();
  }
  auto result = std::make_shared<ArrayVector>(
      &pool,
      type,
      nullptr,
      localCount,
      std::move(offsets),
      std::move(sizes),
      std::move(elements));
  if (nullable != nullptr &&
      nullable->nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
    const auto& validity =
        unwrapNoNullEncoding(nullable->some_nulls().validity(), "validity")
            .flat();
    BOLT_CHECK_EQ(validity.bits_per_value(), 1);
    const auto firstByte = localStart / 8;
    const auto endByte = (localStart + localCount + 7) / 8;
    const auto bytes = readFlatRange(
        validity,
        column,
        page,
        metadata,
        firstByte,
        endByte - firstByte,
        read,
        readCompressed);
    applyLegacyValidityBitmap(
        bytes->as<uint8_t>(),
        localStart - firstByte * 8,
        localCount,
        0,
        *result);
  }
  return result;
}

} // namespace

VectorPtr decodeLegacyFixedSizeListPage(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  return decodeFixedSizePage(
      type,
      logicalType,
      encoding,
      column,
      page,
      metadata,
      localStart,
      localCount,
      pool,
      read,
      readCompressed);
}

VectorPtr decodeLegacyPackedStructPage(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint32_t physicalColumn,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read) {
  BOLT_CHECK_EQ(type->kind(), TypeKind::ROW);
  BOLT_CHECK_EQ(encoding.array_encoding_case(), ArrayEncoding::kPackedStruct);
  const auto& packed = encoding.packed_struct();
  BOLT_CHECK(packed.has_buffer());
  BOLT_CHECK_EQ(packed.inner_size(), type->size());
  const auto& logicalTypes =
      metadata.physicalColumnChildLogicalTypes(physicalColumn);
  BOLT_CHECK_EQ(logicalTypes.size(), type->size());

  std::vector<uint64_t> childWidths;
  childWidths.reserve(type->size());
  uint64_t rowWidth = 0;
  for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
    const auto& childEncoding =
        unwrapNoNullEncoding(packed.inner(childIndex), "packed struct child");
    const auto childBits = fixedEncodingBitWidth(childEncoding);
    BOLT_CHECK_EQ(childBits % 8, 0);
    const auto width = childBits / 8;
    BOLT_CHECK_GT(width, 0);
    childWidths.push_back(width);
    rowWidth += width;
  }

  const auto descriptor = metadata.resolveBuffer(packed.buffer(), column, page);
  BOLT_CHECK_LE(localStart, descriptor.length / rowWidth);
  BOLT_CHECK_LE(localCount, descriptor.length / rowWidth - localStart);
  const auto packedBytes =
      read(descriptor.offset + localStart * rowWidth, localCount * rowWidth);
  std::vector<VectorPtr> children;
  std::vector<BufferPtr> childBuffers;
  std::vector<NativeLancePackedStructColumn> packedColumns;
  children.reserve(type->size());
  childBuffers.reserve(type->size());
  packedColumns.reserve(type->size());
  uint64_t childOffset = 0;
  for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
    const auto width = childWidths[childIndex];
    auto childBytes = AlignedBuffer::allocate<char>(localCount * width, &pool);
    packedColumns.push_back(
        {childOffset, width, childBytes->asMutable<uint8_t>()});
    childBuffers.push_back(std::move(childBytes));
    childOffset += width;
  }
  BOLT_CHECK_EQ(childOffset, rowWidth);
  decodeLancePackedStruct(
      packedBytes->as<uint8_t>(),
      localCount,
      rowWidth,
      packedColumns.data(),
      packedColumns.size());
  for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
    children.push_back(decodeFixedWidthLogical(
        type->childAt(childIndex),
        logicalTypes[childIndex],
        childWidths[childIndex] * 8,
        childBuffers[childIndex],
        localCount,
        pool));
  }
  return std::make_shared<RowVector>(
      &pool, type, nullptr, localCount, std::move(children));
}

} // namespace bytedance::bolt::lance::reader
