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

#include "bolt/dwio/lance/NativeLanceLegacyList.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include <folly/lang/Bits.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceBitpack.h"
#include "bolt/dwio/lance/NativeLanceListOffsets.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using ArrayEncoding = ::lance::encodings::ArrayEncoding;
using ColumnMetadata = ::lance::file::v2::ColumnMetadata;

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
}

const ArrayEncoding& unwrapNoNullEncoding(
    const ArrayEncoding& encoding,
    std::string_view role) {
  if (encoding.array_encoding_case() != ArrayEncoding::kNullable) {
    return encoding;
  }
  BOLT_CHECK_EQ(
      encoding.nullable().nullability_case(),
      ::lance::encodings::Nullable::kNoNulls,
      "Native Lance list reader requires non-null {} encoding",
      role);
  BOLT_CHECK(encoding.nullable().no_nulls().has_values());
  return encoding.nullable().no_nulls().values();
}

const ::lance::encodings::Flat& requireFlat(
    const ArrayEncoding& encoding,
    std::string_view role) {
  const auto& values = unwrapNoNullEncoding(encoding, role);
  BOLT_CHECK_EQ(
      values.array_encoding_case(),
      ArrayEncoding::kFlat,
      "Native Lance list reader requires Flat {} encoding",
      role);
  BOLT_CHECK(values.flat().has_buffer());
  return values.flat();
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
  const auto descriptor = metadata.resolveBuffer(flat.buffer(), column, page);
  const auto compressed = flat.has_compression() &&
      !flat.compression().scheme().empty() &&
      flat.compression().scheme() != "none";
  if (!compressed) {
    BOLT_CHECK_LE(byteStart, descriptor.length);
    BOLT_CHECK_LE(byteLength, descriptor.length - byteStart);
    return read(descriptor.offset + byteStart, byteLength);
  }
  return readCompressed(
      flat.compression().scheme(),
      descriptor.offset,
      descriptor.length,
      byteStart,
      byteLength);
}

BufferPtr readUInt64Encoding(
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    std::string_view role,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  const auto& values = unwrapNoNullEncoding(encoding, role);
  BOLT_CHECK_LE(
      localStart, std::numeric_limits<uint64_t>::max() / sizeof(uint64_t));
  BOLT_CHECK_LE(
      localCount,
      std::numeric_limits<uint64_t>::max() / sizeof(uint64_t) - localStart);
  if (values.array_encoding_case() == ArrayEncoding::kFlat) {
    const auto& flat = requireFlat(values, role);
    BOLT_CHECK_EQ(
        flat.bits_per_value(), 64, "Lance {} must contain UInt64 values", role);
    return readFlatRange(
        flat,
        column,
        page,
        metadata,
        localStart * sizeof(uint64_t),
        localCount * sizeof(uint64_t),
        read,
        readCompressed);
  }

  auto result = AlignedBuffer::allocate<uint64_t>(localCount, &pool);
  auto* output = reinterpret_cast<uint8_t*>(result->asMutable<uint64_t>());
  if (values.array_encoding_case() == ArrayEncoding::kBitpackedForNonNeg) {
    constexpr uint64_t kChunkRows = 1'024;
    const auto& bitpacked = values.bitpacked_for_non_neg();
    const auto compressedBits = bitpacked.compressed_bits_per_value();
    BOLT_CHECK_EQ(bitpacked.uncompressed_bits_per_value(), 64);
    BOLT_CHECK_LE(compressedBits, 64);
    if (compressedBits == 0) {
      std::memset(output, 0, result->size());
      return result;
    }
    BOLT_CHECK(bitpacked.has_buffer());
    const auto descriptor =
        metadata.resolveBuffer(bitpacked.buffer(), column, page);
    const auto chunkBytes = kChunkRows * compressedBits / 8;
    const auto firstChunk = localStart / kChunkRows;
    const auto endChunk =
        (localStart + localCount + kChunkRows - 1) / kChunkRows;
    BOLT_CHECK_LE(endChunk * chunkBytes, descriptor.length);
    const auto packed = read(
        descriptor.offset + firstChunk * chunkBytes,
        (endChunk - firstChunk) * chunkBytes);
    decodeLanceBitpackedForNonNeg(
        packed->as<uint8_t>(),
        packed->size(),
        localStart - firstChunk * kChunkRows,
        localCount,
        compressedBits,
        64,
        output);
    return result;
  }

  BOLT_CHECK_EQ(values.array_encoding_case(), ArrayEncoding::kBitpacked);
  const auto& bitpacked = values.bitpacked();
  BOLT_CHECK_EQ(bitpacked.uncompressed_bits_per_value(), 64);
  BOLT_CHECK(!bitpacked.signed_());
  const auto compressedBits = bitpacked.compressed_bits_per_value();
  BOLT_CHECK_LE(compressedBits, 64);
  if (compressedBits == 0) {
    std::memset(output, 0, result->size());
    return result;
  }
  BOLT_CHECK(bitpacked.has_buffer());
  const auto descriptor =
      metadata.resolveBuffer(bitpacked.buffer(), column, page);
  const auto firstBit = localStart * compressedBits;
  const auto endBit = (localStart + localCount) * compressedBits;
  const auto firstByte = firstBit / 8;
  const auto endByte = (endBit + 7) / 8;
  BOLT_CHECK_LE(endByte, descriptor.length);
  const auto packed = read(descriptor.offset + firstByte, endByte - firstByte);
  decodeLanceBitpacked(
      packed->as<uint8_t>(),
      packed->size(),
      firstBit - firstByte * 8,
      localCount,
      compressedBits,
      64,
      false,
      output);
  return result;
}

struct ListPageLayout {
  uint64_t firstItem;
  uint64_t numItems;
  BufferPtr nulls;
  BufferPtr offsets;
  BufferPtr sizes;
};

ListPageLayout readListPageLayout(
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    uint64_t localStart,
    uint64_t localCount,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  const auto& list = encoding.list();
  BOLT_CHECK(list.has_offsets());
  BOLT_CHECK_GT(list.null_offset_adjustment(), 0);
  const auto firstIndex = localStart == 0 ? 0 : localStart - 1;
  const auto numIndices = localCount + (localStart == 0 ? 0 : 1);
  const auto encodedIndices = readUInt64Encoding(
      list.offsets(),
      column,
      page,
      metadata,
      firstIndex,
      numIndices,
      "list offsets",
      pool,
      read,
      readCompressed);
  const auto* rawIndices = encodedIndices->as<char>();
  const auto firstItem = localStart == 0
      ? 0
      : readLittleEndian<uint64_t>(rawIndices) % list.null_offset_adjustment();
  auto offsets = allocateOffsets(localCount, &pool);
  auto sizes = allocateSizes(localCount, &pool);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  BufferPtr nulls;
  const auto indexShift = localStart == 0 ? 0 : 1;
  const auto* encodedEnds = rawIndices + indexShift * sizeof(uint64_t);
  auto decoded = decodeLanceListOffsets(
      encodedEnds,
      localCount,
      list.null_offset_adjustment(),
      firstItem,
      rawOffsets,
      rawSizes,
      nullptr);
  if (decoded.hasNulls) {
    nulls = allocateNulls(localCount, &pool);
    decoded = decodeLanceListOffsets(
        encodedEnds,
        localCount,
        list.null_offset_adjustment(),
        firstItem,
        rawOffsets,
        rawSizes,
        nulls->asMutable<uint64_t>());
  }
  return {
      firstItem,
      decoded.lastItem - firstItem,
      std::move(nulls),
      std::move(offsets),
      std::move(sizes)};
}

BufferPtr readFlatByteRange(
    uint32_t physicalColumn,
    uint64_t rowStart,
    uint64_t rowCount,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  auto output = AlignedBuffer::allocate<char>(rowCount, &pool);
  if (rowCount == 0) {
    return output;
  }
  const auto& column = metadata.column(physicalColumn);
  uint64_t pageRowStart = 0;
  uint64_t outputOffset = 0;
  const auto rowEnd = rowStart + rowCount;
  for (int32_t pageIndex = 0; pageIndex < column.pages_size(); ++pageIndex) {
    const auto& page = column.pages(pageIndex);
    if (page.length() == 0) {
      continue;
    }
    const auto pageRowEnd = pageRowStart + page.length();
    const auto overlapStart = std::max(rowStart, pageRowStart);
    const auto overlapEnd = std::min(rowEnd, pageRowEnd);
    if (overlapStart < overlapEnd) {
      const auto& encoding = metadata.pageEncoding(physicalColumn, pageIndex);
      const auto& flat = requireFlat(encoding, "binary bytes");
      BOLT_CHECK_EQ(flat.bits_per_value(), 8);
      const auto localStart = overlapStart - pageRowStart;
      const auto localCount = overlapEnd - overlapStart;
      const auto bytes = readFlatRange(
          flat,
          column,
          page,
          metadata,
          localStart,
          localCount,
          read,
          readCompressed);
      std::memcpy(
          output->asMutable<char>() + outputOffset,
          bytes->as<char>(),
          localCount);
      outputOffset += localCount;
    }
    pageRowStart = pageRowEnd;
  }
  BOLT_CHECK_EQ(outputOffset, rowCount);
  return output;
}

} // namespace

VectorPtr decodeLegacyListPage(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    uint32_t physicalColumn,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t itemsOffset,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed,
    const NativeLancePrefetchPhysicalColumn& prefetchChild,
    const NativeLanceDecodePhysicalColumn& decodeChild) {
  BOLT_CHECK_EQ(encoding.array_encoding_case(), ArrayEncoding::kList);
  auto layout = readListPageLayout(
      encoding,
      column,
      page,
      metadata,
      pool,
      localStart,
      localCount,
      read,
      readCompressed);
  const auto childPhysicalColumn = physicalColumn + 1;

  if (type->kind() == TypeKind::VARCHAR ||
      type->kind() == TypeKind::VARBINARY) {
    auto result = BaseVector::create(type, localCount, &pool);
    auto stringBytes = readFlatByteRange(
        childPhysicalColumn,
        itemsOffset + layout.firstItem,
        layout.numItems,
        metadata,
        pool,
        read,
        readCompressed);
    auto* output = result->asFlatVector<StringView>();
    output->addStringBuffer(stringBytes);
    const auto* rawOffsets = layout.offsets->as<vector_size_t>();
    const auto* rawSizes = layout.sizes->as<vector_size_t>();
    for (uint64_t row = 0; row < localCount; ++row) {
      if (layout.nulls != nullptr &&
          bits::isBitNull(layout.nulls->as<uint64_t>(), row)) {
        result->setNull(row, true);
        continue;
      }
      output->setNoCopy(
          row,
          StringView(stringBytes->as<char>() + rawOffsets[row], rawSizes[row]));
    }
    return result;
  }

  if (type->kind() == TypeKind::ARRAY) {
    prefetchChild(
        type->childAt(0),
        childPhysicalColumn,
        itemsOffset + layout.firstItem,
        layout.numItems);
    auto elements = decodeChild(
        type->childAt(0),
        metadata.physicalColumnLogicalType(childPhysicalColumn),
        childPhysicalColumn,
        itemsOffset + layout.firstItem,
        layout.numItems);
    return std::make_shared<ArrayVector>(
        &pool,
        type,
        std::move(layout.nulls),
        localCount,
        std::move(layout.offsets),
        std::move(layout.sizes),
        std::move(elements));
  }

  BOLT_CHECK_EQ(type->kind(), TypeKind::MAP);
  const auto entriesType =
      ROW({"key", "value"}, {type->childAt(0), type->childAt(1)});
  prefetchChild(
      entriesType,
      childPhysicalColumn,
      itemsOffset + layout.firstItem,
      layout.numItems);
  auto entries = decodeChild(
      entriesType,
      metadata.physicalColumnLogicalType(childPhysicalColumn),
      childPhysicalColumn,
      itemsOffset + layout.firstItem,
      layout.numItems);
  const auto* entryRows = entries->as<RowVector>();
  BOLT_CHECK_NOT_NULL(
      entryRows, "Native Lance map entries must decode as a RowVector");
  BOLT_CHECK_EQ(entryRows->childrenSize(), 2);
  return std::make_shared<MapVector>(
      &pool,
      type,
      std::move(layout.nulls),
      localCount,
      std::move(layout.offsets),
      std::move(layout.sizes),
      entryRows->childAt(0),
      entryRows->childAt(1));
}

} // namespace bytedance::bolt::lance::reader
