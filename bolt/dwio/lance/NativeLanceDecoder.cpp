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

#include "bolt/dwio/lance/NativeLanceDecoder.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <functional>
#include <limits>

#include <folly/Portability.h>
#include <folly/lang/Bits.h>
#include <google/protobuf/any.pb.h>
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceBitmap.h"
#include "bolt/dwio/lance/NativeLanceBitpack.h"
#include "bolt/dwio/lance/NativeLanceColumnCursor.h"
#include "bolt/dwio/lance/NativeLanceDecompressor.h"
#include "bolt/dwio/lance/NativeLanceLegacyScalar.h"
#include "bolt/dwio/lance/NativeLanceListOffsets.h"
#include "bolt/dwio/lance/NativeLancePackedStruct.h"
#include "bolt/dwio/lance/NativeLancePageReader.h"
#include "bolt/dwio/lance/NativeLanceStructuralDecoder.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_0.pb.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using ArrayEncoding = ::lance::encodings::ArrayEncoding;
using BufferDescriptor = NativeLanceMetadata::BufferDescriptor;
using ColumnMetadata = ::lance::file::v2::ColumnMetadata;

class DecodeInput {
 public:
  using RawRead = std::function<BufferPtr(uint64_t, uint64_t)>;
  using CompressedRead = std::function<
      BufferPtr(std::string_view, uint64_t, uint64_t, uint64_t, uint64_t)>;

  DecodeInput(RawRead rawRead, CompressedRead compressedRead)
      : rawRead_(std::move(rawRead)),
        compressedRead_(std::move(compressedRead)) {}

  BufferPtr operator()(uint64_t offset, uint64_t length) const {
    return rawRead_(offset, length);
  }

  operator const RawRead&() const {
    return rawRead_;
  }

  const RawRead& rawRead() const {
    return rawRead_;
  }

  const CompressedRead& compressedRead() const {
    return compressedRead_;
  }

  BufferPtr compressedRange(
      std::string_view scheme,
      BufferDescriptor buffer,
      uint64_t decodedOffset,
      uint64_t decodedLength) const {
    return compressedRead_(
        scheme, buffer.offset, buffer.length, decodedOffset, decodedLength);
  }

 private:
  RawRead rawRead_;
  CompressedRead compressedRead_;
};

template <typename T>
T readLittleEndian(const char* data);

const ArrayEncoding& unwrapNoNullEncoding(
    const ArrayEncoding& encoding,
    std::string_view role);
const ::lance::encodings::Flat& requireFlat(
    const ArrayEncoding& encoding,
    std::string_view role);
bool isCompressed(const ::lance::encodings::Flat& flat);
bool hasCompressedFlatBuffer(const ArrayEncoding& encoding);

bool requiresDeferredRead(const TypePtr& type) {
  if (type->kind() == TypeKind::VARCHAR ||
      type->kind() == TypeKind::VARBINARY || type->kind() == TypeKind::ARRAY ||
      type->kind() == TypeKind::MAP) {
    return true;
  }
  if (type->kind() == TypeKind::ROW) {
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      if (requiresDeferredRead(type->childAt(childIndex))) {
        return true;
      }
    }
  }
  return false;
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
      BOLT_CHECK_EQ(
          encoding.nullable().nullability_case(),
          ::lance::encodings::Nullable::kNoNulls,
          "Packed encoding cannot contain nullable values");
      return fixedEncodingBitWidth(encoding.nullable().no_nulls().values());
    default:
      BOLT_UNSUPPORTED(
          "Encoding {} does not have a fixed bit width",
          static_cast<int>(encoding.array_encoding_case()));
  }
}

void enqueueEncodingRanges(
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t elementStart,
    uint64_t elementCount,
    const std::function<void(uint64_t, uint64_t)>& enqueue,
    const std::function<void(std::string_view, BufferDescriptor)>&
        enqueueCompressed) {
  auto enqueueBuffer = [&](const ::lance::encodings::Buffer& encodedBuffer,
                           uint64_t bitWidth,
                           uint64_t start,
                           uint64_t count,
                           bool compressed) {
    BOLT_CHECK_GT(bitWidth, 0);
    const auto buffer = metadata.resolveBuffer(encodedBuffer, column, page);
    if (compressed) {
      if (buffer.length > 0) {
        enqueueCompressed(encoding.flat().compression().scheme(), buffer);
      }
      return;
    }
    BOLT_CHECK_LE(start, std::numeric_limits<uint64_t>::max() / bitWidth);
    BOLT_CHECK_LE(
        count,
        (std::numeric_limits<uint64_t>::max() - start * bitWidth) / bitWidth);
    const auto firstBit = start * bitWidth;
    const auto endBit = (start + count) * bitWidth;
    const auto firstByte = firstBit / 8;
    const auto endByte = (endBit + 7) / 8;
    BOLT_CHECK_LE(firstByte, buffer.length);
    BOLT_CHECK_LE(endByte, buffer.length);
    if (endByte > firstByte) {
      enqueue(buffer.offset + firstByte, endByte - firstByte);
    }
  };

  switch (encoding.array_encoding_case()) {
    case ArrayEncoding::kFlat: {
      const auto& flat = requireFlat(encoding, "prefetch");
      enqueueBuffer(
          flat.buffer(),
          flat.bits_per_value(),
          elementStart,
          elementCount,
          isCompressed(flat));
      return;
    }
    case ArrayEncoding::kBitpacked: {
      const auto& bitpacked = encoding.bitpacked();
      if (bitpacked.compressed_bits_per_value() == 0) {
        return;
      }
      BOLT_CHECK(bitpacked.has_buffer());
      enqueueBuffer(
          bitpacked.buffer(),
          bitpacked.compressed_bits_per_value(),
          elementStart,
          elementCount,
          false);
      return;
    }
    case ArrayEncoding::kBitpackedForNonNeg: {
      constexpr uint64_t kChunkRows = 1'024;
      const auto& bitpacked = encoding.bitpacked_for_non_neg();
      if (bitpacked.compressed_bits_per_value() == 0) {
        return;
      }
      BOLT_CHECK(bitpacked.has_buffer());
      const auto descriptor =
          metadata.resolveBuffer(bitpacked.buffer(), column, page);
      const auto chunkBytes =
          kChunkRows * bitpacked.compressed_bits_per_value() / 8;
      const auto firstChunk = elementStart / kChunkRows;
      const auto endChunk =
          (elementStart + elementCount + kChunkRows - 1) / kChunkRows;
      BOLT_CHECK_LE(firstChunk * chunkBytes, descriptor.length);
      BOLT_CHECK_LE(endChunk * chunkBytes, descriptor.length);
      if (endChunk > firstChunk) {
        enqueue(
            descriptor.offset + firstChunk * chunkBytes,
            (endChunk - firstChunk) * chunkBytes);
      }
      return;
    }
    case ArrayEncoding::kNullable: {
      const auto& nullable = encoding.nullable();
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
        enqueueEncodingRanges(
            nullable.some_nulls().validity(),
            column,
            page,
            metadata,
            elementStart,
            elementCount,
            enqueue,
            enqueueCompressed);
        enqueueEncodingRanges(
            nullable.some_nulls().values(),
            column,
            page,
            metadata,
            elementStart,
            elementCount,
            enqueue,
            enqueueCompressed);
      } else if (
          nullable.nullability_case() ==
          ::lance::encodings::Nullable::kNoNulls) {
        enqueueEncodingRanges(
            nullable.no_nulls().values(),
            column,
            page,
            metadata,
            elementStart,
            elementCount,
            enqueue,
            enqueueCompressed);
      }
      return;
    }
    case ArrayEncoding::kBinary: {
      const auto firstIndex = elementStart == 0 ? 0 : elementStart - 1;
      enqueueEncodingRanges(
          encoding.binary().indices(),
          column,
          page,
          metadata,
          firstIndex,
          elementCount + (elementStart == 0 ? 0 : 1),
          enqueue,
          enqueueCompressed);
      return;
    }
    case ArrayEncoding::kList: {
      const auto firstIndex = elementStart == 0 ? 0 : elementStart - 1;
      enqueueEncodingRanges(
          encoding.list().offsets(),
          column,
          page,
          metadata,
          firstIndex,
          elementCount + (elementStart == 0 ? 0 : 1),
          enqueue,
          enqueueCompressed);
      return;
    }
    case ArrayEncoding::kDictionary:
      enqueueEncodingRanges(
          encoding.dictionary().indices(),
          column,
          page,
          metadata,
          elementStart,
          elementCount,
          enqueue,
          enqueueCompressed);
      enqueueEncodingRanges(
          encoding.dictionary().items(),
          column,
          page,
          metadata,
          0,
          encoding.dictionary().num_dictionary_items(),
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kFsst:
      enqueueEncodingRanges(
          encoding.fsst().binary(),
          column,
          page,
          metadata,
          elementStart,
          elementCount,
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kFixedSizeBinary:
      enqueueEncodingRanges(
          encoding.fixed_size_binary().bytes(),
          column,
          page,
          metadata,
          elementStart,
          elementCount,
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kFixedSizeList:
      enqueueEncodingRanges(
          encoding.fixed_size_list().items(),
          column,
          page,
          metadata,
          elementStart * encoding.fixed_size_list().dimension(),
          elementCount * encoding.fixed_size_list().dimension(),
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kPackedStruct: {
      BOLT_CHECK(encoding.packed_struct().has_buffer());
      uint64_t bitsPerRow = 0;
      for (const auto& child : encoding.packed_struct().inner()) {
        bitsPerRow += fixedEncodingBitWidth(child);
      }
      enqueueBuffer(
          encoding.packed_struct().buffer(),
          bitsPerRow,
          elementStart,
          elementCount,
          false);
      return;
    }
    case ArrayEncoding::kStruct:
      return;
    default:
      // Unsupported encodings are rejected by the decoder with a precise
      // error. Avoid turning a best-effort prefetch into an earlier failure.
      return;
  }
}

const ::lance::encodings::Flat& requireFlat(
    const ArrayEncoding& encoding,
    std::string_view role) {
  BOLT_CHECK_EQ(
      encoding.array_encoding_case(),
      ArrayEncoding::kFlat,
      "Native Lance fixed-width decoder requires Flat {} encoding",
      role);
  const auto& flat = encoding.flat();
  BOLT_CHECK(flat.has_buffer(), "Lance Flat {} encoding has no buffer", role);
  return flat;
}

bool isCompressed(const ::lance::encodings::Flat& flat) {
  return flat.has_compression() && !flat.compression().scheme().empty() &&
      flat.compression().scheme() != "none";
}

bool hasCompressedFlatBuffer(const ArrayEncoding& encoding) {
  switch (encoding.array_encoding_case()) {
    case ArrayEncoding::kFlat:
      return isCompressed(encoding.flat());
    case ArrayEncoding::kNullable: {
      const auto& nullable = encoding.nullable();
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
        return hasCompressedFlatBuffer(nullable.some_nulls().validity()) ||
            hasCompressedFlatBuffer(nullable.some_nulls().values());
      }
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kNoNulls) {
        return hasCompressedFlatBuffer(nullable.no_nulls().values());
      }
      return false;
    }
    case ArrayEncoding::kBinary:
      return hasCompressedFlatBuffer(encoding.binary().indices()) ||
          hasCompressedFlatBuffer(encoding.binary().bytes());
    case ArrayEncoding::kList:
      return hasCompressedFlatBuffer(encoding.list().offsets());
    case ArrayEncoding::kDictionary:
      return hasCompressedFlatBuffer(encoding.dictionary().indices()) ||
          hasCompressedFlatBuffer(encoding.dictionary().items());
    case ArrayEncoding::kFsst:
      return hasCompressedFlatBuffer(encoding.fsst().binary());
    case ArrayEncoding::kFixedSizeBinary:
      return hasCompressedFlatBuffer(encoding.fixed_size_binary().bytes());
    case ArrayEncoding::kFixedSizeList:
      return hasCompressedFlatBuffer(encoding.fixed_size_list().items());
    case ArrayEncoding::kPackedStruct:
      return false;
    case ArrayEncoding::kStruct:
      return false;
    default:
      return false;
  }
}

BufferPtr readFlatRange(
    const ::lance::encodings::Flat& flat,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t byteStart,
    uint64_t byteLength,
    memory::MemoryPool& pool,
    const DecodeInput& read) {
  const auto descriptor = metadata.resolveBuffer(flat.buffer(), column, page);
  if (!isCompressed(flat)) {
    BOLT_CHECK_LE(byteStart, descriptor.length);
    BOLT_CHECK_LE(byteLength, descriptor.length - byteStart);
    return read(descriptor.offset + byteStart, byteLength);
  }

  return read.compressedRange(
      flat.compression().scheme(), descriptor, byteStart, byteLength);
}

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
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

void setFixedSizeBinaryViews(
    const BufferPtr& values,
    uint32_t byteWidth,
    uint64_t count,
    uint64_t outputOffset,
    VectorPtr& result) {
  BOLT_CHECK(
      result->typeKind() == TypeKind::VARCHAR ||
      result->typeKind() == TypeKind::VARBINARY);
  auto* output = result->asFlatVector<StringView>();
  output->addStringBuffer(values);
  for (uint64_t i = 0; i < count; ++i) {
    output->setNoCopy(
        outputOffset + i,
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
    const auto child = decodeFixedWidthLogical(
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
        child);
  }
  if (logicalType.rfind("fixed_size_binary:", 0) == 0 ||
      logicalType == "lance.bfloat16") {
    const auto byteWidth =
        logicalType == "lance.bfloat16" ? 2 : fixedSizeBinaryWidth(logicalType);
    BOLT_CHECK_EQ(bitsPerValue, byteWidth * 8);
    auto result = BaseVector::create(type, count, &pool);
    setFixedSizeBinaryViews(values, byteWidth, count, 0, result);
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

VectorPtr decodePageEncoding(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const DecodeInput& read);

VectorPtr decodeFixedSizeListValues(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const DecodeInput& read) {
  BOLT_CHECK_EQ(type->kind(), TypeKind::ARRAY);
  const ArrayEncoding* fixedEncoding = &encoding;
  const ::lance::encodings::Nullable* nullable = nullptr;
  if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
    nullable = &encoding.nullable();
    if (nullable->nullability_case() ==
        ::lance::encodings::Nullable::kAllNulls) {
      auto offsets = allocateOffsets(localCount, &pool);
      auto sizes = allocateSizes(localCount, &pool);
      auto nulls = allocateNulls(localCount, &pool, bits::kNull);
      return std::make_shared<ArrayVector>(
          &pool,
          type,
          std::move(nulls),
          localCount,
          std::move(offsets),
          std::move(sizes),
          BaseVector::create(type->childAt(0), 0, &pool));
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
  auto elements = decodePageEncoding(
      type->childAt(0),
      itemLogicalType,
      fixed.items(),
      column,
      page,
      metadata,
      firstItem,
      numItems,
      pool,
      read);
  auto offsets = allocateOffsets(localCount, &pool);
  auto sizes = allocateSizes(localCount, &pool);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  for (uint64_t i = 0; i < localCount; ++i) {
    rawOffsets[i] = static_cast<vector_size_t>(i * fixed.dimension());
    rawSizes[i] = static_cast<vector_size_t>(fixed.dimension());
  }
  auto result = std::make_shared<ArrayVector>(
      &pool, type, nullptr, localCount, offsets, sizes, std::move(elements));
  if (nullable != nullptr &&
      nullable->nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
    const auto& validity =
        requireFlat(nullable->some_nulls().validity(), "validity");
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
        pool,
        read);
    applyLegacyValidityBitmap(
        bytes->as<uint8_t>(),
        localStart - firstByte * 8,
        localCount,
        0,
        *result);
  }
  return result;
}

VectorPtr decodePackedStructValues(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint32_t physicalColumnIndex,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const DecodeInput& read) {
  BOLT_CHECK_EQ(type->kind(), TypeKind::ROW);
  BOLT_CHECK_EQ(encoding.array_encoding_case(), ArrayEncoding::kPackedStruct);
  const auto& packed = encoding.packed_struct();
  BOLT_CHECK(packed.has_buffer());
  BOLT_CHECK_EQ(packed.inner_size(), type->size());
  const auto& logicalTypes =
      metadata.physicalColumnChildLogicalTypes(physicalColumnIndex);
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
    BOLT_CHECK_LE(rowWidth, std::numeric_limits<uint64_t>::max() - width);
    rowWidth += width;
  }

  const auto descriptor = metadata.resolveBuffer(packed.buffer(), column, page);
  BOLT_CHECK_LE(localStart, descriptor.length / rowWidth);
  BOLT_CHECK_LE(localCount, descriptor.length / rowWidth - localStart);
  const auto packedBytes =
      read(descriptor.offset + localStart * rowWidth, localCount * rowWidth);
  std::vector<VectorPtr> children;
  children.reserve(type->size());
  std::vector<BufferPtr> childBuffers;
  childBuffers.reserve(type->size());
  std::vector<NativeLancePackedStructColumn> packedColumns;
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
    const auto width = childWidths[childIndex];
    const auto& childType = type->childAt(childIndex);
    const auto childLogicalType = std::string_view(logicalTypes[childIndex]);
    auto child = decodeFixedWidthLogical(
        childType,
        childLogicalType,
        width * 8,
        childBuffers[childIndex],
        localCount,
        pool);
    children.push_back(std::move(child));
  }
  return std::make_shared<RowVector>(
      &pool,
      type,
      nullptr,
      static_cast<vector_size_t>(localCount),
      std::move(children));
}

struct BlobDescriptor {
  uint64_t position;
  uint64_t size;
};

void decodeBlobDescriptions(
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    const DecodeInput& read,
    BlobDescriptor* output) {
  BOLT_CHECK_EQ(encoding.array_encoding_case(), ArrayEncoding::kPackedStruct);
  const auto& packed = encoding.packed_struct();
  BOLT_CHECK_EQ(packed.inner_size(), 2);
  BOLT_CHECK(packed.has_buffer());
  for (const auto& child : packed.inner()) {
    BOLT_CHECK_EQ(
        fixedEncodingBitWidth(unwrapNoNullEncoding(child, "Blob descriptor")),
        64);
  }
  constexpr uint64_t kDescriptorBytes = 2 * sizeof(uint64_t);
  const auto descriptor = metadata.resolveBuffer(packed.buffer(), column, page);
  BOLT_CHECK_LE(localStart, descriptor.length / kDescriptorBytes);
  BOLT_CHECK_LE(localCount, descriptor.length / kDescriptorBytes - localStart);
  const auto bytes = read(
      descriptor.offset + localStart * kDescriptorBytes,
      localCount * kDescriptorBytes);
  for (uint64_t row = 0; row < localCount; ++row) {
    const auto* source = bytes->as<char>() + row * kDescriptorBytes;
    output[outputOffset + row] = {
        readLittleEndian<uint64_t>(source),
        readLittleEndian<uint64_t>(source + sizeof(uint64_t))};
  }
}

uint64_t normalizedStringOffset(uint64_t encoded, uint64_t nullAdjustment) {
  return encoded >= nullAdjustment ? encoded - nullAdjustment : encoded;
}

const ::lance::encodings::Flat& requireNoNullFlat(
    const ArrayEncoding& encoding,
    std::string_view role) {
  if (encoding.array_encoding_case() == ArrayEncoding::kFlat) {
    return requireFlat(encoding, role);
  }
  BOLT_CHECK_EQ(
      encoding.array_encoding_case(),
      ArrayEncoding::kNullable,
      "Native Lance reader requires Flat or Nullable(NoNull(Flat)) {} encoding",
      role);
  BOLT_CHECK_EQ(
      encoding.nullable().nullability_case(),
      ::lance::encodings::Nullable::kNoNulls,
      "Native Lance reader requires non-null {} values",
      role);
  BOLT_CHECK(encoding.nullable().no_nulls().has_values());
  return requireFlat(encoding.nullable().no_nulls().values(), role);
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
    const DecodeInput& read) {
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
        pool,
        read);
  }

  auto result = AlignedBuffer::allocate<uint64_t>(localCount, &pool);
  auto* output = reinterpret_cast<uint8_t*>(result->asMutable<uint64_t>());
  if (values.array_encoding_case() == ArrayEncoding::kBitpackedForNonNeg) {
    constexpr uint64_t kChunkRows = 1'024;
    const auto& bitpacked = values.bitpacked_for_non_neg();
    const auto compressedBits = bitpacked.compressed_bits_per_value();
    BOLT_CHECK_EQ(
        bitpacked.uncompressed_bits_per_value(),
        64,
        "Lance {} must decode to UInt64 values",
        role);
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
    BOLT_CHECK_LE(firstChunk * chunkBytes, descriptor.length);
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

  BOLT_CHECK_EQ(
      values.array_encoding_case(),
      ArrayEncoding::kBitpacked,
      "Native Lance reader requires Flat, Bitpacked, or "
      "BitpackedForNonNeg {} encoding",
      role);
  const auto& bitpacked = values.bitpacked();
  BOLT_CHECK_EQ(
      bitpacked.uncompressed_bits_per_value(),
      64,
      "Lance {} must decode to UInt64 values",
      role);
  BOLT_CHECK(!bitpacked.signed_(), "Lance {} cannot be signed", role);
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

BufferPtr readFlatByteRange(
    uint32_t physicalColumnIndex,
    uint64_t rowStart,
    uint64_t rowCount,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    const DecodeInput& read) {
  auto output = AlignedBuffer::allocate<char>(rowCount, &pool);
  if (rowCount == 0) {
    return output;
  }

  const auto& column = metadata.column(physicalColumnIndex);
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
      const auto& encoding =
          metadata.pageEncoding(physicalColumnIndex, pageIndex);
      const auto& flat = requireNoNullFlat(encoding, "binary bytes");
      BOLT_CHECK_EQ(flat.bits_per_value(), 8);
      const auto localStart = overlapStart - pageRowStart;
      const auto localCount = overlapEnd - overlapStart;
      const auto bytes = readFlatRange(
          flat, column, page, metadata, localStart, localCount, pool, read);
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

void decodeBinaryValues(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    const DecodeInput& read,
    VectorPtr& result) {
  BOLT_CHECK(
      type->kind() == TypeKind::VARCHAR || type->kind() == TypeKind::VARBINARY);
  const auto& binary = encoding.binary();
  BOLT_CHECK(binary.has_indices());
  BOLT_CHECK(binary.has_bytes());
  const auto& indices = requireNoNullFlat(binary.indices(), "binary indices");
  const auto& bytes = requireNoNullFlat(binary.bytes(), "binary bytes");
  BOLT_CHECK_EQ(indices.bits_per_value(), 64);
  BOLT_CHECK_EQ(bytes.bits_per_value(), 8);

  const auto firstIndex = localStart == 0 ? 0 : localStart - 1;
  const auto numIndices = localCount + (localStart == 0 ? 0 : 1);
  const auto encodedIndices = readFlatRange(
      indices,
      column,
      page,
      metadata,
      firstIndex * sizeof(uint64_t),
      numIndices * sizeof(uint64_t),
      *result->pool(),
      read);

  const auto* rawIndices = encodedIndices->as<char>();
  const auto firstOffset = localStart == 0
      ? 0
      : normalizedStringOffset(
            readLittleEndian<uint64_t>(rawIndices), binary.null_adjustment());
  const auto lastOffset = normalizedStringOffset(
      readLittleEndian<uint64_t>(
          rawIndices + (numIndices - 1) * sizeof(uint64_t)),
      binary.null_adjustment());
  BOLT_CHECK_LE(firstOffset, lastOffset);

  auto stringBytes = readFlatRange(
      bytes,
      column,
      page,
      metadata,
      firstOffset,
      lastOffset - firstOffset,
      *result->pool(),
      read);
  auto* output = result->asFlatVector<StringView>();
  output->addStringBuffer(stringBytes);

  uint64_t currentOffset = firstOffset;
  const auto indexShift = localStart == 0 ? 0 : 1;
  for (uint64_t i = 0; i < localCount; ++i) {
    const auto encodedEnd = readLittleEndian<uint64_t>(
        rawIndices + (i + indexShift) * sizeof(uint64_t));
    const auto endOffset =
        normalizedStringOffset(encodedEnd, binary.null_adjustment());
    BOLT_CHECK_LE(currentOffset, endOffset);
    BOLT_CHECK_LE(endOffset, lastOffset);
    BOLT_CHECK_LE(
        endOffset - currentOffset,
        static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
    if (encodedEnd >= binary.null_adjustment()) {
      result->setNull(outputOffset + i, true);
    } else {
      output->setNoCopy(
          outputOffset + i,
          StringView(
              stringBytes->as<char>() + currentOffset - firstOffset,
              static_cast<int32_t>(endOffset - currentOffset)));
    }
    currentOffset = endOffset;
  }
}

VectorPtr decodeFsstValues(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const DecodeInput& read) {
  constexpr uint64_t kFsstMagic = uint64_t{0x46535354} << 32;
  constexpr uint8_t kFsstEscape = 255;
  constexpr size_t kFsstSymbolTableSize = 8 + 256 * 8 + 256;
  BOLT_CHECK_EQ(encoding.array_encoding_case(), ArrayEncoding::kFsst);
  const auto& fsst = encoding.fsst();
  BOLT_CHECK(fsst.has_binary());
  BOLT_CHECK_EQ(fsst.symbol_table().size(), kFsstSymbolTableSize);
  const auto* table = fsst.symbol_table().data();
  const auto header = readLittleEndian<uint64_t>(table);
  BOLT_CHECK_EQ(
      header & 0xffffffff00000000ULL,
      kFsstMagic,
      "Invalid Lance FSST symbol-table magic");

  auto compressed =
      BaseVector::create(type, static_cast<vector_size_t>(localCount), &pool);
  decodeBinaryValues(
      type,
      fsst.binary(),
      column,
      page,
      metadata,
      localStart,
      localCount,
      0,
      read,
      compressed);
  if ((header & (uint64_t{1} << 24)) == 0) {
    return compressed;
  }

  const auto numSymbols = static_cast<uint8_t>(header & 0xff);
  const auto* symbols = table + sizeof(uint64_t);
  const auto* lengths =
      reinterpret_cast<const uint8_t*>(symbols + numSymbols * sizeof(uint64_t));
  const auto* compressedStrings = compressed->as<SimpleVector<StringView>>();
  uint64_t outputSize = 0;
  for (uint64_t row = 0; row < localCount; ++row) {
    if (compressed->isNullAt(row)) {
      continue;
    }
    const auto value = compressedStrings->valueAt(row);
    for (int32_t i = 0; i < value.size(); ++i) {
      const auto code = static_cast<uint8_t>(value.data()[i]);
      if (code == kFsstEscape) {
        BOLT_CHECK_LT(++i, value.size(), "Truncated Lance FSST escape");
        ++outputSize;
      } else {
        BOLT_CHECK_LT(code, numSymbols, "Invalid Lance FSST symbol code");
        BOLT_CHECK_LE(
            outputSize, std::numeric_limits<uint64_t>::max() - lengths[code]);
        outputSize += lengths[code];
      }
    }
  }
  BOLT_CHECK_LE(
      outputSize,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "Decoded Lance FSST page exceeds Bolt StringView capacity");
  auto outputBytes = AlignedBuffer::allocate<char>(outputSize, &pool);
  auto* destination = outputBytes->asMutable<char>();
  auto result =
      BaseVector::create(type, static_cast<vector_size_t>(localCount), &pool);
  auto* output = result->asFlatVector<StringView>();
  output->addStringBuffer(outputBytes);
  uint64_t outputOffset = 0;
  for (uint64_t row = 0; row < localCount; ++row) {
    if (compressed->isNullAt(row)) {
      result->setNull(row, true);
      continue;
    }
    const auto value = compressedStrings->valueAt(row);
    const auto valueStart = outputOffset;
    for (int32_t i = 0; i < value.size(); ++i) {
      const auto code = static_cast<uint8_t>(value.data()[i]);
      if (code == kFsstEscape) {
        destination[outputOffset++] = value.data()[++i];
      } else {
        const auto length = lengths[code];
        BOLT_CHECK_GE(length, 1);
        BOLT_CHECK_LE(length, 8);
        std::memcpy(destination + outputOffset, symbols + code * 8, length);
        outputOffset += length;
      }
    }
    output->setNoCopy(
        row,
        StringView(
            destination + valueStart,
            static_cast<int32_t>(outputOffset - valueStart)));
  }
  BOLT_CHECK_EQ(outputOffset, outputSize);
  return result;
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
      "Native Lance reader requires non-null {} encoding",
      role);
  BOLT_CHECK(encoding.nullable().no_nulls().has_values());
  return encoding.nullable().no_nulls().values();
}

BufferPtr decodeDictionaryIndices(
    const ArrayEncoding& encodedIndices,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const DecodeInput& read) {
  const auto& indices = unwrapNoNullEncoding(encodedIndices, "indices");
  auto result = AlignedBuffer::allocate<uint64_t>(localCount, &pool);
  auto* rawResult = result->asMutable<uint64_t>();
  if (indices.array_encoding_case() == ArrayEncoding::kFlat) {
    const auto& flat = requireFlat(indices, "dictionary indices");
    BOLT_CHECK_EQ(flat.bits_per_value() % 8, 0);
    const auto bytesPerValue = flat.bits_per_value() / 8;
    BOLT_CHECK(
        bytesPerValue == 1 || bytesPerValue == 2 || bytesPerValue == 4 ||
        bytesPerValue == 8);
    const auto values = readFlatRange(
        flat,
        column,
        page,
        metadata,
        localStart * bytesPerValue,
        localCount * bytesPerValue,
        pool,
        read);
    for (uint64_t i = 0; i < localCount; ++i) {
      switch (bytesPerValue) {
        case 1:
          rawResult[i] = values->as<uint8_t>()[i];
          break;
        case 2:
          rawResult[i] = readLittleEndian<uint16_t>(
              values->as<char>() + i * sizeof(uint16_t));
          break;
        case 4:
          rawResult[i] = readLittleEndian<uint32_t>(
              values->as<char>() + i * sizeof(uint32_t));
          break;
        case 8:
          rawResult[i] = readLittleEndian<uint64_t>(
              values->as<char>() + i * sizeof(uint64_t));
          break;
      }
    }
    return result;
  }

  if (indices.array_encoding_case() == ArrayEncoding::kBitpackedForNonNeg) {
    constexpr uint64_t kChunkRows = 1'024;
    const auto& bitpacked = indices.bitpacked_for_non_neg();
    const auto compressedBits = bitpacked.compressed_bits_per_value();
    const auto uncompressedBits = bitpacked.uncompressed_bits_per_value();
    BOLT_CHECK_LE(compressedBits, uncompressedBits);
    BOLT_CHECK(
        uncompressedBits == 8 || uncompressedBits == 16 ||
        uncompressedBits == 32 || uncompressedBits == 64);
    const auto bytesPerValue = uncompressedBits / 8;
    auto unpacked =
        AlignedBuffer::allocate<char>(localCount * bytesPerValue, &pool);
    if (compressedBits == 0) {
      std::memset(unpacked->asMutable<char>(), 0, unpacked->size());
    } else {
      BOLT_CHECK(bitpacked.has_buffer());
      const auto buffer =
          metadata.resolveBuffer(bitpacked.buffer(), column, page);
      const auto chunkBytes = kChunkRows * compressedBits / 8;
      const auto firstChunk = localStart / kChunkRows;
      const auto endChunk =
          (localStart + localCount + kChunkRows - 1) / kChunkRows;
      const auto firstByte = firstChunk * chunkBytes;
      const auto endByte = endChunk * chunkBytes;
      BOLT_CHECK_LE(endByte, buffer.length);
      const auto packed = read(buffer.offset + firstByte, endByte - firstByte);
      decodeLanceBitpackedForNonNeg(
          packed->as<uint8_t>(),
          packed->size(),
          localStart - firstChunk * kChunkRows,
          localCount,
          compressedBits,
          uncompressedBits,
          unpacked->asMutable<uint8_t>());
    }
    for (uint64_t i = 0; i < localCount; ++i) {
      const auto* value = unpacked->as<char>() + i * bytesPerValue;
      switch (uncompressedBits) {
        case 8:
          rawResult[i] = static_cast<uint8_t>(*value);
          break;
        case 16:
          rawResult[i] = readLittleEndian<uint16_t>(value);
          break;
        case 32:
          rawResult[i] = readLittleEndian<uint32_t>(value);
          break;
        case 64:
          rawResult[i] = readLittleEndian<uint64_t>(value);
          break;
      }
    }
    return result;
  }

  BOLT_CHECK_EQ(
      indices.array_encoding_case(),
      ArrayEncoding::kBitpacked,
      "Native Lance dictionary indices require Flat, Bitpacked, or "
      "BitpackedForNonNeg encoding");
  const auto& bitpacked = indices.bitpacked();
  BOLT_CHECK(!bitpacked.signed_());
  BOLT_CHECK_LE(bitpacked.compressed_bits_per_value(), 64);
  if (bitpacked.compressed_bits_per_value() == 0) {
    std::memset(rawResult, 0, localCount * sizeof(uint64_t));
    return result;
  }
  BOLT_CHECK(bitpacked.has_buffer());
  const auto buffer = metadata.resolveBuffer(bitpacked.buffer(), column, page);
  const auto firstBit = localStart * bitpacked.compressed_bits_per_value();
  const auto endBit =
      (localStart + localCount) * bitpacked.compressed_bits_per_value();
  const auto firstByte = firstBit / 8;
  const auto endByte = (endBit + 7) / 8;
  BOLT_CHECK_LE(endByte, buffer.length);
  const auto packed = read(buffer.offset + firstByte, endByte - firstByte);
  decodeLanceBitpacked(
      packed->as<uint8_t>(),
      packed->size(),
      firstBit - firstByte * 8,
      localCount,
      bitpacked.compressed_bits_per_value(),
      64,
      false,
      reinterpret_cast<uint8_t*>(rawResult));
  return result;
}

std::string_view dictionaryValueLogicalType(std::string_view logicalType) {
  if (logicalType.rfind("dict:", 0) != 0) {
    return logicalType;
  }
  return nativeLanceDictionaryValueLogicalType(logicalType);
}

void applyNullableEncoding(
    const ::lance::encodings::Nullable* nullable,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const DecodeInput& read,
    VectorPtr& result) {
  if (nullable == nullptr ||
      nullable->nullability_case() == ::lance::encodings::Nullable::kNoNulls) {
    return;
  }
  if (nullable->nullability_case() == ::lance::encodings::Nullable::kAllNulls) {
    setLegacyAllNull(0, localCount, *result);
    return;
  }
  BOLT_CHECK_EQ(
      nullable->nullability_case(), ::lance::encodings::Nullable::kSomeNulls);
  const auto& validity =
      requireFlat(nullable->some_nulls().validity(), "validity");
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
      pool,
      read);
  applyLegacyValidityBitmap(
      bytes->as<uint8_t>(), localStart - firstByte * 8, localCount, 0, *result);
}

VectorPtr decodeDictionaryItems(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    uint64_t count,
    const DecodeInput& read) {
  return decodePageEncoding(
      type,
      logicalType,
      encoding,
      column,
      page,
      metadata,
      0,
      count,
      pool,
      read);
}

VectorPtr decodeDictionaryValues(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    uint64_t localStart,
    uint64_t localCount,
    const DecodeInput& read) {
  const auto& dictionary = encoding.dictionary();
  BOLT_CHECK(dictionary.has_indices());
  BOLT_CHECK(dictionary.has_items());
  BOLT_CHECK_GT(dictionary.num_dictionary_items(), 0);
  BOLT_CHECK_LE(
      dictionary.num_dictionary_items(),
      static_cast<uint32_t>(std::numeric_limits<vector_size_t>::max()));

  const auto valueLogicalType = dictionaryValueLogicalType(logicalType);
  const auto encodedIndices = decodeDictionaryIndices(
      dictionary.indices(),
      column,
      page,
      metadata,
      localStart,
      localCount,
      pool,
      read);
  // Consume planned index data before dictionary item decoding. Variable-width
  // items can trigger a second BufferedInput load after their offsets are
  // known, which invalidates outstanding streams in DirectBufferedInput.
  auto dictionaryValues = decodeDictionaryItems(
      type,
      valueLogicalType,
      dictionary.items(),
      column,
      page,
      metadata,
      pool,
      dictionary.num_dictionary_items(),
      read);
  auto indices = allocateIndices(localCount, &pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  const auto* rawEncodedIndices = encodedIndices->as<uint64_t>();
  BufferPtr nulls;
  const auto logicalDictionary = logicalType.rfind("dict:", 0) == 0;
  for (uint64_t i = 0; i < localCount; ++i) {
    const auto encodedIndex = rawEncodedIndices[i];
    if (!logicalDictionary && encodedIndex == 0) {
      if (nulls == nullptr) {
        nulls = allocateNulls(localCount, &pool);
      }
      bits::setNull(nulls->asMutable<uint64_t>(), i);
      rawIndices[i] = 0;
    } else {
      const auto dictionaryIndex =
          logicalDictionary ? encodedIndex : encodedIndex - 1;
      BOLT_CHECK_LT(dictionaryIndex, dictionary.num_dictionary_items());
      rawIndices[i] = static_cast<vector_size_t>(dictionaryIndex);
    }
  }
  return BaseVector::wrapInDictionary(
      std::move(nulls),
      std::move(indices),
      static_cast<vector_size_t>(localCount),
      std::move(dictionaryValues));
}

VectorPtr decodePageEncoding(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const DecodeInput& read) {
  const ::lance::encodings::Nullable* nullable = nullptr;
  const ArrayEncoding* values = &encoding;
  if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
    nullable = &encoding.nullable();
    if (nullable->nullability_case() ==
        ::lance::encodings::Nullable::kAllNulls) {
      return BaseVector::createNullConstant(type, localCount, &pool);
    }
    BOLT_CHECK(
        nullable->nullability_case() ==
            ::lance::encodings::Nullable::kNoNulls ||
        nullable->nullability_case() ==
            ::lance::encodings::Nullable::kSomeNulls);
    values =
        nullable->nullability_case() == ::lance::encodings::Nullable::kNoNulls
        ? &nullable->no_nulls().values()
        : &nullable->some_nulls().values();
  }

  if (type->kind() == TypeKind::ARRAY) {
    return decodeFixedSizeListValues(
        type,
        logicalType,
        encoding,
        column,
        page,
        metadata,
        localStart,
        localCount,
        pool,
        read);
  }
  if (values->array_encoding_case() == ArrayEncoding::kDictionary) {
    BOLT_CHECK_NULL(
        nullable, "Nullable Dictionary wrapper is not valid in Lance v2.0");
    return decodeDictionaryValues(
        type,
        logicalType,
        *values,
        column,
        page,
        metadata,
        pool,
        localStart,
        localCount,
        read);
  }

  VectorPtr result;
  if (values->array_encoding_case() == ArrayEncoding::kBinary) {
    result = BaseVector::create(type, localCount, &pool);
    decodeBinaryValues(
        type,
        *values,
        column,
        page,
        metadata,
        localStart,
        localCount,
        0,
        read,
        result);
  } else if (values->array_encoding_case() == ArrayEncoding::kFsst) {
    result = decodeFsstValues(
        type,
        *values,
        column,
        page,
        metadata,
        localStart,
        localCount,
        pool,
        read);
  } else {
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
        read.rawRead(),
        read.compressedRead());
  }
  applyNullableEncoding(
      nullable,
      column,
      page,
      metadata,
      localStart,
      localCount,
      pool,
      read,
      result);
  return result;
}

void decodeLegacyBinaryValues(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    uint32_t physicalColumnIndex,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    uint64_t itemsOffset,
    const DecodeInput& read,
    VectorPtr& result) {
  BOLT_CHECK(
      type->kind() == TypeKind::VARCHAR || type->kind() == TypeKind::VARBINARY);
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
      "binary offsets",
      pool,
      read);
  const auto* rawIndices = encodedIndices->as<char>();
  const auto normalize = [&list](uint64_t value) {
    return value % list.null_offset_adjustment();
  };
  const auto firstOffset =
      localStart == 0 ? 0 : normalize(readLittleEndian<uint64_t>(rawIndices));
  const auto lastOffset = normalize(readLittleEndian<uint64_t>(
      rawIndices + (numIndices - 1) * sizeof(uint64_t)));
  BOLT_CHECK_LE(firstOffset, lastOffset);
  BOLT_CHECK_LE(
      lastOffset - firstOffset,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));

  auto stringBytes = readFlatByteRange(
      physicalColumnIndex + 1,
      itemsOffset + firstOffset,
      lastOffset - firstOffset,
      metadata,
      pool,
      read);
  auto* output = result->asFlatVector<StringView>();
  output->addStringBuffer(stringBytes);
  uint64_t currentOffset = firstOffset;
  const auto indexShift = localStart == 0 ? 0 : 1;
  for (uint64_t i = 0; i < localCount; ++i) {
    const auto encodedEnd = readLittleEndian<uint64_t>(
        rawIndices + (i + indexShift) * sizeof(uint64_t));
    const auto endOffset = normalize(encodedEnd);
    BOLT_CHECK_LE(currentOffset, endOffset);
    BOLT_CHECK_LE(endOffset, lastOffset);
    BOLT_CHECK_LE(
        endOffset - currentOffset,
        static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
    if (encodedEnd >= list.null_offset_adjustment()) {
      result->setNull(outputOffset + i, true);
    } else {
      output->setNoCopy(
          outputOffset + i,
          StringView(
              stringBytes->as<char>() + currentOffset - firstOffset,
              static_cast<int32_t>(endOffset - currentOffset)));
    }
    currentOffset = endOffset;
  }
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
    const DecodeInput& read) {
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
      read);
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

VectorPtr decodeList(
    const NativeLanceDecoder& decoder,
    const TypePtr& type,
    const ArrayEncoding& encoding,
    uint32_t physicalColumnIndex,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t itemsOffset,
    const DecodeInput& read) {
  auto layout = readListPageLayout(
      encoding, column, page, metadata, pool, localStart, localCount, read);

  // v2.0 lays out a list's offsets column immediately before its item field.
  const auto itemPhysicalColumn = physicalColumnIndex + 1;
  decoder.prefetchPhysicalColumn(
      type->childAt(0),
      itemPhysicalColumn,
      itemsOffset + layout.firstItem,
      layout.numItems);
  auto elements = decoder.decodePhysicalColumn(
      type->childAt(0),
      metadata.physicalColumnLogicalType(itemPhysicalColumn),
      itemPhysicalColumn,
      itemsOffset + layout.firstItem,
      layout.numItems);
  auto result = std::make_shared<ArrayVector>(
      &pool,
      type,
      std::move(layout.nulls),
      localCount,
      std::move(layout.offsets),
      std::move(layout.sizes),
      std::move(elements));
  return result;
}

VectorPtr decodeMap(
    const NativeLanceDecoder& decoder,
    const TypePtr& type,
    const ArrayEncoding& encoding,
    uint32_t physicalColumnIndex,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t itemsOffset,
    const DecodeInput& read) {
  auto layout = readListPageLayout(
      encoding, column, page, metadata, pool, localStart, localCount, read);

  // A v2.0 map is a List whose item column is a two-child SimpleStruct.
  // Decode that struct through the normal nested-column path, then expose its
  // children directly as Bolt map keys and values.
  const auto entriesPhysicalColumn = physicalColumnIndex + 1;
  const auto entriesType =
      ROW({"key", "value"}, {type->childAt(0), type->childAt(1)});
  decoder.prefetchPhysicalColumn(
      entriesType,
      entriesPhysicalColumn,
      itemsOffset + layout.firstItem,
      layout.numItems);
  auto entries = decoder.decodePhysicalColumn(
      entriesType,
      metadata.physicalColumnLogicalType(entriesPhysicalColumn),
      entriesPhysicalColumn,
      itemsOffset + layout.firstItem,
      layout.numItems);
  const auto* entryRows = entries->as<RowVector>();
  BOLT_CHECK_NOT_NULL(
      entryRows, "Native Lance map entries must decode as a RowVector");
  BOLT_CHECK_EQ(
      entryRows->childrenSize(),
      2,
      "Native Lance map entries must contain key and value columns");

  auto result = std::make_shared<MapVector>(
      &pool,
      type,
      std::move(layout.nulls),
      localCount,
      std::move(layout.offsets),
      std::move(layout.sizes),
      entryRows->childAt(0),
      entryRows->childAt(1));
  return result;
}

} // namespace

NativeLanceDecoder::NativeLanceDecoder(
    dwio::common::BufferedInput& input,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    NativeLanceReadScheduler::Options readSchedulerOptions)
    : input_(input),
      metadata_(metadata),
      pool_(pool),
      blobResolver_(std::move(blobResolver)),
      compressedStreamChunkBytes_(std::min(
          readSchedulerOptions.maxReadBytes,
          readSchedulerOptions.maxInFlightBytes)),
      readScheduler_(pool, readSchedulerOptions) {}

NativeLanceDecoder::~NativeLanceDecoder() {
  cancel();
}

void NativeLanceDecoder::cancel() {
  {
    std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
    readScheduler_.cancel(&input_);
  }
  std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
  for (auto& [key, session] : legacyPageReaders_) {
    session->cancel();
  }
  legacyPageReaders_.clear();
}

void NativeLanceDecoder::prefetchColumns(
    const std::vector<uint32_t>& columnIndices,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  planColumns(columnIndices, rowStart, rowCount);
  materializeReadPlan();
}

void NativeLanceDecoder::planColumns(
    const std::vector<uint32_t>& columnIndices,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  BOLT_CHECK_LE(rowStart, metadata_.numRows());
  BOLT_CHECK_LE(rowCount, metadata_.numRows() - rowStart);
  if (rowCount == 0) {
    return;
  }

  readScheduler_.clearStage();
  for (const auto logicalColumnIndex : columnIndices) {
    BOLT_CHECK_LT(logicalColumnIndex, metadata_.rowType()->size());
    if (metadata_.usesStructuralEncoding()) {
      enqueueStructuralField(
          metadata_.structuralField(logicalColumnIndex), rowStart, rowCount);
      continue;
    }
    const auto physicalIndex =
        metadata_.physicalColumnIndex(logicalColumnIndex);
    enqueuePhysicalColumn(
        metadata_.rowType()->childAt(logicalColumnIndex),
        physicalIndex,
        rowStart,
        rowCount);
  }
  submitReadPlan();
}

void NativeLanceDecoder::enqueueStructuralField(
    const NativeLanceMetadata::StructuralField& field,
    uint64_t rowStart,
    uint64_t rowCount) const {
  if (!field.leaf) {
    for (const auto& child : field.children) {
      enqueueStructuralField(child, rowStart, rowCount);
    }
    return;
  }
  BOLT_CHECK_LE(
      rowStart, std::numeric_limits<uint64_t>::max() / field.rowsPerParent);
  BOLT_CHECK_LE(
      rowCount, std::numeric_limits<uint64_t>::max() / field.rowsPerParent);
  enqueuePhysicalColumn(
      field.type,
      field.physicalColumnIndex,
      rowStart * field.rowsPerParent,
      rowCount * field.rowsPerParent,
      field.rowsPerParent == 1 ? std::vector<uint32_t>{}
                               : std::vector<uint32_t>{1});
}

void NativeLanceDecoder::prefetchPhysicalColumn(
    const TypePtr& type,
    uint32_t physicalColumnIndex,
    uint64_t rowStart,
    uint64_t rowCount) const {
  prefetchPhysicalColumns({{type, physicalColumnIndex}}, rowStart, rowCount);
}

void NativeLanceDecoder::prefetchPhysicalColumns(
    const std::vector<std::pair<TypePtr, uint32_t>>& columns,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  planPhysicalColumns(columns, rowStart, rowCount);
  materializeReadPlan();
}

void NativeLanceDecoder::planPhysicalColumns(
    const std::vector<std::pair<TypePtr, uint32_t>>& columns,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  readScheduler_.clearStage();
  for (const auto& [type, physicalColumnIndex] : columns) {
    enqueuePhysicalColumn(type, physicalColumnIndex, rowStart, rowCount);
  }
  submitReadPlan();
}

void NativeLanceDecoder::enqueuePhysicalColumn(
    const TypePtr& type,
    uint32_t physicalIndex,
    uint64_t rowStart,
    uint64_t rowCount,
    const std::vector<uint32_t>& arrayDimensions) const {
  BOLT_CHECK_LT(physicalIndex, metadata_.numPhysicalColumns());
  if (metadata_.usesStructuralEncoding()) {
    const auto& column = metadata_.column(physicalIndex);
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(rowStart, rowCount)) {
      const auto& page = column.pages(span.pageIndex);
      const auto rangeRead = lanceStructuralPageSupportsRangeRead(
          type,
          arrayDimensions,
          metadata_.physicalColumnChildLogicalTypes(physicalIndex),
          metadata_.pageLayout(physicalIndex, span.pageIndex));
      for (int32_t buffer = 0; buffer < page.buffer_offsets_size(); ++buffer) {
        if (rangeRead && buffer == 1) {
          continue;
        }
        if (page.buffer_sizes(buffer) > 0) {
          scheduleRead(page.buffer_offsets(buffer), page.buffer_sizes(buffer));
        }
      }
    }
    return;
  }
  auto enqueueCompressed =
      [this](std::string_view scheme, BufferDescriptor buffer) {
        scheduleCompressedRead(scheme, buffer.offset, buffer.length);
      };
  if (metadata_.isBlobColumn(physicalIndex)) {
    const auto& column = metadata_.column(physicalIndex);
    auto enqueue = [this](uint64_t offset, uint64_t length) {
      scheduleRead(offset, length);
    };
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(rowStart, rowCount)) {
      const auto& page = column.pages(span.pageIndex);
      const auto& encoding =
          metadata_.pageEncoding(physicalIndex, span.pageIndex);
      enqueueEncodingRanges(
          encoding,
          column,
          page,
          metadata_,
          span.localRowBegin,
          span.rowCount,
          enqueue,
          enqueueCompressed);
    }
    return;
  }
  // Variable-size payloads cannot be planned until their parent offsets have
  // been decoded, but their offset/index buffers still can. Schedule these
  // first-stage ranges with other columns and let nested decoding issue the
  // second-stage payload plan after offsets are known.
  const auto& column = metadata_.column(physicalIndex);
  if (type->kind() == TypeKind::ROW && column.pages_size() > 0) {
    const auto firstDataPage = std::find_if(
        column.pages().begin(), column.pages().end(), [](const auto& page) {
          return page.length() > 0;
        });
    if (firstDataPage == column.pages().end()) {
      return;
    }
    const auto firstPageIndex =
        static_cast<int32_t>(firstDataPage - column.pages().begin());
    const auto& firstEncoding =
        metadata_.pageEncoding(physicalIndex, firstPageIndex);
    if (firstEncoding.array_encoding_case() == ArrayEncoding::kStruct) {
      // Nested list and map descendants need later payload planning, but their
      // first-stage offset ranges can be loaded with the rest of the batch.
      uint32_t childPhysicalIndex = physicalIndex + 1;
      for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
        enqueuePhysicalColumn(
            type->childAt(childIndex), childPhysicalIndex, rowStart, rowCount);
        childPhysicalIndex += metadata_.physicalColumnSpan(childPhysicalIndex);
      }
      return;
    }
  }

  auto enqueue = [this](uint64_t offset, uint64_t length) {
    scheduleRead(offset, length);
  };
  NativeLanceColumnCursor cursor(
      physicalIndex, metadata_.pageRowStarts(physicalIndex));
  for (const auto& span : cursor.spans(rowStart, rowCount)) {
    const auto& page = column.pages(span.pageIndex);
    const auto& encoding =
        metadata_.pageEncoding(physicalIndex, span.pageIndex);
    enqueueEncodingRanges(
        encoding,
        column,
        page,
        metadata_,
        span.localRowBegin,
        span.rowCount,
        enqueue,
        enqueueCompressed);
  }
}

void NativeLanceDecoder::scheduleRead(uint64_t offset, uint64_t length) const {
  readScheduler_.schedule(input_, offset, length);
}

void NativeLanceDecoder::scheduleCompressedRead(
    std::string_view scheme,
    uint64_t offset,
    uint64_t length) const {
  BOLT_CHECK(!scheme.empty());
  if (scheme == "zstd") {
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    if (legacyPageReaders_.find({offset, length}) != legacyPageReaders_.end()) {
      return;
    }
  }
  scheduleRead(offset, length);
}

void NativeLanceDecoder::submitReadPlan() const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  readScheduler_.submit(input_);
}

void NativeLanceDecoder::materializeReadPlan() const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  readScheduler_.materialize();
}

BufferPtr NativeLanceDecoder::read(uint64_t offset, uint64_t length) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  BOLT_CHECK_LE(offset, input_.getReadFile()->size());
  BOLT_CHECK_LE(length, input_.getReadFile()->size() - offset);
  BOLT_CHECK_LE(
      length,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "Lance data range is too large: {} bytes",
      length);
  if (auto buffer = readScheduler_.take(offset, length)) {
    return buffer;
  }
  auto buffer = AlignedBuffer::allocate<char>(length, &pool_);
  auto stream = input_.enqueue({offset, length});
  input_.load(dwio::common::LogType::BLOCK);
  stream->readFully(buffer->asMutable<char>(), length);
  return buffer;
}

BufferPtr NativeLanceDecoder::readCompressedRange(
    std::string_view scheme,
    uint64_t compressedOffset,
    uint64_t compressedLength,
    uint64_t decodedOffset,
    uint64_t decodedLength) const {
  const CompressedBufferKey key{compressedOffset, compressedLength};
  std::shared_ptr<NativeLanceLegacyPageReader> pageReader;
  {
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    const auto session = legacyPageReaders_.find(key);
    if (session != legacyPageReaders_.end()) {
      pageReader = session->second;
    }
  }
  if (pageReader == nullptr) {
    auto candidate = std::make_shared<NativeLanceLegacyPageReader>(
        std::string(scheme),
        compressedOffset,
        compressedLength,
        pool_,
        [this](uint64_t offset, uint64_t length) {
          return read(offset, length);
        },
        compressedStreamChunkBytes_);
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    const auto [session, inserted] = legacyPageReaders_.emplace(key, candidate);
    pageReader = inserted ? std::move(candidate) : session->second;
  }
  BOLT_CHECK(
      pageReader->accessMode() == NativeLanceDecompressor::accessMode(scheme));
  auto result = pageReader->decodeRange(decodedOffset, decodedLength);
  if (pageReader->finished()) {
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    const auto session = legacyPageReaders_.find(key);
    if (session != legacyPageReaders_.end() &&
        session->second.get() == pageReader.get()) {
      legacyPageReaders_.erase(session);
    }
  }
  return result;
}

bool NativeLanceDecoder::hasCompressedColumn(
    uint32_t columnIndex,
    uint64_t rowStart,
    uint64_t rowCount) const {
  BOLT_CHECK_LT(columnIndex, metadata_.rowType()->size());
  const auto firstPhysical = metadata_.physicalColumnIndex(columnIndex);
  const auto physicalEnd =
      firstPhysical + metadata_.physicalColumnSpan(firstPhysical);
  if (metadata_.usesStructuralEncoding()) {
    for (uint32_t physicalIndex = firstPhysical; physicalIndex < physicalEnd;
         ++physicalIndex) {
      NativeLanceColumnCursor cursor(
          physicalIndex, metadata_.pageRowStarts(physicalIndex));
      for (const auto& span : cursor.spans(rowStart, rowCount)) {
        if (lanceStructuralLayoutHasCompression(
                metadata_.pageLayout(physicalIndex, span.pageIndex))) {
          return true;
        }
      }
    }
    return false;
  }
  for (uint32_t physicalIndex = firstPhysical; physicalIndex < physicalEnd;
       ++physicalIndex) {
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(rowStart, rowCount)) {
      if (hasCompressedFlatBuffer(
              metadata_.pageEncoding(physicalIndex, span.pageIndex))) {
        return true;
      }
    }
  }
  return false;
}

VectorPtr NativeLanceDecoder::decodeColumn(
    uint32_t columnIndex,
    uint64_t rowStart,
    uint64_t rowCount) const {
  BOLT_CHECK_LT(columnIndex, metadata_.rowType()->size());
  BOLT_CHECK_LE(rowStart, metadata_.numRows());
  BOLT_CHECK_LE(rowCount, metadata_.numRows() - rowStart);
  BOLT_CHECK_LE(
      rowCount,
      static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));

  const auto type = metadata_.rowType()->childAt(columnIndex);
  const auto logicalType = metadata_.columnLogicalType(columnIndex);
  const auto physicalIndex = metadata_.physicalColumnIndex(columnIndex);
  if (metadata_.usesStructuralEncoding()) {
    return decodeStructuralField(
        metadata_.structuralField(columnIndex), rowStart, rowCount);
  }
  return decodePhysicalColumn(
      type, logicalType, physicalIndex, rowStart, rowCount);
}

VectorPtr NativeLanceDecoder::decodeSelectedRows(
    uint32_t columnIndex,
    uint64_t batchRowStart,
    folly::Range<const vector_size_t*> rows) const {
  BOLT_CHECK_LT(columnIndex, metadata_.rowType()->size());
  BOLT_CHECK_LE(batchRowStart, metadata_.numRows());
  BOLT_CHECK_LE(
      rows.size(),
      static_cast<size_t>(std::numeric_limits<vector_size_t>::max()));
  const auto type = metadata_.rowType()->childAt(columnIndex);
  if (rows.empty()) {
    return BaseVector::create(type, 0, &pool_);
  }

  BOLT_CHECK_LT(batchRowStart, metadata_.numRows());
  BOLT_CHECK_GE(rows.front(), 0);
  BOLT_CHECK(std::is_sorted(rows.begin(), rows.end()));
  BOLT_CHECK(
      std::adjacent_find(rows.begin(), rows.end()) == rows.end(),
      "Selected Lance row numbers must be unique");
  BOLT_CHECK_LE(
      static_cast<uint64_t>(rows.back()),
      metadata_.numRows() - batchRowStart - 1);

  if (rows.size() == static_cast<size_t>(rows.back() - rows.front() + 1)) {
    prefetchColumns({columnIndex}, batchRowStart + rows.front(), rows.size());
    return decodeColumn(columnIndex, batchRowStart + rows.front(), rows.size());
  }

  // Submit all fixed-width runs before decoding any of them. This gives
  // DirectBufferedInput one complete request set to coalesce and dispatch.
  // Variable-width containers intentionally defer their payload planning
  // until offsets have been decoded.
  if (!input_.supportSyncLoad()) {
    readScheduler_.clearStage();
    size_t scheduledOffset = 0;
    while (scheduledOffset < rows.size()) {
      size_t runEnd = scheduledOffset + 1;
      while (runEnd < rows.size() && rows[runEnd] == rows[runEnd - 1] + 1) {
        ++runEnd;
      }
      enqueuePhysicalColumn(
          type,
          metadata_.physicalColumnIndex(columnIndex),
          batchRowStart + rows[scheduledOffset],
          runEnd - scheduledOffset);
      scheduledOffset = runEnd;
    }
    submitReadPlan();
  }

  auto result =
      BaseVector::create(type, static_cast<vector_size_t>(rows.size()), &pool_);
  size_t outputOffset = 0;
  while (outputOffset < rows.size()) {
    size_t runEnd = outputOffset + 1;
    while (runEnd < rows.size() && rows[runEnd] == rows[runEnd - 1] + 1) {
      ++runEnd;
    }
    const auto runSize = runEnd - outputOffset;
    const auto values =
        decodeColumn(columnIndex, batchRowStart + rows[outputOffset], runSize);
    result->copy(
        values.get(),
        static_cast<vector_size_t>(outputOffset),
        0,
        static_cast<vector_size_t>(runSize));
    outputOffset = runEnd;
  }
  return result;
}

VectorPtr NativeLanceDecoder::decodeStructuralField(
    const NativeLanceMetadata::StructuralField& field,
    uint64_t rowStart,
    uint64_t rowCount) const {
  struct Branch {
    uint32_t physicalColumnIndex;
    TypePtr type;
    uint64_t rowsPerParent;
    std::vector<uint32_t> fixedSizeDimensions;
  };
  std::vector<Branch> branches;
  const auto collectBranches =
      [&](const auto& self,
          const NativeLanceMetadata::StructuralField& node) -> void {
    if (node.leaf) {
      branches.push_back(
          {node.physicalColumnIndex, node.type, node.rowsPerParent, {}});
      return;
    }
    for (const auto& child : node.children) {
      const auto first = branches.size();
      self(self, child);
      for (auto index = first; index < branches.size(); ++index) {
        if (node.type->kind() == TypeKind::ROW) {
          branches[index].type =
              ROW({child.name}, {std::move(branches[index].type)});
        } else if (node.type->kind() == TypeKind::MAP) {
          // Arrow maps are encoded structurally as List<Struct<key, value>>.
          branches[index].type = ARRAY(std::move(branches[index].type));
          branches[index].fixedSizeDimensions.insert(
              branches[index].fixedSizeDimensions.begin(), 0);
        } else {
          BOLT_CHECK_EQ(node.type->kind(), TypeKind::ARRAY);
          branches[index].type = ARRAY(std::move(branches[index].type));
          if (child.rowsPerParent != node.rowsPerParent) {
            BOLT_CHECK_EQ(child.rowsPerParent % node.rowsPerParent, 0);
            branches[index].fixedSizeDimensions.insert(
                branches[index].fixedSizeDimensions.begin(),
                child.rowsPerParent / node.rowsPerParent);
          } else {
            branches[index].fixedSizeDimensions.insert(
                branches[index].fixedSizeDimensions.begin(), 0);
          }
        }
      }
    }
  };
  collectBranches(collectBranches, field);
  BOLT_CHECK_EQ(branches.size(), field.physicalColumnCount);

  std::vector<VectorPtr> decodedBranches;
  decodedBranches.reserve(branches.size());
  for (const auto& branch : branches) {
    decodedBranches.push_back(decodePhysicalColumn(
        branch.type,
        metadata_.physicalColumnLogicalType(branch.physicalColumnIndex),
        branch.physicalColumnIndex,
        rowStart,
        rowCount,
        branch.fixedSizeDimensions));
  }

  const auto checkNulls = [](const BaseVector& expected,
                             const BaseVector& actual) {
    BOLT_CHECK_EQ(expected.size(), actual.size());
    for (vector_size_t row = 0; row < expected.size(); ++row) {
      BOLT_CHECK_EQ(
          expected.isNullAt(row),
          actual.isNullAt(row),
          "Structural sibling validity differs at row {}",
          row);
    }
  };
  const auto merge = [&](const auto& self,
                         const NativeLanceMetadata::StructuralField& node,
                         std::vector<VectorPtr> vectors) -> VectorPtr {
    BOLT_CHECK_EQ(vectors.size(), node.physicalColumnCount);
    if (node.leaf) {
      BOLT_CHECK_EQ(vectors.size(), 1);
      return std::move(vectors.front());
    }
    if (node.type->kind() == TypeKind::ROW) {
      const auto* first = vectors.front()->as<RowVector>();
      BOLT_CHECK_NOT_NULL(first);
      std::vector<VectorPtr> children;
      children.reserve(node.children.size());
      size_t branchIndex = 0;
      for (const auto& child : node.children) {
        std::vector<VectorPtr> childBranches;
        childBranches.reserve(child.physicalColumnCount);
        for (uint32_t i = 0; i < child.physicalColumnCount; ++i) {
          const auto* branch = vectors[branchIndex++]->as<RowVector>();
          BOLT_CHECK_NOT_NULL(branch);
          BOLT_CHECK_EQ(branch->childrenSize(), 1);
          checkNulls(*first, *branch);
          childBranches.push_back(branch->childAt(0));
        }
        children.push_back(self(self, child, std::move(childBranches)));
      }
      BOLT_CHECK_EQ(branchIndex, vectors.size());
      return std::make_shared<RowVector>(
          &pool_,
          node.type,
          first->nulls(),
          first->size(),
          std::move(children));
    }

    BOLT_CHECK(
        node.type->kind() == TypeKind::ARRAY ||
        node.type->kind() == TypeKind::MAP);
    const auto* first = vectors.front()->as<ArrayVector>();
    BOLT_CHECK_NOT_NULL(first);
    std::vector<VectorPtr> elementBranches;
    elementBranches.reserve(vectors.size());
    for (const auto& vector : vectors) {
      const auto* branch = vector->as<ArrayVector>();
      BOLT_CHECK_NOT_NULL(branch);
      checkNulls(*first, *branch);
      for (vector_size_t row = 0; row < first->size(); ++row) {
        BOLT_CHECK_EQ(first->offsetAt(row), branch->offsetAt(row));
        BOLT_CHECK_EQ(first->sizeAt(row), branch->sizeAt(row));
      }
      elementBranches.push_back(branch->elements());
    }
    BOLT_CHECK_EQ(node.children.size(), 1);
    auto elements =
        self(self, node.children.front(), std::move(elementBranches));
    if (node.type->kind() == TypeKind::ARRAY) {
      return std::make_shared<ArrayVector>(
          &pool_,
          node.type,
          first->nulls(),
          first->size(),
          first->offsets(),
          first->sizes(),
          std::move(elements));
    }
    const auto* entries = elements->template as<RowVector>();
    BOLT_CHECK_NOT_NULL(entries);
    BOLT_CHECK_EQ(entries->childrenSize(), 2);
    return std::make_shared<MapVector>(
        &pool_,
        node.type,
        first->nulls(),
        first->size(),
        first->offsets(),
        first->sizes(),
        entries->childAt(0),
        entries->childAt(1));
  };
  return merge(merge, field, std::move(decodedBranches));
}

VectorPtr NativeLanceDecoder::decodePhysicalColumn(
    const TypePtr& type,
    std::string_view logicalType,
    uint32_t physicalIndex,
    uint64_t rowStart,
    uint64_t rowCount,
    const std::vector<uint32_t>& arrayDimensions) const {
  return decodePhysicalColumnNoCache(
      type, logicalType, physicalIndex, rowStart, rowCount, arrayDimensions);
}

VectorPtr NativeLanceDecoder::decodePhysicalColumnNoCache(
    const TypePtr& type,
    std::string_view logicalType,
    uint32_t physicalIndex,
    uint64_t rowStart,
    uint64_t rowCount,
    const std::vector<uint32_t>& arrayDimensions) const {
  BOLT_CHECK_LT(physicalIndex, metadata_.numPhysicalColumns());
  BOLT_CHECK_LE(rowStart, std::numeric_limits<uint64_t>::max() - rowCount);
  if (rowCount == 0) {
    return BaseVector::create(type, 0, &pool_);
  }
  const DecodeInput readInput(
      [this](uint64_t offset, uint64_t length) { return read(offset, length); },
      [this](
          std::string_view scheme,
          uint64_t compressedOffset,
          uint64_t compressedLength,
          uint64_t decodedOffset,
          uint64_t decodedLength) {
        return readCompressedRange(
            scheme,
            compressedOffset,
            compressedLength,
            decodedOffset,
            decodedLength);
      });
  if (metadata_.usesStructuralEncoding()) {
    const auto& column = metadata_.column(physicalIndex);
    uint64_t rowScale = 1;
    for (const auto dimension : arrayDimensions) {
      if (dimension == 0) {
        continue;
      }
      BOLT_CHECK_LE(rowScale, std::numeric_limits<uint64_t>::max() / dimension);
      rowScale *= dimension;
    }
    BOLT_CHECK_LE(rowStart, std::numeric_limits<uint64_t>::max() / rowScale);
    BOLT_CHECK_LE(rowCount, std::numeric_limits<uint64_t>::max() / rowScale);
    const auto physicalRowStart = rowStart * rowScale;
    const auto physicalRowCount = rowCount * rowScale;
    uint64_t outputOffset = 0;
    auto result =
        BaseVector::create(type, static_cast<vector_size_t>(rowCount), &pool_);
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(physicalRowStart, physicalRowCount)) {
      BOLT_CHECK_EQ(span.localRowBegin % rowScale, 0);
      BOLT_CHECK_EQ(span.rowCount % rowScale, 0);
      const auto localStart = span.localRowBegin / rowScale;
      const auto localCount = span.rowCount / rowScale;
      const auto& page = column.pages(span.pageIndex);
      const auto rangeRead = lanceStructuralPageSupportsRangeRead(
          type,
          arrayDimensions,
          metadata_.physicalColumnChildLogicalTypes(physicalIndex),
          metadata_.pageLayout(physicalIndex, span.pageIndex));
      NativeLanceStructuralPageReader pageReader(
          {.key =
               {.physicalColumn = physicalIndex, .pageIndex = span.pageIndex},
           .type = type,
           .logicalType = metadata_.physicalColumnLogicalType(physicalIndex),
           .fixedSizeDimensions = arrayDimensions,
           .packedChildLogicalTypes =
               metadata_.physicalColumnChildLogicalTypes(physicalIndex),
           .localRowStart = localStart,
           .rowCount = localCount},
          metadata_,
          pool_,
          blobResolver_,
          input_.getName(),
          [this](const std::vector<std::pair<uint64_t, uint64_t>>& ranges) {
            std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
            readScheduler_.clearStage();
            for (const auto& [offset, length] : ranges) {
              scheduleRead(offset, length);
            }
            submitReadPlan();
          },
          readInput);
      pageReader.decode();
      auto decoded = pageReader.consume();
      result->copy(
          decoded.get(),
          static_cast<vector_size_t>(outputOffset),
          static_cast<vector_size_t>(rangeRead ? 0 : localStart),
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
    }
    BOLT_CHECK_EQ(outputOffset, rowCount);
    return result;
  }
  if (metadata_.isBlobColumn(physicalIndex)) {
    BOLT_CHECK_EQ(type->kind(), TypeKind::VARBINARY);
    BOLT_CHECK_EQ(logicalType, "large_binary");
    BOLT_CHECK_LE(rowStart, metadata_.numRows());
    BOLT_CHECK_LE(rowCount, metadata_.numRows() - rowStart);
    auto result =
        BaseVector::create(type, static_cast<vector_size_t>(rowCount), &pool_);
    if (rowCount == 0) {
      return result;
    }

    auto descriptors =
        AlignedBuffer::allocate<BlobDescriptor>(rowCount, &pool_);
    auto* rawDescriptors = descriptors->asMutable<BlobDescriptor>();
    const auto& column = metadata_.column(physicalIndex);
    uint64_t outputOffset = 0;
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(rowStart, rowCount)) {
      const auto& page = column.pages(span.pageIndex);
      const auto& encoding =
          metadata_.pageEncoding(physicalIndex, span.pageIndex);
      decodeBlobDescriptions(
          encoding,
          column,
          page,
          metadata_,
          span.localRowBegin,
          span.rowCount,
          outputOffset,
          readInput,
          rawDescriptors);
      outputOffset += span.rowCount;
    }
    BOLT_CHECK_EQ(outputOffset, rowCount);

    uint64_t payloadBytes = 0;
    struct PayloadRead {
      uint64_t row;
      uint64_t outputOffset;
      std::unique_ptr<dwio::common::SeekableInputStream> stream;
    };
    std::vector<PayloadRead, memory::StlAllocator<PayloadRead>> payloadReads{
        memory::StlAllocator<PayloadRead>(&pool_)};
    payloadReads.reserve(rowCount);
    const auto fileSize = input_.getReadFile()->size();
    for (uint64_t row = 0; row < rowCount; ++row) {
      const auto [position, size] = rawDescriptors[row];
      if (position == 1 && size == 0) {
        result->setNull(row, true);
        continue;
      }
      BOLT_CHECK_LE(
          size,
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
          "Lance Blob value exceeds Bolt StringView capacity");
      BOLT_CHECK_LE(position, fileSize);
      BOLT_CHECK_LE(size, fileSize - position);
      BOLT_CHECK_LE(
          payloadBytes,
          std::numeric_limits<uint64_t>::max() - size,
          "Lance Blob payload size overflow");
      if (size > 0) {
        payloadReads.push_back(
            {row, payloadBytes, input_.enqueue({position, size})});
      }
      payloadBytes += size;
    }
    if (!payloadReads.empty()) {
      input_.load(dwio::common::LogType::BLOCK);
    }
    auto payload = AlignedBuffer::allocate<char>(payloadBytes, &pool_);
    auto* values = result->asFlatVector<StringView>();
    if (payloadBytes > 0) {
      values->addStringBuffer(payload);
    }
    for (auto& payloadRead : payloadReads) {
      const auto size = rawDescriptors[payloadRead.row].size;
      payloadRead.stream->readFully(
          payload->asMutable<char>() + payloadRead.outputOffset, size);
    }
    uint64_t payloadOffset = 0;
    for (uint64_t row = 0; row < rowCount; ++row) {
      const auto [position, size] = rawDescriptors[row];
      if (position == 1 && size == 0) {
        continue;
      }
      values->setNoCopy(
          row,
          size == 0 ? StringView()
                    : StringView(
                          payload->as<char>() + payloadOffset,
                          static_cast<int32_t>(size)));
      payloadOffset += size;
    }
    BOLT_CHECK_EQ(payloadOffset, payloadBytes);
    return result;
  }
  if (type->kind() == TypeKind::ROW) {
    const auto& header = metadata_.column(physicalIndex);
    const auto firstDataPage = std::find_if(
        header.pages().begin(), header.pages().end(), [](const auto& page) {
          return page.length() > 0;
        });
    if (firstDataPage != header.pages().end()) {
      BOLT_CHECK(firstDataPage->has_encoding());
      const auto firstPageIndex =
          static_cast<int32_t>(firstDataPage - header.pages().begin());
      const auto& firstEncoding =
          metadata_.pageEncoding(physicalIndex, firstPageIndex);
      if (firstEncoding.array_encoding_case() == ArrayEncoding::kPackedStruct) {
        BOLT_CHECK_LE(rowStart, metadata_.numRows());
        BOLT_CHECK_LE(rowCount, metadata_.numRows() - rowStart);
        uint64_t pageRowStart = 0;
        uint64_t outputOffset = 0;
        auto result = BaseVector::create(
            type, static_cast<vector_size_t>(rowCount), &pool_);
        for (int32_t pageIndex = 0; pageIndex < header.pages_size();
             ++pageIndex) {
          const auto& page = header.pages(pageIndex);
          if (page.length() == 0) {
            continue;
          }
          const auto pageRowEnd = pageRowStart + page.length();
          const auto overlapStart = std::max(rowStart, pageRowStart);
          const auto overlapEnd = std::min(rowStart + rowCount, pageRowEnd);
          if (overlapStart < overlapEnd) {
            const auto& encoding =
                metadata_.pageEncoding(physicalIndex, pageIndex);
            auto pageResult = decodePackedStructValues(
                type,
                encoding,
                header,
                page,
                metadata_,
                physicalIndex,
                overlapStart - pageRowStart,
                overlapEnd - overlapStart,
                pool_,
                readInput);
            if (outputOffset == 0 && pageResult->size() == rowCount) {
              return pageResult;
            }
            result->copy(
                pageResult.get(),
                static_cast<vector_size_t>(outputOffset),
                0,
                pageResult->size());
            outputOffset += pageResult->size();
          }
          pageRowStart = pageRowEnd;
        }
        BOLT_CHECK_EQ(outputOffset, rowCount);
        return result;
      }
    }
    uint64_t headerRows = 0;
    for (int32_t pageIndex = 0; pageIndex < header.pages_size(); ++pageIndex) {
      const auto& page = header.pages(pageIndex);
      if (page.length() == 0) {
        continue;
      }
      BOLT_CHECK(page.has_encoding(), "Lance struct page has no encoding");
      const auto& encoding = metadata_.pageEncoding(physicalIndex, pageIndex);
      BOLT_CHECK_EQ(
          encoding.array_encoding_case(),
          ArrayEncoding::kStruct,
          "Native Lance reader requires SimpleStruct for struct columns");
      BOLT_CHECK_LE(
          page.length(), std::numeric_limits<uint64_t>::max() - headerRows);
      headerRows += page.length();
    }
    BOLT_CHECK_LE(rowStart, headerRows);
    BOLT_CHECK_LE(rowCount, headerRows - rowStart);

    std::vector<VectorPtr> children;
    children.reserve(type->size());
    const auto scheduleChildrenSeparately = requiresDeferredRead(type);
    uint32_t childPhysicalIndex = physicalIndex + 1;
    std::vector<std::pair<TypePtr, uint32_t>> childrenToSchedule;
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      if (scheduleChildrenSeparately) {
        childrenToSchedule.push_back(
            {type->childAt(childIndex), childPhysicalIndex});
      }
      childPhysicalIndex += metadata_.physicalColumnSpan(childPhysicalIndex);
    }
    if (!childrenToSchedule.empty()) {
      prefetchPhysicalColumns(childrenToSchedule, rowStart, rowCount);
    }
    childPhysicalIndex = physicalIndex + 1;
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      children.push_back(decodePhysicalColumn(
          type->childAt(childIndex),
          metadata_.physicalColumnLogicalType(childPhysicalIndex),
          childPhysicalIndex,
          rowStart,
          rowCount));
      childPhysicalIndex += metadata_.physicalColumnSpan(childPhysicalIndex);
    }
    BOLT_CHECK_EQ(
        childPhysicalIndex,
        physicalIndex + metadata_.physicalColumnSpan(physicalIndex));
    return std::make_shared<RowVector>(
        &pool_,
        type,
        nullptr,
        static_cast<vector_size_t>(rowCount),
        std::move(children));
  }
  auto result =
      BaseVector::create(type, static_cast<vector_size_t>(rowCount), &pool_);
  if (rowCount == 0) {
    return result;
  }

  const auto& column = metadata_.column(physicalIndex);
  uint64_t pageRowStart = 0;
  uint64_t outputOffset = 0;
  uint64_t itemsOffset = 0;
  const auto rowEnd = rowStart + rowCount;
  for (int32_t pageIndex = 0; pageIndex < column.pages_size(); ++pageIndex) {
    const auto& page = column.pages(pageIndex);
    if (page.length() == 0) {
      continue;
    }
    const auto pageRowEnd = pageRowStart + page.length();
    const auto overlapStart = std::max(rowStart, pageRowStart);
    const auto overlapEnd = std::min(rowEnd, pageRowEnd);
    if (overlapStart >= overlapEnd) {
      BOLT_CHECK(page.has_encoding(), "Lance page has no encoding");
      const auto& skippedEncoding =
          metadata_.pageEncoding(physicalIndex, pageIndex);
      if (skippedEncoding.array_encoding_case() == ArrayEncoding::kList) {
        itemsOffset += skippedEncoding.list().num_items();
      }
      pageRowStart = pageRowEnd;
      continue;
    }

    BOLT_CHECK(page.has_encoding(), "Lance page has no encoding");
    const auto& encoding = metadata_.pageEncoding(physicalIndex, pageIndex);
    const auto localStart = overlapStart - pageRowStart;
    const auto localCount = overlapEnd - overlapStart;
    const auto* unwrappedEncoding = &encoding;
    if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
      const auto& nullable = encoding.nullable();
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kNoNulls) {
        unwrappedEncoding = &nullable.no_nulls().values();
      } else if (
          nullable.nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
        unwrappedEncoding = &nullable.some_nulls().values();
      }
    }
    if (unwrappedEncoding->array_encoding_case() ==
        ArrayEncoding::kFixedSizeList) {
      auto pageResult = decodeFixedSizeListValues(
          type,
          logicalType,
          encoding,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          pool_,
          readInput);
      if (outputOffset == 0 && pageResult->size() == rowCount) {
        return pageResult;
      }
      result->copy(
          pageResult.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kDictionary) {
      auto pageResult = decodeDictionaryValues(
          type,
          logicalType,
          encoding,
          column,
          page,
          metadata_,
          pool_,
          localStart,
          localCount,
          readInput);
      if (outputOffset == 0 && localCount == rowCount) {
        result = std::move(pageResult);
      } else {
        result->copy(
            pageResult.get(),
            static_cast<vector_size_t>(outputOffset),
            0,
            static_cast<vector_size_t>(localCount));
      }
      outputOffset += localCount;
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kFsst) {
      auto pageResult = decodeFsstValues(
          type,
          encoding,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          pool_,
          readInput);
      if (outputOffset == 0 && localCount == rowCount) {
        result = std::move(pageResult);
      } else {
        result->copy(
            pageResult.get(),
            static_cast<vector_size_t>(outputOffset),
            0,
            static_cast<vector_size_t>(localCount));
      }
      outputOffset += localCount;
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kBinary) {
      decodeBinaryValues(
          type,
          encoding,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          outputOffset,
          readInput,
          result);
      outputOffset += localCount;
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kList &&
        (type->kind() == TypeKind::VARCHAR ||
         type->kind() == TypeKind::VARBINARY)) {
      decodeLegacyBinaryValues(
          type,
          encoding,
          physicalIndex,
          column,
          page,
          metadata_,
          pool_,
          localStart,
          localCount,
          outputOffset,
          itemsOffset,
          readInput,
          result);
      outputOffset += localCount;
      itemsOffset += encoding.list().num_items();
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kList &&
        type->kind() == TypeKind::ARRAY) {
      auto pageResult = decodeList(
          *this,
          type,
          encoding,
          physicalIndex,
          column,
          page,
          metadata_,
          pool_,
          localStart,
          localCount,
          itemsOffset,
          readInput);
      if (outputOffset == 0 && pageResult->size() == rowCount) {
        return pageResult;
      }
      result->copy(
          pageResult.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      itemsOffset += encoding.list().num_items();
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kList &&
        type->kind() == TypeKind::MAP) {
      auto pageResult = decodeMap(
          *this,
          type,
          encoding,
          physicalIndex,
          column,
          page,
          metadata_,
          pool_,
          localStart,
          localCount,
          itemsOffset,
          readInput);
      if (outputOffset == 0 && pageResult->size() == rowCount) {
        return pageResult;
      }
      result->copy(
          pageResult.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      itemsOffset += encoding.list().num_items();
      pageRowStart = pageRowEnd;
      continue;
    }
    auto pageResult = decodeLegacyPrimitivePage(
        type,
        logicalType,
        encoding,
        column,
        page,
        metadata_,
        localStart,
        localCount,
        pool_,
        readInput.rawRead(),
        readInput.compressedRead());
    if (outputOffset == 0 && pageResult->size() == rowCount) {
      return pageResult;
    }
    result->copy(
        pageResult.get(),
        static_cast<vector_size_t>(outputOffset),
        0,
        static_cast<vector_size_t>(localCount));
    outputOffset += localCount;
    pageRowStart = pageRowEnd;
  }

  BOLT_CHECK_EQ(
      outputOffset,
      rowCount,
      "Lance column {} contains fewer rows than requested",
      physicalIndex);
  return result;
}

} // namespace bytedance::bolt::lance::reader
