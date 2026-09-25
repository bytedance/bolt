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

#include "bolt/dwio/lance/NativeLanceLegacyBinary.h"

#include <cstring>
#include <limits>

#include <folly/lang/Bits.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using ArrayEncoding = ::lance::encodings::ArrayEncoding;

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
}

const ::lance::encodings::Flat& requireNoNullFlat(
    const ArrayEncoding& encoding,
    std::string_view role) {
  const auto* values = &encoding;
  if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
    BOLT_CHECK_EQ(
        encoding.nullable().nullability_case(),
        ::lance::encodings::Nullable::kNoNulls,
        "Native Lance binary reader requires non-null {} encoding",
        role);
    BOLT_CHECK(encoding.nullable().no_nulls().has_values());
    values = &encoding.nullable().no_nulls().values();
  }
  BOLT_CHECK_EQ(
      values->array_encoding_case(),
      ArrayEncoding::kFlat,
      "Native Lance binary reader requires Flat {} encoding",
      role);
  BOLT_CHECK(values->flat().has_buffer());
  return values->flat();
}

BufferPtr readFlatRange(
    const ::lance::encodings::Flat& flat,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
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

uint64_t normalizedStringOffset(uint64_t encoded, uint64_t nullAdjustment) {
  return encoded >= nullAdjustment ? encoded - nullAdjustment : encoded;
}

void decodeBinaryValues(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed,
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
      read,
      readCompressed);
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
      read,
      readCompressed);
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
      result->setNull(i, true);
    } else {
      output->setNoCopy(
          i,
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
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  constexpr uint64_t kFsstMagic = uint64_t{0x46535354} << 32;
  constexpr uint8_t kFsstEscape = 255;
  constexpr size_t kFsstSymbolTableSize = 8 + 256 * 8 + 256;
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
      read,
      readCompressed,
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

} // namespace

bool isLegacyBinaryEncoding(const ArrayEncoding& encoding) {
  const auto* values = &encoding;
  if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
    const auto& nullable = encoding.nullable();
    if (nullable.nullability_case() ==
        ::lance::encodings::Nullable::kAllNulls) {
      return true;
    }
    values =
        nullable.nullability_case() == ::lance::encodings::Nullable::kNoNulls
        ? &nullable.no_nulls().values()
        : &nullable.some_nulls().values();
  }
  return values->array_encoding_case() == ArrayEncoding::kBinary ||
      values->array_encoding_case() == ArrayEncoding::kFsst;
}

VectorPtr decodeLegacyBinaryPage(
    const TypePtr& type,
    const ArrayEncoding& encoding,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  const ::lance::encodings::Nullable* nullable = nullptr;
  const auto* values = &encoding;
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
        read,
        readCompressed,
        result);
  } else {
    BOLT_CHECK_EQ(values->array_encoding_case(), ArrayEncoding::kFsst);
    result = decodeFsstValues(
        type,
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

  if (nullable != nullptr &&
      nullable->nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
    const auto& validity =
        requireNoNullFlat(nullable->some_nulls().validity(), "validity");
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

} // namespace bytedance::bolt::lance::reader
