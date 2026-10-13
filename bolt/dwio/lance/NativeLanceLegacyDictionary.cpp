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

#include "bolt/dwio/lance/NativeLanceLegacyDictionary.h"

#include <cstring>
#include <limits>

#include <folly/lang/Bits.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceBitpack.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"

namespace bytedance::bolt::lance::reader {
namespace {

using ArrayEncoding = ::lance::encodings::ArrayEncoding;

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
      "Native Lance dictionary reader requires non-null {} encoding",
      role);
  BOLT_CHECK(encoding.nullable().no_nulls().has_values());
  return encoding.nullable().no_nulls().values();
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
  BOLT_CHECK(flat.has_buffer());
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

BufferPtr decodeDictionaryIndices(
    const ArrayEncoding& encodedIndices,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  const auto& indices = unwrapNoNullEncoding(encodedIndices, "indices");
  auto result = AlignedBuffer::allocate<uint64_t>(localCount, &pool);
  auto* rawResult = result->asMutable<uint64_t>();
  if (indices.array_encoding_case() == ArrayEncoding::kFlat) {
    const auto& flat = indices.flat();
    BOLT_CHECK(flat.has_buffer());
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
        read,
        readCompressed);
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

} // namespace

VectorPtr decodeLegacyDictionaryPage(
    const TypePtr& type,
    std::string_view logicalType,
    const ArrayEncoding& encoding,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  BOLT_CHECK_EQ(encoding.array_encoding_case(), ArrayEncoding::kDictionary);
  const auto& dictionary = encoding.dictionary();
  BOLT_CHECK(dictionary.has_indices());
  BOLT_CHECK(dictionary.has_items());
  BOLT_CHECK_GT(dictionary.num_dictionary_items(), 0);
  BOLT_CHECK_LE(
      dictionary.num_dictionary_items(),
      static_cast<uint32_t>(std::numeric_limits<vector_size_t>::max()));

  const auto valueLogicalType = logicalType.rfind("dict:", 0) == 0
      ? nativeLanceDictionaryValueLogicalType(logicalType)
      : logicalType;
  const auto encodedIndices = decodeDictionaryIndices(
      dictionary.indices(),
      column,
      page,
      metadata,
      localStart,
      localCount,
      pool,
      read,
      readCompressed);
  auto dictionaryValues = isLegacyBinaryEncoding(dictionary.items())
      ? decodeLegacyBinaryPage(
            type,
            dictionary.items(),
            column,
            page,
            metadata,
            0,
            dictionary.num_dictionary_items(),
            pool,
            read,
            readCompressed)
      : decodeLegacyPrimitivePage(
            type,
            valueLogicalType,
            dictionary.items(),
            column,
            page,
            metadata,
            0,
            dictionary.num_dictionary_items(),
            pool,
            read,
            readCompressed);

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

} // namespace bytedance::bolt::lance::reader
