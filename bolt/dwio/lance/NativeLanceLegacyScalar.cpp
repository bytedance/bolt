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

#include "bolt/dwio/lance/NativeLanceLegacyScalar.h"

#include <bit>
#include <cstring>
#include <limits>

#include <folly/lang/Bits.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceBitmap.h"
#include "bolt/dwio/lance/NativeLanceBitpack.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
}

template <typename T>
void decodeIntegers(
    const char* source,
    uint64_t count,
    uint64_t outputOffset,
    FlatVector<T>& result) {
  auto* output = result.mutableRawValues();
  if (folly::kIsLittleEndian) {
    std::memcpy(output + outputOffset, source, count * sizeof(T));
    return;
  }
  for (uint64_t i = 0; i < count; ++i) {
    output[outputOffset + i] = readLittleEndian<T>(source + i * sizeof(T));
  }
}

template <typename T, typename UInt>
void decodeFloatingPoint(
    const char* source,
    uint64_t count,
    uint64_t outputOffset,
    FlatVector<T>& result) {
  auto* output = result.mutableRawValues();
  if (folly::kIsLittleEndian) {
    std::memcpy(output + outputOffset, source, count * sizeof(T));
    return;
  }
  for (uint64_t i = 0; i < count; ++i) {
    const auto bits = readLittleEndian<UInt>(source + i * sizeof(UInt));
    std::memcpy(output + outputOffset + i, &bits, sizeof(bits));
  }
}

int128_t readInt128(const char* source) {
  return HugeInt::build(
      readLittleEndian<uint64_t>(source + sizeof(uint64_t)),
      readLittleEndian<uint64_t>(source));
}

float halfToFloat(uint16_t half) {
  const auto sign = static_cast<uint32_t>(half & 0x8000) << 16;
  auto exponent = static_cast<uint32_t>((half >> 10) & 0x1f);
  auto mantissa = static_cast<uint32_t>(half & 0x03ff);
  uint32_t bits;
  if (exponent == 0) {
    if (mantissa == 0) {
      bits = sign;
    } else {
      int32_t normalizedExponent = -14;
      while ((mantissa & 0x0400) == 0) {
        mantissa <<= 1;
        --normalizedExponent;
      }
      mantissa &= 0x03ff;
      bits = sign | static_cast<uint32_t>(normalizedExponent + 127) << 23 |
          mantissa << 13;
    }
  } else if (exponent == 0x1f) {
    bits = sign | 0x7f800000 | mantissa << 13;
  } else {
    bits = sign | (exponent + 112) << 23 | mantissa << 13;
  }
  return std::bit_cast<float>(bits);
}

template <typename T>
VectorPtr wrapRawFlatValues(
    const TypePtr& type,
    BufferPtr values,
    uint64_t count,
    memory::MemoryPool& pool) {
  BOLT_CHECK_LE(
      count, static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
  return std::make_shared<FlatVector<T>>(
      &pool,
      type,
      nullptr,
      static_cast<vector_size_t>(count),
      std::move(values),
      std::vector<BufferPtr>{});
}

const ::lance::encodings::Flat& requireFlat(
    const ::lance::encodings::ArrayEncoding& encoding,
    std::string_view role) {
  BOLT_CHECK_EQ(
      encoding.array_encoding_case(),
      ::lance::encodings::ArrayEncoding::kFlat,
      "Native Lance scalar page requires Flat {} encoding",
      role);
  const auto& flat = encoding.flat();
  BOLT_CHECK(flat.has_buffer(), "Lance Flat {} encoding has no buffer", role);
  return flat;
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
    VectorPtr& result) {
  BOLT_CHECK(
      result->typeKind() == TypeKind::VARCHAR ||
      result->typeKind() == TypeKind::VARBINARY);
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

} // namespace

VectorPtr decodeLegacyPrimitivePage(
    const TypePtr& type,
    std::string_view logicalType,
    const ::lance::encodings::ArrayEncoding& encoding,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed) {
  using ArrayEncoding = ::lance::encodings::ArrayEncoding;
  auto result =
      BaseVector::create(type, static_cast<vector_size_t>(localCount), &pool);
  const ArrayEncoding* values = &encoding;
  if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
    const auto& nullable = encoding.nullable();
    switch (nullable.nullability_case()) {
      case ::lance::encodings::Nullable::kNoNulls:
        BOLT_CHECK(nullable.no_nulls().has_values());
        values = &nullable.no_nulls().values();
        break;
      case ::lance::encodings::Nullable::kSomeNulls: {
        BOLT_CHECK(nullable.some_nulls().has_validity());
        BOLT_CHECK(nullable.some_nulls().has_values());
        values = &nullable.some_nulls().values();
        const auto& validity =
            requireFlat(nullable.some_nulls().validity(), "validity");
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
        break;
      }
      case ::lance::encodings::Nullable::kAllNulls:
        setLegacyAllNull(0, localCount, *result);
        return result;
      default:
        BOLT_FAIL("Lance Nullable encoding has no nullability mode");
    }
  }

  if (values->array_encoding_case() == ArrayEncoding::kBitpacked) {
    decodeLegacyBitpackedValues(
        type,
        logicalType,
        values->bitpacked(),
        column,
        page,
        metadata,
        localStart,
        localCount,
        0,
        pool,
        read,
        result);
    return result;
  }
  if (values->array_encoding_case() == ArrayEncoding::kBitpackedForNonNeg) {
    decodeLegacyBitpackedForNonNegValues(
        type,
        logicalType,
        values->bitpacked_for_non_neg(),
        column,
        page,
        metadata,
        localStart,
        localCount,
        0,
        pool,
        read,
        result);
    return result;
  }
  if (values->array_encoding_case() == ArrayEncoding::kFixedSizeBinary) {
    const auto& fixed = values->fixed_size_binary();
    BOLT_CHECK_GT(fixed.byte_width(), 0);
    BOLT_CHECK(fixed.has_bytes());
    const auto& bytesEncoding = requireFlat(fixed.bytes(), "fixed-size bytes");
    BOLT_CHECK_EQ(bytesEncoding.bits_per_value(), fixed.byte_width() * 8);
    const auto bytes = readFlatRange(
        bytesEncoding,
        column,
        page,
        metadata,
        localStart * fixed.byte_width(),
        localCount * fixed.byte_width(),
        read,
        readCompressed);
    setFixedSizeBinaryViews(bytes, fixed.byte_width(), localCount, result);
    return result;
  }

  const auto& flat = requireFlat(*values, "values");
  if (type->kind() == TypeKind::BOOLEAN) {
    BOLT_CHECK_EQ(flat.bits_per_value(), 1);
    const auto firstByte = localStart / 8;
    const auto endByte = (localStart + localCount + 7) / 8;
    const auto bytes = readFlatRange(
        flat,
        column,
        page,
        metadata,
        firstByte,
        endByte - firstByte,
        read,
        readCompressed);
    decodeLegacyBitmapValues(
        bytes->as<char>(), localStart - firstByte * 8, localCount, 0, result);
  } else if (
      logicalType.rfind("fixed_size_binary:", 0) == 0 ||
      logicalType == "lance.bfloat16") {
    const auto byteWidth =
        logicalType == "lance.bfloat16" ? 2 : fixedSizeBinaryWidth(logicalType);
    BOLT_CHECK_EQ(flat.bits_per_value(), byteWidth * 8);
    const auto bytes = readFlatRange(
        flat,
        column,
        page,
        metadata,
        localStart * byteWidth,
        localCount * byteWidth,
        read,
        readCompressed);
    setFixedSizeBinaryViews(bytes, byteWidth, localCount, result);
  } else {
    BOLT_CHECK_EQ(flat.bits_per_value() % 8, 0);
    const auto bytesPerValue = flat.bits_per_value() / 8;
    BOLT_CHECK_GT(bytesPerValue, 0);
    const auto bytes = readFlatRange(
        flat,
        column,
        page,
        metadata,
        localStart * bytesPerValue,
        localCount * bytesPerValue,
        read,
        readCompressed);
    const auto allValid =
        encoding.array_encoding_case() != ArrayEncoding::kNullable ||
        encoding.nullable().nullability_case() ==
            ::lance::encodings::Nullable::kNoNulls;
    if (allValid) {
      if (auto wrapped = tryWrapLegacyRawFlatValues(
              type,
              logicalType,
              flat.bits_per_value(),
              bytes,
              localCount,
              pool)) {
        return wrapped;
      }
    }
    decodeLegacyFixedWidthValues(
        type,
        logicalType,
        flat.bits_per_value(),
        bytes->as<char>(),
        localCount,
        0,
        result);
  }
  return result;
}

void decodeLegacyFixedWidthValues(
    const TypePtr& type,
    std::string_view logicalType,
    uint64_t bitsPerValue,
    const char* source,
    uint64_t count,
    uint64_t outputOffset,
    VectorPtr& result) {
  if (type->isShortDecimal()) {
    BOLT_CHECK_EQ(bitsPerValue, 128);
    auto* output = result->asFlatVector<int64_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      const auto value = readInt128(source + i * sizeof(int128_t));
      BOLT_CHECK_GE(value, std::numeric_limits<int64_t>::min());
      BOLT_CHECK_LE(value, std::numeric_limits<int64_t>::max());
      output[outputOffset + i] = static_cast<int64_t>(value);
    }
    return;
  }
  if (type->isLongDecimal()) {
    BOLT_CHECK_EQ(bitsPerValue, 128);
    auto* output = result->asFlatVector<int128_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      output[outputOffset + i] = readInt128(source + i * sizeof(int128_t));
    }
    return;
  }
  if (logicalType == "uint8") {
    BOLT_CHECK_EQ(bitsPerValue, 8);
    auto* output = result->asFlatVector<int16_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      output[outputOffset + i] = static_cast<uint8_t>(source[i]);
    }
    return;
  }
  if (logicalType == "uint16") {
    BOLT_CHECK_EQ(bitsPerValue, 16);
    auto* output = result->asFlatVector<int32_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      output[outputOffset + i] =
          readLittleEndian<uint16_t>(source + i * sizeof(uint16_t));
    }
    return;
  }
  if (logicalType == "uint32") {
    BOLT_CHECK_EQ(bitsPerValue, 32);
    auto* output = result->asFlatVector<int64_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      output[outputOffset + i] =
          readLittleEndian<uint32_t>(source + i * sizeof(uint32_t));
    }
    return;
  }
  if (logicalType == "uint64") {
    BOLT_CHECK_EQ(bitsPerValue, 64);
    auto* output = result->asFlatVector<int128_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      output[outputOffset + i] = static_cast<int128_t>(
          readLittleEndian<uint64_t>(source + i * sizeof(uint64_t)));
    }
    return;
  }
  if (logicalType == "halffloat") {
    BOLT_CHECK_EQ(bitsPerValue, 16);
    auto* output = result->asFlatVector<float>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      output[outputOffset + i] = halfToFloat(
          readLittleEndian<uint16_t>(source + i * sizeof(uint16_t)));
    }
    return;
  }
  if (logicalType == "date64:ms") {
    BOLT_CHECK_EQ(bitsPerValue, 64);
    constexpr int64_t kMillisPerDay = 86'400'000;
    auto* output = result->asFlatVector<int32_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      const auto millis =
          readLittleEndian<int64_t>(source + i * sizeof(int64_t));
      BOLT_CHECK_EQ(
          millis % kMillisPerDay,
          0,
          "Lance date64 value is not aligned to a whole day");
      const auto days = millis / kMillisPerDay;
      BOLT_CHECK_GE(days, std::numeric_limits<int32_t>::min());
      BOLT_CHECK_LE(days, std::numeric_limits<int32_t>::max());
      output[outputOffset + i] = static_cast<int32_t>(days);
    }
    return;
  }
  if (logicalType.rfind("time32:", 0) == 0) {
    BOLT_CHECK_EQ(bitsPerValue, 32);
    auto* output = result->asFlatVector<int64_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      output[outputOffset + i] =
          readLittleEndian<int32_t>(source + i * sizeof(int32_t));
    }
    return;
  }
  if (logicalType.rfind("time64:", 0) == 0) {
    BOLT_CHECK_EQ(bitsPerValue, 64);
    decodeIntegers<int64_t>(
        source, count, outputOffset, *result->asFlatVector<int64_t>());
    return;
  }
  if (logicalType.rfind("duration:", 0) == 0) {
    BOLT_CHECK_EQ(bitsPerValue, 64);
    auto* output = result->asFlatVector<int64_t>()->mutableRawValues();
    for (uint64_t i = 0; i < count; ++i) {
      const auto value =
          readLittleEndian<int64_t>(source + i * sizeof(int64_t));
      if (logicalType == "duration:s") {
        BOLT_CHECK_LE(
            value, std::numeric_limits<int64_t>::max() / kMillisInSecond);
        BOLT_CHECK_GE(
            value, std::numeric_limits<int64_t>::min() / kMillisInSecond);
        output[outputOffset + i] = value * kMillisInSecond;
      } else if (logicalType == "duration:ms") {
        output[outputOffset + i] = value;
      } else {
        const auto divisor = logicalType == "duration:us" ? 1'000 : 1'000'000;
        BOLT_CHECK_EQ(
            value % divisor,
            0,
            "Lance {} value cannot be represented losslessly in Bolt milliseconds",
            logicalType);
        output[outputOffset + i] = value / divisor;
      }
    }
    return;
  }

  switch (type->kind()) {
    case TypeKind::TINYINT:
      BOLT_CHECK_EQ(bitsPerValue, 8);
      decodeIntegers<int8_t>(
          source, count, outputOffset, *result->asFlatVector<int8_t>());
      return;
    case TypeKind::SMALLINT:
      BOLT_CHECK_EQ(bitsPerValue, 16);
      decodeIntegers<int16_t>(
          source, count, outputOffset, *result->asFlatVector<int16_t>());
      return;
    case TypeKind::INTEGER:
      BOLT_CHECK_EQ(bitsPerValue, 32);
      decodeIntegers<int32_t>(
          source, count, outputOffset, *result->asFlatVector<int32_t>());
      return;
    case TypeKind::BIGINT:
      BOLT_CHECK_EQ(bitsPerValue, 64);
      decodeIntegers<int64_t>(
          source, count, outputOffset, *result->asFlatVector<int64_t>());
      return;
    case TypeKind::REAL:
      BOLT_CHECK_EQ(bitsPerValue, 32);
      decodeFloatingPoint<float, uint32_t>(
          source, count, outputOffset, *result->asFlatVector<float>());
      return;
    case TypeKind::DOUBLE:
      BOLT_CHECK_EQ(bitsPerValue, 64);
      decodeFloatingPoint<double, uint64_t>(
          source, count, outputOffset, *result->asFlatVector<double>());
      return;
    case TypeKind::TIMESTAMP: {
      BOLT_CHECK_EQ(bitsPerValue, 64);
      auto* output = result->asFlatVector<Timestamp>()->mutableRawValues();
      for (uint64_t i = 0; i < count; ++i) {
        const auto value =
            readLittleEndian<int64_t>(source + i * sizeof(int64_t));
        if (logicalType.rfind("timestamp:s:", 0) == 0) {
          output[outputOffset + i] = Timestamp(value, 0);
        } else if (logicalType.rfind("timestamp:ms:", 0) == 0) {
          output[outputOffset + i] = Timestamp::fromMillis(value);
        } else if (logicalType.rfind("timestamp:us:", 0) == 0) {
          output[outputOffset + i] = Timestamp::fromMicros(value);
        } else if (logicalType.rfind("timestamp:ns:", 0) == 0) {
          output[outputOffset + i] = Timestamp::fromNanos(value);
        } else {
          BOLT_UNSUPPORTED(
              "Unsupported Lance timestamp logical type: {}", logicalType);
        }
      }
      return;
    }
    default:
      BOLT_UNSUPPORTED(
          "Unsupported native Lance fixed-width output type: {}",
          type->toString());
  }
}

void decodeLegacyBitmapValues(
    const char* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t outputOffset,
    VectorPtr& result) {
  auto* output = result->asFlatVector<bool>();
  auto* rawOutput = output->mutableRawValues<uint64_t>();
  copyLanceBitmap(
      reinterpret_cast<const uint8_t*>(source),
      sourceBitOffset,
      count,
      rawOutput,
      outputOffset);
}

void applyLegacyValidityBitmap(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t outputOffset,
    BaseVector& result) {
  if (count == 0) {
    return;
  }
  if (result.rawNulls() == nullptr &&
      lanceBitmapIsAllSet(source, sourceBitOffset, count)) {
    return;
  }
  copyLanceBitmap(
      source, sourceBitOffset, count, result.mutableRawNulls(), outputOffset);
}

void setLegacyAllNull(
    uint64_t outputOffset,
    uint64_t count,
    BaseVector& result) {
  bits::fillBits(
      result.mutableRawNulls(),
      outputOffset,
      outputOffset + count,
      bits::kNull);
}

void decodeLegacyBitpackedValues(
    const TypePtr& type,
    std::string_view logicalType,
    const ::lance::encodings::Bitpacked& bitpacked,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    VectorPtr& result) {
  const auto compressedBits = bitpacked.compressed_bits_per_value();
  const auto uncompressedBits = bitpacked.uncompressed_bits_per_value();
  BOLT_CHECK_LE(compressedBits, uncompressedBits);
  BOLT_CHECK(
      uncompressedBits == 8 || uncompressedBits == 16 ||
          uncompressedBits == 32 || uncompressedBits == 64,
      "Unsupported Lance bit-packed output width: {}",
      uncompressedBits);
  const auto bytesPerValue = uncompressedBits / 8;
  auto unpacked =
      AlignedBuffer::allocate<char>(localCount * bytesPerValue, &pool);
  if (compressedBits == 0) {
    std::memset(unpacked->asMutable<char>(), 0, unpacked->size());
  } else {
    BOLT_CHECK(bitpacked.has_buffer());
    const auto buffer =
        metadata.resolveBuffer(bitpacked.buffer(), column, page);
    const auto firstBit = localStart * compressedBits;
    const auto endBit = (localStart + localCount) * compressedBits;
    const auto firstByte = firstBit / 8;
    const auto endByte = (endBit + 7) / 8;
    BOLT_CHECK_LE(firstByte, buffer.length);
    BOLT_CHECK_LE(endByte, buffer.length);
    const auto packed = read(buffer.offset + firstByte, endByte - firstByte);
    decodeLanceBitpacked(
        packed->as<uint8_t>(),
        packed->size(),
        firstBit - firstByte * 8,
        localCount,
        compressedBits,
        uncompressedBits,
        bitpacked.signed_(),
        unpacked->asMutable<uint8_t>());
  }
  decodeLegacyFixedWidthValues(
      type,
      logicalType,
      uncompressedBits,
      unpacked->as<char>(),
      localCount,
      outputOffset,
      result);
}

void decodeLegacyBitpackedForNonNegValues(
    const TypePtr& type,
    std::string_view logicalType,
    const ::lance::encodings::BitpackedForNonNeg& bitpacked,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    VectorPtr& result) {
  constexpr uint64_t kChunkRows = 1'024;
  const auto compressedBits = bitpacked.compressed_bits_per_value();
  const auto uncompressedBits = bitpacked.uncompressed_bits_per_value();
  BOLT_CHECK_LE(compressedBits, uncompressedBits);
  BOLT_CHECK(
      uncompressedBits == 8 || uncompressedBits == 16 ||
          uncompressedBits == 32 || uncompressedBits == 64,
      "Unsupported Lance BitpackedForNonNeg output width: {}",
      uncompressedBits);
  const auto bytesPerValue = uncompressedBits / 8;
  auto unpacked =
      AlignedBuffer::allocate<char>(localCount * bytesPerValue, &pool);
  if (compressedBits == 0) {
    std::memset(unpacked->asMutable<char>(), 0, unpacked->size());
  } else {
    BOLT_CHECK(bitpacked.has_buffer());
    const auto descriptor =
        metadata.resolveBuffer(bitpacked.buffer(), column, page);
    const auto chunkBytes = kChunkRows * compressedBits / 8;
    const auto firstChunk = localStart / kChunkRows;
    const auto endChunk =
        (localStart + localCount + kChunkRows - 1) / kChunkRows;
    const auto firstByte = firstChunk * chunkBytes;
    const auto endByte = endChunk * chunkBytes;
    BOLT_CHECK_LE(firstByte, descriptor.length);
    BOLT_CHECK_LE(endByte, descriptor.length);
    const auto packed =
        read(descriptor.offset + firstByte, endByte - firstByte);
    decodeLanceBitpackedForNonNeg(
        packed->as<uint8_t>(),
        packed->size(),
        localStart - firstChunk * kChunkRows,
        localCount,
        compressedBits,
        uncompressedBits,
        unpacked->asMutable<uint8_t>());
  }
  decodeLegacyFixedWidthValues(
      type,
      logicalType,
      uncompressedBits,
      unpacked->as<char>(),
      localCount,
      outputOffset,
      result);
}

VectorPtr tryWrapLegacyRawFlatValues(
    const TypePtr& type,
    std::string_view logicalType,
    uint64_t bitsPerValue,
    BufferPtr values,
    uint64_t count,
    memory::MemoryPool& pool) {
  if (!folly::kIsLittleEndian) {
    return nullptr;
  }
  if (logicalType == "int8" && type->kind() == TypeKind::TINYINT &&
      bitsPerValue == 8) {
    return wrapRawFlatValues<int8_t>(type, std::move(values), count, pool);
  }
  if (logicalType == "int16" && type->kind() == TypeKind::SMALLINT &&
      bitsPerValue == 16) {
    return wrapRawFlatValues<int16_t>(type, std::move(values), count, pool);
  }
  if (logicalType == "int32" && type->kind() == TypeKind::INTEGER &&
      bitsPerValue == 32) {
    return wrapRawFlatValues<int32_t>(type, std::move(values), count, pool);
  }
  if (logicalType == "int64" && type->kind() == TypeKind::BIGINT &&
      bitsPerValue == 64) {
    return wrapRawFlatValues<int64_t>(type, std::move(values), count, pool);
  }
  if (logicalType == "float" && type->kind() == TypeKind::REAL &&
      bitsPerValue == 32) {
    return wrapRawFlatValues<float>(type, std::move(values), count, pool);
  }
  if (logicalType == "double" && type->kind() == TypeKind::DOUBLE &&
      bitsPerValue == 64) {
    return wrapRawFlatValues<double>(type, std::move(values), count, pool);
  }
  return nullptr;
}

} // namespace bytedance::bolt::lance::reader
