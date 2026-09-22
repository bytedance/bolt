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

#include "bolt/dwio/lance/NativeLanceStructuralDecoder.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <numeric>
#include <optional>
#include <unordered_map>
#include <vector>

#include <folly/Portability.h>
#include <folly/lang/Bits.h>
#include <lz4.h>
#include <zstd.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceBitpack.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using CompressiveEncoding = ::lance::encodings21::CompressiveEncoding;
using Compression = ::lance::encodings21::CompressiveEncoding::CompressionCase;
using Page = ::lance::file::v2::ColumnMetadata::Page;
using RepDefLayer = ::lance::encodings21::RepDefLayer;

constexpr uint64_t kChunkRows = 1'024;
constexpr uint64_t kMiniBlockAlignment = 8;

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

uint32_t fixedSizeBinaryWidth(std::string_view logicalType) {
  constexpr auto kPrefix = std::string_view("fixed_size_binary:");
  BOLT_CHECK_EQ(logicalType.rfind(kPrefix, 0), 0);
  const auto width =
      std::stoull(std::string(logicalType.substr(kPrefix.size())));
  BOLT_CHECK_GT(width, 0);
  BOLT_CHECK_LE(width, std::numeric_limits<int32_t>::max());
  return static_cast<uint32_t>(width);
}

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
}

uint64_t alignUp(uint64_t value, uint64_t alignment) {
  BOLT_CHECK_GT(alignment, 0);
  return (value + alignment - 1) / alignment * alignment;
}

BufferPtr
copyBuffer(const char* data, uint64_t size, memory::MemoryPool& pool) {
  auto result = AlignedBuffer::allocate<char>(size, &pool);
  if (size > 0) {
    std::memcpy(result->asMutable<char>(), data, size);
  }
  return result;
}

BufferPtr decompressBuffer(
    const ::lance::encodings21::BufferCompression* compression,
    const BufferPtr& input,
    memory::MemoryPool& pool) {
  if (compression == nullptr ||
      compression->scheme() ==
          ::lance::encodings21::COMPRESSION_ALGORITHM_UNSPECIFIED) {
    return input;
  }
  const auto* source = input->as<char>();
  auto sourceSize = input->size();
  if (compression->scheme() ==
      ::lance::encodings21::COMPRESSION_ALGORITHM_ZSTD) {
    constexpr uint32_t kZstdMagic = 0xFD2FB528;
    const auto rawFrame = sourceSize >= sizeof(uint32_t) &&
        readLittleEndian<uint32_t>(source) == kZstdMagic;
    uint64_t outputSize = 0;
    if (rawFrame) {
      outputSize = ZSTD_getFrameContentSize(source, sourceSize);
      BOLT_CHECK_NE(outputSize, ZSTD_CONTENTSIZE_ERROR);
      BOLT_CHECK_NE(outputSize, ZSTD_CONTENTSIZE_UNKNOWN);
    } else {
      BOLT_CHECK_GE(sourceSize, sizeof(uint64_t));
      outputSize = readLittleEndian<uint64_t>(source);
      source += sizeof(uint64_t);
      sourceSize -= sizeof(uint64_t);
    }
    auto output = AlignedBuffer::allocate<char>(outputSize, &pool);
    const auto decoded = ZSTD_decompress(
        output->asMutable<char>(), outputSize, source, sourceSize);
    BOLT_CHECK(!ZSTD_isError(decoded), "Invalid Lance zstd block");
    BOLT_CHECK_EQ(decoded, outputSize);
    return output;
  }
  if (compression->scheme() ==
      ::lance::encodings21::COMPRESSION_ALGORITHM_LZ4) {
    BOLT_CHECK_GE(sourceSize, sizeof(uint32_t));
    const auto outputSize = readLittleEndian<uint32_t>(source);
    source += sizeof(uint32_t);
    sourceSize -= sizeof(uint32_t);
    BOLT_CHECK_LE(outputSize, std::numeric_limits<int>::max());
    BOLT_CHECK_LE(sourceSize, std::numeric_limits<int>::max());
    auto output = AlignedBuffer::allocate<char>(outputSize, &pool);
    const auto decoded = LZ4_decompress_safe(
        source,
        output->asMutable<char>(),
        static_cast<int>(sourceSize),
        static_cast<int>(outputSize));
    BOLT_CHECK_GE(decoded, 0, "Invalid Lance lz4 block");
    BOLT_CHECK_EQ(decoded, outputSize);
    return output;
  }
  BOLT_UNSUPPORTED(
      "Unsupported Lance structural buffer compression {}",
      static_cast<int>(compression->scheme()));
}

struct DecodedBlock {
  enum class Kind { kFixed, kVariable };
  Kind kind;
  BufferPtr data;
  uint64_t bitsPerValue{0};
  std::vector<uint64_t> offsets;
  std::vector<std::vector<bool>> fixedSizeValidities;
  std::vector<uint64_t> fixedSizeDimensions;
  std::vector<uint64_t> packedChildBits;
  std::vector<DecodedBlock> packedChildren;
  bool allNull{false};

  uint64_t size() const {
    if (allNull) {
      return offsets.empty() ? 0 : offsets.back();
    }
    BOLT_CHECK(kind != Kind::kFixed || bitsPerValue > 0);
    return kind == Kind::kFixed ? data->size() * 8 / bitsPerValue
                                : offsets.size() - 1;
  }
};

DecodedBlock decodeCompressive(
    const CompressiveEncoding& encoding,
    const std::vector<BufferPtr>& inputs,
    uint64_t numValues,
    memory::MemoryPool& pool);

DecodedBlock decodeFixedSizeList(
    const ::lance::encodings21::FixedSizeList& fixedSizeList,
    const std::vector<BufferPtr>& inputs,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK_GT(fixedSizeList.items_per_value(), 0);
  BOLT_CHECK(fixedSizeList.has_values());
  const auto dimension = fixedSizeList.items_per_value();
  BOLT_CHECK_LE(numValues, std::numeric_limits<uint64_t>::max() / dimension);
  const auto childValues = numValues * dimension;
  size_t inputOffset = 0;
  std::vector<bool> validity;
  if (fixedSizeList.has_validity()) {
    BOLT_CHECK(!inputs.empty());
    BOLT_CHECK_GE(inputs.front()->size() * 8, childValues);
    validity.reserve(childValues);
    for (uint64_t i = 0; i < childValues; ++i) {
      validity.push_back(bits::isBitSet(inputs.front()->as<uint64_t>(), i));
    }
    ++inputOffset;
  }
  BOLT_CHECK(
      inputOffset < inputs.size() ||
      fixedSizeList.values().compression_case() ==
          CompressiveEncoding::kConstant);
  std::vector<BufferPtr> childInputs(
      inputs.begin() + inputOffset, inputs.end());
  auto child =
      decodeCompressive(fixedSizeList.values(), childInputs, childValues, pool);
  child.fixedSizeDimensions.insert(
      child.fixedSizeDimensions.begin(), dimension);
  child.fixedSizeValidities.insert(
      child.fixedSizeValidities.begin(), std::move(validity));
  return child;
}

DecodedBlock decodeFlat(
    const ::lance::encodings21::Flat& flat,
    const BufferPtr& input,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK_GT(flat.bits_per_value(), 0);
  auto data =
      decompressBuffer(flat.has_data() ? &flat.data() : nullptr, input, pool);
  BOLT_CHECK_GE(data->size() * 8, numValues * flat.bits_per_value());
  return {
      DecodedBlock::Kind::kFixed, std::move(data), flat.bits_per_value(), {}};
}

DecodedBlock decodeInlineBitpacking(
    const ::lance::encodings21::InlineBitpacking& bitpacked,
    const BufferPtr& input,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  const auto outputBits = bitpacked.uncompressed_bits_per_value();
  BOLT_CHECK(
      outputBits == 8 || outputBits == 16 || outputBits == 32 ||
      outputBits == 64);
  auto bytes = decompressBuffer(
      bitpacked.has_values() ? &bitpacked.values() : nullptr, input, pool);
  const auto wordBytes = outputBits / 8;
  BOLT_CHECK_GE(bytes->size(), wordBytes);
  uint64_t compressedBits = 0;
  switch (outputBits) {
    case 8:
      compressedBits = bytes->as<uint8_t>()[0];
      break;
    case 16:
      compressedBits = readLittleEndian<uint16_t>(bytes->as<char>());
      break;
    case 32:
      compressedBits = readLittleEndian<uint32_t>(bytes->as<char>());
      break;
    case 64:
      compressedBits = readLittleEndian<uint64_t>(bytes->as<char>());
      break;
  }
  BOLT_CHECK_LE(compressedBits, outputBits);
  auto output = AlignedBuffer::allocate<char>(numValues * wordBytes, &pool);
  if (compressedBits == 0) {
    std::memset(output->asMutable<char>(), 0, output->size());
  } else {
    decodeLanceBitpackedForNonNeg(
        bytes->as<uint8_t>() + wordBytes,
        bytes->size() - wordBytes,
        0,
        numValues,
        compressedBits,
        outputBits,
        output->asMutable<uint8_t>());
  }
  return {DecodedBlock::Kind::kFixed, std::move(output), outputBits, {}};
}

DecodedBlock decodeOutOfLineBitpacking(
    const ::lance::encodings21::OutOfLineBitpacking& bitpacked,
    const BufferPtr& input,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(bitpacked.has_values());
  BOLT_CHECK_EQ(
      bitpacked.values().compression_case(), CompressiveEncoding::kFlat);
  const auto outputBits = bitpacked.uncompressed_bits_per_value();
  const auto compressedBits = bitpacked.values().flat().bits_per_value();
  BOLT_CHECK(
      outputBits == 8 || outputBits == 16 || outputBits == 32 ||
      outputBits == 64);
  BOLT_CHECK_LE(compressedBits, outputBits);
  auto packed = decompressBuffer(
      bitpacked.values().flat().has_data() ? &bitpacked.values().flat().data()
                                           : nullptr,
      input,
      pool);
  const auto wordBytes = outputBits / 8;
  const auto chunkBytes = kChunkRows * compressedBits / 8;
  const auto wholeChunks = numValues / kChunkRows;
  const auto tailValues = numValues % kChunkRows;
  const auto rawTailBytes = tailValues * wordBytes;
  const auto tailIsRaw = tailValues > 0 &&
      packed->size() == wholeChunks * chunkBytes + rawTailBytes;
  const auto expectedBytes = wholeChunks * chunkBytes +
      (tailValues == 0 ? 0
           : tailIsRaw ? rawTailBytes
                       : chunkBytes);
  BOLT_CHECK_EQ(packed->size(), expectedBytes);
  auto output = AlignedBuffer::allocate<char>(numValues * wordBytes, &pool);
  if (wholeChunks > 0) {
    decodeLanceBitpackedForNonNeg(
        packed->as<uint8_t>(),
        wholeChunks * chunkBytes,
        0,
        wholeChunks * kChunkRows,
        compressedBits,
        outputBits,
        output->asMutable<uint8_t>());
  }
  if (tailValues > 0) {
    const auto inputOffset = wholeChunks * chunkBytes;
    const auto outputOffset = wholeChunks * kChunkRows * wordBytes;
    if (tailIsRaw) {
      std::memcpy(
          output->asMutable<char>() + outputOffset,
          packed->as<char>() + inputOffset,
          rawTailBytes);
    } else {
      decodeLanceBitpackedForNonNeg(
          packed->as<uint8_t>() + inputOffset,
          chunkBytes,
          0,
          tailValues,
          compressedBits,
          outputBits,
          output->asMutable<uint8_t>() + outputOffset);
    }
  }
  return {DecodedBlock::Kind::kFixed, std::move(output), outputBits, {}};
}

DecodedBlock decodeVariable(
    const ::lance::encodings21::Variable& variable,
    const BufferPtr& input,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(variable.has_offsets());
  BOLT_CHECK_EQ(
      variable.offsets().compression_case(), CompressiveEncoding::kFlat);
  const auto offsetBits = variable.offsets().flat().bits_per_value();
  BOLT_CHECK(offsetBits == 32 || offsetBits == 64);
  const auto offsetBytes = offsetBits / 8;
  BOLT_CHECK_GE(input->size(), offsetBytes);
  uint64_t offsetStart = 0;
  uint64_t valueStart = 0;
  // Variable is used both as a mini-block compressor (offsets start at byte
  // zero) and as a block compressor (word width and value offset form a
  // two-word header).
  const auto firstWord = offsetBits == 32
      ? static_cast<uint64_t>(readLittleEndian<uint32_t>(input->as<char>()))
      : readLittleEndian<uint64_t>(input->as<char>());
  if (firstWord == offsetBits) {
    BOLT_CHECK_GE(input->size(), 2 * offsetBytes);
    offsetStart = 2 * offsetBytes;
    valueStart = offsetBits == 32
        ? static_cast<uint64_t>(
              readLittleEndian<uint32_t>(input->as<char>() + offsetBytes))
        : readLittleEndian<uint64_t>(input->as<char>() + offsetBytes);
  }
  BOLT_CHECK_LE(offsetStart + (numValues + 1) * offsetBytes, input->size());
  std::vector<uint64_t> offsets(numValues + 1);
  for (uint64_t i = 0; i <= numValues; ++i) {
    offsets[i] = offsetBits == 32
        ? readLittleEndian<uint32_t>(
              input->as<char>() + offsetStart + i * offsetBytes)
        : readLittleEndian<uint64_t>(
              input->as<char>() + offsetStart + i * offsetBytes);
  }
  if (offsetStart == 0) {
    valueStart = offsets.front();
  }
  BOLT_CHECK_GE(valueStart, offsetStart + (numValues + 1) * offsetBytes);
  BOLT_CHECK_LE(offsets.back(), input->size());
  for (uint64_t i = 1; i < offsets.size(); ++i) {
    BOLT_CHECK_LE(offsets[i - 1], offsets[i]);
  }
  auto values = copyBuffer(
      input->as<char>() + valueStart,
      offsetStart == 0 ? offsets.back() - valueStart : offsets.back(),
      pool);
  if (offsetStart == 0) {
    for (auto& offset : offsets) {
      offset -= valueStart;
    }
  }
  if (variable.has_values()) {
    values = decompressBuffer(&variable.values(), values, pool);
  }
  return {
      DecodedBlock::Kind::kVariable, std::move(values), 0, std::move(offsets)};
}

DecodedBlock decodeFsstBlock(
    const ::lance::encodings21::Fsst& fsst,
    DecodedBlock compressed,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  constexpr uint64_t kFsstMagic = uint64_t{0x46535354} << 32;
  constexpr uint8_t kFsstEscape = 255;
  constexpr size_t kFsstSymbolTableSize = 8 + 256 * 8 + 256;
  BOLT_CHECK_EQ(fsst.symbol_table().size(), kFsstSymbolTableSize);
  const auto* table = fsst.symbol_table().data();
  const auto header = readLittleEndian<uint64_t>(table);
  BOLT_CHECK_EQ(
      header & 0xffffffff00000000ULL,
      kFsstMagic,
      "Invalid Lance FSST symbol-table magic");

  BOLT_CHECK(compressed.kind == DecodedBlock::Kind::kVariable);
  if ((header & (uint64_t{1} << 24)) == 0) {
    return compressed;
  }
  const auto numSymbols = static_cast<uint8_t>(header & 0xff);
  const auto* symbols = table + sizeof(uint64_t);
  const auto* lengths =
      reinterpret_cast<const uint8_t*>(symbols + numSymbols * sizeof(uint64_t));
  uint64_t outputSize = 0;
  for (uint64_t row = 0; row < numValues; ++row) {
    const auto begin = compressed.offsets[row];
    const auto end = compressed.offsets[row + 1];
    BOLT_CHECK_LE(begin, end);
    BOLT_CHECK_LE(end, compressed.data->size());
    for (auto i = begin; i < end; ++i) {
      const auto code = static_cast<uint8_t>(compressed.data->as<char>()[i]);
      if (code == kFsstEscape) {
        BOLT_CHECK_LT(++i, end, "Truncated Lance FSST escape");
        ++outputSize;
      } else {
        BOLT_CHECK_LT(code, numSymbols, "Invalid Lance FSST symbol code");
        BOLT_CHECK_LE(
            outputSize, std::numeric_limits<uint64_t>::max() - lengths[code]);
        outputSize += lengths[code];
      }
    }
  }
  auto output = AlignedBuffer::allocate<char>(outputSize, &pool);
  std::vector<uint64_t> offsets(numValues + 1);
  uint64_t outputOffset = 0;
  for (uint64_t row = 0; row < numValues; ++row) {
    offsets[row] = outputOffset;
    const auto begin = compressed.offsets[row];
    const auto end = compressed.offsets[row + 1];
    for (auto i = begin; i < end; ++i) {
      const auto code = static_cast<uint8_t>(compressed.data->as<char>()[i]);
      if (code == kFsstEscape) {
        output->asMutable<char>()[outputOffset++] =
            compressed.data->as<char>()[++i];
      } else {
        const auto length = lengths[code];
        BOLT_CHECK_GE(length, 1);
        BOLT_CHECK_LE(length, 8);
        std::memcpy(
            output->asMutable<char>() + outputOffset,
            symbols + code * sizeof(uint64_t),
            length);
        outputOffset += length;
      }
    }
  }
  offsets[numValues] = outputOffset;
  BOLT_CHECK_EQ(outputOffset, outputSize);
  return {
      DecodedBlock::Kind::kVariable, std::move(output), 0, std::move(offsets)};
}

DecodedBlock decodeFsst(
    const ::lance::encodings21::Fsst& fsst,
    const std::vector<BufferPtr>& inputs,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(fsst.has_values());
  return decodeFsstBlock(
      fsst,
      decodeCompressive(fsst.values(), inputs, numValues, pool),
      numValues,
      pool);
}

DecodedBlock decodePerValueGeneral(
    const ::lance::encodings21::General& general,
    DecodedBlock compressed,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(general.has_compression());
  BOLT_CHECK(general.has_values());
  BOLT_CHECK(compressed.kind == DecodedBlock::Kind::kVariable);
  BOLT_CHECK_EQ(compressed.offsets.size(), numValues + 1);
  std::vector<uint64_t> offsets(numValues + 1);
  std::vector<char> decodedBytes;
  for (uint64_t row = 0; row < numValues; ++row) {
    offsets[row] = decodedBytes.size();
    const auto start = compressed.offsets[row];
    const auto end = compressed.offsets[row + 1];
    BOLT_CHECK_LE(start, end);
    BOLT_CHECK_LE(end, compressed.data->size());
    // FullZip omits a value payload for null items. Such an item is still
    // visible to the structural layer and therefore has an empty offset
    // range, but it is not a compressed empty value and must not be passed to
    // zstd/lz4. A valid empty value has a non-empty compressed frame.
    if (start == end) {
      continue;
    }
    auto encoded =
        copyBuffer(compressed.data->as<char>() + start, end - start, pool);
    auto decoded = decompressBuffer(&general.compression(), encoded, pool);
    decodedBytes.insert(
        decodedBytes.end(),
        decoded->as<char>(),
        decoded->as<char>() + decoded->size());
  }
  offsets.back() = decodedBytes.size();
  auto decoded = DecodedBlock{
      DecodedBlock::Kind::kVariable,
      copyBuffer(decodedBytes.data(), decodedBytes.size(), pool),
      0,
      std::move(offsets)};
  BOLT_CHECK_EQ(
      general.values().compression_case(), CompressiveEncoding::kVariable);
  return decoded;
}

DecodedBlock decodeByteStreamSplit(
    const ::lance::encodings21::ByteStreamSplit& byteStreamSplit,
    const std::vector<BufferPtr>& inputs,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(byteStreamSplit.has_values());
  auto transposed =
      decodeCompressive(byteStreamSplit.values(), inputs, numValues, pool);
  BOLT_CHECK(transposed.kind == DecodedBlock::Kind::kFixed);
  BOLT_CHECK(transposed.bitsPerValue == 32 || transposed.bitsPerValue == 64);
  const auto bytesPerValue = transposed.bitsPerValue / 8;
  BOLT_CHECK_EQ(transposed.data->size(), numValues * bytesPerValue);
  auto output = AlignedBuffer::allocate<char>(transposed.data->size(), &pool);
  for (uint64_t row = 0; row < numValues; ++row) {
    for (uint64_t byte = 0; byte < bytesPerValue; ++byte) {
      output->asMutable<char>()[row * bytesPerValue + byte] =
          transposed.data->as<char>()[byte * numValues + row];
    }
  }
  return {
      DecodedBlock::Kind::kFixed,
      std::move(output),
      transposed.bitsPerValue,
      {}};
}

uint64_t readUnsignedWidth(const char* data, uint64_t bitsPerValue) {
  switch (bitsPerValue) {
    case 8:
      return static_cast<uint8_t>(*data);
    case 16:
      return readLittleEndian<uint16_t>(data);
    case 32:
      return readLittleEndian<uint32_t>(data);
    case 64:
      return readLittleEndian<uint64_t>(data);
    default:
      BOLT_UNSUPPORTED("Unsupported Lance integer width {}", bitsPerValue);
  }
}

DecodedBlock decodeRle(
    const ::lance::encodings21::Rle& rle,
    const std::vector<BufferPtr>& inputs,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(inputs.size() == 1 || inputs.size() == 2);
  BOLT_CHECK(rle.has_values());
  BOLT_CHECK(rle.has_run_lengths());
  BufferPtr valuesInput;
  BufferPtr lengthsInput;
  if (inputs.size() == 1) {
    BOLT_CHECK_GE(inputs[0]->size(), sizeof(uint64_t));
    const auto valuesSize = readLittleEndian<uint64_t>(inputs[0]->as<char>());
    BOLT_CHECK_LE(valuesSize, inputs[0]->size() - sizeof(uint64_t));
    valuesInput =
        copyBuffer(inputs[0]->as<char>() + sizeof(uint64_t), valuesSize, pool);
    lengthsInput = copyBuffer(
        inputs[0]->as<char>() + sizeof(uint64_t) + valuesSize,
        inputs[0]->size() - sizeof(uint64_t) - valuesSize,
        pool);
  } else {
    valuesInput = inputs[0];
    lengthsInput = inputs[1];
  }
  auto values = decodeCompressive(rle.values(), {valuesInput}, 0, pool);
  auto lengths = decodeCompressive(rle.run_lengths(), {lengthsInput}, 0, pool);
  BOLT_CHECK(values.kind == DecodedBlock::Kind::kFixed);
  BOLT_CHECK(lengths.kind == DecodedBlock::Kind::kFixed);
  BOLT_CHECK_EQ(values.bitsPerValue % 8, 0);
  BOLT_CHECK_EQ(lengths.bitsPerValue % 8, 0);
  const auto valueBytes = values.bitsPerValue / 8;
  const auto lengthBytes = lengths.bitsPerValue / 8;
  BOLT_CHECK_GT(valueBytes, 0);
  BOLT_CHECK_GT(lengthBytes, 0);
  BOLT_CHECK_EQ(values.data->size() % valueBytes, 0);
  BOLT_CHECK_EQ(lengths.data->size() % lengthBytes, 0);
  const auto numRuns = values.data->size() / valueBytes;
  BOLT_CHECK_EQ(numRuns, lengths.data->size() / lengthBytes);
  auto output = AlignedBuffer::allocate<char>(numValues * valueBytes, &pool);
  uint64_t outputRows = 0;
  for (uint64_t run = 0; run < numRuns; ++run) {
    const auto length = readUnsignedWidth(
        lengths.data->as<char>() + run * lengthBytes, lengths.bitsPerValue);
    BOLT_CHECK_GT(length, 0);
    const auto copyRows = std::min(length, numValues - outputRows);
    for (uint64_t i = 0; i < copyRows; ++i) {
      std::memcpy(
          output->asMutable<char>() + (outputRows + i) * valueBytes,
          values.data->as<char>() + run * valueBytes,
          valueBytes);
    }
    outputRows += copyRows;
    if (outputRows == numValues) {
      break;
    }
  }
  BOLT_CHECK_EQ(outputRows, numValues);
  return {
      DecodedBlock::Kind::kFixed, std::move(output), values.bitsPerValue, {}};
}

DecodedBlock decodeVariablePackedStruct(
    const ::lance::encodings21::VariablePackedStruct& packed,
    const DecodedBlock& rows,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(rows.kind == DecodedBlock::Kind::kVariable);
  BOLT_CHECK_EQ(rows.offsets.size(), numValues + 1);
  struct Accumulator {
    bool fixed;
    uint64_t bits;
    std::vector<char> data;
    std::vector<uint64_t> offsets;
  };
  std::vector<Accumulator> accumulators;
  accumulators.reserve(packed.fields_size());
  for (const auto& field : packed.fields()) {
    BOLT_CHECK(field.has_value());
    if (field.has_bits_per_value()) {
      BOLT_CHECK_EQ(field.bits_per_value() % 8, 0);
      accumulators.push_back({true, field.bits_per_value(), {}, {}});
    } else {
      BOLT_CHECK(field.has_bits_per_length());
      BOLT_CHECK(
          field.bits_per_length() == 32 || field.bits_per_length() == 64);
      accumulators.push_back({false, field.bits_per_length(), {}, {0}});
    }
  }
  for (uint64_t row = 0; row < numValues; ++row) {
    auto cursor = rows.offsets[row];
    const auto end = rows.offsets[row + 1];
    BOLT_CHECK_LE(cursor, end);
    BOLT_CHECK_LE(end, rows.data->size());
    if (cursor == end) {
      for (auto& accumulator : accumulators) {
        if (accumulator.fixed) {
          accumulator.data.resize(
              accumulator.data.size() + accumulator.bits / 8, 0);
        } else {
          accumulator.offsets.push_back(accumulator.data.size());
        }
      }
      continue;
    }
    for (auto& accumulator : accumulators) {
      if (accumulator.fixed) {
        const auto width = accumulator.bits / 8;
        BOLT_CHECK_LE(width, end - cursor);
        accumulator.data.insert(
            accumulator.data.end(),
            rows.data->as<char>() + cursor,
            rows.data->as<char>() + cursor + width);
        cursor += width;
      } else {
        const auto width = accumulator.bits / 8;
        BOLT_CHECK_LE(width, end - cursor);
        const auto length = width == 4
            ? static_cast<uint64_t>(
                  readLittleEndian<uint32_t>(rows.data->as<char>() + cursor))
            : readLittleEndian<uint64_t>(rows.data->as<char>() + cursor);
        cursor += width;
        BOLT_CHECK_LE(length, end - cursor);
        accumulator.data.insert(
            accumulator.data.end(),
            rows.data->as<char>() + cursor,
            rows.data->as<char>() + cursor + length);
        cursor += length;
        accumulator.offsets.push_back(accumulator.data.size());
      }
    }
    BOLT_CHECK_EQ(cursor, end);
  }

  DecodedBlock result{
      DecodedBlock::Kind::kVariable, rows.data, 0, rows.offsets};
  result.packedChildren.reserve(accumulators.size());
  for (int32_t i = 0; i < packed.fields_size(); ++i) {
    auto& accumulator = accumulators[i];
    auto data =
        copyBuffer(accumulator.data.data(), accumulator.data.size(), pool);
    if (accumulator.fixed) {
      result.packedChildren.push_back(
          decodeCompressive(packed.fields(i).value(), {data}, numValues, pool));
    } else {
      DecodedBlock block{
          DecodedBlock::Kind::kVariable,
          std::move(data),
          0,
          std::move(accumulator.offsets)};
      const auto& encoding = packed.fields(i).value();
      if (encoding.compression_case() == CompressiveEncoding::kFsst) {
        block =
            decodeFsstBlock(encoding.fsst(), std::move(block), numValues, pool);
      } else {
        BOLT_CHECK_EQ(
            encoding.compression_case(), CompressiveEncoding::kVariable);
      }
      result.packedChildren.push_back(std::move(block));
    }
  }
  return result;
}

DecodedBlock decodeCompressive(
    const CompressiveEncoding& encoding,
    const std::vector<BufferPtr>& inputs,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  if (encoding.compression_case() == CompressiveEncoding::kFixedSizeList) {
    return decodeFixedSizeList(
        encoding.fixed_size_list(), inputs, numValues, pool);
  }
  if (encoding.compression_case() == CompressiveEncoding::kGeneral) {
    BOLT_CHECK(encoding.general().has_values());
    BOLT_CHECK(encoding.general().has_compression());
    BOLT_CHECK(!inputs.empty());
    auto decompressed =
        decompressBuffer(&encoding.general().compression(), inputs[0], pool);
    std::vector<BufferPtr> innerInputs(inputs);
    innerInputs[0] = std::move(decompressed);
    return decodeCompressive(
        encoding.general().values(), innerInputs, numValues, pool);
  }
  if (encoding.compression_case() == CompressiveEncoding::kRle) {
    return decodeRle(encoding.rle(), inputs, numValues, pool);
  }
  if (encoding.compression_case() == CompressiveEncoding::kConstant) {
    const auto& constant = encoding.constant();
    if (!constant.has_value()) {
      DecodedBlock result{
          DecodedBlock::Kind::kFixed,
          AlignedBuffer::allocate<char>(0, &pool),
          0,
          {0, numValues}};
      result.allNull = true;
      return result;
    }
    BOLT_CHECK(!constant.value().empty());
    auto output = AlignedBuffer::allocate<char>(
        numValues * constant.value().size(), &pool);
    for (uint64_t i = 0; i < numValues; ++i) {
      std::memcpy(
          output->asMutable<char>() + i * constant.value().size(),
          constant.value().data(),
          constant.value().size());
    }
    return {
        DecodedBlock::Kind::kFixed,
        std::move(output),
        constant.value().size() * 8,
        {}};
  }
  BOLT_CHECK_EQ(inputs.size(), 1);
  switch (encoding.compression_case()) {
    case CompressiveEncoding::kFlat:
      return decodeFlat(encoding.flat(), inputs.front(), numValues, pool);
    case CompressiveEncoding::kInlineBitpacking:
      return decodeInlineBitpacking(
          encoding.inline_bitpacking(), inputs.front(), numValues, pool);
    case CompressiveEncoding::kOutOfLineBitpacking:
      return decodeOutOfLineBitpacking(
          encoding.out_of_line_bitpacking(), inputs.front(), numValues, pool);
    case CompressiveEncoding::kVariable:
      return decodeVariable(
          encoding.variable(), inputs.front(), numValues, pool);
    case CompressiveEncoding::kFsst:
      return decodeFsst(encoding.fsst(), inputs, numValues, pool);
    case CompressiveEncoding::kByteStreamSplit:
      return decodeByteStreamSplit(
          encoding.byte_stream_split(), inputs, numValues, pool);
    case CompressiveEncoding::kPackedStruct: {
      BOLT_CHECK(encoding.packed_struct().has_values());
      auto block = decodeCompressive(
          encoding.packed_struct().values(), inputs, numValues, pool);
      BOLT_CHECK(block.kind == DecodedBlock::Kind::kFixed);
      const auto rowBits = std::accumulate(
          encoding.packed_struct().bits_per_value().begin(),
          encoding.packed_struct().bits_per_value().end(),
          uint64_t{0});
      BOLT_CHECK_EQ(block.bitsPerValue, rowBits);
      block.packedChildBits.assign(
          encoding.packed_struct().bits_per_value().begin(),
          encoding.packed_struct().bits_per_value().end());
      return block;
    }
    case CompressiveEncoding::kFixedSizeList:
      BOLT_FAIL("Unreachable FixedSizeList decoder branch");
    case CompressiveEncoding::kGeneral:
    case CompressiveEncoding::kRle:
    case CompressiveEncoding::kConstant:
      BOLT_FAIL("Unreachable structural decoder branch");
    default:
      BOLT_UNSUPPORTED(
          "Unsupported Lance structural compression {}",
          static_cast<int>(encoding.compression_case()));
  }
}

struct StructuralState {
  std::vector<uint16_t> rep;
  std::vector<uint16_t> def;
  std::vector<RepDefLayer> layers;
  std::vector<uint16_t> levelsToRep;
  uint16_t currentDef{0};
  uint16_t currentRep{0};
  size_t currentLayer{0};
  uint64_t numItems{0};

  explicit StructuralState(
      std::vector<uint16_t> repLevels,
      std::vector<uint16_t> defLevels,
      const ::google::protobuf::RepeatedField<int>& encodedLayers,
      uint64_t items)
      : rep(std::move(repLevels)), def(std::move(defLevels)), numItems(items) {
    levelsToRep.push_back(0);
    uint16_t repLevel = 0;
    for (const auto encoded : encodedLayers) {
      const auto layer = static_cast<RepDefLayer>(encoded);
      layers.push_back(layer);
      switch (layer) {
        case ::lance::encodings21::REPDEF_NULLABLE_ITEM:
          levelsToRep.push_back(repLevel);
          break;
        case ::lance::encodings21::REPDEF_NULLABLE_LIST:
        case ::lance::encodings21::REPDEF_EMPTYABLE_LIST:
          levelsToRep.push_back(++repLevel);
          break;
        case ::lance::encodings21::REPDEF_NULL_AND_EMPTY_LIST:
          ++repLevel;
          levelsToRep.push_back(repLevel);
          levelsToRep.push_back(repLevel);
          break;
        default:
          break;
      }
    }
  }

  bool currentLayerAllValid() const {
    BOLT_CHECK_LT(currentLayer, layers.size());
    return layers[currentLayer] ==
        ::lance::encodings21::REPDEF_ALL_VALID_ITEM ||
        layers[currentLayer] == ::lance::encodings21::REPDEF_ALL_VALID_LIST ||
        layers[currentLayer] == ::lance::encodings21::REPDEF_EMPTYABLE_LIST;
  }

  void applyValidity(VectorPtr& vector) {
    BOLT_CHECK_LT(currentLayer, layers.size());
    if (def.empty() || currentLayerAllValid()) {
      ++currentLayer;
      return;
    }
    BOLT_CHECK_EQ(
        layers[currentLayer], ::lance::encodings21::REPDEF_NULLABLE_ITEM);
    const auto compare = currentDef++;
    ++currentLayer;
    uint64_t output = 0;
    for (const auto level : def) {
      BOLT_CHECK_LT(level, levelsToRep.size());
      if (levelsToRep[level] <= currentRep) {
        BOLT_CHECK_LT(output, vector->size());
        vector->setNull(output++, level > compare);
      }
    }
    BOLT_CHECK_EQ(output, vector->size());
  }

  void decimate(uint64_t dimension) {
    BOLT_CHECK_GT(dimension, 0);
    if (rep.empty() && def.empty()) {
      return;
    }
    BOLT_CHECK(rep.empty() || def.empty() || def.size() == rep.size());
    const auto isVisible = [&](size_t index) {
      if (def.empty()) {
        return true;
      }
      BOLT_CHECK_LT(def[index], levelsToRep.size());
      return levelsToRep[def[index]] <= currentRep;
    };
    size_t write = 0;
    uint64_t itemInSlot = 0;
    const auto numLevels = def.empty() ? rep.size() : def.size();
    for (size_t read = 0; read < numLevels; ++read) {
      if (!isVisible(read)) {
        BOLT_CHECK_EQ(
            itemInSlot,
            0,
            "Lance FixedSizeList ancestor special splits a fixed-size slot");
        if (!rep.empty()) {
          rep[write] = rep[read];
        }
        if (!def.empty()) {
          def[write] = def[read];
        }
        ++write;
        continue;
      }
      if (itemInSlot == 0) {
        if (!rep.empty()) {
          rep[write] = rep[read];
        }
        if (!def.empty()) {
          def[write] = def[read];
        }
        ++write;
      }
      itemInSlot = (itemInSlot + 1) % dimension;
    }
    BOLT_CHECK_EQ(
        itemInSlot,
        0,
        "Lance FixedSizeList page ends inside a fixed-size slot");
    if (!rep.empty()) {
      rep.resize(write);
    }
    if (!def.empty()) {
      def.resize(write);
    }
    numItems = write;
  }

  struct Offsets {
    std::vector<vector_size_t> offsets;
    std::vector<bool> valid;
  };

  Offsets unravelOffsets() {
    BOLT_CHECK_LT(currentLayer, layers.size());
    const auto layer = layers[currentLayer++];
    const auto validLevel = currentDef;
    uint16_t nullLevel = 0;
    uint16_t emptyLevel = 0;
    switch (layer) {
      case ::lance::encodings21::REPDEF_NULLABLE_LIST:
        nullLevel = ++currentDef;
        break;
      case ::lance::encodings21::REPDEF_EMPTYABLE_LIST:
        emptyLevel = ++currentDef;
        break;
      case ::lance::encodings21::REPDEF_NULL_AND_EMPTY_LIST:
        nullLevel = currentDef + 1;
        emptyLevel = currentDef + 2;
        currentDef += 2;
        break;
      case ::lance::encodings21::REPDEF_ALL_VALID_LIST:
        break;
      default:
        BOLT_FAIL("Expected a Lance list structural layer");
    }
    auto maxLevel = std::max({validLevel, nullLevel, emptyLevel});
    const auto upperNull = maxLevel;
    for (size_t i = currentLayer; i < layers.size(); ++i) {
      if (layers[i] == ::lance::encodings21::REPDEF_NULLABLE_ITEM) {
        ++maxLevel;
      } else if (layers[i] != ::lance::encodings21::REPDEF_ALL_VALID_ITEM) {
        break;
      }
    }

    Offsets result;
    vector_size_t items = 0;
    ++currentRep;
    if (rep.empty()) {
      BOLT_UNSUPPORTED("Lance list page is missing repetition levels");
    }
    BOLT_CHECK(def.empty() || def.size() == rep.size());
    size_t write = 0;
    for (size_t read = 0; read < rep.size(); ++read) {
      const auto repValue = rep[read];
      const auto defValue = def.empty() ? 0 : def[read];
      if (repValue != 0) {
        rep[write] = repValue - 1;
        if (!def.empty()) {
          def[write] = defValue;
        }
        ++write;
        if (defValue == 0) {
          result.offsets.push_back(items++);
          result.valid.push_back(true);
        } else if (defValue > maxLevel) {
          continue;
        } else if (defValue == nullLevel || defValue > upperNull) {
          result.offsets.push_back(items);
          result.valid.push_back(false);
        } else if (defValue == emptyLevel) {
          result.offsets.push_back(items);
          result.valid.push_back(true);
        } else {
          result.offsets.push_back(items++);
          result.valid.push_back(true);
        }
      } else {
        ++items;
      }
    }
    result.offsets.push_back(items);
    rep.resize(write);
    if (!def.empty()) {
      def.resize(write);
    }
    return result;
  }
};

void appendFixed(
    const DecodedBlock& block,
    uint64_t numValues,
    BufferPtr& output,
    uint64_t& outputValues,
    memory::MemoryPool& pool) {
  BOLT_CHECK(block.kind == DecodedBlock::Kind::kFixed);
  BOLT_CHECK_EQ(block.bitsPerValue % 8, 0);
  const auto oldBytes = output == nullptr ? 0 : output->size();
  if (output == nullptr) {
    output = AlignedBuffer::allocate<char>(block.data->size(), &pool);
  } else {
    AlignedBuffer::reallocate<char>(&output, oldBytes + block.data->size());
  }
  std::memcpy(
      output->asMutable<char>() + oldBytes,
      block.data->as<char>(),
      block.data->size());
  outputValues += numValues;
}

struct MiniBlockPage {
  BufferPtr fixed;
  uint64_t bitsPerValue{0};
  std::vector<std::string> variable;
  std::vector<uint16_t> rep;
  std::vector<uint16_t> def;
  std::optional<DecodedBlock> dictionary;
  std::vector<std::vector<bool>> fixedSizeValidities;
  std::vector<uint64_t> fixedSizeDimensions;
  std::vector<uint64_t> packedChildBits;
  std::vector<DecodedBlock> packedChildren;
  bool allNull{false};
};

std::vector<uint16_t> decodeLevels(
    const CompressiveEncoding& encoding,
    const BufferPtr& input,
    uint64_t numLevels,
    memory::MemoryPool& pool) {
  const auto block = decodeCompressive(encoding, {input}, numLevels, pool);
  BOLT_CHECK(block.kind == DecodedBlock::Kind::kFixed);
  BOLT_CHECK_EQ(block.bitsPerValue, 16);
  std::vector<uint16_t> result(numLevels);
  for (uint64_t i = 0; i < numLevels; ++i) {
    result[i] = readLittleEndian<uint16_t>(
        block.data->as<char>() + i * sizeof(uint16_t));
  }
  return result;
}

MiniBlockPage decodeMiniBlock(
    const ::lance::encodings21::MiniBlockLayout& layout,
    const Page& page,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  BOLT_CHECK_GE(page.buffer_offsets_size(), 2);
  BOLT_CHECK(layout.has_value_compression());
  const auto metadata = read(page.buffer_offsets(0), page.buffer_sizes(0));
  const auto data = read(page.buffer_offsets(1), page.buffer_sizes(1));
  const auto metadataWordBytes = layout.has_large_chunk() ? 4 : 2;
  BOLT_CHECK_EQ(metadata->size() % metadataWordBytes, 0);
  const auto numChunks = metadata->size() / metadataWordBytes;
  BOLT_CHECK_GT(numChunks, 0);

  MiniBlockPage result;
  if (layout.has_dictionary()) {
    BOLT_CHECK_GE(page.buffer_offsets_size(), 3);
    const auto dictionary = read(page.buffer_offsets(2), page.buffer_sizes(2));
    result.dictionary = decodeCompressive(
        layout.dictionary(), {dictionary}, layout.num_dictionary_items(), pool);
  }
  uint64_t dataOffset = 0;
  uint64_t decodedItems = 0;
  for (uint64_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    const auto word = layout.has_large_chunk()
        ? static_cast<uint64_t>(readLittleEndian<uint32_t>(
              metadata->as<char>() + chunkIndex * metadataWordBytes))
        : static_cast<uint64_t>(readLittleEndian<uint16_t>(
              metadata->as<char>() + chunkIndex * metadataWordBytes));
    const auto chunkBytes = ((word >> 4) + 1) * kMiniBlockAlignment;
    const auto logValues = word & 0xf;
    const auto chunkItems = chunkIndex + 1 == numChunks
        ? layout.num_items() - decodedItems
        : uint64_t{1} << logValues;
    BOLT_CHECK_LE(chunkBytes, data->size() - dataOffset);
    const auto* chunk = data->as<char>() + dataOffset;
    uint64_t cursor = 0;
    BOLT_CHECK_GE(chunkBytes, sizeof(uint16_t));
    const auto numLevels = readLittleEndian<uint16_t>(chunk + cursor);
    cursor += sizeof(uint16_t);
    std::optional<uint16_t> repBytes;
    std::optional<uint16_t> defBytes;
    if (layout.has_rep_compression()) {
      repBytes = readLittleEndian<uint16_t>(chunk + cursor);
      cursor += sizeof(uint16_t);
    }
    if (layout.has_def_compression()) {
      defBytes = readLittleEndian<uint16_t>(chunk + cursor);
      cursor += sizeof(uint16_t);
    }
    std::vector<uint32_t> valueBufferSizes(layout.num_buffers());
    for (auto& size : valueBufferSizes) {
      size = layout.has_large_chunk()
          ? readLittleEndian<uint32_t>(chunk + cursor)
          : readLittleEndian<uint16_t>(chunk + cursor);
      cursor += layout.has_large_chunk() ? 4 : 2;
    }
    cursor = alignUp(cursor, kMiniBlockAlignment);
    if (repBytes.has_value()) {
      BOLT_CHECK_LE(*repBytes, chunkBytes - cursor);
      auto encoded = copyBuffer(chunk + cursor, *repBytes, pool);
      auto levels =
          decodeLevels(layout.rep_compression(), encoded, numLevels, pool);
      result.rep.insert(result.rep.end(), levels.begin(), levels.end());
      cursor = alignUp(cursor + *repBytes, kMiniBlockAlignment);
    }
    if (defBytes.has_value()) {
      BOLT_CHECK_LE(*defBytes, chunkBytes - cursor);
      auto encoded = copyBuffer(chunk + cursor, *defBytes, pool);
      auto levels =
          decodeLevels(layout.def_compression(), encoded, numLevels, pool);
      result.def.insert(result.def.end(), levels.begin(), levels.end());
      cursor = alignUp(cursor + *defBytes, kMiniBlockAlignment);
    }
    std::vector<BufferPtr> valueBuffers;
    valueBuffers.reserve(valueBufferSizes.size());
    for (const auto size : valueBufferSizes) {
      BOLT_CHECK_LE(size, chunkBytes - cursor);
      valueBuffers.push_back(copyBuffer(chunk + cursor, size, pool));
      cursor = alignUp(cursor + size, kMiniBlockAlignment);
    }
    BOLT_CHECK_LE(cursor, chunkBytes);
    auto values = decodeCompressive(
        layout.value_compression(), valueBuffers, chunkItems, pool);
    if (result.fixedSizeDimensions.empty()) {
      result.fixedSizeDimensions = values.fixedSizeDimensions;
      result.fixedSizeValidities.resize(values.fixedSizeValidities.size());
    } else {
      BOLT_CHECK(result.fixedSizeDimensions == values.fixedSizeDimensions);
    }
    if (result.packedChildBits.empty()) {
      result.packedChildBits = values.packedChildBits;
    } else {
      BOLT_CHECK(result.packedChildBits == values.packedChildBits);
    }
    BOLT_CHECK_EQ(
        result.fixedSizeValidities.size(), values.fixedSizeValidities.size());
    for (size_t layer = 0; layer < values.fixedSizeValidities.size(); ++layer) {
      result.fixedSizeValidities[layer].insert(
          result.fixedSizeValidities[layer].end(),
          values.fixedSizeValidities[layer].begin(),
          values.fixedSizeValidities[layer].end());
    }
    if (values.kind == DecodedBlock::Kind::kFixed) {
      if (result.bitsPerValue == 0) {
        result.bitsPerValue = values.bitsPerValue;
      }
      BOLT_CHECK_EQ(result.bitsPerValue, values.bitsPerValue);
      appendFixed(values, chunkItems, result.fixed, decodedItems, pool);
    } else {
      for (uint64_t i = 0; i < chunkItems; ++i) {
        result.variable.emplace_back(
            values.data->as<char>() + values.offsets[i],
            values.offsets[i + 1] - values.offsets[i]);
      }
      decodedItems += chunkItems;
    }
    dataOffset += chunkBytes;
  }
  BOLT_CHECK_EQ(decodedItems, layout.num_items());
  BOLT_CHECK_EQ(dataOffset, data->size());
  return result;
}

struct SparsePositionSet {
  enum class Kind { kEmpty, kAll, kRange, kExplicit };

  Kind kind;
  uint64_t start{0};
  uint64_t length{0};
  std::vector<uint64_t> explicitPositions;

  uint64_t size() const {
    return kind == Kind::kExplicit ? explicitPositions.size() : length;
  }

  bool contains(uint64_t position) const {
    switch (kind) {
      case Kind::kEmpty:
        return false;
      case Kind::kAll:
        return position < length;
      case Kind::kRange:
        return position >= start && position - start < length;
      case Kind::kExplicit:
        return std::binary_search(
            explicitPositions.begin(), explicitPositions.end(), position);
    }
    BOLT_FAIL("Invalid Lance Sparse position-set kind");
  }
};

struct SparseValiditySet {
  bool storedPositionsAreValid;
  SparsePositionSet positions;

  bool isValid(uint64_t position) const {
    return positions.contains(position) == storedPositionsAreValid;
  }
};

struct SparseStructuralLayer {
  enum class Kind { kValidity, kList, kFixedSizeList };

  Kind kind;
  uint64_t numSlots;
  uint64_t numChildSlots;
  uint64_t dimension{0};
  SparsePositionSet nonEmptyPositions{
      SparsePositionSet::Kind::kEmpty,
      0,
      0,
      {}};
  std::vector<uint64_t> counts;
  SparseValiditySet validity{
      false,
      {SparsePositionSet::Kind::kEmpty, 0, 0, {}}};
};

uint64_t checkedAdd(uint64_t left, uint64_t right, std::string_view label) {
  BOLT_CHECK_LE(
      left,
      std::numeric_limits<uint64_t>::max() - right,
      "Lance Sparse {} overflows uint64",
      label);
  return left + right;
}

vector_size_t checkedVectorSize(uint64_t value, std::string_view label) {
  BOLT_CHECK_LE(
      value,
      static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()),
      "Lance Sparse {} exceeds Bolt vector size",
      label);
  return static_cast<vector_size_t>(value);
}

std::vector<uint64_t> decodeSparseU64Values(
    const CompressiveEncoding& encoding,
    const BufferPtr& input,
    uint64_t count,
    std::string_view label,
    memory::MemoryPool& pool) {
  checkedVectorSize(count, label);
  const auto decoded = decodeCompressive(encoding, {input}, count, pool);
  BOLT_CHECK(
      !decoded.allNull, "Lance Sparse {} decoded to all-null values", label);
  BOLT_CHECK(
      decoded.kind == DecodedBlock::Kind::kFixed,
      "Lance Sparse {} is not fixed-width",
      label);
  BOLT_CHECK_EQ(decoded.bitsPerValue, 64, "Lance Sparse {} is not u64", label);
  BOLT_CHECK_EQ(
      decoded.data->size(),
      count * sizeof(uint64_t),
      "Lance Sparse {} decoded byte size does not match its cardinality",
      label);
  std::vector<uint64_t> values(count);
  for (uint64_t i = 0; i < count; ++i) {
    values[i] = readLittleEndian<uint64_t>(
        decoded.data->as<char>() + i * sizeof(uint64_t));
  }
  return values;
}

SparsePositionSet decodeSparsePositions(
    const ::lance::encodings21::SparsePositionSet& encoded,
    uint64_t domainLength,
    std::string_view label,
    size_t& bufferIndex,
    const Page& page,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  const auto count = encoded.num_positions();
  SparsePositionSet result;
  switch (encoded.positions_case()) {
    case ::lance::encodings21::SparsePositionSet::kEmpty:
      BOLT_CHECK_EQ(
          count, 0, "Lance Sparse {} empty set has non-zero size", label);
      result = {SparsePositionSet::Kind::kEmpty, 0, 0, {}};
      break;
    case ::lance::encodings21::SparsePositionSet::kAll:
      BOLT_CHECK_EQ(
          count,
          domainLength,
          "Lance Sparse {} all-set size does not match its domain",
          label);
      result = {SparsePositionSet::Kind::kAll, 0, count, {}};
      break;
    case ::lance::encodings21::SparsePositionSet::kRange: {
      const auto start = encoded.range().start();
      const auto length = encoded.range().length();
      BOLT_CHECK_GT(
          length, 0, "Lance Sparse {} range must be non-empty", label);
      BOLT_CHECK_EQ(
          count,
          length,
          "Lance Sparse {} range size does not match its cardinality",
          label);
      BOLT_CHECK_LE(
          start,
          domainLength,
          "Lance Sparse {} range starts outside its domain",
          label);
      BOLT_CHECK_LE(
          length,
          domainLength - start,
          "Lance Sparse {} range ends outside its domain",
          label);
      result = {SparsePositionSet::Kind::kRange, start, length, {}};
      break;
    }
    case ::lance::encodings21::SparsePositionSet::kExplicit: {
      BOLT_CHECK_GT(
          count, 0, "Lance Sparse {} explicit set must be non-empty", label);
      BOLT_CHECK_LT(
          bufferIndex,
          static_cast<size_t>(page.buffer_offsets_size()),
          "Lance Sparse {} is missing its explicit-position buffer",
          label);
      const auto input = read(
          page.buffer_offsets(bufferIndex), page.buffer_sizes(bufferIndex));
      ++bufferIndex;
      auto deltas =
          decodeSparseU64Values(encoded.explicit_(), input, count, label, pool);
      std::vector<uint64_t> positions;
      positions.reserve(deltas.size());
      uint64_t current = 0;
      for (size_t i = 0; i < deltas.size(); ++i) {
        if (i == 0) {
          current = deltas[i];
        } else {
          BOLT_CHECK_GT(
              deltas[i],
              0,
              "Lance Sparse {} positions must be strictly increasing",
              label);
          current = checkedAdd(current, deltas[i], "position");
        }
        BOLT_CHECK_LT(
            current,
            domainLength,
            "Lance Sparse {} position is outside its domain",
            label);
        positions.push_back(current);
      }
      result = {
          SparsePositionSet::Kind::kExplicit, 0, count, std::move(positions)};
      break;
    }
    case ::lance::encodings21::SparsePositionSet::POSITIONS_NOT_SET:
      BOLT_FAIL("Lance Sparse {} position set has no variant", label);
    default:
      BOLT_FAIL("Lance Sparse {} position set has an invalid variant", label);
  }
  return result;
}

SparseValiditySet decodeSparseValidity(
    const ::lance::encodings21::SparseValiditySet& encoded,
    uint64_t domainLength,
    std::string_view label,
    size_t& bufferIndex,
    const Page& page,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  BOLT_CHECK(
      encoded.has_positions(), "Lance Sparse {} has no positions", label);
  bool storedPositionsAreValid = false;
  switch (encoded.meaning()) {
    case ::lance::encodings21::SparseValiditySet::
        SPARSE_VALIDITY_NULL_POSITIONS:
      storedPositionsAreValid = false;
      break;
    case ::lance::encodings21::SparseValiditySet::
        SPARSE_VALIDITY_VALID_POSITIONS:
      storedPositionsAreValid = true;
      break;
    default:
      BOLT_FAIL("Lance Sparse {} has an invalid validity meaning", label);
  }
  return {
      storedPositionsAreValid,
      decodeSparsePositions(
          encoded.positions(),
          domainLength,
          label,
          bufferIndex,
          page,
          pool,
          read)};
}

std::vector<uint64_t> decodeSparseCounts(
    const ::lance::encodings21::SparseCountSet& encoded,
    uint64_t count,
    size_t& bufferIndex,
    const Page& page,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  std::vector<uint64_t> result;
  checkedVectorSize(count, "count cardinality");
  switch (encoded.counts_case()) {
    case ::lance::encodings21::SparseCountSet::kEmpty:
      BOLT_CHECK_EQ(count, 0, "Lance Sparse empty count set is non-empty");
      break;
    case ::lance::encodings21::SparseCountSet::kConstant:
      BOLT_CHECK_GT(count, 0, "Lance Sparse constant count set is empty");
      BOLT_CHECK_GT(
          encoded.constant().value(),
          0,
          "Lance Sparse non-empty list count is zero");
      result.assign(count, encoded.constant().value());
      break;
    case ::lance::encodings21::SparseCountSet::kExplicit: {
      BOLT_CHECK_GT(count, 0, "Lance Sparse explicit count set is empty");
      BOLT_CHECK_LT(
          bufferIndex,
          static_cast<size_t>(page.buffer_offsets_size()),
          "Lance Sparse list is missing its count buffer");
      const auto input = read(
          page.buffer_offsets(bufferIndex), page.buffer_sizes(bufferIndex));
      ++bufferIndex;
      result = decodeSparseU64Values(
          encoded.explicit_(), input, count, "list counts", pool);
      BOLT_CHECK(
          std::none_of(
              result.begin(),
              result.end(),
              [](uint64_t value) { return value == 0; }),
          "Lance Sparse non-empty list count is zero");
      break;
    }
    case ::lance::encodings21::SparseCountSet::COUNTS_NOT_SET:
      BOLT_FAIL("Lance Sparse count set has no variant");
    default:
      BOLT_FAIL("Lance Sparse count set has an invalid variant");
  }
  return result;
}

struct SparsePage {
  MiniBlockPage values;
  std::vector<SparseStructuralLayer> layers;
};

SparsePage decodeSparse(
    const ::lance::encodings21::SparseLayout& layout,
    const Page& page,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  BOLT_CHECK(layout.has_large_chunk(), "Lance Sparse requires u32 chunks");
  BOLT_CHECK(layout.has_value_compression());
  BOLT_CHECK_GE(page.buffer_offsets_size(), 2);
  BOLT_CHECK_EQ(page.buffer_offsets_size(), page.buffer_sizes_size());

  const auto metadata = read(page.buffer_offsets(0), page.buffer_sizes(0));
  const auto data = read(page.buffer_offsets(1), page.buffer_sizes(1));
  BOLT_CHECK_EQ(
      metadata->size() % (2 * sizeof(uint32_t)),
      0,
      "Invalid Lance Sparse value-chunk metadata");
  const auto numChunks = metadata->size() / (2 * sizeof(uint32_t));
  BOLT_CHECK(
      numChunks > 0 || layout.num_visible_items() == 0,
      "Lance Sparse page with values has no chunks");

  SparsePage result;
  uint64_t dataOffset = 0;
  uint64_t decodedValues = 0;
  for (uint64_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    const auto* metadataEntry =
        metadata->as<char>() + chunkIndex * 2 * sizeof(uint32_t);
    const auto dividedBytes =
        static_cast<uint64_t>(readLittleEndian<uint32_t>(metadataEntry)) + 1;
    BOLT_CHECK_LE(
        dividedBytes,
        std::numeric_limits<uint64_t>::max() / kMiniBlockAlignment,
        "Lance Sparse value chunk size overflows");
    const auto chunkBytes = dividedBytes * kMiniBlockAlignment;
    const auto chunkValues =
        readLittleEndian<uint32_t>(metadataEntry + sizeof(uint32_t));
    BOLT_CHECK_GT(chunkValues, 0, "Lance Sparse value chunk is empty");
    BOLT_CHECK_LE(chunkBytes, data->size() - dataOffset);
    const auto* chunk = data->as<char>() + dataOffset;
    BOLT_CHECK_GE(chunkBytes, sizeof(uint16_t));
    uint64_t cursor = 0;
    const auto numLevels = readLittleEndian<uint16_t>(chunk);
    cursor += sizeof(uint16_t);
    BOLT_CHECK_EQ(numLevels, 0, "Lance Sparse value chunk contains levels");
    BOLT_CHECK_LE(
        layout.num_buffers(),
        (chunkBytes - cursor) / sizeof(uint32_t),
        "Lance Sparse value-buffer size table is truncated");
    std::vector<uint32_t> valueBufferSizes(layout.num_buffers());
    for (auto& size : valueBufferSizes) {
      size = readLittleEndian<uint32_t>(chunk + cursor);
      cursor += sizeof(uint32_t);
    }
    cursor = alignUp(cursor, kMiniBlockAlignment);
    BOLT_CHECK_LE(cursor, chunkBytes);
    std::vector<BufferPtr> valueBuffers;
    valueBuffers.reserve(valueBufferSizes.size());
    for (const auto size : valueBufferSizes) {
      BOLT_CHECK_LE(size, chunkBytes - cursor);
      valueBuffers.push_back(copyBuffer(chunk + cursor, size, pool));
      cursor = alignUp(cursor + size, kMiniBlockAlignment);
      BOLT_CHECK_LE(cursor, chunkBytes);
    }
    BOLT_CHECK_EQ(cursor, chunkBytes);
    auto decoded = decodeCompressive(
        layout.value_compression(), valueBuffers, chunkValues, pool);
    if (result.values.fixedSizeDimensions.empty()) {
      result.values.fixedSizeDimensions = decoded.fixedSizeDimensions;
      result.values.fixedSizeValidities.resize(
          decoded.fixedSizeValidities.size());
    } else {
      BOLT_CHECK(
          result.values.fixedSizeDimensions == decoded.fixedSizeDimensions);
    }
    BOLT_CHECK_EQ(
        result.values.fixedSizeValidities.size(),
        decoded.fixedSizeValidities.size());
    for (size_t layer = 0; layer < decoded.fixedSizeValidities.size();
         ++layer) {
      result.values.fixedSizeValidities[layer].insert(
          result.values.fixedSizeValidities[layer].end(),
          decoded.fixedSizeValidities[layer].begin(),
          decoded.fixedSizeValidities[layer].end());
    }
    if (result.values.packedChildBits.empty()) {
      result.values.packedChildBits = decoded.packedChildBits;
    } else {
      BOLT_CHECK(result.values.packedChildBits == decoded.packedChildBits);
    }
    if (decoded.kind == DecodedBlock::Kind::kFixed) {
      if (result.values.bitsPerValue == 0) {
        result.values.bitsPerValue = decoded.bitsPerValue;
      }
      BOLT_CHECK_EQ(result.values.bitsPerValue, decoded.bitsPerValue);
      appendFixed(
          decoded, chunkValues, result.values.fixed, decodedValues, pool);
    } else {
      BOLT_CHECK_EQ(decoded.size(), chunkValues);
      for (uint64_t i = 0; i < chunkValues; ++i) {
        result.values.variable.emplace_back(
            decoded.data->as<char>() + decoded.offsets[i],
            decoded.offsets[i + 1] - decoded.offsets[i]);
      }
      decodedValues = checkedAdd(decodedValues, chunkValues, "value count");
    }
    dataOffset = checkedAdd(dataOffset, chunkBytes, "value data offset");
  }
  BOLT_CHECK_EQ(decodedValues, layout.num_visible_items());
  BOLT_CHECK_EQ(dataOffset, data->size());

  size_t bufferIndex = 2;
  uint64_t expectedSlots = 0;
  uint64_t expectedItems = layout.num_visible_items();
  for (int32_t layerIndex = 0; layerIndex < layout.structural_layers_size();
       ++layerIndex) {
    const auto& encoded = layout.structural_layers(layerIndex);
    SparseStructuralLayer layer;
    switch (encoded.layer_case()) {
      case ::lance::encodings21::SparseStructuralLayer::kValidity: {
        const auto& validity = encoded.validity();
        BOLT_CHECK(validity.has_validity());
        layer.kind = SparseStructuralLayer::Kind::kValidity;
        layer.numSlots = validity.num_slots();
        layer.numChildSlots = validity.num_slots();
        layer.validity = decodeSparseValidity(
            validity.validity(),
            layer.numSlots,
            "validity",
            bufferIndex,
            page,
            pool,
            read);
        break;
      }
      case ::lance::encodings21::SparseStructuralLayer::kList: {
        const auto& list = encoded.list();
        BOLT_CHECK(list.has_non_empty_positions());
        BOLT_CHECK(list.has_counts());
        BOLT_CHECK(list.has_validity());
        layer.kind = SparseStructuralLayer::Kind::kList;
        layer.numSlots = list.num_slots();
        layer.numChildSlots = list.num_child_slots();
        layer.nonEmptyPositions = decodeSparsePositions(
            list.non_empty_positions(),
            layer.numSlots,
            "list non-empty",
            bufferIndex,
            page,
            pool,
            read);
        layer.counts = decodeSparseCounts(
            list.counts(),
            layer.nonEmptyPositions.size(),
            bufferIndex,
            page,
            pool,
            read);
        layer.validity = decodeSparseValidity(
            list.validity(),
            layer.numSlots,
            "list validity",
            bufferIndex,
            page,
            pool,
            read);
        uint64_t childSlots = 0;
        for (const auto count : layer.counts) {
          childSlots = checkedAdd(childSlots, count, "list child count");
        }
        BOLT_CHECK_EQ(
            childSlots,
            layer.numChildSlots,
            "Lance Sparse list counts do not match child slots");
        for (const auto position : layer.nonEmptyPositions.explicitPositions) {
          BOLT_CHECK(
              layer.validity.isValid(position),
              "Lance Sparse list slot is both null and non-empty");
        }
        if (layer.nonEmptyPositions.kind !=
            SparsePositionSet::Kind::kExplicit) {
          for (uint64_t slot = 0; slot < layer.numSlots; ++slot) {
            if (layer.nonEmptyPositions.contains(slot)) {
              BOLT_CHECK(
                  layer.validity.isValid(slot),
                  "Lance Sparse list slot is both null and non-empty");
            }
          }
        }
        expectedItems = checkedAdd(
            expectedItems,
            layer.numSlots - layer.nonEmptyPositions.size(),
            "item count");
        break;
      }
      case ::lance::encodings21::SparseStructuralLayer::kFixedSizeList: {
        const auto& list = encoded.fixed_size_list();
        BOLT_CHECK(list.has_validity());
        BOLT_CHECK_GT(
            list.dimension(),
            0,
            "Lance Sparse FixedSizeList dimension is zero");
        BOLT_CHECK_LE(
            list.num_slots(),
            std::numeric_limits<uint64_t>::max() / list.dimension(),
            "Lance Sparse FixedSizeList child domain overflows");
        layer.kind = SparseStructuralLayer::Kind::kFixedSizeList;
        layer.numSlots = list.num_slots();
        layer.dimension = list.dimension();
        layer.numChildSlots = list.num_slots() * list.dimension();
        layer.validity = decodeSparseValidity(
            list.validity(),
            layer.numSlots,
            "fixed-size-list validity",
            bufferIndex,
            page,
            pool,
            read);
        break;
      }
      case ::lance::encodings21::SparseStructuralLayer::LAYER_NOT_SET:
        BOLT_FAIL("Lance Sparse structural layer has no variant");
      default:
        BOLT_FAIL("Lance Sparse structural layer has an invalid variant");
    }
    checkedVectorSize(layer.numSlots, "layer slot count");
    checkedVectorSize(layer.numChildSlots, "layer child slot count");
    if (layerIndex > 0) {
      BOLT_CHECK_EQ(
          layer.numSlots,
          expectedSlots,
          "Lance Sparse structural layer domains are not contiguous");
    }
    expectedSlots = layer.numChildSlots;
    result.layers.push_back(std::move(layer));
  }
  if (!result.layers.empty()) {
    BOLT_CHECK_EQ(
        expectedSlots,
        layout.num_visible_items(),
        "Lance Sparse terminal domain does not match visible values");
  }
  BOLT_CHECK_EQ(
      expectedItems,
      layout.num_items(),
      "Lance Sparse item count does not match structural layers");
  BOLT_CHECK_EQ(
      bufferIndex,
      static_cast<size_t>(page.buffer_offsets_size()),
      "Lance Sparse page has unexpected structural buffers");
  return result;
}

struct SparseStructuralState {
  using Offsets = StructuralState::Offsets;

  std::vector<SparseStructuralLayer> layers;
  size_t nextLayer;
  bool pendingFixedSizeList{false};

  explicit SparseStructuralState(
      std::vector<SparseStructuralLayer> structuralLayers)
      : layers(std::move(structuralLayers)), nextLayer(layers.size()) {}

  const SparseStructuralLayer& currentLayer() const {
    BOLT_CHECK_GT(
        nextLayer,
        0,
        "Lance Sparse metadata has fewer layers than the Bolt type");
    return layers[nextLayer - 1];
  }

  void consumeLayer() {
    BOLT_CHECK_GT(nextLayer, 0);
    --nextLayer;
  }

  void applyValidity(VectorPtr& vector) {
    const auto& layer = currentLayer();
    if (pendingFixedSizeList) {
      BOLT_CHECK(
          layer.kind == SparseStructuralLayer::Kind::kFixedSizeList,
          "Lance Sparse FixedSizeList layer does not match the Bolt type");
    } else {
      BOLT_CHECK(
          layer.kind == SparseStructuralLayer::Kind::kValidity,
          "Lance Sparse validity layer does not match the Bolt type");
    }
    BOLT_CHECK_EQ(
        vector->size(),
        checkedVectorSize(layer.numSlots, "validity slot count"),
        "Lance Sparse validity domain does not match the Bolt vector");
    for (uint64_t slot = 0; slot < layer.numSlots; ++slot) {
      if (!layer.validity.isValid(slot)) {
        vector->setNull(static_cast<vector_size_t>(slot), true);
      }
    }
    pendingFixedSizeList = false;
    consumeLayer();
  }

  void decimate(uint64_t dimension) {
    BOLT_CHECK(
        !pendingFixedSizeList,
        "Lance Sparse FixedSizeList layer was decimated more than once");
    const auto& layer = currentLayer();
    BOLT_CHECK(
        layer.kind == SparseStructuralLayer::Kind::kFixedSizeList,
        "Lance Sparse layer does not match a Bolt FixedSizeList");
    BOLT_CHECK_EQ(
        layer.dimension,
        dimension,
        "Lance Sparse FixedSizeList dimension does not match the Bolt type");
    pendingFixedSizeList = true;
  }

  Offsets unravelOffsets() {
    BOLT_CHECK(
        !pendingFixedSizeList,
        "Lance Sparse FixedSizeList layer does not match a Bolt List");
    const auto& layer = currentLayer();
    BOLT_CHECK(
        layer.kind == SparseStructuralLayer::Kind::kList,
        "Lance Sparse layer does not match a Bolt List");
    Offsets result;
    BOLT_CHECK_LT(
        layer.numSlots,
        std::numeric_limits<uint64_t>::max(),
        "Lance Sparse list offset count overflows");
    result.offsets.reserve(
        checkedVectorSize(layer.numSlots + 1, "list offset count"));
    result.valid.reserve(
        checkedVectorSize(layer.numSlots, "list validity count"));
    uint64_t childOffset = 0;
    size_t countIndex = 0;
    result.offsets.push_back(0);
    for (uint64_t slot = 0; slot < layer.numSlots; ++slot) {
      const auto valid = layer.validity.isValid(slot);
      result.valid.push_back(valid);
      if (layer.nonEmptyPositions.contains(slot)) {
        BOLT_CHECK(valid, "Lance Sparse list slot is both null and non-empty");
        BOLT_CHECK_LT(countIndex, layer.counts.size());
        childOffset =
            checkedAdd(childOffset, layer.counts[countIndex++], "list offset");
      }
      result.offsets.push_back(
          checkedVectorSize(childOffset, "list child offset"));
    }
    BOLT_CHECK_EQ(countIndex, layer.counts.size());
    BOLT_CHECK_EQ(
        childOffset,
        layer.numChildSlots,
        "Lance Sparse list offsets do not consume the child domain");
    consumeLayer();
    return result;
  }

  void ensureExhausted() const {
    BOLT_CHECK(
        !pendingFixedSizeList,
        "Lance Sparse FixedSizeList layer was not materialized");
    BOLT_CHECK_EQ(
        nextLayer, 0, "Lance Sparse metadata has unconsumed structural layers");
  }
};

uint32_t levelBitWidth(uint32_t maxValue) {
  uint32_t bits = 0;
  while (maxValue > 0) {
    ++bits;
    maxValue >>= 1;
  }
  return bits;
}

std::pair<uint16_t, uint16_t> structuralLevelMaxima(
    const ::google::protobuf::RepeatedField<int>& layers) {
  uint16_t maxRep = 0;
  uint16_t maxDef = 0;
  for (const auto encoded : layers) {
    switch (static_cast<RepDefLayer>(encoded)) {
      case ::lance::encodings21::REPDEF_NULLABLE_ITEM:
      case ::lance::encodings21::REPDEF_NULLABLE_LIST:
      case ::lance::encodings21::REPDEF_EMPTYABLE_LIST:
        ++maxDef;
        break;
      case ::lance::encodings21::REPDEF_NULL_AND_EMPTY_LIST:
        maxDef += 2;
        break;
      default:
        break;
    }
    if (encoded == ::lance::encodings21::REPDEF_ALL_VALID_LIST ||
        encoded == ::lance::encodings21::REPDEF_NULLABLE_LIST ||
        encoded == ::lance::encodings21::REPDEF_EMPTYABLE_LIST ||
        encoded == ::lance::encodings21::REPDEF_NULL_AND_EMPTY_LIST) {
      ++maxRep;
    }
  }
  return {maxRep, maxDef};
}

uint16_t maxVisibleDefinitionLevel(
    const ::google::protobuf::RepeatedField<int>& layers) {
  uint16_t result = 0;
  for (const auto encoded : layers) {
    switch (static_cast<RepDefLayer>(encoded)) {
      case ::lance::encodings21::REPDEF_NULLABLE_ITEM:
        ++result;
        break;
      case ::lance::encodings21::REPDEF_ALL_VALID_ITEM:
        break;
      default:
        return result;
    }
  }
  return result;
}

uint64_t readControlWord(const char* source, uint32_t width) {
  switch (width) {
    case 0:
      return 0;
    case 1:
      return static_cast<uint8_t>(*source);
    case 2:
      return readLittleEndian<uint16_t>(source);
    case 4:
      return readLittleEndian<uint32_t>(source);
    default:
      BOLT_UNSUPPORTED("Unsupported Lance control-word width {}", width);
  }
}

MiniBlockPage decodeFullZip(
    const ::lance::encodings21::FullZipLayout& layout,
    const Page& page,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  BOLT_CHECK_EQ(page.buffer_offsets_size() > 0, true);
  BOLT_CHECK(layout.has_value_compression());
  BOLT_CHECK(
      layout.value_compression().compression_case() ==
              CompressiveEncoding::kFlat ||
          layout.value_compression().compression_case() ==
              CompressiveEncoding::kVariable ||
          layout.value_compression().compression_case() ==
              CompressiveEncoding::kFsst ||
          layout.value_compression().compression_case() ==
              CompressiveEncoding::kGeneral ||
          layout.value_compression().compression_case() ==
              CompressiveEncoding::kConstant ||
          layout.value_compression().compression_case() ==
              CompressiveEncoding::kFixedSizeList ||
          layout.value_compression().compression_case() ==
              CompressiveEncoding::kPackedStruct ||
          layout.value_compression().compression_case() ==
              CompressiveEncoding::kVariablePackedStruct,
      "Unsupported Lance FullZip compression {}",
      static_cast<int>(layout.value_compression().compression_case()));
  auto input = read(page.buffer_offsets(0), page.buffer_sizes(0));
  const auto [maxRep, maxDef] = structuralLevelMaxima(layout.layers());
  const auto repBits = levelBitWidth(maxRep);
  const auto defBits = levelBitWidth(maxDef);
  BOLT_CHECK_EQ(repBits, layout.bits_rep());
  BOLT_CHECK_EQ(defBits, layout.bits_def());
  const auto controlBytes = (repBits + defBits + 7) / 8;
  BOLT_CHECK(
      controlBytes == 0 || controlBytes == 1 || controlBytes == 2 ||
      controlBytes == 4);
  const auto defMask = defBits == 0 ? 0 : (uint64_t{1} << defBits) - 1;
  const auto visibleDef = maxVisibleDefinitionLevel(layout.layers());

  MiniBlockPage result;
  std::vector<char> fixed;
  std::vector<char> variable;
  result.variable.reserve(layout.num_visible_items());
  uint64_t offset = 0;
  uint64_t visibleItems = 0;
  for (uint64_t item = 0; item < layout.num_items(); ++item) {
    BOLT_CHECK_LE(controlBytes, input->size() - offset);
    const auto control =
        readControlWord(input->as<char>() + offset, controlBytes);
    offset += controlBytes;
    const auto rep = repBits == 0 ? 0 : control >> defBits;
    const auto def = defBits == 0 ? 0 : control & defMask;
    if (repBits > 0) {
      result.rep.push_back(static_cast<uint16_t>(rep));
    }
    if (defBits > 0) {
      result.def.push_back(static_cast<uint16_t>(def));
    }
    if (def > visibleDef) {
      continue;
    }
    ++visibleItems;
    if (layout.has_bits_per_value()) {
      BOLT_CHECK_EQ(layout.bits_per_value() % 8, 0);
      const auto bytes = layout.bits_per_value() / 8;
      BOLT_CHECK_LE(bytes, input->size() - offset);
      fixed.insert(
          fixed.end(),
          input->as<char>() + offset,
          input->as<char>() + offset + bytes);
      offset += bytes;
    } else {
      BOLT_CHECK(layout.has_bits_per_offset());
      const auto bytes = layout.bits_per_offset() / 8;
      BOLT_CHECK(bytes == 4 || bytes == 8);
      if (def != 0) {
        result.variable.emplace_back();
        continue;
      }
      BOLT_CHECK_LE(bytes, input->size() - offset);
      const auto length = bytes == 4
          ? static_cast<uint64_t>(
                readLittleEndian<uint32_t>(input->as<char>() + offset))
          : readLittleEndian<uint64_t>(input->as<char>() + offset);
      offset += bytes;
      BOLT_CHECK_LE(length, input->size() - offset);
      const auto valueStart = variable.size();
      variable.insert(
          variable.end(),
          input->as<char>() + offset,
          input->as<char>() + offset + length);
      result.variable.emplace_back(variable.data() + valueStart, length);
      offset += length;
    }
  }
  BOLT_CHECK_EQ(visibleItems, layout.num_visible_items());
  BOLT_CHECK_EQ(offset, input->size());
  if (layout.has_bits_per_value()) {
    auto encoded = copyBuffer(fixed.data(), fixed.size(), pool);
    auto block = decodeCompressive(
        layout.value_compression(), {encoded}, visibleItems, pool);
    result.fixed = std::move(block.data);
    result.bitsPerValue = block.bitsPerValue;
    result.fixedSizeDimensions = std::move(block.fixedSizeDimensions);
    result.fixedSizeValidities = std::move(block.fixedSizeValidities);
    result.packedChildBits = std::move(block.packedChildBits);
    result.allNull = block.allNull;
  } else {
    auto values = copyBuffer(variable.data(), variable.size(), pool);
    std::vector<uint64_t> offsets(result.variable.size() + 1);
    uint64_t valueOffset = 0;
    for (size_t i = 0; i < result.variable.size(); ++i) {
      offsets[i] = valueOffset;
      valueOffset += result.variable[i].size();
    }
    offsets.back() = valueOffset;
    DecodedBlock block{
        DecodedBlock::Kind::kVariable,
        std::move(values),
        0,
        std::move(offsets)};
    if (layout.value_compression().compression_case() ==
        CompressiveEncoding::kFsst) {
      block = decodeFsstBlock(
          layout.value_compression().fsst(),
          std::move(block),
          visibleItems,
          pool);
    } else if (
        layout.value_compression().compression_case() ==
        CompressiveEncoding::kGeneral) {
      block = decodePerValueGeneral(
          layout.value_compression().general(),
          std::move(block),
          visibleItems,
          pool);
    } else if (
        layout.value_compression().compression_case() ==
        CompressiveEncoding::kVariablePackedStruct) {
      block = decodeVariablePackedStruct(
          layout.value_compression().variable_packed_struct(),
          block,
          visibleItems,
          pool);
    }
    if (!block.packedChildren.empty()) {
      result.packedChildren = std::move(block.packedChildren);
    } else {
      result.variable.clear();
      for (uint64_t i = 0; i < visibleItems; ++i) {
        result.variable.emplace_back(
            block.data->as<char>() + block.offsets[i],
            block.offsets[i + 1] - block.offsets[i]);
      }
    }
  }
  return result;
}

VectorPtr makeLeafVector(
    const TypePtr& type,
    std::string_view logicalType,
    const std::vector<std::string>& packedChildLogicalTypes,
    MiniBlockPage& page,
    uint64_t numValues,
    memory::MemoryPool& pool) {
  if (page.allNull) {
    return BaseVector::createNullConstant(type, numValues, &pool);
  }
  if (type->kind() == TypeKind::ROW && !page.packedChildBits.empty()) {
    BOLT_CHECK_NOT_NULL(page.fixed);
    BOLT_CHECK_EQ(type->size(), page.packedChildBits.size());
    BOLT_CHECK_EQ(page.bitsPerValue % 8, 0);
    const auto rowBytes = page.bitsPerValue / 8;
    std::vector<VectorPtr> children;
    children.reserve(type->size());
    uint64_t fieldOffset = 0;
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      const auto childBits = page.packedChildBits[childIndex];
      BOLT_CHECK_EQ(childBits % 8, 0);
      const auto childBytes = childBits / 8;
      auto childData =
          AlignedBuffer::allocate<char>(numValues * childBytes, &pool);
      for (uint64_t row = 0; row < numValues; ++row) {
        std::memcpy(
            childData->asMutable<char>() + row * childBytes,
            page.fixed->as<char>() + row * rowBytes + fieldOffset,
            childBytes);
      }
      MiniBlockPage childPage;
      childPage.fixed = std::move(childData);
      childPage.bitsPerValue = childBits;
      BOLT_CHECK_EQ(packedChildLogicalTypes.size(), type->size());
      children.push_back(makeLeafVector(
          type->childAt(childIndex),
          packedChildLogicalTypes[childIndex],
          {},
          childPage,
          numValues,
          pool));
      fieldOffset += childBytes;
    }
    BOLT_CHECK_EQ(fieldOffset, rowBytes);
    return std::make_shared<RowVector>(
        &pool, type, nullptr, numValues, std::move(children));
  }
  if (type->kind() == TypeKind::ROW && !page.packedChildren.empty()) {
    BOLT_CHECK_EQ(type->size(), page.packedChildren.size());
    BOLT_CHECK_EQ(packedChildLogicalTypes.size(), type->size());
    std::vector<VectorPtr> children;
    children.reserve(type->size());
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      MiniBlockPage childPage;
      auto& block = page.packedChildren[childIndex];
      if (block.kind == DecodedBlock::Kind::kFixed) {
        childPage.fixed = block.data;
        childPage.bitsPerValue = block.bitsPerValue;
      } else {
        for (uint64_t row = 0; row < block.size(); ++row) {
          childPage.variable.emplace_back(
              block.data->as<char>() + block.offsets[row],
              block.offsets[row + 1] - block.offsets[row]);
        }
      }
      children.push_back(makeLeafVector(
          type->childAt(childIndex),
          packedChildLogicalTypes[childIndex],
          {},
          childPage,
          numValues,
          pool));
    }
    return std::make_shared<RowVector>(
        &pool, type, nullptr, numValues, std::move(children));
  }
  if (page.dictionary.has_value()) {
    BOLT_CHECK_NOT_NULL(page.fixed);
    BOLT_CHECK_LE(page.bitsPerValue, 64);
    auto indices = AlignedBuffer::allocate<vector_size_t>(numValues, &pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    const auto indexBytes = page.bitsPerValue / 8;
    BOLT_CHECK_EQ(page.bitsPerValue % 8, 0);
    for (uint64_t i = 0; i < numValues; ++i) {
      const auto* value = page.fixed->as<char>() + i * indexBytes;
      uint64_t index = 0;
      switch (page.bitsPerValue) {
        case 8:
          index = static_cast<uint8_t>(*value);
          break;
        case 16:
          index = readLittleEndian<uint16_t>(value);
          break;
        case 32:
          index = readLittleEndian<uint32_t>(value);
          break;
        case 64:
          index = readLittleEndian<uint64_t>(value);
          break;
        default:
          BOLT_UNSUPPORTED(
              "Unsupported Lance dictionary index width {}", page.bitsPerValue);
      }
      BOLT_CHECK_LT(index, page.dictionary->size());
      rawIndices[i] = static_cast<vector_size_t>(index);
    }
    MiniBlockPage dictionaryPage;
    if (page.dictionary->kind == DecodedBlock::Kind::kVariable) {
      for (uint64_t i = 0; i < page.dictionary->size(); ++i) {
        dictionaryPage.variable.emplace_back(
            page.dictionary->data->as<char>() + page.dictionary->offsets[i],
            page.dictionary->offsets[i + 1] - page.dictionary->offsets[i]);
      }
    } else {
      dictionaryPage.fixed = page.dictionary->data;
      dictionaryPage.bitsPerValue = page.dictionary->bitsPerValue;
    }
    auto valueLogicalType = logicalType;
    if (logicalType.rfind("dict:", 0) == 0) {
      valueLogicalType = nativeLanceDictionaryValueLogicalType(logicalType);
    }
    auto dictionary = makeLeafVector(
        type,
        valueLogicalType,
        {},
        dictionaryPage,
        page.dictionary->size(),
        pool);
    return BaseVector::wrapInDictionary(
        nullptr, std::move(indices), numValues, std::move(dictionary));
  }
  if (logicalType.rfind("fixed_size_binary:", 0) == 0 ||
      logicalType == "lance.bfloat16") {
    BOLT_CHECK_EQ(type->kind(), TypeKind::VARBINARY);
    const auto byteWidth =
        logicalType == "lance.bfloat16" ? 2 : fixedSizeBinaryWidth(logicalType);
    BOLT_CHECK_NOT_NULL(page.fixed);
    BOLT_CHECK_EQ(page.bitsPerValue, byteWidth * 8);
    auto result = BaseVector::create(type, numValues, &pool);
    auto* strings = result->asFlatVector<StringView>();
    for (uint64_t i = 0; i < numValues; ++i) {
      strings->set(
          i, StringView(page.fixed->as<char>() + i * byteWidth, byteWidth));
    }
    return result;
  }
  if (!page.variable.empty() || type->kind() == TypeKind::VARCHAR ||
      type->kind() == TypeKind::VARBINARY) {
    BOLT_CHECK_EQ(
        type->kind() == TypeKind::VARCHAR ||
            type->kind() == TypeKind::VARBINARY,
        true);
    BOLT_CHECK_EQ(page.variable.size(), numValues);
    auto result = BaseVector::create(type, numValues, &pool);
    auto* strings = result->asFlatVector<StringView>();
    for (uint64_t i = 0; i < page.variable.size(); ++i) {
      strings->set(i, StringView(page.variable[i]));
    }
    return result;
  }
  BOLT_CHECK_NOT_NULL(page.fixed);
  BOLT_CHECK_GE(page.fixed->size() * 8, numValues * page.bitsPerValue);
  auto result = BaseVector::create(type, numValues, &pool);
  if (type->kind() == TypeKind::BOOLEAN) {
    BOLT_CHECK_EQ(page.bitsPerValue, 1);
    auto* values = result->asFlatVector<bool>()->mutableRawValues<uint64_t>();
    bits::copyBits(
        reinterpret_cast<const uint64_t*>(page.fixed->as<char>()),
        0,
        values,
        0,
        numValues);
    return result;
  }
  // Structural pages store the same fixed-width little-endian representation
  // used by v2.0 Flat pages.
  if (type->isShortDecimal()) {
    auto* values = result->asFlatVector<int64_t>()->mutableRawValues();
    for (uint64_t i = 0; i < result->size(); ++i) {
      const auto low = readLittleEndian<uint64_t>(
          page.fixed->as<char>() + i * sizeof(int128_t));
      const auto high = readLittleEndian<uint64_t>(
          page.fixed->as<char>() + i * sizeof(int128_t) + sizeof(uint64_t));
      const auto value = HugeInt::build(high, low);
      BOLT_CHECK_GE(value, std::numeric_limits<int64_t>::min());
      BOLT_CHECK_LE(value, std::numeric_limits<int64_t>::max());
      values[i] = static_cast<int64_t>(value);
    }
    return result;
  }
  if (type->isLongDecimal()) {
    auto* values = result->asFlatVector<int128_t>()->mutableRawValues();
    for (uint64_t i = 0; i < result->size(); ++i) {
      values[i] = HugeInt::build(
          readLittleEndian<uint64_t>(
              page.fixed->as<char>() + i * sizeof(int128_t) + sizeof(uint64_t)),
          readLittleEndian<uint64_t>(
              page.fixed->as<char>() + i * sizeof(int128_t)));
    }
    return result;
  }
  const auto copyPrimitive = [&]<typename Input, typename Output>() {
    auto* output = result->asFlatVector<Output>()->mutableRawValues();
    for (uint64_t i = 0; i < result->size(); ++i) {
      output[i] = static_cast<Output>(
          readLittleEndian<Input>(page.fixed->as<char>() + i * sizeof(Input)));
    }
  };
  if (logicalType == "halffloat") {
    BOLT_CHECK_EQ(page.bitsPerValue, 16);
    auto* output = result->asFlatVector<float>()->mutableRawValues();
    for (uint64_t i = 0; i < numValues; ++i) {
      output[i] = halfToFloat(readLittleEndian<uint16_t>(
          page.fixed->as<char>() + i * sizeof(uint16_t)));
    }
  } else if (logicalType == "date64:ms") {
    BOLT_CHECK_EQ(page.bitsPerValue, 64);
    constexpr int64_t kMillisPerDay = 86'400'000;
    auto* output = result->asFlatVector<int32_t>()->mutableRawValues();
    for (uint64_t i = 0; i < numValues; ++i) {
      const auto millis = readLittleEndian<int64_t>(
          page.fixed->as<char>() + i * sizeof(int64_t));
      BOLT_CHECK_EQ(
          millis % kMillisPerDay,
          0,
          "Lance date64 value is not aligned to a whole day");
      const auto days = millis / kMillisPerDay;
      BOLT_CHECK_GE(days, std::numeric_limits<int32_t>::min());
      BOLT_CHECK_LE(days, std::numeric_limits<int32_t>::max());
      output[i] = static_cast<int32_t>(days);
    }
  } else if (logicalType.rfind("time32:", 0) == 0) {
    BOLT_CHECK_EQ(page.bitsPerValue, 32);
    copyPrimitive.template operator()<int32_t, int64_t>();
  } else if (logicalType.rfind("duration:", 0) == 0) {
    BOLT_CHECK_EQ(page.bitsPerValue, 64);
    auto* output = result->asFlatVector<int64_t>()->mutableRawValues();
    for (uint64_t i = 0; i < numValues; ++i) {
      const auto value = readLittleEndian<int64_t>(
          page.fixed->as<char>() + i * sizeof(int64_t));
      if (logicalType == "duration:s") {
        BOLT_CHECK_LE(
            value, std::numeric_limits<int64_t>::max() / kMillisInSecond);
        BOLT_CHECK_GE(
            value, std::numeric_limits<int64_t>::min() / kMillisInSecond);
        output[i] = value * kMillisInSecond;
      } else if (logicalType == "duration:ms") {
        output[i] = value;
      } else {
        const auto divisor = logicalType == "duration:us" ? 1'000 : 1'000'000;
        BOLT_CHECK_EQ(
            value % divisor,
            0,
            "Lance {} value cannot be represented losslessly in Bolt milliseconds",
            logicalType);
        output[i] = value / divisor;
      }
    }
  } else if (logicalType.rfind("timestamp:", 0) == 0) {
    BOLT_CHECK_EQ(page.bitsPerValue, 64);
    auto* output = result->asFlatVector<Timestamp>()->mutableRawValues();
    for (uint64_t i = 0; i < numValues; ++i) {
      const auto value = readLittleEndian<int64_t>(
          page.fixed->as<char>() + i * sizeof(int64_t));
      if (logicalType.rfind("timestamp:s:", 0) == 0) {
        output[i] = Timestamp(value, 0);
      } else if (logicalType.rfind("timestamp:ms:", 0) == 0) {
        output[i] = Timestamp::fromMillis(value);
      } else if (logicalType.rfind("timestamp:us:", 0) == 0) {
        output[i] = Timestamp::fromMicros(value);
      } else if (logicalType.rfind("timestamp:ns:", 0) == 0) {
        output[i] = Timestamp::fromNanos(value);
      } else {
        BOLT_UNSUPPORTED(
            "Unsupported Lance timestamp logical type: {}", logicalType);
      }
    }
  } else if (logicalType == "uint8") {
    copyPrimitive.template operator()<uint8_t, int16_t>();
  } else if (logicalType == "uint16") {
    copyPrimitive.template operator()<uint16_t, int32_t>();
  } else if (logicalType == "uint32") {
    copyPrimitive.template operator()<uint32_t, int64_t>();
  } else if (logicalType == "uint64") {
    copyPrimitive.template operator()<uint64_t, int128_t>();
  } else {
    switch (type->kind()) {
      case TypeKind::TINYINT:
        copyPrimitive.template operator()<int8_t, int8_t>();
        break;
      case TypeKind::SMALLINT:
        copyPrimitive.template operator()<int16_t, int16_t>();
        break;
      case TypeKind::INTEGER:
        copyPrimitive.template operator()<int32_t, int32_t>();
        break;
      case TypeKind::BIGINT:
        copyPrimitive.template operator()<int64_t, int64_t>();
        break;
      case TypeKind::REAL:
        BOLT_CHECK_EQ(page.bitsPerValue, 32);
        std::memcpy(
            result->asFlatVector<float>()->mutableRawValues(),
            page.fixed->as<char>(),
            numValues * sizeof(float));
        break;
      case TypeKind::DOUBLE:
        BOLT_CHECK_EQ(page.bitsPerValue, 64);
        std::memcpy(
            result->asFlatVector<double>()->mutableRawValues(),
            page.fixed->as<char>(),
            numValues * sizeof(double));
        break;
      default:
        BOLT_UNSUPPORTED(
            "Unsupported Lance structural leaf type {}", type->toString());
    }
  }
  return result;
}

template <typename StructuralStateType>
VectorPtr materializeBlobV2(
    MiniBlockPage& page,
    uint64_t numValues,
    StructuralStateType& state,
    memory::MemoryPool& pool,
    const std::shared_ptr<const NativeLanceBlobResolver>& blobResolver,
    std::string_view sourceDataFile,
    const std::function<
        void(const std::vector<std::pair<uint64_t, uint64_t>>&)>& prefetch,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  static const auto descriptorType =
      ROW({"kind", "position", "size", "blob_id", "blob_uri"},
          {SMALLINT(), HUGEINT(), HUGEINT(), BIGINT(), VARCHAR()});
  static const std::vector<std::string> descriptorLogicalTypes{
      "uint8", "uint64", "uint64", "uint32", "string"};
  auto descriptorVector = makeLeafVector(
      descriptorType, "struct", descriptorLogicalTypes, page, numValues, pool);
  state.applyValidity(descriptorVector);
  const auto* descriptors = descriptorVector->as<RowVector>();
  BOLT_CHECK_NOT_NULL(descriptors);
  BOLT_CHECK_EQ(descriptors->childrenSize(), 5);
  const auto* kinds = descriptors->childAt(0)->asFlatVector<int16_t>();
  const auto* positions = descriptors->childAt(1)->asFlatVector<int128_t>();
  const auto* sizes = descriptors->childAt(2)->asFlatVector<int128_t>();
  const auto* blobIds = descriptors->childAt(3)->asFlatVector<int64_t>();
  const auto* blobUris = descriptors->childAt(4)->asFlatVector<StringView>();
  BOLT_CHECK_NOT_NULL(kinds);
  BOLT_CHECK_NOT_NULL(positions);
  BOLT_CHECK_NOT_NULL(sizes);
  BOLT_CHECK_NOT_NULL(blobIds);
  BOLT_CHECK_NOT_NULL(blobUris);

  struct Payload {
    uint64_t row;
    uint64_t position;
    uint64_t size;
    uint64_t outputOffset;
    std::optional<size_t> externalInput;
  };
  struct ExternalInput {
    std::unique_ptr<dwio::common::BufferedInput> input;
    std::vector<std::pair<
        const Payload*,
        std::unique_ptr<dwio::common::SeekableInputStream>>>
        reads;
  };
  std::vector<Payload> payloads;
  payloads.reserve(numValues);
  std::vector<ExternalInput> externalInputs;
  std::unordered_map<std::string, size_t> externalInputByObject;
  std::vector<std::pair<uint64_t, uint64_t>> payloadRanges;
  payloadRanges.reserve(numValues);
  uint64_t payloadBytes = 0;
  for (uint64_t row = 0; row < numValues; ++row) {
    if (descriptors->isNullAt(row)) {
      continue;
    }
    const auto kind = kinds->valueAt(row);
    BOLT_CHECK_LE(kind, 3, "Unknown Lance Blob v2 kind {}", kind);
    const auto position = positions->valueAt(row);
    const auto size = sizes->valueAt(row);
    BOLT_CHECK_GE(position, 0, "Negative Lance Blob v2 payload position");
    BOLT_CHECK_GE(size, 0, "Negative Lance Blob v2 payload size");
    BOLT_CHECK_LE(
        position,
        static_cast<int128_t>(std::numeric_limits<uint64_t>::max()),
        "Lance Blob v2 payload position exceeds uint64");
    BOLT_CHECK_LE(
        size,
        static_cast<int128_t>(std::numeric_limits<uint64_t>::max()),
        "Lance Blob v2 payload size exceeds uint64");
    auto payloadPosition = static_cast<uint64_t>(position);
    auto payloadSize = static_cast<uint64_t>(size);
    std::optional<size_t> externalInput;
    if (kind != 0) {
      BOLT_CHECK_NOT_NULL(
          blobResolver,
          "Lance Blob v2 kind {} requires a dataset/sidecar object resolver",
          kind);
      const auto blobId = blobIds->valueAt(row);
      BOLT_CHECK_GE(blobId, 0, "Negative Lance Blob v2 blob id");
      BOLT_CHECK_LE(
          blobId,
          std::numeric_limits<uint32_t>::max(),
          "Lance Blob v2 blob id exceeds uint32");
      if (kind ==
              static_cast<int16_t>(NativeLanceBlobResolver::Kind::kPacked) ||
          kind ==
              static_cast<int16_t>(NativeLanceBlobResolver::Kind::kDedicated)) {
        BOLT_CHECK_GT(
            blobId, 0, "Managed Lance Blob v2 object has invalid blob id 0");
      }
      if (kind ==
          static_cast<int16_t>(NativeLanceBlobResolver::Kind::kDedicated)) {
        BOLT_CHECK_EQ(
            payloadPosition,
            0,
            "Dedicated Lance Blob v2 descriptor has a non-zero position");
      }
      if (kind ==
          static_cast<int16_t>(NativeLanceBlobResolver::Kind::kExternal)) {
        BOLT_CHECK(
            !blobUris->valueAt(row).empty(),
            "External Lance Blob v2 descriptor has an empty URI");
      }
      NativeLanceBlobResolver::Request request{
          .kind = static_cast<NativeLanceBlobResolver::Kind>(kind),
          .sourceDataFile = std::string(sourceDataFile),
          .blobId = static_cast<uint32_t>(blobId),
          .uri = blobUris->valueAt(row).str(),
          .position = payloadPosition,
          .size = payloadSize};
      const auto objectKey =
          fmt::format("{}:{}:{}", kind, request.blobId, request.uri);
      auto [inputIt, inserted] =
          externalInputByObject.emplace(objectKey, externalInputs.size());
      if (inserted) {
        auto input = blobResolver->resolve(request, pool);
        BOLT_CHECK_NOT_NULL(
            input,
            "Lance Blob v2 resolver returned no input for kind {} blob id {}",
            kind,
            blobId);
        externalInputs.push_back({std::move(input), {}});
      }
      externalInput = inputIt->second;
      const auto& payloadFile =
          externalInputs[*externalInput].input->getReadFile();
      BOLT_CHECK_NOT_NULL(
          payloadFile,
          "Lance Blob v2 resolver returned no object for kind {} blob id {}",
          kind,
          blobId);
      BOLT_CHECK_LE(payloadPosition, payloadFile->size());
      if (kind ==
              static_cast<int16_t>(NativeLanceBlobResolver::Kind::kExternal) &&
          payloadSize == 0) {
        payloadSize = payloadFile->size() - payloadPosition;
      }
      BOLT_CHECK_LE(payloadSize, payloadFile->size() - payloadPosition);
    }
    BOLT_CHECK_LE(
        payloadSize,
        static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
        "Lance Blob v2 value exceeds Bolt StringView capacity");
    BOLT_CHECK_LE(
        payloadBytes,
        std::numeric_limits<uint64_t>::max() - payloadSize,
        "Lance Blob v2 payload size overflow");
    if (payloadSize > 0) {
      if (kind == 0) {
        payloadRanges.emplace_back(payloadPosition, payloadSize);
      }
      payloads.push_back(
          {row, payloadPosition, payloadSize, payloadBytes, externalInput});
    }
    payloadBytes += payloadSize;
  }
  prefetch(payloadRanges);

  auto result = BaseVector::create(VARBINARY(), numValues, &pool);
  auto* values = result->asFlatVector<StringView>();
  auto payload = AlignedBuffer::allocate<char>(payloadBytes, &pool);
  if (payloadBytes > 0) {
    values->addStringBuffer(payload);
  }

  for (const auto& item : payloads) {
    if (!item.externalInput.has_value()) {
      const auto bytes = read(item.position, item.size);
      std::memcpy(
          payload->template asMutable<char>() + item.outputOffset,
          bytes->template as<char>(),
          item.size);
      continue;
    }
    auto& external = externalInputs[*item.externalInput];
    external.reads.emplace_back(
        &item, external.input->enqueue({item.position, item.size}));
  }
  for (auto& external : externalInputs) {
    external.input->load(dwio::common::LogType::BLOCK);
    for (auto& [item, stream] : external.reads) {
      stream->readFully(
          payload->asMutable<char>() + item->outputOffset, item->size);
    }
  }

  size_t payloadIndex = 0;
  for (uint64_t row = 0; row < numValues; ++row) {
    if (descriptors->isNullAt(row)) {
      result->setNull(row, true);
      continue;
    }
    if (payloadIndex >= payloads.size() || payloads[payloadIndex].row != row) {
      values->set(row, StringView{});
      continue;
    }
    const auto& item = payloads[payloadIndex++];
    values->setNoCopy(
        row,
        StringView(
            payload->as<char>() + item.outputOffset,
            static_cast<int32_t>(item.size)));
  }
  BOLT_CHECK_EQ(payloadIndex, payloads.size());
  return result;
}

template <typename StructuralStateType>
VectorPtr applyStructuralLayers(
    const TypePtr& type,
    VectorPtr leaf,
    StructuralStateType& state,
    memory::MemoryPool& pool,
    const std::vector<uint32_t>& fixedSizeDimensions,
    const std::vector<std::vector<bool>>& fixedSizeValidities,
    size_t& fixedSizeLayer,
    bool leafValidityStoredInFixedSizeValues) {
  if (type->kind() == TypeKind::ROW && leaf->type()->equivalent(*type)) {
    if (!leafValidityStoredInFixedSizeValues) {
      state.applyValidity(leaf);
    }
    return leaf;
  }
  if (type->kind() == TypeKind::ROW) {
    BOLT_CHECK_EQ(
        type->size(),
        1,
        "A structural leaf branch must contain exactly one ROW child");
    auto child = applyStructuralLayers(
        type->childAt(0),
        std::move(leaf),
        state,
        pool,
        fixedSizeDimensions,
        fixedSizeValidities,
        fixedSizeLayer,
        leafValidityStoredInFixedSizeValues);
    const auto size = child->size();
    auto row = std::make_shared<RowVector>(
        &pool, type, nullptr, size, std::vector<VectorPtr>{std::move(child)});
    VectorPtr result = row;
    state.applyValidity(result);
    return result;
  }
  if (type->kind() != TypeKind::ARRAY) {
    if (!leafValidityStoredInFixedSizeValues) {
      state.applyValidity(leaf);
    }
    return leaf;
  }
  auto child = applyStructuralLayers(
      type->childAt(0),
      std::move(leaf),
      state,
      pool,
      fixedSizeDimensions,
      fixedSizeValidities,
      fixedSizeLayer,
      leafValidityStoredInFixedSizeValues);
  BOLT_CHECK_GT(fixedSizeLayer, 0);
  const auto layer = --fixedSizeLayer;
  if (fixedSizeDimensions[layer] > 0) {
    const auto dimension = fixedSizeDimensions[layer];
    const std::vector<bool>* valid = layer < fixedSizeValidities.size()
        ? &fixedSizeValidities[layer]
        : nullptr;
    BOLT_CHECK_GT(dimension, 0);
    BOLT_CHECK_EQ(child->size() % dimension, 0);
    const auto size = child->size() / dimension;
    auto offsets = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    auto sizes = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    BufferPtr nulls;
    if (valid != nullptr && !valid->empty()) {
      BOLT_CHECK_EQ(valid->size(), child->size());
      for (vector_size_t item = 0; item < child->size(); ++item) {
        if (!(*valid)[item] && !child->isNullAt(item)) {
          child->setNull(item, true);
        }
      }
      nulls = AlignedBuffer::allocate<bool>(size, &pool, bits::kNotNull);
    }
    for (vector_size_t row = 0; row < size; ++row) {
      offsets->template asMutable<vector_size_t>()[row] = row * dimension;
      sizes->template asMutable<vector_size_t>()[row] = dimension;
    }
    VectorPtr result = std::make_shared<ArrayVector>(
        &pool,
        type,
        std::move(nulls),
        size,
        std::move(offsets),
        std::move(sizes),
        std::move(child));
    if (leafValidityStoredInFixedSizeValues) {
      if (fixedSizeLayer == 0) {
        state.applyValidity(result);
      }
    } else {
      state.decimate(dimension);
      state.applyValidity(result);
    }
    return result;
  }
  auto decoded = state.unravelOffsets();
  BOLT_CHECK_GE(decoded.offsets.size(), 1);
  const auto size = decoded.offsets.size() - 1;
  auto offsets = AlignedBuffer::allocate<vector_size_t>(size, &pool);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(size, &pool);
  BufferPtr nulls;
  const auto hasNulls =
      std::find(decoded.valid.begin(), decoded.valid.end(), false) !=
      decoded.valid.end();
  if (hasNulls) {
    nulls = AlignedBuffer::allocate<bool>(size, &pool, bits::kNotNull);
  }
  for (uint64_t row = 0; row < size; ++row) {
    offsets->template asMutable<vector_size_t>()[row] = decoded.offsets[row];
    sizes->template asMutable<vector_size_t>()[row] =
        decoded.offsets[row + 1] - decoded.offsets[row];
    if (hasNulls && !decoded.valid[row]) {
      bits::setNull(nulls->asMutable<uint64_t>(), row);
    }
  }
  return std::make_shared<ArrayVector>(
      &pool, type, std::move(nulls), size, offsets, sizes, std::move(child));
}

std::string_view primitiveLogicalType(std::string_view logicalType) {
  constexpr auto kPrefix = std::string_view("fixed_size_list:");
  while (logicalType.rfind(kPrefix, 0) == 0) {
    const auto dimension = logicalType.rfind(':');
    BOLT_CHECK_GT(dimension, kPrefix.size());
    logicalType =
        logicalType.substr(kPrefix.size(), dimension - kPrefix.size());
  }
  return logicalType;
}

TypePtr structuralLeafType(TypePtr type) {
  while (type->kind() == TypeKind::ARRAY || type->kind() == TypeKind::ROW) {
    BOLT_CHECK_EQ(type->size(), 1);
    type = type->childAt(0);
  }
  return type;
}

VectorPtr decodeConstantPage(
    const TypePtr& type,
    std::string_view logicalType,
    const std::vector<uint32_t>& fixedSizeDimensions,
    const std::vector<std::string>& packedChildLogicalTypes,
    const Page& page,
    const ::lance::encodings21::ConstantLayout& layout,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  BOLT_CHECK_GT(layout.layers_size(), 0);
  BufferPtr value;
  std::vector<BufferPtr> scalarBuffers;
  int32_t bufferIndex = 0;
  if (layout.has_inline_value()) {
    value = copyBuffer(
        layout.inline_value().data(), layout.inline_value().size(), pool);
    scalarBuffers.push_back(value);
  } else {
    const auto hasScalarBuffer =
        page.buffer_offsets_size() == 1 || page.buffer_offsets_size() == 3;
    if (hasScalarBuffer) {
      const auto encoded = read(
          page.buffer_offsets(bufferIndex), page.buffer_sizes(bufferIndex));
      ++bufferIndex;
      BOLT_CHECK_GE(encoded->size(), sizeof(uint32_t));
      const auto numBuffers = readLittleEndian<uint32_t>(encoded->as<char>());
      BOLT_CHECK_GT(numBuffers, 0);
      BOLT_CHECK_GE(encoded->size(), (numBuffers + 1) * sizeof(uint32_t));
      uint64_t payloadOffset = (numBuffers + 1) * sizeof(uint32_t);
      scalarBuffers.reserve(numBuffers);
      for (uint32_t i = 0; i < numBuffers; ++i) {
        const auto size = readLittleEndian<uint32_t>(
            encoded->as<char>() + (i + 1) * sizeof(uint32_t));
        BOLT_CHECK_LE(size, encoded->size() - payloadOffset);
        scalarBuffers.push_back(
            copyBuffer(encoded->as<char>() + payloadOffset, size, pool));
        payloadOffset += size;
      }
      BOLT_CHECK_EQ(payloadOffset, encoded->size());
      value = scalarBuffers.front();
    }
  }
  std::vector<uint16_t> rep;
  std::vector<uint16_t> def;
  const auto readLevels = [&](const CompressiveEncoding* encoding,
                              uint64_t count) {
    BOLT_CHECK_LT(bufferIndex, page.buffer_offsets_size());
    const auto bytes =
        read(page.buffer_offsets(bufferIndex), page.buffer_sizes(bufferIndex));
    ++bufferIndex;
    if (bytes->size() == 0) {
      BOLT_CHECK_EQ(count, 0);
      return std::vector<uint16_t>{};
    }
    if (encoding != nullptr) {
      BOLT_CHECK_GT(count, 0);
      return decodeLevels(*encoding, bytes, count, pool);
    }
    BOLT_CHECK_EQ(bytes->size() % sizeof(uint16_t), 0);
    if (count == 0) {
      count = bytes->size() / sizeof(uint16_t);
    }
    BOLT_CHECK_EQ(bytes->size(), count * sizeof(uint16_t));
    std::vector<uint16_t> levels(count);
    for (uint64_t i = 0; i < count; ++i) {
      levels[i] =
          readLittleEndian<uint16_t>(bytes->as<char>() + i * sizeof(uint16_t));
    }
    return levels;
  };
  const auto hasLevelBuffers = layout.has_inline_value()
      ? page.buffer_offsets_size() == 2
      : page.buffer_offsets_size() == 2 || page.buffer_offsets_size() == 3;
  if (hasLevelBuffers) {
    rep = readLevels(
        layout.has_rep_compression() ? &layout.rep_compression() : nullptr,
        layout.num_rep_values());
    def = readLevels(
        layout.has_def_compression() ? &layout.def_compression() : nullptr,
        layout.num_def_values());
  }
  BOLT_CHECK_EQ(bufferIndex, page.buffer_offsets_size());
  const auto numItems = !rep.empty() ? rep.size()
      : !def.empty()                 ? def.size()
                                     : page.length();
  const auto visibleDef = maxVisibleDefinitionLevel(layout.layers());
  const auto numVisible = def.empty()
      ? numItems
      : static_cast<uint64_t>(
            std::count_if(def.begin(), def.end(), [visibleDef](auto level) {
              return level <= visibleDef;
            }));
  if (type->kind() == TypeKind::ROW && type->size() == 0) {
    VectorPtr result = std::make_shared<RowVector>(
        &pool, type, nullptr, numVisible, std::vector<VectorPtr>{});
    StructuralState state(
        std::move(rep), std::move(def), layout.layers(), numVisible);
    state.applyValidity(result);
    BOLT_CHECK_EQ(result->size(), page.length());
    return result;
  }
  MiniBlockPage scalar;
  const auto leafType =
      packedChildLogicalTypes.empty() ? structuralLeafType(type) : type;
  if (value == nullptr) {
    scalar.allNull = true;
  } else if (
      leafType->kind() == TypeKind::VARCHAR ||
      leafType->kind() == TypeKind::VARBINARY) {
    BOLT_CHECK_EQ(scalarBuffers.size(), 2);
    BOLT_CHECK_GE(scalarBuffers[0]->size(), 2 * sizeof(uint32_t));
    const auto first = readLittleEndian<uint32_t>(scalarBuffers[0]->as<char>());
    const auto end = readLittleEndian<uint32_t>(
        scalarBuffers[0]->as<char>() + sizeof(uint32_t));
    BOLT_CHECK_LE(first, end);
    BOLT_CHECK_LE(end, scalarBuffers[1]->size());
    scalar.variable.emplace_back(
        scalarBuffers[1]->as<char>() + first, end - first);
  } else {
    scalar.fixed = value;
    scalar.bitsPerValue = value->size() * 8;
  }
  VectorPtr leaf;
  if (scalar.allNull) {
    leaf = BaseVector::create(leafType, numVisible, &pool);
  } else {
    auto scalarVector = makeLeafVector(
        leafType,
        primitiveLogicalType(logicalType),
        packedChildLogicalTypes,
        scalar,
        1,
        pool);
    auto constant = BaseVector::wrapInConstant(numVisible, 0, scalarVector);
    leaf = BaseVector::create(leafType, numVisible, &pool);
    leaf->copy(constant.get(), 0, 0, numVisible);
  }
  StructuralState state(
      std::move(rep), std::move(def), layout.layers(), numVisible);
  size_t fixedSizeLayer = fixedSizeDimensions.size();
  std::vector<std::vector<bool>> fixedSizeValidities;
  auto result = applyStructuralLayers(
      type,
      std::move(leaf),
      state,
      pool,
      fixedSizeDimensions,
      fixedSizeValidities,
      fixedSizeLayer,
      false);
  BOLT_CHECK_EQ(fixedSizeLayer, 0);
  uint64_t expectedRows = page.length();
  for (const auto dimension : fixedSizeDimensions) {
    if (dimension == 0) {
      continue;
    }
    BOLT_CHECK_EQ(expectedRows % dimension, 0);
    expectedRows /= dimension;
  }
  BOLT_CHECK_EQ(result->size(), expectedRows);
  return result;
}

VectorPtr decodeBlobPage(
    const TypePtr& type,
    const Page& page,
    const ::lance::encodings21::BlobLayout& layout,
    memory::MemoryPool& pool,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  BOLT_CHECK_EQ(type->kind(), TypeKind::VARBINARY);
  BOLT_CHECK(layout.has_inner_layout());
  BOLT_CHECK_EQ(
      layout.inner_layout().layout_case(),
      ::lance::encodings21::PageLayout::kMiniBlockLayout);
  auto descriptors = decodeMiniBlock(
      layout.inner_layout().mini_block_layout(), page, pool, read);
  BOLT_CHECK_NOT_NULL(descriptors.fixed);
  BOLT_CHECK_EQ(descriptors.bitsPerValue, 128);
  BOLT_CHECK_EQ(descriptors.fixed->size(), page.length() * 16);
  auto result = BaseVector::create(type, page.length(), &pool);
  auto* values = result->asFlatVector<StringView>();
  for (uint64_t row = 0; row < page.length(); ++row) {
    const auto* descriptor = descriptors.fixed->as<char>() + row * 16;
    const auto position = readLittleEndian<uint64_t>(descriptor);
    const auto size = readLittleEndian<uint64_t>(descriptor + sizeof(uint64_t));
    if (size == 0 && position != 0) {
      result->setNull(row, true);
      continue;
    }
    if (size == 0) {
      values->set(row, StringView{});
      continue;
    }
    BOLT_CHECK_LE(
        size, static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
    const auto payload = read(position, size);
    values->set(row, StringView(payload->as<char>(), size));
  }
  return result;
}

} // namespace

VectorPtr decodeLanceStructuralPage(
    const TypePtr& type,
    std::string_view leafLogicalType,
    const std::vector<uint32_t>& fixedSizeDimensions,
    const std::vector<std::string>& packedChildLogicalTypes,
    const ::lance::file::v2::ColumnMetadata& column,
    const Page& page,
    const ::lance::encodings21::PageLayout& layout,
    memory::MemoryPool& pool,
    const std::shared_ptr<const NativeLanceBlobResolver>& blobResolver,
    std::string_view sourceDataFile,
    const std::function<
        void(const std::vector<std::pair<uint64_t, uint64_t>>&)>& prefetch,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read) {
  if (layout.layout_case() ==
      ::lance::encodings21::PageLayout::kConstantLayout) {
    return decodeConstantPage(
        type,
        leafLogicalType,
        fixedSizeDimensions,
        packedChildLogicalTypes,
        page,
        layout.constant_layout(),
        pool,
        read);
  }
  if (layout.layout_case() == ::lance::encodings21::PageLayout::kBlobLayout) {
    return decodeBlobPage(type, page, layout.blob_layout(), pool, read);
  }
  if (layout.layout_case() == ::lance::encodings21::PageLayout::kSparseLayout) {
    auto decoded = decodeSparse(layout.sparse_layout(), page, pool, read);
    if (leafLogicalType == "lance.blob.v2") {
      BOLT_CHECK(fixedSizeDimensions.empty());
      BOLT_CHECK_EQ(type->kind(), TypeKind::VARBINARY);
      BOLT_CHECK_EQ(packedChildLogicalTypes.size(), 5);
      SparseStructuralState state(std::move(decoded.layers));
      auto result = materializeBlobV2(
          decoded.values,
          layout.sparse_layout().num_visible_items(),
          state,
          pool,
          blobResolver,
          sourceDataFile,
          prefetch,
          read);
      state.ensureExhausted();
      return result;
    }
    uint64_t leafValues = layout.sparse_layout().num_visible_items();
    for (const auto dimension : decoded.values.fixedSizeDimensions) {
      BOLT_CHECK_LE(
          leafValues, std::numeric_limits<uint64_t>::max() / dimension);
      leafValues *= dimension;
    }
    auto leaf = makeLeafVector(
        packedChildLogicalTypes.empty() ? structuralLeafType(type) : type,
        primitiveLogicalType(leafLogicalType),
        packedChildLogicalTypes,
        decoded.values,
        leafValues,
        pool);
    SparseStructuralState state(std::move(decoded.layers));
    std::vector<uint32_t> dimensions = fixedSizeDimensions;
    std::vector<std::vector<bool>> fixedSizeValidities(
        fixedSizeDimensions.size());
    dimensions.reserve(
        dimensions.size() + decoded.values.fixedSizeDimensions.size());
    fixedSizeValidities.reserve(
        fixedSizeValidities.size() + decoded.values.fixedSizeValidities.size());
    for (size_t layer = 0; layer < decoded.values.fixedSizeDimensions.size();
         ++layer) {
      const auto dimension = decoded.values.fixedSizeDimensions[layer];
      BOLT_CHECK_LE(dimension, std::numeric_limits<uint32_t>::max());
      dimensions.push_back(static_cast<uint32_t>(dimension));
      fixedSizeValidities.push_back(
          layer < decoded.values.fixedSizeValidities.size()
              ? std::move(decoded.values.fixedSizeValidities[layer])
              : std::vector<bool>{});
    }
    size_t fixedSizeLayer = dimensions.size();
    auto result = applyStructuralLayers(
        type,
        std::move(leaf),
        state,
        pool,
        dimensions,
        fixedSizeValidities,
        fixedSizeLayer,
        !decoded.values.fixedSizeDimensions.empty());
    BOLT_CHECK_EQ(fixedSizeLayer, 0);
    state.ensureExhausted();
    uint64_t expectedRows = page.length();
    for (const auto dimension : fixedSizeDimensions) {
      if (dimension == 0) {
        continue;
      }
      BOLT_CHECK_EQ(expectedRows % dimension, 0);
      expectedRows /= dimension;
    }
    BOLT_CHECK_EQ(result->size(), expectedRows);
    return result;
  }
  if (layout.layout_case() ==
      ::lance::encodings21::PageLayout::kFullZipLayout) {
    auto decoded = decodeFullZip(layout.full_zip_layout(), page, pool, read);
    if (leafLogicalType == "lance.blob.v2") {
      BOLT_CHECK(fixedSizeDimensions.empty());
      BOLT_CHECK_EQ(type->kind(), TypeKind::VARBINARY);
      BOLT_CHECK_EQ(packedChildLogicalTypes.size(), 5);
      StructuralState state(
          std::move(decoded.rep),
          std::move(decoded.def),
          layout.full_zip_layout().layers(),
          layout.full_zip_layout().num_visible_items());
      return materializeBlobV2(
          decoded,
          layout.full_zip_layout().num_visible_items(),
          state,
          pool,
          blobResolver,
          sourceDataFile,
          prefetch,
          read);
    }
    uint64_t leafValues = layout.full_zip_layout().num_visible_items();
    for (const auto dimension : decoded.fixedSizeDimensions) {
      BOLT_CHECK_LE(
          leafValues, std::numeric_limits<uint64_t>::max() / dimension);
      leafValues *= dimension;
    }
    auto leaf = makeLeafVector(
        packedChildLogicalTypes.empty() ? structuralLeafType(type) : type,
        primitiveLogicalType(leafLogicalType),
        packedChildLogicalTypes,
        decoded,
        leafValues,
        pool);
    StructuralState state(
        std::move(decoded.rep),
        std::move(decoded.def),
        layout.full_zip_layout().layers(),
        layout.full_zip_layout().num_visible_items());
    std::vector<uint32_t> dimensions = fixedSizeDimensions;
    std::vector<std::vector<bool>> fixedSizeValidities(
        fixedSizeDimensions.size());
    for (size_t layer = 0; layer < decoded.fixedSizeDimensions.size();
         ++layer) {
      const auto dimension = decoded.fixedSizeDimensions[layer];
      BOLT_CHECK_LE(dimension, std::numeric_limits<uint32_t>::max());
      dimensions.push_back(static_cast<uint32_t>(dimension));
      fixedSizeValidities.push_back(
          layer < decoded.fixedSizeValidities.size()
              ? std::move(decoded.fixedSizeValidities[layer])
              : std::vector<bool>{});
    }
    size_t fixedSizeLayer = dimensions.size();
    auto result = applyStructuralLayers(
        type,
        std::move(leaf),
        state,
        pool,
        dimensions,
        fixedSizeValidities,
        fixedSizeLayer,
        !decoded.fixedSizeDimensions.empty());
    BOLT_CHECK_EQ(fixedSizeLayer, 0);
    uint64_t expectedRows = page.length();
    for (const auto dimension : fixedSizeDimensions) {
      if (dimension == 0) {
        continue;
      }
      BOLT_CHECK_EQ(expectedRows % dimension, 0);
      expectedRows /= dimension;
    }
    BOLT_CHECK_EQ(result->size(), expectedRows);
    return result;
  }
  BOLT_CHECK_EQ(
      layout.layout_case(),
      ::lance::encodings21::PageLayout::kMiniBlockLayout,
      "Unsupported Lance structural page layout {}",
      static_cast<int>(layout.layout_case()));
  auto decoded = decodeMiniBlock(layout.mini_block_layout(), page, pool, read);
  uint64_t leafValues = layout.mini_block_layout().num_items();
  for (const auto dimension : decoded.fixedSizeDimensions) {
    BOLT_CHECK_LE(leafValues, std::numeric_limits<uint64_t>::max() / dimension);
    leafValues *= dimension;
  }
  auto leaf = makeLeafVector(
      packedChildLogicalTypes.empty() ? structuralLeafType(type) : type,
      primitiveLogicalType(leafLogicalType),
      packedChildLogicalTypes,
      decoded,
      leafValues,
      pool);
  StructuralState state(
      std::move(decoded.rep),
      std::move(decoded.def),
      layout.mini_block_layout().layers(),
      layout.mini_block_layout().num_items());
  std::vector<uint32_t> dimensions = fixedSizeDimensions;
  std::vector<std::vector<bool>> fixedSizeValidities(
      fixedSizeDimensions.size());
  dimensions.reserve(dimensions.size() + decoded.fixedSizeDimensions.size());
  fixedSizeValidities.reserve(
      fixedSizeValidities.size() + decoded.fixedSizeValidities.size());
  for (size_t layer = 0; layer < decoded.fixedSizeDimensions.size(); ++layer) {
    const auto dimension = decoded.fixedSizeDimensions[layer];
    BOLT_CHECK_LE(dimension, std::numeric_limits<uint32_t>::max());
    dimensions.push_back(static_cast<uint32_t>(dimension));
    fixedSizeValidities.push_back(
        layer < decoded.fixedSizeValidities.size()
            ? std::move(decoded.fixedSizeValidities[layer])
            : std::vector<bool>{});
  }
  size_t fixedSizeLayer = dimensions.size();
  auto result = applyStructuralLayers(
      type,
      std::move(leaf),
      state,
      pool,
      dimensions,
      fixedSizeValidities,
      fixedSizeLayer,
      !decoded.fixedSizeDimensions.empty());
  BOLT_CHECK_EQ(fixedSizeLayer, 0);
  uint64_t expectedRows = page.length();
  for (const auto dimension : fixedSizeDimensions) {
    if (dimension == 0) {
      continue;
    }
    BOLT_CHECK_EQ(expectedRows % dimension, 0);
    expectedRows /= dimension;
  }
  BOLT_CHECK_EQ(result->size(), expectedRows);
  return result;
}

bool lanceStructuralLayoutHasCompression(
    const ::lance::encodings21::PageLayout& layout) {
  switch (layout.layout_case()) {
    case ::lance::encodings21::PageLayout::kMiniBlockLayout:
    case ::lance::encodings21::PageLayout::kFullZipLayout:
    case ::lance::encodings21::PageLayout::kBlobLayout:
    case ::lance::encodings21::PageLayout::kSparseLayout:
      return true;
    case ::lance::encodings21::PageLayout::kConstantLayout:
      return layout.constant_layout().has_rep_compression() ||
          layout.constant_layout().has_def_compression();
    default:
      return false;
  }
}

} // namespace bytedance::bolt::lance::reader
