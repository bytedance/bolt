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

#include "bolt/dwio/lance/NativeLanceBitpack.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <numeric>
#include <vector>

#include <folly/Portability.h>
#include <folly/Range.h>
#include <folly/lang/Bits.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/common/BitPackDecoder.h"
#include "bolt/vector/TypeAliases.h"

namespace bytedance::bolt::lance::reader {
namespace {

uint64_t extractPackedValue(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t bitOffset,
    uint8_t bitWidth) {
  BOLT_CHECK_LE(bitOffset + bitWidth, inputBytes * 8);
  uint64_t value = 0;
  uint8_t outputBit = 0;
  while (outputBit < bitWidth) {
    const auto byte = bitOffset / 8;
    const auto bitInByte = static_cast<uint8_t>(bitOffset % 8);
    const auto take = static_cast<uint8_t>(
        std::min<int>(bitWidth - outputBit, 8 - bitInByte));
    const auto mask = static_cast<uint8_t>((1U << take) - 1);
    value |= static_cast<uint64_t>((input[byte] >> bitInByte) & mask)
        << outputBit;
    bitOffset += take;
    outputBit += take;
  }
  return value;
}

template <typename T>
void storeValue(uint64_t value, uint64_t index, uint8_t* output) {
  auto encoded = folly::Endian::little(static_cast<T>(value));
  std::memcpy(output + index * sizeof(T), &encoded, sizeof(T));
}

uint64_t signExtend(
    uint64_t value,
    uint8_t compressedBits,
    uint8_t uncompressedBits,
    bool isSigned) {
  if (!isSigned || compressedBits == uncompressedBits) {
    return value;
  }
  const auto signBit = uint64_t{1} << (compressedBits - 1);
  if ((value & signBit) == 0) {
    return value;
  }
  const auto valueMask = compressedBits == 64
      ? std::numeric_limits<uint64_t>::max()
      : (uint64_t{1} << compressedBits) - 1;
  const auto outputMask = uncompressedBits == 64
      ? std::numeric_limits<uint64_t>::max()
      : (uint64_t{1} << uncompressedBits) - 1;
  return (value | ~valueMask) & outputMask;
}

void storeScalarValue(
    uint64_t value,
    uint64_t index,
    uint8_t uncompressedBits,
    uint8_t* output) {
  switch (uncompressedBits) {
    case 8:
      storeValue<uint8_t>(value, index, output);
      return;
    case 16:
      storeValue<uint16_t>(value, index, output);
      return;
    case 32:
      storeValue<uint32_t>(value, index, output);
      return;
    case 64:
      storeValue<uint64_t>(value, index, output);
      return;
    default:
      BOLT_UNREACHABLE("Unsupported Lance bit-packed output width");
  }
}

template <typename T>
void signExtendValues(
    T* values,
    uint64_t count,
    uint8_t compressedBits,
    bool isSigned) {
  if (!isSigned || compressedBits == sizeof(T) * 8) {
    return;
  }
  const auto signBit = T{1} << (compressedBits - 1);
  const auto extensionMask =
      static_cast<T>(~((uint64_t{1} << compressedBits) - 1));
  for (uint64_t i = 0; i < count; ++i) {
    if ((values[i] & signBit) != 0) {
      values[i] |= extensionMask;
    }
  }
}

template <typename T>
void decodeDenseWithBolt(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t bitOffset,
    uint64_t count,
    uint8_t compressedBits,
    bool isSigned,
    uint8_t* output) {
  auto prefix = uint64_t{0};
  while (prefix < count && (bitOffset + prefix * compressedBits) % 8 != 0) {
    ++prefix;
  }
  if (prefix != 0) {
    decodeLanceBitpackedScalar(
        input,
        inputBytes,
        bitOffset,
        prefix,
        compressedBits,
        sizeof(T) * 8,
        false,
        output);
  }
  const auto consumedBytes = (bitOffset + prefix * compressedBits) / 8;
  auto* inputIter = input + consumedBytes;
  auto* outputIter = reinterpret_cast<T*>(output) + prefix;
  const auto bulkCount = (count - prefix) & ~uint64_t{7};
  if (bulkCount != 0) {
    dwio::common::unpack<T>(
        inputIter,
        inputBytes - consumedBytes,
        bulkCount,
        compressedBits,
        outputIter);
  }
  const auto tailCount = count - prefix - bulkCount;
  if (tailCount != 0) {
    decodeLanceBitpackedScalar(
        inputIter,
        inputBytes - static_cast<uint64_t>(inputIter - input),
        0,
        tailCount,
        compressedBits,
        sizeof(T) * 8,
        false,
        reinterpret_cast<uint8_t*>(outputIter));
  }
  signExtendValues(
      reinterpret_cast<T*>(output), count, compressedBits, isSigned);
}

template <typename T>
void decodeRowsWithBolt(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t bitOffset,
    uint64_t count,
    uint8_t compressedBits,
    bool isSigned,
    uint8_t* output) {
  BOLT_CHECK_LE(
      count, static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
  thread_local std::vector<vector_size_t> rows;
  if (rows.size() < count) {
    const auto oldSize = rows.size();
    rows.resize(count);
    std::iota(rows.begin() + oldSize, rows.end(), oldSize);
  }
  auto* typedOutput = reinterpret_cast<T*>(output);
  dwio::common::unpack<T>(
      reinterpret_cast<const uint64_t*>(input),
      static_cast<int32_t>(bitOffset),
      folly::Range<const vector_size_t*>(rows.data(), count),
      0,
      compressedBits,
      reinterpret_cast<const char*>(input) + inputBytes,
      typedOutput);
  signExtendValues(typedOutput, count, compressedBits, isSigned);
}

template <typename T>
void decodeFastLanesForNonNeg(
    const uint8_t* input,
    uint64_t rowOffset,
    uint64_t count,
    uint8_t compressedBits,
    uint8_t* output) {
  constexpr uint64_t kChunkRows = 1'024;
  constexpr std::array<uint8_t, 8> kGroupForBlock64{0, 4, 2, 6, 1, 5, 3, 7};
  constexpr std::array<uint8_t, 4> kGroupForBlock32{0, 2, 1, 3};
  constexpr std::array<uint8_t, 2> kGroupForBlock16{0, 1};
  constexpr std::array<uint8_t, 1> kGroupForBlock8{0};
  constexpr auto kOutputBits = sizeof(T) * 8;
  constexpr auto kLanes = kChunkRows / kOutputBits;
  const auto chunkBytes = kChunkRows * compressedBits / 8;
  const auto mask = compressedBits == 64 ? std::numeric_limits<uint64_t>::max()
                                         : (uint64_t{1} << compressedBits) - 1;
  auto* typedOutput = reinterpret_cast<T*>(output);
  for (uint64_t outputIndex = 0; outputIndex < count; ++outputIndex) {
    const auto encodedIndex = rowOffset + outputIndex;
    const auto* chunk = input + encodedIndex / kChunkRows * chunkBytes;
    const auto logicalIndex = encodedIndex % kChunkRows;
    const auto sublane = logicalIndex / 128;
    const auto lanePosition = logicalIndex % 128;
    const auto block = lanePosition / kLanes;
    uint64_t group;
    if constexpr (kOutputBits == 64) {
      group = kGroupForBlock64[block];
    } else if constexpr (kOutputBits == 32) {
      group = kGroupForBlock32[block];
    } else if constexpr (kOutputBits == 16) {
      group = kGroupForBlock16[block];
    } else {
      group = kGroupForBlock8[block];
    }
    const auto row = group * 8 + sublane;
    const auto lane = lanePosition % kLanes;
    const auto laneBit = row * compressedBits;
    const auto word = laneBit / kOutputBits;
    const auto bitInWord = laneBit % kOutputBits;
    const auto* wordAddress = chunk + (word * kLanes + lane) * sizeof(T);
    auto value = static_cast<uint64_t>(folly::Endian::little(
                     folly::loadUnaligned<T>(wordAddress))) >>
        bitInWord;
    if (bitInWord + compressedBits > kOutputBits) {
      const auto* nextWordAddress =
          chunk + ((word + 1) * kLanes + lane) * sizeof(T);
      value |= static_cast<uint64_t>(folly::Endian::little(
                   folly::loadUnaligned<T>(nextWordAddress)))
          << (kOutputBits - bitInWord);
    }
    typedOutput[outputIndex] = static_cast<T>(value & mask);
  }
}

} // namespace

void decodeLanceBitpackedScalar(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t bitOffset,
    uint64_t count,
    uint8_t compressedBits,
    uint8_t uncompressedBits,
    bool isSigned,
    uint8_t* output) {
  BOLT_CHECK_LE(compressedBits, uncompressedBits);
  BOLT_CHECK(
      uncompressedBits == 8 || uncompressedBits == 16 ||
      uncompressedBits == 32 || uncompressedBits == 64);
  BOLT_CHECK_LE(bitOffset + count * compressedBits, inputBytes * 8);
  if (compressedBits == 0) {
    std::memset(output, 0, count * uncompressedBits / 8);
    return;
  }
  for (uint64_t i = 0; i < count; ++i) {
    auto value = extractPackedValue(
        input, inputBytes, bitOffset + i * compressedBits, compressedBits);
    value = signExtend(value, compressedBits, uncompressedBits, isSigned);
    storeScalarValue(value, i, uncompressedBits, output);
  }
}

void decodeLanceBitpacked(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t bitOffset,
    uint64_t count,
    uint8_t compressedBits,
    uint8_t uncompressedBits,
    bool isSigned,
    uint8_t* output) {
  BOLT_CHECK_LE(compressedBits, uncompressedBits);
  BOLT_CHECK_LE(bitOffset + count * compressedBits, inputBytes * 8);
  BOLT_CHECK_LE(
      bitOffset, static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
  if (compressedBits == 0) {
    std::memset(output, 0, count * uncompressedBits / 8);
    return;
  }
  if (!folly::kIsLittleEndian) {
    decodeLanceBitpackedScalar(
        input,
        inputBytes,
        bitOffset,
        count,
        compressedBits,
        uncompressedBits,
        isSigned,
        output);
    return;
  }
  switch (uncompressedBits) {
    case 8:
      decodeDenseWithBolt<uint8_t>(
          input,
          inputBytes,
          bitOffset,
          count,
          compressedBits,
          isSigned,
          output);
      return;
    case 16:
      decodeDenseWithBolt<uint16_t>(
          input,
          inputBytes,
          bitOffset,
          count,
          compressedBits,
          isSigned,
          output);
      return;
    case 32:
      decodeDenseWithBolt<uint32_t>(
          input,
          inputBytes,
          bitOffset,
          count,
          compressedBits,
          isSigned,
          output);
      return;
    case 64:
      // BitPackDecoder explicitly instantiates the row-set overload for
      // int64_t. The decoded bits are identical for signed and unsigned Lance
      // values, and sign extension is applied separately below.
      decodeRowsWithBolt<int64_t>(
          input,
          inputBytes,
          bitOffset,
          count,
          compressedBits,
          isSigned,
          output);
      return;
    default:
      BOLT_UNREACHABLE("Unsupported Lance bit-packed output width");
  }
}

void decodeLanceBitpackedForNonNeg(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t rowOffset,
    uint64_t count,
    uint8_t compressedBits,
    uint8_t uncompressedBits,
    uint8_t* output) {
  constexpr uint64_t kChunkRows = 1'024;
  BOLT_CHECK_LE(compressedBits, uncompressedBits);
  BOLT_CHECK(
      uncompressedBits == 8 || uncompressedBits == 16 ||
      uncompressedBits == 32 || uncompressedBits == 64);
  if (count == 0) {
    return;
  }
  if (compressedBits == 0) {
    std::memset(output, 0, count * uncompressedBits / 8);
    return;
  }
  BOLT_CHECK_LE(rowOffset, std::numeric_limits<uint64_t>::max() - count);
  const auto chunkBytes = kChunkRows * compressedBits / 8;
  const auto requiredChunks = (rowOffset + count + kChunkRows - 1) / kChunkRows;
  BOLT_CHECK_LE(requiredChunks, inputBytes / chunkBytes);
  switch (uncompressedBits) {
    case 8:
      decodeFastLanesForNonNeg<uint8_t>(
          input, rowOffset, count, compressedBits, output);
      return;
    case 16:
      decodeFastLanesForNonNeg<uint16_t>(
          input, rowOffset, count, compressedBits, output);
      return;
    case 32:
      decodeFastLanesForNonNeg<uint32_t>(
          input, rowOffset, count, compressedBits, output);
      return;
    case 64:
      decodeFastLanesForNonNeg<uint64_t>(
          input, rowOffset, count, compressedBits, output);
      return;
    default:
      BOLT_UNREACHABLE("Unsupported FastLanes word width");
  }
}

} // namespace bytedance::bolt::lance::reader
