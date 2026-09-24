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

#include "bolt/dwio/dwrf/common/RLEv2.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/dwio/common/SeekableInputStream.h"
#include "bolt/dwio/dwrf/common/Common.h"

#ifdef BOLT_RLEV2_SVE
#include "bolt/dwio/dwrf/common/RLEv2SVE.h"
#endif

namespace bytedance::bolt::dwrf {

using memory::MemoryPool;

struct FixedBitSizes {
  enum FBS {
    ONE = 0,
    TWO,
    THREE,
    FOUR,
    FIVE,
    SIX,
    SEVEN,
    EIGHT,
    NINE,
    TEN,
    ELEVEN,
    TWELVE,
    THIRTEEN,
    FOURTEEN,
    FIFTEEN,
    SIXTEEN,
    SEVENTEEN,
    EIGHTEEN,
    NINETEEN,
    TWENTY,
    TWENTYONE,
    TWENTYTWO,
    TWENTYTHREE,
    TWENTYFOUR,
    TWENTYSIX,
    TWENTYEIGHT,
    THIRTY,
    THIRTYTWO,
    FORTY,
    FORTYEIGHT,
    FIFTYSIX,
    SIXTYFOUR
  };
};

inline uint32_t decodeBitWidth(uint32_t n) {
  if (n <= FixedBitSizes::TWENTYFOUR) {
    return n + 1;
  } else if (n == FixedBitSizes::TWENTYSIX) {
    return 26;
  } else if (n == FixedBitSizes::TWENTYEIGHT) {
    return 28;
  } else if (n == FixedBitSizes::THIRTY) {
    return 30;
  } else if (n == FixedBitSizes::THIRTYTWO) {
    return 32;
  } else if (n == FixedBitSizes::FORTY) {
    return 40;
  } else if (n == FixedBitSizes::FORTYEIGHT) {
    return 48;
  } else if (n == FixedBitSizes::FIFTYSIX) {
    return 56;
  } else {
    return 64;
  }
}

inline uint32_t getClosestFixedBits(uint32_t n) {
  if (n == 0) {
    return 1;
  }

  if (n >= 1 && n <= 24) {
    return n;
  } else if (n > 24 && n <= 26) {
    return 26;
  } else if (n > 26 && n <= 28) {
    return 28;
  } else if (n > 28 && n <= 30) {
    return 30;
  } else if (n > 30 && n <= 32) {
    return 32;
  } else if (n > 32 && n <= 40) {
    return 40;
  } else if (n > 40 && n <= 48) {
    return 48;
  } else if (n > 48 && n <= 56) {
    return 56;
  } else {
    return 64;
  }
}

template <bool isSigned>
int64_t RleDecoderV2<isSigned>::readLongBE(uint64_t bsz) {
  int64_t ret = 0, val;
  uint64_t n = bsz;
  while (n > 0) {
    n--;
    val = readByte();
    ret |= (val << (n * 8));
  }
  return ret;
}

template <bool isSigned>
RleDecoderV2<isSigned>::RleDecoderV2(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    MemoryPool& pool)
    : dwio::common::IntDecoder<isSigned>{std::move(input), false, 0},
      firstByte(0),
      runLength(0),
      runRead(0),
      deltaBase(0),
      byteSize(0),
      firstValue(0),
      prevValue(0),
      bitSize(0),
      bitsLeft(0),
      curByte(0),
      patchBitSize(0),
      unpackedIdx(0),
      patchIdx(0),
      base(0),
      curGap(0),
      curPatch(0),
      patchMask(0),
      actualGap(0),
      unpacked(pool, 0),
      unpackedPatch(pool, 0),
      bulkScratch_(pool, 0) {
  // PASS
}

template RleDecoderV2<true>::RleDecoderV2(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    MemoryPool& pool);
template RleDecoderV2<false>::RleDecoderV2(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    MemoryPool& pool);

template <bool isSigned>
void RleDecoderV2<isSigned>::seekToRowGroup(
    dwio::common::PositionProvider& location) {
  // move the input stream
  dwio::common::IntDecoder<isSigned>::inputStream->seekToPosition(location);
  // clear state
  dwio::common::IntDecoder<isSigned>::bufferEnd =
      dwio::common::IntDecoder<isSigned>::bufferStart = 0;
  runRead = runLength = 0;
  // skip ahead the given number of records
  this->pendingSkip = location.next();
}

template void RleDecoderV2<true>::seekToRowGroup(
    dwio::common::PositionProvider& location);
template void RleDecoderV2<false>::seekToRowGroup(
    dwio::common::PositionProvider& location);

template <bool isSigned>
void RleDecoderV2<isSigned>::skipPending() {
  auto numValues = this->pendingSkip;
  this->pendingSkip = 0;
  if (numValues == 0) {
    return;
  }
  skipValues(numValues);
}

template void RleDecoderV2<true>::skipPending();
template void RleDecoderV2<false>::skipPending();

template <bool isSigned>
void RleDecoderV2<isSigned>::skipValues(uint64_t numValues) {
  // Encoding-aware skip: advance runRead and the bit stream / unpacked index
  // exactly as doNext would, without materializing values. Mirrors the control
  // flow of doNext (resetRun at run boundaries, dispatch by type) but each
  // branch advances state arithmetically instead of decoding into a buffer.
  while (numValues > 0) {
    if (runRead == runLength) {
      resetRun();
    }

    switch (type) {
      case SHORT_REPEAT: {
        // Header decode (same guard as nextShortRepeats).
        if (runRead == runLength) {
          byteSize = (firstByte >> 3) & 0x07;
          byteSize += 1;
          runLength = firstByte & 0x07;
          runLength += RLE_MINIMUM_REPEAT;
          runRead = 0;
          firstValue = readLongBE(byteSize);
          if (isSigned) {
            firstValue =
                ZigZag::decode<uint64_t>(static_cast<uint64_t>(firstValue));
          }
        }
        uint64_t nSkip = std::min(runLength - runRead, numValues);
        // Repeated value: no bit stream to advance, just count.
        runRead += nSkip;
        numValues -= nSkip;
        break;
      }
      case DIRECT: {
        // Header decode (same guard as nextDirect).
        if (runRead == runLength) {
          unsigned char fbo = (firstByte >> 1) & 0x1f;
          bitSize = decodeBitWidth(fbo);
          runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
          runLength |= readByte();
          runLength += 1;
          runRead = 0;
        }
        uint64_t nSkip = std::min(runLength - runRead, numValues);
        // Advance the MSB-first bit stream by nSkip * bitSize bits, leaving
        // curByte/bitsLeft in the same state as if readLongs had decoded the
        // values one-by-one.
        uint64_t totalBits = nSkip * bitSize;
        if (totalBits <= bitsLeft) {
          bitsLeft -= static_cast<uint32_t>(totalBits);
        } else {
          uint64_t need = totalBits - bitsLeft;
          uint64_t extraBytes = (need + 7) / 8;
          for (uint64_t b = 0; b < extraBytes; ++b) {
            curByte = readByte();
          }
          bitsLeft = static_cast<uint32_t>(extraBytes * 8 - need);
        }
        runRead += nSkip;
        numValues -= nSkip;
        break;
      }
      case PATCHED_BASE: {
        // Header decode + full pre-decode (same as nextPatched).
        if (runRead == runLength) {
          unsigned char fbo = (firstByte >> 1) & 0x1f;
          bitSize = decodeBitWidth(fbo);
          runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
          runLength |= readByte();
          runLength += 1;
          runRead = 0;

          uint64_t thirdByte = readByte();
          byteSize = (thirdByte >> 5) & 0x07;
          byteSize += 1;

          uint32_t pwo = thirdByte & 0x1f;
          patchBitSize = decodeBitWidth(pwo);

          uint64_t fourthByte = readByte();
          uint32_t pgw = (fourthByte >> 5) & 0x07;
          pgw += 1;

          size_t pl = fourthByte & 0x1f;
          DWIO_ENSURE_NE(
              pl,
              0,
              "Corrupt PATCHED_BASE encoded data (pl==0)! ",
              dwio::common::IntDecoder<isSigned>::inputStream->getName());

          base = readLongBE(byteSize);
          int64_t mask = (static_cast<int64_t>(1) << ((byteSize * 8) - 1));
          if ((base & mask) != 0) {
            base = base & ~mask;
            base = -base;
          }

          unpacked.resize(runLength);
          unpackedIdx = 0;
          readLongs(unpacked.data(), 0, runLength, bitSize);
          resetReadLongs();

          unpackedPatch.resize(pl);
          patchIdx = 0;
          DWIO_ENSURE_LE(
              (patchBitSize + pgw),
              64,
              "Corrupt PATCHED_BASE encoded data (patchBitSize + pgw > 64)! ",
              dwio::common::IntDecoder<isSigned>::inputStream->getName());
          uint32_t cfb = getClosestFixedBits(patchBitSize + pgw);
          readLongs(unpackedPatch.data(), 0, pl, cfb);
          resetReadLongs();

          patchMask = ((static_cast<int64_t>(1) << patchBitSize) - 1);
          adjustGapAndPatch();
        }
        uint64_t nSkip = std::min(runLength - runRead, numValues);
        // Advance through the pre-decoded run, advancing patch state at each
        // patch position (mirrors nextPatched's per-value loop body).
        for (uint64_t i = 0; i < nSkip; ++i) {
          if (static_cast<int64_t>(unpackedIdx) != actualGap) {
            // no patching required
          } else {
            ++patchIdx;
            if (patchIdx < unpackedPatch.size()) {
              adjustGapAndPatch();
              actualGap += unpackedIdx;
            }
          }
          ++runRead;
          ++unpackedIdx;
        }
        numValues -= nSkip;
        break;
      }
      case DELTA: {
        // Header decode (same guard as nextDelta).
        if (runRead == runLength) {
          unsigned char fbo = (firstByte >> 1) & 0x1f;
          if (fbo != 0) {
            bitSize = decodeBitWidth(fbo);
          } else {
            bitSize = 0;
          }
          runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
          runLength |= readByte();
          ++runLength;
          runRead = deltaBase = 0;

          if constexpr (isSigned) {
            firstValue = dwio::common::IntDecoder<isSigned>::readVsLong();
          } else {
            firstValue = static_cast<int64_t>(
                dwio::common::IntDecoder<isSigned>::readVuLong());
          }
          prevValue = firstValue;
          deltaBase = dwio::common::IntDecoder<isSigned>::readVsLong();
        }
        uint64_t nSkip = std::min(runLength - runRead, numValues);

        if (bitSize == 0) {
          // Fixed delta: pure arithmetic series. The first value (firstValue)
          // occupies runRead==0 with no delta added; each subsequent position
          // adds deltaBase.
          if (runRead == 0) {
            if (nSkip == 1) {
              prevValue = firstValue;
            } else {
              prevValue =
                  firstValue + static_cast<int64_t>(nSkip - 1) * deltaBase;
            }
          } else {
            prevValue += static_cast<int64_t>(nSkip) * deltaBase;
          }
          runRead += nSkip;
        } else {
          // Variable delta: read and accumulate deltas from the bit stream.
          //   - runRead==0: position 0 is firstValue (no delta)
          //   - runRead<2:  position 1 is firstValue + deltaBase
          //   - runRead>=2: subsequent positions add/subtract deltas
          uint64_t remaining = nSkip;
          if (runRead == 0 && remaining > 0) {
            // position 0: firstValue, prevValue already == firstValue
            ++runRead;
            --remaining;
          }
          if (runRead < 2 && remaining > 0) {
            // position 1: firstValue + deltaBase
            prevValue = firstValue + deltaBase;
            ++runRead;
            --remaining;
          }
          if (remaining > 0) {
            constexpr uint64_t SCRATCH = 256;
            int64_t local[SCRATCH];
            int64_t* scratch = local;
            if (remaining > SCRATCH) {
              if (unpacked.size() < remaining) {
                unpacked.resize(remaining);
              }
              scratch = unpacked.data();
            }
            readLongs(scratch, 0, remaining, bitSize, nullptr);
            if (deltaBase < 0) {
              for (uint64_t i = 0; i < remaining; ++i) {
                prevValue -= scratch[i];
              }
            } else {
              for (uint64_t i = 0; i < remaining; ++i) {
                prevValue += scratch[i];
              }
            }
            runRead += remaining;
          }
        }
        numValues -= nSkip;
        break;
      }
      default:
        DWIO_RAISE("unknown encoding");
    }
  }
}

template void RleDecoderV2<true>::skipValues(uint64_t numValues);
template void RleDecoderV2<false>::skipValues(uint64_t numValues);

template <bool isSigned>
void RleDecoderV2<isSigned>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls) {
  skipPending();
  doNext(data, numValues, nulls);
}

template <bool isSigned>
void RleDecoderV2<isSigned>::doNext(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls) {
  uint64_t nRead = 0;

  while (nRead < numValues) {
    // Skip any nulls before attempting to read first byte.
    while (nulls && bits::isBitNull(nulls, nRead)) {
      if (++nRead == numValues) {
        return; // ended with null values
      }
    }

    if (runRead == runLength) {
      resetRun();
    }

    uint64_t offset = nRead, length = numValues - nRead;

    switch (type) {
      case SHORT_REPEAT:
        nRead += nextShortRepeats(data, offset, length, nulls);
        break;
      case DIRECT:
        nRead += nextDirect(data, offset, length, nulls);
        break;
      case PATCHED_BASE:
        nRead += nextPatched(data, offset, length, nulls);
        break;
      case DELTA:
        nRead += nextDelta(data, offset, length, nulls);
        break;
      default:
        DWIO_RAISE("unknown encoding");
    }
  }
}

template void RleDecoderV2<true>::doNext(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls);
template void RleDecoderV2<false>::doNext(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
void RleDecoderV2<isSigned>::readLongsFromBuffer(
    int64_t* data,
    uint64_t len,
    uint64_t fb) {
  auto* input = reinterpret_cast<const uint8_t*>(this->bufferStart);
  uint32_t available = bitsLeft;
  uint32_t byte = curByte;
  for (uint64_t i = 0; i < len; ++i) {
    uint64_t value = 0;
    uint32_t needed = fb;
    while (needed > available) {
      value = (value << available) | (byte & ((1u << available) - 1));
      needed -= available;
      byte = *input++;
      available = 8;
    }
    available -= needed;
    data[i] = static_cast<int64_t>(
        (value << needed) | ((byte >> available) & ((1u << needed) - 1)));
  }
  this->bufferStart = reinterpret_cast<const char*>(input);
  bitsLeft = available;
  curByte = byte;
}

template void
RleDecoderV2<true>::readLongsFromBuffer(int64_t*, uint64_t, uint64_t);
template void
RleDecoderV2<false>::readLongsFromBuffer(int64_t*, uint64_t, uint64_t);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextShortRepeats(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls) {
  if (runRead == runLength) {
    // extract the number of fixed bytes
    byteSize = (firstByte >> 3) & 0x07;
    byteSize += 1;

    runLength = firstByte & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    runLength += RLE_MINIMUM_REPEAT;
    runRead = 0;

    // read the repeated value which is store using fixed bytes
    firstValue = readLongBE(byteSize);

    if (isSigned) {
      firstValue = ZigZag::decode<uint64_t>(static_cast<uint64_t>(firstValue));
    }
  }

  uint64_t nRead = std::min(runLength - runRead, numValues);

#ifdef BOLT_RLEV2_SVE
  if (nRead > 1 && process::hasSve()) {
    runRead += detail::fillRepeatedSve(
        data, offset, offset + nRead, nulls, firstValue);
    return nRead;
  }
#endif

  if (nulls) {
    for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
      if (!bits::isBitNull(nulls, pos)) {
        data[pos] = firstValue;
        ++runRead;
      }
    }
  } else {
    std::fill_n(data + offset, nRead, firstValue);
    runRead += nRead;
  }

  return nRead;
}

template uint64_t RleDecoderV2<true>::nextShortRepeats(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);
template uint64_t RleDecoderV2<false>::nextShortRepeats(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextDirect(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    bitSize = decodeBitWidth(fbo);

    // extract the run length
    runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
    runLength |= readByte();
    // runs are one off
    runLength += 1;
    runRead = 0;
  }

  uint64_t nRead = std::min(runLength - runRead, numValues);

  runRead += readLongs(data, offset, nRead, bitSize, nulls);

  if (isSigned) {
#ifdef BOLT_RLEV2_SVE
    if (nRead > 1 && process::hasSve()) {
      detail::zigzagDecodeSve(data, offset, offset + nRead, nulls);
      return nRead;
    }
#endif
    if (nulls) {
      for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
        if (!bits::isBitNull(nulls, pos)) {
          data[pos] =
              ZigZag::decode<uint64_t>(static_cast<uint64_t>(data[pos]));
        }
      }
    } else {
      using Batch = xsimd::batch<uint64_t>;
      uint64_t pos = offset;
      for (; pos + Batch::size <= offset + nRead; pos += Batch::size) {
        auto* values = reinterpret_cast<uint64_t*>(data + pos);
        const auto encoded = Batch::load_unaligned(values);
        const auto decoded = (encoded >> 1) ^ (Batch(0) - (encoded & Batch(1)));
        decoded.store_unaligned(values);
      }
      for (; pos < offset + nRead; ++pos) {
        data[pos] = ZigZag::decode<uint64_t>(static_cast<uint64_t>(data[pos]));
      }
    }
  }

  return nRead;
}

template uint64_t RleDecoderV2<true>::nextDirect(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);
template uint64_t RleDecoderV2<false>::nextDirect(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextPatched(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    bitSize = decodeBitWidth(fbo);

    // extract the run length
    runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
    runLength |= readByte();
    // runs are one off
    runLength += 1;
    runRead = 0;

    // extract the number of bytes occupied by base
    uint64_t thirdByte = readByte();
    byteSize = (thirdByte >> 5) & 0x07;
    // base width is one off
    byteSize += 1;

    // extract patch width
    uint32_t pwo = thirdByte & 0x1f;
    patchBitSize = decodeBitWidth(pwo);

    // read fourth byte and extract patch gap width
    uint64_t fourthByte = readByte();
    uint32_t pgw = (fourthByte >> 5) & 0x07;
    // patch gap width is one off
    pgw += 1;

    // extract the length of the patch list
    size_t pl = fourthByte & 0x1f;
    DWIO_ENSURE_NE(
        pl,
        0,
        "Corrupt PATCHED_BASE encoded data (pl==0)! ",
        dwio::common::IntDecoder<isSigned>::inputStream->getName());

    // read the next base width number of bytes to extract base value
    base = readLongBE(byteSize);
    int64_t mask = (static_cast<int64_t>(1) << ((byteSize * 8) - 1));
    // if mask of base value is 1 then base is negative value else positive
    if ((base & mask) != 0) {
      base = base & ~mask;
      base = -base;
    }

    // TODO: something more efficient than resize
    unpacked.resize(runLength);
    unpackedIdx = 0;
    readLongs(unpacked.data(), 0, runLength, bitSize);
    // any remaining bits are thrown out
    resetReadLongs();

    // TODO: something more efficient than resize
    unpackedPatch.resize(pl);
    patchIdx = 0;
    // TODO: Skip corrupt?
    //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
    DWIO_ENSURE_LE(
        (patchBitSize + pgw),
        64,
        "Corrupt PATCHED_BASE encoded data (patchBitSize + pgw > 64)! ",
        dwio::common::IntDecoder<isSigned>::inputStream->getName());
    uint32_t cfb = getClosestFixedBits(patchBitSize + pgw);
    readLongs(unpackedPatch.data(), 0, pl, cfb);
    // any remaining bits are thrown out
    resetReadLongs();

    // apply the patch directly when decoding the packed data
    patchMask = ((static_cast<int64_t>(1) << patchBitSize) - 1);

    adjustGapAndPatch();
  }

  uint64_t nRead = std::min(runLength - runRead, numValues);

  for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
    // skip null positions
    if (nulls && bits::isBitNull(nulls, pos)) {
      continue;
    }
    if (static_cast<int64_t>(unpackedIdx) != actualGap) {
      // no patching required. add base to unpacked value to get final value
      data[pos] = base + unpacked[unpackedIdx];
    } else {
      // extract the patch value
      int64_t patchedVal = unpacked[unpackedIdx] | (curPatch << bitSize);

      // add base to patched value
      data[pos] = base + patchedVal;

      // increment the patch to point to next entry in patch list
      ++patchIdx;

      if (patchIdx < unpackedPatch.size()) {
        adjustGapAndPatch();

        // next gap is relative to the current gap
        actualGap += unpackedIdx;
      }
    }

    ++runRead;
    ++unpackedIdx;
  }

  return nRead;
}

template uint64_t RleDecoderV2<true>::nextPatched(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template uint64_t RleDecoderV2<false>::nextPatched(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextDelta(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    if (fbo != 0) {
      bitSize = decodeBitWidth(fbo);
    } else {
      bitSize = 0;
    }

    // extract the run length
    runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
    runLength |= readByte();
    ++runLength; // account for first value
    runRead = deltaBase = 0;

    // read the first value stored as vint
    if constexpr (isSigned) {
      firstValue = dwio::common::IntDecoder<isSigned>::readVsLong();
    } else {
      firstValue = static_cast<int64_t>(
          dwio::common::IntDecoder<isSigned>::readVuLong());
    }

    prevValue = firstValue;

    // read the fixed delta value stored as vint (deltas can be negative even
    // if all number are positive)
    deltaBase = dwio::common::IntDecoder<isSigned>::readVsLong();
  }

  uint64_t nRead = std::min(runLength - runRead, numValues);

  uint64_t pos = offset;
  for (; pos < offset + nRead; ++pos) {
    // skip null positions
    if (!nulls || !bits::isBitNull(nulls, pos)) {
      break;
    }
  }
  if (runRead == 0 && pos < offset + nRead) {
    data[pos++] = firstValue;
    ++runRead;
  }

  if (bitSize == 0) {
#ifdef BOLT_RLEV2_SVE
    if (offset + nRead - pos > 1 && process::hasSve()) {
      runRead += detail::fixedDeltaSve(
          data, pos, offset + nRead, nulls, deltaBase, prevValue);
      return nRead;
    }
#endif
    if (!nulls) {
      using Batch = xsimd::batch<uint64_t>;
      const auto delta = static_cast<uint64_t>(deltaBase);
      const auto steps = (simd::iota<uint64_t>() + Batch(1)) * Batch(delta);
      for (; pos + Batch::size <= offset + nRead; pos += Batch::size) {
        const auto values = Batch(static_cast<uint64_t>(prevValue)) + steps;
        values.store_unaligned(reinterpret_cast<uint64_t*>(data + pos));
        prevValue = static_cast<int64_t>(
            static_cast<uint64_t>(prevValue) + Batch::size * delta);
        runRead += Batch::size;
      }
    }
    // add fixed deltas to adjacent values
    for (; pos < offset + nRead; ++pos) {
      // skip null positions
      if (nulls && bits::isBitNull(nulls, pos)) {
        continue;
      }
      prevValue = data[pos] = prevValue + deltaBase;
      ++runRead;
    }
  } else {
    for (; pos < offset + nRead; ++pos) {
      // skip null positions
      if (!nulls || !bits::isBitNull(nulls, pos)) {
        break;
      }
    }
    if (runRead < 2 && pos < offset + nRead) {
      // add delta base and first value
      prevValue = data[pos++] = firstValue + deltaBase;
      ++runRead;
    }

    // write the unpacked values, add it to previous value and store final
    // value to result buffer. if the delta base value is negative then it
    // is a decreasing sequence else an increasing sequence
    uint64_t remaining = (offset + nRead) - pos;
    runRead += readLongs(data, pos, remaining, bitSize, nulls);

#ifdef BOLT_RLEV2_SVE
    if (remaining > 1 && process::hasSve()) {
      detail::variableDeltaSve(
          data, pos, offset + nRead, nulls, deltaBase < 0, prevValue);
      return nRead;
    }
#endif

    if (deltaBase < 0) {
      for (; pos < offset + nRead; ++pos) {
        // skip null positions
        if (nulls && bits::isBitNull(nulls, pos)) {
          continue;
        }
        prevValue = data[pos] = prevValue - data[pos];
      }
    } else {
      for (; pos < offset + nRead; ++pos) {
        // skip null positions
        if (nulls && bits::isBitNull(nulls, pos)) {
          continue;
        }
        prevValue = data[pos] = prevValue + data[pos];
      }
    }
  }
  return nRead;
}

template uint64_t RleDecoderV2<true>::nextDelta(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template uint64_t RleDecoderV2<false>::nextDelta(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
int64_t RleDecoderV2<isSigned>::readValue() {
  if (runRead == runLength) {
    resetRun();
  }

  uint64_t nRead = 0;
  int64_t value = 0;
  switch (type) {
    case SHORT_REPEAT:
      nRead = nextShortRepeats(&value, 0, 1, nullptr);
      break;
    case DIRECT:
      nRead = nextDirect(&value, 0, 1, nullptr);
      break;
    case PATCHED_BASE:
      nRead = nextPatched(&value, 0, 1, nullptr);
      break;
    case DELTA:
      nRead = nextDelta(&value, 0, 1, nullptr);
      break;
    default:
      DWIO_RAISE("unknown encoding");
  }
  BOLT_CHECK(nRead == (uint64_t)1);
  return value;
}

template int64_t RleDecoderV2<true>::readValue();

template int64_t RleDecoderV2<false>::readValue();

} // namespace bytedance::bolt::dwrf
