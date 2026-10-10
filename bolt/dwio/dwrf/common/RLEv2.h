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

#pragma once

#include "bolt/common/memory/Memory.h"
#include "bolt/common/process/ProcessBase.h"
#include "bolt/dwio/common/Adaptor.h"
#include "bolt/dwio/common/DataBuffer.h"
#include "bolt/dwio/common/IntDecoder.h"
#include "bolt/dwio/common/exception/Exception.h"

#include <type_traits>
#include <vector>
namespace bytedance::bolt::dwrf {

template <bool isSigned>
class RleDecoderV2 : public dwio::common::IntDecoder<isSigned> {
 public:
  enum EncodingType {
    SHORT_REPEAT = 0,
    DIRECT = 1,
    PATCHED_BASE = 2,
    DELTA = 3
  };

  RleDecoderV2(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      memory::MemoryPool& pool);

  /**
   * Seek to a specific row group.
   */
  void seekToRowGroup(dwio::common::PositionProvider&) override;

  void skipPending() override;

  /**
   * Read a number of values into the batch.
   */
  void next(int64_t* data, uint64_t numValues, const uint64_t* nulls) override;

  void nextLengths(int32_t* const data, const int32_t numValues) override {
    skipPending();
    for (int i = 0; i < numValues; ++i) {
      data[i] = readValue();
    }
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    skipPending();
    int32_t current = visitor.start();
    this->template skip<hasNulls>(current, 0, nulls);

    int32_t toSkip;
    bool atEnd = false;
    const bool allowNulls = hasNulls && visitor.allowNulls();
    // Deterministic, dense ColumnVisitors consume every row in order. Hooks
    // and positional filters retain the per-value path: decoding ahead of
    // their callbacks could advance the stream past an early exit or skip.
    bool batchRead = false;
    if constexpr (
        Visitor::dense && Visitor::FilterType::deterministic &&
        !Visitor::kHasHook &&
        !std::is_same_v<typename Visitor::DataType, int128_t>) {
      batchRead = process::hasSimd() && visitor.filter().isDeterministic();
    }

    for (;;) {
      if (hasNulls && allowNulls && bits::isBitNull(nulls, current)) {
        toSkip = visitor.processNull(atEnd);
      } else {
        if (hasNulls && !allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            this->template skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        }

        if constexpr (
            Visitor::dense && Visitor::FilterType::deterministic &&
            !Visitor::kHasHook &&
            !std::is_same_v<typename Visitor::DataType, int128_t>) {
          if (batchRead) {
            // Use the non-const numRows(): it counts visitor rows, not the
            // reader's output rows. Bound temporary storage independently of
            // the size of the read request.
            const auto limit =
                std::min<int32_t>(kBatchSize, visitor.numRows() - current);
            int32_t count = 1;
            while (count < limit &&
                   (!hasNulls || !bits::isBitNull(nulls, current + count))) {
              ++count;
            }
            if (count > 1) {
              if (bulkScratch_.size() < count) {
                bulkScratch_.resize(kBatchSize);
              }
              doNext(bulkScratch_.data(), count, nullptr);
              for (int32_t i = 0; i < count; ++i) {
                toSkip = visitor.process(bulkScratch_[i], atEnd);
                ++current;
                // Do not attempt a scalar fallback after decoding ahead.
                BOLT_CHECK_EQ(toSkip, 0);
                if (atEnd) {
                  BOLT_CHECK_EQ(i + 1, count);
                  return;
                }
              }
              continue;
            }
          }
        }

        // We are at a non-null value on a row to visit.
        auto value = readValue();
        toSkip = visitor.process(value, atEnd);
      }

      ++current;
      if (toSkip) {
        this->template skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

 private:
  static constexpr int32_t kBatchSize = 512;
  static constexpr int32_t kMinBatchSize = 8;

  // Used by PATCHED_BASE
  void adjustGapAndPatch() {
    curGap = static_cast<uint64_t>(unpackedPatch[patchIdx]) >> patchBitSize;
    curPatch = unpackedPatch[patchIdx] & patchMask;
    actualGap = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (curGap == 255 && curPatch == 0) {
      actualGap += 255;
      ++patchIdx;
      curGap = static_cast<uint64_t>(unpackedPatch[patchIdx]) >> patchBitSize;
      curPatch = unpackedPatch[patchIdx] & patchMask;
    }
    // add the left over gap
    actualGap += curGap;
  }

  void resetReadLongs() {
    bitsLeft = 0;
    curByte = 0;
  }

  void resetRun() {
    resetReadLongs();
    bitSize = 0;
    firstByte = readByte();
    type = static_cast<EncodingType>((firstByte >> 6) & 0x03);
  }

  unsigned char readByte() {
    if (dwio::common::IntDecoder<isSigned>::bufferStart ==
        dwio::common::IntDecoder<isSigned>::bufferEnd) {
      int32_t bufferLength;
      const void* bufferPointer;
      DWIO_ENSURE(
          dwio::common::IntDecoder<isSigned>::inputStream->Next(
              &bufferPointer, &bufferLength),
          "bad read in RleDecoderV2::readByte, ",
          dwio::common::IntDecoder<isSigned>::inputStream->getName());
      dwio::common::IntDecoder<isSigned>::bufferStart =
          static_cast<const char*>(bufferPointer);
      dwio::common::IntDecoder<isSigned>::bufferEnd =
          dwio::common::IntDecoder<isSigned>::bufferStart + bufferLength;
    }

    unsigned char result = static_cast<unsigned char>(
        *dwio::common::IntDecoder<isSigned>::bufferStart++);
    return result;
  }

  int64_t readLongBE(uint64_t bsz);
  uint64_t readLongs(
      int64_t* data,
      uint64_t offset,
      uint64_t len,
      uint64_t fb,
      const uint64_t* nulls = nullptr) {
    if (!nulls && fb >= 1 && fb <= 32 && len >= kMinBatchSize &&
        this->bufferStart != this->bufferEnd) {
      const uint64_t requiredBits = len * fb;
      const uint64_t requiredBytes =
          requiredBits > bitsLeft ? (requiredBits - bitsLeft + 7) / 8 : 0;
      if (requiredBytes <=
          static_cast<uint64_t>(this->bufferEnd - this->bufferStart)) {
        readLongsFromBuffer(data + offset, len, fb);
        return len;
      }
    }
    uint64_t ret = 0;

    // TODO: unroll to improve performance
    for (uint64_t i = offset; i < (offset + len); i++) {
      // skip null positions
      if (nulls && bits::isBitNull(nulls, i)) {
        continue;
      }
      uint64_t result = 0;
      uint64_t bitsLeftToRead = fb;
      while (bitsLeftToRead > bitsLeft) {
        result <<= bitsLeft;
        result |= curByte & ((1 << bitsLeft) - 1);
        bitsLeftToRead -= bitsLeft;
        curByte = readByte();
        bitsLeft = 8;
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        bitsLeft -= static_cast<uint32_t>(bitsLeftToRead);
        result |= (curByte >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
      }
      data[i] = static_cast<int64_t>(result);
      ++ret;
    }

    return ret;
  }

  // MSB-first unpacking after the caller has checked the entire byte range.
  // Unlike the general path, this does not refill or check each input byte.
  void readLongsFromBuffer(int64_t* data, uint64_t len, uint64_t fb);

  uint64_t nextShortRepeats(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);
  uint64_t nextDirect(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);
  uint64_t nextPatched(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);
  uint64_t nextDelta(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);

  int64_t readValue();

  void doNext(
      int64_t* const data,
      const uint64_t numValues,
      const uint64_t* const nulls);

  // Encoding-aware skip of `numValues` non-null values. Advances runRead and
  // the bit stream / unpacked index exactly as doNext would, but without
  // materializing values into an output buffer. Called by skipPending.
  void skipValues(uint64_t numValues);

  unsigned char firstByte;
  uint64_t runLength;
  uint64_t runRead;
  int64_t deltaBase; // Used by DELTA
  uint64_t byteSize; // Used by SHORT_REPEAT and PATCHED_BASE
  int64_t firstValue; // Used by SHORT_REPEAT and DELTA
  int64_t prevValue; // Used by DELTA
  uint32_t bitSize; // Used by DIRECT, PATCHED_BASE and DELTA
  uint32_t bitsLeft; // Used by anything that uses readLongs
  uint32_t curByte; // Used by anything that uses readLongs
  uint32_t patchBitSize; // Used by PATCHED_BASE
  uint64_t unpackedIdx; // Used by PATCHED_BASE
  uint64_t patchIdx; // Used by PATCHED_BASE
  int64_t base; // Used by PATCHED_BASE
  uint64_t curGap; // Used by PATCHED_BASE
  int64_t curPatch; // Used by PATCHED_BASE
  int64_t patchMask; // Used by PATCHED_BASE
  int64_t actualGap; // Used by PATCHED_BASE
  EncodingType type;
  dwio::common::DataBuffer<int64_t> unpacked; // Used by PATCHED_BASE
  dwio::common::DataBuffer<int64_t> unpackedPatch; // Used by PATCHED_BASE
  dwio::common::DataBuffer<int64_t> bulkScratch_;
};

} // namespace bytedance::bolt::dwrf
