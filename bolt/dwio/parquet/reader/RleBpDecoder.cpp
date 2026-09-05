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

#include "bolt/dwio/parquet/reader/RleBpDecoder.h"

#include <exception>

#include <folly/Varint.h>
namespace bytedance::bolt::parquet {

void RleBpDecoder::skip(uint64_t numValues) {
  while (numValues > 0) {
    if (!remainingValues_) {
      readHeader(numValues);
    }
    uint64_t count = std::min<int>(numValues, remainingValues_);
    remainingValues_ -= count;
    numValues -= count;
    if (!repeating_) {
      auto numBits = bitWidth_ * count + bitOffset_;
      bufferStart_ += numBits >> 3;
      bitOffset_ = numBits & 7;
    }
  }
}

void RleBpDecoder::readBits(
    int32_t numValues,
    uint64_t* FOLLY_NONNULL outputBuffer,
    bool* FOLLY_NULLABLE allOnes) {
  BOLT_CHECK_EQ(1, bitWidth_);
  auto toRead = numValues;
  int32_t numWritten = 0;
  if (allOnes) {
    // initialize the all ones indicator to false for safety.
    *allOnes = false;
  }
  while (toRead) {
    if (!remainingValues_) {
      readHeader(toRead);
    }
    auto consumed = std::min<int32_t>(toRead, remainingValues_);

    if (repeating_) {
      if (allOnes && value_ && toRead == numValues &&
          remainingValues_ >= numValues) {
        // The whole read is covered by a RLE of ones and 'allOnes' is
        // provided, so we can shortcut the read.
        remainingValues_ -= toRead;
        *allOnes = true;
        return;
      }

      bits::fillBits(
          outputBuffer, numWritten, numWritten + consumed, value_ != 0);
    } else {
      bits::copyBits(
          reinterpret_cast<const uint64_t*>(bufferStart_),
          bitOffset_,
          outputBuffer,
          numWritten,
          consumed);
      int64_t offset = bitOffset_ + consumed;
      bufferStart_ += offset >> 3;
      bitOffset_ = offset & 7;
    }
    numWritten += consumed;
    toRead -= consumed;
    remainingValues_ -= consumed;
  }
}

void RleBpDecoder::readHeader(uint64_t requestedValues) {
  bitOffset_ = 0;
  auto maxVarIntLen = std::min<uint64_t>(
      (uint64_t)folly::kMaxVarintLength64, bufferEnd_ - bufferStart_);
  folly::ByteRange headerRange(
      reinterpret_cast<const unsigned char*>(bufferStart_),
      reinterpret_cast<const unsigned char*>(bufferStart_ + maxVarIntLen));
  // decodeVarint() would advance headerRange's begin pointer.
  uint64_t indicator;
  try {
    indicator = folly::decodeVarint(headerRange);
  } catch (const std::exception& error) {
    BOLT_FAIL(
        "Parquet RLE/bit-pack decoder header failure: role={} "
        "row_group={} column={} page={} buffer_offset={} "
        "buffer_remaining={} requested_values={} remaining_run_values={} "
        "bit_width={} cause={}",
        role_,
        rowGroupOrdinal_,
        columnOrdinal_,
        pageOrdinal_,
        bufferStart_ - bufferBegin_,
        bufferEnd_ - bufferStart_,
        requestedValues,
        remainingValues_,
        bitWidth_,
        error.what());
  }
  // Advance bufferStart_ to the position after where the varint is read
  bufferStart_ = reinterpret_cast<const char*>(headerRange.begin());

  // 0 in low bit means repeating.
  repeating_ = (indicator & 1) == 0;
  uint32_t count = indicator >> 1;
  if (repeating_) {
    remainingValues_ = count;
    // Do not load past buffer end. Reports error in valgrind and could in
    // principle run into unmapped addresses.
    if (bufferStart_ <= lastSafeWord_) {
      value_ = *reinterpret_cast<const int64_t*>(bufferStart_) & bitMask_;
    } else {
      value_ = bits::loadPartialWord(
          reinterpret_cast<const uint8_t*>(bufferStart_), byteWidth_);
    }
    bufferStart_ += byteWidth_;
  } else {
    BOLT_CHECK_LT(0, count);
    BOLT_CHECK_LT(count, std::numeric_limits<int32_t>::max() / 8);
    remainingValues_ = count * 8;
  }
}

} // namespace bytedance::bolt::parquet
