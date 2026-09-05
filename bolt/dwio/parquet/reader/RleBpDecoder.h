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

#include "bolt/common/base/BitUtil.h"
#include "bolt/dwio/common/BitPackDecoder.h"
#include "bolt/dwio/common/DecoderUtil.h"
namespace bytedance::bolt::parquet {

class RleBpDecoder {
 public:
  RleBpDecoder(
      const char* FOLLY_NONNULL start,
      const char* FOLLY_NONNULL end,
      uint8_t bitWidth,
      const char* FOLLY_NONNULL role = "unspecified",
      int32_t rowGroupOrdinal = -1,
      int32_t columnOrdinal = -1,
      int32_t pageOrdinal = -1,
      int64_t expectedValues = -1)
      : bufferStart_(start),
        bufferBegin_(start),
        bufferEnd_(end),
        bitWidth_(bitWidth),
        byteWidth_(bits::roundUp(bitWidth, 8) / 8),
        bitMask_(bits::lowMask(bitWidth)),
        lastSafeWord_(
            end - start >= sizeof(uint64_t) ? end - sizeof(uint64_t) : start),
        role_(role),
        rowGroupOrdinal_(rowGroupOrdinal),
        columnOrdinal_(columnOrdinal),
        pageOrdinal_(pageOrdinal),
        expectedValues_(expectedValues) {}

  void skip(uint64_t numValues);

  /// Decode @param numValues number of values and copy the decoded values into
  /// @param outputBuffer
  template <typename T>
  void next(T* FOLLY_NONNULL& outputBuffer, uint64_t numValues) {
    while (numValues > 0) {
      if (numRemainingUnpackedValues_ > 0) {
        auto numValuesToRead =
            std::min<uint64_t>(numValues, numRemainingUnpackedValues_);
        copyRemainingUnpackedValues(outputBuffer, numValuesToRead);

        numValues -= numValuesToRead;
      } else {
        if (remainingValues_ == 0) {
          readHeader(numValues);
        }

        auto numValuesToRead = std::min<uint32_t>(numValues, remainingValues_);
        if (repeating_) {
          std::fill(outputBuffer, outputBuffer + numValuesToRead, value_);
          outputBuffer += numValuesToRead;
          remainingValues_ -= numValuesToRead;
        } else {
          remainingUnpackedValuesOffset_ = 0;
          // The parquet standard requires the bit packed values are always a
          // multiple of 8. So we read a multiple of 8 values each time
          dwio::common::unpack<T>(
              reinterpret_cast<const uint8_t*&>(bufferStart_),
              bufferEnd_ - bufferStart_,
              numValuesToRead & 0xfffffff8,
              bitWidth_,
              reinterpret_cast<T * FOLLY_NONNULL&>(outputBuffer));
          remainingValues_ -= (numValuesToRead & 0xfffffff8);

          // Unpack the next 8 values to remainingUnpackedValues_ if necessary
          if ((numValuesToRead & 7) != 0) {
            T* output = reinterpret_cast<T*>(remainingUnpackedValues_);
            dwio::common::unpack<T>(
                reinterpret_cast<const uint8_t*&>(bufferStart_),
                bufferEnd_ - bufferStart_,
                8,
                bitWidth_,
                output);
            numRemainingUnpackedValues_ = 8;
            remainingUnpackedValuesOffset_ = 0;

            copyRemainingUnpackedValues(outputBuffer, numValuesToRead & 7);
            remainingValues_ -= 8;
          }
        }

        numValues -= numValuesToRead;
      }
    }
  }

  /// Copies 'numValues' bits from the encoding into 'buffer',
  /// little-endian. If 'allOnes' is non-nullptr, this function may
  /// check if all the bits are ones, as in a RLE run of all ones and
  /// not copy them into 'buffer' but instead may set '*allOnes' to
  /// true. If allOnes is non-nullptr and not all bits are ones, then
  /// '*allOnes' is set to false and the bits are copied to 'buffer'.
  void readBits(
      int32_t numValues,
      uint64_t* FOLLY_NONNULL outputBuffer,
      bool* FOLLY_NULLABLE allOnes = nullptr);

 protected:
  void readHeader(uint64_t requestedValues);

  template <typename T>
  inline void copyRemainingUnpackedValues(
      T* FOLLY_NONNULL& outputBuffer,
      int8_t numValues) {
    BOLT_CHECK_LE(numValues, numRemainingUnpackedValues_);

    std::memcpy(
        outputBuffer,
        reinterpret_cast<T*>(remainingUnpackedValues_) +
            remainingUnpackedValuesOffset_,
        numValues);

    outputBuffer += numValues;
    numRemainingUnpackedValues_ -= numValues;
    remainingUnpackedValuesOffset_ += numValues;
  }

  const char* FOLLY_NULLABLE bufferStart_;
  const char* FOLLY_NULLABLE const bufferBegin_;
  const char* FOLLY_NULLABLE bufferEnd_;
  const int8_t bitWidth_;
  const int8_t byteWidth_;
  const uint64_t bitMask_;
  const char* FOLLY_NONNULL const lastSafeWord_;
  const char* FOLLY_NONNULL const role_;
  const int32_t rowGroupOrdinal_;
  const int32_t columnOrdinal_;
  const int32_t pageOrdinal_;
  const int64_t expectedValues_;
  uint64_t consumedValues_{0};
  uint64_t remainingValues_{0};
  int64_t value_;
  int8_t bitOffset_{0};
  bool repeating_;

  uint64_t remainingUnpackedValues_[8];
  int8_t remainingUnpackedValuesOffset_{0};
  int8_t numRemainingUnpackedValues_{0};
};

} // namespace bytedance::bolt::parquet
