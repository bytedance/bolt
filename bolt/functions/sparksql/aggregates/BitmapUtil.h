/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#pragma once

#include <cstdint>

#include <folly/Portability.h>
#include <xsimd/xsimd.hpp>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

// Matching Spark's BitmapExpressionUtils.NUM_BYTES (4 KiB, 32768 bits).
inline constexpr int32_t kBitmapNumBytes = 4096;
inline constexpr int32_t kBitmapNumBits = kBitmapNumBytes * 8;

// Inline 4096-byte bitmap accumulator. Trivially constructible/destructible so
// Bolt's RowContainer group reuse is safe. uint8_t ensures portable unsigned
// bitwise semantics.
struct BitmapAccumulator {
  uint8_t bitmap_[kBitmapNumBytes] = {};

  FOLLY_ALWAYS_INLINE void setPosition(int64_t position) {
    BOLT_USER_CHECK(
        position >= 0 && position < kBitmapNumBits,
        "Invalid bitmap position: {} (valid range: [0, {}))",
        position,
        static_cast<int64_t>(kBitmapNumBits));
    int32_t byteIdx = static_cast<int32_t>(position / 8);
    int32_t bitIdx = static_cast<int32_t>(position % 8);
    bitmap_[byteIdx] |= static_cast<uint8_t>(1 << bitIdx);
  }

  // Byte-wise bitwise OR. xsimd vectorizes across 16 (NEON) or 32 (AVX2)
  // bytes per iteration.
  void mergeWith(const char* other) {
    const auto* otherBytes = reinterpret_cast<const uint8_t*>(other);
    using Batch = xsimd::batch<uint8_t>;
    static constexpr int32_t kBatchSize = Batch::size;
    int32_t i = 0;
    for (; i + kBatchSize <= kBitmapNumBytes; i += kBatchSize) {
      auto a = Batch::load_unaligned(bitmap_ + i);
      auto b = Batch::load_unaligned(otherBytes + i);
      (a | b).store_unaligned(bitmap_ + i);
    }
    for (; i < kBitmapNumBytes; ++i) {
      bitmap_[i] |= otherBytes[i];
    }
  }
};

static_assert(
    sizeof(BitmapAccumulator) == kBitmapNumBytes,
    "BitmapAccumulator size must be exactly 4096 bytes");

} // namespace bytedance::bolt::functions::aggregate::sparksql
