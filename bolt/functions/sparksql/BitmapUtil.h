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
#include <cstring>

namespace bytedance::bolt::functions::sparksql {

// Spark's BitmapExpressionUtils.NUM_BYTES (4 KiB, 32768 bits).
inline constexpr int32_t kBitmapNumBytes = 4096;
inline constexpr int32_t kBitmapNumBits = kBitmapNumBytes * 8;

/// Popcount over arbitrary byte buffer. memcpy avoids UB from misaligned
/// data (the input may not be 8-byte aligned). Returns 0 for null/empty.
inline int64_t bitmapPopcount(const char* data, size_t size) {
  if (data == nullptr || size == 0) {
    return 0;
  }
  int64_t count = 0;
  const size_t numFullWords = size / sizeof(uint64_t);
  for (size_t i = 0; i < numFullWords; ++i) {
    uint64_t word;
    memcpy(&word, data + (i * sizeof(uint64_t)), sizeof(uint64_t));
    count += __builtin_popcountll(word);
  }
  // Tail: remaining bytes that don't fill a full 64-bit word.
  const size_t tailStart = numFullWords * sizeof(uint64_t);
  const auto* tail = reinterpret_cast<const uint8_t*>(data);
  for (size_t i = tailStart; i < size; ++i) {
    count += __builtin_popcount(tail[i]);
  }
  return count;
}

} // namespace bytedance::bolt::functions::sparksql
