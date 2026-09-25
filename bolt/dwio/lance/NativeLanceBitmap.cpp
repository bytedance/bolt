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

#include "bolt/dwio/lance/NativeLanceBitmap.h"

#include <algorithm>
#include <cstring>

#include "bolt/common/base/BitUtil.h"

namespace bytedance::bolt::lance::reader {
namespace {

uint64_t lowMask(uint64_t width) {
  return width == 64 ? ~uint64_t{0} : (uint64_t{1} << width) - 1;
}

uint64_t loadBits(const uint8_t* source, uint64_t bitOffset, uint64_t width) {
  const auto* bytes = source + bitOffset / 8;
  const auto shift = bitOffset % 8;
  const auto numBytes = (shift + width + 7) / 8;
  uint64_t low = 0;
  std::memcpy(&low, bytes, std::min<uint64_t>(numBytes, sizeof(low)));
  auto value = low >> shift;
  if (numBytes > sizeof(low)) {
    value |= static_cast<uint64_t>(bytes[sizeof(low)]) << (64 - shift);
  }
  return value & lowMask(width);
}

} // namespace

void copyLanceBitmapScalar(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t* target,
    uint64_t targetBitOffset) {
  for (uint64_t i = 0; i < count; ++i) {
    bits::setBit(
        target,
        targetBitOffset + i,
        (source[(sourceBitOffset + i) / 8] &
         (1U << ((sourceBitOffset + i) % 8))) != 0);
  }
}

void copyLanceBitmap(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t* target,
    uint64_t targetBitOffset) {
  if (count == 0) {
    return;
  }
  const auto targetShift = targetBitOffset % 64;
  if (targetShift != 0) {
    const auto width = std::min<uint64_t>(count, 64 - targetShift);
    const auto mask = lowMask(width) << targetShift;
    auto& targetWord = target[targetBitOffset / 64];
    targetWord = (targetWord & ~mask) |
        ((loadBits(source, sourceBitOffset, width) << targetShift) & mask);
    sourceBitOffset += width;
    targetBitOffset += width;
    count -= width;
  }
  while (count >= 64) {
    target[targetBitOffset / 64] = loadBits(source, sourceBitOffset, 64);
    sourceBitOffset += 64;
    targetBitOffset += 64;
    count -= 64;
  }
  if (count != 0) {
    const auto mask = lowMask(count);
    auto& targetWord = target[targetBitOffset / 64];
    targetWord = (targetWord & ~mask) |
        (loadBits(source, sourceBitOffset, count) & mask);
  }
}

bool lanceBitmapIsAllSet(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count) {
  while (count >= 64) {
    if (loadBits(source, sourceBitOffset, 64) != ~uint64_t{0}) {
      return false;
    }
    sourceBitOffset += 64;
    count -= 64;
  }
  return count == 0 ||
      loadBits(source, sourceBitOffset, count) == lowMask(count);
}

} // namespace bytedance::bolt::lance::reader
