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

#pragma once

#include "bolt/common/memory/bm/compress/CompressionConfig.h"

#include <array>
#include <cstdint>
#include <span>

namespace bytedance::bolt::memory::bm::compress {

constexpr uint32_t kSpillRecordMagic = 0x424d5350; // BMSP
constexpr uint16_t kSpillRecordVersion = 1;

struct SpillRecordHeader {
  uint32_t magic{kSpillRecordMagic};
  uint16_t version{kSpillRecordVersion};
  uint16_t headerSize{sizeof(SpillRecordHeader)};
  uint32_t compressionKind{static_cast<uint32_t>(CompressionKind::kNone)};
  uint32_t reserved{0};
  uint64_t rawSize{0};
  uint64_t storedSize{0};
};

std::array<char, sizeof(SpillRecordHeader)> EncodeSpillRecordHeader(
    const SpillRecordHeader& header);

SpillRecordHeader DecodeSpillRecordHeader(
    const char* data,
    size_t size,
    uint64_t expectedRawSize);

} // namespace bytedance::bolt::memory::bm::compress
