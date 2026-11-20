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

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/BlockDescriptor.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <cstdint>
#include <memory>
#include <span>

namespace bytedance::bolt::memory::bm::compress {

struct CompressionRecordResult {
  IoBuffer record;
  uint64_t rawSize{0};
  uint64_t physicalSize{0};
  CompressionKind storedKind{CompressionKind::kNone};
  uint64_t compressionTimeUs{0};
  bool compressed{false};
};

class CompressionManager {
 public:
  explicit CompressionManager(CompressionConfig config);
  ~CompressionManager();

  CompressionManager(const CompressionManager&) = delete;
  CompressionManager& operator=(const CompressionManager&) = delete;

  CompressionRecordResult BuildSpillRecord(
      std::span<const char> payload,
      const BlockDescriptor* descriptor = nullptr);

  IoBuffer DecodeSpillRecord(
      std::span<const char> record,
      uint64_t expectedRawSize,
      MemoryPool* outputPool,
      uint64_t* decompressionTimeUs,
      uint64_t blockId = 0,
      CompressionKind* storedKindOut = nullptr);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace bytedance::bolt::memory::bm::compress
