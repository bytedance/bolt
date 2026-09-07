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

#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/file/ManagedFileSegment.h"

#include <cstdint>
#include <memory>
#include <optional>

namespace bytedance::bolt::memory::bm {

class BufferManager;

enum class BlockMemoryState : uint8_t {
  kInMemory,
  kSpilled,
  kPrefetching,
  kSpilling,
};

struct BlockMemory {
  BlockMemory(uint64_t id, size_t size, MemoryTag tag);
  ~BlockMemory() noexcept;

  uint64_t id;
  size_t size;
  MemoryTag tag;
  std::weak_ptr<BufferManager> owner;
  BlockMemoryState state{BlockMemoryState::kInMemory};
  uint32_t pinCount{0};
  // True if resident payload may be newer than the spill backing. Newly
  // allocated blocks are dirty until their first successful spill. Blocks read
  // from spill backing are clean until a mutable caller explicitly marks them.
  bool dirty{true};
  // Generation token for lazy eviction queue entries. It changes whenever an
  // older queued entry should no longer represent this block's evictability.
  uint64_t evictionSequence{0};
  std::optional<IoBuffer> payload;
  std::optional<ManagedFileSegment> segment;
  std::optional<SpillReadFuture> prefetchFuture;
};

} // namespace bytedance::bolt::memory::bm
