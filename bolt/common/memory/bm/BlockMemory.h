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
