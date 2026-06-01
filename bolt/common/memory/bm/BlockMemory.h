#pragma once

#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/common/memory/bm/OwnedFileSegment.h"
#include "bolt/common/memory/bm/SpillStore.h"

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
  uint64_t evictionSequence{0};
  std::optional<IoBuffer> payload;
  std::optional<OwnedFileSegment> segment;
  std::optional<SpillReadFuture> prefetchFuture;
};

} // namespace bytedance::bolt::memory::bm
