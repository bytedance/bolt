#pragma once

#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/common/memory/bm/OwnedFileExtent.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <cstdint>
#include <future>
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
  std::optional<OwnedFileExtent> extent;
  std::optional<std::future<IoResult>> prefetchFuture;
};

class BlockHandle {
 public:
  explicit BlockHandle(std::shared_ptr<BlockMemory> memory);

  uint64_t id() const;
  size_t size() const;
  MemoryTag tag() const;

 private:
  std::shared_ptr<BlockMemory> memory_;

  friend class BufferHandle;
  friend class BufferManager;
};

std::shared_ptr<BlockHandle> testingCreateBlockHandle(
    size_t size,
    MemoryTag tag);

} // namespace bytedance::bolt::memory::bm
