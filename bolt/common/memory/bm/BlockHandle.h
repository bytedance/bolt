#pragma once

#include "bolt/common/memory/bm/MemoryTag.h"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace bytedance::bolt::memory::bm {

class BufferHandle;
class BufferManager;
struct BlockMemory;

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
