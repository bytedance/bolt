#pragma once

#include "bolt/common/memory/bm/MemoryTag.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>

namespace bytedance::bolt::memory::bm {

class BufferHandle;
class BufferManager;
struct BlockMemory;
class BlockHandle;

using SpillCandidateProvider =
    std::function<std::shared_ptr<BlockMemory>()>;

SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
    std::span<const std::shared_ptr<BlockHandle>> blocks);

class BlockHandle {
 public:
  explicit BlockHandle(std::shared_ptr<BlockMemory> memory);

  uint64_t id() const;
  size_t size() const;
  MemoryTag tag() const;

 private:
  std::shared_ptr<BlockMemory> memory_;

  friend SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
      std::span<const std::shared_ptr<BlockHandle>> blocks);
  friend class BufferHandle;
  friend class BufferManager;
};

std::shared_ptr<BlockHandle> testingCreateBlockHandle(
    size_t size,
    MemoryTag tag);

} // namespace bytedance::bolt::memory::bm
