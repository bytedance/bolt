#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/common/memory/bm/BufferManagerObservability.h"
#include "bolt/common/memory/bm/file/FileBlockAllocator.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

#include <array>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

class EvictionQueue;
class SpillStore;

struct BufferManagerConfig {
  std::string poolName;
  FileBlockAllocatorConfig fileAllocatorConfig;
  IoPriority readPriority{IoPriority::High};
  IoPriority writePriority{IoPriority::Medium};
  IoPriority prefetchPriority{IoPriority::Low};
};

class BufferManager : public std::enable_shared_from_this<BufferManager> {
 public:
  static std::shared_ptr<BufferManager> Create(
      MemoryPool& parent,
      BufferManagerConfig config);
  ~BufferManager();

  BufferHandle Allocate(
      size_t size,
      MemoryTag tag,
      std::shared_ptr<BlockHandle>* block = nullptr);
  bool MaybeReserve(size_t size);
  void ReleaseUnusedReservation();
  BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);
  std::vector<BufferHandle> BatchPin(
      std::span<const std::shared_ptr<BlockHandle>> blocks);
  void Prefetch(std::span<const std::shared_ptr<BlockHandle>> blocks) noexcept;

  uint64_t Reclaim(uint64_t targetBytes);
  uint64_t reclaimableBytes() const;
  BufferManagerStats stats() const;
  std::vector<BufferManagerTagStats> tagStats() const;
  std::string debugString() const;

 private:
  explicit BufferManager(BufferManagerConfig config);

  void Initialize(MemoryPool& parent);
  void Unpin(const std::shared_ptr<BlockHandle>& block) noexcept;
  BufferHandle PinInMemory(const std::shared_ptr<BlockHandle>& block);
  BufferHandle PinSpilled(const std::shared_ptr<BlockHandle>& block);
  BufferHandle PinPrefetching(const std::shared_ptr<BlockHandle>& block);
  void SubmitRead(
      const std::shared_ptr<BlockHandle>& block,
      IoPriority priority);
  uint64_t SpillBlock(const std::shared_ptr<BlockMemory>& memory);
  BufferHandle MakeHandle(const std::shared_ptr<BlockHandle>& block);
  BufferManagerTagStats& MutableTagStats(MemoryTag tag);
  void OnBlockMemoryDestroy(const BlockMemory& memory) noexcept;

  std::shared_ptr<FileBlockAllocator> allocator_;
  std::shared_ptr<MemoryPool> pool_;
  std::unique_ptr<SpillStore> spillStore_;
  BufferManagerConfig config_;
  uint64_t nextBlockId_{1};
  BufferManagerStats stats_;
  std::array<BufferManagerTagStats, kMemoryTagCount> tagStats_;
  std::unique_ptr<EvictionQueue> evictionQueue_;

  friend class BufferHandle;
  friend struct BlockMemory;
};

} // namespace bytedance::bolt::memory::bm
