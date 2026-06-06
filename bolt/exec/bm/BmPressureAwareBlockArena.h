#pragma once

#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/common/memory/bm/BufferManager.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace bytedance::bolt::exec {

struct BmBlockState {
  std::shared_ptr<memory::bm::BlockHandle> block;
  std::optional<memory::bm::BufferHandle> pinnedHandle;
  char* data{nullptr};
  uint32_t capacity{0};
  uint32_t usedBytes{0};
  uint32_t liveRows{0};
  uint64_t lastAccess{0};
};

class BmPressureAwareBlockArena {
 public:
  using CanReclaimFn = std::function<bool(uint32_t blockId)>;

  BmPressureAwareBlockArena(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag);
  ~BmPressureAwareBlockArena();

  bool tryReserve(uint64_t bytes);
  std::optional<uint32_t> tryAllocateBlock(uint32_t capacity);
  uint32_t allocateBlock(
      uint32_t capacity,
      const CanReclaimFn& canReclaim,
      const char* failureMessage =
          "BmPressureAwareBlockArena cannot allocate a new block");
  char* activeData(uint32_t blockId);
  const char* tryPinnedData(uint32_t blockId);
  const char* pinnedData(
      uint32_t blockId,
      const CanReclaimFn& canReclaim,
      const char* failureMessage =
          "BmPressureAwareBlockArena cannot reserve memory to pin a block");
  void pinBlocks(
      std::span<const uint32_t> blockIds,
      const CanReclaimFn& canReclaim);

  BmBlockState& block(uint32_t blockId);
  const BmBlockState& block(uint32_t blockId) const;

  uint64_t allocatedBytes() const;
  uint64_t usedBytes() const;
  uint32_t size() const;
  bool empty() const;

  uint64_t makeBlocksReclaimable(
      uint64_t targetBytes,
      const CanReclaimFn& canReclaim);
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> reclaimableBlocks(
      const CanReclaimFn& canReclaim) const;
  uint32_t spillReclaimableBlocks(
      uint64_t targetBytes,
      const CanReclaimFn& canReclaim);

  void clear();

 private:
  void ensureMemoryForBlock(
      uint32_t capacity,
      const CanReclaimFn& canReclaim,
      const char* failureMessage);
  void touch(BmBlockState& block);

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  memory::bm::MemoryTag tag_;
  std::vector<BmBlockState> blocks_;
  uint64_t accessCounter_{0};
};

} // namespace bytedance::bolt::exec
