#pragma once

#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/bm/BmBlockReclaimPolicy.h"

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
  using CanReclaimFn = std::function<bool(BlockId blockId)>;

  BmPressureAwareBlockArena(
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      std::unique_ptr<BmBlockReclaimPolicy> reclaimPolicy = nullptr);
  ~BmPressureAwareBlockArena();

  BlockId allocateReservedBlock(uint32_t capacity);
  char* activeData(BlockId blockId);
  const char* pinnedData(
      BlockId blockId,
      const CanReclaimFn& canReclaim,
      const char* failureMessage =
          "BmPressureAwareBlockArena cannot reserve memory to pin a block");
  void pinBlocks(
      std::span<const BlockId> blockIds,
      const CanReclaimFn& canReclaim);

  BmBlockState& block(BlockId blockId);
  const BmBlockState& block(BlockId blockId) const;

  uint64_t allocatedBytes() const;
  uint64_t usedBytes() const;
  uint32_t size() const;
  bool empty() const;

  uint32_t spillReclaimableBlocks(
      uint64_t targetBytes,
      const CanReclaimFn& canReclaim);

  void clear();

 private:
  const char* tryPinBlock(BlockId blockId);
  void ensureCapacityForPinnedRead(
      uint32_t capacity,
      const CanReclaimFn& canReclaim,
      const char* failureMessage);
  std::vector<BmBlockReclaimCandidate> reclaimCandidates(
      const CanReclaimFn& canReclaim,
      bool pinnedOnly) const;
  std::vector<BlockId> selectVictims(
      uint64_t targetBytes,
      const CanReclaimFn& canReclaim,
      bool pinnedOnly) const;
  uint64_t releasePinnedVictims(std::span<const BlockId> victims);
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> unpinnedVictimBlocks(
      std::span<const BlockId> victims) const;
  void touch(BmBlockState& block);

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  memory::bm::MemoryTag tag_;
  std::unique_ptr<BmBlockReclaimPolicy> reclaimPolicy_;
  std::vector<BmBlockState> blocks_;
  uint64_t accessCounter_{0};
};

} // namespace bytedance::bolt::exec
