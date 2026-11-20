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
#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/common/memory/bm/BlockDescriptor.h"
#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/common/memory/bm/BufferManagerStats.h"
#include "bolt/common/memory/bm/SpillStoreConfig.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

struct BlockMemory;
class BufferManagerStatsCollector;
class EvictionQueue;
class SpillWriteDriver;
class SpillStore;

struct SpillCompressionOptions {
  std::string kind{"zstd"};
  std::optional<int32_t> level;
};

struct BufferManagerConfig {
  std::string poolName;
  SpillStoreConfig spillStoreConfig;
  IoPriority readPriority{IoPriority::High};
  IoPriority writePriority{IoPriority::Medium};
  IoPriority prefetchPriority{IoPriority::Low};
  uint32_t maxReclaimWriteInflight{128};

  /// Handles execution state changes while the BufferManager pool initiates
  /// memory arbitration. Reclaiming BufferManager blocks remains the
  /// responsibility of BufferManagerReclaimer.
  std::shared_ptr<memory::MemoryReclaimer> arbitrationReclaimer;

  /// Optional named codec settings, resolved and validated by Create before
  /// allocating resources. When absent, the typed spillStoreConfig compression
  /// options are used unchanged. When present, kind and the optional selected
  /// codec level override those options; other codec options are preserved.
  std::optional<SpillCompressionOptions> spillCompression;
};

class BufferManager : public std::enable_shared_from_this<BufferManager> {
 public:
  static std::shared_ptr<BufferManager> Create(
      MemoryPool& parent,
      BufferManagerConfig config);

  ~BufferManager();

  /// Creates a dedicated allocation leaf under this manager's aggregate.
  /// The caller supplies a unique name for each operator instance.
  std::shared_ptr<MemoryPool> addOperatorPool(const std::string& name);
  /// Lazily creates the single allocation leaf used by the legacy APIs.
  std::shared_ptr<MemoryPool> defaultAllocationPool();

  BufferHandle Allocate(size_t size, MemoryTag tag);
  BufferHandle Allocate(
      size_t size,
      MemoryTag tag,
      const std::shared_ptr<MemoryPool>& allocationPool);
  BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);

  std::vector<BufferHandle>
  BatchAllocate(size_t count, size_t size, MemoryTag tag);
  std::vector<BufferHandle> BatchPin(
      std::span<const std::shared_ptr<BlockHandle>> blocks);
  void Prefetch(std::span<const std::shared_ptr<BlockHandle>> blocks);
  void SpillBlocks(std::span<const std::shared_ptr<BlockHandle>> blocks);
  bool HasSpillBacking(const std::shared_ptr<BlockHandle>& block) const;
  bool IsDirty(const std::shared_ptr<BlockHandle>& block) const;
  /// True only after reclamation completes; false while reading or spilling.
  /// Like other block state queries, access must be serialized with
  /// reclamation.
  bool IsSpilled(const std::shared_ptr<BlockHandle>& block) const;
  /// Call before modifying a pinned block read from spill. Clean blocks can
  /// reuse their existing backing during reclamation.
  void MarkDirty(const std::shared_ptr<BlockHandle>& block);
  void SetBlockDescriptor(
      const std::shared_ptr<BlockHandle>& block,
      std::shared_ptr<const BlockDescriptor> descriptor);
  uint64_t DiscardCleanResidentBlocks(
      std::span<const std::shared_ptr<BlockHandle>> blocks);

  bool MaybeReserve(size_t size);
  /// Releases unused reservation from every allocation leaf. Operators with
  /// a dedicated leaf release that leaf directly instead.
  void ReleaseUnusedReservation();
  uint64_t Reclaim(uint64_t targetBytes);
  uint64_t reclaimableBytes() const;
  BufferManagerStats stats() const;
  std::vector<BufferManagerTagStats> tagStats() const;
  std::string debugString() const;

 private:
  explicit BufferManager(BufferManagerConfig config);

  void Initialize(MemoryPool& parent);
  void ValidateAllocationPool(
      const std::shared_ptr<MemoryPool>& allocationPool) const;
  BufferHandle AllocateOne(
      size_t size,
      MemoryTag tag,
      const std::shared_ptr<MemoryPool>& allocationPool);
  void Unpin(const std::shared_ptr<BlockHandle>& block) noexcept;
  BufferHandle PinInMemory(const std::shared_ptr<BlockHandle>& block);
  BufferHandle PinSpilled(const std::shared_ptr<BlockHandle>& block);
  BufferHandle PinPrefetching(const std::shared_ptr<BlockHandle>& block);
  void SubmitRead(
      const std::shared_ptr<BlockHandle>& block,
      IoPriority priority);
  SpillWriteDriver MakeSpillWriteDriver();
  BufferHandle MakeHandle(const std::shared_ptr<BlockHandle>& block);
  void OnBlockMemoryDestroy(const BlockMemory& memory) noexcept;

  std::shared_ptr<MemoryPool> aggregatePool_;
  std::shared_ptr<MemoryPool> defaultAllocationPool_;
  std::vector<std::shared_ptr<MemoryPool>> allocationPools_;
  std::unique_ptr<SpillStore> spillStore_;
  BufferManagerConfig config_;
  uint64_t nextBlockId_{1};
  std::unique_ptr<BufferManagerStatsCollector> accounting_;
  std::unique_ptr<EvictionQueue> evictionQueue_;

  friend class BufferHandle;
  friend struct BlockMemory;
};

} // namespace bytedance::bolt::memory::bm
