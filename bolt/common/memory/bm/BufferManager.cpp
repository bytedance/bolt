#include "bolt/common/memory/bm/BufferManager.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/BufferManagerReclaimer.h"
#include "bolt/common/memory/bm/BufferManagerStats.h"
#include "bolt/common/memory/bm/EvictionQueue.h"
#include "bolt/common/memory/bm/ReclaimWriteWindow.h"
#include "bolt/common/memory/bm/SpillStore.h"

#include <glog/logging.h>

#include <utility>

namespace bytedance::bolt::memory::bm {
std::shared_ptr<BufferManager> BufferManager::Create(
    MemoryPool& parent,
    BufferManagerConfig config) {
  BOLT_CHECK(!config.poolName.empty());
  auto manager =
      std::shared_ptr<BufferManager>(new BufferManager(std::move(config)));
  manager->Initialize(parent);
  return manager;
}

BufferManager::BufferManager(BufferManagerConfig config)
    : config_(std::move(config)),
      accounting_(std::make_unique<BufferManagerStatsCollector>()),
      evictionQueue_(std::make_unique<EvictionQueue>()) {}

BufferManager::~BufferManager() = default;

void BufferManager::Initialize(MemoryPool& parent) {
  // v1 keeps the default thread-safe leaf pool. For strictly thread-local BM
  // instances this can be optimized to addLeafChild(name, false) later.
  pool_ = parent.addLeafChild(config_.poolName);
  BOLT_CHECK_NOT_NULL(pool_);
  spillStore_ =
      std::make_unique<SpillStore>(config_.spillStoreConfig, pool_.get());
  pool_->setReclaimer(
      std::make_unique<BufferManagerReclaimer>(weak_from_this()));
}

BufferHandle BufferManager::Allocate(size_t size, MemoryTag tag) {
  return AllocateOne(size, tag);
}

std::vector<BufferHandle>
BufferManager::BatchAllocate(size_t count, size_t size, MemoryTag tag) {
  std::vector<BufferHandle> handles;
  handles.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    handles.push_back(AllocateOne(size, tag));
  }
  return handles;
}

BufferHandle BufferManager::AllocateOne(size_t size, MemoryTag tag) {
  BOLT_CHECK_GT(size, 0);
  auto memory = std::make_shared<BlockMemory>(nextBlockId_++, size, tag);
  memory->owner = weak_from_this();
  memory->payload = IoBuffer::allocateFromPool(pool_.get(), size);
  memory->pinCount = 1;
  accounting_->RecordAllocate(*memory);
  auto handle = std::make_shared<BlockHandle>(std::move(memory));
  return MakeHandle(handle);
}

bool BufferManager::MaybeReserve(size_t size) {
  VLOG(1) << "BM MaybeReserve begin"
          << " size=" << size << " pool_used=" << pool_->usedBytes()
          << " pool_current=" << pool_->currentBytes()
          << " pool_reserved=" << pool_->reservedBytes()
          << " pool_available_reservation=" << pool_->availableReservation()
          << " pool_releasable_reservation=" << pool_->releasableReservation()
          << " bm=" << debugString();
  const auto ok = pool_->maybeReserve(size);
  VLOG(1) << "BM MaybeReserve end"
          << " ok=" << ok << " size=" << size
          << " pool_used=" << pool_->usedBytes()
          << " pool_current=" << pool_->currentBytes()
          << " pool_reserved=" << pool_->reservedBytes()
          << " pool_available_reservation=" << pool_->availableReservation()
          << " pool_releasable_reservation=" << pool_->releasableReservation()
          << " bm=" << debugString();
  return ok;
}

void BufferManager::ReleaseUnusedReservation() {
  VLOG(1) << "BM ReleaseUnusedReservation begin"
          << " pool_used=" << pool_->usedBytes()
          << " pool_current=" << pool_->currentBytes()
          << " pool_reserved=" << pool_->reservedBytes()
          << " pool_available_reservation=" << pool_->availableReservation()
          << " pool_releasable_reservation=" << pool_->releasableReservation()
          << " bm=" << debugString();
  pool_->release();
  VLOG(1) << "BM ReleaseUnusedReservation end"
          << " pool_used=" << pool_->usedBytes()
          << " pool_current=" << pool_->currentBytes()
          << " pool_reserved=" << pool_->reservedBytes()
          << " pool_available_reservation=" << pool_->availableReservation()
          << " pool_releasable_reservation=" << pool_->releasableReservation()
          << " bm=" << debugString();
}

BufferHandle BufferManager::Pin(const std::shared_ptr<BlockHandle>& block) {
  BOLT_CHECK_NOT_NULL(block);
  accounting_->RecordPinRequest(block->tag());
  auto& memory = *block->memory_;
  switch (memory.state) {
    case BlockMemoryState::kInMemory:
      return PinInMemory(block);
    case BlockMemoryState::kSpilled:
      return PinSpilled(block);
    case BlockMemoryState::kPrefetching:
      return PinPrefetching(block);
    case BlockMemoryState::kSpilling:
      BOLT_FAIL(
          "BM cannot pin a block while it is spilling, block_id={}", memory.id);
  }
  BOLT_FAIL("BM encountered unknown block state, block_id={}", memory.id);
}

std::vector<BufferHandle> BufferManager::BatchPin(
    std::span<const std::shared_ptr<BlockHandle>> blocks) {
  accounting_->RecordBatchPin();
  for (const auto& block : blocks) {
    BOLT_CHECK_NOT_NULL(block);
    auto& memory = *block->memory_;
    if (memory.state == BlockMemoryState::kSpilled) {
      SubmitRead(block, config_.readPriority);
    }
  }

  std::vector<BufferHandle> handles;
  handles.reserve(blocks.size());
  for (const auto& block : blocks) {
    handles.push_back(Pin(block));
  }
  return handles;
}

void BufferManager::Prefetch(
    std::span<const std::shared_ptr<BlockHandle>> blocks) {
  accounting_->RecordPrefetch();
  for (const auto& block : blocks) {
    BOLT_CHECK_NOT_NULL(block);
    auto& memory = *block->memory_;
    if (memory.state == BlockMemoryState::kSpilled) {
      SubmitRead(block, config_.prefetchPriority);
    }
  }
}

uint64_t BufferManager::Reclaim(uint64_t targetBytes) {
  accounting_->RecordReclaim();
  VLOG(1) << "BM Reclaim begin"
          << " target_bytes=" << targetBytes
          << " pool_used=" << pool_->usedBytes()
          << " pool_current=" << pool_->currentBytes()
          << " pool_reserved=" << pool_->reservedBytes()
          << " pool_available_reservation=" << pool_->availableReservation()
          << " pool_releasable_reservation=" << pool_->releasableReservation()
          << " bm=" << debugString();
  BOLT_CHECK_GT(config_.maxReclaimWriteInflight, 0);

  ReclaimWriteWindow writeWindow{
      config_.maxReclaimWriteInflight,
      config_.writePriority,
      [this](IoBuffer& payload, size_t rawSize, IoPriority priority) {
        return spillStore_->SubmitWriteBlock(payload, rawSize, priority);
      },
      *accounting_};
  uint64_t submitted = 0;
  uint64_t reclaimed = 0;
  bool noMoreEvictable = false;

  auto submitMore = [&]() {
    while (writeWindow.canSubmit() &&
           (targetBytes == 0 || submitted < targetBytes) && !noMoreEvictable) {
      auto memory = evictionQueue_->PopEvictable();
      if (!memory) {
        noMoreEvictable = true;
        VLOG(1) << "BM Reclaim no evictable block"
                << " target_bytes=" << targetBytes
                << " submitted_bytes=" << submitted
                << " reclaimed_bytes=" << reclaimed << " bm=" << debugString();
        break;
      }

      accounting_->RecordReclaimAttemptedBlock();
      VLOG(1) << "BM Reclaim spill candidate"
              << " block_id=" << memory->id << " tag=" << toString(memory->tag)
              << " size=" << memory->size
              << " state=" << static_cast<int>(memory->state)
              << " pin_count=" << memory->pinCount
              << " sequence=" << memory->evictionSequence
              << " submitted_so_far=" << submitted
              << " reclaimed_so_far=" << reclaimed
              << " target_bytes=" << targetBytes;

      const auto blockSize = memory->size;
      writeWindow.Submit(std::move(memory));
      submitted += blockSize;
    }
  };

  submitMore();
  while (writeWindow.hasPending()) {
    auto result = writeWindow.HarvestNext();
    if (!result.ok()) {
      BOLT_FAIL(
          "BM spill write failed, block_id={}, io_error={}, native_error={}, bytes={}",
          result.memory->id,
          static_cast<int>(result.io.error),
          result.io.nativeErrorCode,
          result.io.bytes);
    }

    reclaimed += result.reclaimedBytes;
    VLOG(1) << "BM Reclaim spill finished"
            << " block_id=" << result.memory->id
            << " block_reclaimed_bytes=" << result.reclaimedBytes
            << " submitted_so_far=" << submitted
            << " reclaimed_so_far=" << reclaimed
            << " target_bytes=" << targetBytes
            << " inflight=" << writeWindow.pendingCount()
            << " bm=" << debugString();

    if (targetBytes == 0 || reclaimed < targetBytes) {
      submitMore();
    }
  }

  accounting_->RecordReclaimedBytes(reclaimed);
  VLOG(1) << "BM Reclaim end"
          << " target_bytes=" << targetBytes << " reclaimed_bytes=" << reclaimed
          << " pool_used=" << pool_->usedBytes()
          << " pool_current=" << pool_->currentBytes()
          << " pool_reserved=" << pool_->reservedBytes()
          << " pool_available_reservation=" << pool_->availableReservation()
          << " pool_releasable_reservation=" << pool_->releasableReservation()
          << " bm=" << debugString();
  return reclaimed;
}

uint64_t BufferManager::reclaimableBytes() const {
  return accounting_->reclaimableBytes();
}

BufferManagerStats BufferManager::stats() const {
  auto result = accounting_->stats();
  const auto queueStats = evictionQueue_->stats();
  result.evictionQueueSize = queueStats.size;
  result.evictionQueueStaleEntries = queueStats.staleEntries;
  result.reclaimSkippedBlocks = queueStats.staleEntries;
  return result;
}

std::vector<BufferManagerTagStats> BufferManager::tagStats() const {
  return accounting_->tagStats();
}

std::string BufferManager::debugString() const {
  return toDebugString(stats(), accounting_->allTagStats());
}

void BufferManager::Unpin(const std::shared_ptr<BlockHandle>& block) noexcept {
  if (!block || !block->memory_) {
    LOG(FATAL) << "BM Unpin received invalid block handle";
  }

  auto& memory = *block->memory_;
  if (memory.pinCount == 0) {
    LOG(FATAL) << "BM Unpin underflow, block_id=" << memory.id
               << ", tag=" << toString(memory.tag);
  }
  BlockStateMachine::Unpin(memory);
  if (memory.pinCount == 0 && memory.state == BlockMemoryState::kInMemory) {
    accounting_->OnResidentUnpinned(memory);
    evictionQueue_->Add(block->memory_);
  }
}

BufferHandle BufferManager::PinInMemory(
    const std::shared_ptr<BlockHandle>& block) {
  auto& memory = *block->memory_;
  BOLT_CHECK(memory.payload.has_value(), "resident BM block has no payload");
  accounting_->OnResidentPinned(memory);
  BlockStateMachine::PinResident(memory);
  accounting_->RecordPinInMemory();
  return MakeHandle(block);
}

BufferHandle BufferManager::PinSpilled(
    const std::shared_ptr<BlockHandle>& block) {
  SubmitRead(block, config_.readPriority);
  return PinPrefetching(block);
}

BufferHandle BufferManager::PinPrefetching(
    const std::shared_ptr<BlockHandle>& block) {
  auto& memory = *block->memory_;
  auto read = BlockStateMachine::ConsumePrefetch(memory);
  accounting_->OnReadFutureConsumed(memory);
  if (!read.ok()) {
    accounting_->RecordReadIoFailure();
    BlockStateMachine::MarkReadFailed(memory);
    BOLT_FAIL(
        "BM spill read failed, block_id={}, io_error={}, native_error={}, bytes={}",
        memory.id,
        static_cast<int>(read.io.error),
        read.io.nativeErrorCode,
        read.io.bytes);
  }

  accounting_->OnReadCompleted(memory, read);
  auto oldSegment =
      BlockStateMachine::CompleteRead(memory, std::move(read.io.buffer));
  oldSegment.FreeOrFatal("BufferManager::PinPrefetching");
  return MakeHandle(block);
}

void BufferManager::SubmitRead(
    const std::shared_ptr<BlockHandle>& block,
    IoPriority priority) {
  auto& memory = *block->memory_;
  BOLT_CHECK(
      memory.state == BlockMemoryState::kSpilled,
      "BM read submission expects a spilled block");
  BOLT_CHECK(memory.segment.has_value());

  auto future =
      spillStore_->SubmitReadBlock(*memory.segment, memory.size, priority);
  BlockStateMachine::SubmitRead(memory, std::move(future));
  accounting_->OnReadSubmitted(memory);
}

BufferHandle BufferManager::MakeHandle(
    const std::shared_ptr<BlockHandle>& block) {
  auto& memory = *block->memory_;
  BOLT_CHECK(memory.payload.has_value());
  BOLT_CHECK(memory.payload->valid());
  return BufferHandle{weak_from_this(), block, memory.payload->data()};
}

void BufferManager::OnBlockMemoryDestroy(const BlockMemory& memory) noexcept {
  accounting_->OnBlockMemoryDestroy(memory);
}

} // namespace bytedance::bolt::memory::bm
