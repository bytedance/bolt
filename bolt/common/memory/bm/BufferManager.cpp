#include "bolt/common/memory/bm/BufferManager.h"

#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManagerAccounting.h"
#include "bolt/common/memory/bm/BufferManagerReclaimer.h"
#include "bolt/common/memory/bm/EvictionQueue.h"
#include "bolt/common/memory/bm/SpillStore.h"

#include <glog/logging.h>

#include <utility>
#include <vector>

namespace bytedance::bolt::memory::bm {
namespace {

[[noreturn]] void ThrowIoFailure(
    const char* operation,
    const IoResult& result,
    uint64_t blockId) {
  BOLT_FAIL(
      "BM {} IO failed, block_id={}, io_error={}, native_error={}, bytes={}",
      operation,
      blockId,
      static_cast<int>(result.error),
      result.nativeErrorCode,
      result.bytes);
}

struct PendingSpill {
  std::shared_ptr<BlockMemory> memory;
  IoBuffer payload;
  SpillWriteFuture write;
};

SpillWriteResult CompleteWriteFuture(SpillWriteFuture&& write) {
  SpillWriteResult result;
  result.io = write.future.get();
  result.extent = std::move(write.extent);
  result.rawBytes = write.rawBytes;
  result.physicalBytes = write.physicalBytes;
  result.compressionTimeUs = write.compressionTimeUs;
  result.compressed = write.compressed;
  return result;
}

void RequeueIfEvictable(
    EvictionQueue& evictionQueue,
    const std::shared_ptr<BlockMemory>& memory) {
  if (memory->pinCount == 0 && memory->state == BlockMemoryState::kInMemory &&
      memory->payload.has_value()) {
    evictionQueue.Add(memory);
  }
}

void WaitForWriteNoThrow(SpillWriteFuture& write) noexcept {
  try {
    (void)write.future.get();
  } catch (...) {
  }
}

} // namespace

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
      accounting_(std::make_unique<BufferManagerAccounting>()),
      evictionQueue_(std::make_unique<EvictionQueue>()) {
}

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

std::vector<BufferHandle> BufferManager::BatchAllocate(
    size_t count,
    size_t size,
    MemoryTag tag) {
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
          << " size=" << size
          << " pool_used=" << pool_->usedBytes()
          << " pool_current=" << pool_->currentBytes()
          << " pool_reserved=" << pool_->reservedBytes()
          << " pool_available_reservation=" << pool_->availableReservation()
          << " pool_releasable_reservation=" << pool_->releasableReservation()
          << " bm=" << debugString();
  const auto ok = pool_->maybeReserve(size);
  VLOG(1) << "BM MaybeReserve end"
          << " ok=" << ok
          << " size=" << size
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
    std::span<const std::shared_ptr<BlockHandle>> blocks) noexcept {
  accounting_->RecordPrefetch();
  for (const auto& block : blocks) {
    if (!block) {
      accounting_->RecordPrefetchSubmitFailure();
      LOG(WARNING) << "BM Prefetch ignored null block";
      continue;
    }
    try {
      auto& memory = *block->memory_;
      if (memory.state == BlockMemoryState::kSpilled) {
        SubmitRead(block, config_.prefetchPriority);
      }
    } catch (const std::exception& e) {
      accounting_->RecordPrefetchSubmitFailure();
      LOG(WARNING) << "BM Prefetch failed for block_id=" << block->id() << ": "
                   << e.what();
    } catch (...) {
      accounting_->RecordPrefetchSubmitFailure();
      LOG(WARNING) << "BM Prefetch failed for block_id=" << block->id()
                   << " with unknown exception";
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
  uint64_t reclaimed = 0;
  while (targetBytes == 0 || reclaimed < targetBytes) {
    const auto remaining =
        targetBytes == 0 ? 0 : targetBytes - reclaimed;
    const auto batchReclaimed = SpillBlocksForReclaim(remaining);
    if (batchReclaimed == 0) {
      break;
    }
    reclaimed += batchReclaimed;
  }

  accounting_->RecordReclaimedBytes(reclaimed);
  VLOG(1) << "BM Reclaim end"
          << " target_bytes=" << targetBytes
          << " reclaimed_bytes=" << reclaimed
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
    ThrowIoFailure("read", read.io, memory.id);
  }

  accounting_->OnReadCompleted(memory, read);
  auto oldExtent =
      BlockStateMachine::CompleteRead(memory, std::move(read.io.buffer));
  oldExtent.FreeOrFatal("BufferManager::PinPrefetching");
  return MakeHandle(block);
}

void BufferManager::SubmitRead(
    const std::shared_ptr<BlockHandle>& block,
    IoPriority priority) {
  auto& memory = *block->memory_;
  BOLT_CHECK(
      memory.state == BlockMemoryState::kSpilled,
      "BM read submission expects a spilled block");
  BOLT_CHECK(memory.extent.has_value());

  auto future =
      spillStore_->SubmitReadBlock(*memory.extent, memory.size, priority);
  BlockStateMachine::SubmitRead(memory, std::move(future));
  accounting_->OnReadSubmitted(memory);
}

uint64_t BufferManager::SpillBlocksForReclaim(uint64_t targetBytes) {
  BOLT_CHECK_GT(config_.maxReclaimWriteBatchSize, 0);

  std::vector<PendingSpill> pending;
  pending.reserve(config_.maxReclaimWriteBatchSize);
  uint64_t submittedBytes = 0;
  while (pending.size() < config_.maxReclaimWriteBatchSize &&
         (targetBytes == 0 || submittedBytes < targetBytes)) {
    auto memory = evictionQueue_->PopEvictable();
    if (!memory) {
      VLOG(1) << "BM Reclaim no evictable block"
              << " target_bytes=" << targetBytes
              << " submitted_bytes=" << submittedBytes
              << " bm=" << debugString();
      break;
    }

    accounting_->RecordReclaimAttemptedBlock();
    VLOG(1) << "BM Reclaim spill candidate"
            << " block_id=" << memory->id
            << " tag=" << toString(memory->tag)
            << " size=" << memory->size
            << " state=" << static_cast<int>(memory->state)
            << " pin_count=" << memory->pinCount
            << " sequence=" << memory->evictionSequence
            << " submitted_so_far=" << submittedBytes
            << " target_bytes=" << targetBytes;

    auto payload = BlockStateMachine::BeginSpill(*memory);
    accounting_->OnSpillStarted(*memory);
    try {
      auto write = spillStore_->SubmitWriteBlock(
          payload, memory->size, config_.writePriority);
      submittedBytes += memory->size;
      pending.push_back(PendingSpill{
          std::move(memory), std::move(payload), std::move(write)});
    } catch (...) {
      BlockStateMachine::RollbackSpill(*memory, std::move(payload));
      accounting_->OnSpillRolledBack(*memory);
      RequeueIfEvictable(*evictionQueue_, memory);
      throw;
    }
  }

  uint64_t reclaimed = 0;
  for (size_t i = 0; i < pending.size(); ++i) {
    auto& spill = pending[i];
    SpillWriteResult write;
    try {
      write = CompleteWriteFuture(std::move(spill.write));
    } catch (...) {
      BlockStateMachine::RollbackSpill(*spill.memory, std::move(spill.payload));
      accounting_->OnSpillRolledBack(*spill.memory);
      RequeueIfEvictable(*evictionQueue_, spill.memory);
      for (size_t j = i + 1; j < pending.size(); ++j) {
        WaitForWriteNoThrow(pending[j].write);
        BlockStateMachine::RollbackSpill(
            *pending[j].memory, std::move(pending[j].payload));
        accounting_->OnSpillRolledBack(*pending[j].memory);
        RequeueIfEvictable(*evictionQueue_, pending[j].memory);
      }
      throw;
    }
    if (!write.ok()) {
      accounting_->RecordWriteIoFailure();
      BlockStateMachine::RollbackSpill(*spill.memory, std::move(spill.payload));
      accounting_->OnSpillRolledBack(*spill.memory);
      RequeueIfEvictable(*evictionQueue_, spill.memory);
      for (size_t j = i + 1; j < pending.size(); ++j) {
        WaitForWriteNoThrow(pending[j].write);
        BlockStateMachine::RollbackSpill(
            *pending[j].memory, std::move(pending[j].payload));
        accounting_->OnSpillRolledBack(*pending[j].memory);
        RequeueIfEvictable(*evictionQueue_, pending[j].memory);
      }
      ThrowIoFailure("write", write.io, spill.memory->id);
    }

    accounting_->OnSpillCompleted(*spill.memory, write);
    BlockStateMachine::CompleteSpill(*spill.memory, std::move(write.extent));
    reclaimed += spill.memory->size;
    VLOG(1) << "BM Reclaim spill finished"
            << " block_id=" << spill.memory->id
            << " block_reclaimed_bytes=" << spill.memory->size
            << " reclaimed_so_far=" << reclaimed
            << " target_bytes=" << targetBytes
            << " bm=" << debugString();
  }

  return reclaimed;
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
