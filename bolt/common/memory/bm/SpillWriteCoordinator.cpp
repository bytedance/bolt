#include "bolt/common/memory/bm/SpillWriteCoordinator.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/BufferManagerAccounting.h"
#include "bolt/common/memory/bm/EvictionQueue.h"

#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

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
    (void)write.get();
  } catch (...) {
  }
}

} // namespace

SpillWriteCoordinator::SpillWriteCoordinator(
    size_t maxInflight,
    IoPriority priority,
    SubmitWrite submitWrite,
    BufferManagerAccounting& accounting,
    EvictionQueue& evictionQueue)
    : maxInflight_(maxInflight),
      priority_(priority),
      submitWrite_(std::move(submitWrite)),
      accounting_(accounting),
      evictionQueue_(evictionQueue) {
  BOLT_CHECK_GT(maxInflight_, 0);
  BOLT_CHECK(static_cast<bool>(submitWrite_));
}

bool SpillWriteCoordinator::canSubmit() const {
  return pending_.size() < maxInflight_;
}

bool SpillWriteCoordinator::hasPending() const {
  return !pending_.empty();
}

size_t SpillWriteCoordinator::pendingCount() const {
  return pending_.size();
}

void SpillWriteCoordinator::Submit(std::shared_ptr<BlockMemory> memory) {
  BOLT_CHECK_NOT_NULL(memory);
  BOLT_CHECK(canSubmit());

  auto payload = BlockStateMachine::BeginSpill(*memory);
  accounting_.OnSpillStarted(*memory);
  try {
    auto write = submitWrite_(payload, memory->size, priority_);
    pending_.push_back(PendingWrite{
        std::move(memory), std::move(payload), std::move(write)});
  } catch (...) {
    BlockStateMachine::RollbackSpill(*memory, std::move(payload));
    accounting_.OnSpillRolledBack(*memory);
    RequeueIfEvictable(evictionQueue_, memory);
    throw;
  }
}

SpillWriteCoordinator::HarvestResult SpillWriteCoordinator::HarvestNext() {
  BOLT_CHECK(!pending_.empty());

  auto pending = std::move(pending_.front());
  pending_.pop_front();

  SpillWriteResult write;
  try {
    write = pending.write.get();
  } catch (...) {
    RollbackCurrentAndPending(std::move(pending));
    throw;
  }

  if (!write.ok()) {
    accounting_.RecordWriteIoFailure();
    IoResult io = std::move(write.io);
    auto memory = pending.memory;
    RollbackCurrentAndPending(std::move(pending));
    return HarvestResult{std::move(memory), std::move(io), 0};
  }

  accounting_.OnSpillCompleted(*pending.memory, write);
  BlockStateMachine::CompleteSpill(*pending.memory, std::move(write.extent));
  const auto reclaimedBytes = pending.memory->size;
  return HarvestResult{
      std::move(pending.memory), std::move(write.io), reclaimedBytes};
}

void SpillWriteCoordinator::RollbackPending() {
  while (!pending_.empty()) {
    auto pending = std::move(pending_.front());
    pending_.pop_front();
    Rollback(std::move(pending));
  }
}

void SpillWriteCoordinator::Rollback(PendingWrite&& pending) {
  WaitForWriteNoThrow(pending.write);
  BlockStateMachine::RollbackSpill(*pending.memory, std::move(pending.payload));
  accounting_.OnSpillRolledBack(*pending.memory);
  RequeueIfEvictable(evictionQueue_, pending.memory);
}

void SpillWriteCoordinator::RollbackCurrentAndPending(
    PendingWrite&& current) {
  Rollback(std::move(current));
  RollbackPending();
}

} // namespace bytedance::bolt::memory::bm
