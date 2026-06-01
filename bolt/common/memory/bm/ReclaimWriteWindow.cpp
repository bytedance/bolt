#include "bolt/common/memory/bm/ReclaimWriteWindow.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/BufferManagerAccounting.h"

#include <utility>

namespace bytedance::bolt::memory::bm {
ReclaimWriteWindow::ReclaimWriteWindow(
    size_t maxInflight,
    IoPriority priority,
    SubmitWrite submitWrite,
    BufferManagerAccounting& accounting)
    : maxInflight_(maxInflight),
      priority_(priority),
      submitWrite_(std::move(submitWrite)),
      accounting_(accounting) {
  BOLT_CHECK_GT(maxInflight_, 0);
  BOLT_CHECK(static_cast<bool>(submitWrite_));
}

bool ReclaimWriteWindow::canSubmit() const {
  return pending_.size() < maxInflight_;
}

bool ReclaimWriteWindow::hasPending() const {
  return !pending_.empty();
}

size_t ReclaimWriteWindow::pendingCount() const {
  return pending_.size();
}

void ReclaimWriteWindow::Submit(std::shared_ptr<BlockMemory> memory) {
  BOLT_CHECK_NOT_NULL(memory);
  BOLT_CHECK(canSubmit());

  auto payload = BlockStateMachine::BeginSpill(*memory);
  accounting_.OnSpillStarted(*memory);
  auto write = submitWrite_(payload, memory->size, priority_);
  pending_.push_back(
      PendingWrite{std::move(memory), std::move(payload), std::move(write)});
}

ReclaimWriteWindow::HarvestResult ReclaimWriteWindow::HarvestNext() {
  BOLT_CHECK(!pending_.empty());

  auto pending = std::move(pending_.front());
  pending_.pop_front();

  auto write = pending.write.get();

  if (!write.ok()) {
    accounting_.RecordWriteIoFailure();
    IoResult io = std::move(write.io);
    auto memory = pending.memory;
    return HarvestResult{std::move(memory), std::move(io), 0};
  }

  accounting_.OnSpillCompleted(*pending.memory, write);
  BlockStateMachine::CompleteSpill(*pending.memory, std::move(write.segment));
  const auto reclaimedBytes = pending.memory->size;
  return HarvestResult{
      std::move(pending.memory), std::move(write.io), reclaimedBytes};
}

} // namespace bytedance::bolt::memory::bm
