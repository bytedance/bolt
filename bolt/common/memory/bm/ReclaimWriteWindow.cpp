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

#include "bolt/common/memory/bm/ReclaimWriteWindow.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BlockStateMachine.h"
#include "bolt/common/memory/bm/BufferManagerStats.h"

#include <utility>

namespace bytedance::bolt::memory::bm {
ReclaimWriteWindow::ReclaimWriteWindow(
    size_t maxInflight,
    IoPriority priority,
    SubmitWrite submitWrite,
    BufferManagerStatsCollector& accounting)
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
