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

#include "bolt/common/memory/bm/BlockStateMachine.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

void BlockStateMachine::PinResident(BlockMemory& memory) {
  BOLT_CHECK(memory.state == BlockMemoryState::kInMemory);
  BOLT_CHECK(memory.payload.has_value(), "resident BM block has no payload");
  ++memory.pinCount;
  ++memory.evictionSequence;
}

void BlockStateMachine::Unpin(BlockMemory& memory) {
  BOLT_CHECK_GT(memory.pinCount, 0);
  --memory.pinCount;
  if (memory.pinCount == 0 && memory.state == BlockMemoryState::kInMemory) {
    ++memory.evictionSequence;
  }
}

void BlockStateMachine::SubmitRead(
    BlockMemory& memory,
    SpillReadFuture future) {
  BOLT_CHECK(
      memory.state == BlockMemoryState::kSpilled,
      "BM read submission expects a spilled block");
  BOLT_CHECK(memory.segment.has_value());
  memory.prefetchFuture = std::move(future);
  memory.state = BlockMemoryState::kPrefetching;
}

SpillReadResult BlockStateMachine::ConsumePrefetch(BlockMemory& memory) {
  BOLT_CHECK(memory.prefetchFuture.has_value());
  auto read = memory.prefetchFuture->get();
  memory.prefetchFuture.reset();
  return read;
}

void BlockStateMachine::MarkReadFailed(BlockMemory& memory) {
  memory.state = BlockMemoryState::kSpilled;
}

ManagedFileSegment BlockStateMachine::CompleteRead(
    BlockMemory& memory,
    IoBuffer payload) {
  BOLT_CHECK(memory.segment.has_value());
  auto oldSegment = std::move(*memory.segment);
  memory.segment.reset();
  memory.payload = std::move(payload);
  memory.state = BlockMemoryState::kInMemory;
  memory.pinCount = 1;
  memory.dirty = false;
  ++memory.evictionSequence;
  return oldSegment;
}

void BlockStateMachine::CompleteReadKeepBacking(
    BlockMemory& memory,
    IoBuffer payload) {
  BOLT_CHECK(memory.segment.has_value());
  memory.payload = std::move(payload);
  memory.state = BlockMemoryState::kInMemory;
  memory.pinCount = 1;
  memory.dirty = false;
  ++memory.evictionSequence;
}

IoBuffer BlockStateMachine::BeginSpill(BlockMemory& memory) {
  BOLT_CHECK(memory.state == BlockMemoryState::kInMemory);
  BOLT_CHECK_EQ(memory.pinCount, 0);
  BOLT_CHECK(memory.payload.has_value());

  auto payload = std::move(*memory.payload);
  memory.payload.reset();
  memory.state = BlockMemoryState::kSpilling;
  return payload;
}

IoBuffer BlockStateMachine::DiscardResidentWithBacking(BlockMemory& memory) {
  BOLT_CHECK(memory.state == BlockMemoryState::kInMemory);
  BOLT_CHECK_EQ(memory.pinCount, 0);
  BOLT_CHECK(memory.payload.has_value());
  BOLT_CHECK(memory.segment.has_value());
  BOLT_CHECK(!memory.dirty);

  auto payload = std::move(*memory.payload);
  memory.payload.reset();
  memory.state = BlockMemoryState::kSpilled;
  ++memory.evictionSequence;
  return payload;
}

void BlockStateMachine::RollbackSpill(BlockMemory& memory, IoBuffer payload) {
  memory.payload = std::move(payload);
  memory.state = BlockMemoryState::kInMemory;
}

void BlockStateMachine::CompleteSpill(
    BlockMemory& memory,
    ManagedFileSegment segment) {
  memory.segment = std::move(segment);
  memory.state = BlockMemoryState::kSpilled;
  memory.dirty = false;
  ++memory.evictionSequence;
}

} // namespace bytedance::bolt::memory::bm
