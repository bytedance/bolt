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
  ++memory.evictionSequence;
  return oldSegment;
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

void BlockStateMachine::RollbackSpill(BlockMemory& memory, IoBuffer payload) {
  memory.payload = std::move(payload);
  memory.state = BlockMemoryState::kInMemory;
}

void BlockStateMachine::CompleteSpill(
    BlockMemory& memory,
    ManagedFileSegment segment) {
  memory.segment = std::move(segment);
  memory.state = BlockMemoryState::kSpilled;
  ++memory.evictionSequence;
}

} // namespace bytedance::bolt::memory::bm
