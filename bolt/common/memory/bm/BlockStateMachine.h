#pragma once

#include "bolt/common/memory/bm/BlockMemory.h"

namespace bytedance::bolt::memory::bm {

class BlockStateMachine {
 public:
  static void PinResident(BlockMemory& memory);
  static void Unpin(BlockMemory& memory);
  static void SubmitRead(BlockMemory& memory, SpillReadFuture future);
  static SpillReadResult ConsumePrefetch(BlockMemory& memory);
  static void MarkReadFailed(BlockMemory& memory);
  static ManagedFileSegment CompleteRead(BlockMemory& memory, IoBuffer payload);
  static IoBuffer BeginSpill(BlockMemory& memory);
  static void RollbackSpill(BlockMemory& memory, IoBuffer payload);
  static void CompleteSpill(BlockMemory& memory, ManagedFileSegment segment);
};

} // namespace bytedance::bolt::memory::bm
