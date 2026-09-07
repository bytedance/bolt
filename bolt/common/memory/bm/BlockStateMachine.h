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
  static void CompleteReadKeepBacking(BlockMemory& memory, IoBuffer payload);
  static IoBuffer BeginSpill(BlockMemory& memory);
  static IoBuffer DiscardResidentWithBacking(BlockMemory& memory);
  static void RollbackSpill(BlockMemory& memory, IoBuffer payload);
  static void CompleteSpill(BlockMemory& memory, ManagedFileSegment segment);
};

} // namespace bytedance::bolt::memory::bm
