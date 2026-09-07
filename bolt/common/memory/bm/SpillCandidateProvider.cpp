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

#include "bolt/common/memory/bm/SpillCandidateProvider.h"

#include "bolt/common/memory/bm/BlockMemory.h"

namespace bytedance::bolt::memory::bm {

SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
    std::span<const std::shared_ptr<BlockHandle>> blocks) {
  size_t index = 0;
  return [blocks, index]() mutable -> std::shared_ptr<BlockMemory> {
    while (index < blocks.size()) {
      const auto& block = blocks[index++];
      if (!block || !block->memory_) {
        continue;
      }

      auto memory = block->memory_;
      if (memory->pinCount == 0 &&
          memory->state == BlockMemoryState::kInMemory &&
          memory->payload.has_value()) {
        return memory;
      }
    }
    return nullptr;
  };
}

} // namespace bytedance::bolt::memory::bm
