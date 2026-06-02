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
