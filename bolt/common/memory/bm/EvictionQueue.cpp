#include "bolt/common/memory/bm/EvictionQueue.h"

namespace bytedance::bolt::memory::bm {

void EvictionQueue::Add(const std::shared_ptr<BlockMemory>& block) {
  queue_.push_back({block, block->evictionSequence});
}

std::shared_ptr<BlockMemory> EvictionQueue::PopEvictable() {
  while (!queue_.empty()) {
    auto entry = queue_.front();
    queue_.pop_front();

    auto block = entry.block.lock();
    if (IsEvictable(block, entry.sequence)) {
      return block;
    }
  }
  return nullptr;
}

bool EvictionQueue::empty() const {
  return queue_.empty();
}

bool EvictionQueue::IsEvictable(
    const std::shared_ptr<BlockMemory>& block,
    uint64_t sequence) {
  return block && block->evictionSequence == sequence && block->pinCount == 0 &&
      block->state == BlockMemoryState::kInMemory && block->payload.has_value();
}

} // namespace bytedance::bolt::memory::bm
