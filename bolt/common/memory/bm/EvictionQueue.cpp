#include "bolt/common/memory/bm/EvictionQueue.h"

#include "bolt/common/memory/bm/BlockMemory.h"

namespace bytedance::bolt::memory::bm {

void EvictionQueue::Add(const std::shared_ptr<BlockMemory>& block) {
  // Snapshot the block generation so stale entries can be skipped lazily.
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
    ++staleEntries_;
  }
  return nullptr;
}

bool EvictionQueue::empty() const {
  return queue_.empty();
}

EvictionQueue::Stats EvictionQueue::stats() const {
  return Stats{queue_.size(), staleEntries_};
}

bool EvictionQueue::IsEvictable(
    const std::shared_ptr<BlockMemory>& block,
    uint64_t sequence) {
  // A mismatched generation means this queue entry predates a pin/unpin or
  // spill/read transition, so it must not be used as a reclaim candidate.
  return block && block->evictionSequence == sequence && block->pinCount == 0 &&
      block->state == BlockMemoryState::kInMemory && block->payload.has_value();
}

} // namespace bytedance::bolt::memory::bm
