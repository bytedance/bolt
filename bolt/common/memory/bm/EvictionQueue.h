#pragma once

#include "bolt/common/memory/bm/BlockHandle.h"

#include <deque>
#include <memory>

namespace bytedance::bolt::memory::bm {

class EvictionQueue {
 public:
  void Add(const std::shared_ptr<BlockMemory>& block);
  std::shared_ptr<BlockMemory> PopEvictable();
  bool empty() const;

 private:
  struct Entry {
    std::weak_ptr<BlockMemory> block;
    uint64_t sequence{0};
  };

  static bool IsEvictable(
      const std::shared_ptr<BlockMemory>& block,
      uint64_t sequence);

  std::deque<Entry> queue_;
};

} // namespace bytedance::bolt::memory::bm
