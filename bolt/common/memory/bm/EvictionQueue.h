#pragma once

#include <deque>
#include <memory>

namespace bytedance::bolt::memory::bm {

struct BlockMemory;

class EvictionQueue {
 public:
  struct Stats {
    uint64_t size{0};
    uint64_t staleEntries{0};
  };

  void Add(const std::shared_ptr<BlockMemory>& block);
  std::shared_ptr<BlockMemory> PopEvictable();
  bool empty() const;
  Stats stats() const;

 private:
  struct Entry {
    std::weak_ptr<BlockMemory> block;
    uint64_t sequence{0};
  };

  static bool IsEvictable(
      const std::shared_ptr<BlockMemory>& block,
      uint64_t sequence);

  std::deque<Entry> queue_;
  uint64_t staleEntries_{0};
};

} // namespace bytedance::bolt::memory::bm
