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
