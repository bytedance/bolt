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

#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>

namespace bytedance::bolt::memory::bm {

class BufferManagerStatsCollector;
struct BlockMemory;

class ReclaimWriteWindow {
 public:
  using SubmitWrite =
      std::function<SpillWriteFuture(IoBuffer&, size_t, IoPriority)>;

  struct HarvestResult {
    std::shared_ptr<BlockMemory> memory;
    IoResult io;
    uint64_t reclaimedBytes{0};

    bool ok() const {
      return io.ok();
    }
  };

  ReclaimWriteWindow(
      size_t maxInflight,
      IoPriority priority,
      SubmitWrite submitWrite,
      BufferManagerStatsCollector& accounting);

  bool canSubmit() const;
  bool hasPending() const;
  size_t pendingCount() const;

  void Submit(std::shared_ptr<BlockMemory> memory);
  HarvestResult HarvestNext();

 private:
  struct PendingWrite {
    std::shared_ptr<BlockMemory> memory;
    IoBuffer payload;
    SpillWriteFuture write;
  };

  size_t maxInflight_{0};
  IoPriority priority_;
  SubmitWrite submitWrite_;
  BufferManagerStatsCollector& accounting_;
  std::deque<PendingWrite> pending_;
};

} // namespace bytedance::bolt::memory::bm
