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

#include "bolt/common/memory/bm/BufferManagerReclaimer.h"

#include "bolt/common/memory/bm/BufferManager.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

BufferManagerReclaimer::BufferManagerReclaimer(
    std::weak_ptr<BufferManager> manager)
    : memory::MemoryReclaimer(0), manager_(std::move(manager)) {}

bool BufferManagerReclaimer::reclaimableBytes(
    const MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  auto manager = manager_.lock();
  if (!manager) {
    reclaimableBytes = 0;
    return false;
  }
  reclaimableBytes = manager->reclaimableBytes();
  return reclaimableBytes > 0;
}

uint64_t BufferManagerReclaimer::reclaim(
    MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    Stats& stats) {
  // v1 intentionally ignores maxWaitMs. Reclaim writes are synchronous and
  // timeout budgeting will be added with the production arbitration policy.
  (void)maxWaitMs;
  BOLT_CHECK_NOT_NULL(pool);
  auto manager = manager_.lock();
  if (!manager) {
    return 0;
  }
  const auto reclaimedByRecorder = memory::MemoryReclaimer::run(
      [&]() {
        int64_t reclaimedBytes{0};
        {
          memory::ScopedReclaimedBytesRecorder recorder(pool, &reclaimedBytes);
          manager->Reclaim(targetBytes);
          pool->release();
        }
        return reclaimedBytes;
      },
      stats);
  return reclaimedByRecorder;
}

} // namespace bytedance::bolt::memory::bm
