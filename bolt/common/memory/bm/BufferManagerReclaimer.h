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

#include "bolt/common/memory/MemoryArbitrator.h"

#include <memory>

namespace bytedance::bolt::memory::bm {

class BufferManager;

class BufferManagerReclaimer final : public memory::MemoryReclaimer {
 public:
  explicit BufferManagerReclaimer(
      std::weak_ptr<BufferManager> manager,
      std::shared_ptr<memory::MemoryReclaimer> arbitrationReclaimer = nullptr);

  void enterArbitration() override;

  void leaveArbitration() noexcept override;

  bool reclaimableBytes(const MemoryPool& pool, uint64_t& reclaimableBytes)
      const override;

  uint64_t reclaim(
      MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      Stats& stats) override;

 private:
  std::weak_ptr<BufferManager> manager_;
  std::shared_ptr<memory::MemoryReclaimer> arbitrationReclaimer_;
};

} // namespace bytedance::bolt::memory::bm
