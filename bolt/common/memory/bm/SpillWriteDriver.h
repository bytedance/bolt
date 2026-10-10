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

#include "bolt/common/memory/bm/SpillCandidateProvider.h"
#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

#include <cstdint>
#include <functional>

namespace bytedance::bolt::memory::bm {

class BufferManagerStatsCollector;
struct BlockDescriptor;

class SpillWriteDriver {
 public:
  using SubmitWrite = std::function<SpillWriteFuture(
      IoBuffer&,
      size_t,
      IoPriority,
      std::shared_ptr<const BlockDescriptor>)>;

  SpillWriteDriver(
      uint32_t maxInflight,
      IoPriority priority,
      SubmitWrite submitWrite,
      BufferManagerStatsCollector& accounting);

  uint64_t Spill(uint64_t targetBytes, SpillCandidateProvider nextCandidate);

 private:
  uint32_t maxInflight_{0};
  IoPriority priority_{IoPriority::Medium};
  SubmitWrite submitWrite_;
  BufferManagerStatsCollector& accounting_;
};

} // namespace bytedance::bolt::memory::bm
