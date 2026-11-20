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

#include "bolt/common/memory/bm/MemoryTag.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>

namespace bytedance::bolt::memory::bm {

class BufferHandle;
class BufferManager;
struct BlockMemory;
class BlockHandle;

using SpillCandidateProvider = std::function<std::shared_ptr<BlockMemory>()>;

SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
    std::span<const std::shared_ptr<BlockHandle>> blocks);

class BlockHandle {
 public:
  explicit BlockHandle(std::shared_ptr<BlockMemory> memory);

  uint64_t id() const;
  size_t size() const;
  MemoryTag tag() const;

 private:
  std::shared_ptr<BlockMemory> memory_;

  friend SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
      std::span<const std::shared_ptr<BlockHandle>> blocks);
  friend class BufferHandle;
  friend class BufferManager;
};

std::shared_ptr<BlockHandle> testingCreateBlockHandle(
    size_t size,
    MemoryTag tag);

} // namespace bytedance::bolt::memory::bm
