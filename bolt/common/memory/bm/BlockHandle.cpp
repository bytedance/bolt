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

#include "bolt/common/memory/bm/BlockHandle.h"

#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BufferManager.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

BlockMemory::BlockMemory(uint64_t id, size_t size, MemoryTag tag)
    : id(id), size(size), tag(tag) {}

BlockMemory::~BlockMemory() noexcept {
  auto manager = owner.lock();
  if (manager) {
    manager->OnBlockMemoryDestroy(*this);
  }
}

BlockHandle::BlockHandle(std::shared_ptr<BlockMemory> memory)
    : memory_(std::move(memory)) {}

uint64_t BlockHandle::id() const {
  return memory_->id;
}

size_t BlockHandle::size() const {
  return memory_->size;
}

MemoryTag BlockHandle::tag() const {
  return memory_->tag;
}

std::shared_ptr<BlockHandle> testingCreateBlockHandle(
    size_t size,
    MemoryTag tag) {
  return std::make_shared<BlockHandle>(
      std::make_shared<BlockMemory>(0, size, tag));
}

} // namespace bytedance::bolt::memory::bm
