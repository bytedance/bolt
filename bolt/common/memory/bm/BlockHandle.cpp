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
