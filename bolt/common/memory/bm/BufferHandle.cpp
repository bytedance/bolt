#include "bolt/common/memory/bm/BufferHandle.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BufferManager.h"

#include <glog/logging.h>
#include <utility>

namespace bytedance::bolt::memory::bm {

BufferHandle::BufferHandle(
    std::weak_ptr<BufferManager> manager,
    std::shared_ptr<BlockHandle> block,
    char* data)
    : manager_(std::move(manager)), block_(std::move(block)), data_(data) {}

BufferHandle::~BufferHandle() noexcept {
  Destroy();
}

BufferHandle::BufferHandle(BufferHandle&& other) noexcept
    : manager_(std::move(other.manager_)),
      block_(std::move(other.block_)),
      data_(other.data_) {
  other.data_ = nullptr;
}

BufferHandle& BufferHandle::operator=(BufferHandle&& other) noexcept {
  if (this != &other) {
    Destroy();
    manager_ = std::move(other.manager_);
    block_ = std::move(other.block_);
    data_ = other.data_;
    other.data_ = nullptr;
  }
  return *this;
}

char* BufferHandle::Ptr() const {
  BOLT_CHECK_NOT_NULL(data_);
  return data_;
}

std::shared_ptr<BlockHandle> BufferHandle::block() const {
  BOLT_CHECK_NOT_NULL(block_);
  return block_;
}

bool BufferHandle::valid() const {
  return data_ != nullptr && block_ != nullptr;
}

void BufferHandle::Destroy() noexcept {
  if (!block_) {
    return;
  }

  auto manager = manager_.lock();
  if (!manager) {
    const auto& memory = block_->memory_;
    LOG(FATAL) << "BufferHandle outlived BufferManager, block_id="
               << (memory ? memory->id : 0)
               << ", tag=" << (memory ? toString(memory->tag) : "Unknown");
  }

  manager->Unpin(block_);
  block_.reset();
  data_ = nullptr;
}

} // namespace bytedance::bolt::memory::bm
