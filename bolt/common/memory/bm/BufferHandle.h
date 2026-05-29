#pragma once

#include "bolt/common/memory/bm/BlockHandle.h"

#include <memory>

namespace bytedance::bolt::memory::bm {

class BufferManager;

class BufferHandle {
 public:
  BufferHandle() = default;
  BufferHandle(
      std::weak_ptr<BufferManager> manager,
      std::shared_ptr<BlockHandle> block,
      char* data);
  ~BufferHandle() noexcept;

  BufferHandle(BufferHandle&& other) noexcept;
  BufferHandle& operator=(BufferHandle&& other) noexcept;

  BufferHandle(const BufferHandle&) = delete;
  BufferHandle& operator=(const BufferHandle&) = delete;

  char* Ptr() const;
  bool valid() const;
  void Destroy() noexcept;

 private:
  std::weak_ptr<BufferManager> manager_;
  std::shared_ptr<BlockHandle> block_;
  char* data_{nullptr};
};

} // namespace bytedance::bolt::memory::bm
