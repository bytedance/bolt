#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include <memory>
#include <utility>

namespace bytedance::bolt::memory::bm {

std::shared_ptr<FileBlockAllocator> CreateFileBlockAllocator(
    FileBlockAllocatorConfig config) {
  return std::make_shared<FileBlockAllocatorImpl>(std::move(config));
}

} // namespace bytedance::bolt::memory::bm
