#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include <memory>
#include <utility>

namespace bytedance::bolt::memory::bm {

std::unique_ptr<FileBlockAllocator> CreateFileBlockAllocator(
    FileBlockAllocatorConfig config) {
  return std::make_unique<FileBlockAllocatorImpl>(std::move(config));
}

} // namespace bytedance::bolt::memory::bm
