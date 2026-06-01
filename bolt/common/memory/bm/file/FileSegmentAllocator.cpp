#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"

#include "bolt/common/memory/bm/file/FileSegmentAllocatorImpl.h"

#include <memory>
#include <utility>

namespace bytedance::bolt::memory::bm {

std::shared_ptr<FileSegmentAllocator> CreateFileSegmentAllocator(
    FileSegmentAllocatorConfig config) {
  return std::make_shared<FileSegmentAllocatorImpl>(std::move(config));
}

} // namespace bytedance::bolt::memory::bm
