#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

namespace bytedance::bolt::memory::bm {

class FileBlockAllocator {
 public:
  virtual ~FileBlockAllocator() = default;

  virtual FileAllocateResult allocate(int64_t size) = 0;
  virtual FileFreeResult free(const FileExtent& extent) = 0;
};

void initFileBlockAllocator(FileBlockAllocatorConfig config);
FileBlockAllocator& fileBlockAllocator();
void shutdownFileBlockAllocator();

} // namespace bytedance::bolt::memory::bm
