#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

namespace bytedance::bolt::memory::bm {

class FileBlockAllocator {
 public:
  virtual ~FileBlockAllocator() = default;

  virtual FileAllocateResult Allocate(int64_t size) = 0;
  virtual FileFreeResult Free(const FileExtent& extent) = 0;
};

void InitFileBlockAllocator(FileBlockAllocatorConfig config);
FileBlockAllocator& GetFileBlockAllocator();
void ShutdownFileBlockAllocator();

} // namespace bytedance::bolt::memory::bm
