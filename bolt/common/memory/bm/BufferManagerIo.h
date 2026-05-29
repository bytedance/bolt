#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/OwnedFileExtent.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <future>
#include <memory>

namespace bytedance::bolt::memory::bm {

class BufferManagerIo {
 public:
  BufferManagerIo(
      std::shared_ptr<FileBlockAllocator> allocator,
      MemoryPool* pool);

  FileAllocateResult AllocateExtent(size_t size);
  FileFreeResult FreeExtent(const FileExtent& extent);
  OwnedFileExtent OwnExtent(FileExtent extent) const;

  std::future<IoResult>
  SubmitRead(const OwnedFileExtent& extent, size_t size, IoPriority priority);
  IoResult
  Write(const FileExtent& extent, IoBuffer& payload, IoPriority priority);

 private:
  void EnsureSchedulerReadyForPayloadMove();

  std::shared_ptr<FileBlockAllocator> allocator_;
  MemoryPool* pool_{nullptr};
  bool schedulerReadyForPayloadMove_{false};
};

} // namespace bytedance::bolt::memory::bm
