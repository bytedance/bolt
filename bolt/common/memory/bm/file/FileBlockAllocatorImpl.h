#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocator.h"
#include "bolt/common/memory/bm/file/BucketBlockAllocator.h"
#include "bolt/common/memory/bm/file/DedicatedFileAllocator.h"
#include "bolt/common/memory/bm/file/ExtentRegistry.h"

#include <memory>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

class FileBlockAllocatorImpl : public FileBlockAllocator {
 public:
  explicit FileBlockAllocatorImpl(FileBlockAllocatorConfig config);
  ~FileBlockAllocatorImpl() override;

  FileBlockAllocatorImpl(const FileBlockAllocatorImpl&) = delete;
  FileBlockAllocatorImpl& operator=(const FileBlockAllocatorImpl&) = delete;

  FileAllocateResult Allocate(int64_t size) override;
  FileFreeResult Free(const FileExtent& extent) override;

 private:
  FileAllocateResult AllocateBucket(int64_t size, size_t bucket_index);
  FileAllocateResult AllocateDedicated(int64_t size);
  FileFreeResult FreeBucket(const ExtentRecord& record);
  FileFreeResult FreeDedicated(const ExtentRecord& record);

  FileBlockAllocatorConfig config_;
  std::string allocator_id_;
  std::string directory_;
  std::vector<std::unique_ptr<BucketBlockAllocator>> buckets_;
  DedicatedFileAllocator dedicated_allocator_;
  ExtentRegistry registry_;
  bool shutdown_{false};
};

} // namespace bytedance::bolt::memory::bm
