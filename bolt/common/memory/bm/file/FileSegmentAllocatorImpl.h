#pragma once

#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"
#include "bolt/common/memory/bm/file/BucketPlacer.h"
#include "bolt/common/memory/bm/file/DedicatedPlacer.h"
#include "bolt/common/memory/bm/file/SegmentRegistry.h"

#include <memory>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

class FileSegmentAllocatorImpl : public FileSegmentAllocator {
 public:
  explicit FileSegmentAllocatorImpl(FileSegmentAllocatorConfig config);
  ~FileSegmentAllocatorImpl() override;

  FileSegmentAllocatorImpl(const FileSegmentAllocatorImpl&) = delete;
  FileSegmentAllocatorImpl& operator=(const FileSegmentAllocatorImpl&) = delete;

  FileAllocateResult Allocate(int64_t size) override;
  FileFreeResult Free(const FileSegment& segment) override;

 private:
  FileAllocateResult AllocateBucket(int64_t size, size_t bucket_index);
  FileAllocateResult AllocateDedicated(int64_t size);
  FileFreeResult FreeBucket(const SegmentRecord& record);
  FileFreeResult FreeDedicated(const SegmentRecord& record);

  FileSegmentAllocatorConfig config_;
  std::string allocator_id_;
  std::string directory_;
  std::vector<std::unique_ptr<BucketPlacer>> buckets_;
  DedicatedPlacer dedicated_placer_;
  SegmentRegistry registry_;
  bool shutdown_{false};
};

} // namespace bytedance::bolt::memory::bm
