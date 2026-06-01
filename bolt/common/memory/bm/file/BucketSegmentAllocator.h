#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bolt/common/memory/bm/file/SegmentRegistry.h"
#include "bolt/common/memory/bm/file/FileSegmentAllocatorTypes.h"
#include "bolt/common/memory/bm/file/ManagedOpenFile.h"

namespace bytedance::bolt::memory::bm {

class BucketSegmentAllocator {
 public:
  BucketSegmentAllocator(
      std::string directory,
      uint64_t bucket_size,
      uint64_t file_size_limit_bytes,
      uint32_t max_open_files);
  ~BucketSegmentAllocator();

  BucketSegmentAllocator(const BucketSegmentAllocator&) = delete;
  BucketSegmentAllocator& operator=(const BucketSegmentAllocator&) = delete;

  FileAllocation Allocate(int64_t requested_size, uint64_t segment_id);
  FileFreeResult Free(const SegmentRecord& record);

 private:
  struct BucketFile {
    uint64_t file_index{0};
    ManagedOpenFile file;
    uint64_t next_offset{0};
    uint64_t active_segments{0};
    std::vector<uint64_t> free_segment_offsets;
  };

  BucketFile* FindReusableFile();
  FileAllocateResult CreateFile();
  BucketFile* FindFileByIndex(uint64_t file_index);
  void DeleteFile(uint64_t file_index);

  const std::string directory_;
  const uint64_t bucket_size_;
  const uint64_t file_size_limit_bytes_;
  const uint32_t max_open_files_;

  uint64_t next_file_index_{0};
  std::vector<std::unique_ptr<BucketFile>> files_;
};

} // namespace bytedance::bolt::memory::bm
