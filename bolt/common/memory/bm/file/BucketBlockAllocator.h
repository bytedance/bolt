#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "bolt/common/memory/bm/file/ExtentRegistry.h"
#include "bolt/common/memory/bm/file/FileBlockAllocatorTypes.h"
#include "bolt/common/memory/bm/file/OwnedFile.h"

namespace bytedance::bolt::memory::bm {

class BucketBlockAllocator {
 public:
  BucketBlockAllocator(
      std::string directory,
      uint64_t bucket_size,
      uint64_t file_size_limit_bytes,
      uint32_t max_open_files);
  ~BucketBlockAllocator();

  BucketBlockAllocator(const BucketBlockAllocator&) = delete;
  BucketBlockAllocator& operator=(const BucketBlockAllocator&) = delete;

  FileAllocation Allocate(int64_t requested_size, uint64_t extent_id);
  FileFreeResult Free(const ExtentRecord& record);

 private:
  struct BucketFile {
    uint64_t file_index{0};
    OwnedFile file;
    uint64_t next_offset{0};
    uint64_t active_blocks{0};
    std::vector<uint64_t> free_offsets;
  };

  BucketFile* FindReusableFileLocked();
  FileAllocateResult CreateFileLocked();
  BucketFile* FindFileByIndexLocked(uint64_t file_index);
  void DeleteFileLocked(uint64_t file_index);

  const std::string directory_;
  const uint64_t bucket_size_;
  const uint64_t file_size_limit_bytes_;
  const uint32_t max_open_files_;

  std::mutex mutex_;
  uint64_t next_file_index_{0};
  std::vector<std::unique_ptr<BucketFile>> files_;
};

} // namespace bytedance::bolt::memory::bm
