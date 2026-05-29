#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>

#include "bolt/common/memory/bm/file/FileBlockAllocatorTypes.h"

namespace bytedance::bolt::memory::bm {

struct ExtentRecord {
  FileExtent extent;
  size_t bucket_index{0};
  uint64_t file_index{0};
};

struct FileAllocation {
  FileAllocateResult result;
  ExtentRecord record;
};

class ExtentRegistry {
 public:
  uint64_t NextExtentId();
  void Register(ExtentRecord record);
  FileErrorCode Take(uint64_t extent_id, ExtentRecord* record);

 private:
  uint64_t next_extent_id_{1};
  std::unordered_map<uint64_t, ExtentRecord> records_;
};

} // namespace bytedance::bolt::memory::bm
