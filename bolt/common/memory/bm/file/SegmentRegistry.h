#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>

#include "bolt/common/memory/bm/file/FileSegmentAllocatorTypes.h"

namespace bytedance::bolt::memory::bm {

struct SegmentRecord {
  FileSegment segment;
  size_t bucket_index{0};
  uint64_t file_index{0};
};

struct FileAllocation {
  FileAllocateResult result;
  SegmentRecord record;
};

class SegmentRegistry {
 public:
  uint64_t NextSegmentId();
  void Register(SegmentRecord record);
  FileErrorCode Take(uint64_t segment_id, SegmentRecord* record);

 private:
  uint64_t next_segment_id_{1};
  std::unordered_map<uint64_t, SegmentRecord> records_;
};

} // namespace bytedance::bolt::memory::bm
