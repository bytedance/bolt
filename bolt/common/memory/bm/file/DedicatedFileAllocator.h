#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "bolt/common/memory/bm/file/SegmentRegistry.h"
#include "bolt/common/memory/bm/file/FileSegmentAllocatorTypes.h"
#include "bolt/common/memory/bm/file/ManagedOpenFile.h"

namespace bytedance::bolt::memory::bm {

class DedicatedFileAllocator {
 public:
  explicit DedicatedFileAllocator(std::string directory);
  ~DedicatedFileAllocator();

  DedicatedFileAllocator(const DedicatedFileAllocator&) = delete;
  DedicatedFileAllocator& operator=(const DedicatedFileAllocator&) = delete;

  FileAllocation Allocate(int64_t requested_size, uint64_t segment_id);
  FileFreeResult Free(const SegmentRecord& record);
  void RemoveAllFiles();

 private:
  const std::string directory_;
  std::unordered_map<uint64_t, ManagedOpenFile> files_;
};

} // namespace bytedance::bolt::memory::bm
