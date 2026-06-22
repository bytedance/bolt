#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "bolt/common/memory/bm/file/FileSegmentAllocatorTypes.h"
#include "bolt/common/memory/bm/file/ManagedOpenFile.h"
#include "bolt/common/memory/bm/file/SegmentRegistry.h"

namespace bytedance::bolt::memory::bm {

class DedicatedPlacer {
 public:
  explicit DedicatedPlacer(std::string directory);
  ~DedicatedPlacer();

  DedicatedPlacer(const DedicatedPlacer&) = delete;
  DedicatedPlacer& operator=(const DedicatedPlacer&) = delete;

  FileAllocation Allocate(int64_t requested_size, uint64_t segment_id);
  FileFreeResult Free(const SegmentRecord& record);
  void RemoveAllFiles();

 private:
  const std::string directory_;
  std::unordered_map<uint64_t, ManagedOpenFile> files_;
};

} // namespace bytedance::bolt::memory::bm
