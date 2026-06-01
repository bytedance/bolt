#include "bolt/common/memory/bm/file/FileAllocatorPath.h"

#include <string>

namespace bytedance::bolt::memory::bm {

std::string MakeBucketFilePath(
    const std::string& directory,
    uint64_t bucket_size,
    uint64_t file_index) {
  return directory + "/bucket_" + std::to_string(bucket_size) + "_" +
      std::to_string(file_index) + ".bm";
}

std::string MakeDedicatedFilePath(
    const std::string& directory,
    uint64_t segment_id) {
  return directory + "/dedicated_" + std::to_string(segment_id) + ".bm";
}

} // namespace bytedance::bolt::memory::bm
