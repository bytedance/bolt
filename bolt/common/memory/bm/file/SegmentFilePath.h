#pragma once

#include <cstdint>
#include <string>

namespace bytedance::bolt::memory::bm {

std::string MakeBucketSegmentFilePath(
    const std::string& directory,
    uint64_t bucket_size,
    uint64_t file_index);

std::string MakeDedicatedSegmentFilePath(
    const std::string& directory,
    uint64_t segment_id);

} // namespace bytedance::bolt::memory::bm
