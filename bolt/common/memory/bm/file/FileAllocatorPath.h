#pragma once

#include <cstdint>
#include <string>

namespace bytedance::bolt::memory::bm {

std::string MakeBucketFilePath(
    const std::string& directory,
    uint64_t bucket_size,
    uint64_t file_index);

std::string MakeDedicatedFilePath(
    const std::string& directory,
    uint64_t extent_id);

} // namespace bytedance::bolt::memory::bm
