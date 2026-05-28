#pragma once

#include <string>
#include <vector>

#include "bolt/common/memory/bm/file/FileBlockAllocatorTypes.h"

namespace bytedance::bolt::memory::bm {

struct FileBlockAllocatorConfig {
  std::string directory;
  std::vector<int64_t> bucket_sizes;
  int64_t file_size_limit_bytes{0};
  uint32_t max_open_files_per_bucket{0};
};

bool IsFileBlockAligned(int64_t value);

FileErrorCode ValidateFileBlockAllocatorConfig(
    const FileBlockAllocatorConfig& config);

} // namespace bytedance::bolt::memory::bm
