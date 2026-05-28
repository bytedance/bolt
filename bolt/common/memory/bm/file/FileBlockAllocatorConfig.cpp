#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

namespace bytedance::bolt::memory::bm {

bool IsFileBlockAligned(int64_t value) {
  return value > 0 && value % kFileBlockAlignment == 0;
}

FileErrorCode ValidateFileBlockAllocatorConfig(
    const FileBlockAllocatorConfig& config) {
  if (config.directory.empty() || config.bucket_sizes.empty() ||
      config.file_size_limit_bytes <= 0 ||
      config.max_open_files_per_bucket == 0) {
    return FileErrorCode::kInvalidConfig;
  }
  if (!IsFileBlockAligned(config.file_size_limit_bytes)) {
    return FileErrorCode::kInvalidConfig;
  }
  int64_t previous = 0;
  for (const auto bucket_size : config.bucket_sizes) {
    if (!IsFileBlockAligned(bucket_size) || bucket_size <= previous) {
      return FileErrorCode::kInvalidConfig;
    }
    previous = bucket_size;
  }
  if (config.file_size_limit_bytes < config.bucket_sizes.back()) {
    return FileErrorCode::kInvalidConfig;
  }
  return FileErrorCode::kOk;
}

} // namespace bytedance::bolt::memory::bm
