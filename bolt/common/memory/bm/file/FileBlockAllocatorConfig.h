#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

constexpr int64_t kFileBlockAlignment = 4 * 1024;

enum class FileErrorCode : uint8_t {
  Ok,
  InvalidConfig,
  InvalidSize,
  InvalidExtent,
  DoubleFree,
  TooManyOpenFiles,
  IoError,
  Shutdown,
};

enum class FileExtentKind : uint8_t {
  Bucket,
  Dedicated,
};

struct FileBlockAllocatorConfig {
  std::string directory;
  std::vector<int64_t> bucketSizes;
  int64_t fileSizeLimitBytes{0};
  uint32_t maxOpenFilesPerBucket{0};
};

struct FileExtent {
  int fd{-1};
  uint64_t offset{0};
  uint64_t requestedSize{0};
  uint64_t allocatedSize{0};
  FileExtentKind kind{FileExtentKind::Bucket};
  uint64_t id{0};
};

struct FileAllocateResult {
  FileErrorCode error{FileErrorCode::Ok};
  int nativeErrorCode{0};
  FileExtent extent;

  bool ok() const {
    return error == FileErrorCode::Ok;
  }
};

struct FileFreeResult {
  FileErrorCode error{FileErrorCode::Ok};
  int nativeErrorCode{0};

  bool ok() const {
    return error == FileErrorCode::Ok;
  }
};

inline bool isFileBlockAligned(int64_t value) {
  return value > 0 && value % kFileBlockAlignment == 0;
}

inline FileErrorCode validateFileBlockAllocatorConfig(
    const FileBlockAllocatorConfig& config) {
  if (config.directory.empty() || config.bucketSizes.empty() ||
      config.fileSizeLimitBytes <= 0 ||
      config.maxOpenFilesPerBucket == 0) {
    return FileErrorCode::InvalidConfig;
  }
  if (!isFileBlockAligned(config.fileSizeLimitBytes)) {
    return FileErrorCode::InvalidConfig;
  }
  int64_t previous = 0;
  for (const auto bucketSize : config.bucketSizes) {
    if (!isFileBlockAligned(bucketSize) || bucketSize <= previous) {
      return FileErrorCode::InvalidConfig;
    }
    previous = bucketSize;
  }
  if (config.fileSizeLimitBytes < config.bucketSizes.back()) {
    return FileErrorCode::InvalidConfig;
  }
  return FileErrorCode::Ok;
}

} // namespace bytedance::bolt::memory::bm
