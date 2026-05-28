#pragma once

#include <cstdint>

namespace bytedance::bolt::memory::bm {

constexpr int64_t kFileBlockAlignment = 4 * 1024;

enum class FileErrorCode : uint8_t {
  kOk,
  kInvalidConfig,
  kInvalidSize,
  kInvalidExtent,
  kDoubleFree,
  kTooManyOpenFiles,
  kIoError,
  kShutdown,
};

enum class FileExtentKind : uint8_t {
  kBucket,
  kDedicated,
};

struct FileExtent {
  int fd{-1};
  uint64_t offset{0};
  uint64_t requested_size{0};
  uint64_t allocated_size{0};
  FileExtentKind kind{FileExtentKind::kBucket};
  uint64_t id{0};
};

struct FileAllocateResult {
  FileErrorCode error{FileErrorCode::kOk};
  int native_error_code{0};
  FileExtent extent;

  bool ok() const {
    return error == FileErrorCode::kOk;
  }
};

struct FileFreeResult {
  FileErrorCode error{FileErrorCode::kOk};
  int native_error_code{0};

  bool ok() const {
    return error == FileErrorCode::kOk;
  }
};

} // namespace bytedance::bolt::memory::bm
