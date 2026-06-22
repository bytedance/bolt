#pragma once

#include <cstdint>

namespace bytedance::bolt::memory::bm {

constexpr int64_t kFileSegmentAlignment = 4 * 1024;

enum class FileIoMode : uint8_t {
  kBuffered,
  kDirect,
};

inline bool IsValidFileIoMode(FileIoMode mode) {
  return mode == FileIoMode::kBuffered || mode == FileIoMode::kDirect;
}

inline uint64_t AlignFileIoSize(uint64_t size) {
  return ((size + kFileSegmentAlignment - 1) / kFileSegmentAlignment) *
      kFileSegmentAlignment;
}

inline bool IsFileIoAligned(uint64_t value) {
  return value % kFileSegmentAlignment == 0;
}

inline bool IsFileIoAlignedPtr(const void* ptr) {
  return reinterpret_cast<uintptr_t>(ptr) % kFileSegmentAlignment == 0;
}

enum class FileErrorCode : uint8_t {
  kOk,
  kInvalidConfig,
  kInvalidSize,
  kInvalidSegment,
  kDoubleFree,
  kTooManyOpenFiles,
  kIoError,
  kShutdown,
};

enum class FileSegmentKind : uint8_t {
  kBucket,
  kDedicated,
};

struct FileSegment {
  int fd{-1};
  uint64_t offset{0};
  uint64_t requested_size{0};
  uint64_t allocated_size{0};
  FileSegmentKind kind{FileSegmentKind::kBucket};
  uint64_t id{0};
};

struct FileAllocateResult {
  FileErrorCode error{FileErrorCode::kOk};
  int native_error_code{0};
  FileSegment segment;

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
