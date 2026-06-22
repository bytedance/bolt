#pragma once

#include "bolt/common/memory/bm/file/FileSegmentAllocatorTypes.h"
#include "bolt/common/memory/bm/file/ManagedOpenFile.h"

#include <string>

namespace bytedance::bolt::memory::bm {

struct ManagedOpenFileCreateResult {
  FileErrorCode error{FileErrorCode::kOk};
  int native_error_code{0};
  ManagedOpenFile file;

  bool ok() const {
    return error == FileErrorCode::kOk;
  }
};

ManagedOpenFileCreateResult CreateExclusiveReadWriteManagedOpenFile(
    const std::string& path,
    FileIoMode ioMode = FileIoMode::kBuffered);

} // namespace bytedance::bolt::memory::bm
