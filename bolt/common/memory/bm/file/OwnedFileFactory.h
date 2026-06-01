#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorTypes.h"
#include "bolt/common/memory/bm/file/OwnedFile.h"

#include <string>

namespace bytedance::bolt::memory::bm {

struct OwnedFileCreateResult {
  FileErrorCode error{FileErrorCode::kOk};
  int native_error_code{0};
  OwnedFile file;

  bool ok() const {
    return error == FileErrorCode::kOk;
  }
};

OwnedFileCreateResult CreateExclusiveReadWriteOwnedFile(
    const std::string& path);

} // namespace bytedance::bolt::memory::bm
