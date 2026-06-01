#include "bolt/common/memory/bm/file/OwnedFileFactory.h"

#include <cerrno>

#include <fcntl.h>

namespace bytedance::bolt::memory::bm {

OwnedFileCreateResult CreateExclusiveReadWriteOwnedFile(
    const std::string& path) {
  const int fd =
      ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    OwnedFileCreateResult result;
    result.error = FileErrorCode::kIoError;
    result.native_error_code = errno;
    return result;
  }

  OwnedFileCreateResult result;
  result.file = OwnedFile(path, fd);
  return result;
}

} // namespace bytedance::bolt::memory::bm
