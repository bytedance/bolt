#include "bolt/common/memory/bm/file/ManagedOpenFileFactory.h"

#include <cerrno>

#include <fcntl.h>

namespace bytedance::bolt::memory::bm {

ManagedOpenFileCreateResult CreateExclusiveReadWriteManagedOpenFile(
    const std::string& path) {
  const int fd =
      ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    ManagedOpenFileCreateResult result;
    result.error = FileErrorCode::kIoError;
    result.native_error_code = errno;
    return result;
  }

  ManagedOpenFileCreateResult result;
  result.file = ManagedOpenFile(path, fd);
  return result;
}

} // namespace bytedance::bolt::memory::bm
