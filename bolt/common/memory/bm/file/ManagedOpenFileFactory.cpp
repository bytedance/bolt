#include "bolt/common/memory/bm/file/ManagedOpenFileFactory.h"

#include <cerrno>

#include <fcntl.h>

namespace bytedance::bolt::memory::bm {

ManagedOpenFileCreateResult CreateExclusiveReadWriteManagedOpenFile(
    const std::string& path) {
  // TODO: Consider supporting O_DIRECT for BM spill files. Direct I/O can avoid
  // page cache copy and cache pollution for large temporary spill reads/writes,
  // which is attractive because BufferManager already owns the resident memory
  // lifecycle. It is not enabled here yet because O_DIRECT applies to the whole
  // file descriptor and requires buffer address, file offset, and I/O length
  // alignment. Small or compressed records may need padding and extra copies,
  // read-after-write workloads may benefit from page cache, and unsupported
  // filesystems need a buffered-I/O fallback path.
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
