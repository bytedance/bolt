#include "bolt/common/memory/bm/SpillIo.h"

#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

std::future<IoResult> SpillIo::SubmitReadRaw(
    const ManagedFileSegment& segment,
    size_t size,
    IoPriority priority) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = priority;
  request.fd = segment.segment().fd;
  request.fileOffset = segment.segment().offset;
  request.buffer = IoBuffer::allocateFromMalloc(size);

  return diskIoScheduler().submit(std::move(request));
}

std::future<IoResult> SpillIo::SubmitWriteRaw(
    const FileSegment& segment,
    IoBuffer& payload,
    IoPriority priority) {
  // This is intentionally outside BufferManager hot path. The current scheduler
  // facade initializes lazily; do it before moving the only payload owner into
  // IoRequest so initialization failure cannot destroy the resident payload.
  EnsureWriteSchedulerReady();

  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = priority;
  request.fd = segment.fd;
  request.fileOffset = segment.offset;
  request.buffer = std::move(payload);

  return diskIoScheduler().submit(std::move(request));
}

void SpillIo::EnsureWriteSchedulerReady() {
  if (writeSchedulerReady_) {
    return;
  }
  diskIoScheduler().ensureReady();
  writeSchedulerReady_ = true;
}

} // namespace bytedance::bolt::memory::bm
