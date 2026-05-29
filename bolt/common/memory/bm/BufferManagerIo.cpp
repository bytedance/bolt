#include "bolt/common/memory/bm/BufferManagerIo.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

BufferManagerIo::BufferManagerIo(
    std::shared_ptr<FileBlockAllocator> allocator,
    MemoryPool* pool)
    : allocator_(std::move(allocator)), pool_(pool) {
  BOLT_CHECK_NOT_NULL(allocator_);
  BOLT_CHECK_NOT_NULL(pool_);
}

FileAllocateResult BufferManagerIo::AllocateExtent(size_t size) {
  return allocator_->Allocate(static_cast<int64_t>(size));
}

FileFreeResult BufferManagerIo::FreeExtent(const FileExtent& extent) {
  return allocator_->Free(extent);
}

OwnedFileExtent BufferManagerIo::OwnExtent(FileExtent extent) const {
  return OwnedFileExtent{extent, allocator_};
}

std::future<IoResult> BufferManagerIo::SubmitRead(
    const OwnedFileExtent& extent,
    size_t size,
    IoPriority priority) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = priority;
  request.fd = extent.extent().fd;
  request.fileOffset = extent.extent().offset;
  request.buffer = IoBuffer::allocateFromPool(pool_, size);

  return diskIoScheduler().submit(std::move(request));
}

IoResult BufferManagerIo::Write(
    const FileExtent& extent,
    IoBuffer& payload,
    IoPriority priority) {
  // This is intentionally outside BufferManager hot path. The current scheduler
  // facade initializes lazily; do it before moving the only payload owner into
  // IoRequest so initialization failure cannot destroy the resident payload.
  EnsureSchedulerReadyForPayloadMove();

  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = priority;
  request.fd = extent.fd;
  request.fileOffset = extent.offset;
  request.buffer = std::move(payload);

  return diskIoScheduler().submit(std::move(request)).get();
}

void BufferManagerIo::EnsureSchedulerReadyForPayloadMove() {
  if (schedulerReadyForPayloadMove_) {
    return;
  }
  (void)diskIoScheduler().stats();
  schedulerReadyForPayloadMove_ = true;
}

} // namespace bytedance::bolt::memory::bm
