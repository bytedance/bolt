#include "bolt/common/memory/bm/SpillStore.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <glog/logging.h>

#include <span>
#include <utility>

namespace bytedance::bolt::memory::bm {

namespace {

IoResult MakeInvalidRecordResult(IoBuffer buffer) {
  IoResult result;
  result.error = IoErrorCode::InvalidRequest;
  result.buffer = std::move(buffer);
  return result;
}

} // namespace

SpillReadFuture::SpillReadFuture(
    std::future<IoResult> rawFuture,
    MemoryPool* pool,
    compress::CompressionConfig compressionConfig,
    size_t expectedRawSize)
    : rawFuture_(std::move(rawFuture)),
      pool_(pool),
      compressionConfig_(std::move(compressionConfig)),
      expectedRawSize_(expectedRawSize) {
  BOLT_CHECK_NOT_NULL(pool_);
}

SpillReadResult SpillReadFuture::get() {
  auto raw = rawFuture_.get();
  SpillReadResult result;
  result.physicalBytes = raw.bytes;
  if (!raw.ok()) {
    result.io = std::move(raw);
    return result;
  }

  try {
    compress::CompressionManager compression(compressionConfig_);
    auto decoded = compression.DecodeSpillRecord(
        std::span<const char>(raw.buffer.data(), raw.buffer.length()),
        expectedRawSize_,
        pool_,
        &result.decompressionTimeUs);
    result.rawBytes = decoded.length();
    result.io.bytes = decoded.length();
    result.io.buffer = std::move(decoded);
    return result;
  } catch (...) {
    result.io = MakeInvalidRecordResult(std::move(raw.buffer));
    return result;
  }
}

SpillStore::SpillStore(SpillStoreConfig config, MemoryPool* pool)
    : config_(std::move(config)),
      compression_(config_.compressionConfig),
      pool_(pool) {
  allocator_ = CreateFileBlockAllocator(config_.fileAllocatorConfig);
  BOLT_CHECK_NOT_NULL(allocator_);
  BOLT_CHECK_NOT_NULL(pool_);
}

FileAllocateResult SpillStore::AllocateExtent(size_t size) {
  return allocator_->Allocate(static_cast<int64_t>(size));
}

FileFreeResult SpillStore::FreeExtent(const FileExtent& extent) {
  return allocator_->Free(extent);
}

OwnedFileExtent SpillStore::OwnExtent(FileExtent extent) const {
  return OwnedFileExtent{extent, allocator_};
}

std::future<IoResult> SpillStore::SubmitReadRaw(
    const OwnedFileExtent& extent,
    size_t size,
    IoPriority priority) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = priority;
  request.fd = extent.extent().fd;
  request.fileOffset = extent.extent().offset;
  request.buffer = IoBuffer::allocateFromMalloc(size);

  return diskIoScheduler().submit(std::move(request));
}

IoResult SpillStore::WriteRaw(
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

SpillWriteResult SpillStore::WriteBlock(
    IoBuffer& payload,
    size_t rawSize,
    IoPriority priority) {
  BOLT_CHECK(payload.valid());
  BOLT_CHECK_EQ(payload.length(), rawSize);

  auto record = compression_.BuildSpillRecord(
      std::span<const char>(payload.data(), payload.length()));

  auto allocation = AllocateExtent(record.physicalSize);
  if (!allocation.ok()) {
    BOLT_FAIL(
        "BM file allocation failed for spill record, file_error={}, native_error={}, bytes={}",
        static_cast<int>(allocation.error),
        allocation.native_error_code,
        record.physicalSize);
  }

  SpillWriteResult write;
  write.rawBytes = rawSize;
  write.physicalBytes = record.physicalSize;
  write.compressionTimeUs = record.compressionTimeUs;
  write.compressed = record.compressed;
  try {
    write.io = WriteRaw(allocation.extent, record.record, priority);
  } catch (...) {
    auto freeResult = FreeExtent(allocation.extent);
    if (!freeResult.ok()) {
      LOG(FATAL)
          << "BM failed to free extent after spill write exception, extent_id="
          << allocation.extent.id
          << ", file_error=" << static_cast<int>(freeResult.error)
          << ", native_error=" << freeResult.native_error_code;
    }
    throw;
  }

  if (!write.io.ok()) {
    auto freeResult = FreeExtent(allocation.extent);
    if (!freeResult.ok()) {
      LOG(FATAL)
          << "BM failed to free extent after failed spill write, extent_id="
          << allocation.extent.id
          << ", file_error=" << static_cast<int>(freeResult.error)
          << ", native_error=" << freeResult.native_error_code;
    }
    return write;
  }

  write.extent = OwnExtent(allocation.extent);
  return write;
}

SpillReadFuture SpillStore::SubmitReadBlock(
    const OwnedFileExtent& extent,
    size_t expectedRawSize,
    IoPriority priority) {
  return SpillReadFuture{
      SubmitReadRaw(extent, extent.extent().requested_size, priority),
      pool_,
      config_.compressionConfig,
      expectedRawSize};
}

void SpillStore::EnsureSchedulerReadyForPayloadMove() {
  if (schedulerReadyForPayloadMove_) {
    return;
  }
  (void)diskIoScheduler().stats();
  schedulerReadyForPayloadMove_ = true;
}

} // namespace bytedance::bolt::memory::bm
