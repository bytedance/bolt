#include "bolt/common/memory/bm/SpillStore.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <glog/logging.h>

#include <cstring>
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
    size_t expectedRawSize)
    : rawFuture_(std::move(rawFuture)),
      pool_(pool),
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
    const auto header = compress::DecodeSpillRecordHeader(
        raw.buffer.data(), raw.buffer.length(), expectedRawSize_);
    result.rawBytes = header.rawSize;
    auto storedPayload = IoBuffer::allocateFromPool(pool_, header.storedSize);
    std::memcpy(
        storedPayload.data(),
        raw.buffer.data() + header.headerSize,
        header.storedSize);
    auto decoded = compress::Decompress(
        std::move(storedPayload),
        header.rawSize,
        header.storedSize,
        static_cast<compress::CompressionKind>(header.compressionKind),
        pool_,
        &result.decompressionTimeUs);
    result.io.bytes = header.rawSize;
    result.io.buffer = std::move(decoded);
    return result;
  } catch (...) {
    result.io = MakeInvalidRecordResult(std::move(raw.buffer));
    return result;
  }
}

SpillStore::SpillStore(SpillStoreConfig config, MemoryPool* pool)
    : config_(std::move(config)), pool_(pool) {
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
  request.buffer = IoBuffer::allocateFromPool(pool_, size);

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

  auto payloadCopy = IoBuffer::allocateFromPool(pool_, rawSize);
  std::memcpy(payloadCopy.data(), payload.data(), rawSize);
  auto compressed = compressionCodec_.TryCompress(
      std::move(payloadCopy), config_.compressionConfig, pool_);

  compress::SpillRecordHeader header;
  header.compressionKind = static_cast<uint32_t>(compressed.storedKind);
  header.rawSize = compressed.rawSize;
  header.storedSize = compressed.storedSize;
  const auto encodedHeader = compress::EncodeSpillRecordHeader(header);
  const auto recordSize = encodedHeader.size() + compressed.storedSize;
  auto record = IoBuffer::allocateFromPool(pool_, recordSize);
  std::memcpy(record.data(), encodedHeader.data(), encodedHeader.size());
  std::memcpy(
      record.data() + encodedHeader.size(),
      compressed.buffer.data(),
      compressed.storedSize);

  auto allocation = AllocateExtent(recordSize);
  if (!allocation.ok()) {
    BOLT_FAIL(
        "BM file allocation failed for spill record, file_error={}, native_error={}, bytes={}",
        static_cast<int>(allocation.error),
        allocation.native_error_code,
        recordSize);
  }

  SpillWriteResult write;
  write.rawBytes = rawSize;
  write.physicalBytes = recordSize;
  write.compressionTimeUs = compressed.compressionTimeUs;
  write.compressed = compressed.compressed;
  try {
    write.io = WriteRaw(allocation.extent, record, priority);
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
