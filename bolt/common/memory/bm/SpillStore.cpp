#include "bolt/common/memory/bm/SpillStore.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <memory>
#include <mutex>
#include <span>
#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

std::future<IoResult> SubmitReadRaw(
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

std::future<IoResult> SubmitWriteRaw(
    const FileSegment& segment,
    IoBuffer& payload,
    IoPriority priority) {
  // Keep scheduler initialization before moving the only payload owner into
  // IoRequest, so initialization failure cannot destroy resident payload.
  static std::once_flag schedulerReadyOnce;
  std::call_once(
      schedulerReadyOnce, [] { diskIoScheduler().ensureReady(); });

  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = priority;
  request.fd = segment.fd;
  request.fileOffset = segment.offset;
  request.buffer = std::move(payload);

  return diskIoScheduler().submit(std::move(request));
}

} // namespace

SpillWriteFuture::SpillWriteFuture(
    std::future<IoResult> rawFuture,
    ManagedFileSegment segment,
    SpillWriteMetadata metadata)
    : rawFuture_(std::move(rawFuture)),
      segment_(std::move(segment)),
      metadata_(metadata) {}

SpillWriteResult SpillWriteFuture::get() {
  SpillWriteResult result;
  result.io = rawFuture_.get();
  result.segment = std::move(segment_);
  result.rawBytes = metadata_.rawBytes;
  result.physicalBytes = metadata_.physicalBytes;
  result.compressionTimeUs = metadata_.compressionTimeUs;
  result.compressed = metadata_.compressed;
  return result;
}

SpillReadFuture::SpillReadFuture(
    std::future<IoResult> rawFuture,
    std::shared_ptr<compress::CompressionManager> compression,
    MemoryPool* pool,
    size_t expectedRawSize)
    : rawFuture_(std::move(rawFuture)),
      compression_(std::move(compression)),
      pool_(pool),
      expectedRawSize_(expectedRawSize) {
  BOLT_CHECK_NOT_NULL(compression_);
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

  auto decoded = compression_->DecodeSpillRecord(
      std::span<const char>(raw.buffer.data(), raw.buffer.length()),
      expectedRawSize_,
      pool_,
      &result.decompressionTimeUs);
  result.rawBytes = decoded.length();
  result.io.bytes = decoded.length();
  result.io.buffer = std::move(decoded);
  return result;
}

SpillStore::SpillStore(SpillStoreConfig config, MemoryPool* pool)
    : config_(std::move(config)),
      compression_(std::make_shared<compress::CompressionManager>(
          config_.compressionConfig)),
      pool_(pool) {
  allocator_ = CreateFileSegmentAllocator(config_.fileAllocatorConfig);
  BOLT_CHECK_NOT_NULL(allocator_);
  BOLT_CHECK_NOT_NULL(pool_);
}

SpillStore::~SpillStore() = default;

FileAllocateResult SpillStore::AllocateSegment(size_t size) {
  return allocator_->Allocate(static_cast<int64_t>(size));
}

FileFreeResult SpillStore::FreeSegment(const FileSegment& segment) {
  return allocator_->Free(segment);
}

ManagedFileSegment SpillStore::OwnSegment(FileSegment segment) const {
  return ManagedFileSegment{segment, allocator_};
}

SpillWriteFuture SpillStore::SubmitWriteBlock(
    IoBuffer& payload,
    size_t rawSize,
    IoPriority priority) {
  BOLT_CHECK(payload.valid());
  BOLT_CHECK_EQ(payload.length(), rawSize);

  auto record = compression_->BuildSpillRecord(
      std::span<const char>(payload.data(), payload.length()));

  auto allocation = AllocateSegment(record.physicalSize);
  if (!allocation.ok()) {
    BOLT_FAIL(
        "BM file allocation failed for spill record, file_error={}, native_error={}, bytes={}",
        static_cast<int>(allocation.error),
        allocation.native_error_code,
        record.physicalSize);
  }

  SpillWriteMetadata metadata;
  metadata.rawBytes = rawSize;
  metadata.physicalBytes = record.physicalSize;
  metadata.compressionTimeUs = record.compressionTimeUs;
  metadata.compressed = record.compressed;

  auto ownedSegment = OwnSegment(allocation.segment);
  auto rawFuture =
      SubmitWriteRaw(ownedSegment.segment(), record.record, priority);

  return SpillWriteFuture{
      std::move(rawFuture), std::move(ownedSegment), metadata};
}

SpillReadFuture SpillStore::SubmitReadBlock(
    const ManagedFileSegment& segment,
    size_t expectedRawSize,
    IoPriority priority) {
  return SpillReadFuture{
      SubmitReadRaw(segment, segment.segment().requested_size, priority),
      compression_,
      pool_,
      expectedRawSize};
}

} // namespace bytedance::bolt::memory::bm
