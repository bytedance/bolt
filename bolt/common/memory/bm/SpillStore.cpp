#include "bolt/common/memory/bm/SpillStore.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <memory>
#include <cstring>
#include <mutex>
#include <span>
#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

std::future<IoResult> SubmitReadRaw(
    const ManagedFileSegment& segment,
    size_t size,
    IoPriority priority,
    FileIoMode ioMode) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = priority;
  request.fd = segment.segment().fd;
  request.fileOffset = segment.segment().offset;
  // TODO: This allocates a fresh malloc-backed read buffer for every spill
  // read. Consider a malloc-backed reusable buffer pool here, but do not
  // allocate from MemoryPool because spill reads can happen while MemoryPool is
  // under pressure and pool allocation may recurse back into spill.
  request.buffer = ioMode == FileIoMode::kDirect
      ? IoBuffer::allocateAlignedFromMalloc(size, kFileSegmentAlignment)
      : IoBuffer::allocateFromMalloc(size);

  if (ioMode == FileIoMode::kDirect) {
    BOLT_CHECK(IsFileIoAligned(request.fileOffset));
    BOLT_CHECK(IsFileIoAligned(request.buffer.length()));
    BOLT_CHECK(IsFileIoAlignedPtr(request.buffer.ioData()));
  }

  return diskIoScheduler().submit(std::move(request));
}

std::future<IoResult> SubmitWriteRaw(
    const FileSegment& segment,
    IoBuffer& payload,
    IoPriority priority,
    FileIoMode ioMode) {
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

  if (ioMode == FileIoMode::kDirect) {
    BOLT_CHECK(IsFileIoAligned(request.fileOffset));
    BOLT_CHECK(IsFileIoAligned(request.buffer.length()));
    BOLT_CHECK(IsFileIoAlignedPtr(request.buffer.ioData()));
  }

  return diskIoScheduler().submit(std::move(request));
}

size_t SpillIoSize(size_t recordSize, FileIoMode ioMode) {
  return ioMode == FileIoMode::kDirect ? AlignFileIoSize(recordSize)
                                       : recordSize;
}

IoBuffer PrepareWriteBuffer(IoBuffer record, size_t ioSize, FileIoMode ioMode) {
  if (ioMode == FileIoMode::kBuffered) {
    return record;
  }

  BOLT_CHECK_GE(ioSize, record.length());
  auto aligned =
      IoBuffer::allocateAlignedFromMalloc(ioSize, kFileSegmentAlignment);
  std::memcpy(aligned.data(), record.data(), record.length());
  if (ioSize > record.length()) {
    std::memset(aligned.data() + record.length(), 0, ioSize - record.length());
  }
  aligned.setLength(ioSize);
  return aligned;
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
  result.ioBytes = metadata_.ioBytes;
  result.compressionTimeUs = metadata_.compressionTimeUs;
  result.compressed = metadata_.compressed;
  return result;
}

SpillReadFuture::SpillReadFuture(
    std::future<IoResult> rawFuture,
    std::shared_ptr<compress::CompressionManager> compression,
    MemoryPool* pool,
    size_t expectedRawSize)
    : SpillReadFuture(
          std::move(rawFuture),
          std::move(compression),
          pool,
          0,
          expectedRawSize) {}

SpillReadFuture::SpillReadFuture(
    std::future<IoResult> rawFuture,
    std::shared_ptr<compress::CompressionManager> compression,
    MemoryPool* pool,
    size_t recordSize,
    size_t expectedRawSize)
    : rawFuture_(std::move(rawFuture)),
      compression_(std::move(compression)),
      pool_(pool),
      recordSize_(recordSize),
      expectedRawSize_(expectedRawSize) {
  BOLT_CHECK_NOT_NULL(compression_);
  BOLT_CHECK_NOT_NULL(pool_);
}

SpillReadResult SpillReadFuture::get() {
  auto raw = rawFuture_.get();
  SpillReadResult result;
  result.ioBytes = raw.bytes;
  if (!raw.ok()) {
    result.physicalBytes = raw.bytes;
    result.io = std::move(raw);
    return result;
  }
  const auto recordSize =
      recordSize_ == 0 ? raw.buffer.length() : recordSize_;
  result.physicalBytes = recordSize;

  auto decoded = compression_->DecodeSpillRecord(
      std::span<const char>(raw.buffer.data(), recordSize),
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
  const auto ioSize =
      SpillIoSize(record.physicalSize, config_.fileAllocatorConfig.ioMode);

  auto allocation = allocator_->Allocate(
      static_cast<int64_t>(record.physicalSize), static_cast<int64_t>(ioSize));
  if (!allocation.ok()) {
    BOLT_FAIL(
        "BM file allocation failed for spill record, file_error={}, native_error={}, bytes={}",
        static_cast<int>(allocation.error),
        allocation.native_error_code,
        ioSize);
  }

  SpillWriteMetadata metadata;
  metadata.rawBytes = rawSize;
  metadata.physicalBytes = record.physicalSize;
  metadata.ioBytes = ioSize;
  metadata.compressionTimeUs = record.compressionTimeUs;
  metadata.compressed = record.compressed;

  auto ownedSegment = OwnSegment(allocation.segment);
  auto writeBuffer = PrepareWriteBuffer(
      std::move(record.record), ioSize, config_.fileAllocatorConfig.ioMode);
  auto rawFuture =
      SubmitWriteRaw(
          ownedSegment.segment(),
          writeBuffer,
          priority,
          config_.fileAllocatorConfig.ioMode);

  return SpillWriteFuture{
      std::move(rawFuture), std::move(ownedSegment), metadata};
}

SpillReadFuture SpillStore::SubmitReadBlock(
    const ManagedFileSegment& segment,
    size_t expectedRawSize,
    IoPriority priority) {
  return SpillReadFuture{
      SubmitReadRaw(
          segment,
          SpillIoSize(
              segment.segment().requested_size,
              config_.fileAllocatorConfig.ioMode),
          priority,
          config_.fileAllocatorConfig.ioMode),
      compression_,
      pool_,
      segment.segment().requested_size,
      expectedRawSize};
}

} // namespace bytedance::bolt::memory::bm
