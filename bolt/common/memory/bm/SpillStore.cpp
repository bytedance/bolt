#include "bolt/common/memory/bm/SpillStore.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/SpillCodec.h"
#include "bolt/common/memory/bm/SpillIo.h"

#include <memory>
#include <span>
#include <utility>

namespace bytedance::bolt::memory::bm {

SpillWriteFuture::SpillWriteFuture(
    std::future<IoResult> rawFuture,
    OwnedFileSegment segment,
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
    std::shared_ptr<SpillCodec> codec,
    MemoryPool* pool,
    size_t expectedRawSize)
    : rawFuture_(std::move(rawFuture)),
      codec_(std::move(codec)),
      pool_(pool),
      expectedRawSize_(expectedRawSize) {
  BOLT_CHECK_NOT_NULL(codec_);
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

  auto decoded = codec_->Decode(
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
      codec_(std::make_shared<SpillCodec>(config_.compressionConfig)),
      io_(std::make_unique<SpillIo>()),
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

OwnedFileSegment SpillStore::OwnSegment(FileSegment segment) const {
  return OwnedFileSegment{segment, allocator_};
}

SpillWriteFuture SpillStore::SubmitWriteBlock(
    IoBuffer& payload,
    size_t rawSize,
    IoPriority priority) {
  BOLT_CHECK(payload.valid());
  BOLT_CHECK_EQ(payload.length(), rawSize);

  auto record =
      codec_->Build(std::span<const char>(payload.data(), payload.length()));

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
      io_->SubmitWriteRaw(ownedSegment.segment(), record.record, priority);

  return SpillWriteFuture{
      std::move(rawFuture), std::move(ownedSegment), metadata};
}

SpillReadFuture SpillStore::SubmitReadBlock(
    const OwnedFileSegment& segment,
    size_t expectedRawSize,
    IoPriority priority) {
  return SpillReadFuture{
      io_->SubmitReadRaw(segment, segment.segment().requested_size, priority),
      codec_,
      pool_,
      expectedRawSize};
}

} // namespace bytedance::bolt::memory::bm
