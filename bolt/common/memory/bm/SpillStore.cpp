#include "bolt/common/memory/bm/SpillStore.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/SpillCodec.h"
#include "bolt/common/memory/bm/SpillIo.h"

#include <glog/logging.h>

#include <memory>
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

SpillWriteFuture::SpillWriteFuture(
    std::future<IoResult> rawFuture,
    OwnedFileExtent extent,
    SpillWriteMetadata metadata)
    : rawFuture_(std::move(rawFuture)),
      extent_(std::move(extent)),
      metadata_(metadata) {}

SpillWriteResult SpillWriteFuture::get() {
  SpillWriteResult result;
  result.io = rawFuture_.get();
  result.extent = std::move(extent_);
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

  try {
    auto decoded = codec_->Decode(
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
      codec_(std::make_shared<SpillCodec>(config_.compressionConfig)),
      io_(std::make_unique<SpillIo>()),
      pool_(pool) {
  allocator_ = CreateFileBlockAllocator(config_.fileAllocatorConfig);
  BOLT_CHECK_NOT_NULL(allocator_);
  BOLT_CHECK_NOT_NULL(pool_);
}

SpillStore::~SpillStore() = default;

FileAllocateResult SpillStore::AllocateExtent(size_t size) {
  return allocator_->Allocate(static_cast<int64_t>(size));
}

FileFreeResult SpillStore::FreeExtent(const FileExtent& extent) {
  return allocator_->Free(extent);
}

OwnedFileExtent SpillStore::OwnExtent(FileExtent extent) const {
  return OwnedFileExtent{extent, allocator_};
}

SpillWriteFuture SpillStore::SubmitWriteBlock(
    IoBuffer& payload,
    size_t rawSize,
    IoPriority priority) {
  BOLT_CHECK(payload.valid());
  BOLT_CHECK_EQ(payload.length(), rawSize);

  auto record =
      codec_->Build(std::span<const char>(payload.data(), payload.length()));

  auto allocation = AllocateExtent(record.physicalSize);
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

  std::future<IoResult> rawFuture;
  try {
    rawFuture = io_->SubmitWriteRaw(allocation.extent, record.record, priority);
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

  return SpillWriteFuture{
      std::move(rawFuture), OwnExtent(allocation.extent), metadata};
}

SpillReadFuture SpillStore::SubmitReadBlock(
    const OwnedFileExtent& extent,
    size_t expectedRawSize,
    IoPriority priority) {
  return SpillReadFuture{
      io_->SubmitReadRaw(extent, extent.extent().requested_size, priority),
      codec_,
      pool_,
      expectedRawSize};
}

} // namespace bytedance::bolt::memory::bm
