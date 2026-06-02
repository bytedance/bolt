#include "bolt/common/memory/bm/file/FileSegmentAllocatorImpl.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Uuid.h"

#include <algorithm>
#include <filesystem>
#include <utility>

namespace bytedance::bolt::memory::bm {

FileSegmentAllocatorImpl::FileSegmentAllocatorImpl(
    FileSegmentAllocatorConfig config)
    : config_(std::move(config)),
      allocator_id_(bytedance::bolt::makeUuid()),
      directory_(
          (std::filesystem::path(config_.directory) / allocator_id_).string()),
      dedicated_placer_(directory_) {
  BOLT_CHECK(
      ValidateFileSegmentAllocatorConfig(config_) == FileErrorCode::kOk,
      "invalid FileSegmentAllocatorConfig");
  std::filesystem::create_directories(config_.directory);
  std::filesystem::create_directories(directory_);

  buckets_.reserve(config_.bucket_sizes.size());
  for (const auto bucket_size : config_.bucket_sizes) {
    buckets_.push_back(std::make_unique<BucketPlacer>(
        directory_,
        static_cast<uint64_t>(bucket_size),
        static_cast<uint64_t>(config_.file_size_limit_bytes),
        config_.max_open_files_per_bucket));
  }
}

FileSegmentAllocatorImpl::~FileSegmentAllocatorImpl() {
  shutdown_ = true;
  buckets_.clear();
  dedicated_placer_.RemoveAllFiles();
  std::filesystem::remove_all(directory_);
}

FileAllocateResult FileSegmentAllocatorImpl::Allocate(int64_t size) {
  if (shutdown_) {
    FileAllocateResult result;
    result.error = FileErrorCode::kShutdown;
    return result;
  }
  if (size <= 0) {
    FileAllocateResult result;
    result.error = FileErrorCode::kInvalidSize;
    return result;
  }

  const auto it = std::lower_bound(
      config_.bucket_sizes.begin(), config_.bucket_sizes.end(), size);
  if (it == config_.bucket_sizes.end()) {
    return AllocateDedicated(size);
  }
  return AllocateBucket(
      size, static_cast<size_t>(it - config_.bucket_sizes.begin()));
}

FileFreeResult FileSegmentAllocatorImpl::Free(const FileSegment& segment) {
  SegmentRecord record;
  const auto registry_error = registry_.Take(segment.id, &record);
  if (registry_error != FileErrorCode::kOk) {
    FileFreeResult result;
    result.error = registry_error;
    return result;
  }

  if (record.segment.kind == FileSegmentKind::kDedicated) {
    return FreeDedicated(record);
  }
  return FreeBucket(record);
}

FileAllocateResult FileSegmentAllocatorImpl::AllocateBucket(
    int64_t size,
    size_t bucket_index) {
  const auto segment_id = registry_.NextSegmentId();
  auto allocation = buckets_[bucket_index]->Allocate(size, segment_id);
  if (!allocation.result.ok()) {
    return allocation.result;
  }
  allocation.record.bucket_index = bucket_index;
  registry_.Register(std::move(allocation.record));
  return allocation.result;
}

FileAllocateResult FileSegmentAllocatorImpl::AllocateDedicated(int64_t size) {
  const auto segment_id = registry_.NextSegmentId();
  auto allocation = dedicated_placer_.Allocate(size, segment_id);
  if (!allocation.result.ok()) {
    return allocation.result;
  }
  registry_.Register(std::move(allocation.record));
  return allocation.result;
}

FileFreeResult FileSegmentAllocatorImpl::FreeBucket(
    const SegmentRecord& record) {
  if (record.bucket_index >= buckets_.size()) {
    FileFreeResult result;
    result.error = FileErrorCode::kInvalidSegment;
    return result;
  }
  return buckets_[record.bucket_index]->Free(record);
}

FileFreeResult FileSegmentAllocatorImpl::FreeDedicated(
    const SegmentRecord& record) {
  return dedicated_placer_.Free(record);
}

} // namespace bytedance::bolt::memory::bm
