#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <filesystem>
#include <utility>

namespace bytedance::bolt::memory::bm {

FileBlockAllocatorImpl::FileBlockAllocatorImpl(FileBlockAllocatorConfig config)
    : config_(std::move(config)), dedicated_allocator_(config_.directory) {
  BOLT_CHECK(
      ValidateFileBlockAllocatorConfig(config_) == FileErrorCode::kOk,
      "invalid FileBlockAllocatorConfig");
  std::filesystem::remove_all(config_.directory);
  std::filesystem::create_directories(config_.directory);

  buckets_.reserve(config_.bucket_sizes.size());
  for (const auto bucket_size : config_.bucket_sizes) {
    buckets_.push_back(std::make_unique<BucketBlockAllocator>(
        config_.directory,
        static_cast<uint64_t>(bucket_size),
        static_cast<uint64_t>(config_.file_size_limit_bytes),
        config_.max_open_files_per_bucket));
  }
}

FileBlockAllocatorImpl::~FileBlockAllocatorImpl() {
  shutdown_ = true;
}

FileAllocateResult FileBlockAllocatorImpl::Allocate(int64_t size) {
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

  const auto it =
      std::lower_bound(config_.bucket_sizes.begin(), config_.bucket_sizes.end(), size);
  if (it == config_.bucket_sizes.end()) {
    return AllocateDedicated(size);
  }
  return AllocateBucket(
      size, static_cast<size_t>(it - config_.bucket_sizes.begin()));
}

FileFreeResult FileBlockAllocatorImpl::Free(const FileExtent& extent) {
  ExtentRecord record;
  const auto registry_error = registry_.Take(extent.id, &record);
  if (registry_error != FileErrorCode::kOk) {
    FileFreeResult result;
    result.error = registry_error;
    return result;
  }

  if (record.extent.kind == FileExtentKind::kDedicated) {
    return FreeDedicated(record);
  }
  return FreeBucket(record);
}

FileAllocateResult FileBlockAllocatorImpl::AllocateBucket(
    int64_t size,
    size_t bucket_index) {
  const auto extent_id = registry_.NextExtentId();
  auto allocation = buckets_[bucket_index]->Allocate(size, extent_id);
  if (!allocation.result.ok()) {
    return allocation.result;
  }
  allocation.record.bucket_index = bucket_index;
  registry_.Register(std::move(allocation.record));
  return allocation.result;
}

FileAllocateResult FileBlockAllocatorImpl::AllocateDedicated(int64_t size) {
  const auto extent_id = registry_.NextExtentId();
  auto allocation = dedicated_allocator_.Allocate(size, extent_id);
  if (!allocation.result.ok()) {
    return allocation.result;
  }
  registry_.Register(std::move(allocation.record));
  return allocation.result;
}

FileFreeResult FileBlockAllocatorImpl::FreeBucket(const ExtentRecord& record) {
  if (record.bucket_index >= buckets_.size()) {
    FileFreeResult result;
    result.error = FileErrorCode::kInvalidExtent;
    return result;
  }
  return buckets_[record.bucket_index]->Free(record);
}

FileFreeResult FileBlockAllocatorImpl::FreeDedicated(
    const ExtentRecord& record) {
  return dedicated_allocator_.Free(record);
}

} // namespace bytedance::bolt::memory::bm
