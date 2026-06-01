#include "bolt/common/memory/bm/file/BucketBlockAllocator.h"

#include "bolt/common/memory/bm/file/FileAllocatorPath.h"
#include "bolt/common/memory/bm/file/OwnedFileFactory.h"

#include <algorithm>
#include <utility>

namespace bytedance::bolt::memory::bm {

BucketBlockAllocator::BucketBlockAllocator(
    std::string directory,
    uint64_t bucket_size,
    uint64_t file_size_limit_bytes,
    uint32_t max_open_files)
    : directory_(std::move(directory)),
      bucket_size_(bucket_size),
      file_size_limit_bytes_(file_size_limit_bytes),
      max_open_files_(max_open_files) {}

BucketBlockAllocator::~BucketBlockAllocator() {
  for (auto& file : files_) {
    file->file.CloseAndRemove();
  }
  files_.clear();
}

FileAllocation BucketBlockAllocator::Allocate(
    int64_t requested_size,
    uint64_t extent_id) {
  FileAllocation allocation;

  BucketFile* file = FindReusableFile();
  if (file == nullptr) {
    if (files_.size() >= max_open_files_) {
      allocation.result.error = FileErrorCode::kTooManyOpenFiles;
      return allocation;
    }
    auto created = CreateFile();
    if (!created.ok()) {
      allocation.result = created;
      return allocation;
    }
    file = files_.back().get();
  }

  uint64_t offset = 0;
  if (!file->free_offsets.empty()) {
    offset = file->free_offsets.back();
    file->free_offsets.pop_back();
  } else {
    offset = file->next_offset;
    file->next_offset += bucket_size_;
  }
  ++file->active_blocks;

  auto& extent = allocation.result.extent;
  extent.fd = file->file.fd();
  extent.offset = offset;
  extent.requested_size = static_cast<uint64_t>(requested_size);
  extent.allocated_size = bucket_size_;
  extent.kind = FileExtentKind::kBucket;
  extent.id = extent_id;

  allocation.record.extent = extent;
  allocation.record.file_index = file->file_index;
  return allocation;
}

FileFreeResult BucketBlockAllocator::Free(const ExtentRecord& record) {
  auto* file = FindFileByIndex(record.file_index);
  if (file == nullptr || file->active_blocks == 0) {
    FileFreeResult result;
    result.error = FileErrorCode::kInvalidExtent;
    return result;
  }

  file->free_offsets.push_back(record.extent.offset);
  --file->active_blocks;

  if (file->active_blocks == 0) {
    DeleteFile(record.file_index);
  }
  return FileFreeResult{};
}

BucketBlockAllocator::BucketFile* BucketBlockAllocator::FindReusableFile() {
  for (auto& file : files_) {
    if (!file->free_offsets.empty() ||
        file->next_offset + bucket_size_ <= file_size_limit_bytes_) {
      return file.get();
    }
  }
  return nullptr;
}

FileAllocateResult BucketBlockAllocator::CreateFile() {
  const auto file_index = next_file_index_++;
  const auto path = MakeBucketFilePath(directory_, bucket_size_, file_index);
  auto created = CreateExclusiveReadWriteOwnedFile(path);
  if (!created.ok()) {
    FileAllocateResult result;
    result.error = created.error;
    result.native_error_code = created.native_error_code;
    return result;
  }

  auto file = std::make_unique<BucketFile>();
  file->file_index = file_index;
  file->file = std::move(created.file);
  files_.push_back(std::move(file));
  return FileAllocateResult{};
}

BucketBlockAllocator::BucketFile*
BucketBlockAllocator::FindFileByIndex(uint64_t file_index) {
  for (auto& file : files_) {
    if (file->file_index == file_index) {
      return file.get();
    }
  }
  return nullptr;
}

void BucketBlockAllocator::DeleteFile(uint64_t file_index) {
  auto it = std::remove_if(
      files_.begin(),
      files_.end(),
      [&](const std::unique_ptr<BucketFile>& file) {
        if (file->file_index != file_index) {
          return false;
        }
        file->file.CloseAndRemove();
        return true;
      });
  files_.erase(it, files_.end());
}

} // namespace bytedance::bolt::memory::bm
