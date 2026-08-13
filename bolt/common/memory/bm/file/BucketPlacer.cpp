#include "bolt/common/memory/bm/file/BucketPlacer.h"

#include "bolt/common/memory/bm/file/ManagedOpenFileFactory.h"
#include "bolt/common/memory/bm/file/SegmentFilePath.h"

#include <algorithm>
#include <utility>

namespace bytedance::bolt::memory::bm {

BucketPlacer::BucketPlacer(
    std::string directory,
    uint64_t bucket_size,
    uint64_t file_size_limit_bytes,
    uint32_t max_open_files)
    : directory_(std::move(directory)),
      bucket_size_(bucket_size),
      file_size_limit_bytes_(file_size_limit_bytes),
      max_open_files_(max_open_files) {}

BucketPlacer::~BucketPlacer() {
  for (auto& file : files_) {
    file->file.CloseAndRemove();
  }
  files_.clear();
}

FileAllocation BucketPlacer::Allocate(
    int64_t requested_size,
    uint64_t segment_id) {
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
  if (!file->free_segment_offsets.empty()) {
    offset = file->free_segment_offsets.back();
    file->free_segment_offsets.pop_back();
  } else {
    offset = file->next_offset;
    file->next_offset += bucket_size_;
  }
  ++file->active_segments;

  auto& segment = allocation.result.segment;
  segment.fd = file->file.fd();
  segment.offset = offset;
  segment.requested_size = static_cast<uint64_t>(requested_size);
  segment.allocated_size = bucket_size_;
  segment.kind = FileSegmentKind::kBucket;
  segment.id = segment_id;

  allocation.record.segment = segment;
  allocation.record.file_index = file->file_index;
  return allocation;
}

FileFreeResult BucketPlacer::Free(const SegmentRecord& record) {
  auto* file = FindFileByIndex(record.file_index);
  if (file == nullptr || file->active_segments == 0) {
    FileFreeResult result;
    result.error = FileErrorCode::kInvalidSegment;
    return result;
  }

  file->free_segment_offsets.push_back(record.segment.offset);
  --file->active_segments;

  if (file->active_segments == 0) {
    DeleteFile(record.file_index);
  }
  return FileFreeResult{};
}

BucketPlacer::BucketFile* BucketPlacer::FindReusableFile() {
  for (auto& file : files_) {
    if (!file->free_segment_offsets.empty() ||
        file->next_offset + bucket_size_ <= file_size_limit_bytes_) {
      return file.get();
    }
  }
  return nullptr;
}

FileAllocateResult BucketPlacer::CreateFile() {
  const auto file_index = next_file_index_++;
  const auto path =
      MakeBucketSegmentFilePath(directory_, bucket_size_, file_index);
  auto created = CreateExclusiveReadWriteManagedOpenFile(path);
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

BucketPlacer::BucketFile* BucketPlacer::FindFileByIndex(uint64_t file_index) {
  for (auto& file : files_) {
    if (file->file_index == file_index) {
      return file.get();
    }
  }
  return nullptr;
}

void BucketPlacer::DeleteFile(uint64_t file_index) {
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
