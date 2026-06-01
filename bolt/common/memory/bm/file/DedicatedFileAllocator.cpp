#include "bolt/common/memory/bm/file/DedicatedFileAllocator.h"

#include "bolt/common/memory/bm/file/FileAllocatorPath.h"
#include "bolt/common/memory/bm/file/OwnedFileFactory.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

DedicatedFileAllocator::DedicatedFileAllocator(std::string directory)
    : directory_(std::move(directory)) {}

DedicatedFileAllocator::~DedicatedFileAllocator() {
  RemoveAllFiles();
}

void DedicatedFileAllocator::RemoveAllFiles() {
  for (auto& [_, file] : files_) {
    file.CloseAndRemove();
  }
  files_.clear();
}

FileAllocation DedicatedFileAllocator::Allocate(
    int64_t requested_size,
    uint64_t extent_id) {
  FileAllocation allocation;
  const auto path = MakeDedicatedFilePath(directory_, extent_id);
  auto created = CreateExclusiveReadWriteOwnedFile(path);
  if (!created.ok()) {
    allocation.result.error = FileErrorCode::kIoError;
    allocation.result.native_error_code = created.native_error_code;
    return allocation;
  }

  FileExtent extent;
  extent.fd = created.file.fd();
  extent.offset = 0;
  extent.requested_size = static_cast<uint64_t>(requested_size);
  extent.allocated_size = static_cast<uint64_t>(requested_size);
  extent.kind = FileExtentKind::kDedicated;
  extent.id = extent_id;

  files_.emplace(extent_id, std::move(created.file));

  allocation.result.extent = extent;
  allocation.record.extent = extent;
  return allocation;
}

FileFreeResult DedicatedFileAllocator::Free(const ExtentRecord& record) {
  OwnedFile file;
  const auto it = files_.find(record.extent.id);
  if (it == files_.end()) {
    FileFreeResult result;
    result.error = FileErrorCode::kInvalidExtent;
    return result;
  }
  file = std::move(it->second);
  files_.erase(it);

  file.CloseAndRemove();
  return FileFreeResult{};
}

} // namespace bytedance::bolt::memory::bm
