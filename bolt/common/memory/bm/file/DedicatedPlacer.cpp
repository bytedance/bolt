#include "bolt/common/memory/bm/file/DedicatedPlacer.h"

#include "bolt/common/memory/bm/file/ManagedOpenFileFactory.h"
#include "bolt/common/memory/bm/file/SegmentFilePath.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

DedicatedPlacer::DedicatedPlacer(std::string directory)
    : directory_(std::move(directory)) {}

DedicatedPlacer::~DedicatedPlacer() {
  RemoveAllFiles();
}

void DedicatedPlacer::RemoveAllFiles() {
  for (auto& [_, file] : files_) {
    file.CloseAndRemove();
  }
  files_.clear();
}

FileAllocation DedicatedPlacer::Allocate(
    int64_t requested_size,
    uint64_t segment_id) {
  FileAllocation allocation;
  const auto path = MakeDedicatedSegmentFilePath(directory_, segment_id);
  auto created = CreateExclusiveReadWriteManagedOpenFile(path);
  if (!created.ok()) {
    allocation.result.error = FileErrorCode::kIoError;
    allocation.result.native_error_code = created.native_error_code;
    return allocation;
  }

  FileSegment segment;
  segment.fd = created.file.fd();
  segment.offset = 0;
  segment.requested_size = static_cast<uint64_t>(requested_size);
  segment.allocated_size = static_cast<uint64_t>(requested_size);
  segment.kind = FileSegmentKind::kDedicated;
  segment.id = segment_id;

  files_.emplace(segment_id, std::move(created.file));

  allocation.result.segment = segment;
  allocation.record.segment = segment;
  return allocation;
}

FileFreeResult DedicatedPlacer::Free(const SegmentRecord& record) {
  ManagedOpenFile file;
  const auto it = files_.find(record.segment.id);
  if (it == files_.end()) {
    FileFreeResult result;
    result.error = FileErrorCode::kInvalidSegment;
    return result;
  }
  file = std::move(it->second);
  files_.erase(it);

  file.CloseAndRemove();
  return FileFreeResult{};
}

} // namespace bytedance::bolt::memory::bm
