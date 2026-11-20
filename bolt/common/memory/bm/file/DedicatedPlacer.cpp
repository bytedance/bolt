/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
