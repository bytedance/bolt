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

#pragma once

#include "bolt/common/memory/bm/file/BucketPlacer.h"
#include "bolt/common/memory/bm/file/DedicatedPlacer.h"
#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"
#include "bolt/common/memory/bm/file/SegmentRegistry.h"

#include <memory>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

class FileSegmentAllocatorImpl : public FileSegmentAllocator {
 public:
  explicit FileSegmentAllocatorImpl(FileSegmentAllocatorConfig config);
  ~FileSegmentAllocatorImpl() override;

  FileSegmentAllocatorImpl(const FileSegmentAllocatorImpl&) = delete;
  FileSegmentAllocatorImpl& operator=(const FileSegmentAllocatorImpl&) = delete;

  FileAllocateResult Allocate(int64_t size) override;
  FileFreeResult Free(const FileSegment& segment) override;

 private:
  FileAllocateResult AllocateBucket(int64_t size, size_t bucket_index);
  FileAllocateResult AllocateDedicated(int64_t size);
  FileFreeResult FreeBucket(const SegmentRecord& record);
  FileFreeResult FreeDedicated(const SegmentRecord& record);

  FileSegmentAllocatorConfig config_;
  std::string allocator_id_;
  std::string directory_;
  std::vector<std::unique_ptr<BucketPlacer>> buckets_;
  DedicatedPlacer dedicated_placer_;
  SegmentRegistry registry_;
  bool shutdown_{false};
};

} // namespace bytedance::bolt::memory::bm
