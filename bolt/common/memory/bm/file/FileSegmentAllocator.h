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

#include "bolt/common/memory/bm/file/FileSegmentAllocatorConfig.h"

#include <memory>

namespace bytedance::bolt::memory::bm {

class FileSegmentAllocator {
 public:
  virtual ~FileSegmentAllocator() = default;

  // Not thread-safe. Callers must serialize Allocate() and Free() on the same
  // allocator instance.
  virtual FileAllocateResult Allocate(int64_t size) = 0;
  virtual FileFreeResult Free(const FileSegment& segment) = 0;
};

std::shared_ptr<FileSegmentAllocator> CreateFileSegmentAllocator(
    FileSegmentAllocatorConfig config);

} // namespace bytedance::bolt::memory::bm
