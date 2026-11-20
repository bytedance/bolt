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

#include <cstddef>
#include <cstdint>
#include <unordered_map>

#include "bolt/common/memory/bm/file/FileSegmentAllocatorTypes.h"

namespace bytedance::bolt::memory::bm {

struct SegmentRecord {
  FileSegment segment;
  size_t bucket_index{0};
  uint64_t file_index{0};
};

struct FileAllocation {
  FileAllocateResult result;
  SegmentRecord record;
};

class SegmentRegistry {
 public:
  uint64_t NextSegmentId();
  void Register(SegmentRecord record);
  FileErrorCode Take(uint64_t segment_id, SegmentRecord* record);

 private:
  uint64_t next_segment_id_{1};
  std::unordered_map<uint64_t, SegmentRecord> records_;
};

} // namespace bytedance::bolt::memory::bm
