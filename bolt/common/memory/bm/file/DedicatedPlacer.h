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

#include <cstdint>
#include <string>
#include <unordered_map>

#include "bolt/common/memory/bm/file/FileSegmentAllocatorTypes.h"
#include "bolt/common/memory/bm/file/ManagedOpenFile.h"
#include "bolt/common/memory/bm/file/SegmentRegistry.h"

namespace bytedance::bolt::memory::bm {

class DedicatedPlacer {
 public:
  explicit DedicatedPlacer(std::string directory);
  ~DedicatedPlacer();

  DedicatedPlacer(const DedicatedPlacer&) = delete;
  DedicatedPlacer& operator=(const DedicatedPlacer&) = delete;

  FileAllocation Allocate(int64_t requested_size, uint64_t segment_id);
  FileFreeResult Free(const SegmentRecord& record);
  void RemoveAllFiles();

 private:
  const std::string directory_;
  std::unordered_map<uint64_t, ManagedOpenFile> files_;
};

} // namespace bytedance::bolt::memory::bm
