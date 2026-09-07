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

#include "bolt/common/memory/bm/file/SegmentFilePath.h"

#include <string>

namespace bytedance::bolt::memory::bm {

std::string MakeBucketSegmentFilePath(
    const std::string& directory,
    uint64_t bucket_size,
    uint64_t file_index) {
  return directory + "/bucket_" + std::to_string(bucket_size) + "_" +
      std::to_string(file_index) + ".bm";
}

std::string MakeDedicatedSegmentFilePath(
    const std::string& directory,
    uint64_t segment_id) {
  return directory + "/dedicated_" + std::to_string(segment_id) + ".bm";
}

} // namespace bytedance::bolt::memory::bm
