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
#include <vector>

namespace bytedance::bolt::lance::reader {

struct NativeLancePageSpan {
  uint32_t physicalColumn;
  int32_t pageIndex;
  uint64_t pageRowBegin;
  uint64_t localRowBegin;
  uint64_t rowCount;
};

/// Maps physical row ranges to page-local spans.
class NativeLanceColumnCursor {
 public:
  NativeLanceColumnCursor(
      uint32_t physicalColumn,
      const std::vector<uint64_t>& pageRowStarts);

  std::vector<NativeLancePageSpan> spans(uint64_t rowStart, uint64_t rowCount)
      const;

 private:
  uint32_t physicalColumn_;
  const std::vector<uint64_t>* pageRowStarts_;
};

} // namespace bytedance::bolt::lance::reader
