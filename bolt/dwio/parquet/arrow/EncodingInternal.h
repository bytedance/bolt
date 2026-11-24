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

#include <algorithm>
#include <cstdint>
#include <memory>

#include "bolt/dwio/parquet/arrow/LevelRun.h"

namespace arrow {

class ResizableBuffer;

} // namespace arrow

namespace bytedance::bolt::parquet::arrow {

// Internal helper for incrementally building def/rep level RLE streams into a
// growable buffer while preserving RLE state across batches in the same data
// page.
class IncrementalLevelRleEncoder {
 public:
  explicit IncrementalLevelRleEncoder(::arrow::ResizableBuffer* buffer);
  ~IncrementalLevelRleEncoder();

  IncrementalLevelRleEncoder(IncrementalLevelRleEncoder&&) noexcept;
  IncrementalLevelRleEncoder& operator=(IncrementalLevelRleEncoder&&) noexcept;

  IncrementalLevelRleEncoder(const IncrementalLevelRleEncoder&) = delete;
  IncrementalLevelRleEncoder& operator=(const IncrementalLevelRleEncoder&) =
      delete;

  void Reset(int16_t maxLevel, int prefixSize);
  void Put(const int16_t* levels, int64_t numLevels);
  // Equivalent to count scalar Puts; splits runs above INT32_MAX without
  // changing the surrounding page lifecycle. A zero count is a no-op.
  void PutRepeated(int16_t value, int64_t count);
  int32_t Flush();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// One cursor per stream and per synchronous write. Page resets do not rewind
// it.
class LevelRunCursor {
 public:
  explicit LevelRunCursor(LevelRunView runs) : runs_(runs) {}

  void Put(
      IncrementalLevelRleEncoder& encoder,
      const int16_t* levels,
      int64_t offset,
      int64_t count) {
    if (runs_.size == 0) {
      encoder.Put(levels, count);
      return;
    }
    const auto end = offset + count;
    auto position = offset;
    while (index_ < runs_.size && position < end) {
      const auto& run = runs_.data[index_];
      const auto runEnd = run.offset + run.count;
      if (runEnd <= position) {
        ++index_;
        continue;
      }
      if (run.offset >= end) {
        break;
      }
      if (position < run.offset) {
        encoder.Put(levels + (position - offset), run.offset - position);
        position = run.offset;
      }
      const auto length = std::min(end, runEnd) - position;
      encoder.PutRepeated(run.value, length);
      position += length;
      if (position == runEnd) {
        ++index_;
      }
    }
    if (position < end) {
      encoder.Put(levels + (position - offset), end - position);
    }
  }

 private:
  LevelRunView runs_;
  int64_t index_{0};
};

} // namespace bytedance::bolt::parquet::arrow
