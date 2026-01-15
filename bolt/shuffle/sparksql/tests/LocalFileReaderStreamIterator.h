/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <vector>
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"

namespace bytedance::bolt::shuffle::sparksql::test {

struct SegmentInfo {
  std::string filename;
  int64_t offset;
  int64_t length;
};

class LocalFileReaderStreamIterator : public ReaderStreamIterator {
 public:
  explicit LocalFileReaderStreamIterator(std::vector<SegmentInfo> segments)
      : segments_(std::move(segments)), current_(0) {}

  std::shared_ptr<arrow::io::InputStream> nextStream(
      arrow::MemoryPool* pool) override {
    if (current_ >= segments_.size()) {
      return nullptr;
    }
    const auto& seg = segments_[current_++];
    auto file_res = arrow::io::ReadableFile::Open(seg.filename);
    if (!file_res.ok()) {
      BOLT_FAIL("Failed to open file: " + seg.filename);
      return nullptr;
    }
    auto file = *file_res;

    // We need a way to limit the stream to 'length'.
    // Arrow has RandomAccessFile::GetStream(file, offset, length).
    auto stream_res =
        arrow::io::RandomAccessFile::GetStream(file, seg.offset, seg.length);
    if (!stream_res.ok()) {
      return nullptr;
    }
    return *stream_res;
  }

  void close() override {
    // nothing to do
  }

  void updateMetrics(
      int64_t numRows,
      int64_t numBatches,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t totalReadTime) override {
    // no-op for test
  }

 private:
  std::vector<SegmentInfo> segments_;
  size_t current_;
};

} // namespace bytedance::bolt::shuffle::sparksql::test
