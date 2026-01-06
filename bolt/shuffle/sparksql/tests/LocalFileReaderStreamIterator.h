/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
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
      // In test, we can just throw or return null.
      // But better to log?
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
