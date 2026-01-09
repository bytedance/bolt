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

#include <arrow/io/memory.h>
#include <vector>
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"

namespace bytedance::bolt::shuffle::sparksql::test {

class MemoryReaderStreamIterator : public ReaderStreamIterator {
 public:
  explicit MemoryReaderStreamIterator(std::vector<std::vector<char>> buffers)
      : buffers_(std::move(buffers)), current_(0) {}

  std::shared_ptr<arrow::io::InputStream> nextStream(
      arrow::MemoryPool* pool) override {
    if (current_ >= buffers_.size()) {
      return nullptr;
    }
    const auto& buf = buffers_[current_++];
    // We need to keep the buffer alive.
    // BufferReader refers to data.
    // We can copy the data into an arrow::Buffer or keep shared_ptr.
    // For simplicity, let's create an arrow::Buffer.
    auto arrow_buf =
        arrow::Buffer::FromString(std::string(buf.begin(), buf.end()));
    return std::make_shared<arrow::io::BufferReader>(arrow_buf);
  }

  void close() override {}

  void updateMetrics(
      int64_t numRows,
      int64_t numBatches,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t totalReadTime) override {}

 private:
  std::vector<std::vector<char>> buffers_;
  size_t current_;
};

} // namespace bytedance::bolt::shuffle::sparksql::test
