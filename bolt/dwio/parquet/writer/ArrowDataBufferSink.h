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

#include <arrow/io/interfaces.h>
#include <memory>

#include "bolt/dwio/common/DataBuffer.h"
#include "bolt/dwio/parquet/writer/WriterMetrics.h"
namespace bytedance::bolt {
namespace memory {
class MemoryPool;
}
namespace dwio::common {
class FileSink;
}
namespace parquet {

// Utility for buffering Arrow output with a DataBuffer.
class ArrowDataBufferSink : public ::arrow::io::OutputStream {
 public:
  /// @param growRatio Growth factor used when invoking the reserve() method of
  /// DataSink, thereby helping to minimize frequent memcpy operations.
  ArrowDataBufferSink(
      std::unique_ptr<dwio::common::FileSink> sink,
      memory::MemoryPool& pool,
      int64_t flushThresholdBytes,
      double growRatio,
      std::shared_ptr<WriterMetricsCollector> metrics);

  ::arrow::Status Write(const std::shared_ptr<::arrow::Buffer>& data) override;

  ::arrow::Status Write(const void* data, int64_t nbytes) override;

  ::arrow::Status Flush() override;

  ::arrow::Result<int64_t> Tell() const override;

  ::arrow::Status Close() override;

  bool closed() const override;

  void abort();

 private:
  ::arrow::Status WriteZeroCopy(const uint8_t* data, int64_t nbytes);

  ::arrow::Status Flush(const bolt::BufferPtr& buffer);

  ::arrow::Status Flush(dwio::common::DataBuffer<char>& buffer);

  bolt::memory::MemoryPool* const pool_;
  std::unique_ptr<dwio::common::FileSink> sink_;
  const int64_t flushThresholdBytes_;
  const double growRatio_;
  BufferPtr buffer_;
  int64_t bytesFlushed_ = 0;
  const std::shared_ptr<WriterMetricsCollector> metrics_;
};
} // namespace parquet
} // namespace bytedance::bolt
