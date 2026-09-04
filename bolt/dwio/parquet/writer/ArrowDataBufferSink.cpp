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

#include "bolt/dwio/parquet/writer/ArrowDataBufferSink.h"

#include <arrow/buffer.h>
#include "bolt/buffer/Buffer.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/dwio/common/FileSink.h"
namespace bytedance::bolt::parquet {
ArrowDataBufferSink::ArrowDataBufferSink(
    std::unique_ptr<dwio::common::FileSink> sink,
    memory::MemoryPool& pool,
    int64_t flushThresholdBytes,
    double growRatio,
    std::shared_ptr<WriterMetricsCollector> metrics)
    : pool_(&pool),
      sink_(std::move(sink)),
      flushThresholdBytes_(flushThresholdBytes),
      growRatio_(growRatio),
      metrics_(std::move(metrics)) {}

::arrow::Status ArrowDataBufferSink::Write(
    const std::shared_ptr<::arrow::Buffer>& data) {
  return Write(reinterpret_cast<const void*>(data->data()), data->size());
}

::arrow::Status ArrowDataBufferSink::Write(const void* data, int64_t nbytes) {
  if (!buffer_) {
    buffer_ = AlignedBuffer::allocate<uint8_t>(flushThresholdBytes_, pool_);
    buffer_->setSize(0);
  }
  while (nbytes > 0) {
    if (buffer_->size() == 0 && nbytes >= flushThresholdBytes_) {
      return WriteZeroCopy(reinterpret_cast<const uint8_t*>(data), nbytes);
    }
    auto writeSize =
        std::min<int64_t>(nbytes, buffer_->capacity() - buffer_->size());
    memcpy(buffer_->asMutable<uint8_t>() + buffer_->size(), data, writeSize);
    buffer_->setSize(buffer_->size() + writeSize);
    nbytes -= writeSize;
    data = reinterpret_cast<const char*>(data) + writeSize;
    if (buffer_->size() >= flushThresholdBytes_) {
      RETURN_NOT_OK(Flush(buffer_));
      buffer_->setSize(0);
    }
  }
  return ::arrow::Status::OK();
}

struct BufferReleaser {
  BufferReleaser() = default;
  void addRef() const {}
  void release() const {}
};

::arrow::Status ArrowDataBufferSink::WriteZeroCopy(
    const uint8_t* data,
    int64_t nbytes) {
  auto bufferView = bolt::BufferView<BufferReleaser>::create(
      data, nbytes, BufferReleaser(), true);
  return Flush(bufferView);
}

::arrow::Status ArrowDataBufferSink::Flush(const bolt::BufferPtr& buffer) {
  auto dataBuffer = dwio::common::DataBuffer<char>::wrap(buffer);
  return Flush(*const_cast<dwio::common::DataBuffer<char>*>(dataBuffer.get()));
}

::arrow::Status ArrowDataBufferSink::Flush() {
  if (buffer_) {
    RETURN_NOT_OK(Flush(buffer_));
    buffer_ = nullptr;
  }
  return ::arrow::Status::OK();
}

::arrow::Status ArrowDataBufferSink::Flush(
    dwio::common::DataBuffer<char>& buffer) {
  if (buffer.size() > 0) {
    bytesFlushed_ += buffer.size();
    WriterMetricTimer timer(&metrics_->writeIOWallNanos, true);
    sink_->write(std::move(buffer));
  }
  return ::arrow::Status::OK();
}

::arrow::Result<int64_t> ArrowDataBufferSink::Tell() const {
  return bytesFlushed_ + (buffer_ ? buffer_->size() : 0);
}

::arrow::Status ArrowDataBufferSink::Close() {
  ARROW_RETURN_NOT_OK(Flush());
  WriterMetricTimer timer(&metrics_->writeIOWallNanos, true);
  sink_->close();
  return ::arrow::Status::OK();
}

bool ArrowDataBufferSink::closed() const {
  return sink_->isClosed();
}

void ArrowDataBufferSink::abort() {
  sink_.reset();
  buffer_ = nullptr;
}

} // namespace bytedance::bolt::parquet
