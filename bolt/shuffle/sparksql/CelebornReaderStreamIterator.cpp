/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "bolt/shuffle/sparksql/CelebornReaderStreamIterator.h"

#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <celeborn/client/ShuffleClient.h>
#include <glog/logging.h>

#include <utility>

#include "bolt/common/base/Exceptions.h"

namespace {
class CelebornArrowInputStream final : public arrow::io::InputStream {
 public:
  CelebornArrowInputStream(
      std::unique_ptr<celeborn::client::CelebornInputStream> stream,
      arrow::MemoryPool* pool)
      : stream_(std::move(stream)), pool_(pool) {}

  arrow::Status Close() override {
    if (closed_) {
      return arrow::Status::OK();
    }
    closed_ = true;
    stream_.reset();
    return arrow::Status::OK();
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Tell() const override {
    return position_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    if (closed_) {
      return arrow::Status::IOError("Celeborn input stream is closed");
    }
    if (nbytes < 0) {
      return arrow::Status::Invalid("Read length must be non-negative");
    }
    if (nbytes == 0) {
      return 0;
    }
    auto* outBytes = reinterpret_cast<uint8_t*>(out);
    int64_t total = 0;
    while (total < nbytes) {
      auto bytesRead = stream_->read(
          outBytes,
          static_cast<size_t>(total),
          static_cast<size_t>(nbytes - total));
      if (bytesRead <= 0) {
        break;
      }
      total += bytesRead;
    }
    position_ += total;
    return total;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(
        auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_));
    ARROW_ASSIGN_OR_RAISE(auto bytesRead, Read(nbytes, buffer->mutable_data()));
    RETURN_NOT_OK(buffer->Resize(bytesRead, /*shrink_to_fit=*/true));
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
  }

 private:
  std::unique_ptr<celeborn::client::CelebornInputStream> stream_;
  arrow::MemoryPool* pool_{nullptr};
  int64_t position_{0};
  bool closed_{false};
};
} // namespace

namespace bytedance::bolt::shuffle::sparksql {

CelebornReaderStreamIterator::CelebornReaderStreamIterator(
    std::shared_ptr<celeborn::client::ShuffleClient> shuffleClient,
    int32_t shuffleId,
    std::vector<int32_t> partitionIds,
    int32_t attemptNumber,
    int32_t startMapIndex,
    int32_t endMapIndex,
    bool needCompression)
    : shuffleClient_(std::move(shuffleClient)),
      shuffleId_(shuffleId),
      partitionIds_(std::move(partitionIds)),
      attemptNumber_(attemptNumber),
      startMapIndex_(startMapIndex),
      endMapIndex_(endMapIndex),
      needCompression_(needCompression) {
  BOLT_CHECK_NOT_NULL(
      shuffleClient_, "ShuffleClient is null for Celeborn reader");
}

CelebornReaderStreamIterator::~CelebornReaderStreamIterator() {
  close();
}

std::shared_ptr<arrow::io::InputStream>
CelebornReaderStreamIterator::nextStream(arrow::MemoryPool* pool) {
  if (closed_ || nextPartitionIndex_ >= partitionIds_.size()) {
    return nullptr;
  }
  auto partitionId = partitionIds_[nextPartitionIndex_++];
  auto celebornStream = shuffleClient_->readPartition(
      shuffleId_,
      partitionId,
      attemptNumber_,
      startMapIndex_,
      endMapIndex_,
      needCompression_);
  BOLT_CHECK_NOT_NULL(
      celebornStream,
      "Failed to create CelebornInputStream for partition " +
          std::to_string(partitionId));
  return std::make_shared<CelebornArrowInputStream>(
      std::move(celebornStream), pool);
}

void CelebornReaderStreamIterator::close() {
  if (closed_) {
    return;
  }
  closed_ = true;
}

void CelebornReaderStreamIterator::updateMetrics(
    int64_t numRows,
    int64_t numBatches,
    int64_t decompressTime,
    int64_t deserializeTime,
    int64_t totalReadTime) {
  // TODO(celeborn): metric updater should be independent from reader iterator
  BOLT_UNSUPPORTED(
      "CelebornReaderStreamIterator does not support updateMetrics");
}

} // namespace bytedance::bolt::shuffle::sparksql
