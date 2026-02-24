/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <numeric>

#include <bolt/common/time/Timer.h>
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/CelebornPartitionWriter.h"
namespace bytedance::bolt::shuffle::sparksql {

void CelebornPartitionWriter::init() {
  bytesEvicted_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);
}

arrow::Status CelebornPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  // Push data and collect metrics.
  auto totalBytesEvicted =
      std::accumulate(bytesEvicted_.begin(), bytesEvicted_.end(), 0LL);
  celebornClient_->stop();
  // Populate metrics.
  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += spillTime_;
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesEvicted += totalBytesEvicted;
  metrics->totalBytesWritten += totalBytesEvicted;
  metrics->partitionLengths = std::move(bytesEvicted_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  *actual = 0;
  return arrow::Status::OK();
}

arrow::Status CelebornPartitionWriter::evict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    Evict::type evictType,
    bool reuseBuffers,
    bool hasComplexType) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->getBufferSize();

  bytedance::bolt::NanosecondTimer timer(&spillTime_);
  auto payloadType =
      (codec_ && inMemoryPayload->numRows() >= options_.compressionThreshold)
      ? Payload::Type::kCompressed
      : Payload::Type::kUncompressed;
  ARROW_ASSIGN_OR_RAISE(
      auto payload,
      inMemoryPayload->toBlockPayload(
          payloadType,
          payloadPool_.get(),
          codec_ ? codec_.get() : nullptr,
          hasComplexType));
  // Copy payload to arrow buffered os.
  ARROW_ASSIGN_OR_RAISE(
      auto celebornBufferOs,
      arrow::io::BufferOutputStream::Create(options_.pushBufferMaxSize, pool_));
  RETURN_NOT_OK(payload->serialize(celebornBufferOs.get()));
  payload = nullptr; // Invalidate payload immediately.

  // Push.
  ARROW_ASSIGN_OR_RAISE(auto buffer, celebornBufferOs->Finish());
  bytesEvicted_[partitionId] += celebornClient_->pushPartitionData(
      partitionId,
      reinterpret_cast<char*>(const_cast<uint8_t*>(buffer->data())),
      buffer->size());
  return arrow::Status::OK();
}

// for BoltRowBasedSortShuffleWriter
arrow::Status CelebornPartitionWriter::evict(
    std::vector<std::vector<uint8_t*>>& rows,
    std::vector<int64_t>& partitionBytes,
    const bool isCompositeVector) {
  // evict rows in all partitions
  if (!zstdCodec_) {
    // Disable checksum for Celeborn cause celeborn already has checksum inside
    zstdCodec_ = std::make_shared<AdaptiveParallelZstdCodec>(
        options_.compressionLevel, true, payloadPool_.get(), false);
  }
  RowVectorLayout layout = isCompositeVector ? RowVectorLayout::kComposite
                                             : RowVectorLayout::kColumnar;
  int64_t pBytes = 0;
  // compress and flush
  for (auto pid = 0; pid < rows.size(); ++pid) {
    if (isCompositeVector) {
      pBytes = std::accumulate(
          rows[pid].begin(),
          rows[pid].end(),
          0,
          [](uint64_t sum, uint8_t* row) { return sum + *(int32_t*)row; });
    } else {
      pBytes = partitionBytes[pid];
      partitionBytes[pid] = 0;
    }
    if (!rows[pid].empty()) {
      auto totalRowCount = rows[pid].size();
      size_t slicedAvgNumRows = totalRowCount;
      int32_t fragCnt = 1;
      if (pBytes > options_.shuffleBufferSize) {
        fragCnt = std::round(1.0 * pBytes / options_.shuffleBufferSize);
        slicedAvgNumRows = (totalRowCount + fragCnt - 1) / fragCnt;
        VLOG(1) << __FUNCTION__ << ": pBytes " << pBytes
                << ", options_.shuffleBufferSize " << options_.shuffleBufferSize
                << ", fragCnt = " << fragCnt
                << ", slicedAvgNumRows = " << slicedAvgNumRows
                << ", totalRowCount = " << totalRowCount;
      }
      BOLT_DCHECK(
          pBytes != 0,
          "rows = " + std::to_string(rows[pid].size()) +
              ", but bytes = " + std::to_string(pBytes));

      size_t startIndex = 0;
      do {
        auto slicedNumRows =
            std::min(slicedAvgNumRows, totalRowCount - startIndex);
        auto payload = std::make_unique<RowBlockPayload>(
            folly::Range<uint8_t**>(
                rows[pid].data() + startIndex, slicedNumRows),
            pBytes / fragCnt,
            payloadPool_.get(),
            zstdCodec_.get(),
            layout);

        // Copy payload to arrow buffered os.
        ARROW_ASSIGN_OR_RAISE(
            auto celebornBufferOs,
            arrow::io::BufferOutputStream::Create(
                options_.pushBufferMaxSize, pool_));
        RETURN_NOT_OK(payload->serialize(celebornBufferOs.get()));
        payload = nullptr; // Invalidate payload immediately.

        ARROW_ASSIGN_OR_RAISE(auto buffer, celebornBufferOs->Finish());
        bytesEvicted_[pid] += celebornClient_->pushPartitionData(
            pid,
            reinterpret_cast<char*>(const_cast<uint8_t*>(buffer->data())),
            buffer->size());
        startIndex += slicedNumRows;
      } while (startIndex < totalRowCount);
      BOLT_CHECK(startIndex == totalRowCount);

      rawPartitionLengths_[pid] += pBytes;
      rows[pid].clear();
    }
  }
  return arrow::Status::OK();
}

} // namespace bytedance::bolt::shuffle::sparksql
