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

#pragma once

#include <arrow/ipc/writer.h>
#include <cstdint>
#include <numeric>
#include <utility>

#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/ShuffleMemoryPool.h"
#include "bolt/shuffle/sparksql/partition_writer/PartitionWriter.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioner.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioning.h"
namespace bytedance::bolt::shuffle::sparksql {

class ShuffleWriter {
 public:
  static constexpr int64_t kMinMemLimit = 128LL * 1024 * 1024;

  virtual arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) = 0;

  virtual arrow::Status split(
      bytedance::bolt::RowVectorPtr rv,
      int64_t memLimit) = 0;

  virtual arrow::Status stop() = 0;

  int32_t numPartitions() const {
    return numPartitions_;
  }

  int64_t partitionBufferSize() const {
    return partitionBufferPool_->bytes_allocated();
  }

  int64_t maxPartitionBufferSize() const {
    return partitionBufferPool_->max_memory();
  }

  int64_t totalBytesWritten() const {
    return metrics_.totalBytesWritten;
  }

  int64_t totalBytesEvicted() const {
    return metrics_.totalBytesEvicted;
  }

  int64_t totalWriteTime() const {
    return metrics_.totalWriteTime;
  }

  int64_t totalEvictTime() const {
    return metrics_.totalEvictTime;
  }

  int64_t totalCompressTime() const {
    return metrics_.totalCompressTime;
  }

  int64_t avgPeallocSize() const {
    return metrics_.avgPreallocSize;
  }

  int64_t useV2() const {
    return metrics_.useV2;
  }

  int64_t rowVectorModeCompress() const {
    return metrics_.rowVectorModeCompress;
  }

  int64_t combinedVectorNumber() const {
    return metrics_.combinedVectorNumber;
  }

  int64_t combineVectorTimes() const {
    return metrics_.combineVectorTimes;
  }

  int64_t combineVectorCost() const {
    return metrics_.combineVectorCost;
  }

  int64_t useRowBased() const {
    return metrics_.useRowBased;
  }

  int64_t totalConvertTime() const {
    return metrics_.convertTime;
  }

  int64_t totalFlattenTime() const {
    return metrics_.flattenTime;
  }

  int64_t totalComputePidTime() const {
    return metrics_.computePidTime;
  }

  const std::vector<int64_t>& partitionLengths() const {
    return metrics_.partitionLengths;
  }

  const std::vector<int64_t>& rawPartitionLengths() const {
    return metrics_.rawPartitionLengths;
  }

  virtual const uint64_t cachedPayloadSize() const = 0;

  ShuffleWriterMetrics& metrics() {
    return metrics_;
  }

  virtual std::string toString() const = 0;

 protected:
  ShuffleWriter(
      int32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      arrow::MemoryPool* pool,
      std::shared_ptr<arrow::MemoryPool> spillPool = nullptr)
      : numPartitions_(numPartitions),
        options_(std::move(options)),
        pool_(pool),
        spillPool_(std::move(spillPool)),
        partitionBufferPool_(std::make_unique<ShuffleMemoryPool>(pool)),
        partitionWriter_(std::move(partitionWriter)) {}

  virtual ~ShuffleWriter() = default;

  int32_t numPartitions_;

  ShuffleWriterOptions options_;

  arrow::MemoryPool* pool_;
  // Must outlive partition buffers and writers that may allocate from it.
  std::shared_ptr<arrow::MemoryPool> spillPool_;
  // Memory Pool used to track memory usage of partition buffers.
  // The actual allocation is delegated to options_.memoryPool.
  std::unique_ptr<ShuffleMemoryPool> partitionBufferPool_;

  std::unique_ptr<PartitionWriter> partitionWriter_;

  std::shared_ptr<arrow::Schema> schema_;

  // Column index, partition id, buffers.
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::ResizableBuffer>>>>
      partitionBuffers_;

  std::shared_ptr<Partitioner> partitioner_;

  ShuffleWriterMetrics metrics_{};
};

} // namespace bytedance::bolt::shuffle::sparksql
