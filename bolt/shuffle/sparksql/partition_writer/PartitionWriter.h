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

#include <iostream>
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/ShuffleMemoryPool.h"
#include "bolt/shuffle/sparksql/Spill.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
namespace bytedance::bolt::shuffle::sparksql {

struct Evict {
  enum type { kCache, kSpill, kCacheNoMerge };
};

class PartitionWriter {
 public:
  PartitionWriter(
      uint32_t numPartitions,
      PartitionWriterOptions options,
      arrow::MemoryPool* pool)
      : numPartitions_(numPartitions),
        options_(std::move(options)),
        pool_(pool) {
    payloadPool_ = std::make_unique<ShuffleMemoryPool>(pool);
    codec_ = createCodec(
        options_.compressionType,
        CodecOptions{
            getCodecBackend(options_.codecBackend),
            options_.compressionLevel,
            options_.checksumEnabled});
  }

  static std::unique_ptr<PartitionWriter> create(
      PartitionWriterOptions options,
      arrow::MemoryPool* pool);

  virtual ~PartitionWriter() = default;

  virtual arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) = 0;

  virtual arrow::Status stop(ShuffleWriterMetrics* metrics) = 0;

  /// Evict buffers for `partitionId` partition.
  virtual arrow::Status evict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      Evict::type evictType,
      bool reuseBuffers,
      bool hasComplexType) = 0;

  uint64_t cachedPayloadSize() {
    return payloadPool_->bytes_allocated();
  }

  // for V2
  virtual arrow::Status reclaimFixedSizeNoMerge(int64_t size, int64_t* actual) {
    return arrow::Status::OK();
  }
  virtual arrow::Status evictPayLoadCache() {
    return arrow::Status::OK();
  }
  virtual arrow::Status startClearPayLoadCacheSequential() {
    return arrow::Status::OK();
  }
  virtual arrow::Status stopClearPayLoadCacheSequential() {
    return arrow::Status::OK();
  }
  virtual arrow::Status clearSpecificPayLoadCache(uint32_t pid) {
    return arrow::Status::OK();
  }
  virtual bool canSpill() {
    return false;
  }

  // for BoltRowBasedSortShuffleWriter
  virtual arrow::Status evict(
      std::vector<std::vector<uint8_t*>>& rows,
      std::vector<int64_t>& partitionBytes,
      const bool isCompositeVector) {
    return arrow::Status::OK();
  }
  FLATTEN void setRowFormat(bool isRow) {
    isRowFormat_ = isRow;
  };

 protected:
  uint32_t numPartitions_;
  PartitionWriterOptions options_;
  arrow::MemoryPool* pool_;

  // Memory Pool used to track memory allocation of partition payloads.
  // The actual allocation is delegated to options_.memoryPool.
  std::unique_ptr<ShuffleMemoryPool> payloadPool_;

  std::unique_ptr<Codec> codec_;

  uint64_t compressTime_{0};
  uint64_t spillTime_{0};
  uint64_t writeTime_{0};

  bool isRowFormat_{false};
};
} // namespace bytedance::bolt::shuffle::sparksql
