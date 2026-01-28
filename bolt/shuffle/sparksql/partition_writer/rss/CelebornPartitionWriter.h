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

#include <arrow/io/api.h>
#include <arrow/memory_pool.h>

#include "bolt/shuffle/sparksql/partition_writer/rss/RemotePartitionWriter.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"
namespace bytedance::bolt::shuffle::sparksql {

class CelebornPartitionWriter final : public RemotePartitionWriter {
 public:
  CelebornPartitionWriter(
      uint32_t numPartitions,
      PartitionWriterOptions options,
      arrow::MemoryPool* pool,
      std::shared_ptr<RssClient> celebornClient)
      : RemotePartitionWriter(numPartitions, std::move(options), pool),
        celebornClient_(celebornClient) {
    init();
  }

  arrow::Status evict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      Evict::type evictType,
      bool reuseBuffers,
      bool hasComplexType) override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status stop(ShuffleWriterMetrics* metrics) override;

  // for BoltRowBasedSortShuffleWriter
  arrow::Status evict(
      std::vector<std::vector<uint8_t*>>& rows,
      std::vector<int64_t>& partitionBytes,
      const bool isCompositeVector) override;

 private:
  void init();

  std::shared_ptr<RssClient> celebornClient_;

  std::vector<int64_t> bytesEvicted_;
  std::vector<int64_t> rawPartitionLengths_;

  // for BoltRowBasedSortShuffleWriter
  std::shared_ptr<AdaptiveParallelZstdCodec> zstdCodec_;
};
} // namespace bytedance::bolt::shuffle::sparksql
