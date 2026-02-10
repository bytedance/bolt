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

#include <arrow/ipc/options.h>
#include <arrow/util/compression.h>
#include <bolt/common/base/Exceptions.h>
#include <fmt/format.h>
#include <cstdint>
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioning.h"
namespace bytedance::bolt::shuffle::sparksql {

static constexpr int16_t kDefaultBatchSize = 4096;
static constexpr int32_t kDefaultShuffleBatchByteSize = 41943040;
static constexpr int16_t kDefaultShuffleWriterBufferSize = 4096;
static constexpr int64_t kDefaultMergeBufferBytesThreshold = 40 * 1024 * 1024;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr int32_t kDefaultCompressionThreshold = 100;
static constexpr int32_t kDefaultBufferAlignment = 64;
static constexpr double kDefaultBufferReallocThreshold = 0.25;
static constexpr double kDefaultMergeBufferThreshold = 0.25;
static constexpr bool kEnableBufferedWrite = true;
static constexpr int32_t kDefaultForceShuffleWriterType = 0;
static constexpr int32_t kDefaultUseV2PreallocSizeThreshold = 0;
static constexpr int32_t kDefaultRowVectorModeCompressionMinColumns = 20;
static constexpr int32_t kDefaultRowVectorModeCompressionMaxBufferSize =
    5 * 1024 * 1024;
static constexpr int32_t kDefaultPreAllocSize =
    std::numeric_limits<int32_t>::max();
static constexpr int32_t kDefaultAccumulateBatchMaxBatches = 65535;
static constexpr int32_t kDefaultAccumulateBatchMaxColumns =
    0; // default is close

static constexpr int32_t rowBasePartitionThreshold = 8000;
static constexpr int32_t rowBaseColumnNumThreshold = 5;

static constexpr int32_t kMaxShuffleWriterBatchBytes =
    200 * 1024 * 1024; // 200MB

enum class ShuffleWriterType { Adaptive = 0, V1 = 1, V2 = 2, RowBased = 3 };

enum PartitionWriterType { kLocal, kCeleborn };

inline enum PartitionWriterType getPartitionWriterType(
    const std::string& type) {
  if (type == "local") {
    return PartitionWriterType::kLocal;
  } else if (type == "celeborn") {
    return PartitionWriterType::kCeleborn;
  } else {
    BOLT_FAIL("Unsupported partition writer type: " + type);
  }
}

enum class RowVectorLayout {
  kRowStart = 9,
  kColumnar = 10,
  kComposite = 11,
  kInvalid = 12
};

struct ShuffleReaderOptions {
  arrow::Compression::type compressionType =
      arrow::Compression::type::LZ4_FRAME;
  std::string codecBackend = "none";
  int32_t batchSize = kDefaultBatchSize;
  int32_t shuffleBatchByteSize = kDefaultShuffleBatchByteSize;
  int32_t numPartitions = -1;
  std::string partitionShortName = "";
  int32_t forceShuffleWriterType = -1;
};

struct PartitionWriterOptions {
  int32_t numPartitions = -1;
  int32_t mergeBufferSize = kDefaultShuffleWriterBufferSize;
  double mergeThreshold = kDefaultMergeBufferThreshold;

  int32_t compressionThreshold = kDefaultCompressionThreshold;
  arrow::Compression::type compressionType = arrow::Compression::LZ4_FRAME;
  std::string codecBackend = "none";
  int32_t compressionLevel = arrow::util::kUseDefaultCompressionLevel;
  std::string compressionMode = "buffer";

  bool bufferedWrite = kEnableBufferedWrite;

  int32_t numSubDirs = kDefaultNumSubDirs;

  int32_t pushBufferMaxSize = kDefaultShuffleWriterBufferSize;

  int32_t shuffleBufferSize = kDefaultShuffleBatchByteSize;

  int64_t rowvectorModeCompressionMinColumns =
      kDefaultRowVectorModeCompressionMinColumns;
  int64_t rowvectorModeCompressionMaxBufferSize =
      kDefaultRowVectorModeCompressionMaxBufferSize;

  PartitionWriterType partitionWriterType = PartitionWriterType::kLocal;
  // for LocalPartitionWriter
  std::string dataFile;
  std::vector<std::string> configuredDirs;

  // for CelebornPartitionWriter
  std::shared_ptr<RssClient> rssClient;
};

struct ShuffleWriterOptions {
  int32_t bufferSize = kDefaultShuffleWriterBufferSize;
  double bufferReallocThreshold = kDefaultBufferReallocThreshold;
  Partitioning partitioning;
  int64_t taskAttemptId = -1;
  int32_t startPartitionId = 0;
  bool sort_before_repartition = true;
  int32_t forceShuffleWriterType = kDefaultForceShuffleWriterType;
  int32_t useV2PreallocSizeThreshold = kDefaultUseV2PreallocSizeThreshold;
  int32_t rowvectorModeCompressionMinColumns =
      kDefaultRowVectorModeCompressionMinColumns;
  int32_t rowvectorModeCompressionMaxBufferSize =
      kDefaultRowVectorModeCompressionMaxBufferSize;
  bool enableVectorCombination = true;
  int32_t accumulateBatchMaxColumns = kDefaultAccumulateBatchMaxColumns;
  int32_t accumulateBatchMaxBatches = kDefaultAccumulateBatchMaxBatches;
  int32_t recommendedColumn2RowSize = 0;
  PartitionWriterOptions partitionWriterOptions{};
};

struct ShuffleWriterMetrics {
  int64_t totalInputRowNumber{0};
  int64_t totalInputBatches{0};
  int64_t totalBytesWritten{0};
  int64_t totalBytesEvicted{0};
  int64_t totalWriteTime{0};
  int64_t totalEvictTime{0};
  int64_t totalCompressTime{0};
  int64_t maxPartitionBufferSize{0};
  int64_t avgPreallocSize{0};
  int64_t useV2{0};
  int64_t rowVectorModeCompress{0};
  int64_t combinedVectorNumber{0};
  int64_t combineVectorTimes{0};
  int64_t combineVectorCost{0};
  int64_t useRowBased{0};
  int64_t splitTime{0};
  int64_t convertTime{0};
  int64_t flattenTime{0};
  int64_t computePidTime{0};
  // total time for shuffle write, including split, evict, write, compress
  int64_t shuffleWriteTime{0};
  int64_t dataSize{0};
  std::vector<int64_t> partitionLengths{};
  std::vector<int64_t> rawPartitionLengths{}; // Uncompressed size.
};
} // namespace bytedance::bolt::shuffle::sparksql

template <>
struct fmt::formatter<bytedance::bolt::shuffle::sparksql::PartitionWriterType>
    : fmt::formatter<std::string_view> {
  auto format(
      bytedance::bolt::shuffle::sparksql::PartitionWriterType c,
      format_context& ctx) const {
    switch (c) {
      case bytedance::bolt::shuffle::sparksql::kLocal:
        return fmt::formatter<std::string_view>::format("kLocal", ctx);
      case bytedance::bolt::shuffle::sparksql::kCeleborn:
        return fmt::formatter<std::string_view>::format("kCeleborn", ctx);
      default:
        return fmt::format_to(ctx.out(), "unknown[{}]", static_cast<int>(c));
    }
  }
};
