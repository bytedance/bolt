/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/shuffle/sparksql/Options.h"

#include <fmt/format.h>
#include <folly/String.h>

#include "bolt/common/base/SuccinctPrinter.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {

// Partitioning and row format names, matching the names used by the
// configuration (see toPartitioning() and row::RowFormat).
const char* partitioningName(Partitioning partitioning) {
  switch (partitioning) {
    case Partitioning::kSingle:
      return "single";
    case Partitioning::kRoundRobin:
      return "rr";
    case Partitioning::kHash:
      return "hash";
    case Partitioning::kRange:
      return "range";
  }
  return "unknown";
}

const char* rowFormatName(row::RowFormat format) {
  switch (format) {
    case row::RowFormat::DENSE:
      return "dense";
    case row::RowFormat::COMPACT:
      return "compact";
  }
  return "unknown";
}

} // namespace

std::string ShuffleReaderOptions::toString() const {
  return fmt::format(
      "compressionType={} codecBackend={} batchSize={} "
      "shuffleBatchByteSize={} shuffleBufferSize={} numPartitions={} "
      "partitionShortName={} forceShuffleWriterType={} rowFormat={} "
      "rowBasedShuffleThreshold={} checksumEnabled={} "
      "reuseBufferedInputStream={} reuseColumnBuffer={}",
      arrow::util::Codec::GetCodecAsString(compressionType),
      codecBackend,
      batchSize,
      shuffleBatchByteSize,
      shuffleBufferSize,
      numPartitions,
      partitionShortName,
      forceShuffleWriterType,
      rowFormatName(rowFormat),
      rowBasedShuffleThreshold,
      checksumEnabled,
      reuseBufferedInputStream,
      reuseColumnBuffer);
}

std::string PartitionWriterOptions::toString() const {
  return fmt::format(
      "numPartitions={} partitionWriterType={} dataFile={} numSubDirs={} "
      "configuredDirs=[{}] compressionType={} codecBackend={} "
      "compressionLevel={} compressionMode={} compressionThreshold={} "
      "mergeBufferSize={} mergeThreshold={} bufferedWrite={} "
      "bufferedSpillWrite={} pushBufferMaxSize={} shuffleBufferSize={} "
      "rowvectorModeCompressionMinColumns={} "
      "rowvectorModeCompressionMaxBufferSize={} checksumEnabled={} "
      "rssClient={}",
      numPartitions,
      partitionWriterType,
      dataFile,
      numSubDirs,
      folly::join(",", configuredDirs),
      arrow::util::Codec::GetCodecAsString(compressionType),
      codecBackend,
      compressionLevel,
      compressionMode,
      compressionThreshold,
      mergeBufferSize,
      mergeThreshold,
      bufferedWrite,
      bufferedSpillWrite,
      pushBufferMaxSize,
      shuffleBufferSize,
      rowvectorModeCompressionMinColumns,
      rowvectorModeCompressionMaxBufferSize,
      checksumEnabled,
      rssClient ? "set" : "unset");
}

std::string ShuffleWriterOptions::toString() const {
  return fmt::format(
      "partitioning={} taskAttemptId={} startPartitionId={} bufferSize={} "
      "bufferReallocThreshold={} shuffleBatchSize={} "
      "forceShuffleWriterType={} useV2PreallocSizeThreshold={} "
      "enableVectorCombination={} accumulateBatchMaxColumns={} "
      "accumulateBatchMaxBatches={} recommendedColumn2RowSize={} "
      "rowFormat={} rowBasedShuffleThreshold={} shuffleCheckRatio={} "
      "shuffleCheckMaxColumns={} rowvectorModeCompressionMinColumns={} "
      "rowvectorModeCompressionMaxBufferSize={} partitionWriterOptions=[{}]",
      partitioningName(partitioning),
      taskAttemptId,
      startPartitionId,
      bufferSize,
      bufferReallocThreshold,
      shuffleBatchSize,
      forceShuffleWriterType,
      useV2PreallocSizeThreshold,
      enableVectorCombination,
      accumulateBatchMaxColumns,
      accumulateBatchMaxBatches,
      recommendedColumn2RowSize,
      rowFormatName(rowFormat),
      rowBasedShuffleThreshold,
      shuffleCheckRatio,
      shuffleCheckMaxColumns,
      rowvectorModeCompressionMinColumns,
      rowvectorModeCompressionMaxBufferSize,
      partitionWriterOptions.toString());
}

std::string ShuffleWriterMetrics::toString() const {
  const auto asMillis = [](int64_t nanos) {
    return fmt::format("{:.3f}ms", static_cast<double>(nanos) / 1'000'000);
  };
  int64_t totalPartitionLengths{0};
  int64_t totalRawPartitionLengths{0};
  for (auto length : partitionLengths) {
    totalPartitionLengths += length;
  }
  for (auto length : rawPartitionLengths) {
    totalRawPartitionLengths += length;
  }
  return fmt::format(
      "totalInputRowNumber={} totalInputBatches={} totalBytesWritten={} "
      "totalBytesEvicted={} totalWriteTime={} totalEvictTime={} "
      "totalCompressTime={} splitTime={} convertTime={} flattenTime={} "
      "computePidTime={} shuffleWriteTime={} externalReclaimTime={} "
      "maxPartitionBufferSize={} avgPreallocSize={} dataSize={} peakBytes={} "
      "useV2={} useRowBased={} rowVectorModeCompress={} "
      "combinedVectorNumber={} combineVectorTimes={} combineVectorCost={} "
      "numPartitions={} totalPartitionLengths={} totalRawPartitionLengths={}",
      totalInputRowNumber,
      totalInputBatches,
      succinctBytes(totalBytesWritten),
      succinctBytes(totalBytesEvicted),
      asMillis(totalWriteTime),
      asMillis(totalEvictTime),
      asMillis(totalCompressTime),
      asMillis(splitTime),
      asMillis(convertTime),
      asMillis(flattenTime),
      asMillis(computePidTime),
      asMillis(shuffleWriteTime),
      asMillis(externalReclaimTime),
      succinctBytes(maxPartitionBufferSize),
      succinctBytes(avgPreallocSize),
      succinctBytes(dataSize),
      succinctBytes(peakBytes),
      useV2,
      useRowBased,
      rowVectorModeCompress,
      combinedVectorNumber,
      combineVectorTimes,
      asMillis(combineVectorCost),
      partitionLengths.size(),
      succinctBytes(totalPartitionLengths),
      succinctBytes(totalRawPartitionLengths));
}

} // namespace bytedance::bolt::shuffle::sparksql
