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
#include <string>
#include "bolt/row/RowFormat.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioning.h"
namespace bytedance::bolt::shuffle::sparksql {

static constexpr int16_t kDefaultBatchSize = 4096;
static constexpr int32_t kDefaultShuffleBatchByteSize = 41943040;
static constexpr int32_t kDefaultShuffleBufferSize = 40 * 1024 * 1024;
static constexpr int16_t kDefaultShuffleWriterBufferSize = 4096;
static constexpr int64_t kDefaultMergeBufferBytesThreshold = 40 * 1024 * 1024;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr int32_t kDefaultCompressionThreshold = 100;
static constexpr int32_t kDefaultBufferAlignment = 64;
static constexpr double kDefaultBufferReallocThreshold = 0.25;
static constexpr double kDefaultMergeBufferThreshold = 0.25;
static constexpr bool kEnableBufferedWrite = true;
static constexpr bool kEnableBufferedSpillWrite = false;
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
static constexpr int32_t kDefaultShuffleCheckMaxColumns =
    std::numeric_limits<int32_t>::max();
static constexpr int64_t kDefaultRowBasedShuffleThreshold =
    std::numeric_limits<int64_t>::max();
static constexpr int32_t rowBasePartitionThreshold = 8000;
static constexpr int32_t rowBaseColumnNumThreshold = 5;

// Whether to reuse a single BufferedInputStream across all reader streams by
// chaining them into one continuous stream, instead of creating a new
// BufferedInputStream (and deserializer) per stream.
static constexpr bool kDefaultReuseBufferedInputStream = false;

static constexpr bool kDefaultReuseColumnBuffer = false;

static constexpr int32_t kMaxShuffleWriterBatchBytes =
    200 * 1024 * 1024; // 200MB

enum class ShuffleWriterType {
  Adaptive = 0,
  V1 = 1,
  V2 = 2,
  RowBased = 3,
  Cell = 4
};

// Cell memory thresholds and encoding policy; compression uses
// PartitionWriterOptions. On Celeborn nothing is spilled: the spill
// thresholds push the window at the next batch boundary instead.
struct CellShuffleOptions {
  // Spill threshold on the chunk memory plus the spill data kept in memory
  // (spillMemoryBytes), as a share of the writer's memory limit: the
  // tighter of its pool's cap and the task's execution memory, which the
  // CellShuffleWriter init line shows. From 0 to 2: past 1, a task may
  // hold more than its share while fewer tasks run. 0, or no limit, turns
  // it off. Not a hard memory cap.
  double cellMemorySpillRatio = 0;
  // Local shuffle: the newest spill data - runs and sealed payloads on
  // their way to the merge, the composite rows' blocks included - kept in
  // memory instead of the spill files, in blocks of up to 4 MiB of the
  // writer's pool, at most this many bytes of them. Full, or short of
  // memory for a new block, it writes its oldest block to disk and reuses
  // it. Memory pressure writes its oldest blocks to disk first, as much as
  // the event needs: a reclaim, a chunk reservation that fails or passes
  // the cellMemorySpillRatio threshold, or composite rows short of room;
  // from the first such event on, it only reuses the blocks it holds. -1:
  // no cap of its own; it grows until the first memory pressure, and a
  // reclaim then finds it as the writer's memory, competing with the task's
  // other operators by size. 0: off, every spill byte goes to disk.
  int64_t spillMemoryBytes = -1;
  // Local shuffle: the cell data a writer must hold before a reclaim of a
  // given size writes a run of it to disk. Below it, the reclaim frees only
  // what needs no run - idle chunks and the spill memory - and the
  // arbitrator looks to other operators for the rest: a squeezed writer
  // holding little data, reclaimed after every batch, would otherwise write
  // a run of a few bytes per partition each time. The floor in effect is the
  // smaller of this and an eighth of the writer's memory limit - the tighter
  // of the pool's cap and the task's execution memory - or this with
  // neither. A reclaim of everything (a size of 0 or less) and a chunk
  // growth short of memory write whatever the cells hold. 0: off. Must not
  // be negative.
  int64_t reclaimRunFloorBytes = 64LL << 20;
  // Local shuffle: the cell data the writer holds per stream of every
  // partition before it spills without memory pressure. At a batch
  // boundary, cell data past partitions x streams x this many bytes spills,
  // the largest partitions first, down to half of it: the cells stay
  // bounded, their frames large enough to compress well, and the rest of
  // the memory keeps compressed spill data instead. When the product passes
  // what memory allows, memory pressure comes first and the writer spills
  // as before; a very large value turns this off. Must be positive.
  int64_t cellSpillBytesPerStream = 512LL << 10;
  // Seal a partition's payload at a batch boundary once it exceeds this
  // size; other partitions stay open.
  int64_t partitionSealBytes = 40LL << 20;
  // Seal the window once the moved null cells exceed this. A reclaim
  // requests a seal above a quarter of it.
  int64_t nullMemLimitBytes = 16LL << 20;
  // Probe the first batch to enable dictionary encoding per string column.
  bool enableStringDictionary = true;
  // Compress runs through the codec's stream form, straight from the cells
  // with fixed buffers; off, each frame is gathered into a spill-pool
  // workspace
  // and compressed at once. Codecs with no stream form are always one-shot.
  // Both write the same run layouts.
  bool streamCompression = true;
  // Learn whether compressing pays, per stream and for COMBINED frames and
  // null bodies: after a frame that compressed too little to be kept
  // (random keys, hashes, packed doubles), only one frame of its kind in ten
  // is compressed and judged, the others stored without a codec call, so a
  // kind whose data starts to compress is picked up again. Off, every frame
  // is compressed and judged.
  bool adaptiveCompression = true;
  // Shred ARRAY, MAP and ROW columns into plain columns of a nested
  // layout (cell/CellFormat.h). Off, the type adapter packs them row by row
  // into one binary column: a fallback for wide nested schemas at high
  // partition counts, whose per-stream
  // state the adapter's single column avoids. The reader must be set alike
  // (ShuffleReaderOptions::cellNativeNestedTypes).
  bool nativeNestedTypes = true;

  /// Returns all options in a human readable form.
  std::string toString() const;
};

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
  int32_t shuffleBufferSize = kDefaultShuffleBufferSize;
  int32_t numPartitions = -1;
  std::string partitionShortName = "";
  int32_t forceShuffleWriterType = -1;

  // On-wire row format for the row-based shuffle. Must match the writer side.
  row::RowFormat rowFormat = row::RowFormat::COMPACT;

  int64_t rowBasedShuffleThreshold = kDefaultRowBasedShuffleThreshold;

  // Enable checksum in codec for shuffle data corruption detection
  bool checksumEnabled = true;

  // Reuse a single BufferedInputStream across all reader streams to avoid the
  // overhead of repeatedly creating/destroying one per stream.
  bool reuseBufferedInputStream = kDefaultReuseBufferedInputStream;

  bool reuseColumnBuffer = kDefaultReuseColumnBuffer;

  // The writer side's preferCellShuffle: the writer then writes Cell when
  // adaptive (unless that picks the row-based writer) or forced to V1 or V2
  // (see writesCellShuffle()). Must match the writer.
  bool preferCellShuffle = false;

  // The writer side's CellShuffleOptions::nativeNestedTypes: whether Cell
  // payloads carry nested columns in a nested layout or packed by the type
  // adapter. Both sides must agree; the payloads do not say.
  bool cellNativeNestedTypes = true;

  /// Returns the options in a human readable form.
  std::string toString() const;
};

struct PartitionWriterOptions {
  int32_t numPartitions = -1;
  int32_t mergeBufferSize = kDefaultShuffleWriterBufferSize;
  double mergeThreshold = kDefaultMergeBufferThreshold;

  int32_t compressionThreshold = kDefaultCompressionThreshold;
  arrow::Compression::type compressionType = arrow::Compression::LZ4_FRAME;
  std::string codecBackend = "none";
  int32_t compressionLevel = kDefaultCompressionLevel;
  std::string compressionMode = "buffer";

  bool bufferedWrite = kEnableBufferedWrite;

  bool bufferedSpillWrite = kEnableBufferedSpillWrite;

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

  // Enable checksum in codec for shuffle data corruption detection
  bool checksumEnabled = true;

  /// Returns the options in a human readable form.
  std::string toString() const;
};

struct ShuffleWriterOptions {
  int32_t bufferSize = kDefaultShuffleWriterBufferSize;
  double bufferReallocThreshold = kDefaultBufferReallocThreshold;
  Partitioning partitioning;
  int64_t taskAttemptId = -1;
  int32_t startPartitionId = 0;
  int32_t forceShuffleWriterType = kDefaultForceShuffleWriterType;
  // Write Cell in place of the adaptive choice, unless that is the
  // row-based writer, and in place of a forced V1 or V2, under every
  // partitioning. A forced RowBased or Cell stays as forced. The readers
  // must set it alike. See writesCellShuffle().
  bool preferCellShuffle = false;
  int32_t useV2PreallocSizeThreshold = kDefaultUseV2PreallocSizeThreshold;
  int32_t rowvectorModeCompressionMinColumns =
      kDefaultRowVectorModeCompressionMinColumns;
  int32_t rowvectorModeCompressionMaxBufferSize =
      kDefaultRowVectorModeCompressionMaxBufferSize;
  bool enableVectorCombination = true;
  int32_t accumulateBatchMaxColumns = kDefaultAccumulateBatchMaxColumns;
  int32_t accumulateBatchMaxBatches = kDefaultAccumulateBatchMaxBatches;
  int32_t recommendedColumn2RowSize = 0;
  int32_t shuffleBatchSize = kDefaultShuffleBatchByteSize;
  double shuffleCheckRatio = 0;
  int32_t shuffleCheckMaxColumns = kDefaultShuffleCheckMaxColumns;
  row::RowFormat rowFormat = row::RowFormat::COMPACT;
  int64_t rowBasedShuffleThreshold = kDefaultRowBasedShuffleThreshold;
  CellShuffleOptions cellOptions{};
  PartitionWriterOptions partitionWriterOptions{};

  /// Returns the options in a human readable form.
  std::string toString() const;
};

struct ShuffleWriterMetrics {
  int64_t totalInputRowNumber{0};
  int64_t totalInputBatches{0};
  // CellShuffleWriter: seals, whole-window and per-partition.
  int64_t spillCount{0};
  // Cell row counts summed over dictionary-enabled columns.
  int64_t dictionaryMatchedRows{0};
  int64_t dictionaryFallbackRows{0};
  int64_t totalBytesWritten{0};
  int64_t totalBytesEvicted{0};
  // Number of temporary local shuffle files created, excluding final output.
  int64_t totalSpillFiles{0};
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
  // 1 when the writer is the Cell shuffle (CellShuffleWriter); summed over
  // tasks it counts Cell writers.
  int64_t useCellShuffle{0};
  int64_t splitTime{0};
  // CellShuffleWriter: time spent sealing payloads and spilling or pushing
  // cell data during split, excluded from splitTime.
  int64_t sealSpillTime{0};
  int64_t convertTime{0};
  int64_t flattenTime{0};
  int64_t computePidTime{0};
  // total time for shuffle write, including split, evict, write, compress
  int64_t shuffleWriteTime{0};
  // Total wall time of reclaims triggered from outside this operator, i.e. not
  // covered by shuffleWriteTime.
  int64_t externalReclaimTime{0};
  int64_t dataSize{0};
  int64_t peakBytes{0};
  std::vector<int64_t> partitionLengths{};
  std::vector<int64_t> rawPartitionLengths{}; // Uncompressed size.

  /// Returns all metrics in a human readable form; per-partition lengths are
  /// summarized.
  std::string toString() const;
};

// Only partitioning that has pid support adaptive shuffle writer, otherwise
// force use V1
inline bool supportAdaptiveShuffleWriter(const Partitioning& partitioning) {
  return partitioning == Partitioning::kRange ||
      partitioning == Partitioning::kHash;
}

/// Whether the writer writes Cell payloads, which only
/// SparkShuffleReaderNode reads: the one rule the writer's type choice and
/// both readers follow, under every partitioning: a forced Cell always; a
/// forced V1 or V2 when preferCellShuffle is set, which takes priority over
/// them; a forced RowBased never; the adaptive choice when
/// preferCellShuffle is set, unless it picked the row-based writer
/// (`isRowBased`, never so without a pid). Where this is false, a
/// partitioning without a pid (single, round-robin) writes V1.
inline bool writesCellShuffle(
    const Partitioning& /*partitioning*/,
    ShuffleWriterType type,
    bool preferCellShuffle,
    bool isRowBased) {
  switch (type) {
    case ShuffleWriterType::Cell:
      return true;
    case ShuffleWriterType::V1:
    case ShuffleWriterType::V2:
      return preferCellShuffle;
    case ShuffleWriterType::Adaptive:
      return preferCellShuffle && !isRowBased;
    default:
      return false;
  }
}

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
