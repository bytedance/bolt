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

#pragma once

#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/base/PartitionBlockOutput.h"
#include "bolt/shuffle/sparksql/cell/CellBuffer.h"
#include "bolt/shuffle/sparksql/cell/RunCompressor.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Per column, its committed position counts across all partitions.
/// Empty, or a nullptr entry, uses the payload's row counts. A nonempty
/// table has layout->numColumns() entries; each non-null entry has
/// numPartitions counts. The table and counts are borrowed until the output
/// call returns.
using ColumnPositionCounts = std::span<const uint32_t* const>;

/// What an output backend reads from the splitter when draining or sealing:
/// the live window state. Borrowed for the duration of a call. A
/// partition's counts run since its payload opened: a partition seal closes
/// single payloads, so they may have opened at different times.
struct CellWindowInput {
  const DataCells* cells;
  const NullCells* nulls;
  const CellLayout* layout;
  /// Per partition: rows and string bytes of its open payload.
  std::span<const uint32_t> rowCounts;
  std::span<const uint64_t> variableBytes;
  /// Payload encoding tags: ceil(layout->numColumns() / 8) bytes, one bit per
  /// column, set when the column is written in dictionary form. Constant
  /// for the writer's lifetime (the probe decides once).
  std::span<const uint8_t> encodingTags;
  /// Also the length of the per-partition arrays below.
  uint32_t numPartitions;
  /// Per wire column, the column's position count per partition; nullptr,
  /// or a nullptr entry, means rowCounts.
  const uint32_t* const* columnPositions = nullptr;
  /// For a Run only: per partition, nonzero when the Run takes it. Null
  /// when the Run takes every partition.
  const uint8_t* runPartitions = nullptr;
  /// For a seal only: per partition, nonzero when the seal closes its
  /// payload. Null when the seal closes every partition's: the whole
  /// window.
  const uint8_t* sealPartitions = nullptr;
};

/// The Cell shuffle writer's output backend. A Run writes resident cells
/// out to free memory; it can happen at any allocation point and never
/// closes a payload. A seal closes payloads. Local and RSS backends differ
/// only here.
class CellOutput {
 public:
  virtual ~CellOutput() = default;

  /// Whether sealPayloads() consumes the window's resident cells itself. A
  /// backend that does (RSS: every payload is pushed whole at the seal) is
  /// never asked to spill cells; otherwise the splitter drains the cells into
  /// a run before sealing.
  virtual bool sealsResidentCells() const {
    return false;
  }

  /// Writes the DataCells chains of the partitions the input
  /// names (every partition by default) into a Run of the current window.
  /// Does not release the cells: the splitter recycles them (so a
  /// spill fired from inside an append never frees held ids twice).
  virtual void spillCells(const CellWindowInput& in) = 0;

  /// Closes the payloads of the partitions the input names (every
  /// partition by default): records their null bodies and row counts.
  /// The other partitions' payloads stay open. Precondition
  /// (splitter-orchestrated): the named partitions' caches flushed, and
  /// their cells drained unless sealsResidentCells().
  virtual void sealPayloads(const CellWindowInput& in) = 0;

  /// Final merge. Writes any unsealed window partitions with rows directly
  /// from memory, after any runs that window already spilled (RSS sends them).
  /// Fills the metrics fields it owns (partitionLengths, rawPartitionLengths,
  /// totalBytesWritten, totalBytesEvicted, write/evict times).
  virtual void finalize(
      const CellWindowInput& in,
      ShuffleWriterMetrics& metrics) = 0;

  /// Bytes the task's output - this one and the composite output it ends
  /// together with - spilled or pushed so far.
  virtual int64_t bytesEvicted() const = 0;
};

/// The ESS backend: Runs and sealed windows go to the task's shared spill file
/// - kept in memory while memory allows - and finalize() merges them into the
/// shuffle data file, partition-major, together with the task's composite
/// output.
///
/// Spill segments are runs in their final wire form, written by the
/// RunCompressor as any run of the same bytes. The merge writes a payload's
/// header, then its spilled segments, oldest first, each a run of its own
/// copied verbatim, and last - for the window still in memory - its
/// resident cells as one more run. A
/// task whose data never needed a spill writes payloads straight from the
/// live cells and touches no temporary file at all.
class LocalCellOutput final : public CellOutput, public PartialSpiller {
 public:
  /// `composite` holds the task's CompositeRowVector blocks, merged after
  /// this output's payloads in every partition; both share `spill`, which must
  /// outlive them. `options` gives the codec, the data file and the spill
  /// directories; `pool` backs the scratch buffer and must outlive this
  /// object. The compression workspaces come from the spill pool.
  LocalCellOutput(
      PartialSpiller& composite,
      ShuffleSpillFile& spill,
      const CellLayout* layout,
      CellShuffleOptions cellOptions,
      PartitionWriterOptions options,
      memory::MemoryPool* pool);

  void spillCells(const CellWindowInput& in) override;
  void sealPayloads(const CellWindowInput& in) override;

  /// Writes the data file: mergeToDataFile() of this output and the
  /// composite one. Here, not in the writer, because the current window -
  /// written straight from memory - only lives through this call.
  void finalize(const CellWindowInput& in, ShuffleWriterMetrics& metrics)
      override;

  int64_t bytesEvicted() const override {
    return spilledBytes();
  }

 private:
  /// The partition's payloads: every sealed window's, then the window
  /// borrowed by finalize().
  void writePartitionShare(uint32_t pid, DataFileWriter& out) override;

  static constexpr uint32_t kNoSegment = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kSegmentChunkShift = 12;
  static constexpr uint32_t kSegmentChunkSize = 1u << kSegmentChunkShift;
  static constexpr uint32_t kSegmentChunkMask = kSegmentChunkSize - 1;

  /// One partition's slice of a spilled run.
  struct SpillSegment {
    uint64_t begin;
    uint64_t end;
  };

  /// Fixed-size SoA block of spilled segments and intrusive next links.
  struct SegmentChunk {
    SpillSegment segments[kSegmentChunkSize];
    uint32_t next[kSegmentChunkSize];
  };

  /// Intrusive singly-linked list of a partition's open spilled segments,
  /// oldest first.
  struct SegmentChain {
    uint32_t head{kNoSegment};
    uint32_t tail{kNoSegment};
    uint32_t count{0};
  };

  /// One partition's closed payload: its run segments chain, oldest first,
  /// and its null body in the spill file.
  struct SealedPayload {
    uint32_t firstSegment{kNoSegment};
    uint32_t segmentCount{0};
    uint64_t nullOffset{0};
    uint32_t nullLength{0};
    /// The null body's size decompressed; 0 when stored as it is.
    uint32_t nullDecodedSize{0};
    uint32_t rows{0};
    uint64_t variableBytes{0};
  };

  /// Appends one spilled segment onto partition `pid`'s open chain.
  void appendSegment(uint32_t pid, SpillSegment segment);

  /// Copies `count` segments chained from `head` into the data file.
  void copySegmentChain(DataFileWriter& out, uint32_t head, uint32_t count);

  /// Appends partition `pid`'s sealed payload `payload`.
  void writeSealedPayload(
      DataFileWriter& out,
      uint32_t pid,
      const SealedPayload& payload,
      std::span<const uint8_t> encodingTags);

  /// Appends one partition's payload for the current (open) window:
  /// header, null body and row counts straight from memory, then the
  /// mid-window spilled runs and the still-resident cells as one more run,
  /// in that order. The residual never takes a spill round-trip.
  void writeOpenPayload(
      DataFileWriter& out,
      const CellWindowInput& in,
      uint32_t pid);

  /// The kind byte that opens every block on the wire.
  void appendBlockKind();

  PartialSpiller& composite_;
  const PartitionWriterOptions options_;
  const CellLayout* const layout_;
  memory::MemoryPool* const pool_;
  /// Time metrics align with the V1/V2 partition writers and never
  /// overlap: compress = codec calls only; write = fwrite/fflush moments
  /// on the data file; evict = fwrite/fflush moments on the spill file.
  /// Assembly glue (chain scans, null bodies, spill read-back) is counted
  /// nowhere, as in V1 where it rides inside split.
  uint64_t compressTimeNs_{0};
  /// Every run's compression.
  RunCompressor compressor_;

  /// Chunked SoA storage for spilled segments across all partitions.
  std::vector<std::unique_ptr<SegmentChunk>> segmentChunks_;
  uint32_t numSegments_{0};
  /// Per partition: open spilled segment chain, oldest first.
  std::vector<SegmentChain> pendingSegments_;
  /// Non-empty spill runs written so far.
  uint64_t numRuns_{0};
  /// Per partition: its sealed payloads, oldest first.
  std::vector<std::vector<SealedPayload>> sealed_;
  /// Window and partition seals completed so far.
  uint64_t numSeals_{0};
  /// The merge: spilled segments copied.
  uint64_t copiedSegments_{0};
  /// Borrowed finalize() input for writePartitionShare().
  const CellWindowInput* mergeInput_{nullptr};

  /// Pre-compression bytes merged so far.
  uint64_t rawBytesWritten_{0};
  /// Pre-compression bytes per partition: spilled runs accumulated in
  /// spillCells(), sealed/open headers and resident tails added at merge.
  std::vector<int64_t> rawPartitionLengths_;
  BufferPtr scratch_;
};

/// The remote shuffle service (RSS) backend, e.g. Celeborn. The service may
/// reorder a mapper's pushes and interleave other mappers' pushes between
/// them, so every block is exactly one complete ColumnarPayload: a window is
/// only ever output whole, at a seal - one writeBlocks() of every partition's
/// payload - and nothing is spilled. Each payload carries the partition's
/// resident cells as one run, written by the RunCompressor. The payloads go
/// through `blocks`, the push channel the task's composite blocks share, which
/// finalize() ends once for both.
class RssCellOutput final : public CellOutput {
 public:
  /// `blocks` must outlive this object. `options` gives the codec and
  /// `cellOptions` its mode; `pool` backs the payloads and must outlive
  /// this object. The compression workspaces come from the spill pool.
  RssCellOutput(
      PartitionBlockOutput& blocks,
      const CellLayout* layout,
      const PartitionWriterOptions& options,
      const CellShuffleOptions& cellOptions,
      memory::MemoryPool* pool);

  bool sealsResidentCells() const override {
    return true;
  }

  void spillCells(const CellWindowInput& in) override;
  void sealPayloads(const CellWindowInput& in) override;
  void finalize(const CellWindowInput& in, ShuffleWriterMetrics& metrics)
      override;

  int64_t bytesEvicted() const override {
    return blocks_.bytesEvicted();
  }

 private:
  /// One writeBlocks() carrying the whole payload of every selected partition
  /// with rows.
  void sendPayloads(const CellWindowInput& in);

  /// Assembles partition `pid`'s payload into `payload`.
  void
  buildPayload(const CellWindowInput& in, uint32_t pid, BufferPtr& payload);

  PartitionBlockOutput& blocks_;
  const CellLayout* const layout_;
  memory::MemoryPool* const pool_;
  uint64_t compressTimeNs_{0};
  RunCompressor compressor_;

  std::vector<int64_t> rawPartitionLengths_;
  uint64_t numSeals_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
