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

#include <cstdio>
#include <span>
#include <string>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/PartitionBlockOutput.h"
#include "bolt/shuffle/sparksql/cell/CellBuffer.h"
#include "bolt/shuffle/sparksql/cell/RunCompressor.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// One payload as the writer wrote it into the data file: the file offset
/// of its kind byte, and its header's row and run counts.
struct CellPayloadRecord {
  uint64_t offset;
  uint32_t rows;
  uint32_t runCount;
};

/// What verifyCellDataFile() walked.
struct CellDataFileSummary {
  uint64_t partitions{0};
  uint64_t payloads{0};
  uint64_t compositeBlocks{0};
  uint64_t runs{0};
};

/// Checks the structure of the Cell shuffle data file at `path`, reading
/// only its block and run headers: partition p is the `partitionLengths[p]`
/// bytes after partition p - 1, blocks back to back (CellFormat.h) of a
/// layout of `numWireColumns` wire columns and `numStreams` streams. Every
/// read must stay within its partition, and the last block end exactly at
/// its end; every header field must be one the writer writes - rows >= 1,
/// runCount <= 65536, a known layout, sizes within the partition, a run
/// not empty - and the lengths must add up to the file's size. With
/// `written`, the file's payloads must be those, in order: at the same
/// offsets, with the same row and run counts. Otherwise fails with "cell
/// data file verification failed", naming the partition, the block, its
/// file offset, the field at fault with its expected and actual values,
/// and the 32 bytes at the field. A diagnostic of the writer's
/// (CellShuffleOptions::verifyDataFile) for a file just written, still in
/// the page cache.
CellDataFileSummary verifyCellDataFile(
    const std::string& path,
    std::span<const int64_t> partitionLengths,
    uint32_t numWireColumns,
    uint32_t numStreams,
    const std::vector<CellPayloadRecord>* written = nullptr);

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
  /// Payload encoding tags: ceil(numWireColumns / 8) bytes, one bit per
  /// wire column, set when the column is written in dictionary form. Constant
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

  /// Whether sealWindow() consumes the window's resident cells itself. A
  /// backend that does (RSS: every payload is pushed whole at the seal) is
  /// never asked for a Run; otherwise the splitter drains the cells into a
  /// Run before sealing.
  virtual bool sealsResidentCells() const {
    return false;
  }

  /// Writes the DataCells chains of the partitions the input
  /// names (every partition by default) into a Run of the current window.
  /// Does not release the cells: the splitter recycles them (so a
  /// spill fired from inside an append never frees held ids twice).
  virtual void spillRun(const CellWindowInput& in) = 0;

  /// Closes the payloads of the partitions the input names (every
  /// partition by default): records their null bodies and row counts.
  /// The other partitions' payloads stay open. Precondition
  /// (splitter-orchestrated): the named partitions' caches flushed, and
  /// their cells drained unless sealsResidentCells().
  virtual void sealWindow(const CellWindowInput& in) = 0;

  /// Final merge. `windowHasData` says whether the unsealed window still
  /// has rows; the backend writes them directly from memory, after any runs
  /// that window already spilled (RSS sends them). Fills the metrics
  /// fields it owns (partitionLengths, rawPartitionLengths,
  /// totalBytesWritten, totalBytesEvicted, write/evict times).
  virtual void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics& metrics) = 0;

  /// Bytes the task's output - this one and the composite output it ends
  /// together with - spilled or pushed so far.
  virtual int64_t bytesEvicted() const = 0;
};

/// The ESS backend: Runs and sealed windows go to a spill file of its own -
/// kept in the writer's SpillMemory while memory allows - and finalize()
/// merges them into the shuffle data file, partition-major, together with
/// the task's composite output.
///
/// Spill segments are runs in their final wire form, written by the
/// RunCompressor as any run of the same bytes. The merge writes a payload's
/// header, then its spilled segments, oldest first, each a run of its own
/// copied verbatim, and last - for the window still in memory - its
/// resident cells as one more run. A
/// task whose data never needed a spill writes payloads straight from the
/// live cells and touches no temporary file at all. With
/// CellShuffleOptions::verifyDataFile, finalize() checks the data file
/// against the payloads the merge wrote (verifyCellDataFile()) once it is
/// closed.
class LocalCellOutput final : public CellOutput, public PartialSpiller {
 public:
  /// `composite` holds the task's CompositeRowVector blocks, merged after
  /// this output's payloads in every partition; it must outlive this
  /// object. `options` gives the codec, the data file and the spill
  /// directories; `pool` backs the scratch buffer and must outlive this
  /// object. The compression workspaces come from the spill pool.
  /// `memory`, if not null, may keep the spill file's newest bytes and must
  /// outlive this object.
  LocalCellOutput(
      PartialSpiller& composite,
      const CellLayout* layout,
      CellShuffleOptions cellOptions,
      PartitionWriterOptions options,
      memory::MemoryPool* pool,
      SpillMemory* memory = nullptr);

  void spillRun(const CellWindowInput& in) override;
  void sealWindow(const CellWindowInput& in) override;

  /// Writes the data file: mergeToDataFile() of this output and the
  /// composite one. Here, not in the writer, because the current window -
  /// written straight from memory - only lives through this call.
  void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return spilledBytes() + composite_.spilledBytes();
  }

 private:
  /// The partition's payloads: every sealed window's, then the window
  /// borrowed by finalize().
  void writePartitionShare(uint32_t pid, DataFileWriter& out) override;

  struct SpilledRun {
    /// P + 1 absolute spill-file offsets; equal neighbours mean an empty
    /// segment for that partition.
    std::vector<uint64_t> pidEnds;
  };

  /// One partition's slice of a SpilledRun.
  struct SpillSegment {
    uint64_t begin;
    uint64_t end;
  };

  /// One partition's closed payload: its run segments, oldest first, and
  /// its null body in the spill file.
  struct SealedPayload {
    std::vector<SpillSegment> segments;
    uint64_t nullOffset{0};
    uint32_t nullLength{0};
    /// The null body's size decompressed; 0 when stored as it is.
    uint32_t nullDecodedSize{0};
    uint32_t rows{0};
    uint64_t variableBytes{0};
  };

  /// Logs the resident window and the task's seal and run totals: two INFO
  /// lines, the runs listed at most 16.
  void logWindowDiagnostics(const CellWindowInput& in, bool windowHasData);

  /// The non-empty segments of `pid` across runs [from, end), oldest first.
  static std::vector<SpillSegment> partitionSegments(
      const std::vector<SpilledRun>& runs,
      uint32_t pid,
      size_t from);

  /// Appends one partition's sealed payload.
  void writeSealedPayload(
      DataFileWriter& out,
      const SealedPayload& payload,
      std::span<const uint8_t> encodingTags);

  /// Copies one spilled run segment, in its final form, into the data
  /// file.
  void writeSpilledSegment(DataFileWriter& out, const SpillSegment& segment);

  /// Appends one partition's payload for the current (unsealed) window:
  /// header, null body and row counts straight from memory, then the
  /// mid-window spilled runs and the still-resident cells as one more run,
  /// in that order. The residual never takes a spill round-trip.
  void writeResidentPayload(
      DataFileWriter& out,
      const CellWindowInput& in,
      uint32_t pid);

  /// The kind byte that opens every block on the wire.
  void appendBlockKind();

  /// Notes, for the verification, a payload whose kind byte the next write
  /// to `out` starts.
  void
  recordPayload(const DataFileWriter& out, uint32_t rows, uint32_t runCount) {
    if (verifyDataFile_) {
      writtenPayloads_.push_back({out.position(), rows, runCount});
    }
  }

  /// verifyCellDataFile() of the data file the merge closed, logged.
  void verifyDataFile(const ShuffleWriterMetrics& metrics);

  PartialSpiller& composite_;
  const PartitionWriterOptions options_;
  const CellLayout* const layout_;
  memory::MemoryPool* const pool_;
  const bool verifyDataFile_;
  /// Every payload the merge wrote, in file order, when verifyDataFile_.
  std::vector<CellPayloadRecord> writtenPayloads_;
  /// Time metrics align with the V1/V2 partition writers and never
  /// overlap: compress = codec calls only; write = fwrite/fflush moments
  /// on the data file; evict = fwrite/fflush moments on the spill file.
  /// Assembly glue (chain scans, null bodies, spill read-back) is counted
  /// nowhere, as in V1 where it rides inside split.
  uint64_t compressTimeNs_{0};
  /// Every run's compression.
  RunCompressor compressor_;

  /// Every Run, oldest first. A partition's open payload owns its segments
  /// from openFrom_[pid] on; a seal moves them into the partition's
  /// SealedPayload and the partition's open payload past them.
  std::vector<SpilledRun> runs_;
  std::vector<size_t> openFrom_;
  /// Per partition: its sealed payloads, oldest first.
  std::vector<std::vector<SealedPayload>> sealed_;
  /// Seals, and the partition payloads they closed.
  uint64_t numSeals_{0};
  uint64_t numSealedPayloads_{0};
  /// The merge: spilled segments copied.
  uint64_t copiedSegments_{0};
  /// The window still in memory, borrowed by finalize() for its merge.
  const CellWindowInput* resident_{nullptr};
  bool residentHasData_{false};

  /// Pre-compression bytes merged so far.
  uint64_t rawBytesWritten_{0};
  /// Pre-compression bytes merged per partition.
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

  void spillRun(const CellWindowInput& in) override;
  void sealWindow(const CellWindowInput& in) override;
  void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return blocks_.bytesEvicted();
  }

 private:
  /// One writeBlocks() carrying the whole payload of every partition with rows.
  void sendWindow(const CellWindowInput& in);

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
