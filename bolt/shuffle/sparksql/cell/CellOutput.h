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
#include <iosfwd>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/RawPartitionOutput.h"
#include "bolt/shuffle/sparksql/cell/CellBuffer.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// What an output backend reads from the splitter when draining or sealing:
/// the live window state. Borrowed for the duration of a call.
struct CellWindowInput {
  const DataCells* cells;
  const NullCells* nulls;
  const CellLayout* layout;
  /// Per partition: rows and raw variable bytes of the current window.
  const uint32_t* rowCounts;
  const uint64_t* variableBytes;
  /// Payload encoding tags: ceil(C/8) bytes, one bit per
  /// column, set when the column is written in dictionary form. Constant
  /// for the writer's lifetime (the probe decides once).
  const uint8_t* encodingTags;
  uint32_t numPartitions;
};

/// The Cell shuffle writer's output backend. Run = physical
/// swap-out (may happen at any allocation point, never closes the logical
/// window); sealed window = payload boundary. Local and RSS backends differ
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

  /// Physically drains every DataCell chain into a Run of the current
  /// window. Does not release the cells: the splitter recycles them (so a
  /// spill fired from inside an append never frees held ids twice).
  virtual void spillRun(const CellWindowInput& in) = 0;

  /// Closes the logical window: records the null region and row counts.
  /// Precondition (splitter-orchestrated): caches flushed, and the cells
  /// drained unless sealsResidentCells().
  virtual void sealWindow(const CellWindowInput& in) = 0;

  /// Final merge. `windowHasData` says whether the current (unsealed)
  /// window still carries rows; the backend either seals it through disk or
  /// assembles it live when nothing was ever spilled. Fills the metrics
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

/// The ESS backend: Runs and sealed windows go to a spill file of its own;
/// finalize() merges them into the shuffle data file, partition-major,
/// together with the task's composite output.
///
/// Spill segments are stored in the exact Run body form of the wire format:
/// COMBINED when spill compression pays, COMBINED_UNCOMPRESSED otherwise. A
/// payload that is a single run (one spilled segment, or only resident
/// cells) is a header plus a verbatim copy. With coalesceMergedRuns,
/// several segments - or segments plus resident cells - are gathered
/// stream-major and written as one run; without it, each segment stays a
/// run of its own. A task whose data never needed a spill writes payloads
/// straight from the live cells and touches no temporary file at all.
class LocalCellOutput final : public CellOutput, public PartialSpiller {
 public:
  /// `composite` holds the task's CompositeRowVector blocks, merged after
  /// this output's payloads in every partition; it must outlive this
  /// object. `options` gives the codec, the data file and the spill
  /// directories; `pool` backs the gather/compress workspaces so they stay
  /// inside task memory accounting and must outlive this object.
  LocalCellOutput(
      PartialSpiller& composite,
      const CellLayout* layout,
      CellShuffleOptions cellOptions,
      PartitionWriterOptions options,
      memory::MemoryPool* pool);

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
  void mergeToFinal(uint32_t pid, DataFileWriter& out) override;

  struct SpilledRun {
    /// P + 1 absolute spill-file offsets; equal neighbours mean an empty
    /// segment for that partition.
    std::vector<uint64_t> pidEnds;
    /// Whether the run's COMBINED_UNCOMPRESSED segments already are their final
    /// form: there is no codec, or spill-time compression was tried and did
    /// not pay. False with compressSpill off, or when a pressure spill could
    /// not fund the compression workspace; merge then compresses them.
    bool storedIsFinal{false};
  };

  /// One partition's slice of a SpilledRun.
  struct SpillSegment {
    uint64_t begin;
    uint64_t end;
    bool storedIsFinal;
  };

  struct SealedWindow {
    std::vector<SpilledRun> runs;
    std::vector<uint64_t> nullOffset;
    std::vector<uint32_t> nullLength;
    std::vector<uint32_t> rowCounts;
    std::vector<uint64_t> variableBytes;
  };

  /// One INFO line per checkpoint window at finalize - rows, non-empty
  /// partitions, null bytes and every run's spill-file span - plus the
  /// residual window and a summary. Diagnostic output, capped so a
  /// checkpoint-heavy task cannot flood the log.
  void logWindowDiagnostics(const CellWindowInput& in, bool windowHasData);

  /// Appends "N runs [a, b, ...]" for one window's runs and returns their
  /// total spill bytes.
  static uint64_t describeRuns(
      const std::vector<SpilledRun>& runs,
      uint32_t numPartitions,
      std::ostream& os);

  /// The non-empty segments of `pid` across `runs`, oldest first.
  static std::vector<SpillSegment> partitionSegments(
      const std::vector<SpilledRun>& runs,
      uint32_t pid);

  /// Appends one partition's payload assembled from a sealed window.
  void writeDiskPayload(
      DataFileWriter& out,
      const SealedWindow& w,
      const uint8_t* encodingTags,
      uint32_t pid);

  /// Copies one spilled run segment into the data file: verbatim when it
  /// already is in final form, else through the compressing run writer -
  /// or still verbatim when that writer's workspace cannot be funded, since
  /// the stored form is a valid run too.
  void writeSpilledSegment(DataFileWriter& out, const SpillSegment& segment);

  /// Coalesced merge: reads every spilled segment of one partition
  /// (decompressing the ones compressed at spill time), lays their bytes
  /// out stream-major in gather_, and appends the still-resident cells -
  /// when `resident` is set - as the chronologically last piece of every
  /// stream. Fills the per-stream sizes and returns the total.
  uint64_t gatherPartitionRuns(
      const std::vector<SpillSegment>& segments,
      const CellWindowInput* resident,
      uint32_t pid,
      std::vector<uint64_t>& streamSizes);

  /// Compression policy shared by spill and merge: points `body`/`stored`
  /// at the COMBINED form when the codec shrinks the bytes, else at the
  /// input (the stored form when compression does not pay). Returns the
  /// layout to declare.
  RunLayout maybeCompressRun(
      const char* data,
      uint64_t dataBytes,
      const char*& body,
      uint64_t& stored);

  /// Writes one run to the data file, compressed per the policy above.
  /// `data` is the concatenated stream bytes; sizes are the per-stream
  /// decoded lengths.
  void writeRun(
      DataFileWriter& out,
      const char* data,
      uint64_t dataBytes,
      const uint64_t* decodedSizes);

  /// Appends one partition's payload for the current (unsealed) window:
  /// header, null body and row counts straight from memory, then the
  /// mid-window spilled runs and the still-resident cells - coalesced into
  /// one run, or as separate runs in that order. The residual never takes a
  /// spill round-trip.
  void writeCurrentWindowPayload(
      DataFileWriter& out,
      const CellWindowInput& in,
      uint32_t pid);

  /// The kind byte that opens every block on the wire.
  void appendBlockKind();

  PartialSpiller& composite_;
  const PartitionWriterOptions options_;
  const CellLayout* const layout_;
  const CellShuffleOptions cellOptions_;
  memory::MemoryPool* const pool_;
  /// Spill and merge codec; null when compressionType is UNCOMPRESSED.
  std::unique_ptr<Codec> codec_;
  /// Time metrics align with the V1/V2 partition writers and never
  /// overlap: compress = codec calls only; write = fwrite/fflush moments
  /// on the data file; evict = fwrite/fflush moments on the spill file.
  /// Assembly glue (chain scans, null bodies, spill read-back) is counted
  /// nowhere, as in V1 where it rides inside split.
  uint64_t compressTimeNs_{0};
  /// Pool-backed workspaces, released after every spill and after finalize
  /// so their capacity never sits on the reservation between uses. When the
  /// pool cannot fund them during a pressure spill, spillRun degrades to
  /// streaming the run out uncompressed instead of failing, and the merge
  /// makes the compression attempt for it.
  BufferPtr runScratch_;
  BufferPtr compressScratch_;
  BufferPtr gather_;

  /// Runs of the still-open window, then folded into a SealedWindow.
  std::vector<SpilledRun> openWindowRuns_;
  std::vector<SealedWindow> sealed_;
  /// The window still in memory, borrowed by finalize() for its merge.
  const CellWindowInput* residual_{nullptr};
  bool residualHasData_{false};

  uint64_t rawAccum_{0};
  /// Pre-compression bytes merged per partition.
  std::vector<int64_t> rawLengths_;
  BufferPtr scratch_;
};

/// The remote shuffle service (RSS) backend, e.g. Celeborn. The service may
/// reorder a mapper's pushes and interleave other mappers' pushes between
/// them, so every block is exactly one complete ColumnarPayload: a window is
/// only ever output whole, at a seal - one spillRaw() of every partition's
/// payload - and nothing is spilled. Each payload carries the partition's
/// resident cells as one run, compressed when that pays. The payloads go
/// through `raw`, the push channel the task's composite blocks share, which
/// finalize() ends once for both.
class RssCellOutput final : public CellOutput {
 public:
  /// `raw` must outlive this object. `options` gives the codec; `pool`
  /// backs the payload workspaces so they stay inside task memory
  /// accounting and must outlive this object.
  RssCellOutput(
      RawPartitionOutput& raw,
      const CellLayout* layout,
      const PartitionWriterOptions& options,
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
    return raw_.bytesEvicted();
  }

 private:
  /// One spillRaw() carrying the whole payload of every partition with rows.
  void sendWindow(const CellWindowInput& in);

  /// Assembles partition `pid`'s payload into `payload`.
  void
  buildPayload(const CellWindowInput& in, uint32_t pid, BufferPtr& payload);

  RawPartitionOutput& raw_;
  const CellLayout* const layout_;
  memory::MemoryPool* const pool_;
  /// Null when compressionType is UNCOMPRESSED.
  std::unique_ptr<Codec> codec_;

  BufferPtr compressScratch_;
  std::vector<uint64_t> streamSizes_;

  std::vector<int64_t> rawPartitionLengths_;
  uint64_t numSealed_{0};
  uint64_t compressTimeNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
