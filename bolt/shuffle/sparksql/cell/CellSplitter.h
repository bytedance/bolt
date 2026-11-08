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

#include <optional>
#include <span>

#include "bolt/common/memory/AllocationPool.h"
#include "bolt/shuffle/sparksql/cell/CellBuffer.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/vector/DecodedVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// One decoded input batch, pid column already excluded. Borrowed for the
/// duration of a split call.
struct SplitBatch {
  /// One DecodedVector per layout column. Mutable because
  /// nulls() may materialize lazily.
  std::span<DecodedVector> decoded;
  /// The partition of each row: one entry per row of the batch.
  std::span<const uint32_t> row2Partition;
  /// Rows this batch adds per partition (the partitioner's output); lets a
  /// whole-batch fast path work per partition instead of per row.
  std::span<const uint32_t> partition2RowCount;
  /// Per column, see NullClass.
  std::span<const NullClass> nullClass;
  /// Position of row r within its partition in this batch. The splitter
  /// adds the partition's position base to locate the null bit. May be
  /// empty when no column of this batch requires per-row null checks.
  std::span<const uint32_t> rowIndexInPid;
  /// The layout columns this batch covers; nullopt: every column.
  std::optional<std::span<const uint32_t>> columns;
  /// Per partition, how many positions the columns hold in the window
  /// before this batch. nullptr: the window's rows, which split() then
  /// advances by partition2RowCount; otherwise the caller advances its own.
  const uint32_t* positionBase = nullptr;

  uint32_t numRows() const {
    return static_cast<uint32_t>(row2Partition.size());
  }
};

/// Splits decoded column values into partitioned cells. Every (partition,
/// stream) owns one 64-byte cache line; the row loop writes values into those
/// lines and a full line becomes one Encoding Block (or a raw copy) appended
/// to the DataCells.
///
/// Locality by construction: a stream's cache lines are contiguous
/// (64B * P, L2-resident), its cursors are a P-byte array (L1-resident), and
/// the only scattered store of the hot loop lands inside that window.
/// Owns the cells, nulls and window metadata, and submits them to CellOutput.
/// The writer decides when to spill, seal a window or finish.
///
/// The window is every partition's open payload. Per-partition window
/// counts (windowRowCounts(), a row's position in the window) run since
/// that partition's payload opened: a partition seal closes single
/// payloads, so they need not have opened together.
class CellSplitter {
 public:
  /// The layout, allocator, output and pool must outlive this splitter.
  CellSplitter(
      const CellLayout& layout,
      uint32_t numPartitions,
      ChunkAllocator& allocator,
      CellOutput& output,
      memory::MemoryPool& pool);

  BOLT_CELL_KERNEL void split(const SplitBatch& batch);

  /// Per wire column, its position counts per partition, for its null
  /// bodies; a nullptr entry, or none set, means the partition's rows. The
  /// arrays must stay valid through every later output call.
  void setColumnPositions(std::vector<const uint32_t*> positions);

  /// Writes and recycles the linked cells, retaining caches and window state.
  /// Safe during a split or cache flush; nested output calls are suppressed.
  void spillRun();

  /// spillRun() of the largest partitions only, enough of them to cover
  /// `targetBytes` of cell data; every partition when that is all of it.
  /// The rest stay resident for the merge to write directly. Returns the
  /// cell bytes written.
  uint64_t spillPartitions(uint64_t targetBytes);

  /// The data bytes the cells hold, every partition's: what spillRun()
  /// writes.
  uint64_t cellDataBytes() const {
    return cells_.totalBytes();
  }

  /// DataCells::compact() between output calls: empties chunks for shrink()
  /// to return. Returns the bytes of the chunks emptied.
  uint64_t compactCells(uint64_t targetBytes);

  /// Flushes caches, drains cells (unless the output seals resident cells)
  /// and seals the window, then recycles every cell and resets its state.
  /// Called only at a complete batch boundary.
  void sealWindow();

  /// sealWindow() of the partitions whose bytes since their payload opened
  /// exceed `maxBytes`: only their payloads close, and only their cells are
  /// drained, so a skewed partition does not take every other partition's
  /// data through the spill file with it. Returns the partitions sealed.
  uint32_t sealPartitionsOver(uint64_t maxBytes);

  /// Flushes caches and finalizes the output, allowing the backend to consume
  /// resident cells directly. Populates output and dictionary metrics.
  void finish(ShuffleWriterMetrics& metrics);

  /// Largest bytes appended to one partition since its payload opened.
  /// Drives the partition seal and approximates that payload's size.
  uint64_t maxPartitionBytes() const {
    return maxPartitionBytes_;
  }

  bool hasWindowRows() const {
    return totalWindowRows_ != 0;
  }

  uint32_t maxWindowRows() const {
    return maxWindowRows_;
  }

  /// Rows per partition in the current window.
  std::span<const uint32_t> windowRowCounts() const {
    return windowRowCounts_;
  }

  /// Window and partition seals so far; a seal zeroes the rows of the
  /// partitions it closes.
  uint64_t sealCount() const {
    return sealCount_;
  }

  int64_t nullMovedBytes() const {
    return nulls_.movedCellBytes();
  }

  /// All memory the null state holds; see NullCells::footprintBytes().
  int64_t nullFootprintBytes() const {
    return nulls_.footprintBytes();
  }

  /// Reclaim must not change cells while the output is reading them.
  bool isWritingOutput() const {
    return writingOutput_;
  }

  /// Switches one string column to dictionary form for the
  /// writer's lifetime and sets its encoding tag. Called by the probe before
  /// the first split.
  void enableDictionary(uint32_t col);

 private:
  /// Encodes cache residues as stream tail blocks before a window closes:
  /// of the partitions `only` marks, or of every partition. Chunk growth
  /// during this step may still spill complete prior appends.
  void flushCaches(const uint8_t* only = nullptr);

  /// Clears nulls, row counts and dictionary state after successful output.
  void resetWindow();

  /// resetWindow() of the partitions `only` marks.
  void resetPartitions(const uint8_t* only);

  /// The Run step of spillRun()/spillPartitions(): writes the marked
  /// partitions' cells (every partition's when `only` is null) and
  /// recycles them.
  void drainCells(const uint8_t* only);

  /// The seal of the marked partitions (every partition when null).
  void seal(const uint8_t* only);

  CellWindowInput windowInput() const;

  const uint32_t* positionBase(const SplitBatch& batch) const {
    return batch.positionBase != nullptr ? batch.positionBase
                                         : windowRowCounts_.data();
  }

  /// Boolean values as 0/1 bytes of a Raw Data stream.
  template <bool kHasNulls>
  BOLT_CELL_KERNEL void splitBoolean(uint32_t col, const SplitBatch& batch);

  /// The full 16-byte Timestamp: seconds and nanos into two Bigint encoded
  /// streams whose cache cursors move in lockstep.
  template <bool kHasNulls, bool kIndexed>
  BOLT_CELL_KERNEL void splitTimestamp(uint32_t col, const SplitBatch& batch);

  /// log2(kCacheLineBytes): a line's offset from its stream's base.
  static constexpr uint32_t kLineShift = 6;
  static_assert(kCacheLineBytes == 1u << kLineShift);

  char* cacheLine(uint32_t stream, uint32_t pid) const {
    return cacheBase_ +
        ((static_cast<size_t>(stream) * numPartitions_ + pid) << kLineShift);
  }

  uint8_t* cursors(uint32_t stream) const {
    return cursors_ + static_cast<size_t>(stream) * numPartitions_;
  }

  /// Encodes the full or partial cache line of an encoded stream into
  /// the cells; `line` is the stream's staged line of partition `pid`, as
  /// cacheLine() gives it. Out of the hot loop by design.
  template <typename T>
  FOLLY_ALWAYS_INLINE void
  flushEncoded(uint32_t stream, uint32_t pid, const char* line, uint8_t* cur);

  /// Flushes a raw stream's staged cache line `line` bytes as they are.
  void flushRaw(uint32_t stream, uint32_t pid, const char* line, uint8_t* cur);

  void bumpPartitionBytes(uint32_t pid, uint64_t bytes) {
    partitionBytes_[pid] += bytes;
    if (partitionBytes_[pid] > maxPartitionBytes_) {
      maxPartitionBytes_ = partitionBytes_[pid];
    }
  }

  template <typename T, bool kHasNulls, bool kIndexed>
  BOLT_CELL_KERNEL void splitFixed(uint32_t col, const SplitBatch& batch);

  template <typename T, bool kHasNulls, bool kIndexed>
  BOLT_CELL_KERNEL void splitRawFixed(uint32_t col, const SplitBatch& batch);

  template <bool kHasNulls, bool kIndexed>
  BOLT_CELL_KERNEL void splitString(uint32_t col, const SplitBatch& batch);

  template <typename T>
  void dispatchEncoded(uint32_t col, const SplitBatch& batch, bool hasNulls);

  template <typename T>
  void dispatchRaw(uint32_t col, const SplitBatch& batch, bool hasNulls);

  /// Lifetime dictionary row counters for logging and metrics. Updated when
  /// segments close and in the fallback row loop, off the dictionary hit path.
  struct DictColumnStats {
    uint64_t matchedRows{0}; // rows written as dictionary indexes
    uint64_t fallbackRows{0}; // rows written through the fallback tail
  };

  template <bool kHasNulls, bool kIndexed>
  BOLT_CELL_KERNEL void splitStringDict(uint32_t col, const SplitBatch& batch);

  /// Per (partition, dictionary column): the open segment's bookkeeping, 8
  /// bytes. Its entries are staged in the column's idle data-stream cache
  /// line as [len][bytes]..., the line's cursor being the serialized size,
  /// and reach the cells only when the segment closes, so a spill never
  /// splits a dictionary across Runs. In dictionary mode the length cache
  /// stages index bytes. The probe walks the entries in the line, one load
  /// per entry yielding the length byte and the characters together.
  struct DictState {
    static constexpr uint8_t kModeDict = 0;
    static constexpr uint8_t kModeFallback = 1;
    /// Writer-side cap on entries per segment (the serialized budget would
    /// allow up to 63 one-byte entries; past the cap the segment just
    /// closes, which only costs a few framing bytes on degenerate
    /// vocabularies). Also bounds the boundary walk.
    static constexpr uint32_t kMaxEntries = 16;
    /// uniformLen values: every entry so far has this length (dictionary
    /// vocabularies usually do), so boundaries are arithmetic and the
    /// probe's loads are independent - and a value of any other length
    /// cannot match at all. kUniformMixed disables the shortcut.
    static constexpr uint8_t kUniformUnset = 0xFF;
    static constexpr uint8_t kUniformMixed = 0xFE;
    uint32_t matched{0}; // rows indexed by the open segment
    uint8_t mode{kModeDict};
    uint8_t entryCount{0};
    uint8_t uniformLen{kUniformUnset};
    uint8_t pad_{0};
  };
  static_assert(sizeof(DictState) == 8, "keep the sidecar a shift");

  /// Writes the open segment - the data-stream cache line's dataCur[pid]
  /// serialized bytes - as [entries][terminator][matched u32] into the
  /// data stream cells, then clears the open-segment fields and the
  /// cursor. Always appends the framing: callers skip a segment with no
  /// indexed row unless a demote owes the empty sequence.
  void closeDictSegment(
      uint32_t dataStream,
      uint32_t pid,
      DictState& st,
      uint8_t* dataCur,
      bool last);

  /// Appends one non-null value of a dictionary-mode partition. Returns
  /// false when the partition just demoted to the fallback tail (the value
  /// was not appended; the caller routes it through the fallback path).
  /// Always inlined: it runs once per non-null row.
  FOLLY_ALWAYS_INLINE bool appendDictValue(
      uint32_t lengthStream,
      uint32_t dataStream,
      uint32_t pid,
      DictState& st,
      const StringView& view,
      uint8_t* lengthCur,
      uint8_t* dataCur);

  const CellLayout& layout_;
  ChunkAllocator& allocator_;
  CellOutput& output_;
  const uint32_t numPartitions_;
  const uint32_t numStreams_;
  DataCells cells_;
  NullCells nulls_;
  bool writingOutput_{false};

  /// Rows committed per partition in the current window, updated after split.
  std::vector<uint32_t> windowRowCounts_;
  uint64_t totalWindowRows_{0};
  uint32_t maxWindowRows_{0};
  uint64_t sealCount_{0};
  std::vector<const uint32_t*> columnPositions_;
  /// One bit per wire column, set by enableDictionary for the writer lifetime.
  std::vector<uint8_t> encodingTags_;

  /// Cache lines (64B * P * S) and cursors (P * S), one arena allocation,
  /// huge-page backed above the threshold.
  memory::AllocationPool arena_;
  char* cacheBase_{nullptr};
  uint8_t* cursors_{nullptr};

  std::vector<uint64_t> partitionBytes_;
  std::vector<uint64_t> variableBytes_;
  uint64_t maxPartitionBytes_{0};

  /// Per column: dictionary form on/off (lifetime), and if on, one
  /// DictState per partition.
  std::vector<uint8_t> dictEnabled_;
  std::vector<std::vector<DictState>> dictStates_;
  std::vector<DictColumnStats> dictStats_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
