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

#include "bolt/common/memory/AllocationPool.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/vector/DecodedVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Per-column null shape of one batch. Resolved by the writer with a
/// word-level scan of the decoded nulls, because mayHaveNulls() only says a
/// nulls buffer EXISTS - upstream operators routinely attach defensively
/// allocated, all-set buffers, and trusting them would tax every row of the
/// loop for nulls that are not there.
enum class BatchNullClass : uint8_t {
  kNoNulls = 0, // buffer absent, all-set, or constant non-null
  kSomeNulls = 1, // genuinely mixed: the per-row null path applies
  kAllNull = 2, // every batch row null: one counted run per partition
};

/// One decoded input batch, pid column already excluded. Borrowed for the
/// duration of a split call.
struct SplitBatch {
  /// One DecodedVector per logical column of the layout. Mutable because
  /// nulls() may materialize lazily.
  DecodedVector* decoded;
  const uint32_t* row2Partition;
  /// Rows this batch adds per partition (the partitioner's output); lets a
  /// whole-batch fast path work per partition instead of per row.
  const uint32_t* partition2RowCount;
  /// Per column, see BatchNullClass.
  const BatchNullClass* nullClass;
  uint32_t numRows;
  /// Position of row r within its partition in this batch. The splitter
  /// adds its current window row count to locate the null bit. May be null
  /// when no column of this batch requires per-row null checks.
  const uint32_t* rowIndexInPid;
};

/// Splits decoded column values into partitioned cells. Every (partition,
/// stream) owns one 64-byte cache line; the row loop writes values into those
/// lines and a full line becomes one Encoding Block (or a raw copy) appended
/// to the DataCells.
///
/// Locality by construction: a column's cache lines are contiguous
/// (64B * P, L2-resident), its cursors are a P-byte array (L1-resident), and
/// the only scattered store of the hot loop lands inside that window.
/// Owns the cells, nulls and window metadata, and submits them to CellOutput.
/// The writer decides when to spill, seal a window or finish.
class CellSplitter {
 public:
  /// The layout, allocator, output and pool must outlive this splitter.
  CellSplitter(
      const CellLayout& layout,
      uint32_t numPartitions,
      ChunkAllocator& allocator,
      CellOutput& output,
      memory::MemoryPool& pool);

  void split(const SplitBatch& batch);

  /// Switches one string column to dictionary form (spec section 8) for the
  /// writer's lifetime and sets its encoding tag. Called by the probe before
  /// the first split.
  void enableDictionary(uint32_t col);

  /// Writes and recycles the linked cells, retaining caches and window state.
  /// Safe during a split or cache flush; nested output calls are suppressed.
  void spillRun();

  /// Flushes caches, drains cells and seals the window, then resets its state.
  /// Called only at a complete batch boundary.
  void sealWindow();

  /// Flushes caches and finalizes the output, allowing the backend to consume
  /// resident cells directly. Populates output and dictionary metrics.
  void finish(ShuffleWriterMetrics& metrics);

  /// Maximum bytes appended to a partition since the window opened; drives
  /// the checkpoint trigger and approximates its payload size.
  uint64_t maxPartitionBytes() const {
    return maxPartitionBytes_;
  }

  bool hasWindowRows() const {
    return totalWindowRows_ != 0;
  }

  uint32_t maxWindowRows() const {
    return maxWindowRows_;
  }

  int64_t nullBytes() const {
    return nulls_.allocatedBytes();
  }

  /// Reclaim must not change cells while the output is reading them.
  bool isWritingOutput() const {
    return writingOutput_;
  }

  /// Fixed memory held by the splitter (caches, cursors).
  int64_t residentBytes() const {
    return residentBytes_;
  }

 private:
  /// Lifetime dictionary row counters for logging and metrics. Updated when
  /// segments close and in the fallback row loop, off the dictionary hit path.
  struct DictColumnStats {
    uint64_t matchedRows{0}; // rows written as dictionary indexes
    uint64_t fallbackRows{0}; // rows written through the fallback tail
  };

  /// Encodes cache residues as stream tail blocks before a window closes.
  /// Chunk growth during this step may still spill complete prior appends.
  void flushAll();

  /// Clears nulls, row counts and dictionary state after successful output.
  void resetWindow();

  CellWindowInput windowInput() const;

  /// Boolean values as 0/1 bytes of a Raw Data stream.
  template <bool kHasNulls>
  void splitBoolean(uint32_t col, const SplitBatch& batch);

  /// The full 16-byte Timestamp: seconds and nanos into two Bigint
  /// Encoding Loop streams whose cache cursors move in lockstep.
  template <bool kHasNulls, bool kIndexed>
  void splitTimestamp(uint32_t col, const SplitBatch& batch);

  char* cacheLine(uint32_t stream, uint32_t pid) const {
    return cacheBase_ +
        ((static_cast<size_t>(stream) * numPartitions_ + pid) << 6);
  }

  uint8_t* cursors(uint32_t stream) const {
    return cursors_ + static_cast<size_t>(stream) * numPartitions_;
  }

  /// Encodes the full or partial cache line of an Encoding Loop stream into
  /// the cells. Out of the hot loop by design.
  template <typename T>
  FOLLY_ALWAYS_INLINE void
  flushEncoded(uint32_t stream, uint32_t pid, uint8_t* cur);

  /// Flushes a raw stream's cache line bytes as they are.
  void flushRaw(uint32_t stream, uint32_t pid, uint8_t* cur);

  void bumpPartitionBytes(uint32_t pid, uint64_t bytes) {
    partitionBytes_[pid] += bytes;
    if (partitionBytes_[pid] > maxPartitionBytes_) {
      maxPartitionBytes_ = partitionBytes_[pid];
    }
  }

  template <typename T, bool kHasNulls, bool kIndexed>
  void splitFixed(uint32_t col, const SplitBatch& batch);

  template <typename T, bool kHasNulls, bool kIndexed>
  void splitRawFixed(uint32_t col, const SplitBatch& batch);

  template <bool kHasNulls, bool kIndexed>
  void splitString(uint32_t col, const SplitBatch& batch);

  template <bool kHasNulls, bool kIndexed>
  void splitStringDict(uint32_t col, const SplitBatch& batch);

  /// Per (partition, dictionary column): the open dictionary segment. Its
  /// entries live here, never in the cells, until the segment closes - a
  /// spill between two closes must not split a dictionary across Runs
  /// (spec section 5.4). While `mode` is dictionary, the column's length
  /// cache stages raw index bytes and its data cache stays empty.
  /// Per (partition, dictionary column) bookkeeping only - 8 bytes. The
  /// dictionary bytes themselves live in the column's DATA-stream cache
  /// line, which is idle while the partition is in dictionary mode (hits
  /// write nothing but an index byte to the length stream), in the wire
  /// serialization [len][bytes][len][bytes]... with no unpacked lengths,
  /// offsets or key shadows (a hard design constraint). The line's cursor
  /// doubles as the serialized byte count. Only what the wire framing
  /// needs (matched) plus mode and entry count lives here, an
  /// L1-resident array. The probe walks the boundary chain in the line;
  /// each step is one 8-byte load yielding the length byte and up to
  /// seven content bytes together, so a short value compares whole in the
  /// same register.
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
  /// cursor. A segment with no indexed row is written only when `last`
  /// requires the framing (a demote before any hit still owes the empty
  /// sequence).
  void closeDictSegment(
      uint32_t dataStream,
      uint32_t pid,
      DictState& st,
      uint8_t* dataCur,
      bool last);

  /// Appends one non-null value of a dictionary-mode partition. `key` is
  /// the value's first 8 StringView bytes, loaded once by the caller; its
  /// low word is the size. Returns false when the partition just demoted
  /// to the fallback tail (the value was not appended; the caller routes
  /// it through the fallback path). Always inlined into the row loop: a
  /// per-row call was 13% of the dictionary split profile.
  FOLLY_ALWAYS_INLINE bool appendDictValue(
      uint32_t lengthStream,
      uint32_t dataStream,
      uint32_t pid,
      DictState& st,
      const StringView& view,
      uint64_t key,
      uint8_t* lengthCur,
      uint8_t* dataCur);

  template <typename T>
  void dispatchEncoded(uint32_t col, const SplitBatch& batch, bool hasNulls);

  template <typename T>
  void dispatchRaw(uint32_t col, const SplitBatch& batch, bool hasNulls);

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
  /// One bit per wire column, set by enableDictionary for the writer lifetime.
  std::vector<uint8_t> encodingTags_;

  /// Cache lines (64B * P * S) and cursors (P * S), one arena allocation,
  /// huge-page backed above the threshold.
  memory::AllocationPool arena_;
  char* cacheBase_{nullptr};
  uint8_t* cursors_{nullptr};
  int64_t residentBytes_{0};

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
