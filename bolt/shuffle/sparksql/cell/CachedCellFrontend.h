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
#include "bolt/shuffle/sparksql/cell/SplitFrontend.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// The CacheCell split front end: every (partition, stream) owns one 64-byte
/// cache line; the row loop writes values into those lines and a full line
/// becomes one Encoding Block (or a raw copy) appended to the DataCells.
///
/// Locality by construction: a column's cache lines are contiguous
/// (64B * P, L2-resident), its cursors are a P-byte array (L1-resident), and
/// the only scattered store of the hot loop lands inside that window.
class CachedCellFrontend final : public SplitFrontend {
 public:
  CachedCellFrontend(
      const CellLayout* layout,
      DataCells* cells,
      NullCells* nulls,
      memory::MemoryPool* pool,
      ChunkAllocator::GrowCallback beforeGrow);

  void split(const SplitBatch& batch) override;
  void enableDictionary(uint32_t col) override;
  void flushAll() override;

  uint64_t partitionBytes(uint32_t pid) const override {
    return partitionBytes_[pid];
  }

  uint64_t maxPartitionBytes() const override {
    return maxPartitionBytes_;
  }

  uint64_t variableBytes(uint32_t pid) const override {
    return variableBytes_[pid];
  }

  const uint64_t* variableBytesArray() const override {
    return variableBytes_.data();
  }

  void resetWindowStats() override;

  int64_t residentBytes() const override {
    return residentBytes_;
  }

 private:
  char* cacheLine(uint32_t stream, uint32_t pid) const {
    return cacheBase_ + ((static_cast<size_t>(stream) * numPartitions_ + pid)
                         << 6);
  }

  uint8_t* cursors(uint32_t stream) const {
    return cursors_ + static_cast<size_t>(stream) * numPartitions_;
  }

  /// Encodes the full or partial cache line of an Encoding Loop stream into
  /// the cells. Out of the hot loop by design.
  template <typename T>
  FOLLY_ALWAYS_INLINE void flushEncoded(uint32_t stream, uint32_t pid, uint8_t* cur);

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
  /// 128 bytes, power-of-two stride: the per-row state address is a shift.
  /// The dictionary is stored ONLY in its wire serialization,
  /// [len][bytes][len][bytes]... - no unpacked lengths, offsets or key
  /// shadows (a hard design constraint). The probe walks the boundary
  /// chain, but each step is one 8-byte load that yields the length byte
  /// and up to seven content bytes together, so a short value compares
  /// whole in the same register and the walk never touches a second
  /// stream of loads.
  struct alignas(128) DictState {
    static constexpr uint8_t kModeDict = 0;
    static constexpr uint8_t kModeFallback = 1;
    /// Writer-side cap on entries per segment (the serialized budget would
    /// allow up to 63 one-byte entries; past the cap the segment just
    /// closes, which only costs a few framing bytes on degenerate
    /// vocabularies). Also bounds the boundary walk.
    static constexpr uint32_t kMaxEntries = 16;
    uint32_t matched{0}; // rows indexed by the open segment
    uint8_t mode{kModeDict};
    uint8_t entryCount{0};
    uint8_t entryBytes{0}; // serialized [len][bytes] total, <= 63
    /// Wire-form entries; close is a straight copy. Eight slack bytes at
    /// the end keep the walk's 8-byte window load inside the array when it
    /// anchors on the last boundary.
    char entries[kDictSerializedBudget + 1 + 8];
  };
  static_assert(sizeof(DictState) == 128, "keep the stride a shift");

  /// Writes the open segment as [entries][terminator][matched u32] into the
  /// data stream and clears the open-segment fields. A segment with no
  /// indexed row is written only when `last` requires the framing (a
  /// demote before any hit still owes the empty sequence).
  void closeDictSegment(
      uint32_t dataStream,
      uint32_t pid,
      DictState& st,
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
      uint8_t* lengthCur);

  template <typename T>
  void dispatchEncoded(uint32_t col, const SplitBatch& batch, bool hasNulls);

  template <typename T>
  void dispatchRaw(uint32_t col, const SplitBatch& batch, bool hasNulls);

  const CellLayout* const layout_;
  DataCells* const cells_;
  NullCells* const nulls_;
  const ChunkAllocator::GrowCallback beforeGrow_;
  const uint32_t numPartitions_;
  const uint32_t numStreams_;

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
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
