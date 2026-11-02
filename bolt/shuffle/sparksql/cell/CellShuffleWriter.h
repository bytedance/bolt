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

#include <array>
#include <string>

#include "bolt/vector/ComplexVector.h"

#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/cell/CellNestedShredder.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/shuffle/sparksql/cell/CompositeShuffleWriter.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Prepares decoded and partitioned input batches and schedules spills,
/// window seals and final output through CellSplitter.
///
/// Accepts logical rows (pid column first), so callers never see the
/// physical Cell schema: the columns of a nested layout are shredded into
/// plain ones by a CellNestedShredder, and other complex columns packed into
/// the trailing binary column by the type adapter. CompositeRowVectors are
/// handled entirely by the CompositeShuffleWriter base.
///
/// Memory decisions never consult the split() memLimit parameter (it is
/// ignored by design): the only choke point is reserveChunk() at chunk growth,
/// where a failed maybeReserve — or the optional self cap — triggers a
/// physical Run spill; external reclaim drains and shrinks. Accounting is
/// the allocator's exact chunk/cell counters.
///
/// On Celeborn a window can only be pushed whole, so every one of those
/// spills becomes a seal at the next batch boundary instead: batches are
/// sliced small, each slice is admitted against the pool first, and a
/// reclaim arriving mid-batch is deferred to the boundary.
class CellShuffleWriter final : public CompositeShuffleWriter {
 public:
  CellShuffleWriter(
      ShuffleWriterOptions options,
      memory::MemoryPool* boltPool,
      arrow::MemoryPool* arrowPool);

  ~CellShuffleWriter() override;

  const uint64_t cachedPayloadSize() const override {
    return 0; // no payload cache layer, by design
  }

  std::string toString() const override;

 protected:
  arrow::Status splitOrdinary(RowVectorPtr rv, int64_t memLimitIgnored)
      override;
  arrow::Status reclaimOrdinary(int64_t size, int64_t* actual) override;
  arrow::Status flushOrdinary() override;
  arrow::Status stopOrdinary() override;
  PartitionBlockOutput& output() override {
    return *blocks_;
  }

 private:
  /// Why a whole window was sealed.
  enum class SealReason : uint8_t {
    kRows, // a payload's rows would pass kMaxPayloadRows
    kMemory, // memory pressure asked for it
    kNulls, // the null bitmaps passed nullMemLimitBytes
    kFlush, // composite rows come next
    kReserve, // the next slice could not be reserved
    kCount,
  };

  /// What memory pressure made the writer do, counted per event - never per
  /// row - and logged once at stop.
  struct PressureStats {
    /// Reclaims, and those answered with idle chunks alone, or deferred to
    /// the next batch boundary (remote, mid-batch).
    uint64_t reclaims{0};
    uint64_t idleReclaims{0};
    uint64_t deferredReclaims{0};
    uint64_t requestedBytes{0};
    uint64_t freedBytes{0};
    /// Chunk bytes freed by each step of a reclaim.
    uint64_t freedIdle{0};
    uint64_t freedBySpill{0};
    uint64_t freedByCompaction{0};
    uint64_t freedByRun{0};
    uint64_t freedByPush{0};
    /// Partial runs of the largest partitions: asked by a reclaim, or by
    /// the chunk growth past the cap or the reservation; with cell bytes.
    uint64_t reclaimSpills{0};
    uint64_t reclaimSpillBytes{0};
    uint64_t growthSpills{0};
    uint64_t growthSpillBytes{0};
    /// Cell compactions and the chunk bytes they emptied.
    uint64_t compactions{0};
    uint64_t compactedBytes{0};
    /// Whole-window runs a reclaim fell back to.
    uint64_t fullRuns{0};
    std::array<uint64_t, static_cast<size_t>(SealReason::kCount)> seals{};
    /// Seals of oversized partitions alone, and the partitions they closed.
    uint64_t partitionSeals{0};
    uint64_t partitionsSealed{0};
    /// The most memory the null state held after a batch.
    uint64_t nullPeakBytes{0};

    /// One key=value line.
    std::string toString() const;
  };

  void initOnFirstBatch(const RowVector& rv);
  arrow::Status splitBatch(RowVectorPtr rv);
  /// splitBatch() of a nested layout: seals the window first when the
  /// batch's positions do not fit beside it.
  arrow::Status splitNested(RowVectorPtr rv);
  /// The single dictionary probe (first batch, lifetime decision) of the
  /// string columns - a nested layout's at the rows' positions - as
  /// `decoded` and `nullClass` hold them.
  void probeDictionary(
      uint32_t numRows,
      const DecodedVector* decoded,
      const NullClass* nullClass);
  /// The allocator's ReserveChunkFn: reserves a chunk's memory within
  /// cellMemoryCapBytes. Locally, when it cannot, it spills the largest
  /// partitions until half the cell data is covered, and the growth then
  /// takes their recycled cells. Remotely, a window is released only whole
  /// at a batch boundary, so it requests that and lets this growth proceed.
  void reserveChunk();
  void checkpoint(SealReason reason);
  /// Seals the whole window - on a remote shuffle, pushes it - and returns
  /// its memory; returns the chunk bytes freed.
  int64_t releaseWindow(SealReason reason);
  void maybeCheckpoint();
  /// Answers a reclaim by releasing the whole window (remote).
  arrow::Status reclaimWindow(int64_t size, int64_t* actual);

  /// Celeborn partition writer: windows are pushed whole, never spilled.
  const bool remote_;

  bool initialized_{false};
  bool inSplit_{false};
  bool stopped_{false};
  bool checkpointRequested_{false};
  bool sealing_{false};
  bool dictProbed_{false};
  PressureStats pressure_;
  /// Time the flat path spends decoding the columns of its batches and
  /// classifying their nulls, logged at stop. The nested path's is part of
  /// the shred time.
  uint64_t nullClassifyNs_{0};

  std::unique_ptr<CellShuffleTypeAdapter> adapter_;
  /// Set, in place of adapter_, when the schema takes a nested layout.
  std::unique_ptr<CellNestedLayout> nested_;
  CellLayout layout_;
  std::unique_ptr<ChunkAllocator> allocator_;
  /// The block output that serves the composite rows: locally a spill of
  /// its own, merged by output_ after the Cell payloads; on Celeborn also
  /// the push channel of those payloads, and finishing it ends the attempt
  /// for both. Outlives output_, which refers to it.
  std::unique_ptr<PartitionBlockOutput> blocks_;
  std::unique_ptr<CellOutput> output_;
  std::unique_ptr<CellSplitter> splitter_;
  std::unique_ptr<CellNestedShredder> shredder_;

  std::vector<uint32_t> rowIndexInPid_;
  std::vector<uint32_t> perPidCounter_;
  std::vector<DecodedVector> decoded_;
  std::vector<NullClass> nullClass_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
