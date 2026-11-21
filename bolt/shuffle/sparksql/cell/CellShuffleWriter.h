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
#include <span>
#include <string>

#include "bolt/vector/ComplexVector.h"

#include "bolt/shuffle/sparksql/base/CompositeShuffleWriter.h"
#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/cell/CellNestedShredder.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

class CellShuffleCheck;

/// Prepares logical rows for CellSplitter and schedules spills and seals.
/// Hash/range input includes a leading pid column; single/round-robin does not.
/// Nested columns are shredded or packed by the type adapter.
///
/// Local memory pressure releases buffered spill data, then cell partitions.
/// Celeborn pushes whole windows at batch boundaries. Memory limits come from
/// the pool and task; split()'s memLimit is unused.
class CellShuffleWriter final : public CompositeShuffleWriter {
 public:
  CellShuffleWriter(
      ShuffleWriterOptions options,
      memory::MemoryPool* boltPool,
      arrow::MemoryPool* arrowPool);

  ~CellShuffleWriter() override;

  const uint64_t cachedPayloadSize() const override {
    return 0;
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
  SpillMemory* spillMemory() override {
    return spillMemory_.get();
  }

 private:
  /// Why a whole window was sealed.
  enum class SealReason : uint8_t {
    kPositionLimit,
    kMemoryPressure,
    kNullBitmapLimit,
    kComposite,
    kReserveFailed,
    kCount,
  };

  /// What memory pressure made the writer do, counted per event - never per
  /// row - and logged once at stop.
  struct PressureStats {
    /// Reclaims, and those answered without a run: by idle chunks and the
    /// spill memory, refused as the cells held less than the run floor, or
    /// deferred to the next batch boundary (remote, mid-batch).
    uint64_t reclaims{0};
    uint64_t idleReclaims{0};
    uint64_t belowFloorReclaims{0};
    uint64_t deferredReclaims{0};
    uint64_t requestedBytes{0};
    uint64_t freedBytes{0};
    /// Bytes freed by each step of a reclaim: idle chunks, the spill memory
    /// written out, then the chunks runs and compaction empty.
    uint64_t freedIdle{0};
    uint64_t freedSpillMemory{0};
    uint64_t freedByPartialRun{0};
    uint64_t freedByCompaction{0};
    uint64_t freedByRun{0};
    uint64_t freedByPush{0};
    /// Partial runs of the largest partitions: asked by a reclaim, or by
    /// the chunk growth past the cap or the reservation; with cell bytes.
    uint64_t reclaimPartialRuns{0};
    uint64_t reclaimPartialRunBytes{0};
    uint64_t growthPartialRuns{0};
    uint64_t growthPartialRunBytes{0};
    /// Chunk growths that writing the spill memory out made room for.
    uint64_t growthFlushes{0};
    /// Cell compactions and the chunk bytes they emptied.
    uint64_t compactions{0};
    uint64_t compactedBytes{0};
    /// Whole-window runs a reclaim fell back to.
    uint64_t fullRuns{0};
    /// Partial runs the cell threshold asked for at a batch boundary,
    /// without memory pressure; with cell bytes.
    uint64_t thresholdRuns{0};
    uint64_t thresholdRunBytes{0};
    std::array<uint64_t, static_cast<size_t>(SealReason::kCount)> seals{};
    /// Seals of oversized partitions alone, and the partitions they closed.
    uint64_t partitionSeals{0};
    uint64_t partitionsSealed{0};
    /// The most memory the null state held after a batch.
    uint64_t nullPeakBytes{0};

    /// One key=value line.
    std::string toString() const;
  };

  /// Times only the outermost seal/spill within split(), so releaseWindow ->
  /// sealWindow and allocation-triggered reclaims do not double count time.
  /// External reclaims and stop() are accounted separately.
  class SealSpillTimer {
   public:
    explicit SealSpillTimer(CellShuffleWriter& writer);
    ~SealSpillTimer();

    SealSpillTimer(const SealSpillTimer&) = delete;
    SealSpillTimer& operator=(const SealSpillTimer&) = delete;

   private:
    /// Null when this one times nothing.
    CellShuffleWriter* const writer_;
    const uint64_t start_;
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
      std::span<const DecodedVector> decoded,
      std::span<const NullClass> nullClass);
  /// Before chunk growth, reserves capacity or spills local data. Remote
  /// pressure requests a seal at the next batch boundary.
  void prepareChunkGrowth();
  ColumnPositionCounts columnPositionCounts() const {
    return shredder_ != nullptr ? shredder_->columnPositionCounts()
                                : ColumnPositionCounts{};
  }
  void sealWindow(SealReason reason);
  /// Seals the whole window - on a remote shuffle, pushes it - and returns
  /// its memory; returns the chunk bytes freed.
  int64_t releaseWindow(SealReason reason);
  void maybeSeal();
  /// Logs the "CellShuffleWriter pressure" line at stop, whether the
  /// output's finish succeeded or failed: pressure_, the null
  /// classification and seal-spill times, the spill memory's counters.
  void logPressure() const;
  /// Answers a reclaim by releasing the whole window (remote).
  arrow::Status reclaimWindow(int64_t size, int64_t* actual);

  /// Celeborn partition writer: windows are pushed whole, never spilled.
  const bool isRemote_;

  bool initialized_{false};
  bool inSplit_{false};
  bool sealRequested_{false};
  bool sealing_{false};
  bool dictProbed_{false};
  /// The run floor: a reclaim of a given size writes no run while the cells
  /// hold less data than this. CellShuffleOptions::reclaimRunFloorBytes, at
  /// most an eighth of the memory limit, set at the first batch; 0 is off.
  int64_t reclaimRunFloorBytes_{0};
  /// The memory threshold on the chunk memory plus the spill memory:
  /// CellShuffleOptions::cellMemorySpillRatio of the memory limit, set at
  /// the first batch; 0 is off.
  int64_t memorySpillThresholdBytes_{0};
  /// The cell threshold: cell data past it spills at the next batch
  /// boundary, down to half. CellShuffleOptions::cellSpillBytesPerStream
  /// times the partitions and streams, saturating, and at least
  /// cellSpillMinBytes; set at the first batch.
  uint64_t cellSpillThresholdBytes_{0};
  PressureStats pressure_;
  /// Time the flat path spends decoding the columns of its batches and
  /// classifying their nulls, logged at stop. The nested path's is part of
  /// the shred time.
  uint64_t nullClassifyNs_{0};
  /// What SealSpillTimer measured: sealSpillTime at stop. splitTime,
  /// convertTime and the shred time leave it out.
  uint64_t sealSpillNs_{0};
  /// A SealSpillTimer is running.
  bool timingSealSpill_{false};

  /// Only retained when complex columns need packing.
  std::unique_ptr<CellShuffleTypeAdapter> adapter_;
  /// Set, in place of adapter_, when the schema takes a nested layout.
  std::unique_ptr<CellNestedLayout> nestedLayout_;
  CellLayout layout_;
  std::unique_ptr<ChunkAllocator> allocator_;
  /// Locally, where both outputs' spill files keep their newest bytes;
  /// declared before them, as it must outlive them. Null on Celeborn.
  std::unique_ptr<SpillMemory> spillMemory_;
  /// The block output that serves the composite rows: locally a spill of
  /// its own, merged by output_ after the Cell payloads; on Celeborn also
  /// the push channel of those payloads, and finishing it ends the attempt
  /// for both. Outlives output_, which refers to it.
  std::unique_ptr<PartitionBlockOutput> blocks_;
  std::unique_ptr<CellOutput> output_;
  std::unique_ptr<CellSplitter> splitter_;
  std::unique_ptr<CellShuffleCheck> shuffleCheck_;
  std::unique_ptr<CellNestedShredder> shredder_;

  /// Per row, its index among the batch's rows of its partition, for null
  /// positions; built only when a column has some nulls, counting through
  /// perPidCounter_.
  std::vector<uint32_t> rowIndexInPid_;
  std::vector<uint32_t> perPidCounter_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
