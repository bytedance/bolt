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
#include <optional>
#include <span>
#include <string>

#include "bolt/common/time/Timer.h"
#include "bolt/vector/ComplexVector.h"

#include "bolt/shuffle/sparksql/base/CompositeShuffleWriter.h"
#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/cell/CellNestedSplitter.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

class CellShuffleCheck;

/// Prepares logical rows for CellSplitter and schedules spills and seals.
/// Hash/range input includes a leading pid column; single/round-robin does not.
/// CellNestedSplitter writes nested columns, or the type adapter packs them.
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
    BOLT_CHECK(
        isCompositeActive() || isStopped(),
        "composite output accessed while ordinary side is active");
    return *blocks_;
  }
  ShuffleSpillFile* spillFile() override {
    return spillFile_.get();
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
    std::optional<NanosecondTimer> timer_;
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
  /// Before chunk growth, reserves capacity or spills local data. RSS
  /// pressure requests a seal at the next batch boundary.
  void prepareChunkGrowth();
  ColumnPositionCounts columnPositionCounts() const {
    return nestedSplitter_ != nullptr ? nestedSplitter_->columnPositionCounts()
                                      : ColumnPositionCounts{};
  }
  void sealWindow(SealReason reason);
  /// Seals the whole window - on RSS, pushes it - and returns its memory;
  /// returns the chunk bytes freed.
  int64_t releaseWindow(SealReason reason);
  void maybeSeal();
  /// Logs the "CellShuffleWriter pressure" line at stop, whether the
  /// output's finish succeeded or failed: pressure_, the null
  /// classification and seal-spill times, the spill memory's counters.
  void logPressure() const;
  /// Answers a reclaim by releasing the whole window (RSS).
  arrow::Status reclaimWindow(int64_t size, int64_t* actual);

  /// Lifecycle and reentrancy state of the ordinary (Cell) path.
  enum class State {
    kUninitialized,
    kIdle,
    kSplitting,
    kSealing,
  };

  bool isInitialized() const {
    return state_ != State::kUninitialized;
  }

  /// RSS (Celeborn) partition writer: windows are pushed whole, never spilled.
  const bool isRss_;

  State state_{State::kUninitialized};
  /// A window seal deferred to the next batch boundary (maybeSeal()).
  bool sealDeferred_{false};
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
  /// classifying their nulls, logged at stop. The nested splitter includes
  /// this work in its preparation time.
  uint64_t nullClassifyNs_{0};
  /// Total wall time of adapter_->pack() and splitBatch()/splitNested(), plus
  /// the nested seal/spill time bucketed by active phase so stopOrdinary()
  /// computes disjoint convertTime/flattenTime/splitTime/spillTime once.
  uint64_t convertWallNs_{0};
  uint64_t splitWallNs_{0};
  uint64_t convertSpillNs_{0};
  uint64_t flattenSpillNs_{0};
  uint64_t splitSpillNs_{0};
  uint64_t otherSpillNs_{0};
  uint64_t* activeSpillNs_{&otherSpillNs_};
  /// A SealSpillTimer is running.
  bool timingSealSpill_{false};

  /// Only retained when complex columns need packing.
  std::unique_ptr<CellShuffleTypeAdapter> adapter_;
  /// Set, in place of adapter_, when the schema takes a nested layout.
  std::unique_ptr<CellNestedLayout> nestedLayout_;
  CellLayout layout_;
  std::unique_ptr<ChunkAllocator> allocator_;
  /// Locally, the shared spill file (and its in-memory block cache) used by
  /// both outputs; declared before them, as it must outlive them. Null on
  /// Celeborn.
  std::unique_ptr<ShuffleSpillFile> spillFile_;
  /// The block output that serves the composite rows: locally a spill of
  /// its own, merged by output_ after the Cell payloads; on Celeborn also
  /// the push channel of those payloads, and finishing it ends the attempt
  /// for both. Outlives output_, which refers to it.
  std::unique_ptr<PartitionBlockOutput> blocks_;
  std::unique_ptr<CellOutput> output_;
  std::unique_ptr<CellSplitter> splitter_;
  std::unique_ptr<CellShuffleCheck> shuffleCheck_;
  std::unique_ptr<CellNestedSplitter> nestedSplitter_;

  /// Per row, its index among the batch's rows of its partition, for null
  /// positions; built only when a column has some nulls, counting through
  /// perPidCounter_.
  std::vector<uint32_t> rowIndexInPid_;
  std::vector<uint32_t> perPidCounter_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
