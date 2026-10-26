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

#include "bolt/vector/ComplexVector.h"

#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/shuffle/sparksql/cell/CompositeShuffleWriter.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Prepares decoded and partitioned input batches and schedules spills,
/// window seals and final output through CellSplitter.
///
/// Accepts logical rows (pid column first): complex columns are packed into
/// the trailing binary column by the type adapter here, so callers never
/// see the physical Cell schema. CompositeRowVectors are handled entirely by
/// the CompositeShuffleWriter base.
///
/// Memory decisions never consult the split() memLimit parameter (it is
/// ignored by design): the only choke point is the chunk-grow callback,
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
  RawPartitionOutput& output() override {
    return *raw_;
  }

 private:
  void initOnFirstBatch(const RowVector& rv);
  /// Splits one physical (adapter-converted) batch, slicing oversized ones.
  arrow::Status splitPhysical(RowVectorPtr rv);
  arrow::Status splitBatch(RowVectorPtr rv);
  /// Remote only: pushes the window first when the next slice of about
  /// `estimatedBytes` cannot be reserved.
  void admitRemoteSlice(int64_t estimatedBytes);
  /// The single dictionary probe (first batch, lifetime decision).
  void probeDictionary(uint32_t numRows);
  void onBeforeChunkGrow();
  void checkpoint();
  /// Remote: seals (pushes) the window and returns its memory; returns the
  /// chunk bytes freed.
  int64_t flushRemoteWindow();
  void maybeCheckpoint();
  arrow::Status reclaimRemote(int64_t size, int64_t* actual);

  /// Celeborn partition writer: windows are pushed whole, never spilled.
  const bool remote_;

  bool initialized_{false};
  bool inSplit_{false};
  bool stopped_{false};
  bool checkpointRequested_{false};
  bool sealing_{false};
  bool dictProbed_{false};

  std::unique_ptr<CellShuffleTypeAdapter> adapter_;
  CellLayout layout_;
  std::unique_ptr<ChunkAllocator> allocator_;
  /// The composite blocks' output: locally a spill of its own, merged by
  /// output_ after the Cell payloads; on Celeborn also the push channel of
  /// those payloads. Outlives output_, which refers to it.
  std::unique_ptr<RawPartitionOutput> raw_;
  std::unique_ptr<CellOutput> output_;
  std::unique_ptr<CellSplitter> splitter_;

  std::vector<uint32_t> rowIndexInPid_;
  std::vector<uint32_t> perPidCounter_;
  std::vector<DecodedVector> decoded_;
  std::vector<BatchNullClass> nullClass_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
