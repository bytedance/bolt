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

#include "bolt/shuffle/sparksql/ShuffleWriter.h"
#include "bolt/shuffle/sparksql/cell/CachedCellFrontend.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// L5 of the Cell shuffle: the writer orchestrating split, Run spill,
/// window seal and final merge.
///
/// Memory decisions never consult the split() memLimit parameter (it is
/// ignored by design): the only choke point is the chunk-grow callback,
/// where a failed maybeReserve — or the optional self cap — triggers a
/// physical Run spill; external reclaim drains and shrinks. Accounting is
/// the allocator's exact chunk/cell counters.
class CellShuffleWriter final : public ShuffleWriter {
 public:
  CellShuffleWriter(
      ShuffleWriterOptions options,
      memory::MemoryPool* boltPool,
      arrow::MemoryPool* arrowPool);

  arrow::Status split(RowVectorPtr rv, int64_t memLimitIgnored) override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status stop() override;

  const uint64_t cachedPayloadSize() const override {
    return 0; // no payload cache layer, by design
  }

  std::string toString() const override;

 private:
  void initOnFirstBatch(const RowVector& rv);
  arrow::Status splitBatch(RowVectorPtr rv);
  const int32_t* pidArray(const RowVector& rv);
  void onBeforeChunkGrow();
  void spillRunNow();
  void checkpoint();
  void maybeCheckpoint();
  CellWindowInput windowInput();

  memory::MemoryPool* const boltPool_;

  bool initialized_{false};
  bool inSplit_{false};
  bool spilling_{false};
  bool stopped_{false};
  bool checkpointRequested_{false};

  CellLayout layout_;
  std::unique_ptr<ChunkAllocator> allocator_;
  std::unique_ptr<DataCells> cells_;
  std::unique_ptr<NullCells> nulls_;
  std::unique_ptr<SplitFrontend> frontend_;
  std::unique_ptr<CellOutput> output_;

  std::vector<uint32_t> row2Partition_;
  std::vector<uint32_t> partition2RowCount_;
  /// Rows per partition since the window opened; the null-bit base offset.
  std::vector<uint32_t> windowRowStart_;
  std::vector<uint32_t> rowIndexInPid_;
  std::vector<uint32_t> perPidCounter_;
  std::vector<int32_t> pidValues_;
  std::vector<DecodedVector> decoded_;
  std::vector<BatchNullClass> nullClass_;
  DecodedVector pidDecoded_;

  uint64_t totalWindowRows_{0};
  uint32_t maxWindowRows_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
