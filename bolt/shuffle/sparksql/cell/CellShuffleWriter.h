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
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Prepares decoded and partitioned input batches and schedules spills,
/// window seals and final output through CellSplitter.
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

  ~CellShuffleWriter() override;

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
  /// The single dictionary probe (first batch, lifetime decision).
  void probeDictionary(uint32_t numRows);
  const int32_t* pidArray(const RowVector& rv);
  void onBeforeChunkGrow();
  void checkpoint();
  void maybeCheckpoint();

  memory::MemoryPool* const boltPool_;

  bool initialized_{false};
  bool inSplit_{false};
  bool stopped_{false};
  bool checkpointRequested_{false};
  bool dictProbed_{false};

  CellLayout layout_;
  std::unique_ptr<ChunkAllocator> allocator_;
  std::unique_ptr<CellOutput> output_;
  std::unique_ptr<CellSplitter> splitter_;

  std::vector<uint32_t> row2Partition_;
  std::vector<uint32_t> partition2RowCount_;
  std::vector<uint32_t> rowIndexInPid_;
  std::vector<uint32_t> perPidCounter_;
  std::vector<int32_t> pidValues_;
  std::vector<DecodedVector> decoded_;
  std::vector<BatchNullClass> nullClass_;
  DecodedVector pidDecoded_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
