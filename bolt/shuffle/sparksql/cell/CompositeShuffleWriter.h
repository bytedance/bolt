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
#include "bolt/vector/DecodedVector.h"

#include "bolt/shuffle/sparksql/RawPartitionOutput.h"
#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include "bolt/shuffle/sparksql/ShuffleWriter.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Base of a shuffle writer that also takes CompositeRowVectors
/// (HashAggregation composite output). All composite handling lives here;
/// the subclass implements the ordinary path through the protected hooks
/// and never sees a composite vector.
///
/// Only one side holds data at a time: a batch of the other kind first
/// writes everything of the current one out, so each side reclaims only its
/// own state. Composite rows - serialized aggregation state, kept byte for
/// byte - are buffered per partition and written through output() as
/// CompositeRows blocks, all partitions in one spillRaw() call; the kind
/// byte that opens every block on the wire tells the reader which is which.
class CompositeShuffleWriter : public ShuffleWriter {
 public:
  arrow::Status split(RowVectorPtr rv, int64_t memLimit) final;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) final;

  arrow::Status stop() final;

 protected:
  /// Builds the hash/range partitioner both sides share.
  CompositeShuffleWriter(
      ShuffleWriterOptions options,
      memory::MemoryPool* boltPool,
      arrow::MemoryPool* arrowPool);

  ~CompositeShuffleWriter() override;

  /// An ordinary (non-composite) batch, pid column first.
  virtual arrow::Status splitOrdinary(RowVectorPtr rv, int64_t memLimit) = 0;

  /// Reclaim while the ordinary side holds the data.
  virtual arrow::Status reclaimOrdinary(int64_t size, int64_t* actual) = 0;

  /// A composite batch follows: write out everything held and return its
  /// memory.
  virtual arrow::Status flushOrdinary() = 0;

  /// Final output, after the composite rows went to output().
  virtual arrow::Status stopOrdinary() = 0;

  /// Where the composite blocks go; exists from construction. The subclass
  /// ends it together with its own output in stopOrdinary().
  virtual RawPartitionOutput& output() = 0;

  memory::MemoryPool* const boltPool_;

 private:
  void splitComposite(const CompositeRowVectorPtr& rv);

  /// Writes the held composite rows out - or, while a split or write is
  /// under way, right after it.
  void flushComposite();

  void writeComposite();

  /// The RawBlockSource of writeComposite(): partition `pid`'s next block.
  bool nextBlock(uint32_t pid, BufferPtr& block);

  bool finished_{false};
  /// The last batch was composite, so that side holds the data.
  bool compositeHeld_{false};

  /// Null when compressionType is UNCOMPRESSED.
  std::unique_ptr<Codec> codec_;
  ShuffleRowToRowConverter converter_;
  /// Per partition: the held rows, each pointing at [size i32][bytes].
  std::vector<std::vector<uint8_t*>> rows_;
  std::vector<uint32_t> row2Partition_;
  std::vector<uint32_t> partition2RowCount_;
  std::vector<int32_t> pids_;
  DecodedVector pidDecoded_;
  int64_t heldBytes_{0};
  bool busy_{false};
  bool flushRequested_{false};
  /// Rows of the current partition already handed out during a write.
  size_t nextRow_{0};
  BufferPtr scratch_;

  std::vector<int64_t> rawBytes_;
  uint64_t compressTimeNs_{0};
  uint64_t convertTimeNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
