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

#include <memory>
#include <vector>

#include "bolt/vector/ComplexVector.h"

#include "bolt/shuffle/sparksql/ShuffleWriter.h"
#include "bolt/shuffle/sparksql/base/PartitionBlockOutput.h"

namespace bytedance::bolt::shuffle::sparksql {

/// Base of a shuffle writer that also takes CompositeRowVectors
/// (HashAggregation composite output). All composite handling lives here;
/// the subclass implements the ordinary path through the protected hooks
/// and never sees a composite vector.
///
/// Only one side holds data at a time: a batch of the other kind first
/// writes everything of the current one out, so each side reclaims only its
/// own state. Composite rows - serialized aggregation state, kept byte for
/// byte - are buffered per partition and written through output() as
/// CompositeRows blocks, all partitions in one writeBlocks() call; the kind
/// byte that opens every block on the wire tells the reader which is which.
class CompositeShuffleWriter : public ShuffleWriter {
 public:
  arrow::Status split(RowVectorPtr rv, int64_t memLimit) final;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) final;

  arrow::Status stop() final;

 protected:
  /// Builds the partitioner both sides share. Composite rows require a pid
  /// column; ordinary batches retain the configured partitioning's shape.
  CompositeShuffleWriter(
      ShuffleWriterOptions options,
      memory::MemoryPool* boltPool,
      arrow::MemoryPool* arrowPool);

  ~CompositeShuffleWriter() override;

  /// An ordinary batch, with a leading pid column only when hasPid() is true.
  virtual arrow::Status splitOrdinary(RowVectorPtr rv, int64_t memLimit) = 0;

  /// Reclaim while the ordinary side holds the data.
  virtual arrow::Status reclaimOrdinary(int64_t size, int64_t* actual) = 0;

  /// A composite batch follows: write out everything held and return its
  /// memory.
  virtual arrow::Status flushOrdinary() = 0;

  /// Final output. Called once, after every composite row went to
  /// output(); nothing is written to output() afterwards. It must end
  /// output() together with the ordinary data, in one partition-major pass:
  /// locally a single data file in which each partition's ordinary data is
  /// followed by its composite blocks - the merge seals both spill files,
  /// so a block written later would be lost - and on Celeborn the end of
  /// the map attempt. It reports the ordinary side's rawPartitionLengths
  /// only: stop() adds the composite bytes and derives dataSize after it.
  virtual arrow::Status stopOrdinary() = 0;

  /// Where the composite blocks go; exists from construction. Locally it is
  /// a partial spiller that the ordinary output merges into its data file;
  /// the subclass ends it in stopOrdinary().
  virtual PartitionBlockOutput& output() = 0;

  /// The shared local spill file whose newest bytes stay in memory, or null.
  /// Memory pressure on the composite side writes its oldest blocks to disk
  /// first, and the held rows after it, into the blocks it holds.
  virtual ShuffleSpillFile* spillFile() {
    return nullptr;
  }

  /// Inside split(): a reclaim reached now is nested in the split, which
  /// the operator times as a whole, not as externalReclaimTime.
  bool inSplitCall() const {
    return lifecycle_ == LifecycleState::kSplitting;
  }

  /// True once stop() has begun.
  bool isStopped() const {
    return lifecycle_ == LifecycleState::kStopped;
  }

  /// True while the composite path owns buffered state and handles reclaims.
  bool isCompositeActive() const {
    return compositeActive_;
  }

  /// Computes partition ids into row2Partition_ and counts into
  /// partition2RowCount_ for the configured partitioning (single, round-robin,
  /// or leading INTEGER pid column).
  arrow::Status computePartitions(const RowVector& rv);

  memory::MemoryPool* const boltPool_;
  /// Shared by the two paths, which never split at the same time. A reclaim
  /// inside a split goes to its active side; composite flushes wait for it.
  std::vector<uint32_t> row2Partition_;
  std::vector<uint32_t> partition2RowCount_;

 private:
  /// Lazily initialized on the first composite batch so ordinary-only shuffles
  /// pay no per-partition buffer or codec setup cost.
  class CompositeRowWriter;

  enum class LifecycleState : uint8_t {
    kIdle,
    kSplitting,
    kStopped,
  };

  LifecycleState lifecycle_{LifecycleState::kIdle};
  /// Whether the composite path currently owns buffered state and handles
  /// reclaims. Switching sides flushes the other first.
  bool compositeActive_{false};
  std::unique_ptr<CompositeRowWriter> composite_;
};

} // namespace bytedance::bolt::shuffle::sparksql
