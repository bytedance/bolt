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

#include "bolt/shuffle/sparksql/base/CompositeShuffleWriter.h"

#include <folly/ScopeGuard.h>
#include <algorithm>
#include <numeric>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include "bolt/shuffle/sparksql/base/ShuffleBlockFormat.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {

/// Raw body bytes per CompositeRows block: bounds the block workspaces and
/// gives the reader batch-sized pieces.
constexpr uint64_t kCompositeBlockBytes = 4 << 20;

/// The partition count of `options`, checked before any per-partition state
/// is sized from it.
int32_t checkedNumPartitions(const ShuffleWriterOptions& options) {
  const int32_t numPartitions = options.partitionWriterOptions.numPartitions;
  BOLT_USER_CHECK_GT(
      numPartitions, 0, "Shuffle writer needs at least one partition");
  if (options.partitioning == Partitioning::kSingle) {
    BOLT_USER_CHECK_EQ(
        numPartitions, 1, "Single partitioning takes exactly one partition");
  }
  return numPartitions;
}

} // namespace

class CompositeShuffleWriter::CompositeRowWriter {
 public:
  explicit CompositeRowWriter(CompositeShuffleWriter& owner)
      : owner_(owner),
        converter_(owner.boltPool_),
        heldRows_(owner.numPartitions_),
        rawPartitionBytes_(owner.numPartitions_, 0) {
    const auto& writerOptions = owner_.options_.partitionWriterOptions;
    if (writerOptions.compressionType != arrow::Compression::UNCOMPRESSED) {
      codec_ = createCodec(
          writerOptions.compressionType,
          CodecOptions{
              getCodecBackend(writerOptions.codecBackend),
              writerOptions.compressionLevel,
              writerOptions.checksumEnabled});
    }
  }

  ~CompositeRowWriter() {
    // A failed split or stop may leave composite rows held.
    converter_.reset();
  }

  void split(const CompositeRowVectorPtr& rv) {
    BOLT_CHECK(
        owner_.compositeActive_,
        "composite split while ordinary side is active");
    bool needsFlush = false;
    {
      BOLT_CHECK(
          state_ == WriteState::kIdle,
          "composite split is already in progress");
      state_ = WriteState::kBusy;
      SCOPE_EXIT {
        needsFlush = (state_ == WriteState::kFlushDeferred);
        state_ = WriteState::kIdle;
      };
      const vector_size_t numRows = rv->size();
      if (numRows == 0) {
        return;
      }
      // Composite rows are copied in whole batches: make room first. A
      // failed reservation is memory pressure: the oldest spill data in
      // memory goes to disk first, as much as the batch takes, and the held
      // rows after it, into the blocks the spill file holds.
      if (hasHeldRows_ && !owner_.boltPool_->maybeReserve(rv->totalRowSize())) {
        if (auto* spill = owner_.spillFile()) {
          spill->release(rv->totalRowSize());
        }
        write();
      }
      const auto status = owner_.computePartitions(*rv);
      BOLT_CHECK(status.ok(), "{}", status.ToString());
      uint64_t convertNs = 0;
      {
        NanosecondTimer timer(&convertNs);
        converter_.convert(rv, owner_.row2Partition_, heldRows_);
      }
      owner_.metrics_.convertTime += static_cast<int64_t>(convertNs);
      hasHeldRows_ = true;
      owner_.metrics_.totalInputRowNumber += numRows;
      owner_.metrics_.totalInputBatches += 1;
    }
    if (needsFlush) {
      // A reclaim asked for it: memory pressure, which that reclaim met with
      // the spill file's memory first.
      flush();
    }
  }

  /// Writes the held composite rows out - or, while a split or write is
  /// under way, right after it.
  void flush() {
    if (state_ != WriteState::kIdle) {
      state_ = WriteState::kFlushDeferred;
      return;
    }
    state_ = WriteState::kBusy;
    SCOPE_EXIT {
      state_ = WriteState::kIdle;
    };
    write();
  }

  bool hasHeldRows() const {
    return hasHeldRows_;
  }

  bool isIdle() const {
    return state_ == WriteState::kIdle;
  }

  /// Adds composite raw partition lengths and compression time after
  /// stopOrdinary() has written the ordinary side's metrics.
  void addMetrics(ShuffleWriterMetrics& metrics) const {
    for (int32_t pid = 0; pid < owner_.numPartitions_; ++pid) {
      metrics.rawPartitionLengths[pid] += rawPartitionBytes_[pid];
    }
    metrics.totalCompressTime += static_cast<int64_t>(compressTimeNs_);
  }

 private:
  /// Writes the held rows now. The caller holds state_ != kIdle.
  void write() {
    SCOPE_EXIT {
      if (state_ == WriteState::kFlushDeferred) {
        state_ = WriteState::kBusy;
      }
    };
    if (!hasHeldRows_) {
      return;
    }
    BOLT_CHECK(
        owner_.compositeActive_ || owner_.isStopped(),
        "composite write while ordinary side is active");
    BufferPtr block;
    BufferPtr compressedBuffer;
    owner_.output().writeBlocks([&](uint32_t pid, BlockEmitter emit) {
      emitPartitionBlocks(pid, block, compressedBuffer, emit);
    });
    converter_.reset();
    hasHeldRows_ = false;
    block.reset();
    compressedBuffer.reset();
    owner_.boltPool_->release();
  }

  /// Emits partition `pid`'s held rows as framed CompositeRows blocks.
  void emitPartitionBlocks(
      uint32_t pid,
      BufferPtr& block,
      BufferPtr& compressedBuffer,
      BlockEmitter emit) {
    auto& rows = heldRows_[pid];
    size_t nextRow = 0;
    while (nextRow < rows.size()) {
      const size_t begin = nextRow;
      uint64_t raw = 0;
      while (nextRow < rows.size()) {
        const uint64_t bytes = framedRowBytes(rows[nextRow]);
        if (nextRow > begin && raw + bytes > kCompositeBlockBytes) {
          break;
        }
        raw += bytes;
        ++nextRow;
      }
      buildRowBlock(
          ShuffleBlockKind::kCompositeRows,
          rows.data() + begin,
          nextRow - begin,
          raw,
          codec_.get(),
          block,
          compressedBuffer,
          owner_.boltPool_,
          compressTimeNs_);
      rawPartitionBytes_[pid] +=
          static_cast<int64_t>(kRowBlockHeaderBytes + raw);
      emit(block->asMutable<char>(), block->size());
    }
    rows.clear();
  }

  /// Reentrancy state for split/write. A flush reached while kBusy (e.g. a
  /// reclaim triggered by allocation) transitions to kFlushDeferred so it runs
  /// after the in-flight composite split completes.
  enum class WriteState : uint8_t {
    kIdle,
    kBusy,
    kFlushDeferred,
  };

  CompositeShuffleWriter& owner_;
  /// Null when compressionType is UNCOMPRESSED.
  std::unique_ptr<Codec> codec_;
  ShuffleRowToRowConverter converter_;
  /// Per partition: the held rows, each pointing at [size i32][bytes].
  std::vector<std::vector<uint8_t*>> heldRows_;
  bool hasHeldRows_{false};
  WriteState state_{WriteState::kIdle};
  /// Accumulated here, not in metrics_: stopOrdinary() overwrites the ordinary
  /// side's raw partition lengths and compression time, and stop() adds these
  /// after.
  std::vector<int64_t> rawPartitionBytes_;
  uint64_t compressTimeNs_{0};
};

CompositeShuffleWriter::CompositeShuffleWriter(
    ShuffleWriterOptions options,
    memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool)
    : ShuffleWriter(
          checkedNumPartitions(options),
          /*partitionWriter=*/nullptr, // output goes through output()
          options, // copied: the count above is read from it
          arrowPool),
      boltPool_(boltPool),
      // Partitioner::compute fills but does not size this.
      partition2RowCount_(numPartitions_) {
  auto maybePartitioner = Partitioner::make(
      options_.partitioning, numPartitions_, options_.startPartitionId);
  BOLT_CHECK(
      maybePartitioner.ok(),
      "Failed to create partitioner: {}",
      maybePartitioner.status().ToString());
  partitioner_ = *maybePartitioner;
}

arrow::Status CompositeShuffleWriter::computePartitions(const RowVector& rv) {
  if (options_.partitioning == Partitioning::kSingle) {
    row2Partition_.assign(rv.size(), 0);
    std::fill(partition2RowCount_.begin(), partition2RowCount_.end(), 0);
    partition2RowCount_[0] = rv.size();
    return arrow::Status::OK();
  }
  if (!partitioner_->hasPid()) {
    return partitioner_->compute(
        nullptr, rv.size(), row2Partition_, partition2RowCount_);
  }
  BOLT_CHECK_GT(rv.childrenSize(), 0, "expected a pid column");
  const auto& pidChild = rv.childAt(0);
  BOLT_CHECK_EQ(
      static_cast<int32_t>(pidChild->typeKind()),
      static_cast<int32_t>(TypeKind::INTEGER),
      "pid column must be INTEGER");
  const int32_t* pids;
  std::vector<int32_t> pidValues;
  if (pidChild->isFlatEncoding()) {
    pids = pidChild->asUnchecked<FlatVector<int32_t>>()->rawValues();
  } else {
    DecodedVector pidDecoded(*pidChild);
    pidValues.resize(rv.size());
    for (vector_size_t row = 0; row < rv.size(); ++row) {
      pidValues[row] = pidDecoded.valueAt<int32_t>(row);
    }
    pids = pidValues.data();
  }
  return partitioner_->compute(
      pids, rv.size(), row2Partition_, partition2RowCount_);
}

CompositeShuffleWriter::~CompositeShuffleWriter() {
  composite_.reset();
  boltPool_->release();
}

arrow::Status CompositeShuffleWriter::split(RowVectorPtr rv, int64_t memLimit) {
  BOLT_CHECK(
      lifecycle_ == LifecycleState::kIdle,
      "{}",
      lifecycle_ == LifecycleState::kStopped ? "split after stop"
                                             : "nested split call");
  lifecycle_ = LifecycleState::kSplitting;
  SCOPE_EXIT {
    lifecycle_ = LifecycleState::kIdle;
  };
  if (!RowVector::isComposite(rv)) {
    if (compositeActive_) {
      composite_->flush();
      BOLT_CHECK(
          !composite_->hasHeldRows() && composite_->isIdle(),
          "composite side must be drained before switching to ordinary");
      compositeActive_ = false;
    }
    BOLT_CHECK(
        !compositeActive_ &&
            (composite_ == nullptr ||
             (!composite_->hasHeldRows() && composite_->isIdle())),
        "ordinary split while composite side is active");
    return splitOrdinary(std::move(rv), memLimit);
  }
  BOLT_CHECK(
      partitioner_->hasPid(),
      "CompositeShuffleWriter takes composite rows only under hash or range "
      "partitioning");
  if (rv->size() == 0) {
    return arrow::Status::OK();
  }
  if (!compositeActive_) {
    RETURN_NOT_OK(flushOrdinary());
    if (composite_ == nullptr) {
      composite_ = std::make_unique<CompositeRowWriter>(*this);
    }
    compositeActive_ = true;
  }
  BOLT_CHECK(compositeActive_, "composite split while ordinary side is active");
  composite_->split(std::static_pointer_cast<CompositeRowVector>(rv));
  return arrow::Status::OK();
}

arrow::Status CompositeShuffleWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  if (!compositeActive_) {
    BOLT_CHECK(
        composite_ == nullptr ||
            (!composite_->hasHeldRows() && composite_->isIdle()),
        "ordinary reclaim while composite side holds data or is active");
    return reclaimOrdinary(size, actual);
  }
  const int64_t before = boltPool_->reservedBytes();
  // Memory pressure: the spill file stops growing in memory, so that the held
  // rows written now reuse its blocks. Then its oldest blocks go to disk, as
  // much as the request still needs. Mid-write of the composite blocks the
  // held rows wait for the next batch boundary, but the blocks still go: a
  // block's reservation is taken with the file's blocks full.
  auto* spill = spillFile();
  if (spill != nullptr) {
    spill->release(0);
  }
  composite_->flush();
  boltPool_->release();
  if (spill != nullptr) {
    const int64_t freed = before - boltPool_->reservedBytes();
    if (size <= 0) {
      spill->releaseAll();
    } else if (freed < size) {
      spill->release(size - freed);
    }
    boltPool_->release();
  }
  *actual = std::max<int64_t>(0, before - boltPool_->reservedBytes());
  return arrow::Status::OK();
}

arrow::Status CompositeShuffleWriter::stop() {
  BOLT_CHECK(
      lifecycle_ == LifecycleState::kIdle,
      "{}",
      lifecycle_ == LifecycleState::kStopped ? "stop called twice"
                                             : "stop during split");
  lifecycle_ = LifecycleState::kStopped;
  // Order matters: stopOrdinary() ends output() - it merges the shared spill
  // file into the data file, or ends the Celeborn attempt - so every held
  // composite row must be in output() before it runs.
  if (composite_ != nullptr) {
    composite_->flush();
    BOLT_CHECK(
        !composite_->hasHeldRows() && composite_->isIdle(),
        "composite side must be drained before stopOrdinary()");
  }
  compositeActive_ = false;
  RETURN_NOT_OK(stopOrdinary());
  // stopOrdinary() reported the ordinary bytes only; the composite blocks'
  // raw bytes are added here, partition by partition.
  metrics_.rawPartitionLengths.resize(numPartitions_, 0);
  if (composite_ != nullptr) {
    composite_->addMetrics(metrics_);
  }
  metrics_.dataSize = std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      int64_t{0});
  metrics_.peakBytes = boltPool_->peakBytes();
  return arrow::Status::OK();
}

} // namespace bytedance::bolt::shuffle::sparksql
