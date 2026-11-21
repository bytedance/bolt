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
#include "bolt/shuffle/sparksql/base/ShuffleBlockFormat.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {

/// Raw body bytes per CompositeRows block: bounds the block workspaces and
/// gives the reader batch-sized pieces.
constexpr uint64_t kCompositeBlockBytes = 4 << 20;

} // namespace

CompositeShuffleWriter::CompositeShuffleWriter(
    ShuffleWriterOptions options,
    memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool)
    : ShuffleWriter(
          options.partitionWriterOptions.numPartitions,
          /*partitionWriter=*/nullptr, // output goes through output()
          options, // copied: the count above is read from it
          arrowPool),
      boltPool_(boltPool),
      // Partitioner::compute fills but does not size this.
      partition2RowCount_(numPartitions_),
      converter_(boltPool),
      heldRows_(numPartitions_),
      rawPartitionBytes_(numPartitions_, 0) {
  auto maybePartitioner = Partitioner::make(
      options_.partitioning, numPartitions_, options_.startPartitionId);
  BOLT_CHECK(
      maybePartitioner.ok(),
      "Failed to create partitioner: {}",
      maybePartitioner.status().ToString());
  partitioner_ = *maybePartitioner;
  const auto& writerOptions = options_.partitionWriterOptions;
  if (writerOptions.compressionType != arrow::Compression::UNCOMPRESSED) {
    codec_ = createCodec(
        writerOptions.compressionType,
        CodecOptions{
            getCodecBackend(writerOptions.codecBackend),
            writerOptions.compressionLevel,
            writerOptions.checksumEnabled});
  }
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
  if (pidChild->isFlatEncoding()) {
    pids = pidChild->asUnchecked<FlatVector<int32_t>>()->rawValues();
  } else {
    pidDecoded_.decode(*pidChild);
    pidValues_.resize(rv.size());
    for (vector_size_t row = 0; row < rv.size(); ++row) {
      pidValues_[row] = pidDecoded_.valueAt<int32_t>(row);
    }
    pids = pidValues_.data();
  }
  return partitioner_->compute(
      pids, rv.size(), row2Partition_, partition2RowCount_);
}

CompositeShuffleWriter::~CompositeShuffleWriter() {
  // A failed split or stop may leave composite rows held.
  converter_.reset();
  boltPool_->release();
}

arrow::Status CompositeShuffleWriter::split(RowVectorPtr rv, int64_t memLimit) {
  BOLT_CHECK(!stopped_, "split after stop");
  inSplitCall_ = true;
  SCOPE_EXIT {
    inSplitCall_ = false;
  };
  if (!RowVector::isComposite(rv)) {
    if (compositeActive_) {
      flushComposite();
      compositeActive_ = false;
    }
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
    compositeActive_ = true;
  }
  splitComposite(std::static_pointer_cast<CompositeRowVector>(rv));
  return arrow::Status::OK();
}

arrow::Status CompositeShuffleWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  if (!compositeActive_) {
    return reclaimOrdinary(size, actual);
  }
  const int64_t before = boltPool_->reservedBytes();
  // Memory pressure: the spill memory stops growing, so that the held rows
  // written now reuse its blocks. Then its oldest blocks go to disk, as
  // much as the request still needs. Mid-write of the composite blocks the
  // held rows wait for the next batch boundary, but the blocks still go: a
  // block's reservation is taken with its file's blocks full.
  auto* memory = spillMemory();
  if (memory != nullptr) {
    memory->release(0);
  }
  flushComposite();
  boltPool_->release();
  if (memory != nullptr) {
    const int64_t freed = before - boltPool_->reservedBytes();
    if (size <= 0) {
      memory->releaseAll();
    } else if (freed < size) {
      memory->release(size - freed);
    }
    boltPool_->release();
  }
  *actual = std::max<int64_t>(0, before - boltPool_->reservedBytes());
  return arrow::Status::OK();
}

arrow::Status CompositeShuffleWriter::stop() {
  BOLT_CHECK(!stopped_, "stop called twice");
  stopped_ = true;
  // Order matters: stopOrdinary() ends output() - it merges the composite
  // spill file into the data file, or ends the Celeborn attempt - so every
  // held composite row must be in output() before it runs.
  flushComposite();
  RETURN_NOT_OK(stopOrdinary());
  // stopOrdinary() reported the ordinary bytes only; the composite blocks'
  // raw bytes are added here, partition by partition.
  metrics_.rawPartitionLengths.resize(numPartitions_, 0);
  for (int32_t pid = 0; pid < numPartitions_; ++pid) {
    metrics_.rawPartitionLengths[pid] += rawPartitionBytes_[pid];
  }
  metrics_.dataSize = std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      int64_t{0});
  metrics_.totalCompressTime += static_cast<int64_t>(compressTimeNs_);
  metrics_.peakBytes = boltPool_->peakBytes();
  return arrow::Status::OK();
}

void CompositeShuffleWriter::splitComposite(const CompositeRowVectorPtr& rv) {
  {
    compositeInProgress_ = true;
    SCOPE_EXIT {
      compositeInProgress_ = false;
    };
    const vector_size_t numRows = rv->size();
    if (numRows == 0) {
      return;
    }
    // Composite rows are copied in whole batches: make room first. A
    // failed reservation is memory pressure: the oldest spill data in
    // memory goes to disk first, as much as the batch takes, and the held
    // rows after it, into the blocks the spill memory holds.
    if (heldRowBytes_ > 0 && !boltPool_->maybeReserve(rv->totalRowSize())) {
      if (auto* memory = spillMemory()) {
        memory->release(rv->totalRowSize());
      }
      writeComposite();
    }
    const auto status = computePartitions(*rv);
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    const uint64_t start = currentTimeNs();
    converter_.convert(rv, row2Partition_, heldRows_);
    metrics_.convertTime +=
        std::max<int64_t>(0, static_cast<int64_t>(currentTimeNs() - start));
    heldRowBytes_ += rv->totalRowSize();
    metrics_.totalInputRowNumber += numRows;
    metrics_.totalInputBatches += 1;
  }
  if (flushDeferred_) {
    // A reclaim asked for it: memory pressure, which that reclaim met with
    // the spill memory first.
    flushComposite();
  }
}

void CompositeShuffleWriter::flushComposite() {
  if (compositeInProgress_) {
    flushDeferred_ = true;
    return;
  }
  compositeInProgress_ = true;
  SCOPE_EXIT {
    compositeInProgress_ = false;
  };
  writeComposite();
}

void CompositeShuffleWriter::writeComposite() {
  SCOPE_EXIT {
    flushDeferred_ = false;
  };
  if (heldRowBytes_ == 0) {
    return;
  }
  size_t nextRow = 0;
  BufferPtr compressedBuffer;
  output().writeBlocks([&](uint32_t pid, BufferPtr& block) {
    return nextBlock(pid, block, nextRow, compressedBuffer);
  });
  converter_.reset();
  heldRowBytes_ = 0;
  compressedBuffer.reset();
  boltPool_->release();
}

bool CompositeShuffleWriter::nextBlock(
    uint32_t pid,
    BufferPtr& block,
    size_t& nextRow,
    BufferPtr& compressedBuffer) {
  auto& rows = heldRows_[pid];
  if (nextRow >= rows.size()) {
    rows.clear();
    nextRow = 0;
    return false;
  }
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
      boltPool_,
      compressTimeNs_);
  rawPartitionBytes_[pid] += static_cast<int64_t>(kRowBlockHeaderBytes + raw);
  return true;
}

} // namespace bytedance::bolt::shuffle::sparksql
