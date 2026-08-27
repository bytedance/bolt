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

#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"

#include <chrono>

#include "bolt/common/base/BitUtil.h"
#include "bolt/shuffle/sparksql/cell/LocalCellOutput.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

uint64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

uint32_t prevPowerOfTwo(uint64_t value) {
  return uint32_t{1} << (63 - __builtin_clzll(value));
}

} // namespace

namespace {
int32_t numPartitionsOf(const ShuffleWriterOptions& options) {
  return options.partitionWriterOptions.numPartitions;
}
} // namespace

CellShuffleWriter::CellShuffleWriter(
    ShuffleWriterOptions options,
    memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool)
    : ShuffleWriter(
          numPartitionsOf(options), // read before the move below can happen
          /*partitionWriter=*/nullptr, // all output goes through CellOutput
          options,
          arrowPool),
      boltPool_(boltPool) {
  auto maybePartitioner = Partitioner::make(
      options_.partitioning, numPartitions_, options_.startPartitionId);
  BOLT_CHECK(
      maybePartitioner.ok(),
      "Failed to create partitioner: {}",
      maybePartitioner.status().ToString());
  partitioner_ = *maybePartitioner;
  BOLT_CHECK(
      partitioner_->hasPid(),
      "CellShuffleWriter requires hash or range partitioning");
}

void CellShuffleWriter::initOnFirstBatch(const RowVector& rv) {
  const auto& inputType = rv.type()->asRow();
  BOLT_CHECK_GE(inputType.size(), 2, "expected a pid column plus data");
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(inputType.size() - 1);
  types.reserve(inputType.size() - 1);
  for (uint32_t i = 1; i < inputType.size(); ++i) {
    names.push_back(inputType.nameOf(i));
    types.push_back(inputType.childAt(i));
  }
  layout_ = CellLayout::create(ROW(std::move(names), std::move(types)));

  const auto& cellOpts = options_.cellOptions;
  const uint32_t numStreams = layout_.numStreams();
  int64_t budget = cellOpts.cellMemoryBudgetBytes;
  if (budget <= 0) {
    const int64_t capacity = boltPool_->maxCapacity();
    // An unlimited pool reports kMaxMemory; fall back to a sane default.
    budget = (capacity <= 0 || capacity > (int64_t{1} << 40))
        ? (int64_t{1} << 30)
        : std::min<int64_t>(capacity / 4, int64_t{1} << 30);
  }
  const int64_t perStream = budget / 8 / numPartitions_ / numStreams;
  const int64_t cellCap =
      std::min<int64_t>(cellOpts.maxDataCellBytes, cellOpts.chunkBytes / 4);
  const uint32_t cellBytes = prevPowerOfTwo(std::max<int64_t>(
      cellOpts.minDataCellBytes, std::min<int64_t>(perStream, cellCap)));

  allocator_ = std::make_unique<ChunkAllocator>(
      boltPool_, cellOpts.chunkBytes, cellBytes);
  cells_ = std::make_unique<DataCells>(
      boltPool_, allocator_.get(), numPartitions_, numStreams);
  nulls_ = std::make_unique<NullCells>(
      boltPool_, numPartitions_, layout_.numColumns());
  frontend_ = std::make_unique<CachedCellFrontend>(
      &layout_, cells_.get(), nulls_.get(), boltPool_, [this]() {
        onBeforeChunkGrow();
      });
  output_ = std::make_unique<LocalCellOutput>(
      options_.partitionWriterOptions, &layout_);

  windowRowStart_.assign(numPartitions_, 0);
  perPidCounter_.assign(numPartitions_, 0);
  // Partitioner::compute fills but does not size this.
  partition2RowCount_.resize(numPartitions_);
  decoded_.resize(layout_.numColumns());

  // Warm the reservation for the resident structures and the first chunks;
  // failure is not fatal, allocation will arbitrate.
  boltPool_->maybeReserve(
      frontend_->residentBytes() + 2 * cellOpts.chunkBytes);
  initialized_ = true;
}

const int32_t* CellShuffleWriter::pidArray(const RowVector& rv) {
  const auto& pidChild = rv.childAt(0);
  BOLT_CHECK_EQ(
      static_cast<int32_t>(pidChild->typeKind()),
      static_cast<int32_t>(TypeKind::INTEGER),
      "pid column must be INTEGER");
  if (pidChild->isFlatEncoding()) {
    return pidChild->asUnchecked<FlatVector<int32_t>>()->rawValues();
  }
  // Defensive path: materialize a wrapped pid column.
  pidDecoded_.decode(*pidChild);
  pidValues_.resize(rv.size());
  for (uint32_t row = 0; row < rv.size(); ++row) {
    pidValues_[row] = pidDecoded_.valueAt<int32_t>(row);
  }
  return pidValues_.data();
}

arrow::Status CellShuffleWriter::split(
    RowVectorPtr rv,
    int64_t /*memLimitIgnored: cell memory decisions never use it*/) {
  const uint64_t start = nowNs();
  BOLT_CHECK(!stopped_, "split after stop");
  inSplit_ = true;
  if (!initialized_) {
    initOnFirstBatch(*rv);
  }
  const uint32_t numRows = rv->size();
  if (numRows == 0) {
    inSplit_ = false;
    return arrow::Status::OK();
  }

  const int32_t* pids = pidArray(*rv);
  auto status =
      partitioner_->compute(pids, numRows, row2Partition_, partition2RowCount_);
  if (!status.ok()) {
    inSplit_ = false;
    return status;
  }

  bool anyNullable = false;
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    decoded_[col].decode(*rv->childAt(col + 1));
    anyNullable = anyNullable || decoded_[col].mayHaveNulls();
  }
  if (anyNullable) {
    // One shared pass gives every nullable column its per-partition row
    // index; per-column counters would cost this once per column instead.
    rowIndexInPid_.resize(numRows);
    std::fill(perPidCounter_.begin(), perPidCounter_.end(), 0);
    for (uint32_t row = 0; row < numRows; ++row) {
      rowIndexInPid_[row] = perPidCounter_[row2Partition_[row]]++;
    }
  }

  SplitBatch batch;
  batch.decoded = &decoded_;
  batch.row2Partition = row2Partition_.data();
  batch.numRows = numRows;
  batch.rowIndexInPid = anyNullable ? rowIndexInPid_.data() : nullptr;
  batch.windowRowStart = windowRowStart_.data();
  frontend_->split(batch);

  for (uint32_t pid = 0; pid < static_cast<uint32_t>(numPartitions_); ++pid) {
    const uint32_t added = partition2RowCount_[pid];
    if (added == 0) {
      continue;
    }
    windowRowStart_[pid] += added;
    if (windowRowStart_[pid] > maxWindowRows_) {
      maxWindowRows_ = windowRowStart_[pid];
    }
  }
  totalWindowRows_ += numRows;
  metrics_.totalInputRowNumber += numRows;
  metrics_.totalInputBatches += 1;
  inSplit_ = false;

  maybeCheckpoint();
  metrics_.splitTime += static_cast<int64_t>(nowNs() - start);
  return arrow::Status::OK();
}

CellWindowInput CellShuffleWriter::windowInput() {
  CellWindowInput in;
  in.cells = cells_.get();
  in.nulls = nulls_.get();
  in.layout = &layout_;
  in.rowCounts = windowRowStart_.data();
  in.variableBytes = frontend_->variableBytesArray();
  in.numPartitions = static_cast<uint32_t>(numPartitions_);
  return in;
}

void CellShuffleWriter::onBeforeChunkGrow() {
  if (spilling_) {
    return;
  }
  const auto& cellOpts = options_.cellOptions;
  if (cellOpts.cellMemoryCapBytes > 0 &&
      allocator_->allocatedBytes() + cellOpts.chunkBytes >
          cellOpts.cellMemoryCapBytes) {
    spillRunNow();
    return;
  }
  if (!boltPool_->maybeReserve(cellOpts.chunkBytes)) {
    spillRunNow();
  }
}

void CellShuffleWriter::spillRunNow() {
  if (spilling_ || !initialized_ || cells_->totalBytes() == 0) {
    return;
  }
  spilling_ = true;
  output_->spillRun(windowInput());
  cells_->releaseAll();
  spilling_ = false;
}

void CellShuffleWriter::checkpoint() {
  frontend_->flushAll();
  spillRunNow();
  output_->sealWindow(windowInput());
  nulls_->reset();
  std::fill(windowRowStart_.begin(), windowRowStart_.end(), 0);
  frontend_->resetWindowStats();
  totalWindowRows_ = 0;
  maxWindowRows_ = 0;
  checkpointRequested_ = false;
  // A sealed window is a batch boundary: drop reservation slack.
  boltPool_->release();
}

void CellShuffleWriter::maybeCheckpoint() {
  if (!initialized_ || totalWindowRows_ == 0) {
    return;
  }
  const auto& cellOpts = options_.cellOptions;
  if (checkpointRequested_ ||
      frontend_->maxPartitionBytes() >
          static_cast<uint64_t>(cellOpts.checkpointPartitionBytes) ||
      nulls_->allocatedBytes() > cellOpts.nullMemLimitBytes ||
      maxWindowRows_ > cellOpts.maxWindowRows) {
    checkpoint();
  }
}

arrow::Status CellShuffleWriter::reclaimFixedSize(
    int64_t /*size*/,
    int64_t* actual) {
  *actual = 0;
  if (!initialized_ || spilling_ || stopped_) {
    return arrow::Status::OK();
  }
  const auto& cellOpts = options_.cellOptions;
  if (allocator_->allocatedBytes() < 2 * cellOpts.chunkBytes) {
    // Not enough to be worth a spill; avoids reclaim churn.
    return arrow::Status::OK();
  }
  spillRunNow();
  *actual = allocator_->shrink();
  // Freed chunks alone are not enough: the reservation built up by the
  // chunk-grow choke point must go back too, or the arbitrator's requester
  // still sees no room (the standard spill-then-release() pattern).
  boltPool_->release();
  // Close the window at the next batch boundary so the resident null and
  // row-count state goes too.
  checkpointRequested_ = true;
  metrics_.totalBytesEvicted = output_->bytesEvicted();
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::stop() {
  BOLT_CHECK(!stopped_, "stop called twice");
  stopped_ = true;
  if (!initialized_) {
    metrics_.partitionLengths.assign(numPartitions_, 0);
    metrics_.rawPartitionLengths.assign(numPartitions_, 0);
    return arrow::Status::OK();
  }
  const bool windowHasData = totalWindowRows_ > 0;
  if (windowHasData) {
    frontend_->flushAll();
  }
  output_->finalize(windowInput(), windowHasData, metrics_);
  metrics_.dataSize = metrics_.totalBytesWritten;
  metrics_.peakBytes = boltPool_->peakBytes();

  // Return everything: drop chains, chunks and the reservation.
  cells_->releaseAll();
  allocator_->shrink();
  nulls_->reset();
  boltPool_->release();
  return arrow::Status::OK();
}

std::string CellShuffleWriter::toString() const {
  return "CellShuffleWriter";
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
