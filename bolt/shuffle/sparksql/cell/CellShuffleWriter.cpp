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
#include <numeric>

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
      options_.partitionWriterOptions, &layout_, cellOpts, boltPool_);

  windowRowStart_.assign(numPartitions_, 0);
  perPidCounter_.assign(numPartitions_, 0);
  // Partitioner::compute fills but does not size this.
  partition2RowCount_.resize(numPartitions_);
  decoded_.resize(layout_.numColumns());
  encodingTags_.assign((layout_.numColumns() + 7) / 8, 0);

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
  BOLT_CHECK(!stopped_, "split after stop");
  // The factory keeps composite plans on V1 via the query config; a
  // composite vector arriving anyway must fail loudly rather than be
  // written as bytes the reader cannot interpret.
  BOLT_CHECK(
      !RowVector::isComposite(rv),
      "CellShuffleWriter cannot split a CompositeRowVector");
  if (!initialized_) {
    initOnFirstBatch(*rv);
  }
  // Oversized inputs are sliced so a checkpoint boundary exists inside
  // them: one giant batch must not inflate a single payload window past
  // the reader-side bounds (the legacy writers slice the same way).
  const int64_t flatSize = rv->estimateFlatSize();
  if (flatSize > kMaxShuffleWriterBatchBytes && rv->size() > 1) {
    const int32_t pieces = static_cast<int32_t>(std::min<int64_t>(
        rv->size(),
        (flatSize + kMaxShuffleWriterBatchBytes - 1) /
            kMaxShuffleWriterBatchBytes));
    const int32_t rowsPerPiece = (rv->size() + pieces - 1) / pieces;
    for (int32_t begin = 0; begin < rv->size(); begin += rowsPerPiece) {
      const int32_t length =
          std::min<int32_t>(rowsPerPiece, rv->size() - begin);
      auto piece = std::dynamic_pointer_cast<RowVector>(
          rv->slice(begin, length));
      BOLT_CHECK_NOT_NULL(piece);
      RETURN_NOT_OK(splitBatch(std::move(piece)));
    }
    return arrow::Status::OK();
  }
  return splitBatch(std::move(rv));
}

arrow::Status CellShuffleWriter::splitBatch(RowVectorPtr rv) {
  const uint64_t start = nowNs();
  inSplit_ = true;
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
  nullClass_.resize(layout_.numColumns());
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    auto& decoded = decoded_[col];
    decoded.decode(*rv->childAt(col + 1));
    // Classify the batch's nulls up front: mayHaveNulls() only means a
    // buffer exists, and defensively allocated all-set buffers are common.
    // A word-level scan (~n/64 compares, early exit) is far cheaper than
    // taxing every row of the loop with a bit test for absent nulls.
    auto klass = BatchNullClass::kNoNulls;
    if (decoded.mayHaveNulls()) {
      if (decoded.isConstantMapping()) {
        klass = decoded.isNullAt(0) ? BatchNullClass::kAllNull
                                    : BatchNullClass::kNoNulls;
      } else if (decoded.isIdentityMapping()) {
        const uint64_t* nulls = decoded.nulls();
        if (nulls == nullptr || bits::isAllSet(nulls, 0, numRows, true)) {
          klass = BatchNullClass::kNoNulls; // stale buffer, no actual null
        } else if (bits::isAllSet(nulls, 0, numRows, false)) {
          klass = BatchNullClass::kAllNull;
        } else {
          klass = BatchNullClass::kSomeNulls;
        }
      } else {
        // Wrapped nulls: take the per-row path rather than materializing
        // a combined bitmap here.
        klass = BatchNullClass::kSomeNulls;
      }
    }
    nullClass_[col] = klass;
    anyNullable = anyNullable || klass == BatchNullClass::kSomeNulls;
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

  if (FOLLY_UNLIKELY(!dictProbed_)) {
    // The single probe of the dictionary design: the first batch decides,
    // per string column and for the writer's lifetime, before its first
    // byte is split (cell bytes are final wire form; a payload's form
    // cannot change once written).
    probeDictionary(numRows);
    dictProbed_ = true;
  }

  SplitBatch batch;
  batch.decoded = &decoded_;
  batch.row2Partition = row2Partition_.data();
  batch.partition2RowCount = partition2RowCount_.data();
  batch.nullClass = nullClass_.data();
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

void CellShuffleWriter::probeDictionary(uint32_t numRows) {
  const auto& cellOpts = options_.cellOptions;
  if (!cellOpts.enableStringDictionary ||
      numRows < static_cast<uint32_t>(cellOpts.dictMinProbeRows)) {
    return;
  }
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    if (!layout_.isStringColumn(col) ||
        nullClass_[col] == BatchNullClass::kAllNull) {
      continue;
    }
    const auto& decoded = decoded_[col];
    const bool hasNulls = nullClass_[col] == BatchNullClass::kSomeNulls;
    // Distinct scan with early exit: once past the 64-byte serialization
    // budget the column can never meet the conservative criterion, so a
    // high-cardinality column costs only a handful of rows here.
    std::vector<StringView> seen;
    uint32_t serialized = 0;
    uint64_t nonNull = 0;
    bool fits = true;
    for (uint32_t row = 0; row < numRows; ++row) {
      if (hasNulls && decoded.isNullAt(row)) {
        continue;
      }
      const auto view = decoded.valueAt<StringView>(row);
      ++nonNull;
      if (view.size() > kDictEntryMaxLen) {
        fits = false;
        break;
      }
      bool found = false;
      for (const auto& entry : seen) {
        if (entry.size() == view.size() &&
            ::memcmp(entry.data(), view.data(), view.size()) == 0) {
          found = true;
          break;
        }
      }
      if (!found) {
        serialized += 1 + view.size();
        if (serialized > kDictSerializedBudget) {
          fits = false;
          break;
        }
        seen.push_back(view);
      }
    }
    const bool enable = fits && !seen.empty() &&
        nonNull >= static_cast<uint64_t>(cellOpts.dictMinRepeatRatio) *
            seen.size();
    if (enable) {
      frontend_->enableDictionary(col);
      encodingTags_[col / 8] |= static_cast<uint8_t>(1u << (col % 8));
    }
    LOG(INFO) << "CellShuffleWriter dictionary probe: column " << col
              << (enable ? " ON" : " OFF") << " (ndv=" << seen.size()
              << (fits ? "" : "+") << ", serializedBytes=" << serialized
              << ", nonNull=" << nonNull << " of " << numRows << " rows)";
  }
}

CellWindowInput CellShuffleWriter::windowInput() {
  CellWindowInput in;
  in.cells = cells_.get();
  in.nulls = nulls_.get();
  in.layout = &layout_;
  in.rowCounts = windowRowStart_.data();
  in.variableBytes = frontend_->variableBytesArray();
  in.encodingTags = encodingTags_.data();
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
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    if ((encodingTags_[col / 8] >> (col % 8)) & 1) {
      const auto stats = frontend_->dictionaryStats(col);
      metrics_.dictionaryMatchedRows +=
          static_cast<int64_t>(stats.matchedRows);
      metrics_.dictionaryFallbackRows +=
          static_cast<int64_t>(stats.fallbackRows);
      const uint64_t rows = stats.matchedRows + stats.fallbackRows;
      LOG(INFO) << "CellShuffleWriter dictionary column " << col
                << ": matched " << stats.matchedRows << " of " << rows
                << " rows ("
                << (rows > 0 ? 100.0 * stats.matchedRows / rows : 0.0)
                << "%), " << stats.segments << " segments, " << stats.demotes
                << " demotes";
    }
  }
  // Data size is the pre-compression payload volume (what the raw lengths
  // account); bytes written is the compressed file. Reporting them as the
  // same number would hide the compression ratio from the engine metrics.
  metrics_.dataSize = std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      int64_t{0});
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
