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

constexpr uint32_t kChunkBytes = 4 << 20;
constexpr uint32_t kMinDataCellBytes = 256;
constexpr uint32_t kMaxDataCellBytes = 64 << 10;
constexpr uint32_t kDictMinProbeRows = 1024;
constexpr uint32_t kDictMinRepeatRatio = 4;

uint64_t currentTimeNs() {
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

CellShuffleWriter::~CellShuffleWriter() {
  // Failed split/stop paths must return the warm reservation as well.
  splitter_.reset();
  output_.reset();
  allocator_.reset();
  boltPool_->release();
}

void CellShuffleWriter::initOnFirstBatch(const RowVector& rv) {
  const auto& inputType = rv.type()->asRow();
  // A pid column alone is legal: the payloads then carry only row counts.
  BOLT_CHECK_GE(inputType.size(), 1, "expected a pid column");
  // The adapter rejects unsupported logical types and maps complex columns
  // onto one trailing binary column; the pid column stays first.
  adapter_ = std::make_unique<CellShuffleTypeAdapter>(asRowType(rv.type()));
  const auto& physicalType = adapter_->physicalType()->asRow();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(physicalType.size() - 1);
  types.reserve(physicalType.size() - 1);
  for (uint32_t i = 1; i < physicalType.size(); ++i) {
    names.push_back(physicalType.nameOf(i));
    types.push_back(physicalType.childAt(i));
  }
  layout_ = CellLayout::create(ROW(std::move(names), std::move(types)));

  const auto& cellOpts = options_.cellOptions;
  const uint32_t numStreams = layout_.numStreams();
  const int64_t capacity = boltPool_->maxCapacity();
  // An unlimited pool reports kMaxMemory; use a 1 GiB sizing budget.
  const int64_t budget = (capacity <= 0 || capacity > (int64_t{1} << 40))
      ? (int64_t{1} << 30)
      : std::min<int64_t>(capacity / 4, int64_t{1} << 30);
  const int64_t perStream =
      budget / 8 / numPartitions_ / std::max<uint32_t>(1, numStreams);
  const uint32_t cellBytes = prevPowerOfTwo(
      std::clamp<int64_t>(perStream, kMinDataCellBytes, kMaxDataCellBytes));

  allocator_ = std::make_unique<ChunkAllocator>(
      boltPool_, kChunkBytes, cellBytes, [this]() { onBeforeChunkGrow(); });
  output_ = std::make_unique<LocalCellOutput>(
      options_.partitionWriterOptions, &layout_, cellOpts, boltPool_);
  splitter_ = std::make_unique<CellSplitter>(
      layout_, numPartitions_, *allocator_, *output_, *boltPool_);

  perPidCounter_.assign(numPartitions_, 0);
  // Partitioner::compute fills but does not size this.
  partition2RowCount_.resize(numPartitions_);
  decoded_.resize(layout_.numColumns());

  // Warm the reservation for the resident structures and the first chunks;
  // failure is not fatal, allocation will arbitrate.
  if (numStreams != 0) {
    boltPool_->maybeReserve(splitter_->residentBytes() + 2 * kChunkBytes);
  }
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
  if (!adapter_->hasComplexColumns()) {
    return splitPhysical(std::move(rv));
  }
  // Complex columns are serialized in bounded slices, each split on its own.
  vector_size_t offset = 0;
  do {
    RowVectorPtr physical;
    {
      const uint64_t start = currentTimeNs();
      physical = adapter_->encodeNext(rv, offset, boltPool_);
      metrics_.convertTime += static_cast<int64_t>(currentTimeNs() - start);
    }
    RETURN_NOT_OK(splitPhysical(std::move(physical)));
  } while (offset < rv->size());
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::splitPhysical(RowVectorPtr rv) {
  // Oversized inputs are sliced so a checkpoint boundary exists inside
  // them: one giant batch must not inflate a single payload window past
  // the reader-side bounds (the legacy writers slice the same way). Lazy
  // children are loaded first: unloaded they estimate as zero bytes and
  // the byte bound would never fire.
  if (isLazyNotLoaded(*rv)) {
    rv->loadedVector();
  }
  const int64_t flatSize = rv->estimateFlatSize();
  const int32_t rowLimit = std::max<int32_t>(
      1, std::min<int64_t>(options_.cellOptions.maxWindowRows, 1 << 24));
  if ((flatSize > kMaxShuffleWriterBatchBytes || rv->size() > rowLimit) &&
      rv->size() > 1) {
    const int64_t pieces = std::max<int64_t>(
        1,
        std::min<int64_t>(
            rv->size(),
            (flatSize + kMaxShuffleWriterBatchBytes - 1) /
                kMaxShuffleWriterBatchBytes));
    const int32_t rowsPerPiece =
        std::min<int64_t>(rowLimit, (rv->size() + pieces - 1) / pieces);
    for (int32_t begin = 0; begin < rv->size(); begin += rowsPerPiece) {
      const int32_t length =
          std::min<int32_t>(rowsPerPiece, rv->size() - begin);
      auto piece =
          std::dynamic_pointer_cast<RowVector>(rv->slice(begin, length));
      BOLT_CHECK_NOT_NULL(piece);
      RETURN_NOT_OK(splitBatch(std::move(piece)));
    }
    return arrow::Status::OK();
  }
  return splitBatch(std::move(rv));
}

arrow::Status CellShuffleWriter::splitBatch(RowVectorPtr rv) {
  const uint64_t start = currentTimeNs();
  const uint64_t rowLimit = std::max<int64_t>(
      1, std::min<int64_t>(options_.cellOptions.maxWindowRows, 1 << 24));
  if (splitter_->hasWindowRows() &&
      splitter_->maxWindowRows() + uint64_t(rv->size()) > rowLimit) {
    checkpoint();
  }
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
    if (layout_.isUnknownColumn(col)) {
      nullClass_[col] = BatchNullClass::kAllNull;
      continue;
    }
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
    // cannot change once written). A first batch below the probe minimum -
    // tiny, or sliced small because its rows are wide - therefore settles
    // on no dictionary rather than deferring; wide rows gain little from
    // 64-byte entries anyway.
    probeDictionary(numRows);
    dictProbed_ = true;
  }

  SplitBatch batch;
  batch.decoded = decoded_.data();
  batch.row2Partition = row2Partition_.data();
  batch.partition2RowCount = partition2RowCount_.data();
  batch.nullClass = nullClass_.data();
  batch.numRows = numRows;
  batch.rowIndexInPid = anyNullable ? rowIndexInPid_.data() : nullptr;
  splitter_->split(batch);

  metrics_.totalInputRowNumber += numRows;
  metrics_.totalInputBatches += 1;
  inSplit_ = false;

  maybeCheckpoint();
  metrics_.splitTime += static_cast<int64_t>(currentTimeNs() - start);
  return arrow::Status::OK();
}

void CellShuffleWriter::probeDictionary(uint32_t numRows) {
  const auto& cellOpts = options_.cellOptions;
  if (!cellOpts.enableStringDictionary || numRows < kDictMinProbeRows) {
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
        nonNull >= static_cast<uint64_t>(kDictMinRepeatRatio) * seen.size();
    if (enable) {
      splitter_->enableDictionary(col);
    }
    LOG(INFO) << "CellShuffleWriter dictionary probe: column " << col
              << (enable ? " ON" : " OFF") << " (ndv=" << seen.size()
              << (fits ? "" : "+") << ", serializedBytes=" << serialized
              << ", nonNull=" << nonNull << " of " << numRows << " rows)";
  }
}

void CellShuffleWriter::onBeforeChunkGrow() {
  if (!initialized_ || splitter_->isWritingOutput()) {
    return;
  }
  const auto& cellOpts = options_.cellOptions;
  if (cellOpts.cellMemoryCapBytes > 0 &&
      allocator_->allocatedBytes() + kChunkBytes >
          cellOpts.cellMemoryCapBytes) {
    splitter_->spillRun();
    return;
  }
  if (!boltPool_->maybeReserve(kChunkBytes)) {
    splitter_->spillRun();
  }
}

void CellShuffleWriter::checkpoint() {
  splitter_->sealWindow();
  checkpointRequested_ = false;
  // A sealed window is a batch boundary: drop reservation slack.
  boltPool_->release();
}

void CellShuffleWriter::maybeCheckpoint() {
  if (!initialized_ || !splitter_->hasWindowRows()) {
    return;
  }
  const auto& cellOpts = options_.cellOptions;
  if (checkpointRequested_ ||
      splitter_->maxPartitionBytes() >
          static_cast<uint64_t>(cellOpts.checkpointPartitionBytes) ||
      splitter_->nullBytes() > cellOpts.nullMemLimitBytes ||
      splitter_->maxWindowRows() > cellOpts.maxWindowRows) {
    checkpoint();
  }
}

arrow::Status CellShuffleWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  *actual = 0;
  if (!initialized_) {
    return arrow::Status::OK();
  }
  // Free memory first, without touching data: chunks that hold no live
  // cell (the freelist is chunk-packed after every spill, so mid-refill
  // the untouched tail chunks are all returnable) and reservation slack.
  // shrink() never frees a chunk with a live cell, so this is safe while
  // the output reads cells - and a window seal, which recycles every cell
  // before writing its null bodies, is exactly when most chunks are idle.
  *actual = allocator_->shrink();
  boltPool_->release();
  if (stopped_ || splitter_->isWritingOutput()) {
    // The output is reading the cells: no run may drain them now.
    return arrow::Status::OK();
  }
  // A run costs an O(partitions x streams) drain and a segment with its
  // own header and spill compression context per non-empty partition, so
  // it is produced only when actually needed: when the free memory above
  // already covers the request, or the writer holds nothing meaningful,
  // no run is written. Anything beyond that means the requester's need
  // is real and unmet - a small run is a far lesser evil than an OOM,
  // and the allocation churn case never reaches here (its small asks are
  // satisfied by the idle memory).
  if ((size > 0 && *actual >= size) ||
      allocator_->allocatedBytes() < 2 * kChunkBytes) {
    return arrow::Status::OK();
  }
  splitter_->spillRun();
  *actual += allocator_->shrink();
  // Freed chunks alone are not enough: the reservation built up by the
  // chunk-grow choke point must go back too, or the arbitrator's requester
  // still sees no room (the standard spill-then-release() pattern).
  boltPool_->release();
  // Close the window at the next batch boundary only when the resident
  // null-bitmap state is actually worth returning; requesting a seal on
  // every reclaim turns pressure churn into a checkpoint storm.
  if (splitter_->nullBytes() > options_.cellOptions.nullMemLimitBytes / 4) {
    checkpointRequested_ = true;
  }
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
  splitter_->finish(metrics_);
  // Data size is the pre-compression payload volume (what the raw lengths
  // account); bytes written is the compressed file. Reporting them as the
  // same number would hide the compression ratio from the engine metrics.
  metrics_.dataSize = std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      int64_t{0});
  metrics_.peakBytes = boltPool_->peakBytes();

  // The splitter has released its window; return idle chunks and reservation.
  allocator_->shrink();
  boltPool_->release();
  return arrow::Status::OK();
}

std::string CellShuffleWriter::toString() const {
  return "CellShuffleWriter";
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
