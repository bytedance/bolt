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

#include <limits>
#include <numeric>

#include <fmt/format.h>

#include <folly/ScopeGuard.h>
#include <folly/lang/Bits.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleCheck.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

constexpr uint32_t kChunkBytes = 4 << 20;
/// Free enough cells for compaction to return a whole chunk, allowing for
/// partitions kept together and cells pinned by an append in progress.
constexpr int64_t kMinReclaimRunBytes = 2 * kChunkBytes;
/// Slice bound on a remote shuffle, where a window is released only whole
/// at a batch boundary: one slice bounds how far memory runs past a failed
/// reservation before the window can be pushed.
constexpr int64_t kRemoteSliceBytes = 8 << 20;
constexpr uint32_t kMinDataCellBytes = 256;
constexpr uint32_t kMaxDataCellBytes = 64 << 10;
constexpr uint32_t kDictMinRepeatRatio = 4;
/// A pressure spill writes this share of cell data, largest partitions first.
constexpr uint64_t kPartialSpillDivisor = 2;
/// The memory a writer counts on when no limit is known.
constexpr int64_t kUnlimitedBudgetBytes = int64_t{1} << 30;

/// CellShuffleOptions::spillMemoryBytes of -1: no cap of the spill memory's
/// own, which grows until the first memory pressure.
constexpr int64_t kNoSpillMemoryCap = std::numeric_limits<int64_t>::max();

} // namespace

CellShuffleWriter::CellShuffleWriter(
    ShuffleWriterOptions options,
    memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool)
    : CompositeShuffleWriter(std::move(options), boltPool, arrowPool),
      isRemote_(
          options_.partitionWriterOptions.partitionWriterType ==
          PartitionWriterType::kCeleborn) {
  metrics_.useCellShuffle = 1;
  // Composite-only input needs an output before ordinary layout initialization.
  if (isRemote_) {
    BOLT_CHECK_NOT_NULL(
        options_.partitionWriterOptions.rssClient,
        "CellShuffleWriter on Celeborn requires an RSS client");
    blocks_ = std::make_unique<RssBlockOutput>(options_.partitionWriterOptions);
    output_ = std::make_unique<RssCellOutput>(
        *blocks_,
        &layout_,
        options_.partitionWriterOptions,
        options_.cellOptions,
        boltPool_);
  } else {
    // Ordinary and composite outputs share one spill-memory budget.
    const int64_t spillMemoryBytes = options_.cellOptions.spillMemoryBytes;
    spillMemory_ = std::make_unique<SpillMemory>(
        boltPool_,
        spillMemoryBytes >= 0 ? spillMemoryBytes : kNoSpillMemoryCap);
    auto blocks = std::make_unique<LocalBlockOutput>(
        options_.partitionWriterOptions, spillMemory_.get());
    output_ = std::make_unique<LocalCellOutput>(
        *blocks,
        &layout_,
        options_.cellOptions,
        options_.partitionWriterOptions,
        boltPool_,
        spillMemory_.get());
    blocks_ = std::move(blocks);
  }
}

CellShuffleWriter::~CellShuffleWriter() {
  // Failed split/stop paths must return the chunk reservations as well.
  shuffleCheck_.reset();
  shredder_.reset();
  splitter_.reset();
  output_.reset();
  allocator_.reset();
  boltPool_->release();
}

void CellShuffleWriter::initOnFirstBatch(const RowVector& rv) {
  const auto& inputType = rv.type()->asRow();
  const uint32_t columnOffset = partitioner_->hasPid() ? 1 : 0;
  BOLT_CHECK_GE(inputType.size(), columnOffset, "expected a pid column");
  auto logicalType = ROW(
      std::vector<std::string>(
          inputType.names().begin() + columnOffset, inputType.names().end()),
      std::vector<TypePtr>(
          inputType.children().begin() + columnOffset,
          inputType.children().end()));
  BOLT_CHECK(
      CellShuffleTypeAdapter::isSupported(logicalType),
      "Unsupported Cell logical schema: {}",
      logicalType->toString());
  if (options_.cellOptions.nativeNestedTypes &&
      CellNestedLayout::supports(logicalType)) {
    nestedLayout_ = std::make_unique<CellNestedLayout>(std::move(logicalType));
    layout_ = nestedLayout_->layout();
  } else {
    auto adapter =
        std::make_unique<CellShuffleTypeAdapter>(asRowType(rv.type()));
    const auto& physicalType = adapter->physicalType()->asRow();
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(physicalType.size() - columnOffset);
    types.reserve(physicalType.size() - columnOffset);
    for (uint32_t i = columnOffset; i < physicalType.size(); ++i) {
      names.push_back(physicalType.nameOf(i));
      types.push_back(physicalType.childAt(i));
    }
    layout_ = CellLayout::create(ROW(std::move(names), std::move(types)));
    if (adapter->hasComplexColumns()) {
      adapter_ = std::move(adapter);
    }
  }

  const uint32_t numStreams = layout_.numStreams();
  // Use the tighter configured pool/task limit; larger sentinel values are
  // unbounded. Size cells here, once the stream count is known.
  const auto bounded = [](int64_t bytes) {
    return bytes > 0 && bytes <= (int64_t{1} << 40);
  };
  const int64_t poolCap = boltPool_->maxCapacity();
  const auto perTask =
      memory::sparksql::ExecutionMemoryPool::getAvailableMemoryPerTask();
  int64_t limit = bounded(poolCap) ? poolCap : 0;
  const char* limitSource = limit > 0 ? "pool cap" : "none";
  if (perTask.has_value() && bounded(*perTask) &&
      (limit == 0 || *perTask < limit)) {
    limit = *perTask;
    limitSource = "task memory";
  }
  const int64_t budget = limit > 0
      ? std::min<int64_t>(limit / 4, kUnlimitedBudgetBytes)
      : kUnlimitedBudgetBytes;
  const int64_t perStream =
      budget / 8 / numPartitions_ / std::max<uint32_t>(1, numStreams);
  const auto cellBytes = static_cast<uint32_t>(
      folly::prevPowTwo(static_cast<uint64_t>(std::clamp<int64_t>(
          perStream, kMinDataCellBytes, kMaxDataCellBytes))));
  // Bound the minimum useful run by the task's memory budget.
  const int64_t runFloor = options_.cellOptions.reclaimRunFloorBytes;
  BOLT_USER_CHECK_GE(
      runFloor,
      0,
      "CellShuffleOptions::reclaimRunFloorBytes must not be negative");
  reclaimRunFloorBytes_ =
      limit > 0 ? std::min<int64_t>(limit / 8, runFloor) : runFloor;
  const double spillRatio = options_.cellOptions.cellMemorySpillRatio;
  BOLT_USER_CHECK(
      spillRatio >= 0 && spillRatio <= 2,
      "CellShuffleOptions::cellMemorySpillRatio must be within [0, 2], got {}",
      spillRatio);
  memorySpillThresholdBytes_ =
      static_cast<int64_t>(spillRatio * static_cast<double>(limit));
  // A data threshold triggers spilling even without memory pressure.
  const int64_t perStreamSpill = options_.cellOptions.cellSpillBytesPerStream;
  BOLT_USER_CHECK_GT(
      perStreamSpill,
      0,
      "CellShuffleOptions::cellSpillBytesPerStream must be positive");
  const int64_t minSpill = options_.cellOptions.cellSpillMinBytes;
  BOLT_USER_CHECK_GE(
      minSpill,
      0,
      "CellShuffleOptions::cellSpillMinBytes must not be negative");
  const __uint128_t threshold = static_cast<__uint128_t>(perStreamSpill) *
      numPartitions_ * std::max<uint32_t>(1, numStreams);
  cellSpillThresholdBytes_ = std::max<uint64_t>(
      static_cast<uint64_t>(minSpill),
      static_cast<uint64_t>(std::min<__uint128_t>(threshold, UINT64_MAX)));

  allocator_ = std::make_unique<ChunkAllocator>(
      boltPool_, kChunkBytes, cellBytes, [this]() { prepareChunkGrowth(); });
  splitter_ = std::make_unique<CellSplitter>(
      layout_,
      numPartitions_,
      *allocator_,
      *output_,
      *boltPool_,
      options_.cellOptions.enableNonTemporalStores);
  if (nestedLayout_ != nullptr) {
    shredder_ = std::make_unique<CellNestedShredder>(
        *nestedLayout_, *splitter_, numPartitions_, *boltPool_, &sealSpillNs_);
  }

  shuffleCheck_ = CellShuffleCheck::create(
      *layout_.rowType(),
      numPartitions_,
      options_,
      nestedLayout_ == nullptr && adapter_ == nullptr,
      boltPool_);
  splitter_->setStreamCheck(shuffleCheck_.get());

  initialized_ = true;
  const std::string spillMemory = spillMemory_ == nullptr ? "0"
      : spillMemory_->capBytes() == kNoSpillMemoryCap
      ? "no cap"
      : std::to_string(spillMemory_->capBytes());
  LOG(INFO) << "CellShuffleWriter init: layout="
            << (nestedLayout_ != nullptr  ? "native nested"
                    : adapter_ != nullptr ? "adapter"
                                          : "flat")
            << ", logicalColumns=" << inputType.size() - columnOffset
            << ", layoutColumns=" << layout_.numColumns()
            << ", streams=" << numStreams << ", partitions=" << numPartitions_
            << ", maxCapacity=" << poolCap << ", perTask="
            << (perTask.has_value() ? std::to_string(*perTask) : "none")
            << ", limit=" << limit << " (" << limitSource
            << "), budget=" << budget << ", perStream=" << perStream
            << ", cellBytes=" << cellBytes
            << ", reclaimRunFloor=" << reclaimRunFloorBytes_
            << ", memorySpillThreshold=" << memorySpillThresholdBytes_
            << ", spillMemory=" << spillMemory
            << ", cellSpillThreshold=" << cellSpillThresholdBytes_;
}

arrow::Status CellShuffleWriter::splitOrdinary(
    RowVectorPtr rv,
    int64_t /*memLimitIgnored: cell memory decisions never use it*/) {
  if (!initialized_) {
    initOnFirstBatch(*rv);
  }
  if (adapter_ != nullptr) {
    const uint64_t start = currentTimeNs();
    const uint64_t sealSpillStart = sealSpillNs_;
    rv = adapter_->pack(rv, boltPool_);
    // Less the spills of a reclaim that the pack's allocations set off.
    const auto elapsed = static_cast<int64_t>(currentTimeNs() - start);
    const auto sealSpill = static_cast<int64_t>(sealSpillNs_ - sealSpillStart);
    metrics_.convertTime += std::max<int64_t>(0, elapsed - sealSpill);
  }
  // Load lazy columns before sizing slices; unloaded columns estimate as zero.
  if (isLazyNotLoaded(*rv)) {
    rv->loadedVector();
  }
  const int64_t flatSize = rv->estimateFlatSize();
  // Reserve both the slice and room to push the largest partition payload.
  const auto prepareRemoteSlice = [this](int64_t estimatedBytes) {
    if (!isRemote_ || !splitter_->hasWindowRows()) {
      return;
    }
    const int64_t sliceBytes = estimatedBytes + kChunkBytes;
    const int64_t pushBytes =
        2 * static_cast<int64_t>(splitter_->maxPartitionBytes());
    const int64_t cap = memorySpillThresholdBytes_;
    if ((cap > 0 && allocator_->allocatedBytes() + sliceBytes > cap) ||
        !boltPool_->maybeReserve(sliceBytes + pushBytes)) {
      releaseWindow(SealReason::kReserveFailed);
    }
  };
  const int64_t maxBytes =
      isRemote_ ? kRemoteSliceBytes : kMaxShuffleWriterBatchBytes;
  if (flatSize > maxBytes && rv->size() > 1) {
    const int64_t pieces = std::max<int64_t>(
        1, std::min<int64_t>(rv->size(), (flatSize + maxBytes - 1) / maxBytes));
    const int32_t rowsPerPiece = (rv->size() + pieces - 1) / pieces;
    for (int32_t begin = 0; begin < rv->size(); begin += rowsPerPiece) {
      const int32_t length =
          std::min<int32_t>(rowsPerPiece, rv->size() - begin);
      auto piece =
          std::dynamic_pointer_cast<RowVector>(rv->slice(begin, length));
      BOLT_CHECK_NOT_NULL(piece);
      prepareRemoteSlice(flatSize * length / rv->size());
      RETURN_NOT_OK(splitBatch(std::move(piece)));
    }
    return arrow::Status::OK();
  }
  prepareRemoteSlice(flatSize);
  return splitBatch(std::move(rv));
}

arrow::Status CellShuffleWriter::splitBatch(RowVectorPtr rv) {
  if (shredder_ != nullptr) {
    return splitNested(std::move(rv));
  }
  const uint64_t start = currentTimeNs();
  const uint64_t sealSpillStart = sealSpillNs_;
  // Payload row counts are u32, independent of the reader's batch size.
  if (splitter_->hasWindowRows() &&
      splitter_->maxWindowRows() + uint64_t(rv->size()) > kMaxPayloadRows) {
    LOG(INFO) << "CellShuffleWriter seals the window for its rows: "
              << "maxWindowRows=" << splitter_->maxWindowRows()
              << ", batchRows=" << rv->size();
    sealWindow(SealReason::kPositionLimit);
  }
  const uint32_t numRows = rv->size();
  if (numRows == 0) {
    return arrow::Status::OK();
  }
  {
    inSplit_ = true;
    SCOPE_EXIT {
      inSplit_ = false;
    };
    RETURN_NOT_OK(computePartitions(*rv));

    const uint64_t classifyStart = currentTimeNs();
    const uint32_t columnOffset = partitioner_->hasPid() ? 1 : 0;
    std::vector<DecodedVector> decoded(layout_.numColumns());
    std::vector<NullClass> nullClass(layout_.numColumns());
    bool anyNullable = false;
    for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
      if (layout_.isUnknownColumn(col)) {
        nullClass[col] = NullClass::kAllNull;
        continue;
      }
      auto& column = decoded[col];
      column.decode(*rv->childAt(col + columnOffset));
      // Classify once so stale null buffers do not add a per-row bit test.
      auto klass = NullClass::kNoNulls;
      if (column.mayHaveNulls()) {
        if (column.isConstantMapping()) {
          klass =
              column.isNullAt(0) ? NullClass::kAllNull : NullClass::kNoNulls;
        } else if (column.isIdentityMapping()) {
          const uint64_t* nulls = column.nulls();
          if (nulls == nullptr || bits::isAllSet(nulls, 0, numRows, true)) {
            klass = NullClass::kNoNulls;
          } else if (bits::isAllSet(nulls, 0, numRows, false)) {
            klass = NullClass::kAllNull;
          } else {
            klass = NullClass::kSomeNulls;
          }
        } else {
          klass = NullClass::kSomeNulls;
        }
      }
      nullClass[col] = klass;
      anyNullable = anyNullable || klass == NullClass::kSomeNulls;
    }
    nullClassifyNs_ += currentTimeNs() - classifyStart;
    if (anyNullable) {
      // All nullable columns share this per-partition row index.
      rowIndexInPid_.resize(numRows);
      perPidCounter_.resize(numPartitions_);
      std::fill(perPidCounter_.begin(), perPidCounter_.end(), 0);
      for (uint32_t row = 0; row < numRows; ++row) {
        rowIndexInPid_[row] = perPidCounter_[row2Partition_[row]]++;
      }
    }

    if (FOLLY_UNLIKELY(!dictProbed_)) {
      // Decide before writing any bytes: a payload's encoding cannot change.
      probeDictionary(numRows, decoded, nullClass);
      dictProbed_ = true;
    }
    SplitBatch batch;
    batch.decoded = decoded;
    batch.row2Partition = std::span(row2Partition_).first(numRows);
    batch.partition2RowCount = partition2RowCount_;
    batch.nullClass = nullClass;
    if (anyNullable) {
      batch.rowIndexInPid = rowIndexInPid_;
    }
    {
      auto cleanup = folly::makeGuard([&]() noexcept {
        if (shuffleCheck_ != nullptr) {
          shuffleCheck_->endBatch();
        }
      });
      if (shuffleCheck_ != nullptr) {
        shuffleCheck_->prepareInput(
            *rv, options_.partitioning == Partitioning::kHash);
        shuffleCheck_->beginBatch(batch);
      }
      splitter_->split(batch);
    }
    metrics_.totalInputRowNumber += numRows;
    metrics_.totalInputBatches += 1;
  }

  maybeSeal();
  const auto elapsed = static_cast<int64_t>(currentTimeNs() - start);
  const auto sealSpill = static_cast<int64_t>(sealSpillNs_ - sealSpillStart);
  metrics_.splitTime += std::max<int64_t>(0, elapsed - sealSpill);
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::splitNested(RowVectorPtr rv) {
  const uint64_t start = currentTimeNs();
  const uint64_t sealSpillStart = sealSpillNs_;
  if (splitter_->hasWindowRows() &&
      splitter_->maxWindowRows() + uint64_t(rv->size()) > kMaxPayloadRows) {
    sealWindow(SealReason::kPositionLimit);
  }
  const uint32_t numRows = rv->size();
  if (numRows == 0) {
    return arrow::Status::OK();
  }
  // Nested position counts can reach their bound even in a small byte payload.
  shredder_->prepare(*rv, partitioner_->hasPid() ? 1 : 0);
  if (!shredder_->fitsWindow()) {
    LOG(INFO) << "CellShuffleWriter seals the window for its nested "
              << "positions: maxWindowRows=" << splitter_->maxWindowRows()
              << ", batchRows=" << numRows;
    sealWindow(SealReason::kPositionLimit);
    BOLT_CHECK(shredder_->fitsWindow());
  }

  {
    inSplit_ = true;
    SCOPE_EXIT {
      inSplit_ = false;
    };
    RETURN_NOT_OK(computePartitions(*rv));
    if (FOLLY_UNLIKELY(!dictProbed_)) {
      probeDictionary(numRows, shredder_->decoded(), shredder_->nullClass());
      dictProbed_ = true;
    }
    shredder_->split(
        std::span(row2Partition_).first(numRows), partition2RowCount_);
    metrics_.totalInputRowNumber += numRows;
    metrics_.totalInputBatches += 1;
  }

  maybeSeal();
  // Shredding and sealing are disjoint metrics, both excluded from splitTime.
  const int64_t shred = shredder_->takeShredTimeNs();
  metrics_.flattenTime += shred;
  const auto elapsed = static_cast<int64_t>(currentTimeNs() - start);
  const auto sealSpill = static_cast<int64_t>(sealSpillNs_ - sealSpillStart);
  metrics_.splitTime += std::max<int64_t>(0, elapsed - shred - sealSpill);
  return arrow::Status::OK();
}

void CellShuffleWriter::probeDictionary(
    uint32_t numRows,
    std::span<const DecodedVector> decoded,
    std::span<const NullClass> nullClass) {
  const auto& cellOpts = options_.cellOptions;
  // The first batch fixes dictionary eligibility for the writer lifetime.
  const auto skipped = [&](const std::string& reason) {
    LOG(INFO) << fmt::format(
        "CellShuffleWriter dictionary probe skipped ({} rows): {}",
        numRows,
        reason);
  };
  if (!cellOpts.enableStringDictionary) {
    skipped("enableStringDictionary is off");
    return;
  }
  if (numRows < cellOpts.stringDictionaryProbeRowCount) {
    skipped(fmt::format(
        "a first batch below {} rows, the probe minimum",
        cellOpts.stringDictionaryProbeRowCount));
    return;
  }
  // Per-column decisions, logged together after the loop.
  std::string enabled;
  std::string disabled;
  uint32_t nestedStrings = 0;
  uint32_t allNullStrings = 0;
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    // Every string column, or a nested layout's at the rows' positions.
    if (!layout_.isStringColumn(col)) {
      continue;
    }
    if (nestedLayout_ != nullptr && !nestedLayout_->inRowSpace(col)) {
      ++nestedStrings;
      continue;
    }
    if (cellOpts.stringDictionaryProbeRowCount == 0) {
      splitter_->enableDictionary(col);
      enabled +=
          fmt::format("{}col {} (forced)", enabled.empty() ? "" : ", ", col);
      continue;
    }
    if (nullClass[col] == NullClass::kAllNull) {
      ++allNullStrings;
      continue;
    }
    const auto& column = decoded[col];
    const bool hasNulls = nullClass[col] == NullClass::kSomeNulls;
    // Stop probing once the column cannot fit the dictionary budget.
    std::vector<StringView> seen;
    uint32_t serialized = 0;
    uint64_t nonNull = 0;
    bool fits = true;
    for (uint32_t row = 0; row < numRows; ++row) {
      if (hasNulls && column.isNullAt(row)) {
        continue;
      }
      const auto view = column.valueAt<StringView>(row);
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
    auto& list = enable ? enabled : disabled;
    list += fmt::format(
        "{}col {} (ndv={}{}, {}B, nonNull={})",
        list.empty() ? "" : ", ",
        col,
        seen.size(),
        fits ? "" : "+",
        serialized,
        nonNull);
  }
  if (enabled.empty() && disabled.empty()) {
    skipped(
        nestedStrings + allNullStrings == 0
            ? std::string("no string column")
            : fmt::format(
                  "no string column to probe: {} all null in the first "
                  "batch, {} inside nested values",
                  allNullStrings,
                  nestedStrings));
    return;
  }
  std::string parts;
  if (!enabled.empty()) {
    parts = "ON " + enabled;
  }
  if (!disabled.empty()) {
    parts += fmt::format("{}OFF {}", parts.empty() ? "" : "; ", disabled);
  }
  LOG(INFO) << fmt::format(
      "CellShuffleWriter dictionary probe ({} rows): {}", numRows, parts);
}

CellShuffleWriter::SealSpillTimer::SealSpillTimer(CellShuffleWriter& writer)
    : writer_(
          writer.inSplitCall() && !writer.timingSealSpill_ ? &writer : nullptr),
      start_(writer_ != nullptr ? currentTimeNs() : 0) {
  if (writer_ != nullptr) {
    writer_->timingSealSpill_ = true;
  }
}

CellShuffleWriter::SealSpillTimer::~SealSpillTimer() {
  if (writer_ != nullptr) {
    writer_->sealSpillNs_ += currentTimeNs() - start_;
    writer_->timingSealSpill_ = false;
  }
}

void CellShuffleWriter::prepareChunkGrowth() {
  // While the output reads cells nothing may spill, so growth there
  // proceeds unreserved and the pool arbitrates.
  if (!initialized_ || splitter_->isWritingOutput()) {
    return;
  }
  // Count both allocated chunks and retained spill bytes.
  const auto overBytes = [&]() {
    return allocator_->allocatedBytes() + kChunkBytes +
        (spillMemory_ != nullptr ? spillMemory_->heldBytes() : 0) -
        memorySpillThresholdBytes_;
  };
  const auto overCap = [&]() {
    return memorySpillThresholdBytes_ > 0 && overBytes() > 0;
  };
  if (isRemote_) {
    // Defer remote pushes until the batch is complete.
    if (overCap() || !boltPool_->maybeReserve(kChunkBytes)) {
      sealRequested_ = true;
    }
    return;
  }
  if (overCap() || !boltPool_->maybeReserve(kChunkBytes)) {
    // Flush buffered spill bytes first, at least one reservation quantum.
    SealSpillTimer timer(*this);
    const int64_t over = overCap() ? overBytes() : 0;
    if (spillMemory_->release(std::max<int64_t>(over, 2 * kChunkBytes)) > 0 &&
        !overCap() && boltPool_->maybeReserve(kChunkBytes)) {
      ++pressure_.growthFlushes;
      return;
    }
    // Growth retries the freed cells before allocating another chunk.
    const uint64_t spilled = splitter_->spillPartitions(
        static_cast<uint64_t>(allocator_->usedBytes()) / kPartialSpillDivisor);
    if (spilled > 0) {
      ++pressure_.growthPartialRuns;
      pressure_.growthPartialRunBytes += spilled;
      metrics_.totalBytesEvicted = output_->bytesEvicted();
    }
  }
}

void CellShuffleWriter::sealWindow(SealReason reason) {
  SealSpillTimer timer(*this);
  ++pressure_.seals[static_cast<size_t>(reason)];
  {
    sealing_ = true;
    SCOPE_EXIT {
      sealing_ = false;
    };
    splitter_->sealWindow(columnPositionCounts());
  }
  sealRequested_ = false;
  // A sealed window is a batch boundary: drop reservation slack.
  boltPool_->release();
}

int64_t CellShuffleWriter::releaseWindow(SealReason reason) {
  // The seal - on Celeborn the push - and the chunks it empties, together.
  SealSpillTimer timer(*this);
  sealWindow(reason);
  // Every cell was recycled by the seal: the chunks go back as well.
  const int64_t freed = allocator_->shrink();
  boltPool_->release();
  return freed;
}

void CellShuffleWriter::maybeSeal() {
  if (!initialized_ || !splitter_->hasWindowRows()) {
    return;
  }
  pressure_.nullPeakBytes = std::max<uint64_t>(
      pressure_.nullPeakBytes, splitter_->nullFootprintBytes());
  if (isRemote_ && sealRequested_) {
    // Memory ran short mid-batch: push the window now that it is whole.
    releaseWindow(SealReason::kMemoryPressure);
    return;
  }
  const auto& cellOpts = options_.cellOptions;
  if (sealRequested_) {
    sealWindow(SealReason::kMemoryPressure);
  } else if (splitter_->nullMovedBytes() > cellOpts.nullMemLimitBytes) {
    sealWindow(SealReason::kNullBitmapLimit);
  } else if (
      splitter_->maxPartitionBytes() >
      static_cast<uint64_t>(cellOpts.partitionSealBytes)) {
    // Close only oversized partitions; other payloads remain open.
    SealSpillTimer timer(*this);
    sealing_ = true;
    SCOPE_EXIT {
      sealing_ = false;
    };
    const uint32_t sealed = splitter_->sealPartitionsOver(
        static_cast<uint64_t>(cellOpts.partitionSealBytes),
        columnPositionCounts());
    pressure_.partitionSeals += sealed > 0;
    pressure_.partitionsSealed += sealed;
  }
  const uint64_t cellData = splitter_->cellDataBytes();
  if (!isRemote_ && cellData > cellSpillThresholdBytes_) {
    // Spill the largest partitions down to half the data threshold.
    SealSpillTimer timer(*this);
    const uint64_t spilled =
        splitter_->spillPartitions(cellData - cellSpillThresholdBytes_ / 2);
    if (spilled > 0) {
      ++pressure_.thresholdRuns;
      pressure_.thresholdRunBytes += spilled;
      metrics_.totalBytesEvicted = output_->bytesEvicted();
    }
  }
}

arrow::Status CellShuffleWriter::reclaimOrdinary(
    int64_t size,
    int64_t* actual) {
  *actual = 0;
  if (!initialized_) {
    // No ordinary batch yet: only the composite blocks may hold spill
    // memory.
    if (spillMemory_ != nullptr) {
      *actual =
          size > 0 ? spillMemory_->release(size) : spillMemory_->releaseAll();
    }
    if (*actual > 0) {
      boltPool_->release();
    }
    return arrow::Status::OK();
  }
  ++pressure_.reclaims;
  pressure_.requestedBytes += std::max<int64_t>(size, 0);
  SCOPE_EXIT {
    pressure_.freedBytes += std::max<int64_t>(*actual, 0);
  };
  // Idle chunks and unused shredder scratch can be released even during output.
  *actual = allocator_->shrink();
  if (shredder_ != nullptr) {
    *actual += shredder_->releaseScratch();
  }
  pressure_.freedIdle += *actual;
  boltPool_->release();
  if (isStopped()) {
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  if (spillMemory_ != nullptr) {
    // Output reserves between blocks, so its completed spill blocks are
    // reclaimable.
    SealSpillTimer timer(*this);
    const int64_t freed = size > 0 ? spillMemory_->release(size - *actual)
                                   : spillMemory_->releaseAll();
    if (freed > 0) {
      pressure_.freedSpillMemory += freed;
      *actual += freed;
      boltPool_->release();
      metrics_.totalBytesEvicted = output_->bytesEvicted();
    }
  }
  if (splitter_->isWritingOutput()) {
    // The output is reading the cells: no run may drain them now.
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  if (isRemote_) {
    return reclaimWindow(size, actual);
  }
  SealSpillTimer timer(*this);
  const auto maybeRequestNullSeal = [&]() {
    // Seal only when enough null state can be freed to justify closing
    // payloads.
    if (splitter_->nullMovedBytes() >
        options_.cellOptions.nullMemLimitBytes / 4) {
      sealRequested_ = true;
    }
  };
  // Avoid a run when idle memory suffices or fewer than two chunks remain.
  if (size > 0 && *actual >= size) {
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  if (allocator_->allocatedBytes() < 2 * kChunkBytes) {
    maybeRequestNullSeal();
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  // Small requests below the run floor must not produce many tiny runs.
  if (size > 0 &&
      splitter_->cellDataBytes() <
          static_cast<uint64_t>(reclaimRunFloorBytes_)) {
    maybeRequestNullSeal();
    ++pressure_.belowFloorReclaims;
    return arrow::Status::OK();
  }
  // Spill large partitions, then compact to free whole chunks. Fall back to
  // a full run only if the request remains unmet.
  if (size > 0) {
    const uint64_t spilled = splitter_->spillPartitions(
        static_cast<uint64_t>(std::max(size - *actual, kMinReclaimRunBytes)));
    if (spilled > 0) {
      ++pressure_.reclaimPartialRuns;
      pressure_.reclaimPartialRunBytes += spilled;
    }
    int64_t freed = allocator_->shrink();
    pressure_.freedByPartialRun += freed;
    *actual += freed;
    if (*actual < size) {
      ++pressure_.compactions;
      pressure_.compactedBytes +=
          splitter_->compactCells(static_cast<uint64_t>(size - *actual));
      freed = allocator_->shrink();
      pressure_.freedByCompaction += freed;
      *actual += freed;
    }
  }
  if (size <= 0 || *actual < size) {
    ++pressure_.fullRuns;
    splitter_->spillRun();
    const int64_t freed = allocator_->shrink();
    pressure_.freedByRun += freed;
    *actual += freed;
  }
  // Return reservation slack so the arbitrator can reuse the freed capacity.
  boltPool_->release();
  maybeRequestNullSeal();
  metrics_.totalBytesEvicted = output_->bytesEvicted();
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::reclaimWindow(int64_t size, int64_t* actual) {
  // Remote pushes must wait for a complete batch and an inactive seal.
  if (inSplit_ || sealing_) {
    sealRequested_ = true;
    ++pressure_.deferredReclaims;
    return arrow::Status::OK();
  }
  if ((size > 0 && *actual >= size) || !splitter_->hasWindowRows() ||
      allocator_->allocatedBytes() < 2 * kChunkBytes) {
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  const int64_t freed = releaseWindow(SealReason::kMemoryPressure);
  pressure_.freedByPush += freed;
  *actual += freed;
  metrics_.totalBytesEvicted = output_->bytesEvicted();
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::flushOrdinary() {
  // Switching to composite input releases the ordinary window.
  if (initialized_ && splitter_->hasWindowRows()) {
    releaseWindow(SealReason::kComposite);
  }
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::stopOrdinary() {
  if (initialized_) {
    try {
      splitter_->finish(metrics_, columnPositionCounts());
    } catch (...) {
      logPressure();
      throw;
    }
  } else {
    // Empty or composite-only input needs only the block output.
    blocks_->finish(metrics_);
    metrics_.rawPartitionLengths.assign(numPartitions_, 0);
  }
  // Report uncompressed payload volume separately from bytes written.
  metrics_.dataSize = std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      int64_t{0});
  metrics_.sealSpillTime += static_cast<int64_t>(sealSpillNs_);
  if (initialized_) {
    logPressure();
  }

  shuffleCheck_.reset();

  // The splitter has released its window; return idle chunks, the
  // shredder's scratch and reservation.
  if (shredder_ != nullptr) {
    shredder_->releaseScratch();
  }
  if (allocator_ != nullptr) {
    allocator_->shrink();
  }
  boltPool_->release();
  return arrow::Status::OK();
}

void CellShuffleWriter::logPressure() const {
  std::string spillMemory;
  if (spillMemory_ != nullptr) {
    const auto mb = [](uint64_t bytes) { return bytes / double(1 << 20); };
    spillMemory = fmt::format(
        ", spill memory: held peak={:.1f}MB flushed={:.1f}MB ({} flushes) "
        "direct={:.1f}MB",
        mb(spillMemory_->peakBytes()),
        mb(spillMemory_->flushedBytes()),
        spillMemory_->flushes(),
        mb(spillMemory_->directBytes()));
  }
  LOG(INFO) << "CellShuffleWriter pressure: " << pressure_.toString()
            << fmt::format(
                   ", nullClassify={:.1f}ms, sealSpill={:.1f}ms",
                   nullClassifyNs_ / 1e6,
                   sealSpillNs_ / 1e6)
            << spillMemory;
}

std::string CellShuffleWriter::PressureStats::toString() const {
  const auto mb = [](uint64_t bytes) { return bytes / double(1 << 20); };
  const auto seal = [&](SealReason reason) {
    return seals[static_cast<size_t>(reason)];
  };
  return fmt::format(
      "reclaims={} (idle={} below floor={} deferred={}) requested={:.1f}MB "
      "freed={:.1f}MB [idle={:.1f}MB memory={:.1f}MB spill={:.1f}MB "
      "compaction={:.1f}MB run={:.1f}MB push={:.1f}MB]; partial runs "
      "reclaim={} ({:.1f}MB) growth={} ({:.1f}MB), growth flushes={}; "
      "compactions={} ({:.1f}MB emptied); full runs={}; seals positions={} "
      "memoryPressure={} nullBitmapLimit={} composite={} reserveFailed={}; partition seals={} ({} "
      "partitions); null peak={:.1f}MB; threshold runs={} ({:.1f}MB)",
      reclaims,
      idleReclaims,
      belowFloorReclaims,
      deferredReclaims,
      mb(requestedBytes),
      mb(freedBytes),
      mb(freedIdle),
      mb(freedSpillMemory),
      mb(freedByPartialRun),
      mb(freedByCompaction),
      mb(freedByRun),
      mb(freedByPush),
      reclaimPartialRuns,
      mb(reclaimPartialRunBytes),
      growthPartialRuns,
      mb(growthPartialRunBytes),
      growthFlushes,
      compactions,
      mb(compactedBytes),
      fullRuns,
      seal(SealReason::kPositionLimit),
      seal(SealReason::kMemoryPressure),
      seal(SealReason::kNullBitmapLimit),
      seal(SealReason::kComposite),
      seal(SealReason::kReserveFailed),
      partitionSeals,
      partitionsSealed,
      mb(nullPeakBytes),
      thresholdRuns,
      mb(thresholdRunBytes));
}

std::string CellShuffleWriter::toString() const {
  return "CellShuffleWriter";
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
