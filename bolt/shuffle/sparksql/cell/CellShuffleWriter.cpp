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

#include <numeric>
#include <optional>

#include <fmt/format.h>

#include <folly/ScopeGuard.h>
#include <folly/lang/Bits.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

constexpr uint32_t kChunkBytes = 4 << 20;
/// The least a reclaim's partial run writes. Memory goes back to the pool
/// only in whole chunks: a run of less than a chunk leaves the cells it
/// frees scattered over every chunk, so shrink() returns nothing, and
/// compaction cannot empty a chunk either - moving one chunk's live cells
/// takes about a chunk of free cells elsewhere - so the reclaim would fall
/// through to a full run. Reclaims reach the writer with no minimum size,
/// a few hundred KB at times. Two chunks leave a chunk of margin - the
/// cells of an append in progress cannot move, and partitions go whole -
/// and match maybeReserve()'s 8 MiB quantum. What is freed beyond the
/// request stays free capacity of the task's root pool, so the next small
/// allocation needs no reclaim.
constexpr int64_t kMinReclaimRunBytes = 2 * kChunkBytes;
/// Slice bound on a remote shuffle, where a window is released only whole
/// at a batch boundary: one slice bounds how far memory runs past a failed
/// reservation before the window can be pushed.
constexpr int64_t kRemoteSliceBytes = 8 << 20;
constexpr uint32_t kMinDataCellBytes = 256;
constexpr uint32_t kMaxDataCellBytes = 64 << 10;
/// The fewest rows the dictionary probe judges: the sixteen entries of a
/// dictionary segment, kDictMinRepeatRatio times each. A table scan sizes
/// its batches by the estimated bytes of the rows it reads, so a scan of a
/// wide table can start a narrow shuffle with batches of a few hundred
/// rows.
constexpr uint32_t kDictMinProbeRows = 64;
constexpr uint32_t kDictMinRepeatRatio = 4;
/// A pressure spill writes the largest partitions until this share of the
/// cell data is covered, and keeps the rest resident for the merge to write
/// directly, as the V1/V2 writers evict partitions until the request is met.
constexpr uint64_t kPartialSpillDivisor = 2;
/// The memory a writer counts on when no limit is known.
constexpr int64_t kUnlimitedBudgetBytes = int64_t{1} << 30;

/// The memory the writer can count on: the tighter of the root pool's cap,
/// which maxCapacity() reads, and the Spark execution memory per task, each
/// where it is set (a pool without a cap reports kMaxMemory: under Spark the
/// limit lives in the task's execution memory).
struct MemoryLimit {
  int64_t poolCap;
  std::optional<int64_t> perTask;
  /// 0 when neither is set.
  int64_t bytes;
  const char* source;
};

MemoryLimit memoryLimit(const memory::MemoryPool& pool) {
  constexpr int64_t kUnlimited = int64_t{1} << 40;
  const auto bounded = [](int64_t bytes) {
    return bytes > 0 && bytes <= kUnlimited;
  };
  MemoryLimit limit;
  limit.poolCap = pool.maxCapacity();
  limit.perTask =
      memory::sparksql::ExecutionMemoryPool::getAvailableMemoryPerTask();
  limit.bytes = bounded(limit.poolCap) ? limit.poolCap : 0;
  limit.source = limit.bytes > 0 ? "pool cap" : "none";
  if (limit.perTask.has_value() && bounded(*limit.perTask) &&
      (limit.bytes == 0 || *limit.perTask < limit.bytes)) {
    limit.bytes = *limit.perTask;
    limit.source = "task memory";
  }
  return limit;
}

/// The caps of the spill memory: of its pages, and of the writer's pool
/// once it takes one.
struct SpillMemoryCaps {
  int64_t capBytes;
  int64_t totalCapBytes;
};

/// CellShuffleOptions::spillMemoryBytes resolved. Whatever it says, a page
/// may take the writer's pool - cells and pages together - to half the
/// memory limit at most, or half the budget assumed without one: the pages'
/// cap alone would let them take the whole limit beside the cells. -1 caps
/// the pages at that half as well.
SpillMemoryCaps spillMemoryCaps(
    const CellShuffleOptions& options,
    const memory::MemoryPool& pool) {
  const int64_t limit = memoryLimit(pool).bytes;
  const int64_t half = (limit > 0 ? limit : kUnlimitedBudgetBytes) / 2;
  return {
      options.spillMemoryBytes >= 0 ? options.spillMemoryBytes : half, half};
}

} // namespace

CellShuffleWriter::CellShuffleWriter(
    ShuffleWriterOptions options,
    memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool)
    : CompositeShuffleWriter(std::move(options), boltPool, arrowPool),
      isRemote_(
          options_.partitionWriterOptions.partitionWriterType ==
          PartitionWriterType::kCeleborn) {
  // Built up front: the composite side writes through it even when no
  // ordinary batch ever comes. Neither backend reads layout_ before the
  // first ordinary batch fills it.
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
    // Two outputs with a spill file each, merged into the one data file:
    // the Cell output, and `blocks`, which serves the composite rows. One
    // cap of spill memory covers both: only one kind holds data at a time,
    // so the kind written now gets what the other does not hold. Either
    // takes a page only while the writer's whole pool stays within half its
    // memory limit.
    const auto caps = spillMemoryCaps(options_.cellOptions, *boltPool_);
    spillMemory_ = std::make_unique<SpillMemory>(
        boltPool_, caps.capBytes, caps.totalCapBytes);
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
  shredder_.reset();
  splitter_.reset();
  output_.reset();
  allocator_.reset();
  boltPool_->release();
}

void CellShuffleWriter::initOnFirstBatch(const RowVector& rv) {
  const auto& inputType = rv.type()->asRow();
  // A pid column alone is legal: the payloads then carry only row counts.
  BOLT_CHECK_GE(inputType.size(), 1, "expected a pid column");
  auto logicalType =
      ROW(std::vector<std::string>(
              inputType.names().begin() + 1, inputType.names().end()),
          std::vector<TypePtr>(
              inputType.children().begin() + 1, inputType.children().end()));
  BOLT_CHECK(
      CellShuffleTypeAdapter::isSupported(logicalType),
      "Unsupported Cell logical schema: {}",
      logicalType->toString());
  if (options_.cellOptions.nativeNestedTypes &&
      CellNestedLayout::supports(logicalType)) {
    // ARRAY, MAP and ROW columns shred into plain columns, a container's
    // elements at positions of their own.
    nested_ = std::make_unique<CellNestedLayout>(std::move(logicalType));
    layout_ = nested_->layout();
  } else {
    // The adapter rejects unsupported logical types and maps complex
    // columns onto one trailing binary column; the pid column stays first.
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
  }

  const uint32_t numStreams = layout_.numStreams();
  // Cell tail slack is bounded by a share of the memory the writer can count
  // on, so sizing from a budget far above the real one wastes a large part
  // of the real memory in half-filled tail cells. With no limit, a 1 GiB
  // budget.
  const auto memory = memoryLimit(*boltPool_);
  const int64_t limit = memory.bytes;
  const int64_t budget = limit > 0
      ? std::min<int64_t>(limit / 4, kUnlimitedBudgetBytes)
      : kUnlimitedBudgetBytes;
  const int64_t perStream =
      budget / 8 / numPartitions_ / std::max<uint32_t>(1, numStreams);
  const auto cellBytes = static_cast<uint32_t>(
      folly::prevPowTwo(static_cast<uint64_t>(std::clamp<int64_t>(
          perStream, kMinDataCellBytes, kMaxDataCellBytes))));
  // Many small reclaims must not each write a run of a few bytes per
  // partition: one of a given size waits for this much cell data, at most
  // an eighth of the limit.
  const int64_t runFloor = options_.cellOptions.reclaimRunFloorBytes;
  BOLT_USER_CHECK_GE(
      runFloor,
      0,
      "CellShuffleOptions::reclaimRunFloorBytes must not be negative");
  reclaimRunFloorBytes_ =
      limit > 0 ? std::min<int64_t>(limit / 8, runFloor) : runFloor;

  allocator_ = std::make_unique<ChunkAllocator>(
      boltPool_, kChunkBytes, cellBytes, [this]() { reserveChunk(); });
  splitter_ = std::make_unique<CellSplitter>(
      layout_, numPartitions_, *allocator_, *output_, *boltPool_);
  if (nested_ != nullptr) {
    shredder_ = std::make_unique<CellNestedShredder>(
        *nested_, *splitter_, numPartitions_, *boltPool_, &sealSpillNs_);
  }

  perPidCounter_.assign(numPartitions_, 0);
  decoded_.resize(layout_.numColumns());

  initialized_ = true;
  const std::string spillMemory = spillMemory_ != nullptr
      ? fmt::format(
            "{} (writer total <= {})",
            spillMemory_->capBytes(),
            spillMemory_->totalCapBytes())
      : "0";
  LOG(INFO) << "CellShuffleWriter init: layout="
            << (nested_ != nullptr                  ? "native nested"
                    : adapter_->hasComplexColumns() ? "adapter"
                                                    : "flat")
            << ", logicalColumns=" << inputType.size() - 1
            << ", layoutColumns=" << layout_.numColumns()
            << ", streams=" << numStreams << ", partitions=" << numPartitions_
            << ", maxCapacity=" << memory.poolCap << ", perTask="
            << (memory.perTask.has_value() ? std::to_string(*memory.perTask)
                                           : "none")
            << ", limit=" << limit << " (" << memory.source
            << "), budget=" << budget << ", perStream=" << perStream
            << ", cellBytes=" << cellBytes
            << ", reclaimRunFloor=" << reclaimRunFloorBytes_
            << ", spillMemory=" << spillMemory;
}

arrow::Status CellShuffleWriter::splitOrdinary(
    RowVectorPtr rv,
    int64_t /*memLimitIgnored: cell memory decisions never use it*/) {
  BOLT_CHECK(!stopped_, "split after stop");
  if (!initialized_) {
    initOnFirstBatch(*rv);
  }
  if (shredder_ == nullptr && adapter_->hasComplexColumns()) {
    const uint64_t start = currentTimeNs();
    const uint64_t sealSpillStart = sealSpillNs_;
    rv = adapter_->pack(rv, boltPool_);
    // Less the spills of a reclaim that the pack's allocations set off.
    metrics_.convertTime += static_cast<int64_t>(
        currentTimeNs() - start - (sealSpillNs_ - sealSpillStart));
  }
  // Oversized inputs are sliced so a seal boundary exists inside
  // them: one giant batch must not inflate a single payload window past
  // the partition byte bound (the legacy writers slice the same way). Lazy
  // children are loaded first: unloaded they estimate as zero bytes and
  // the byte bound would never fire.
  if (isLazyNotLoaded(*rv)) {
    rv->loadedVector();
  }
  const int64_t flatSize = rv->estimateFlatSize();
  // Remote only: a window is pushed only whole, so reserve room for the
  // next slice of about `estimatedBytes` (plus the largest payload and its
  // compressed copy, which the push needs) before it starts; push the
  // window first if that fails.
  const auto reserveSlice = [this](int64_t estimatedBytes) {
    if (!isRemote_ || !splitter_->hasWindowRows()) {
      return;
    }
    const int64_t sliceBytes = estimatedBytes + kChunkBytes;
    const int64_t pushBytes =
        2 * static_cast<int64_t>(splitter_->maxPartitionBytes());
    const int64_t cap = options_.cellOptions.cellMemorySpillBytes;
    if ((cap > 0 && allocator_->allocatedBytes() + sliceBytes > cap) ||
        !boltPool_->maybeReserve(sliceBytes + pushBytes)) {
      releaseWindow(SealReason::kReserve);
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
      reserveSlice(flatSize * length / rv->size());
      RETURN_NOT_OK(splitBatch(std::move(piece)));
    }
    return arrow::Status::OK();
  }
  reserveSlice(flatSize);
  return splitBatch(std::move(rv));
}

arrow::Status CellShuffleWriter::splitBatch(RowVectorPtr rv) {
  if (shredder_ != nullptr) {
    return splitNested(std::move(rv));
  }
  const uint64_t start = currentTimeNs();
  const uint64_t sealSpillStart = sealSpillNs_;
  // The row fields of a payload are u32: a window closes before any of
  // its partitions could pass kMaxPayloadRows. Nothing else bounds the
  // rows - the reader decodes a payload in batches of its own size.
  if (splitter_->hasWindowRows() &&
      splitter_->maxWindowRows() + uint64_t(rv->size()) > kMaxPayloadRows) {
    LOG(INFO) << "CellShuffleWriter seals the window for its rows: "
              << "maxWindowRows=" << splitter_->maxWindowRows()
              << ", batchRows=" << rv->size();
    sealWindow(SealReason::kRows);
  }
  inSplit_ = true;
  const uint32_t numRows = rv->size();
  if (numRows == 0) {
    inSplit_ = false;
    return arrow::Status::OK();
  }

  auto status = computePartitions(*rv);
  if (!status.ok()) {
    inSplit_ = false;
    return status;
  }

  bool anyNullable = false;
  const uint64_t classifyStart = currentTimeNs();
  nullClass_.resize(layout_.numColumns());
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    if (layout_.isUnknownColumn(col)) {
      nullClass_[col] = NullClass::kAllNull;
      continue;
    }
    auto& decoded = decoded_[col];
    decoded.decode(*rv->childAt(col + 1));
    // Classify the batch's nulls up front: mayHaveNulls() only means a
    // buffer exists, and defensively allocated all-set buffers are common.
    // A word-level scan (~n/64 compares, early exit) is far cheaper than
    // taxing every row of the loop with a bit test for absent nulls.
    auto klass = NullClass::kNoNulls;
    if (decoded.mayHaveNulls()) {
      if (decoded.isConstantMapping()) {
        klass = decoded.isNullAt(0) ? NullClass::kAllNull : NullClass::kNoNulls;
      } else if (decoded.isIdentityMapping()) {
        const uint64_t* nulls = decoded.nulls();
        if (nulls == nullptr || bits::isAllSet(nulls, 0, numRows, true)) {
          klass = NullClass::kNoNulls; // stale buffer, no actual null
        } else if (bits::isAllSet(nulls, 0, numRows, false)) {
          klass = NullClass::kAllNull;
        } else {
          klass = NullClass::kSomeNulls;
        }
      } else {
        // Wrapped nulls: take the per-row path rather than materializing
        // a combined bitmap here.
        klass = NullClass::kSomeNulls;
      }
    }
    nullClass_[col] = klass;
    anyNullable = anyNullable || klass == NullClass::kSomeNulls;
  }
  nullClassifyNs_ += currentTimeNs() - classifyStart;
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
    // cannot change once written). A first batch below the probe minimum
    // therefore settles on no dictionary rather than deferring.
    probeDictionary(numRows, decoded_, nullClass_);
    dictProbed_ = true;
  }

  SplitBatch batch;
  batch.decoded = decoded_;
  batch.row2Partition = std::span(row2Partition_).first(numRows);
  batch.partition2RowCount = partition2RowCount_;
  batch.nullClass = nullClass_;
  if (anyNullable) {
    batch.rowIndexInPid = rowIndexInPid_;
  }
  splitter_->split(batch);

  metrics_.totalInputRowNumber += numRows;
  metrics_.totalInputBatches += 1;
  inSplit_ = false;

  maybeSeal();
  // splitTime is the split alone, as V1 counts it with its eviction outside
  // the timer: the seals, spills and pushes timed meanwhile - the rows'
  // seal, the partial spills of chunk growths, maybeSeal()'s seals and a
  // nested reclaim's spills - are sealSpillTime.
  const auto elapsed = static_cast<int64_t>(currentTimeNs() - start);
  const auto sealSpill = static_cast<int64_t>(sealSpillNs_ - sealSpillStart);
  metrics_.splitTime += std::max<int64_t>(0, elapsed - sealSpill);
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::splitNested(RowVectorPtr rv) {
  const uint64_t start = currentTimeNs();
  const uint64_t sealSpillStart = sealSpillNs_;
  // The rows' bound, as for flat batches.
  if (splitter_->hasWindowRows() &&
      splitter_->maxWindowRows() + uint64_t(rv->size()) > kMaxPayloadRows) {
    sealWindow(SealReason::kRows);
  }
  const uint32_t numRows = rv->size();
  if (numRows == 0) {
    return arrow::Status::OK();
  }
  // Every space's positions too: a few bytes may hold any number of them,
  // so only their count bounds a payload. Checked before the split begins,
  // as the rows' are; prepare() fails a batch no payload carries.
  shredder_->prepare(*rv);
  if (!shredder_->fitsWindow()) {
    LOG(INFO) << "CellShuffleWriter seals the window for its nested "
              << "positions: maxWindowRows=" << splitter_->maxWindowRows()
              << ", batchRows=" << numRows;
    sealWindow(SealReason::kRows);
    BOLT_CHECK(shredder_->fitsWindow());
  }

  inSplit_ = true;
  auto status = computePartitions(*rv);
  if (!status.ok()) {
    inSplit_ = false;
    return status;
  }
  if (FOLLY_UNLIKELY(!dictProbed_)) {
    probeDictionary(numRows, shredder_->decoded(), shredder_->nullClass());
    dictProbed_ = true;
  }
  shredder_->split(
      std::span(row2Partition_).first(numRows), partition2RowCount_);
  metrics_.totalInputRowNumber += numRows;
  metrics_.totalInputBatches += 1;
  inSplit_ = false;

  maybeSeal();
  // The shred time is reported as flattenTime, the metric the writers
  // share for reshaping a batch before the split, and the seals, spills
  // and pushes as sealSpillTime, as for flat batches: splitTime keeps
  // neither. The shred time leaves out the sealSpillTime, so the two do
  // not overlap.
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
  // The probe runs once per writer and logs one line: the columns it
  // decided, or why it decided none.
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
  if (numRows < kDictMinProbeRows) {
    skipped(fmt::format(
        "a first batch below {} rows, the probe minimum", kDictMinProbeRows));
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
    if (shredder_ != nullptr && !shredder_->inRowSpace(col)) {
      ++nestedStrings;
      continue;
    }
    if (nullClass[col] == NullClass::kAllNull) {
      ++allNullStrings;
      continue;
    }
    const auto& column = decoded[col];
    const bool hasNulls = nullClass[col] == NullClass::kSomeNulls;
    // Distinct scan with early exit: once past the 64-byte serialization
    // budget the column can never meet the conservative criterion, so a
    // high-cardinality column costs only a handful of rows here.
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

void CellShuffleWriter::reserveChunk() {
  // While the output reads cells nothing may spill, so growth there
  // proceeds unreserved and the pool arbitrates.
  if (!initialized_ || splitter_->isWritingOutput()) {
    return;
  }
  const auto& cellOpts = options_.cellOptions;
  // The threshold counts the spill data in memory too, which a flush gives
  // back.
  const auto overCap = [&]() {
    return cellOpts.cellMemorySpillBytes > 0 &&
        allocator_->allocatedBytes() + kChunkBytes +
            (spillMemory_ != nullptr ? spillMemory_->heldBytes() : 0) >
        cellOpts.cellMemorySpillBytes;
  };
  if (isRemote_) {
    // A remote window is pushed only whole, at a batch boundary: ask for
    // that there and let this growth proceed.
    if (overCap() || !boltPool_->maybeReserve(kChunkBytes)) {
      sealRequested_ = true;
    }
    return;
  }
  if (overCap() || !boltPool_->maybeReserve(kChunkBytes)) {
    // Memory pressure: the spill data in memory goes to disk first, which
    // may make the room.
    SealSpillTimer timer(*this);
    SpillMemory::Pressure pressure(spillMemory_.get());
    if (pressure.freedBytes() > 0 && !overCap() &&
        boltPool_->maybeReserve(kChunkBytes)) {
      ++pressure_.growthFlushes;
      return;
    }
    // Then a partial Run, straight to disk. The cells freed go back to the
    // freelist, which the growth retries first: it makes room without
    // returning a chunk.
    ++pressure_.growthPartialRuns;
    pressure_.growthPartialRunBytes += splitter_->spillPartitions(
        static_cast<uint64_t>(allocator_->usedBytes()) / kPartialSpillDivisor);
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
    splitter_->sealWindow();
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
    releaseWindow(SealReason::kMemory);
    return;
  }
  const auto& cellOpts = options_.cellOptions;
  if (sealRequested_) {
    // A reclaim asked for it: memory pressure. The spill data in memory
    // goes to disk first, and the whole window straight after it.
    SealSpillTimer timer(*this);
    SpillMemory::Pressure pressure(spillMemory_.get());
    sealWindow(SealReason::kMemory);
  } else if (splitter_->nullMovedBytes() > cellOpts.nullMemLimitBytes) {
    // The null bitmaps of every partition: the whole window.
    sealWindow(SealReason::kNulls);
  } else if (
      splitter_->maxPartitionBytes() >
      static_cast<uint64_t>(cellOpts.partitionSealBytes)) {
    // A payload grown past its bound: close that partition's alone, so a
    // skewed partition does not seal - and in local mode spill - every
    // other partition with it.
    SealSpillTimer timer(*this);
    sealing_ = true;
    SCOPE_EXIT {
      sealing_ = false;
    };
    const uint32_t sealed = splitter_->sealPartitionsOver(
        static_cast<uint64_t>(cellOpts.partitionSealBytes));
    pressure_.partitionSeals += sealed > 0;
    pressure_.partitionsSealed += sealed;
  }
}

arrow::Status CellShuffleWriter::reclaimOrdinary(
    int64_t size,
    int64_t* actual) {
  *actual = 0;
  if (!initialized_) {
    // No ordinary batch yet: only the composite blocks may hold spill
    // memory.
    SpillMemory::Pressure pressure(spillMemory_.get());
    *actual = pressure.freedBytes();
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
  // Free memory first, without touching data: chunks that hold no live
  // cell (the freelist is chunk-packed after every spill, so mid-refill
  // the untouched tail chunks are all returnable) and reservation slack.
  // shrink() never frees a chunk with a live cell, so this is safe while
  // the output reads cells - and a window seal, which recycles every cell
  // before writing its null bodies, is exactly when most chunks are idle.
  // The shredder's scratch goes too, unless a batch is being shredded.
  *actual = allocator_->shrink();
  if (shredder_ != nullptr) {
    *actual += shredder_->releaseScratch();
  }
  pressure_.freedIdle += *actual;
  boltPool_->release();
  if (stopped_ || splitter_->isWritingOutput()) {
    // The output is reading the cells: no run may drain them now.
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  if (isRemote_) {
    return reclaimWindow(size, actual);
  }
  // Memory pressure: the spill data in memory goes to disk first, and
  // whatever spills below straight after it. A reclaim reached from inside
  // a split spills for that split: sealSpillTime.
  SealSpillTimer timer(*this);
  SpillMemory::Pressure pressure(spillMemory_.get());
  if (pressure.freedBytes() > 0) {
    pressure_.freedSpillMemory += pressure.freedBytes();
    *actual += pressure.freedBytes();
    boltPool_->release();
    metrics_.totalBytesEvicted = output_->bytesEvicted();
  }
  // A run costs a drain and a compressed segment per partition, so skip it
  // when idle memory and the spill memory already cover the request or the
  // writer holds under two chunks.
  if ((size > 0 && *actual >= size) ||
      allocator_->allocatedBytes() < 2 * kChunkBytes) {
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  // A run must carry real data: under many small reclaims, a run each
  // would write a few bytes per partition and re-grow every tail cell.
  // Below the run floor, a reclaim of a given size returns what it freed
  // above, and the arbitrator turns to other operators for the rest; one
  // that asks for everything writes whatever the cells hold.
  if (size > 0 &&
      splitter_->cellDataBytes() <
          static_cast<uint64_t>(reclaimRunFloorBytes_)) {
    ++pressure_.belowFloorReclaims;
    return arrow::Status::OK();
  }
  // The largest partitions first, as much as the request asks and at least
  // kMinReclaimRunBytes. Their cells are spread over every chunk, so
  // compaction then moves the remaining cells out of the emptiest chunks
  // for shrink() to return. Only when that still falls short does the
  // whole window go.
  if (size > 0) {
    ++pressure_.reclaimPartialRuns;
    pressure_.reclaimPartialRunBytes += splitter_->spillPartitions(
        static_cast<uint64_t>(std::max(size - *actual, kMinReclaimRunBytes)));
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
  // Freed chunks alone are not enough: the reservation built up by the
  // chunk-grow choke point must go back too, or the arbitrator's requester
  // still sees no room (the standard spill-then-release() pattern).
  boltPool_->release();
  // Close the window at the next batch boundary only when the resident
  // null-bitmap state is actually worth returning; requesting a seal on
  // every reclaim turns pressure churn into a seal storm.
  if (splitter_->nullMovedBytes() >
      options_.cellOptions.nullMemLimitBytes / 4) {
    sealRequested_ = true;
  }
  metrics_.totalBytesEvicted = output_->bytesEvicted();
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::reclaimWindow(int64_t size, int64_t* actual) {
  // Only a whole window can be pushed: mid-batch or mid-seal the request
  // is honored at the next batch boundary, and this reclaim returns just
  // the idle memory freed above.
  if (inSplit_ || sealing_) {
    sealRequested_ = true;
    ++pressure_.deferredReclaims;
    return arrow::Status::OK();
  }
  // Same bar as a local run: no push when the idle memory already covers
  // the request or the window holds nothing meaningful.
  if ((size > 0 && *actual >= size) || !splitter_->hasWindowRows() ||
      allocator_->allocatedBytes() < 2 * kChunkBytes) {
    ++pressure_.idleReclaims;
    return arrow::Status::OK();
  }
  const int64_t freed = releaseWindow(SealReason::kMemory);
  pressure_.freedByPush += freed;
  *actual += freed;
  metrics_.totalBytesEvicted = output_->bytesEvicted();
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::flushOrdinary() {
  // Composite rows come next: the window goes out whole and its memory
  // back, so the two kinds never share memory or a payload.
  if (initialized_ && splitter_->hasWindowRows()) {
    releaseWindow(SealReason::kFlush);
  }
  return arrow::Status::OK();
}

arrow::Status CellShuffleWriter::stopOrdinary() {
  BOLT_CHECK(!stopped_, "stop called twice");
  stopped_ = true;
  if (initialized_) {
    try {
      splitter_->finish(metrics_);
    } catch (...) {
      // A failed merge needs the pressure line most: what memory pressure
      // made the writer do.
      logPressure();
      throw;
    }
  } else {
    // No ordinary batch ever came: the block output alone writes the data file
    // - empty, or the composite rows - or ends the Celeborn attempt.
    blocks_->finish(metrics_);
    metrics_.rawPartitionLengths.assign(numPartitions_, 0);
  }
  // Data size is the pre-compression payload volume (what the raw lengths
  // account); bytes written is the compressed file. Reporting them as the
  // same number would hide the compression ratio from the engine metrics.
  metrics_.dataSize = std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      int64_t{0});
  metrics_.peakBytes = boltPool_->peakBytes();
  metrics_.sealSpillTime += static_cast<int64_t>(sealSpillNs_);
  if (initialized_) {
    logPressure();
  }

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
      "compactions={} ({:.1f}MB emptied); full runs={}; seals rows={} "
      "memory={} nulls={} flush={} reserve={}; partition seals={} ({} "
      "partitions); null peak={:.1f}MB",
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
      seal(SealReason::kRows),
      seal(SealReason::kMemory),
      seal(SealReason::kNulls),
      seal(SealReason::kFlush),
      seal(SealReason::kReserve),
      partitionSeals,
      partitionsSealed,
      mb(nullPeakBytes));
}

std::string CellShuffleWriter::toString() const {
  return "CellShuffleWriter";
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
