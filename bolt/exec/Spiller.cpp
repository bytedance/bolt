/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <boost/sort/pdqsort/pdqsort.hpp>
#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include <gfx/timsort.hpp>
#include <cstddef>
#include <cstdint>

#include "bolt/common/base/AsyncSource.h"
#include "bolt/common/base/BitUtil.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/exec/HybridSorter.h"
#include "bolt/exec/Spiller.h"
#include "bolt/type/StringView.h"

#ifdef ENABLE_META_SORT
#include "bolt/exec/meta/MetaRowSorterApi.h"
#endif
#include <gfx/timsort.hpp>

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::exec {
namespace {
#define CHECK_NOT_FINALIZED() \
  BOLT_CHECK(!finalized_, "Spiller has been finalized")

#define CHECK_FINALIZED() \
  BOLT_CHECK(finalized_, "Spiller hasn't been finalized yet");
} // namespace

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const common::SpillConfig* spillConfig)
    : Spiller(
          type,
          container,
          std::move(rowType),
          HashBitRange{},
          numSortingKeys,
          sortCompareFlags,
          spillConfig->spillIOConfig(1),
          std::numeric_limits<uint64_t>::max(),
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->rowBasedSpillMode) {
  setSpillConfig(spillConfig);
  BOLT_CHECK(
      type_ == Type::kOrderByInput || type_ == Type::kAggregateInput,
      "Unexpected spiller type: {}",
      typeName(type_));
  BOLT_CHECK_EQ(state_.maxPartitions(), 1);
  BOLT_CHECK_EQ(state_.targetFileSize(), std::numeric_limits<uint64_t>::max());
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    const common::SpillConfig* spillConfig)
    : Spiller(
          type,
          container,
          std::move(rowType),
          HashBitRange{},
          0,
          {},
          spillConfig->spillIOConfig(1),
          std::numeric_limits<uint64_t>::max(),
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->rowBasedSpillMode) {
  setSpillConfig(spillConfig);
  BOLT_CHECK(
      type_ == Type::kAggregateOutput || type_ == Type::kOrderByOutput,
      "Unexpected spiller type: {}",
      typeName(type_));
  BOLT_CHECK_EQ(state_.maxPartitions(), 1);
  BOLT_CHECK_EQ(state_.targetFileSize(), std::numeric_limits<uint64_t>::max());
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    const common::SpillConfig* spillConfig,
    const uint32_t maxBatchRows)
    : Spiller(
          type,
          container,
          std::move(rowType),
          HashBitRange{},
          0,
          {},
          spillConfig->spillIOConfig(1),
          std::numeric_limits<uint64_t>::max(),
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->rowBasedSpillMode) {
  setSpillConfig(spillConfig);
  BOLT_CHECK(
      type_ == Type::kAggregateOutput || type_ == Type::kOrderByOutput,
      "Unexpected spiller type: {}",
      typeName(type_));
  BOLT_CHECK_EQ(state_.maxPartitions(), 1);
  BOLT_CHECK_EQ(state_.targetFileSize(), std::numeric_limits<uint64_t>::max());
  state_.setMaxBatchRows(maxBatchRows);
}

Spiller::Spiller(
    Type type,
    RowTypePtr rowType,
    HashBitRange bits,
    const common::SpillConfig* spillConfig,
    uint64_t targetFileSize)
    : Spiller(
          type,
          nullptr,
          std::move(rowType),
          bits,
          0,
          {},
          spillConfig->spillIOConfig(bits.numPartitions()),
          targetFileSize,
          spillConfig->executor,
          0,
          spillConfig->rowBasedSpillMode) {
  setSpillConfig(spillConfig);
  BOLT_CHECK_EQ(
      type_,
      Type::kHashJoinProbe,
      "Unexpected spiller type: {}",
      typeName(type_));
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    HashBitRange bits,
    const common::SpillConfig* spillConfig,
    uint64_t targetFileSize,
    bool supportSkewPartition)
    : Spiller(
          type,
          container,
          std::move(rowType),
          bits,
          0,
          {},
          spillConfig->spillIOConfig(bits.numPartitions()),
          targetFileSize,
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->rowBasedSpillMode) {
  setSpillConfig(spillConfig);
  BOLT_CHECK_EQ(
      type_,
      Type::kHashJoinBuild,
      "Unexpected spiller type: {}",
      typeName(type_));
  supportSkewPartition_ = supportSkewPartition;
}

Spiller::Spiller(
    Type type,
    RowTypePtr rowType,
    const common::SpillConfig* spillConfig)
    : Spiller(
          type,
          nullptr,
          std::move(rowType),
          HashBitRange{},
          0,
          {},
          spillConfig->spillIOConfig(1),
          0, /*targetFileSize*/
          spillConfig->executor,
          0,
          common::RowBasedSpillMode::DISABLE) {
  setSpillConfig(spillConfig);
  BOLT_CHECK_EQ(
      type_,
      Type::kHashJoinProbeMatchFlag,
      "Unexpected spiller type: {}",
      typeName(type_));
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    HashBitRange bits,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const common::SpillConfig::SpillIOConfig& ioConfig,
    uint64_t targetFileSize,
    folly::Executor* executor,
    uint64_t maxSpillRunRows,
    common::RowBasedSpillMode rowBasedSpillMode)
    : type_(type),
      container_(container),
      executor_(executor),
      bits_(bits),
      rowType_(std::move(rowType)),
      maxSpillRunRows_(maxSpillRunRows),
      state_(
          ioConfig,
          bits.numPartitions(),
          numSortingKeys,
          sortCompareFlags,
          targetFileSize,
          memory::spillMemoryPool(),
          &stats_),
      spillMode_(Mode::kRowVector),
      rowInfo_(std::nullopt) {
  if (rowBasedSpillMode != common::RowBasedSpillMode::DISABLE) {
    spillMode_ = Mode::kRowContainer;
    rowInfo_ = std::make_optional<RowFormatInfo>(
        container_,
        rowBasedSpillMode == common::RowBasedSpillMode::COMPRESSION);
  }
  TestValue::adjust(
      "bytedance::bolt::exec::Spiller", const_cast<HashBitRange*>(&bits_));

  BOLT_CHECK_EQ(
      container_ == nullptr,
      (type_ == Type::kHashJoinProbe ||
       type_ == Type::kHashJoinProbeMatchFlag));
  spillRuns_.reserve(state_.maxPartitions());
  for (int i = 0; i < state_.maxPartitions(); ++i) {
    spillRuns_.emplace_back(*memory::spillMemoryPool());
  }
  if (type_ == Type::kHashJoinProbeMatchFlag) {
    state_.setImmediateFlush(true);
  }
}

void Spiller::setRowFormatInfo(bool isSerialized) {
  BOLT_CHECK_GT(rowInfo_.has_value(), 0);
  rowInfo_.value().serialized = isSerialized;
}

void Spiller::extractSpill(folly::Range<char**> rows, RowVectorPtr& resultPtr) {
  if (!resultPtr) {
    resultPtr = BaseVector::create<RowVector>(
        rowType_, rows.size(), memory::spillMemoryPool());
  } else {
    resultPtr->prepareForReuse();
    resultPtr->resize(rows.size());
  }
  auto result = resultPtr.get();
  auto& types = container_->columnTypes();
  for (auto i = 0; i < types.size(); ++i) {
    container_->extractColumn(rows.data(), rows.size(), i, result->childAt(i));
  }

  auto& accumulators = container_->accumulators();

  auto numKeys = types.size();
  for (auto i = 0; i < accumulators.size(); ++i) {
    accumulators[i].extractForSpill(rows, result->childAt(i + numKeys));
  }
}

int64_t Spiller::extractSpillVector(
    SpillRows& rows,
    int32_t maxRows,
    int64_t maxBytes,
    RowVectorPtr& spillVector,
    size_t& nextBatchIndex) {
  BOLT_CHECK_NE(type_, Type::kHashJoinProbe);

  auto limit = std::min<size_t>(rows.size() - nextBatchIndex, maxRows);
  BOLT_CHECK(!rows.empty());
  int32_t numRows = 0;
  int64_t bytes = 0;
  uint64_t convertTimeUs{0};
  uint32_t avgRowByte = 0;
  if (container_->hasVariableAccumulator()) {
    // accumulator may has variable data that was not tracked, so use row
    // container's average row byte if it is larger to avoid large row vector
    avgRowByte = container_->usedBytes() / container_->numRows();
  }
  {
    MicrosecondTimer timer(&convertTimeUs);
    for (; numRows < limit; ++numRows) {
      bytes += std::max(
          avgRowByte, container_->rowSize(rows[nextBatchIndex + numRows]));
      if (bytes > maxBytes) {
        // Increment because the row that went over the limit is part
        // of the result. We must spill at least one row.
        ++numRows;
        break;
      }
    }
    extractSpill(folly::Range(&rows[nextBatchIndex], numRows), spillVector);
    nextBatchIndex += numRows;
  }
  updateSpillConvertTime(convertTimeUs);
  return bytes;
}

namespace {
// A stream of ordered rows being read from the in memory
// container. This is the part of a spillable range that is not yet
// spilled when starting to produce output. This is only used for
// sorted spills since for hash join spilling we just use the data in
// the RowContainer as is.
class RowContainerSpillMergeStream : public SpillMergeStream {
 public:
  RowContainerSpillMergeStream(
      int32_t numSortKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      Spiller::SpillRows&& rows,
      Spiller& spiller)
      : numSortKeys_(numSortKeys),
        sortCompareFlags_(sortCompareFlags),
        rows_(std::move(rows)),
        spiller_(spiller) {
    if (!rows_.empty()) {
      nextBatch();
    }
  }

  uint32_t id() const override {
    // Returns the max uint32_t as the special id for in-memory spill merge
    // stream.
    return std::numeric_limits<uint32_t>::max();
  }

 private:
  int32_t numSortKeys() const override {
    return numSortKeys_;
  }

  const std::vector<CompareFlags>& sortCompareFlags() const override {
    return sortCompareFlags_;
  }

  void nextBatch() override {
    // Extracts up to 64 rows at a time. Small batch size because may
    // have wide data and no advantage in large size for narrow data
    // since this is all processed row by row.
    static constexpr vector_size_t kMaxRows = 64;
    constexpr uint64_t kMaxBytes = 1 << 18;
    if (nextBatchIndex_ >= rows_.size()) {
      index_ = 0;
      size_ = 0;
      return;
    }
    spiller_.extractSpillVector(
        rows_, kMaxRows, kMaxBytes, rowVector_, nextBatchIndex_);
    size_ = rowVector_->size();
    index_ = 0;
  }

  const int32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;

  Spiller::SpillRows rows_;
  Spiller& spiller_;
  size_t nextBatchIndex_ = 0;
};
} // namespace

std::unique_ptr<SpillMergeStream> Spiller::spillMergeStreamOverRows(
    int32_t partition) {
  CHECK_FINALIZED();
  BOLT_CHECK_LT(partition, state_.maxPartitions());

  if (!state_.isPartitionSpilled(partition)) {
    return nullptr;
  }
  // Skip the merge stream from row container if it is empty.
  if (spillRuns_[partition].rows.empty()) {
    return nullptr;
  }
  ensureSorted(spillRuns_[partition]);
  return std::make_unique<RowContainerSpillMergeStream>(
      container_->keyTypes().size(),
      state_.sortCompareFlags(),
      std::move(spillRuns_[partition].rows),
      *this);
}

void Spiller::ensureSorted(SpillRun& run) {
  // The spill data of a hash join doesn't need to be sorted.
  if (run.sorted || !needSort()) {
    return;
  }

  uint64_t sortTimeUs{0};
  {
    MicrosecondTimer timer(&sortTimeUs);

    std::vector<CompareFlags> compareFlags;
    bool noFlags = state_.sortCompareFlags().size() == 0;
    if (noFlags) {
      for (auto i = 0; i < container_->keyIndices().size(); ++i)
        compareFlags.emplace_back(CompareFlags());
    }

#ifdef ENABLE_BOLT_JIT
    if (cmp_ == nullptr && spillConfig_ &&
        spillConfig_->getJITenabledForSpill()) {
      if (container_->JITable(container_->keyTypes())) {
        auto [jitMod, rowRowCmpfn] = container_->codegenCompare(
            container_->keyTypes(),
            noFlags ? compareFlags : state_.sortCompareFlags(),
            bytedance::bolt::jit::CmpType::SORT_LESS,
            true);
        jitModule_ = std::move(jitMod);
        cmp_ = (RowRowCompare)jitModule_->getFuncPtr(rowRowCmpfn);
      }
    }
    if (cmp_) {
#if DEBUG_JIT
      sorter_.sort(
          run.rows.begin(),
          run.rows.end(),
          [&](const char* left, const char* right) {
            auto expected = container_->compareRows(
                                left, right, state_.sortCompareFlags()) < 0;
            auto res = cmp_(left, right) > 0;
            bool jitEqual = (int)res > 0; // as cmp_ may return 255 for true
            if ((expected != jitEqual)) {
              std::stringstream ss;
              ss << " spill sort expected: " << (int)expected
                 << " jitEqual: " << (int)jitEqual
                 << " left: " << container_->toString(left)
                 << " right: " << container_->toString(right) << std::endl;
              std::cerr << ss.str() << std::endl;
              BOLT_CHECK(false);
            }
            return expected;
          });
#else
      sorter_.sort(run.rows.begin(), run.rows.end(), cmp_);
#endif
    } else {
#endif

#ifdef ENABLE_META_SORT
      MetaRowsSorterWraper<SpillRows>::MetaCodegenSort(
          run.rows,
          container_,
          sorter_,
          container_->keyIndices(),
          noFlags ? compareFlags : state_.sortCompareFlags());
#else
    sorter_.sort(
        run.rows.begin(),
        run.rows.end(),
        [&](const char* left, const char* right) {
          return container_->compareRows(
                     left, right, state_.sortCompareFlags()) < 0;
        });

#endif

#ifdef ENABLE_BOLT_JIT
    }
#endif

    run.sorted = true;
  }

  // NOTE: Always set a non-zero sort time to avoid flakiness in tests which
  // check sort time.
  updateSpillSortTime(std::max<uint64_t>(1, sortTimeUs));
}

size_t Spiller::setNextEqualForAgg(SpillRun& run) {
  if ((type_ != Type::kAggregateInput && type_ != Type::kAggregateOutput) ||
      run.rows.empty()) {
    return false;
  }
  CHECK(container_->probedFlagOffset() > 0);
  size_t i = 0;
  bool equal = false;
  size_t equalNum = 0;
  for (; i < run.rows.size() - 1; i++) {
    if (cmp_ != nullptr) {
      // the sorted rows[i] <= rows[i+1] results from cmp_ compares rows[i] <
      // rows[i+1], so cmp_ returns false means rows[i] == rows[i+1]
      equal = !cmp_(run.rows[i], run.rows[i + 1]);
#if DEBUG_JIT
      bool jitEqual = (int)equal > 0; // as cmp_ may return 255 for true
      auto expected =
          container_->compareRows(
              run.rows[i], run.rows[i + 1], state_.sortCompareFlags()) == 0;
      if ((expected != jitEqual)) {
        std::stringstream ss;
        ss << " bypass expected: " << (int)expected
           << " jitEqual: " << (int)jitEqual
           << " row[i]: " << container_->toString(run.rows[i])
           << " row[i+1]: " << container_->toString(run.rows[i + 1])
           << std::endl;
        std::cerr << ss.str() << std::endl;
        BOLT_CHECK(false);
      }
#endif
    } else {
      equal = container_->compareRows(
                  run.rows[i], run.rows[i + 1], state_.sortCompareFlags()) == 0;
    }
    if (equal) {
      bits::setBit(run.rows[i], container_->probedFlagOffset(), true);
      equalNum += 1;
    }
#ifdef DEBUG
    bool writeEqual =
        bits::isBitSet(run.rows[i], container_->probedFlagOffset());
    if (equal != writeEqual ||
        (equal && writeEqual && !spillConfig_->needSetNextEqual)) {
      LOG(ERROR) << "i: " << i << " of total rows " << run.rows.size()
                 << ", type: " << typeName(type_)
                 << ", writeEqual: " << writeEqual
                 << ", offset = " << container_->probedFlagOffset()
                 << ", equal: " << equal
                 << ", needSetNextEqual: " << spillConfig_->needSetNextEqual;
      CHECK_EQ(equal, writeEqual);
      CHECK(spillConfig_->needSetNextEqual);
    }
#endif
  }
  // the last is false
  bits::setBit(run.rows[i], container_->probedFlagOffset(), false);
  return equalNum;
}

std::unique_ptr<Spiller::SpillStatus> Spiller::writeSpill(int32_t partition) {
  BOLT_CHECK_NE(type_, Type::kHashJoinProbe);

  // 1. The flush thredhold is controlled by writeBufferSize_ from configuration
  // 2. The materialized size is controlled by kMaxReadBufferSize
  constexpr int32_t kTargetBatchBytes = (1UL << 20) -
      AlignedBuffer::kPaddedSize; // 1M, Same as kMaxReadBufferSize
  constexpr int32_t kTargetBatchRows = 4096;

  RowVectorPtr spillVector;
  auto& run = spillRuns_[partition];
  try {
    ensureSorted(run);
    int64_t totalBytes = 0;
    size_t written = 0;
    if (spillMode_ == Mode::kRowVector) {
      while (written < run.rows.size()) {
        extractSpillVector(
            run.rows,
            kTargetBatchRows,
            kTargetBatchBytes,
            spillVector,
            written);
        totalBytes += state_.appendToPartition(partition, spillVector);
        spillVector->prepareForReuse();
        if (totalBytes > state_.targetFileSize()) {
          BOLT_CHECK(!needSort());
          state_.finishFile(partition);
          // reset to 0 after close file
          totalBytes = 0;
        }
      }
    } else {
      if (spillConfig_->needSetNextEqual) {
        spillConfig_->aggBypassHTEqualNum += setNextEqualForAgg(run);
      }
      written = run.rows.size();
      state_.appendToPartition(partition, run.rows, rowType_, rowInfo_.value());
    }
    return std::make_unique<SpillStatus>(partition, written, nullptr);
  } catch (const std::exception& e) {
    // The exception is passed to the caller thread which checks this in
    // advanceSpill().
    return std::make_unique<SpillStatus>(
        partition, 0, std::current_exception());
  }
}

void Spiller::runSpill(bool lastRun) {
  ++stats_.wlock()->spillRuns;

  std::vector<std::shared_ptr<AsyncSource<SpillStatus>>> writes;
#ifdef DEBUG
  if (type_ == Type::kAggregateInput || type_ == Type::kAggregateOutput) {
    CHECK(spillRuns_.size() == 1);
    CHECK(executor_ == nullptr);
  }
#endif
  for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
    BOLT_CHECK(
        state_.isPartitionSpilled(partition),
        "Partition {} is not marked as spilled",
        partition);
    if (spillRuns_[partition].rows.empty()) {
      continue;
    }
    writes.push_back(std::make_shared<AsyncSource<SpillStatus>>(
        [partition, this]() { return writeSpill(partition); }));
    if (executor_) {
      executor_->add([source = writes.back()]() { source->prepare(); });
    }
  }
  auto sync = folly::makeGuard([&]() {
    for (auto& write : writes) {
      // We consume the result for the pending writes. This is a
      // cleanup in the guard and must not throw. The first error is
      // already captured before this runs.
      try {
        write->move();
      } catch (const std::exception& e) {
      }
    }
  });

  std::vector<std::unique_ptr<SpillStatus>> results;
  results.reserve(writes.size());
  for (auto& write : writes) {
    results.push_back(write->move());
  }
  for (auto& result : results) {
    if (result->error != nullptr) {
      std::rethrow_exception(result->error);
    }
    const auto numWritten = result->rowsWritten;
    auto partition = result->partition;
    auto& run = spillRuns_[partition];
    BOLT_CHECK_EQ(numWritten, run.rows.size());
    run.clear();
    // When a sorted run ends, we start with a new file next time.
    if (needSort()) {
      state_.finishFile(partition);
    }
  }

  // For aggregation output / orderby output spiller, we expect only one spill
  // call to spill all the rows starting from the specified row offset.
  if (lastRun &&
      (type_ == Spiller::Type::kAggregateOutput ||
       type_ == Spiller::Type::kOrderByOutput)) {
    for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
      state_.finishFile(partition);
    }
  }
}

void Spiller::updateSpillFillTime(uint64_t timeUs) {
  stats_.wlock()->spillFillTimeUs += timeUs;
  common::updateGlobalSpillFillTime(timeUs);
}

void Spiller::updateSpillSortTime(uint64_t timeUs) {
  stats_.wlock()->spillSortTimeUs += timeUs;
  common::updateGlobalSpillSortTime(timeUs);
}

void Spiller::updateSpillConvertTime(uint64_t timeUs) {
  stats_.wlock()->spillConvertTimeUs += timeUs;
  common::updateGlobalSpillConvertTime(timeUs);
}

void Spiller::updateSpillTotalTime(uint64_t timeUs) {
  stats_.wlock()->spillTotalTimeUs += timeUs;
  common::updateGlobalSpillTotalTime(timeUs);
}

bool Spiller::needSort() const {
  return type_ != Type::kHashJoinProbe && type_ != Type::kHashJoinBuild &&
      type_ != Type::kAggregateOutput && type_ != Type::kOrderByOutput;
}

void Spiller::spill() {
  return spill(nullptr);
}

void Spiller::spill(const RowContainerIterator& startRowIter) {
  BOLT_CHECK_EQ(type_, Type::kAggregateOutput);
  return spill(&startRowIter);
}

void Spiller::spill(const RowContainerIterator* startRowIter) {
  CHECK_NOT_FINALIZED();
  BOLT_CHECK_NE(type_, Type::kHashJoinProbe);
  BOLT_CHECK_NE(type_, Type::kOrderByOutput);
  const bool doPotentialRangePartitionCheck =
      (supportSkewPartition_ && container_ &&
       container_->numRows() >
           std::max(maxSpillRunRows_, kDefaultSpillRunRows));
  uint64_t totalTimeUs{0};
  {
    MicrosecondTimer timer(&totalTimeUs);
    markAllPartitionsSpilled();

    RowContainerIterator rowIter;
    if (startRowIter != nullptr) {
      rowIter = *startRowIter;
    }

    bool lastRun{false};
    do {
      lastRun = fillSpillRuns(&rowIter);
      runSpill(lastRun);
      if (doPotentialRangePartitionCheck) {
        prepareForRangPartitionIfNeeded();
      }
    } while (!lastRun);

    checkEmptySpillRuns();
  }
  updateSpillTotalTime(totalTimeUs);
}

void Spiller::spill(std::vector<char*>& rows, bool lastRun) {
  CHECK_NOT_FINALIZED();
  BOLT_CHECK(!rows.empty());
  uint64_t totalTimeUs{0};
  {
    MicrosecondTimer timer(&totalTimeUs);
    markAllPartitionsSpilled();

    fillSpillRun(rows);
    runSpill(lastRun);
    checkEmptySpillRuns();
  }
  updateSpillTotalTime(totalTimeUs);
}

void Spiller::checkEmptySpillRuns() const {
  for (int32_t i = 0; i < spillRuns_.size(); ++i) {
    const auto& spillRun = spillRuns_[i];
    BOLT_CHECK(
        spillRun.rows.empty(),
        fmt::format(
            "{} th spillRun.rows is not empty, which size is {}",
            i,
            spillRun.rows.size()));
  }
}

void Spiller::markAllPartitionsSpilled() {
  for (auto partition = 0; partition < state_.maxPartitions(); ++partition) {
    if (!state_.isPartitionSpilled(partition)) {
      state_.setPartitionSpilled(partition);
    }
  }
}

void Spiller::spill(uint32_t partition, const RowVectorPtr& spillVector) {
  CHECK_NOT_FINALIZED();
  BOLT_CHECK(
      type_ == Type::kHashJoinProbe || type_ == Type::kHashJoinBuild ||
          type_ == Type::kHashJoinProbeMatchFlag,
      "Unexpected spiller type: {}",
      typeName(type_));
  if (FOLLY_UNLIKELY(!state_.isPartitionSpilled(partition))) {
    BOLT_FAIL(
        "Can't spill vector to a non-spilling partition: {}, {}",
        partition,
        toString());
  }
  BOLT_DCHECK(spillRuns_[partition].rows.empty());

  if (FOLLY_UNLIKELY(spillVector == nullptr)) {
    return;
  }
  uint64_t totalTimeUs{0};
  {
    MicrosecondTimer timer(&totalTimeUs);
    state_.appendToPartition(partition, spillVector);
  }
  updateSpillTotalTime(totalTimeUs);
  if (supportSkewPartition_ && partition == skewedVictim_ &&
      maxRowsPerFile_ > 0 &&
      state_.rowsInCurrentFile(partition) >= maxRowsPerFile_) {
    state_.finishFile(partition);
  }
}

void Spiller::finishSpill(SpillPartitionSet& partitionSet) {
  finalizeSpill();

  for (auto& partition : state_.spilledPartitionSet()) {
    const SpillPartitionId partitionId(bits_.begin(), partition);
    if (FOLLY_UNLIKELY(partitionSet.count(partitionId) == 0)) {
      partitionSet.emplace(
          partitionId,
          std::make_unique<SpillPartition>(
              partitionId, state_.finish(partition)));
    } else {
      partitionSet[partitionId]->addFiles(state_.finish(partition));
    }
  }

  if (supportSkewPartition_) {
    trySplitSkewPartition(partitionSet);
  }
}

SpillPartition Spiller::finishSpill() {
  BOLT_CHECK_EQ(state_.maxPartitions(), 1);
  BOLT_CHECK(state_.isPartitionSpilled(0));

  finalizeSpill();
  return SpillPartition(SpillPartitionId{bits_.begin(), 0}, state_.finish(0));
}

void Spiller::finalizeSpill() {
  CHECK_NOT_FINALIZED();
  finalized_ = true;
}

bool Spiller::fillSpillRuns(RowContainerIterator* iterator) {
  checkEmptySpillRuns();
  bool lastRun{false};
  uint64_t execTimeUs{0};
  {
    MicrosecondTimer timer(&execTimeUs);

    // Number of rows to hash and divide into spill partitions at a time.
    constexpr int32_t kHashBatchSize = 4096;
    std::vector<uint64_t> hashes(kHashBatchSize);
    std::vector<char*> rows(kHashBatchSize);
    const bool isSinglePartition = bits_.numPartitions() == 1;

    uint64_t totalRows{0};
    for (;;) {
      const auto numRows = container_->listRows(
          iterator, rows.size(), RowContainer::kUnlimited, rows.data());
      if (numRows == 0) {
        lastRun = true;
        break;
      }

      // Calculate hashes for this batch of spill candidates.
      auto rowSet = folly::Range<char**>(rows.data(), numRows);

      if (!isSinglePartition) {
        for (auto i = 0; i < container_->keyTypes().size(); ++i) {
          container_->hash(i, rowSet, i > 0, hashes.data());
        }
      }

      // Put each in its run.
      for (auto i = 0; i < numRows; ++i) {
        // TODO: consider to cache the hash bits in row container so we only
        // need to calculate them once.
        const auto partition = isSinglePartition
            ? 0
            : bits_.partition(hashes[i], state_.maxPartitions());
        BOLT_DCHECK_GE(partition, 0);
        spillRuns_[partition].rows.push_back(rows[i]);
        spillRuns_[partition].numBytes += container_->rowSize(rows[i]);
      }

      totalRows += numRows;
      if (maxSpillRunRows_ > 0 && totalRows >= maxSpillRunRows_) {
        break;
      }
    }
    totalFillRows_.has_value() ? (*totalFillRows_ += totalRows)
                               : (totalFillRows_ = totalRows);
  }
  updateSpillFillTime(execTimeUs);

  return lastRun;
}

void Spiller::fillSpillRun(std::vector<char*>& rows) {
  BOLT_CHECK_EQ(bits_.numPartitions(), 1);
  checkEmptySpillRuns();
  uint64_t execTimeUs{0};
  {
    MicrosecondTimer timer(&execTimeUs);
    spillRuns_[0].rows =
        SpillRows(rows.begin(), rows.end(), spillRuns_[0].rows.get_allocator());
    for (const auto* row : rows) {
      spillRuns_[0].numBytes += container_->rowSize(row);
    }
  }
  updateSpillFillTime(execTimeUs);
}

Spiller& Spiller::setSpillConfig(const common::SpillConfig* cfg) {
  spillConfig_ = const_cast<common::SpillConfig*>(cfg);
  return *this;
}

void Spiller::doSkewPartitionSplit(
    SpillPartitionSet& partitionSet,
    uint8_t bitOffset,
    int32_t partitionNumber,
    int32_t estimatedPartitions) {
  SpillPartitionId key{bitOffset, partitionNumber};
  // make sure partition exists
  BOLT_CHECK_GT(partitionSet.count(key), 0);
  BOLT_CHECK_GT(state_.maxPartitions(), 1);
  auto pNumFiles = partitionSet[key]->numFiles();
  auto splitedSpillPartitions =
      partitionSet[key]->split(std::min(estimatedPartitions, pNumFiles));
  partitionSet.erase(key);

  for (auto i = 0; i < splitedSpillPartitions.size(); ++i) {
    // make sure every range has at least one file
    BOLT_CHECK_GT(splitedSpillPartitions[i]->numFiles(), 0);
    bool isLast = (i == splitedSpillPartitions.size() - 1);
    splitedSpillPartitions[i]->setSubRangePartitionNumber(i, isLast);
    const SpillPartitionId partitionId(bitOffset, partitionNumber, i, isLast);
    partitionSet.emplace(partitionId, std::move(splitedSpillPartitions[i]));
  }
  LOG(INFO) << __FUNCTION__ << ": skew partition " << partitionNumber
            << ", partitionBitOffset = " << (int32_t)bitOffset
            << ", skew partition numFiles = " << pNumFiles
            << ", splitedRangePartition number = "
            << splitedSpillPartitions.size();
}

void Spiller::trySplitSkewPartition(SpillPartitionSet& partitionSet) {
  uint8_t bitOffset = 0;
  uint64_t totalFileSize = 0, avgFileSize = 0;
  uint64_t totalRowCount = 0, avgRowCount = 0;

  for (auto& partitionEntry : partitionSet) {
    totalFileSize += partitionEntry.second->size();
    totalRowCount +=
        state_.spilledRowCount(partitionEntry.first.partitionNumber());
  }

  int32_t expectedPartitions = maxRowsInMemory_
      ? (totalRowCount / maxRowsInMemory_ + 1)
      : state_.maxPartitions();
  avgFileSize = totalFileSize / expectedPartitions;
  avgRowCount = totalRowCount / expectedPartitions;

  if (avgFileSize == 0 || avgRowCount == 0) {
    return;
  }

  LOG(INFO) << __FUNCTION__
            << ": totalFileSize = " << succinctBytes(totalFileSize)
            << ", totalRowCount = " << totalRowCount
            << ", avgFileSize = " << succinctBytes(avgFileSize)
            << ", avgRowCount = " << avgRowCount
            << ", maxRowsInMemory_ = " << maxRowsInMemory_
            << ", expectedPartitions = " << expectedPartitions;
  std::vector<std::tuple<uint8_t, int32_t, uint64_t>> skewedPartitions;

  for (auto& partitionEntry : partitionSet) {
    auto partitionNumber = partitionEntry.first.partitionNumber();
    auto fileSize = partitionEntry.second->size();
    auto rowCount = state_.spilledRowCount(partitionNumber);
    double fileSizeRatio = fileSize * 1.0 / avgFileSize;
    double rowCountRatio = rowCount * 1.0 / avgRowCount;
    bool skewed = false;
    if (fileSizeRatio >= 1.5 || rowCountRatio >= 1.5) {
      rangePartitioningApplicable_ = true;
      skewed = true;
      uint64_t partitionNumber = partitionEntry.first.partitionNumber();
      skewedPartitions.emplace_back(std::make_tuple(
          partitionEntry.first.partitionBitOffset(),
          partitionNumber,
          std::max((uint64_t)fileSizeRatio, (uint64_t)rowCountRatio) + 1));
    }
    LOG(INFO) << __FUNCTION__ << ":" << (skewed ? " " : "not ")
              << "skew partition " << partitionNumber << ", fileSize[Current "
              << fileSize << ", Ratio " << fileSizeRatio
              << "], rowCount[Current " << rowCount << ", Ratio "
              << rowCountRatio << "]";
  }

  for (auto i = 0; i < skewedPartitions.size(); ++i) {
    doSkewPartitionSplit(
        partitionSet,
        std::get<0>(skewedPartitions[i]),
        std::get<1>(skewedPartitions[i]),
        std::get<2>(skewedPartitions[i]));
  }
}

void Spiller::prepareForRangPartitionIfNeeded() {
  if (skewedVictim_ >= 0 && maxRowsPerFile_ >= 0 &&
      state_.rowsInCurrentFile(skewedVictim_) >= maxRowsPerFile_) {
    state_.finishFile(skewedVictim_);
    return;
  }
  const auto& rowCountVec = state_.spilledRowCount();
  uint64_t maxRowCount = std::numeric_limits<uint64_t>::min();
  uint64_t minRowCount = std::numeric_limits<uint64_t>::max();
  int32_t skewedPartitionNumber = -1;

  for (auto i = 0; i < rowCountVec.size(); ++i) {
    auto rowCount = rowCountVec[i];
    if (rowCount > 0) {
      if (rowCountVec[i] > maxRowCount) {
        maxRowCount = rowCount;
        skewedPartitionNumber = i;
      }
      minRowCount = std::min(minRowCount, rowCountVec[i]);
    }
  }
  if (minRowCount != 0 &&
      maxRowCount / minRowCount > skewRowCountRatioThreshold_ &&
      state_.numFinishedFiles(skewedPartitionNumber) == 0) {
    skewedVictim_ = skewedPartitionNumber;
    maxRowsPerFile_ = maxRowCount;
    state_.finishFile(skewedPartitionNumber);
    BOLT_CHECK(state_.numFinishedFiles(skewedPartitionNumber) > 0);
    LOG(INFO) << __FUNCTION__ << ": set maxRowsPerFile_ = " << maxRowsPerFile_
              << ", skewedVictim_ = " << skewedVictim_;
  }
}

std::string Spiller::toString() const {
  return fmt::format(
      "{}\t{}\tMAX_PARTITIONS:{}\tFINALIZED:{}",
      typeName(type_),
      rowType_->toString(),
      state_.maxPartitions(),
      finalized_);
}

// static
std::string Spiller::typeName(Type type) {
  switch (type) {
    case Type::kOrderByInput:
      return "ORDER_BY_INPUT";
    case Type::kOrderByOutput:
      return "ORDER_BY_OUTPUT";
    case Type::kHashJoinBuild:
      return "HASH_JOIN_BUILD";
    case Type::kHashJoinProbe:
      return "HASH_JOIN_PROBE";
    case Type::kAggregateInput:
      return "AGGREGATE_INPUT";
    case Type::kAggregateOutput:
      return "AGGREGATE_OUTPUT";
    case Type::kHashJoinProbeMatchFlag:
      return "kHashJoinProbeMatchFlag";
    default:
      BOLT_UNREACHABLE("Unknown type: {}", static_cast<int>(type));
  }
}

common::SpillStats Spiller::stats() const {
  return stats_.copy();
}
} // namespace bytedance::bolt::exec
