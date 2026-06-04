/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/connectors/hive/bytelake/BytelakeSplitReader.h"
#include "bolt/common/time/Timer.h"
#include "bolt/vector/SelectivityVector.h"

namespace bytedance::bolt::connector::hive {

BytelakeSplitReader::BytelakeSplitReader(
    std::vector<std::unique_ptr<SplitReader>> splitReaders,
    const std::shared_ptr<io::IoStatistics> ioStats,
    std::vector<std::unique_ptr<SplitReader>> keyOnlySplitReaders,
    std::optional<BytelakeKeyOnlySchema> keyOnlySchema)
    : splitReaders_(std::move(splitReaders)),
      keyOnlySplitReaders_(std::move(keyOnlySplitReaders)),
      keyOnlySchema_(std::move(keyOnlySchema)),
      ioStats_(ioStats) {}

std::vector<uint64_t> BytelakeSplitReader::buildBatchBitmap(
    size_t fileIdx,
    uint64_t baseRow,
    uint64_t sliceSize) const {
  const auto& sortedLosers = losersPerFile_[fileIdx];
  auto lo = std::lower_bound(
      sortedLosers.begin(), sortedLosers.end(), static_cast<uint32_t>(baseRow));
  auto hi = std::lower_bound(
      lo, sortedLosers.end(), static_cast<uint32_t>(baseRow + sliceSize));

  if (lo == hi) {
    return {};
  }

  // Bit i = skip i-th row of this batch (file row baseRow + i).
  std::vector<uint64_t> bitmap((sliceSize + 63) / 64, 0);
  for (auto it = lo; it != hi; ++it) {
    const uint32_t localPos = *it - static_cast<uint32_t>(baseRow);
    bitmap[localPos / 64] |= (uint64_t{1} << (localPos % 64));
  }
  return bitmap;
}

uint64_t BytelakeSplitReader::next(int64_t size, VectorPtr& output) {
  // Lazily run Phase 1+2 on first next() — defers the expensive key-only
  // scan until the operator actually starts pulling.
  if (!losersBuilt_) {
    buildLosers();
    losersBuilt_ = true;
  }

  // Phase 3: stream winner rows from full-schema readers, applying a
  // per-batch Mutation bitmap covering this batch's loser positions.
  while (currentFileIdx_ < splitReaders_.size()) {
    const uint64_t fileRowCount = fileRowCounts_[currentFileIdx_];

    if (currentFileRow_ >= fileRowCount) {
      ++currentFileIdx_;
      currentFileRow_ = 0;
      continue;
    }

    // Reader may return fewer rows than sliceSize (e.g. row-group boundary);
    // trailing bits in the bitmap are safely ignored.
    const uint64_t sliceSize =
        std::min<uint64_t>(size, fileRowCount - currentFileRow_);
    const auto bitmap =
        buildBatchBitmap(currentFileIdx_, currentFileRow_, sliceSize);

    dwio::common::Mutation mutation;
    mutation.deletedRows = bitmap.empty() ? nullptr : bitmap.data();

    const uint64_t rowsRead =
        splitReaders_[currentFileIdx_]->next(size, output, &mutation);

    // Fail-safe: reader hit EOF before our tracking expected. Advance
    // rather than spinning.
    if (rowsRead == 0) {
      ++currentFileIdx_;
      currentFileRow_ = 0;
      continue;
    }

    currentFileRow_ += rowsRead;

    auto rowVec = std::dynamic_pointer_cast<RowVector>(output);
    const vector_size_t emitted = rowVec ? rowVec->size() : 0;
    if (emitted > 0) {
      return static_cast<uint64_t>(emitted);
    }
    // Empty batch (all rows were losers): continue same file.
  }

  output = BaseVector::create(output->type(), 0, output->pool());
  return 0;
}

void BytelakeSplitReader::buildLosers() {
  BOLT_CHECK(
      keyOnlySchema_.has_value(),
      "buildLosers() requires keyOnlySchema_ populated by HiveDataSource");
  BOLT_CHECK_EQ(
      keyOnlySplitReaders_.size(),
      splitReaders_.size(),
      "keyOnlySplitReaders_ count must match splitReaders_");

  const auto buildStartTime = getCurrentTimeMicro();
  const size_t numFiles = keyOnlySplitReaders_.size();
  BytelakeHashDedup dedup(keyOnlySchema_.value(), numFiles);
  fileRowCounts_.assign(numFiles, 0);

  for (size_t fileIdx = 0; fileIdx < numFiles; ++fileIdx) {
    auto* reader = keyOnlySplitReaders_[fileIdx].get();
    uint32_t batchStartRow = 0;

    while (true) {
      VectorPtr batch =
          BaseVector::create(keyOnlySchema_->rowType, 0, reader->pool());
      uint64_t rowsRead = reader->next(bytelake::kMAX_BATCH_SIZE, batch);
      if (rowsRead == 0) {
        break;
      }
      auto rowVec = std::static_pointer_cast<RowVector>(batch);
      if (rowVec->size() == 0) {
        continue;
      }

      // Force materialize key columns before reader advances — Bolt's
      // LazyVector becomes invalid once the next reader.next() runs
      // (ColumnLoader version check).
      for (column_index_t i = 0; i < rowVec->childrenSize(); ++i) {
        rowVec->childAt(i)->loadedVector();
      }

      dedup.addBatch(rowVec, static_cast<uint32_t>(fileIdx), batchStartRow);
      batchStartRow += static_cast<uint32_t>(rowVec->size());
    }

    // Phase 1 ScanSpec is filter-free, so total read == physical row count.
    fileRowCounts_[fileIdx] = batchStartRow;
  }

  losersPerFile_ = std::move(dedup).takeLosers();

  // Sort so next() can binary-search the batch range per call.
  for (auto& vec : losersPerFile_) {
    std::sort(vec.begin(), vec.end());
  }

  // Account Phase 1+2 wall time as merge time (parallel to legacy K-way
  // merge timing for monitoring continuity).
  const auto buildTimeNs = (getCurrentTimeMicro() - buildStartTime) * 1000;
  ioStats_->incTotalMergeTime(buildTimeNs);
}

bool BytelakeSplitReader::allPrefetchIssued() const {
  bool result = true;
  for (const auto& splitReader : splitReaders_) {
    result = result && splitReader->allPrefetchIssued();
  }
  return result;
}

bool BytelakeSplitReader::emptySplit() const {
  bool result = true;
  for (const auto& splitReader : splitReaders_) {
    result = result && splitReader->emptySplit();
  }
  return result;
}

void BytelakeSplitReader::resetFilterCaches() {
  for (const auto& splitReader : splitReaders_) {
    splitReader->resetFilterCaches();
  }
}

int64_t BytelakeSplitReader::estimatedRowSize() const {
  return splitReaders_[0]->estimatedRowSize();
}

void BytelakeSplitReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  for (const auto& splitReader : splitReaders_) {
    splitReader->updateRuntimeStats(stats);
  }
  // Include Phase 1 key-only I/O so monitoring sees the real cost.
  for (const auto& splitReader : keyOnlySplitReaders_) {
    splitReader->updateRuntimeStats(stats);
  }
}

void BytelakeSplitReader::resetSplit() {
  for (const auto& splitReader : splitReaders_) {
    splitReader->resetSplit();
  }
  for (const auto& splitReader : keyOnlySplitReaders_) {
    splitReader->resetSplit();
  }
  losersBuilt_ = false;
  losersPerFile_.clear();
  fileRowCounts_.clear();
  currentFileIdx_ = 0;
  currentFileRow_ = 0;
}

} // namespace bytedance::bolt::connector::hive
