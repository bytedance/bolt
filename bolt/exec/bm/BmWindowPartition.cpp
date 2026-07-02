#include "bolt/exec/bm/BmWindowPartition.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/exec/bm/BmRangeFrameBounds.h"

#include <folly/Synchronized.h>

#include <algorithm>
#include <unordered_set>

namespace bytedance::bolt::exec::window {

namespace {

constexpr vector_size_t kExtractNullsBatchRows = 4096;
constexpr vector_size_t kDirectRowNumberRunRows = 8;
constexpr vector_size_t kCachedNullsMinRows = 1024;

folly::Synchronized<BmWindowPartitionTestStats>& testStats() {
  static folly::Synchronized<BmWindowPartitionTestStats> stats;
  return stats;
}

void recordExtractNullBatch(vector_size_t numRows) {
  testStats().withWLock([&](auto& stats) {
    ++stats.extractNullCalls;
    stats.extractNullRows += numRows;
    stats.maxExtractNullBatchRows =
        std::max<uint64_t>(stats.maxExtractNullBatchRows, numRows);
  });
}

void recordLoadedRows(vector_size_t numRows) {
  testStats().withWLock([&](auto& stats) {
    ++stats.loadRowsCalls;
    stats.loadedRows += numRows;
    stats.maxLoadedRows = std::max<uint64_t>(stats.maxLoadedRows, numRows);
  });
}

void recordReclaimReadChunks() {
  testStats().withWLock([&](auto& stats) { ++stats.reclaimReadChunksCalls; });
}

void recordRowNumberResidentExtract(vector_size_t numRows) {
  testStats().withWLock([&](auto& stats) {
    ++stats.rowNumberResidentExtractCalls;
    stats.rowNumberResidentExtractRows += numRows;
  });
}

std::vector<exec::bm::SegmentId> collectSegmentIds(
    const std::vector<exec::bm::SegmentRowRange>& ranges) {
  std::vector<exec::bm::SegmentId> segments;
  segments.reserve(ranges.size());
  if (ranges.size() > 8) {
    std::unordered_set<exec::bm::SegmentId> seen;
    seen.reserve(ranges.size());
    for (const auto& range : ranges) {
      if (range.count == 0) {
        continue;
      }
      if (seen.insert(range.segment).second) {
        segments.push_back(range.segment);
      }
    }
    return segments;
  }

  for (const auto& range : ranges) {
    if (range.count == 0) {
      continue;
    }
    if (std::find(segments.begin(), segments.end(), range.segment) ==
        segments.end()) {
      segments.push_back(range.segment);
    }
  }
  return segments;
}

} // namespace

void resetBmWindowPartitionTestStats() {
  testStats().withWLock(
      [](auto& stats) { stats = BmWindowPartitionTestStats{}; });
}

BmWindowPartitionTestStats bmWindowPartitionTestStats() {
  return testStats().copy();
}

BmWindowPartition::BmWindowPartition(
    exec::bm::BmRowContainer* data,
    memory::MemoryPool* pool,
    std::vector<TypePtr> logicalTypes,
    BmWindowPartitionDescriptor descriptor,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo)
    : WindowPartition(inputMapping, sortKeyInfo),
      data_(data),
      pool_(pool),
      logicalTypes_(std::move(logicalTypes)),
      residentRows_(std::move(descriptor.residentRows)),
      rowRanges_(std::move(descriptor.ranges)),
      segmentIds_(
          residentRows_.empty() ? collectSegmentIds(rowRanges_)
                                : std::vector<exec::bm::SegmentId>{}),
      peerStartBits_(std::move(descriptor.peerStartBits)),
      numRows_(descriptor.numRows),
      peerBoundaryMode_(descriptor.peerBoundaryMode) {
  BOLT_CHECK_NOT_NULL(data_);
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK_EQ(logicalTypes_.size(), inputMapping_.size());
  if (!residentRows_.empty()) {
    BOLT_CHECK_EQ(
        residentRows_.size(),
        numRows_,
        "Resident row cache size {} must match partition row count {}",
        residentRows_.size(),
        numRows_);
  }
  rowAccessMode_ = residentRows_.empty() ? RowAccessMode::kUndecided
                                         : RowAccessMode::kResidentRows;
  uint64_t rangeRows = 0;
  for (const auto& range : rowRanges_) {
    rangeRows += range.count;
  }
  BOLT_CHECK_EQ(
      rangeRows,
      numRows_,
      "Range row count {} must match partition row count {}",
      rangeRows,
      numRows_);
  initializePhysicalTypes();
}

bool BmWindowPartition::canBulkReadPartition() const {
  if (rowRanges_.empty() || segmentIds_.empty()) {
    return false;
  }
  return data_->canBulkRead({segmentIds_.data(), segmentIds_.size()});
}

BmWindowPartition::RowAccessMode BmWindowPartition::ensureAccessMode() const {
  if (rowAccessMode_ != RowAccessMode::kUndecided) {
    return rowAccessMode_;
  }

  if (!canBulkReadPartition()) {
    readSession_.emplace(data_->beginReadOnlyWindowReadSegments(
        {segmentIds_.data(), segmentIds_.size()}));
    rowAccessMode_ = RowAccessMode::kWindowRead;
    return rowAccessMode_;
  }

  bulkSession_.emplace(
      data_->beginBulkReadSegments({segmentIds_.data(), segmentIds_.size()}));
  bulkSession_->load();
  rowAccessMode_ = RowAccessMode::kBulkRead;
  return rowAccessMode_;
}

vector_size_t BmWindowPartition::numRows() const {
  return numRows_;
}

std::vector<const char*> BmWindowPartition::loadRows(
    vector_size_t partitionOffset,
    vector_size_t numRows) const {
  BOLT_CHECK_LE(partitionOffset + numRows, this->numRows());
  if (numRows == 0) {
    return {};
  }

  switch (ensureAccessMode()) {
    case RowAccessMode::kResidentRows:
      return loadResidentRows(partitionOffset, numRows);
    case RowAccessMode::kBulkRead: {
      BOLT_CHECK(rowRanges_.empty() == false);
      BOLT_CHECK(bulkSession_.has_value());
      auto ranges = getSegmentRanges(partitionOffset, numRows);
      return bulkSession_->loadRows({ranges.data(), ranges.size()});
    }
    case RowAccessMode::kWindowRead:
      BOLT_CHECK(readSession_.has_value());
      recordLoadedRows(numRows);
      BOLT_CHECK(!rowRanges_.empty());
      {
        auto ranges = getSegmentRanges(partitionOffset, numRows);
        return readSession_->loadRows({ranges.data(), ranges.size()});
      }
    case RowAccessMode::kUndecided:
      BOLT_UNREACHABLE();
  }
  BOLT_UNREACHABLE();
}

std::vector<const char*> BmWindowPartition::loadResidentRows(
    vector_size_t partitionOffset,
    vector_size_t numRows) const {
  std::vector<const char*> rows;
  rows.reserve(numRows);
  for (auto i = 0; i < numRows; ++i) {
    rows.push_back(residentRows_[partitionOffset + i]);
  }
  return rows;
}

const char* const* BmWindowPartition::residentRowsData(
    vector_size_t partitionOffset) const {
  BOLT_CHECK_LE(partitionOffset, numRows());
  return reinterpret_cast<const char* const*>(
      residentRows_.data() + partitionOffset);
}

std::vector<exec::bm::SegmentRowRange> BmWindowPartition::getSegmentRanges(
    vector_size_t partitionOffset,
    vector_size_t numRows) const {
  BOLT_CHECK_LE(partitionOffset + numRows, this->numRows());
  std::vector<exec::bm::SegmentRowRange> ranges;
  if (numRows == 0) {
    return ranges;
  }

  const auto requestedBegin = partitionOffset;
  const auto requestedEnd = partitionOffset + numRows;
  vector_size_t partitionBegin = 0;
  for (const auto& range : rowRanges_) {
    const auto partitionEnd =
        partitionBegin + static_cast<vector_size_t>(range.count);
    if (partitionEnd <= requestedBegin) {
      partitionBegin = partitionEnd;
      continue;
    }
    if (partitionBegin >= requestedEnd) {
      break;
    }

    const auto overlapBegin = std::max(partitionBegin, requestedBegin);
    const auto overlapEnd = std::min(partitionEnd, requestedEnd);
    const auto localOffset = overlapBegin - partitionBegin;
    exec::bm::SegmentRowRange selected;
    selected.segment = range.segment;
    selected.begin =
        static_cast<exec::bm::RowNumber>(range.begin + localOffset);
    selected.count =
        static_cast<exec::bm::RowNumber>(overlapEnd - overlapBegin);
    ranges.push_back(selected);
    partitionBegin = partitionEnd;
  }
  uint64_t selectedRows = 0;
  for (const auto& range : ranges) {
    selectedRows += range.count;
  }
  BOLT_CHECK_EQ(selectedRows, numRows);
  return ranges;
}

uint64_t BmWindowPartition::reclaimReadChunks(uint64_t targetBytes) const {
  if (rowAccessMode_ == RowAccessMode::kWindowRead &&
      readSession_.has_value()) {
    recordReclaimReadChunks();
    return readSession_->evictLoadedChunks(targetBytes);
  }
  return 0;
}

VectorPtr BmWindowPartition::extractColumnFromRows(
    const std::vector<const char*>& rows,
    int32_t physicalColumn) const {
  auto result =
      BaseVector::create(physicalTypes_[physicalColumn], rows.size(), pool_);
  if (!rows.empty()) {
    data_->extractColumnResident(
        rows.data(), rows.size(), physicalColumn, result);
  }
  return result;
}

vector_size_t BmWindowPartition::peerStartAtOrBeforeFromMetadata(
    vector_size_t row) const {
  BOLT_CHECK_LT(row, numRows());
  if (row == 0 || peerStartBits_.empty()) {
    return 0;
  }

  const auto end = std::min<vector_size_t>(
      row + 1, static_cast<vector_size_t>(peerStartBits_.size() * 64));
  if (end <= 1) {
    return 0;
  }
  const auto found =
      bits::findLastBit(peerStartBits_.data(), 1, static_cast<int32_t>(end));
  return found < 0 ? 0 : static_cast<vector_size_t>(found);
}

vector_size_t BmWindowPartition::nextPeerStartFromMetadata(
    vector_size_t row) const {
  BOLT_CHECK_LT(row, numRows());
  if (peerStartBits_.empty() || row + 1 >= numRows()) {
    return numRows();
  }

  const auto begin = row + 1;
  const auto end = std::min<vector_size_t>(
      numRows(), static_cast<vector_size_t>(peerStartBits_.size() * 64));
  if (begin >= end) {
    return numRows();
  }
  const auto found = bits::findFirstBit(
      peerStartBits_.data(),
      static_cast<int32_t>(begin),
      static_cast<int32_t>(end));
  return found < 0 ? numRows() : static_cast<vector_size_t>(found);
}

void BmWindowPartition::extractRowsToVector(
    const std::vector<const char*>& rows,
    int32_t physicalColumn,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  extractRowsToVector(
      rows.data(),
      rows.size(),
      physicalColumn,
      resultOffset,
      result,
      exactSize);
}

void BmWindowPartition::extractRowsToVector(
    const char* const* rows,
    vector_size_t numRows,
    int32_t physicalColumn,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  if (numRows == 0) {
    result->resize(resultOffset);
    return;
  }

  data_->extractColumnResident(
      rows, numRows, physicalColumn, resultOffset, result, exactSize);
}

void BmWindowPartition::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  if (ensureAccessMode() == RowAccessMode::kResidentRows) {
    extractRowsToVector(
        residentRowsData(partitionOffset),
        numRows,
        inputMapping_[columnIndex],
        resultOffset,
        result,
        exactSize);
    return;
  }

  auto rows = loadRows(partitionOffset, numRows);
  extractRowsToVector(
      rows, inputMapping_[columnIndex], resultOffset, result, exactSize);
}

void BmWindowPartition::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  const auto resultSize = resultOffset + rowNumbers.size();
  result->resize(resultSize);

  std::vector<const char*> selectedRows;
  std::vector<vector_size_t> selectedPositions;
  selectedRows.reserve(rowNumbers.size());
  selectedPositions.reserve(rowNumbers.size());

  std::vector<exec::bm::SegmentRowRange> selectedRanges;
  const auto mode = ensureAccessMode();
  if (mode == RowAccessMode::kResidentRows) {
    recordRowNumberResidentExtract(rowNumbers.size());
    data_->extractColumnResident(
        residentRowsData(0),
        rowNumbers,
        inputMapping_[columnIndex],
        resultOffset,
        result,
        exactSize);
    return;
  }

  if (mode != RowAccessMode::kResidentRows) {
    BOLT_CHECK(!rowRanges_.empty());
    selectedRanges.reserve(rowNumbers.size());
  }

  VectorPtr repeatedValue;
  auto copyRepeatedRow = [&](vector_size_t position,
                             vector_size_t rowNumber,
                             vector_size_t runRows) {
    if (!repeatedValue) {
      repeatedValue = BaseVector::create(logicalTypes_[columnIndex], 1, pool_);
    } else {
      BaseVector::prepareForReuse(repeatedValue, 1);
    }
    extractColumn(columnIndex, rowNumber, 1, 0, repeatedValue, false);
    for (auto offset = 0; offset < runRows; ++offset) {
      result->copy(repeatedValue.get(), resultOffset + position + offset, 0, 1);
    }
  };

  auto addFallbackRow = [&](vector_size_t position, vector_size_t rowNumber) {
    selectedPositions.push_back(position);
    if (mode == RowAccessMode::kResidentRows) {
      selectedRows.push_back(residentRows_[rowNumber]);
    } else {
      auto ranges = getSegmentRanges(rowNumber, 1);
      selectedRanges.insert(selectedRanges.end(), ranges.begin(), ranges.end());
    }
  };

  for (auto i = 0; i < rowNumbers.size();) {
    const auto rowNumber = rowNumbers[i];
    if (rowNumber < 0) {
      result->setNull(resultOffset + i, true);
      ++i;
      continue;
    }
    BOLT_CHECK_LT(rowNumber, this->numRows());

    vector_size_t repeatRows = 1;
    while (i + repeatRows < rowNumbers.size() &&
           rowNumbers[i + repeatRows] == rowNumber) {
      ++repeatRows;
    }
    if (repeatRows >= kDirectRowNumberRunRows) {
      copyRepeatedRow(i, rowNumber, repeatRows);
      i += repeatRows;
      continue;
    }

    vector_size_t runRows = 1;
    while (i + runRows < rowNumbers.size()) {
      const auto nextRowNumber = rowNumbers[i + runRows];
      if (nextRowNumber < 0 || nextRowNumber != rowNumber + runRows) {
        break;
      }
      BOLT_CHECK_LT(nextRowNumber, this->numRows());
      ++runRows;
    }

    if (runRows >= kDirectRowNumberRunRows) {
      extractColumn(
          columnIndex, rowNumber, runRows, resultOffset + i, result, false);
      result->resize(resultSize, false);
    } else {
      for (auto offset = 0; offset < runRows; ++offset) {
        addFallbackRow(i + offset, rowNumber + offset);
      }
    }
    i += runRows;
  }

  if (mode == RowAccessMode::kBulkRead && !selectedRanges.empty()) {
    BOLT_CHECK(bulkSession_.has_value());
    selectedRows =
        bulkSession_->loadRows({selectedRanges.data(), selectedRanges.size()});
  } else if (mode == RowAccessMode::kWindowRead && !selectedRanges.empty()) {
    BOLT_CHECK(readSession_.has_value());
    recordLoadedRows(selectedRanges.size());
    selectedRows =
        readSession_->loadRows({selectedRanges.data(), selectedRanges.size()});
  }

  if (!selectedRows.empty()) {
    auto temp = BaseVector::create(
        logicalTypes_[columnIndex], selectedRows.size(), result->pool());
    data_->extractColumnResident(
        selectedRows.data(),
        selectedRows.size(),
        inputMapping_[columnIndex],
        temp,
        exactSize);
    for (auto i = 0; i < selectedPositions.size(); ++i) {
      result->copy(temp.get(), resultOffset + selectedPositions[i], i, 1);
    }
  }
  result->resize(resultSize, false);
}

bool BmWindowPartition::copyCachedNulls(
    int32_t physicalColumn,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  if (physicalColumn >= nullsCache_.size()) {
    return false;
  }
  const auto& cache = nullsCache_[physicalColumn];
  if (!cache.valid || cache.partitionOffset != partitionOffset ||
      cache.numRows != numRows) {
    return false;
  }

  bits::copyBits(
      cache.nulls->as<uint64_t>(),
      0,
      nullsBuffer->asMutable<uint64_t>(),
      0,
      numRows);
  return true;
}

void BmWindowPartition::cacheNulls(
    int32_t physicalColumn,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  if (numRows < kCachedNullsMinRows || physicalColumn >= physicalTypes_.size()) {
    return;
  }
  if (nullsCache_.empty()) {
    nullsCache_.resize(physicalTypes_.size());
  }

  auto& cache = nullsCache_[physicalColumn];
  cache.valid = true;
  cache.partitionOffset = partitionOffset;
  cache.numRows = numRows;
  cache.nulls = AlignedBuffer::allocate<bool>(numRows, pool_);
  bits::copyBits(
      nullsBuffer->as<uint64_t>(),
      0,
      cache.nulls->asMutable<uint64_t>(),
      0,
      numRows);
}

void BmWindowPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  BOLT_DCHECK(nullsBuffer->size() >= bits::nbytes(numRows));
  const auto physicalColumn = inputMapping_[columnIndex];
  if (copyCachedNulls(physicalColumn, partitionOffset, numRows, nullsBuffer)) {
    return;
  }

  auto* rawNulls = nullsBuffer->asMutable<uint64_t>();
  bits::fillBits(rawNulls, 0, numRows, false);
  if (numRows == 0) {
    return;
  }

  if (ensureAccessMode() == RowAccessMode::kResidentRows) {
    BufferPtr batchNulls;
    vector_size_t offset = 0;
    while (offset < numRows) {
      const auto batchRows =
          std::min<vector_size_t>(kExtractNullsBatchRows, numRows - offset);
      recordExtractNullBatch(batchRows);

      if (offset == 0 && batchRows == numRows) {
        data_->extractNullsResident(
            residentRowsData(partitionOffset),
            batchRows,
            physicalColumn,
            nullsBuffer);
      } else {
        if (!batchNulls) {
          batchNulls =
              AlignedBuffer::allocate<bool>(kExtractNullsBatchRows, pool_);
        }
        data_->extractNullsResident(
            residentRowsData(partitionOffset + offset),
            batchRows,
            physicalColumn,
            batchNulls);
        bits::copyBits(
            batchNulls->as<uint64_t>(), 0, rawNulls, offset, batchRows);
      }

      offset += batchRows;
    }
    cacheNulls(physicalColumn, partitionOffset, numRows, nullsBuffer);
    return;
  }

  BufferPtr batchNulls;
  vector_size_t offset = 0;
  while (offset < numRows) {
    const auto batchRows =
        std::min<vector_size_t>(kExtractNullsBatchRows, numRows - offset);
    auto rows = loadRows(partitionOffset + offset, batchRows);
    recordExtractNullBatch(batchRows);

    if (offset == 0 && batchRows == numRows) {
      data_->extractNullsResident(
          rows.data(), rows.size(), physicalColumn, nullsBuffer);
    } else {
      if (!batchNulls) {
        batchNulls =
            AlignedBuffer::allocate<bool>(kExtractNullsBatchRows, pool_);
      }
      data_->extractNullsResident(
          rows.data(), rows.size(), physicalColumn, batchNulls);
      bits::copyBits(
          batchNulls->as<uint64_t>(), 0, rawNulls, offset, batchRows);
    }

    offset += batchRows;
  }
  cacheNulls(physicalColumn, partitionOffset, numRows, nullsBuffer);
}

void BmWindowPartition::extractColumns(
    folly::Range<const column_index_t*> columnIndices,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    folly::Range<const VectorPtr*> results,
    bool exactSize) const {
  BOLT_CHECK_EQ(columnIndices.size(), results.size());
  if (ensureAccessMode() == RowAccessMode::kResidentRows) {
    const auto* rows = residentRowsData(partitionOffset);
    for (auto i = 0; i < columnIndices.size(); ++i) {
      extractRowsToVector(
          rows,
          numRows,
          inputMapping_[columnIndices[i]],
          0,
          results[i],
          exactSize);
    }
    return;
  }

  auto rows = loadRows(partitionOffset, numRows);
  for (auto i = 0; i < columnIndices.size(); ++i) {
    extractRowsToVector(
        rows, inputMapping_[columnIndices[i]], 0, results[i], exactSize);
  }
}

std::pair<vector_size_t, vector_size_t> BmWindowPartition::computePeerBuffers(
    vector_size_t start,
    vector_size_t end,
    vector_size_t prevPeerStart,
    vector_size_t prevPeerEnd,
    vector_size_t* rawPeerStarts,
    vector_size_t* rawPeerEnds,
    bool /*enableJit*/) const {
  BOLT_CHECK_LE(start, end);
  BOLT_CHECK_LE(end, numRows());
  if (start == end) {
    return {prevPeerStart, prevPeerEnd};
  }

  if (sortKeyInfo_.empty()) {
    const auto lastPartitionRow = numRows() - 1;
    for (auto i = start, j = 0; i < end; ++i, ++j) {
      rawPeerStarts[j] = 0;
      rawPeerEnds[j] = lastPartitionRow;
    }
    return {0, numRows()};
  }

  if (peerBoundaryMode_ == BmPeerBoundaryMode::kNotNeeded) {
    for (auto i = start, j = 0; i < end; ++i, ++j) {
      rawPeerStarts[j] = i;
      rawPeerEnds[j] = i;
    }
    return {end - 1, end};
  }

  BOLT_CHECK(
      peerBoundaryMode_ == BmPeerBoundaryMode::kPrecomputed,
      "Unexpected BM peer boundary mode");
  auto peerStart = prevPeerStart;
  auto peerEnd = prevPeerEnd;
  for (auto i = start, j = 0; i < end; ++i, ++j) {
    if (i == 0 || i < peerStart || i >= peerEnd) {
      peerStart = peerStartAtOrBeforeFromMetadata(i);
      peerEnd = nextPeerStartFromMetadata(peerStart);
    }
    rawPeerStarts[j] = peerStart;
    rawPeerEnds[j] = peerEnd - 1;
  }
  return {peerStart, peerEnd};
}

void BmWindowPartition::computeKRangeFrameBounds(
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerStarts,
    vector_size_t* rawFrameBounds) const {
  BmRangeFrameBounds::compute(
      *this,
      isStartBound,
      isPreceding,
      frameColumn,
      startRow,
      numRows,
      rawPeerStarts,
      rawFrameBounds);
}

} // namespace bytedance::bolt::exec::window
