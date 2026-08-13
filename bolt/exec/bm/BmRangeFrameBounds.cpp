#include "bolt/exec/bm/BmRangeFrameBounds.h"

#include "bolt/exec/bm/BmWindowPartition.h"

#include <algorithm>
#include <optional>

namespace bytedance::bolt::exec::window {
namespace {

constexpr vector_size_t kWindowReadBatchRows = 1024;
constexpr vector_size_t kRangeSearchInitialBatchRows = 128;

} // namespace

template <typename BoundTest>
vector_size_t BmRangeFrameBounds::searchFrameValue(
    const BmWindowPartition& partition,
    const SearchParams<BoundTest>& params,
    vector_size_t start,
    column_index_t orderByPhysicalColumn,
    column_index_t framePhysicalColumn,
    const char* frameRow) {
  const auto order = partition.sortKeyInfo()[0].second;
  const CompareFlags flags{
      .nullsFirst = order.isNullsFirst(),
      .ascending = order.isAscending(),
      .equalsOnly = false};

  auto checkRow =
      [&](const char* orderRow,
          vector_size_t logicalRow) -> std::optional<vector_size_t> {
    const auto compareResult = partition.data_->compare(
        orderRow, frameRow, orderByPhysicalColumn, framePhysicalColumn, flags);

    if (compareResult == 0 && params.firstMatch) {
      return logicalRow;
    }

    if (params.boundTest(compareResult)) {
      return params.firstMatch ? logicalRow : logicalRow - params.step;
    }

    return std::nullopt;
  };

  if (params.step == 1) {
    auto scan = start;
    while (scan < partition.numRows()) {
      const auto batchLimit =
          scan == start ? kRangeSearchInitialBatchRows : kWindowReadBatchRows;
      const auto batchRows =
          std::min<vector_size_t>(batchLimit, partition.numRows() - scan);
      std::vector<const char*> loadedRows;
      const char* const* rows;
      if (partition.ensureAccessMode() ==
          BmWindowPartition::RowAccessMode::kResidentRows) {
        rows = partition.residentRowsData(scan);
      } else {
        loadedRows = partition.loadRows(scan, batchRows);
        rows = loadedRows.data();
      }
      for (auto i = 0; i < batchRows; ++i) {
        if (auto bound = checkRow(rows[i], scan + i)) {
          return *bound;
        }
      }
      scan += batchRows;
    }
  } else {
    auto scanEnd = start + 1;
    while (scanEnd > 0) {
      const auto batchLimit = scanEnd == start + 1
          ? kRangeSearchInitialBatchRows
          : kWindowReadBatchRows;
      const auto scanBegin = scanEnd > batchLimit ? scanEnd - batchLimit : 0;
      const auto batchRows = scanEnd - scanBegin;
      std::vector<const char*> loadedRows;
      const char* const* rows;
      if (partition.ensureAccessMode() ==
          BmWindowPartition::RowAccessMode::kResidentRows) {
        rows = partition.residentRowsData(scanBegin);
      } else {
        loadedRows = partition.loadRows(scanBegin, batchRows);
        rows = loadedRows.data();
      }
      for (int32_t i = batchRows - 1; i >= 0; --i) {
        const auto logicalRow = scanBegin + i;
        if (auto bound = checkRow(rows[i], logicalRow)) {
          return *bound;
        }
      }
      scanEnd = scanBegin;
    }
  }

  return params.step == 1 ? partition.numRows() + 1 : -1;
}

template <typename BoundTest>
void BmRangeFrameBounds::updateKRangeFrameBounds(
    const BmWindowPartition& partition,
    const SearchParams<BoundTest>& params,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBounds,
    vector_size_t* rawFrameBounds) {
  const auto orderByPhysicalColumn = partition.sortKeyInfo()[0].first;
  const auto framePhysicalColumn = partition.inputMapping_[frameColumn];
  std::vector<const char*> loadedFrameRows;
  const char* const* frameRows;
  if (partition.ensureAccessMode() ==
      BmWindowPartition::RowAccessMode::kResidentRows) {
    frameRows = partition.residentRowsData(startRow);
  } else {
    loadedFrameRows = partition.loadRows(startRow, numRows);
    frameRows = loadedFrameRows.data();
  }

  for (auto i = 0; i < numRows; ++i) {
    const auto currentRow = startRow + i;
    if (partition.data_->isNull(frameRows[i], framePhysicalColumn)) {
      rawFrameBounds[i] = rawPeerBounds[i];
    } else {
      rawFrameBounds[i] = searchFrameValue(
          partition,
          params,
          currentRow,
          orderByPhysicalColumn,
          framePhysicalColumn,
          frameRows[i]);
    }
  }
}

void BmRangeFrameBounds::compute(
    const BmWindowPartition& partition,
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBounds,
    vector_size_t* rawFrameBounds) {
  using BoundTest = bool (*)(int);
  if (isPreceding) {
    updateKRangeFrameBounds(
        partition,
        SearchParams<BoundTest>{
            !isStartBound,
            -1,
            [](int compareResult) -> bool { return compareResult < 0; }},
        frameColumn,
        startRow,
        numRows,
        rawPeerBounds,
        rawFrameBounds);
  } else {
    updateKRangeFrameBounds(
        partition,
        SearchParams<BoundTest>{
            isStartBound,
            1,
            [](int compareResult) -> bool { return compareResult > 0; }},
        frameColumn,
        startRow,
        numRows,
        rawPeerBounds,
        rawFrameBounds);
  }
}

} // namespace bytedance::bolt::exec::window
