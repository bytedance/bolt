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

#include "bolt/exec/WindowPartition.h"
#include "bolt/exec/RowToColumnVector.h"
namespace bytedance::bolt::exec {

WindowPartition::WindowPartition(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    bool enableJit)
    : data_(data),
      partition_(rows),
      inputMapping_(inputMapping),
      sortKeyInfo_(sortKeyInfo),
      enableJit_(enableJit) {
  for (int i = 0; i < inputMapping_.size(); i++) {
    columns_.emplace_back(data_->columnAt(inputMapping_[i]));
  }
}

void WindowPartition::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  RowContainer::extractColumn(
      partition_.data(),
      rowNumbers,
      columns_[columnIndex],
      resultOffset,
      result,
      exactSize);
}

void WindowPartition::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  RowContainer::extractColumn(
      partition_.data() + partitionOffset - offsetInPartition(),
      numRows,
      columns_[columnIndex],
      resultOffset,
      result,
      exactSize);
}

void WindowPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  RowContainer::extractNulls(
      partition_.data() + partitionOffset,
      numRows,
      columns_[columnIndex],
      nullsBuffer);
}

namespace {

std::pair<vector_size_t, vector_size_t> findMinMaxFrameBounds(
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds) {
  auto rawFrameStarts = frameStarts->as<vector_size_t>();
  auto rawFrameEnds = frameEnds->as<vector_size_t>();

  auto firstValidRow = validRows.begin();
  vector_size_t minFrame = rawFrameStarts[firstValidRow];
  vector_size_t maxFrame = rawFrameEnds[firstValidRow];
  validRows.applyToSelected([&](auto i) {
    minFrame = std::min(minFrame, rawFrameStarts[i]);
    maxFrame = std::max(maxFrame, rawFrameEnds[i]);
  });
  return {minFrame, maxFrame};
}

}; // namespace

std::optional<std::pair<vector_size_t, vector_size_t>>
WindowPartition::extractNulls(
    column_index_t col,
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds,
    BufferPtr* nulls) const {
  BOLT_CHECK(validRows.hasSelections(), "Buffer has no active rows");
  auto [minFrame, maxFrame] =
      findMinMaxFrameBounds(validRows, frameStarts, frameEnds);

  // Add 1 since maxFrame is the index of the frame end row.
  auto framesSize = maxFrame - minFrame + 1;
  AlignedBuffer::reallocate<bool>(nulls, framesSize);

  extractNulls(col, minFrame, framesSize, *nulls);
  auto foundNull =
      bits::findFirstBit((*nulls)->as<uint64_t>(), 0, framesSize) >= 0;
  return foundNull ? std::make_optional(std::make_pair(minFrame, framesSize))
                   : std::nullopt;
}

bool WindowPartition::compareRowsWithSortKeys(const char* lhs, const char* rhs)
    const {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : sortKeyInfo_) {
    if (auto result = data_->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return result < 0;
    }
  }
  return false;
}

std::pair<vector_size_t, vector_size_t> WindowPartition::computePeerBuffers(
    vector_size_t start,
    vector_size_t end,
    vector_size_t prevPeerStart,
    vector_size_t prevPeerEnd,
    vector_size_t* rawPeerStarts,
    vector_size_t* rawPeerEnds) const {
  auto peerCompare = [&](const char* lhs, const char* rhs) -> bool {
    return sortKeyInfo_.size() == 0 ? false : compareRowsWithSortKeys(lhs, rhs);
  };

  BOLT_CHECK_LE(end, numRows());

  auto lastPartitionRow = numRows() - 1;
  auto peerStart = prevPeerStart;
  auto peerEnd = prevPeerEnd;
  for (auto i = start, j = 0; i < end; i++, j++) {
    // When traversing input partition rows, the peers are the rows
    // with the same values for the ORDER BY clause. These rows
    // are equal in some ways and affect the results of ranking functions.
    // This logic exploits the fact that all rows between the peerStart
    // and peerEnd have the same values for rawPeerStarts and rawPeerEnds.
    // So we can compute them just once and reuse across the rows in that peer
    // interval. Note: peerStart and peerEnd can be maintained across
    // getOutput calls. Hence, they are returned to the caller.

    if (i == 0 || i >= peerEnd) {
      // Compute peerStart and peerEnd rows for the first row of the partition
      // or when past the previous peerGroup.
      peerStart = i;
      peerEnd = i;
      while (peerEnd <= lastPartitionRow) {
        if (peerCompare(
                partition_[peerStart - offsetInPartition()],
                partition_[peerEnd - offsetInPartition()])) {
          break;
        }
        peerEnd++;
      }
    }

    rawPeerStarts[j] = peerStart;
    rawPeerEnds[j] = peerEnd - 1;
  }
  return {peerStart, peerEnd};
}

// Searches for start[frameColumn] in orderByColumn. Depending on
// preceding or following, this function traverses from start
// to the respective end of partition looking for the frame value.
// The current implementation is a very naive sequential search.
// There are few ideas for future optimizations:
// i)  The current code traverses from start to param.limit
//  a single row at a time. This can be improved to skipping
//  multiple rows.
// ii) Binary search style.
// iii) Use cached value of previous row result to start searching
// from instead of the current start row. Since row values
// show good locality this could give good results.
template <typename BoundTest>
vector_size_t WindowPartition::searchFrameValue(
    const RangeSearchParams<BoundTest>& params,
    vector_size_t start,
    column_index_t orderByColumn,
    column_index_t frameColumn) const {
  auto startRow = partition_[start];
  auto order = sortKeyInfo_[0].second;
  for (vector_size_t i = start; i >= 0 && i < numRows(); i += params.step) {
    auto compareResult = data_->compare(
        partition_[i],
        startRow,
        orderByColumn,
        frameColumn,
        {order.isNullsFirst(), order.isAscending(), false});

    // The bound value was found. Return if firstMatch required.
    // If the last match is required, then we need to find the first row that
    // crosses the bound and return the previous (or following, based on skip)
    // row.
    if (compareResult == 0) {
      if (params.firstMatch) {
        return i;
      }
    }

    // Bound is crossed. Last match needs the previous row.
    // But for first row matches, this is the first
    // row that has crossed, but not equals boundary (The equal boundary case
    // is covered by the condition above). So the bound matches this row itself.
    if (params.boundTest(compareResult)) {
      return params.firstMatch ? i : i - params.step;
    }
  }

  // Return a row beyond the partition boundary. The logic to determine valid
  // frames handles the out of bound and empty frames from this value.
  return params.step == 1 ? numRows() + 1 : -1;
}

template <typename BoundTest>
void WindowPartition::updateKRangeFrameBounds(
    const RangeSearchParams<BoundTest>& params,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBounds,
    vector_size_t* rawFrameBounds) const {
  column_index_t orderByColumn = sortKeyInfo_[0].first;
  RowColumn frameRowColumn = columns_[frameColumn];

  for (auto i = 0; i < numRows; i++) {
    auto currentRow = startRow + i;
    bool frameIsNull = RowContainer::isNullAt(
        partition_[currentRow],
        frameRowColumn.nullByte(),
        frameRowColumn.nullMask());
    // For NULL values, CURRENT ROW semantics apply. So get frame bound from
    // peer buffer.
    if (frameIsNull) {
      rawFrameBounds[i] = rawPeerBounds[i];
    } else {
      // This does a naive search that looks for the frame value from the
      // current row to the partition boundary in a sequential manner. This
      // search can be optimized to start from a previously cached row value
      // instead.
      rawFrameBounds[i] = searchFrameValue(
          params, currentRow, orderByColumn, inputMapping_[frameColumn]);
    }
  }
}

void WindowPartition::computeKRangeFrameBounds(
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBuffer,
    vector_size_t* rawFrameBounds) const {
  typedef bool (*boundTest)(int);

  if (isPreceding) {
    updateKRangeFrameBounds(
        RangeSearchParams<boundTest>(
            {!isStartBound,
             -1,
             [](int compareResult) -> bool { return compareResult < 0; }}),
        frameColumn,
        startRow,
        numRows,
        rawPeerBuffer,
        rawFrameBounds);
  } else {
    updateKRangeFrameBounds(
        RangeSearchParams<boundTest>(
            {isStartBound,
             1,
             [](int compareResult) -> bool { return compareResult > 0; }}),
        frameColumn,
        startRow,
        numRows,
        rawPeerBuffer,
        rawFrameBounds);
  }
}

template <RowFormat T>
WindowPartitionImpl<T>::WindowPartitionImpl(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    bool enableJit)
    : WindowPartition(data, rows, inputMapping, sortKeyInfo, enableJit) {}

template <RowFormat T>
void WindowPartitionImpl<T>::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  if constexpr (T == RowFormat::kRowContainer) {
    RowContainer::extractColumn(
        partition_.data(),
        rowNumbers,
        columns_[columnIndex],
        resultOffset,
        result,
        exactSize);
    return;
  } else if constexpr (T == RowFormat::kSerializedRows) {
    rowToColumnVector(
        partition_.data(),
        rowNumbers,
        columns_[columnIndex],
        resultOffset,
        result);
    return;
  }
  BOLT_FAIL("Unsupported RowFormat!");
}

template <RowFormat T>
void WindowPartitionImpl<T>::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) const {
  if constexpr (T == RowFormat::kRowContainer) {
    RowContainer::extractColumn(
        partition_.data() + partitionOffset - offsetInPartition(),
        numRows,
        columns_[columnIndex],
        resultOffset,
        result,
        exactSize);
    return;

  } else if constexpr (T == RowFormat::kSerializedRows) {
    rowToColumnVector(
        partition_.data() + partitionOffset - offsetInPartition(),
        numRows,
        columns_[columnIndex],
        resultOffset,
        result);
    return;
  }
  BOLT_FAIL("Unsupported RowFormat!");
}

template <RowFormat T>
bool WindowPartitionImpl<T>::compareRowsWithSortKeys(
    const char* lhs,
    const char* rhs) const {
  if constexpr (T == RowFormat::kRowContainer) {
    if (lhs == rhs) {
      return false;
    }
    for (auto& key : sortKeyInfo_) {
      if (auto result = data_->compare(
              lhs,
              rhs,
              key.first,
              {key.second.isNullsFirst(), key.second.isAscending(), false})) {
        return result < 0;
      }
    }
    return false;
  } else if constexpr (T == RowFormat::kSerializedRows) {
    if (lhs == rhs) {
      return false;
    }
#ifdef ENABLE_BOLT_JIT
    if (enableJit_ && rowCmpRowFunc_ == nullptr) {
      std::vector<column_index_t> sortKeyIndexs;
      std::vector<CompareFlags> cmpFlags;
      std::vector<TypePtr> sortKeyTypes;
      for (auto& key : sortKeyInfo_) {
        auto columnIndex = key.first;
        sortKeyIndexs.push_back(columnIndex);
        sortKeyTypes.push_back(data_->columnTypes()[columnIndex]);
        cmpFlags.push_back(CompareFlags{
            key.second.isNullsFirst(), key.second.isAscending(), false});
      }
      if (RowContainer::JITable(sortKeyTypes)) {
        if (cmpFlags.empty()) {
          cmpFlags.resize(sortKeyTypes.size(), CompareFlags());
        }
        auto [jitMod, fn] = data_->codegenCompare(
            sortKeyTypes,
            cmpFlags,
            bytedance::bolt::jit::CmpType::CMP_SPILL,
            true,
            sortKeyIndexs);
        jitModuleRow_ = std::move(jitMod);
        rowCmpRowFunc_ = (RowRowCompare)jitModuleRow_->getFuncPtr(fn);
      }
    }
    if (rowCmpRowFunc_) {
      auto result = rowCmpRowFunc_(lhs, rhs);
      return result < 0;
    }
#endif

    auto types = data_->columnTypes();
    for (auto& key : sortKeyInfo_) {
      auto columnIndex = key.first;
      auto result = BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          compareByRow,
          types[columnIndex]->kind(),
          lhs,
          rhs,
          data_->columnAt(columnIndex),
          data_->columnAt(columnIndex),
          {key.second.isNullsFirst(), key.second.isAscending(), false},
          types[columnIndex].get());
      if (result) {
        return result < 0;
      }
    }
    return false;
  }
  BOLT_FAIL("Unsupported RowFormat!");
} // namespace bytedance::bolt::exec

template <RowFormat T>
void WindowPartitionImpl<T>::computeKRangeFrameBounds(
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBuffer,
    vector_size_t* rawFrameBounds) const {
  typedef bool (*boundTest)(int);
  if constexpr (T == RowFormat::kRowContainer) {
    if (isPreceding) {
      updateKRangeFrameBounds(
          RangeSearchParams<boundTest>(
              {!isStartBound,
               -1,
               [](int compareResult) -> bool { return compareResult < 0; }}),
          frameColumn,
          startRow,
          numRows,
          rawPeerBuffer,
          rawFrameBounds);
    } else {
      updateKRangeFrameBounds(
          RangeSearchParams<boundTest>(
              {isStartBound,
               1,
               [](int compareResult) -> bool { return compareResult > 0; }}),
          frameColumn,
          startRow,
          numRows,
          rawPeerBuffer,
          rawFrameBounds);
    }
    return;
  } else if constexpr (T == RowFormat::kSerializedRows) {
    if (isPreceding) {
      updateKRangeFrameBounds(
          RangeSearchParams<boundTest>(
              {!isStartBound,
               -1,
               sortKeyInfo_[0].second.isAscending()
               ? [](int compareResult) -> bool { return compareResult < 0; }
               : [](int compareResult) -> bool { return compareResult > 0; }}),
          frameColumn,
          startRow,
          numRows,
          rawPeerBuffer,
          rawFrameBounds);
    } else {
      updateKRangeFrameBounds(
          RangeSearchParams<boundTest>(
              {isStartBound,
               1,
               sortKeyInfo_[0].second.isAscending()
               ? [](int compareResult) -> bool { return compareResult > 0; }
               : [](int compareResult) -> bool { return compareResult < 0; }}),
          frameColumn,
          startRow,
          numRows,
          rawPeerBuffer,
          rawFrameBounds);
    }
    return;
  }
  BOLT_FAIL("Unsupported RowFormat!");
}

template <RowFormat T>
template <typename BoundTest>
void WindowPartitionImpl<T>::updateKRangeFrameBounds(
    const RangeSearchParams<BoundTest>& params,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBounds,
    vector_size_t* rawFrameBounds) const {
  column_index_t orderByColumn = sortKeyInfo_[0].first;
  RowColumn frameRowColumn = columns_[frameColumn];

  for (auto i = 0; i < numRows; i++) {
    auto currentRow = startRow + i;
    bool frameIsNull = RowContainer::isNullAt(
        partition_[currentRow],
        frameRowColumn.nullByte(),
        frameRowColumn.nullMask());
    // For NULL values, CURRENT ROW semantics apply. So get frame bound from
    // peer buffer.
    if (frameIsNull) {
      rawFrameBounds[i] = rawPeerBounds[i];
    } else {
      // This does a naive search that looks for the frame value from the
      // current row to the partition boundary in a sequential manner. This
      // search can be optimized to start from a previously cached row value
      // instead.
      rawFrameBounds[i] = searchFrameValue(
          params, currentRow, orderByColumn, inputMapping_[frameColumn]);
    }
  }
}

// Searches for start[frameColumn] in orderByColumn. Depending on
// preceding or following, this function traverses from start
// to the respective end of partition looking for the frame value.
// The current implementation is a very naive sequential search.
// There are few ideas for future optimizations:
// i)  The current code traverses from start to param.limit
//  a single row at a time. This can be improved to skipping
//  multiple rows.
// ii) Binary search style.
// iii) Use cached value of previous row result to start searching
// from instead of the current start row. Since row values
// show good locality this could give good results.
template <RowFormat T>
template <typename BoundTest>
vector_size_t WindowPartitionImpl<T>::searchFrameValue(
    const RangeSearchParams<BoundTest>& params,
    vector_size_t start,
    column_index_t orderByColumn,
    column_index_t frameColumn) const {
  auto startRow = partition_[start];
  auto order = sortKeyInfo_[0].second;
  for (vector_size_t i = start; i >= 0 && i < numRows(); i += params.step) {
    int compareResult = 0;
    auto types = data_->columnTypes();
    if constexpr (T == RowFormat::kRowContainer) {
      compareResult = data_->compare(
          partition_[i],
          startRow,
          orderByColumn,
          frameColumn,
          {order.isNullsFirst(), order.isAscending(), false});
    } else if constexpr (T == RowFormat::kSerializedRows) {
      compareResult = BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          compareByRow,
          types[orderByColumn]->kind(),
          partition_[i],
          startRow,
          columns_[orderByColumn],
          columns_[frameColumn],
          CompareFlags(),
          types[orderByColumn].get());
    }
    // The bound value was found. Return if firstMatch required.
    // If the last match is required, then we need to find the first row that
    // crosses the bound and return the previous (or following, based on skip)
    // row.
    if (compareResult == 0) {
      if (params.firstMatch) {
        return i;
      }
    }

    // Bound is crossed. Last match needs the previous row.
    // But for first row matches, this is the first
    // row that has crossed, but not equals boundary (The equal boundary case
    // is covered by the condition above). So the bound matches this row itself.
    if (params.boundTest(compareResult)) {
      if (params.firstMatch) {
        return i;
      } else {
        return i - params.step;
      }
    }
  }

  // Return a row beyond the partition boundary. The logic to determine valid
  // frames handles the out of bound and empty frames from this value.
  return params.step == 1 ? numRows() + 1 : -1;
}

template <RowFormat R>
SpilledWindowPartition<R>::SpilledWindowPartition(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    std::unique_ptr<UnorderedStreamReader<BatchStream>>&& merge,
    std::vector<std::deque<VectorPtr>>&& aggregateResults,
    const uint64_t numRows,
    const RowTypePtr& outputType,
    const std::vector<column_index_t>* outputChannels,
    bolt::memory::MemoryPool* pool,
    bool enableJit)
    : WindowPartitionImpl<R>(data, rows, inputMapping, sortKeyInfo, enableJit),
      merge_(std::move(merge)),
      aggregateResults_(std::move(aggregateResults)),
      numRows_(numRows),
      outputType_(outputType),
      outputChannels_(outputChannels),
      pool_(pool) {}

template <RowFormat R>
bool SpilledWindowPartition<R>::getBatch(RowVectorPtr& output, bool& isEnd) {
  bool ret = false;
  RowVectorPtr tmpBatch;
  isEnd = false;

  if constexpr (R == RowFormat::kSerializedRows) {
    std::vector<char*> tmpRows;
    output = nullptr;
    ret = merge_->nextBatch(tmpRows);
    output = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, tmpRows.size(), pool_));
    if (LIKELY(ret)) {
      for (auto i = 0; i < outputChannels_->size(); i++) {
        rowToColumnVector(
            tmpRows.data(),
            tmpRows.size(),
            this->columns_[i],
            0,
            output->childAt(i));
      }

      processedRows_ += output->size();
      isEnd = (processedRows_ == numRows_);
    }
  } else {
    ret = merge_->nextBatch(tmpBatch);
    if (LIKELY(ret)) {
      std::vector<VectorPtr> children(tmpBatch->childrenSize());
      for (auto i = 0; i < outputChannels_->size(); ++i) {
        children[(*outputChannels_)[i]] = tmpBatch->childAt(i);
      }
      output = std::make_shared<RowVector>(
          pool_,
          outputType_,
          BufferPtr(nullptr),
          tmpBatch->size(),
          std::move(children));
      processedRows_ += output->size();
      isEnd = (processedRows_ == numRows_);
    }
  }
  return ret;
}

template <RowFormat R>
void SpilledWindowPartition<R>::getWindowFunctionResults(
    VectorPtr& result,
    int32_t numOutputRows,
    int32_t functionIndex,
    bool isAggregateWindowFunc) {
  if (isAggregateWindowFunc) {
    const auto* aggResult = aggregateResults_[functionIndex].back().get();
    for (auto i = 0; i < numOutputRows; ++i) {
      result->copy(aggResult, i, 0, 1);
    }
  } else {
    auto& aggResults = aggregateResults_[functionIndex];
    int32_t target_index = 0;
    while (numOutputRows && (aggResults.size() > 0)) {
      const auto* currentResult = aggResults.front().get();
      auto actualCopySize = std::min(numOutputRows, currentResult->size());
      result->copy(currentResult, target_index, 0, actualCopySize);
      numOutputRows = numOutputRows - actualCopySize;
      target_index = target_index + actualCopySize;
      if (actualCopySize == currentResult->size()) {
        // If we copied all rows from the first aggregate result, we can
        // prepare it for reuse and remove it from the list.
        aggResults.front()->prepareForReuse();
        aggResults.pop_front();
      } else {
        // If we copied only part of the first aggregate result, we slice it
        aggResults.front() = currentResult->slice(
            actualCopySize, currentResult->size() - actualCopySize);
      }
    }
  }
}

template class WindowPartitionImpl<RowFormat::kRowContainer>;
template class WindowPartitionImpl<RowFormat::kSerializedRows>;

template class SpilledWindowPartition<RowFormat::kRowContainer>;
template class SpilledWindowPartition<RowFormat::kSerializedRows>;

} // namespace bytedance::bolt::exec
