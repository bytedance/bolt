/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include <bolt/common/base/SuccinctPrinter.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include "bolt/row/CompactRow.h"
#include "bolt/row/dense/DenseRow.h"
using namespace bytedance;
namespace bytedance::bolt::shuffle::sparksql {

void ShuffleColumnarToRowConverter::init(
    const bytedance::bolt::RowTypePtr& rowType) {
  if (rowFormat_ == row::RowFormat::COMPACT) {
    if (auto fixedRowSize = bolt::row::CompactRow::fixedRowSize(rowType)) {
      fixedRowSize_ = fixedRowSize.value();
    }
  }
}
ShuffleColumnarToRowConverter::RowVectorWithStats
ShuffleColumnarToRowConverter::getWithStats(
    const bytedance::bolt::RowVectorPtr& rowVector,
    int64_t maxBatchSize) {
  BOLT_CHECK_GT(maxBatchSize, 0);
  RowVectorWithStats stats;
  stats.rowVectorHolder_ = rowVector;
  stats.numRows = rowVector->size();
  auto numRows = rowVector->size();

  if (rowFormat_ == row::RowFormat::COMPACT) {
    stats.compactRow = std::make_shared<bolt::row::CompactRow>(rowVector);
    if (fixedRowSize_) {
      stats.rowSizes_.assign(numRows, fixedRowSize_);
    } else {
      for (auto i = 0; i < numRows; ++i) {
        stats.rowSizes_.push_back(stats.compactRow->rowSize(i));
      }
    }
  } else {
    stats.denseRow = std::make_shared<row::DenseRow>(rowVector);
    const auto& denseRowSizes = stats.denseRow->rowSizes();
    stats.rowSizes_.assign(denseRowSizes.begin(), denseRowSizes.end());
  }

  vector_size_t rangeOffset = 0;
  vector_size_t rangeRows = 0;
  int64_t rangeBytes = 0;
  auto closeRange = [&]() {
    if (rangeRows > 0) {
      stats.ranges_.push_back({rangeOffset, rangeRows, rangeBytes});
      rangeOffset += rangeRows;
      rangeRows = 0;
      rangeBytes = 0;
    }
  };
  if (fixedRowSize_ && rowFormat_ == row::RowFormat::COMPACT) {
    const auto rowBytes =
        static_cast<int64_t>(fixedRowSize_) + kSizeOfRowHeader;
    stats.totalMemorySize = rowBytes * numRows;
    const auto rowsPerRange =
        std::max<vector_size_t>(1, maxBatchSize / rowBytes);
    for (vector_size_t offset = 0; offset < numRows; offset += rowsPerRange) {
      const auto length = std::min(rowsPerRange, numRows - offset);
      stats.ranges_.push_back({offset, length, rowBytes * length});
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      const auto rowSize = stats.rowSizes_[i];
      const auto rowBytes = static_cast<int64_t>(rowSize) + kSizeOfRowHeader;
      if (rangeRows > 0 && rangeBytes + rowBytes > maxBatchSize) {
        closeRange();
      }
      stats.totalMemorySize += rowBytes;
      rangeBytes += rowBytes;
      ++rangeRows;
    }
  }
  closeRange();
  if (stats.ranges_.size() > 1 &&
      stats.ranges_.back().bytes < maxBatchSize * 0.8) {
    auto tail = stats.ranges_.back();
    stats.ranges_.pop_back();
    auto& previous = stats.ranges_.back();
    previous.length += tail.length;
    previous.bytes += tail.bytes;
  }
  return stats;
}

ShuffleColumnarToRowConverter::RowVectorWithStats
ShuffleColumnarToRowConverter::sliceStats(
    const RowVectorWithStats& stats,
    const RowVectorWithStats::Range& range) {
  RowVectorWithStats sliced;
  sliced.rowVectorHolder_ = stats.rowVectorHolder_;
  sliced.compactRow = stats.compactRow;
  sliced.denseRow = stats.denseRow;
  sliced.rowOffset = stats.rowOffset + range.offset;
  sliced.numRows = range.length;
  sliced.totalMemorySize = range.bytes;
  sliced.rowSizes_.assign(
      stats.rowSizes_.begin() + range.offset,
      stats.rowSizes_.begin() + range.offset + range.length);
  sliced.ranges_.push_back({0, range.length, range.bytes});
  return sliced;
}

void ShuffleColumnarToRowConverter::convert(
    const RowVectorWithStats& rowVector,
    const std::vector<uint32_t>& indexes,
    std::vector<std::vector<uint8_t*>>& sortedRows,
    std::vector<int64_t>& partitionBytes) {
  const auto numRows = rowVector.numRows;
  totalBufferSize_ += rowVector.totalMemorySize;
  boltBuffers_.emplace_back(
      RowInternalBuffer::allocate(rowVector.totalMemorySize, boltPool_));
  bufferAddress_ = boltBuffers_.back()->mutable_data();
  averageRowSize_ = numRows ? (rowVector.totalMemorySize / numRows) : 0;

  if (rowFormat_ == row::RowFormat::DENSE) {
    std::vector<size_t> bodyOffsets(numRows);
    uint32_t cursor = 0;
    for (int64_t r = 0; r < numRows; ++r) {
      const auto rowSize = static_cast<int32_t>(rowVector.rowSizes_[r]);
      *reinterpret_cast<int32_t*>(bufferAddress_ + cursor) = rowSize;
      bodyOffsets[r] = cursor + kSizeOfRowHeader;
      sortedRows[indexes[r]].push_back(bufferAddress_ + cursor);
      partitionBytes[indexes[r]] += rowSize + kSizeOfRowHeader;
      cursor += static_cast<uint32_t>(rowSize) + kSizeOfRowHeader;
    }

    rowVector.denseRow->serialize(
        rowVector.rowOffset,
        numRows,
        bufferAddress_,
        folly::Range<const size_t*>(bodyOffsets.data(), bodyOffsets.size()));
    return;
  }

  std::memset(bufferAddress_, 0, rowVector.totalMemorySize);
  size_t offset = kSizeOfRowHeader;
  for (auto i = 0; i < numRows; ++i) {
    auto rowSize = rowVector.compactRow->serialize(
        rowVector.rowOffset + i,
        reinterpret_cast<char*>(bufferAddress_ + offset));
    BOLT_DCHECK_EQ(rowSize, rowVector.rowSizes_[i]);
    // set rowSize
    *(int32_t*)(bufferAddress_ + offset - kSizeOfRowHeader) = rowSize;
    sortedRows[indexes[i]].push_back(
        bufferAddress_ + offset - kSizeOfRowHeader);
    partitionBytes[indexes[i]] += rowSize + kSizeOfRowHeader;
    offset += rowSize + kSizeOfRowHeader;
  }
}

void ShuffleRowToRowConverter::convert(
    const bytedance::bolt::CompositeRowVectorPtr& rowVector,
    const std::vector<uint32_t>& indexes,
    std::vector<std::vector<uint8_t*>>& sortedRows) {
  auto totalMemorySize = rowVector->totalRowSize();
  boltBuffers_.emplace_back(
      RowInternalBuffer::allocate(totalMemorySize, boltPool_));
  bufferAddress_ = boltBuffers_.back()->mutable_data();
  std::vector<int32_t> offsets;
  rowVector->deepCopyAndMakeContinuous(
      (char*)bufferAddress_, totalMemorySize, offsets);

  for (auto i = 0; i < rowVector->size(); ++i) {
    sortedRows[indexes[i]].emplace_back(bufferAddress_ + offsets[i]);
  }
}

} // namespace bytedance::bolt::shuffle::sparksql
