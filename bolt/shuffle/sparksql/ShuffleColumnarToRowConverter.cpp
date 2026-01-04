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
#include <cstddef>
#include <cstdint>

#include "bolt/row/CompactRow.h"
using namespace bytedance;
namespace bytedance::bolt::shuffle::sparksql {

void ShuffleColumnarToRowConverter::init(
    const bytedance::bolt::RowTypePtr& rowType) {
  if (auto fixedRowSize = bolt::row::CompactRow::fixedRowSize(rowType)) {
    fixedRowSize_ = fixedRowSize.value();
  }
}

ShuffleColumnarToRowConverter::RowVectorWithStats
ShuffleColumnarToRowConverter::getWithStats(
    const bytedance::bolt::RowVectorPtr& rowVector) {
  RowVectorWithStats stats;
  stats.compactRow = std::make_shared<bolt::row::CompactRow>(rowVector);
  stats.numRows = rowVector->size();
  stats.totalMemorySize = 0;
  auto numRows = rowVector->size();
  if (fixedRowSize_) {
    stats.totalMemorySize = fixedRowSize_ * numRows;
  } else {
    for (auto i = 0; i < numRows; ++i) {
      stats.totalMemorySize += stats.compactRow->rowSize(i);
    }
  }
  // layout : rowSize | unsafeRow
  stats.totalMemorySize += numRows * kSizeOfRowHeader;
  return stats;
}

void ShuffleColumnarToRowConverter::convert(
    const RowVectorWithStats& rowVector,
    const std::vector<uint32_t>& indexes,
    std::vector<std::vector<uint8_t*>>& sortedRows,
    std::vector<int64_t>& partitionBytes) {
  auto numRows = rowVector.numRows;
  totalBufferSize_ += rowVector.totalMemorySize;
  boltBuffers_.emplace_back(
      RowInternalBuffer::allocate(rowVector.totalMemorySize, boltPool_));
  bufferAddress_ = boltBuffers_.back()->mutable_data();
  memset(bufferAddress_, 0, sizeof(int8_t) * rowVector.totalMemorySize);
  averageRowSize_ = numRows ? (rowVector.totalMemorySize / numRows) : 0;
  size_t offset = kSizeOfRowHeader;
  for (auto i = 0; i < numRows; ++i) {
    auto rowSize =
        rowVector.compactRow->serialize(i, (char*)(bufferAddress_ + offset));
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
