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

ShuffleColumnarToRowConverter::RowVectorWithStats
ShuffleColumnarToRowConverter::getWithStats(
    const bytedance::bolt::RowVectorPtr& rowVector) {
  RowVectorWithStats stats;
  stats.numRows = rowVector->size();
  // Build the serializer (size pass) once in the configured format. The
  // partitioner guarantees no top-level null rows; convert() reuses the sizes
  // computed here for the write pass.
  int64_t payloadBytes = 0;
  if (rowFormat_ == bytedance::bolt::row::RowFormat::Compact) {
    // Plain CompactRow, used exactly as elsewhere. Its DecodedVector is
    // non-owning, so keep the source alive across getWithStats()/convert(), and
    // cache the per-row sizes for the layout (fixed-width: one size; else
    // per-row rowSize) — same as the canonical CompactRow serializer.
    stats.compactInput = rowVector;
    stats.compactRow =
        std::make_unique<bytedance::bolt::row::CompactRow>(rowVector);
    const auto n = stats.numRows;
    stats.compactSizes.resize(n);
    if (auto fixed = bytedance::bolt::row::CompactRow::fixedRowSize(
            asRowType(rowVector->type()))) {
      const auto sz = static_cast<size_t>(*fixed);
      std::fill(stats.compactSizes.begin(), stats.compactSizes.end(), sz);
      payloadBytes = static_cast<int64_t>(sz) * n;
    } else {
      for (int64_t r = 0; r < n; ++r) {
        const auto sz = static_cast<size_t>(
            stats.compactRow->rowSize(static_cast<vector_size_t>(r)));
        stats.compactSizes[r] = sz;
        payloadBytes += static_cast<int64_t>(sz);
      }
    }
  } else {
    // DenseRow holds a shared_ptr to the source RowVector; convert() reuses its
    // cached sizes/plan for the write pass.
    stats.denseRow.emplace(rowVector);
    payloadBytes = static_cast<int64_t>(stats.denseRow->totalSize());
  }
  stats.totalMemorySize = payloadBytes + stats.numRows * kSizeOfRowHeader;
  // The partition buffer is addressed with 32-bit offsets (header is an
  // int32 row length, body offsets are uint32). Fail loudly instead of
  // letting the cursor wrap on a pathologically large batch.
  BOLT_USER_CHECK_LE(
      stats.totalMemorySize,
      static_cast<int64_t>(std::numeric_limits<uint32_t>::max()),
      "ShuffleColumnarToRow: partition batch too large: {} bytes",
      stats.totalMemorySize);
  return stats;
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

  const bool isCompact =
      (rowFormat_ == bytedance::bolt::row::RowFormat::Compact);
  // CompactRow needs the row bytes pre-zeroed for null-bit handling; the
  // null-fused Dense format overwrites everything and needs no zeroing.
  if (isCompact) {
    std::memset(bufferAddress_, 0, rowVector.totalMemorySize);
  }

  // Lay out [int32 rowSize | rowBytes] per row in the partition buffer;
  // bodyOffsets[r] points just past row r's header so serialize() writes each
  // row's bytes directly into the partition slot. Per-row sizes come from the
  // serializer built in getWithStats(), so their lengths always agree.
  const std::vector<size_t>& rowSizesVec =
      isCompact ? rowVector.compactSizes : rowVector.denseRow->rowSizes();
  std::vector<size_t> bodyOffsets(numRows);
  uint32_t cursor = 0;
  for (int64_t r = 0; r < numRows; ++r) {
    const auto rowSize = static_cast<int32_t>(rowSizesVec[r]);
    *reinterpret_cast<int32_t*>(bufferAddress_ + cursor) = rowSize;
    bodyOffsets[r] = cursor + kSizeOfRowHeader;
    sortedRows[indexes[r]].push_back(bufferAddress_ + cursor);
    partitionBytes[indexes[r]] += rowSize + kSizeOfRowHeader;
    cursor += static_cast<uint32_t>(rowSize) + kSizeOfRowHeader;
  }

  if (isCompact) {
    // Identical to the canonical CompactRow batch serialize.
    rowVector.compactRow->serialize(
        0,
        static_cast<vector_size_t>(numRows),
        bodyOffsets.data(),
        reinterpret_cast<char*>(bufferAddress_));
  } else {
    rowVector.denseRow->serialize(
        bufferAddress_,
        folly::Range<const size_t*>(bodyOffsets.data(), bodyOffsets.size()));
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
