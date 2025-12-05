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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "BoltRowBasedSortShuffleWriter.h"
#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioning.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::shuffle::sparksql {

arrow::Status BoltRowBasedSortShuffleWriter::init() {
#if defined(__x86_64__)
  supportAvx512_ = __builtin_cpu_supports("avx512bw");
#else
  supportAvx512_ = false;
#endif
  ARROW_ASSIGN_OR_RAISE(
      partitioner_,
      Partitioner::make(
          options_.partitioning,
          numPartitions_,
          options_.startPartitionId,
          options_.sort_before_repartition));
  partition2RowCount_.resize(numPartitions_);
  partitionWriter_->setRowFormat(true);
  return arrow::Status::OK();
}

arrow::Status BoltRowBasedSortShuffleWriter::split(
    bytedance::bolt::RowVectorPtr rv,
    int64_t memLimit) {
  bytedance::bolt::NanosecondTimer splitTimer(&totalSplitTime_);
  updateInputMetrics(rv);
  bytedance::bolt::RowVectorPtr strippedRv;
  uint32_t slicedBatchSize = 0;
  std::vector<bytedance::bolt::RowVectorPtr> slicedBatches;
  if (bytedance::bolt::RowVector::isComposite(rv)) {
    // from columnar to composite, flush all previous batches
    if (vectorLayout_ == RowVectorLayout::kColumnar) {
      RETURN_NOT_OK(tryEvict());
    }

    return splitCompositeVector(rv, memLimit);
  }

  // input vector is not composite , but layout is kComposite
  // which means from composite to columnar switch, flush all previous batches
  if (vectorLayout_ == RowVectorLayout::kComposite) {
    RETURN_NOT_OK(tryEvict());
  }

  vectorLayout_ = RowVectorLayout::kColumnar;
  {
    bytedance::bolt::NanosecondTimer timer(&flattenTime_);
    constexpr uint32_t minBatchSize = 512;

    ensurePartialFlatten(rv, {0});
    ensureVectorLoaded(rv);
    BOLT_CHECK(
        rv != nullptr && (options_.partitioning != Partitioning::kSingle) &&
        partitioner_->hasPid());
    if (!isInitialized_) {
      // Caution: rv is not stripped and the first column is pid
      RETURN_NOT_OK(initFromRowVector(*rv));
      isInitialized_ = true;
    }

    // calc rv memory size and try reserve, reserve failed
    auto incrementSize = std::max(
        rv->estimateFlatSize(), rv->size() * rowConverter_->averageRowSize());
    if (!boltPool_->maybeReserve(incrementSize)) {
      RETURN_NOT_OK(tryEvict());
      requestSpill_ = false;
    }

    // estimate batchSize based on UnsafeRowSize
    slicedBatchSize = rowConverter_->averageRowSize()
        ? (options_.recommendedColumn2RowSize / rowConverter_->averageRowSize())
        : 0;

    if (slicedBatchSize > minBatchSize) {
      uint32_t numRows = rv->size();
      uint32_t numBatches = numRows / slicedBatchSize;
      slicedBatchSize = (numRows - slicedBatchSize * numBatches < 0.2 * numRows)
          ? (numRows + numBatches - 1) / numBatches
          : slicedBatchSize;
      if (numRows > slicedBatchSize) {
        for (auto i = 0; i < numRows; i += slicedBatchSize) {
          slicedBatches.emplace_back(
              std::dynamic_pointer_cast<bytedance::bolt::RowVector>(
                  rv->slice(i, std::min(slicedBatchSize, numRows - i))));
        }
      } else {
        slicedBatches.push_back(std::move(rv));
      }
      // variables' name not accurate, just for reuse
      combinedVectorNumber_ += slicedBatchSize;
      ++combineVectorTimes_;
    } else {
      slicedBatches.push_back(std::move(rv));
    }
  }

  for (const auto& rv : slicedBatches) {
    {
      bytedance::bolt::NanosecondTimer timer(&computePidTime_);
      auto pidArr = getFirstColumnWrapper(*rv);
      RETURN_NOT_OK(partitioner_->compute(
          pidArr, rv->size(), row2Partition_, partition2RowCount_));
      strippedRv = getStrippedRowVectorWrapper(*rv);
      setSplitState(SplitState::kSplit);
    }
    // RowVector->UnsafeRow
    {
      bytedance::bolt::NanosecondTimer timer(&convertTime_);
      rowConverter_->convert(
          strippedRv, row2Partition_, sortedRows_, partitionBytes_);
    }
  }

  // under memory pressure
  if (requestSpill_) {
    // or totalBufferSize() >= memLimit ?
    // memory used for evicting one partition is supposed to be small
    RETURN_NOT_OK(tryEvict());
    requestSpill_ = false;
  }
  setSplitState(SplitState::kInit);
  return arrow::Status::OK();
}

arrow::Status BoltRowBasedSortShuffleWriter::initFromRowVector(
    const bytedance::bolt::RowVector& rv) {
  // rv is not stripped
  auto&& rowType = getStrippedRowVectorType(rv);
  rowConverter_ =
      std::make_unique<ShuffleColumnarToRowConverter>(rowType, boltPool_);
  sortedRows_.resize(numPartitions_);
  partitionBytes_.resize(numPartitions_, 0);
  return arrow::Status::OK();
}

arrow::Status BoltRowBasedSortShuffleWriter::tryEvict(int64_t) {
  BOLT_DCHECK(vectorLayout_ != RowVectorLayout::kInvalid);
  if (vectorLayout_ == RowVectorLayout::kColumnar) {
    RETURN_NOT_OK(partitionWriter_->evict(sortedRows_, partitionBytes_, false));
    // release all row's memory
    rowConverter_->reset();
    boltPool_->release();
  } else {
    RETURN_NOT_OK(tryEvictComposite());
  }
  return arrow::Status::OK();
}

arrow::Status BoltRowBasedSortShuffleWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  // do nothing if not in proper state or not initialized
  if (evictState_ == EvictState::kUnevictable ||
      splitState_ == SplitState::kStop ||
      (vectorLayout_ == RowVectorLayout::kColumnar && !isInitialized_) ||
      (vectorLayout_ == RowVectorLayout::kComposite &&
       !isCompositeInitialized_) ||
      vectorLayout_ == RowVectorLayout::kInvalid) {
    *actual = 0;
    return arrow::Status::OK();
  }
  EvictGuard evictGuard{evictState_};
  if (splitState_ == SplitState::kInit) {
    auto memoryBefore = partitionBufferSize() + boltPool_->currentBytes();
    RETURN_NOT_OK(tryEvict());
    *actual =
        memoryBefore - (partitionBufferSize() + boltPool_->currentBytes());
    if (*actual < 0) [[unlikely]] {
      LOG(INFO) << __FUNCTION__ << ": reclaim negative memory " << *actual;
      *actual = 0;
    }
  } else {
    requestSpill_ = true;
    *actual = 0;
  }
  return arrow::Status::OK();
}

arrow::Status BoltRowBasedSortShuffleWriter::stop() {
  bytedance::bolt::NanosecondTimer stopTimer(&stopTime_);
  setSplitState(SplitState::kStop);
  RETURN_NOT_OK(tryEvict());
  {
    RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
    metrics_.useRowBased = 1;
    combinedVectorNumber_ = combineVectorTimes_ > 0
        ? (combinedVectorNumber_ / combineVectorTimes_)
        : combinedVectorNumber_;
    finalizeMetrics();
  }
  return arrow::Status::OK();
}

bytedance::bolt::RowTypePtr
BoltRowBasedSortShuffleWriter::getStrippedRowVectorType(
    const bytedance::bolt::RowVector& rv) {
  // get new row type
  auto rowType = rv.type()->asRow();
  auto typeChildren = rowType.children();
  typeChildren.erase(typeChildren.begin());
  return bytedance::bolt::ROW(std::move(typeChildren));
}

} // namespace bytedance::bolt::shuffle::sparksql
