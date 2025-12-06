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

#include "BoltShuffleWriterV2.h"
#include "ArrowFixedSizeBufferOutputStream.h"
#include "BoltShuffleWriter.h"
#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioning.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::shuffle::sparksql {

#define V2_RUNTIME_CHECK 0
arrow::Status BoltShuffleWriterV2::split(
    bytedance::bolt::RowVectorPtr rv,
    int64_t memLimit) {
  bytedance::bolt::NanosecondTimer splitTimer(&totalSplitTime_);
  updateInputMetrics(rv);
  BOLT_DCHECK(options_.partitioning != Partitioning::kSingle);
  if (bytedance::bolt::RowVector::isComposite(rv)) {
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
    ensureLoaded(rv);
    auto flatSize = rv->estimateFlatSize();
    ensureFlatten(rv);
    // try evict before split if current batch size is larger than memLimit
    if (flatSize > memLimit) {
      requestSpill_ = true;
    }
    RETURN_NOT_OK(tryEvict(memLimit));
    // tryEvict may release memory to boltPool
    memLimit = std::max(memLimit, (int64_t)boltPool_->freeBytes());

    auto maxBatchBytes = std::min(
        maxBatchBytes_, (uint64_t)std::max(memLimit, kMinMemLimit) / 2);
    maxBatchBytes = hasComplexType_
        ? std::min(maxBatchBytes, (uint64_t)maxCombinedBytesWithComplexType_)
        : maxBatchBytes;
    if (flatSize > maxBatchBytes) {
      return splitExtremelyLargeBatch(rv, memLimit, flatSize, maxBatchBytes);
    }
    numRowsInBatches_ += rv->size();
    numBytesInBatches_ += flatSize;
    batches_.emplace_back(rv);
    // when BoltMemoryManager is enabled, memLimit is lower than what we got in
    // Java, so we make threshold lower to store more batches.
    double ratio = memory::sparksql::ExecutionMemoryPool::inited() ? 0.5 : 0.25;
    if (options_.enableVectorCombination &&
        (!hasComplexType_ ||
         numBytesInBatches_ < maxCombinedBytesWithComplexType_) &&
        numRowsInBatches_ < options_.bufferSize &&
        numBytesInBatches_ < memLimit * ratio && !boltColumnTypes_.empty()) {
      return arrow::Status::OK();
    }
  }
  BOLT_CHECK(partitioner_->hasPid());
  if (batches_.size() == 1) {
    RETURN_NOT_OK(splitSingleBatch(memLimit));
  } else {
    RETURN_NOT_OK(splitBatches(memLimit));
  }

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::splitExtremelyLargeBatch(
    bytedance::bolt::RowVectorPtr& rv,
    int64_t memLimit,
    uint64_t flatSize,
    uint64_t maxBatchBytes) {
  auto numBatches = batches_.size();
  // split unsplited batches first if any
  if (numBatches) {
    if (numBatches == 1) {
      RETURN_NOT_OK(splitSingleBatch(memLimit));
    } else {
      RETURN_NOT_OK(splitBatches(memLimit));
    }
  }
  // split current extremely large rowvector
  int32_t numSplits = flatSize / maxBatchBytes + 1;
  auto numRows = rv->size();
  auto splitedBatchSize = (numRows + numSplits - 1) / numSplits;
  LOG(INFO) << __FUNCTION__
            << ": splitExtremelyLargeBatch numRows = " << numRows << ", size ["
            << flatSize << ", " << maxBatchBytes << ", " << memLimit << "], "
            << ", current stored batches_ = " << numBatches
            << ", numSplits = " << numSplits
            << ", splited numRows per batch = " << splitedBatchSize;
  int32_t offset = 0;
  do {
    auto length = std::min(splitedBatchSize, numRows);
    batches_.push_back(
        std::move(std::dynamic_pointer_cast<bytedance::bolt::RowVector>(
            rv->slice(offset, length))));
    LOG(INFO) << __FUNCTION__
              << ": splitExtremelyLargeBatch offset = " << offset
              << ", numRows = " << length;
    requestSpill_ = true;
    RETURN_NOT_OK(splitSingleBatch(memLimit));
    offset += length;
    numRows -= length;
  } while (numRows);
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::splitSingleBatch(int64_t memLimit) {
  auto rv = batches_[0];
  std::fill(
      partitionBufferBaseInBatches_.begin(),
      partitionBufferBaseInBatches_.end(),
      0);
  auto pidArr = getFirstColumnWrapper(*rv);
  START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
  RETURN_NOT_OK(partitioner_->compute(
      pidArr, rv->size(), row2Partition_, partition2RowCount_));
  END_TIMING();
  auto strippedRv = getStrippedRowVectorWrapper(*rv);
  RETURN_NOT_OK(initFromRowVector(*strippedRv));
  RETURN_NOT_OK(doSplit(*strippedRv, true, true, memLimit));
  batches_.clear();
  numRowsInBatches_ = 0;
  numBytesInBatches_ = 0;
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::splitBatches(int64_t memLimit) {
  START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
  for (int32_t i = 0; i < batches_.size(); ++i) {
    auto batch = batches_[i];
    int32_t* pidAddr = const_cast<int*>(getFirstColumnWrapper(*batch));
    // pidAddr->pid and partition2RowCount for all batches
    RETURN_NOT_OK(partitioner_->precompute(
        pidAddr, batch->size(), combinedPartition2RowCount_, (i == 0)));
  }
  END_TIMING();

  // prealloc for all batches
  START_TIMING(cpuWallTimingList_[CpuWallTimingIteratePartitions]);
  setSplitState(SplitState::kPreAlloc);
  combinedPartitionUsed_.clear();
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (combinedPartition2RowCount_[pid] > 0) {
      combinedPartitionUsed_.push_back(pid);
    }
  }
  for (int32_t i = 0; i < batches_.size(); ++i) {
    RETURN_NOT_OK(updateInputHasNull(*(batches_[i]), 1));
  }
  RETURN_NOT_OK(preAllocPartitionBuffers(
      combinedPartition2RowCount_, combinedPartitionUsed_));
  END_TIMING();

  std::fill(
      partitionBufferBaseInBatches_.begin(),
      partitionBufferBaseInBatches_.end(),
      0);
  for (int32_t i = 0; i < batches_.size(); ++i) {
    auto batch = batches_[i];
    auto pidAddr = getFirstColumnWrapper(*batch);
    RETURN_NOT_OK(partitioner_->fill(
        pidAddr, batch->size(), row2Partition_, partition2RowCount_));
    auto strippedRv = getStrippedRowVectorWrapper(*batch);
    RETURN_NOT_OK(initFromRowVector(*strippedRv));
    RETURN_NOT_OK(
        doSplit(*strippedRv, false, (i == batches_.size() - 1), memLimit));
    // release split batch
    batches_[i] = nullptr;
  }
  combinedVectorNumber_ += batches_.size();
  ++combineVectorTimes_;
  batches_.clear();
  numRowsInBatches_ = 0;
  numBytesInBatches_ = 0;
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::initPartitions() {
  auto simpleColumnCount = simpleColumnIndices_.size();

  partitionValidityAddrs_.resize(simpleColumnCount);
  std::for_each(
      partitionValidityAddrs_.begin(),
      partitionValidityAddrs_.end(),
      [this](std::vector<uint8_t*>& v) { v.resize(numPartitions_, nullptr); });

  partitionFixedWidthValueAddrsVector_.resize(fixedWidthColumnCount_);
  std::for_each(
      partitionFixedWidthValueAddrsVector_.begin(),
      partitionFixedWidthValueAddrsVector_.end(),
      [this](std::vector<std::vector<uint8_t*>>& v) {
        v.resize(numPartitions_);
      });

  partitionBinaryAddrsVector_.resize(binaryColumnIndices_.size());
  std::for_each(
      partitionBinaryAddrsVector_.begin(),
      partitionBinaryAddrsVector_.end(),
      [this](std::vector<std::vector<BinaryBuf>>& v) {
        v.resize(numPartitions_);
      });

  partitionValidityBuffers_.resize(simpleColumnCount);
  std::for_each(
      partitionValidityBuffers_.begin(),
      partitionValidityBuffers_.end(),
      [this](std::vector<std::shared_ptr<arrow::ResizableBuffer>>& v) {
        v.resize(numPartitions_);
      });

  batchNumRows_.resize(numPartitions_);
  combinedPartition2RowCount_.resize(numPartitions_);
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::stop() {
  bytedance::bolt::NanosecondTimer stopTimer(&stopTime_);
  if (vectorLayout_ == RowVectorLayout::kColumnar) {
    partitionWriter_->setRowFormat(false);
    if (batches_.size()) {
      RETURN_NOT_OK(splitBatches(INT64_MAX));
    }
    setSplitState(SplitState::kStop);
    shrinkBufferPoolMemory();
    ARROW_ASSIGN_OR_RAISE(auto ret, sequentialEvictAllPartitions());
    (void)ret;
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
      setSplitState(SplitState::kStop);
      RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
      metrics_.rowVectorModeCompress = rowVectorModeCompress_;
      releaseBufferPoolMemory();
    }

    stat();
  } else {
    setSplitState(SplitState::kStop);
    RETURN_NOT_OK(tryEvict());
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
      RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
    }
  }
  metrics_.useV2 = 1;
  finalizeMetrics();
  boltPool_->release();

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::tryEvict(int64_t memLimit) {
  if (vectorLayout_ == RowVectorLayout::kColumnar) {
    partitionWriter_->setRowFormat(false);
    RETURN_NOT_OK(evictFullPartitions());
    int64_t minReserveBufferSize = 8 * 1024 * 1024; // 8MB
    if (requestSpill_ ||
        std::max(maxVariableMemoryUsage_, minReserveBufferSize) >=
            memLimit / 4) {
      LOG(INFO) << "tryEvict: requestSpill_ = "
                << (requestSpill_ ? "true" : "false")
                << ", maxVariableMemoryUsage_ = " << maxVariableMemoryUsage_
                << ", memLimit = " << memLimit
                << ", poolSize = " << boltPool_->currentBytes()
                << ", poolFreeSize = " << boltPool_->freeBytes();
      shrinkBufferPoolMemory();
      ARROW_ASSIGN_OR_RAISE(auto ret, sequentialEvictAllPartitions());
      if (!ret && partitionWriter_->canSpill()) {
        // all rows cached, payload cache not emtpy
        RETURN_NOT_OK(partitionWriter_->evictPayLoadCache());
      }
      releaseBufferPoolMemory();
      std::fill(variableMemoryUsage_.begin(), variableMemoryUsage_.end(), 0);
      maxVariableMemoryUsage_ = 0;
    } else {
      // todo : actively do evict?
      if (pool_ != spillArrowPool_) {
        // try evict all cached payload if using different spill pool
        RETURN_NOT_OK(partitionWriter_->evictPayLoadCache());
      }
    }
    requestSpill_ = false;
    fullBatch_.clear();
    boltPool_->release();
  } else {
    RETURN_NOT_OK(tryEvictComposite());
  }
  return arrow::Status::OK();
}

arrow::Result<bool> BoltShuffleWriterV2::sequentialEvictAllPartitions() {
  uint32_t pid = 0;
  bool evicted = false;
  for (; pid < numPartitions_; ++pid) {
    if (partitionBufferBase_[pid]) {
      break;
    }
  }
  // if has uncached rows
  if (pid < numPartitions_) {
    evicted = true;
    RETURN_NOT_OK(partitionWriter_->startClearPayLoadCacheSequential());
    // start from 0
    for (pid = 0; pid < numPartitions_; ++pid) {
      RETURN_NOT_OK(evictPartitionBuffers(pid, Evict::kCacheNoMerge));
      RETURN_NOT_OK(partitionWriter_->clearSpecificPayLoadCache(pid));
    }
    RETURN_NOT_OK(partitionWriter_->stopClearPayLoadCacheSequential());
  }
  return evicted;
}

arrow::Status BoltShuffleWriterV2::doSplit(
    const bytedance::bolt::RowVector& rv,
    bool doAlloc,
    bool doEvict,
    int64_t memLimit) {
  auto rowNum = rv.size();
  RETURN_NOT_OK(buildPartition2Row(rowNum));

  if (doAlloc) {
    RETURN_NOT_OK(updateInputHasNull(rv));
    START_TIMING(cpuWallTimingList_[CpuWallTimingIteratePartitions]);
    setSplitState(SplitState::kPreAlloc);
    // Calculate buffer size based on available offheap memory, history average
    // bytes per row and options_.buffer_size.
    RETURN_NOT_OK(
        preAllocPartitionBuffers(partition2RowCount_, partitionUsed_));
    END_TIMING();
  }

  setSplitState(SplitState::kSplit);
  RETURN_NOT_OK(splitRowVector(rv));

  if (doEvict) {
    RETURN_NOT_OK(tryEvict(memLimit));
  }

  setSplitState(SplitState::kInit);
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::splitRowVector(
    const bytedance::bolt::RowVector& rv) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingSplitRV]);
  // now start to split the RowVector
  RETURN_NOT_OK(
      splitFixedWidthValueBuffer(rv, partitionFixedWidthValueAddrsVector_));
  RETURN_NOT_OK(splitValidityBuffer<false>(rv));
  RETURN_NOT_OK(splitBinaryArray(rv));
  RETURN_NOT_OK(splitComplexType(rv));

  for (auto& pid : partitionUsed_) {
    partitionBufferBase_[pid] += partition2RowCount_[pid];
    partitionBufferBaseInBatches_[pid] += partition2RowCount_[pid];
    if (partitionBufferBase_[pid] >= options_.bufferSize ||
        (hasComplexType_ && arenas_[pid]->size() > maxComplexTypePageSize_)) {
      fullBatch_[pid] = true;
    }
  }

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::initFixedColumnSize(
    const bytedance::bolt::RowVector& rv) {
  uint32_t numberBooleanColumns = 0;
  uint32_t bitmapBytes =
      bytedance::bolt::bits::roundUp(rv.childrenSize(), 8) / 8;
  needAlignmentBitmap_.resize(bitmapBytes, 0);

  for (size_t i = 0; i < fixedWidthColumnCount_; ++i) {
    switch (arrowColumnTypes_[simpleColumnIndices_[i]]->id()) {
      case arrow::BooleanType::type_id: {
        ++numberBooleanColumns;
        fixedColValueSize_.push_back(0);
      } break;
      default: {
        fixedColValueSize_.push_back(valueBufferSizeForFixedWidthArray(i, 1));
        if (fixedColValueSize_.back() > 8) {
          bytedance::bolt::bits::setBit(needAlignmentBitmap_.data(), i);
        }
      } break;
    }
  }

  if (numberBooleanColumns) {
    partitionBooleanValueBuffers_.resize(numberBooleanColumns);
    std::for_each(
        partitionBooleanValueBuffers_.begin(),
        partitionBooleanValueBuffers_.end(),
        [this](std::vector<std::unique_ptr<arrow::ResizableBuffer>>& v) {
          v.resize(numPartitions_);
        });
  }

  partitionBytesPerBatch_.resize(numPartitions_, 0);
  isValidityBufferRowVectorMode_.insert(
      isValidityBufferRowVectorMode_.begin(), 2 + hasComplexType_, false);

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::initFromRowVector(
    const bytedance::bolt::RowVector& rv) {
  if (boltColumnTypes_.empty()) {
    RETURN_NOT_OK(initColumnTypes(rv));
    RETURN_NOT_OK(initFixedColumnSize(rv));
    RETURN_NOT_OK(initPartitions());
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::allocateValidityBuffer(
    const uint32_t col,
    const uint32_t partitionId,
    const int32_t bytesNeeded) {
  if (inputHasNull_[col]) {
    auto& partitionValidityBuffer = partitionValidityBuffers_[col][partitionId];
    if (partitionValidityBuffer == nullptr) { // new alloc
      auto validityBufferResult = arrow::AllocateResizableBuffer(
          bytesNeeded, partitionBufferPool_.get());
      BOLT_CHECK(
          validityBufferResult.ok(), validityBufferResult.status().ToString());
      auto validityBuffer = std::move(validityBufferResult).ValueOrDie();
      // initialize all true once allocated
      memset(validityBuffer->mutable_data(), 0xFF, validityBuffer->capacity());
      partitionValidityAddrs_[col][partitionId] =
          validityBuffer->mutable_data();
      partitionValidityBuffer = std::move(validityBuffer);
    } else if (partitionValidityBuffer->size() < bytesNeeded) { // reallocate
      int32_t oldSize = partitionValidityBuffer->size();
      BOLT_CHECK(partitionValidityBuffer->Resize(bytesNeeded).ok());
      auto delta = partitionValidityBuffer->capacity() - oldSize;
      memset(partitionValidityBuffer->mutable_data() + oldSize, 0xFF, delta);
      partitionValidityAddrs_[col][partitionId] =
          partitionValidityBuffer->mutable_data();
    } else if (
        partitionValidityBuffer->mutable_data() !=
        partitionValidityAddrs_[col][partitionId]) {
      BOLT_CHECK(partitionValidityAddrs_[col][partitionId] == nullptr);
      partitionValidityAddrs_[col][partitionId] =
          partitionValidityBuffer->mutable_data();
    }
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::allocatePartitionBuffer(
    uint32_t partitionId,
    uint32_t newSize) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingAllocateBuffer]);
  uint32_t booleanColumnIndex = 0;
  uint32_t currentRowNum = partitionBufferBase_[partitionId];
  // fixed width columns
  for (auto i = 0; i < fixedWidthColumnCount_; ++i) {
    auto columnType = schema_->field(simpleColumnIndices_[i])->type()->id();
    int64_t valueBufferSize = 0;
    if (columnType == arrow::BooleanType::type_id) {
      int32_t bytesNeeded =
          arrow::bit_util::BytesForBits(currentRowNum + newSize);
      auto& booleanValueBuffer =
          partitionBooleanValueBuffers_[booleanColumnIndex][partitionId];
      // alloc or realloc
      if (booleanValueBuffer == nullptr) {
        auto numBytes = bytesNeeded * 4;
        auto result = arrow::AllocateResizableBuffer(
            numBytes, partitionBufferPool_.get());
        BOLT_CHECK(result.ok(), result.status().ToString());
        booleanValueBuffer = std::move(result).ValueOrDie();
        BOLT_CHECK(
            partitionFixedWidthValueAddrsVector_[i][partitionId].empty());
        partitionFixedWidthValueAddrsVector_[i][partitionId].push_back(
            booleanValueBuffer->mutable_data());
      } else if (booleanValueBuffer->size() < bytesNeeded) {
        int32_t oldSize = booleanValueBuffer->size();
        auto numBytes = std::max(bytesNeeded, 2 * oldSize);
        BOLT_CHECK(booleanValueBuffer->Resize(numBytes).ok());
        BOLT_CHECK(
            partitionFixedWidthValueAddrsVector_[i][partitionId].size() == 1);
        partitionFixedWidthValueAddrsVector_[i][partitionId][0] =
            booleanValueBuffer->mutable_data();
      }
      booleanColumnIndex++;
    } else { // not boolean
      bool needAlignment =
          bytedance::bolt::bits::isBitSet(needAlignmentBitmap_.data(), i);
      valueBufferSize = newSize * fixedColValueSize_[i];
      uint8_t* valueBuffer = nullptr;
      if (needAlignment) {
        RETURN_NOT_OK(allocateBufferAligned(valueBufferSize, &valueBuffer, 16));
      } else {
        RETURN_NOT_OK(allocateBuffer(valueBufferSize, &valueBuffer));
      }
      partitionFixedWidthValueAddrsVector_[i][partitionId].push_back(
          valueBuffer);
      partitionBytesPerBatch_[partitionId] += valueBufferSize;
    }
  }

  // variable length columns, allocate validityBuffer
  // length and value buffer allocate in splitBinaryType
  int32_t bytesNeeded = arrow::bit_util::BytesForBits(currentRowNum + newSize);
  for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
    ARROW_RETURN_NOT_OK(allocateValidityBuffer(i, partitionId, bytesNeeded));
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::splitBinaryType(
    uint32_t binaryIdx,
    const bytedance::bolt::FlatVector<bytedance::bolt::StringView>& src,
    std::vector<std::vector<BinaryBuf>>& dst) {
  auto rawValues = src.rawValues();

  for (auto& pid : partitionUsed_) {
    auto rowOffsetBase = partition2RowOffsetBase_[pid];
    auto numRows = partition2RowOffsetBase_[pid + 1] - rowOffsetBase;
    uint64_t lengthBufferLength = numRows * sizeof(BinaryArrayLengthBufferType);

    dst[pid].emplace_back(BinaryBuf(nullptr, nullptr, lengthBufferLength, 0));
    auto& binaryBuf = dst[pid].back();

    RETURN_NOT_OK(allocateBuffer(lengthBufferLength, &binaryBuf.lengthPtr));
    // use 32bit offset
    auto dstLengthBase = (BinaryArrayLengthBufferType*)(binaryBuf.lengthPtr);

    // calculate and allocate space for string's value
    for (auto i = 0; i < numRows; i++) {
      auto rowId = rowOffset2RowId_[rowOffsetBase + i];
      auto& stringView = rawValues[rowId];
      auto stringLen = src.isNullAt(rowId) ? 0 : stringView.size();
      // copy length
      dstLengthBase[i] = stringLen;
      binaryBuf.valueOffset += stringLen;
    }
    partitionBytesPerBatch_[pid] +=
        (lengthBufferLength + binaryBuf.valueOffset);

    if (binaryBuf.valueOffset) {
      RETURN_NOT_OK(allocateBuffer(binaryBuf.valueOffset, &binaryBuf.valuePtr));
      updateVariableMemoryUsage(
          pid, lengthBufferLength + binaryBuf.valueOffset);
    } else {
      updateVariableMemoryUsage(pid, lengthBufferLength);
      continue;
    }

    uint64_t offset = 0;
    for (auto i = 0; i < numRows; i++) {
      auto rowId = rowOffset2RowId_[rowOffsetBase + i];
      auto& stringView = rawValues[rowId];
      auto stringLen = src.isNullAt(rowId) ? 0 : stringView.size();
      // copy value
      if (stringLen) {
        fastCopy(binaryBuf.valuePtr + offset, stringView.data(), stringLen);
        offset += stringLen;
      }
    }
    BOLT_CHECK(
        offset == binaryBuf.valueOffset,
        "offset = {}, binaryBuf.valueOffset = {}",
        offset,
        binaryBuf.valueOffset);
  }

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::splitBinaryArray(
    const bytedance::bolt::RowVector& rv) {
  for (auto col = fixedWidthColumnCount_; col < simpleColumnIndices_.size();
       ++col) {
    auto binaryIdx = col - fixedWidthColumnCount_;
    auto& dstAddrs = partitionBinaryAddrsVector_[binaryIdx];
    auto colIdx = simpleColumnIndices_[col];
    auto column =
        rv.childAt(colIdx)->asFlatVector<bytedance::bolt::StringView>();
    RETURN_NOT_OK(splitBinaryType(binaryIdx, *column, dstAddrs));
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::evictPartitionBuffers(
    uint32_t partitionId,
    Evict::type evictType) {
  auto numRows = partitionBufferBase_[partitionId];
  if (numRows > 0) {
    bool mayUseRowVectorMode = (evictType == Evict::kCacheNoMerge);
    // assembleBuffersGeneral will finally decide RowVector mode or not
    ARROW_ASSIGN_OR_RAISE(
        auto buffers, assembleBuffersGeneral(partitionId, mayUseRowVectorMode));
    if (!buffers.empty()) {
      auto payload = std::make_unique<InMemoryPayload>(
          numRows,
          mayUseRowVectorMode ? &isValidityBufferRowVectorMode_
                              : &isValidityBuffer_,
          std::move(buffers),
          mayUseRowVectorMode);
      RETURN_NOT_OK(partitionWriter_->evict(
          partitionId, std::move(payload), evictType, false, hasComplexType_));
      RETURN_NOT_OK(resetValidityBuffer(partitionId));
    }
  }
  return arrow::Status::OK();
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
BoltShuffleWriterV2::assembleBuffersGeneral(
    uint32_t partitionId,
    bool& mayUseRowVectorMode) {
  if (mayUseRowVectorMode &&
      simpleColumnIndices_.size() >=
          options_.rowvectorModeCompressionMinColumns &&
      partitionBytesPerBatch_[partitionId] <=
          options_.rowvectorModeCompressionMaxBufferSize) {
    return assembleBuffersRowVectorMode(partitionId);
  }
  mayUseRowVectorMode = false;
  const auto& batchRows = batchNumRows_[partitionId];
  const uint32_t batchCount = batchRows.size();
  if (batchCount == 1) {
    return assembleBuffersOneBatch(partitionId);
  }

  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCreateRbFromBuffer]);
  auto numRows = partitionBufferBase_[partitionId];
  if (numRows == 0) [[unlikely]] {
    return std::vector<std::shared_ptr<arrow::Buffer>>{};
  }

#if V2_RUNTIME_CHECK
  uint64_t accumulatedNumRows = 0;
  for (auto i = 0; i < batchCount; ++i) {
    accumulatedNumRows += batchRows[i];
  }
  BOLT_CHECK(
      accumulatedNumRows == numRows,
      "accumulatedNumRows = {}, numRows = {}",
      accumulatedNumRows,
      numRows);
#endif
  const auto lengthBytes = numRows * kSizeOfBinaryArrayLengthBuffer;
  uint64_t validityBytes = arrow::bit_util::BytesForBits(numRows);

  // already filled
  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto numFields = schema_->num_fields();

  std::vector<std::shared_ptr<arrow::Buffer>> allBuffers;
  allBuffers.reserve(
      fixedWidthColumnCount_ * 2 + binaryColumnIndices_.size() * 3 +
      hasComplexType_);

  assembleBufferPool_.reset();
  for (int i = 0; i < numFields; ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        // validity buffer
        if (partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx]
                                   [partitionId] != nullptr) {
          allBuffers.push_back(arrow::SliceBuffer(
              partitionValidityBuffers_[fixedWidthColumnCount_ + binaryIdx]
                                       [partitionId],
              0,
              validityBytes));
        } else {
          allBuffers.push_back(nullptr);
        }

        const auto& binaryBufs =
            partitionBinaryAddrsVector_[binaryIdx][partitionId];
        // length buffer
        uint8_t* lengthBuffer;
        uint64_t valueLength = 0, valueOffset = 0;
        RETURN_NOT_OK(allocateassembledBuffer(lengthBytes, &lengthBuffer));
#if V2_RUNTIME_CHECK
        int32_t actualLength = 0;
#endif
        for (auto n = 0; n < binaryBufs.size(); ++n) {
          fastCopy(
              lengthBuffer + valueOffset,
              binaryBufs[n].lengthPtr,
              binaryBufs[n].valueCapacity);
#if V2_RUNTIME_CHECK
          actualLength += binaryBufs[n].valueCapacity;
#endif
          valueOffset += binaryBufs[n].valueCapacity;
          valueLength += binaryBufs[n].valueOffset;
        }
#if V2_RUNTIME_CHECK
        BOLT_CHECK(
            lengthBytes == actualLength,
            " lengthBytes = {}, actualLength = {}",
            lengthBytes,
            actualLength);
#endif
        allBuffers.push_back(
            std::make_shared<arrow::Buffer>(lengthBuffer, lengthBytes));

        // value buffer
        if (valueLength > 0) {
          uint8_t* valueBuffer;
          valueOffset = 0;
          RETURN_NOT_OK(allocateassembledBuffer(valueLength, &valueBuffer));
          for (uint32_t n = 0; n < binaryBufs.size(); ++n) {
            fastCopy(
                valueBuffer + valueOffset,
                binaryBufs[n].valuePtr,
                binaryBufs[n].valueOffset);
            valueOffset += binaryBufs[n].valueOffset;
          }
          allBuffers.push_back(
              std::make_shared<arrow::Buffer>(valueBuffer, valueLength));
        } else {
          allBuffers.push_back(zeroLengthNullBuffer());
        }

        partitionBinaryAddrsVector_[binaryIdx][partitionId].clear();
        partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx]
                               [partitionId] = nullptr;
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
      } break;
      case arrow::NullType::type_id: {
      } break;
      default: {
        // validity buffer
        if (partitionValidityAddrs_[fixedWidthIdx][partitionId] != nullptr) {
          allBuffers.push_back(arrow::SliceBuffer(
              partitionValidityBuffers_[fixedWidthIdx][partitionId],
              0,
              validityBytes));
        } else {
          allBuffers.push_back(nullptr);
        }

        // value buffer
        if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
          allBuffers.push_back(std::make_shared<arrow::Buffer>(
              partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId]
                                                  [0],
              validityBytes));
        } else {
          auto fixedLen = fixedColValueSize_[fixedWidthIdx];
          const auto& fixedValues =
              partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId];
          uint8_t* valueBuffer;
          uint64_t valueOffset = 0;
          RETURN_NOT_OK(
              allocateassembledBuffer(fixedLen * numRows, &valueBuffer));
          for (uint32_t n = 0; n < batchCount; ++n) {
            uint64_t valueLen = batchRows[n] * fixedLen;
            BOLT_DCHECK(valueLen != 0);
            fastCopy(valueBuffer + valueOffset, fixedValues[n], valueLen);
            valueOffset += valueLen;
          }
          BOLT_CHECK(valueOffset == fixedLen * numRows);
          allBuffers.push_back(
              std::make_shared<arrow::Buffer>(valueBuffer, valueOffset));
          partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId]
              .clear();
        }

        partitionValidityAddrs_[fixedWidthIdx][partitionId] = nullptr;
        fixedWidthIdx++;
        break;
      }
    }
  }

  if (hasComplexType_ && complexTypeData_[partitionId] != nullptr) {
    auto serializedSize = complexTypeData_[partitionId]->maxSerializedSize();
    ARROW_ASSIGN_OR_RAISE(
        auto flushBuffer,
        arrow::AllocateBuffer(serializedSize, spillArrowPool_));
    std::shared_ptr<arrow::Buffer> valueBuffer = std::move(flushBuffer);
    auto output =
        std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
    bytedance::bolt::serializer::presto::PrestoOutputStreamListener listener;
    ArrowFixedSizeBufferOutputStream out(output, &listener);
    complexTypeData_[partitionId]->flush(&out);
    allBuffers.emplace_back(valueBuffer);
    complexTypeData_[partitionId] = nullptr;
    arenas_[partitionId] = nullptr;
  }

  partitionBufferBase_[partitionId] = 0;
  batchNumRows_[partitionId].clear();
  partitionBytesPerBatch_[partitionId] = 0;
  return allBuffers;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
BoltShuffleWriterV2::assembleBuffersOneBatch(uint32_t partitionId) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCreateRbFromBuffer]);

  const auto& batchRows = batchNumRows_[partitionId];
  const uint32_t batchCount = batchRows.size();
  auto numRows = partitionBufferBase_[partitionId];
  BOLT_CHECK(batchCount == 1 && numRows == batchRows[0]);

  if (numRows == 0) [[unlikely]] {
    return std::vector<std::shared_ptr<arrow::Buffer>>{};
  }

  const auto lengthBytes = numRows * kSizeOfBinaryArrayLengthBuffer;
  uint64_t validityBytes = arrow::bit_util::BytesForBits(numRows);

  // already filled
  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto numFields = schema_->num_fields();

  std::vector<std::shared_ptr<arrow::Buffer>> allBuffers;
  allBuffers.reserve(
      fixedWidthColumnCount_ * 2 + binaryColumnIndices_.size() * 3 +
      hasComplexType_);

  assembleBufferPool_.reset();

  for (int i = 0; i < numFields; ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        // validity buffer
        if (partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx]
                                   [partitionId] != nullptr) {
          allBuffers.push_back(arrow::SliceBuffer(
              partitionValidityBuffers_[fixedWidthColumnCount_ + binaryIdx]
                                       [partitionId],
              0,
              validityBytes));
        } else {
          allBuffers.push_back(nullptr);
        }

        const auto& binaryBufs =
            partitionBinaryAddrsVector_[binaryIdx][partitionId];
        if (binaryBufs.size() == 1) {
          // length buffer
          allBuffers.push_back(std::make_shared<arrow::Buffer>(
              binaryBufs[0].lengthPtr, lengthBytes));

          // value buffer
          if (binaryBufs[0].valueOffset > 0) {
            allBuffers.push_back(std::make_shared<arrow::Buffer>(
                binaryBufs[0].valuePtr, binaryBufs[0].valueOffset));
          } else {
            allBuffers.push_back(zeroLengthNullBuffer());
          }
        } else {
          uint8_t* lengthBuffer;
          uint64_t valueLength = 0, valueOffset = 0;
          RETURN_NOT_OK(allocateassembledBuffer(lengthBytes, &lengthBuffer));
          for (auto n = 0; n < binaryBufs.size(); ++n) {
            // valueCapacity stores batchRows[n] *
            // sizeof(BinaryArrayLengthBufferType)
            fastCopy(
                lengthBuffer + valueOffset,
                binaryBufs[n].lengthPtr,
                binaryBufs[n].valueCapacity);
            valueOffset += binaryBufs[n].valueCapacity;
            valueLength += binaryBufs[n].valueOffset;
          }
          allBuffers.push_back(
              std::make_shared<arrow::Buffer>(lengthBuffer, lengthBytes));

          // value buffer
          if (valueLength > 0) {
            uint8_t* valueBuffer;
            valueOffset = 0;
            RETURN_NOT_OK(allocateassembledBuffer(valueLength, &valueBuffer));
            for (uint32_t n = 0; n < binaryBufs.size(); ++n) {
              fastCopy(
                  valueBuffer + valueOffset,
                  binaryBufs[n].valuePtr,
                  binaryBufs[n].valueOffset);
              valueOffset += binaryBufs[n].valueOffset;
            }
            allBuffers.push_back(
                std::make_shared<arrow::Buffer>(valueBuffer, valueLength));
          } else {
            allBuffers.push_back(zeroLengthNullBuffer());
          }
        }

        partitionBinaryAddrsVector_[binaryIdx][partitionId].clear();
        partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx]
                               [partitionId] = nullptr;
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
      } break;
      case arrow::NullType::type_id: {
      } break;
      default: {
        // validity buffer
        if (partitionValidityAddrs_[fixedWidthIdx][partitionId] != nullptr) {
          allBuffers.push_back(arrow::SliceBuffer(
              partitionValidityBuffers_[fixedWidthIdx][partitionId],
              0,
              validityBytes));
        } else {
          allBuffers.push_back(nullptr);
        }

        // value buffer
        if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
          allBuffers.push_back(std::make_shared<arrow::Buffer>(
              partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId]
                                                  [0],
              validityBytes));
        } else {
          auto fixedLen = fixedColValueSize_[fixedWidthIdx];
          const auto& fixedValues =
              partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId];
          allBuffers.push_back(std::make_shared<arrow::Buffer>(
              fixedValues[0], fixedLen * numRows));
          partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId]
              .clear();
        }

        partitionValidityAddrs_[fixedWidthIdx][partitionId] = nullptr;
        fixedWidthIdx++;
        break;
      }
    }
  }

  if (hasComplexType_ && complexTypeData_[partitionId] != nullptr) {
    auto serializedSize = complexTypeData_[partitionId]->maxSerializedSize();
    ARROW_ASSIGN_OR_RAISE(
        auto flushBuffer,
        arrow::AllocateBuffer(serializedSize, spillArrowPool_));
    std::shared_ptr<arrow::Buffer> valueBuffer = std::move(flushBuffer);
    auto output =
        std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
    bytedance::bolt::serializer::presto::PrestoOutputStreamListener listener;
    ArrowFixedSizeBufferOutputStream out(output, &listener);
    complexTypeData_[partitionId]->flush(&out);
    allBuffers.emplace_back(valueBuffer);
    complexTypeData_[partitionId] = nullptr;
    arenas_[partitionId] = nullptr;
  }

  partitionBufferBase_[partitionId] = 0;
  batchNumRows_[partitionId].clear();
  partitionBytesPerBatch_[partitionId] = 0;

  return allBuffers;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
BoltShuffleWriterV2::assembleBuffersRowVectorMode(uint32_t partitionId) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCreateRbFromBuffer]);
  const auto& batchRows = batchNumRows_[partitionId];
  const uint32_t batchCount = batchRows.size();
  auto numRows = partitionBufferBase_[partitionId];
  if (numRows == 0) [[unlikely]] {
    return std::vector<std::shared_ptr<arrow::Buffer>>{};
  }

  ++rowVectorModeCompress_;
  const auto lengthBytes = numRows * kSizeOfBinaryArrayLengthBuffer;
  uint64_t validityBytes = arrow::bit_util::BytesForBits(numRows);

  // already filled
  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto numFields = schema_->num_fields();

  auto bufferCount =
      fixedWidthColumnCount_ * 2 + binaryColumnIndices_.size() * 3;
  std::vector<std::shared_ptr<arrow::Buffer>> allBuffers;
  // length buffer + value buffer + complextype buffer (if any)
  allBuffers.reserve(2 + hasComplexType_);
  assembleBufferPool_.reset();

  // Length buffer layout |buffer
  // unCompressedLength|buffers.size()|complexBuffer count|buffer1 size|buffer2
  // size
  uint64_t totalLengthBufferSize = (bufferCount + 3) * sizeof(int64_t);
  uint64_t totalUncompressedSize = partitionBytesPerBatch_[partitionId] +
      validityBytes *
          (simpleColumnIndices_.size() + partitionBooleanValueBuffers_.size());
  // alloc assembled length buffer and uncompressed value buffer
  uint8_t* lengthBuffer;
  RETURN_NOT_OK(allocateassembledBuffer(totalLengthBufferSize, &lengthBuffer));
  uint8_t* uncompressedBufferPtr;
  RETURN_NOT_OK(
      allocateassembledBuffer(totalUncompressedSize, &uncompressedBufferPtr));

  int32_t pos = 3;
  int64_t uncompressedSize = 0;
  int64_t* lengthBufferPtr = (int64_t*)lengthBuffer;
  lengthBufferPtr[1] = bufferCount;
  lengthBufferPtr[2] = hasComplexType_;

  for (int i = 0; i < numFields; ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        // validity buffer
        if (partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx]
                                   [partitionId] != nullptr) {
          auto& validityBuffer =
              partitionValidityBuffers_[fixedWidthColumnCount_ + binaryIdx]
                                       [partitionId];
          lengthBufferPtr[pos++] = validityBytes;
          fastCopy(
              uncompressedBufferPtr + uncompressedSize,
              validityBuffer->mutable_data(),
              validityBytes);
          uncompressedSize += validityBytes;
        } else {
          // see static constexpr int64_t kNullBuffer = -1 defined in Payload.cc
          lengthBufferPtr[pos++] = -1;
        }

        const auto& binaryBufs =
            partitionBinaryAddrsVector_[binaryIdx][partitionId];
        auto valueStartPtr =
            uncompressedBufferPtr + uncompressedSize + lengthBytes;
        auto valueOffset = 0;
        // copy both length and value
        for (uint32_t n = 0; n < binaryBufs.size(); ++n) {
          fastCopy(
              uncompressedBufferPtr + uncompressedSize,
              binaryBufs[n].lengthPtr,
              binaryBufs[n].valueCapacity);
          uncompressedSize += binaryBufs[n].valueCapacity;
          fastCopy(
              valueStartPtr + valueOffset,
              binaryBufs[n].valuePtr,
              binaryBufs[n].valueOffset);
          valueOffset += binaryBufs[n].valueOffset;
        }
        uncompressedSize += valueOffset;
        lengthBufferPtr[pos++] = lengthBytes;
        lengthBufferPtr[pos++] = valueOffset;

        partitionBinaryAddrsVector_[binaryIdx][partitionId].clear();
        partitionValidityAddrs_[fixedWidthColumnCount_ + binaryIdx]
                               [partitionId] = nullptr;
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
      } break;
      case arrow::NullType::type_id: {
      } break;
      default: {
        // validity buffer
        if (partitionValidityAddrs_[fixedWidthIdx][partitionId] != nullptr) {
          auto& validityBuffer =
              partitionValidityBuffers_[fixedWidthIdx][partitionId];
          lengthBufferPtr[pos++] = validityBytes;
          fastCopy(
              uncompressedBufferPtr + uncompressedSize,
              validityBuffer->mutable_data(),
              validityBytes);
          uncompressedSize += validityBytes;
        } else {
          lengthBufferPtr[pos++] = -1;
        }

        // value buffer
        if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
          const auto& fixedValues =
              partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId];
          fastCopy(
              uncompressedBufferPtr + uncompressedSize,
              fixedValues[0],
              validityBytes);
          uncompressedSize += validityBytes;
          lengthBufferPtr[pos++] = validityBytes;
        } else {
          auto fixedLen = fixedColValueSize_[fixedWidthIdx];
          const auto& fixedValues =
              partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId];
          for (uint32_t n = 0; n < batchCount; ++n) {
            uint64_t valueLen = batchRows[n] * fixedLen;
            BOLT_DCHECK(valueLen != 0);
            fastCopy(
                uncompressedBufferPtr + uncompressedSize,
                fixedValues[n],
                valueLen);
            uncompressedSize += valueLen;
          }
          lengthBufferPtr[pos++] = numRows * fixedLen;
          partitionFixedWidthValueAddrsVector_[fixedWidthIdx][partitionId]
              .clear();
        }

        partitionValidityAddrs_[fixedWidthIdx][partitionId] = nullptr;
        fixedWidthIdx++;
        break;
      }
    }
  }
  lengthBufferPtr[0] = uncompressedSize;
  allBuffers.push_back(
      std::make_shared<arrow::Buffer>(lengthBuffer, totalLengthBufferSize));
  allBuffers.push_back(
      std::make_shared<arrow::Buffer>(uncompressedBufferPtr, uncompressedSize));

  // complextype buffer is seperately stored
  if (hasComplexType_ && complexTypeData_[partitionId] != nullptr) {
    auto serializedSize = complexTypeData_[partitionId]->maxSerializedSize();
    ARROW_ASSIGN_OR_RAISE(
        auto flushBuffer,
        arrow::AllocateBuffer(serializedSize, spillArrowPool_));
    std::shared_ptr<arrow::Buffer> valueBuffer = std::move(flushBuffer);
    auto output =
        std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
    bytedance::bolt::serializer::presto::PrestoOutputStreamListener listener;
    ArrowFixedSizeBufferOutputStream out(output, &listener);
    complexTypeData_[partitionId]->flush(&out);
    allBuffers.emplace_back(valueBuffer);
    complexTypeData_[partitionId] = nullptr;
    arenas_[partitionId] = nullptr;
  }

  partitionBufferBase_[partitionId] = 0;
  batchNumRows_[partitionId].clear();
  partitionBytesPerBatch_[partitionId] = 0;
  return allBuffers;
}

arrow::Status BoltShuffleWriterV2::resetValidityBuffer(uint32_t partitionId) {
  std::for_each(
      partitionValidityBuffers_.begin(),
      partitionValidityBuffers_.end(),
      [partitionId](auto& bufs) {
        if (bufs[partitionId] != nullptr && bufs[partitionId]->size() > 0) {
          memset(
              bufs[partitionId]->mutable_data(),
              0xff,
              bufs[partitionId]->capacity());
        }
      });
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::evictFullPartitions() {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingEvictPartition]);
  for (const auto& [pid, dummy] : fullBatch_) {
    RETURN_NOT_OK(evictPartitionBuffers(pid, Evict::kCacheNoMerge));
    variableMemoryUsage_[pid] = 0;
  }
  maxVariableMemoryUsage_ = *std::max_element(
      variableMemoryUsage_.begin(), variableMemoryUsage_.end());
  fullBatch_.clear();
  return arrow::Status::OK();
}

arrow::Result<int64_t> BoltShuffleWriterV2::evictPartitionBuffersMinSize(
    int64_t /*size*/) {
  // Evict partition buffers, only when splitState_ == SplitState::kInit
  int64_t beforeEvict = partitionBufferPool_->bytes_allocated();
  shrinkBufferPoolMemory();
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (partitionBufferBase_[pid] == 0) {
      continue;
    }
    RETURN_NOT_OK(evictPartitionBuffers(pid, Evict::kSpill));
  }
  releaseBufferPoolMemory();
  std::fill(variableMemoryUsage_.begin(), variableMemoryUsage_.end(), 0);
  maxVariableMemoryUsage_ = 0;
  return beforeEvict - partitionBufferPool_->bytes_allocated();
}

arrow::Result<int64_t> BoltShuffleWriterV2::evictCachedPayloadNoMerge(
    int64_t size) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingEvictPartition]);
  int64_t actual;
  RETURN_NOT_OK(partitionWriter_->reclaimFixedSizeNoMerge(size, &actual));
  return actual;
}

arrow::Status BoltShuffleWriterV2::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable ||
      splitState_ == SplitState::kStop ||
      (vectorLayout_ == RowVectorLayout::kComposite &&
       !isCompositeInitialized_)) {
    *actual = 0;
    return arrow::Status::OK();
  }
  EvictGuard evictGuard{evictState_};

  if (vectorLayout_ == RowVectorLayout::kComposite) {
    RETURN_NOT_OK(reclaimFixedSizeInCompositeLayout(actual));
  } else {
    int64_t reclaimed = 0;
    // reclaim triggered by other operator
    if (splitState_ == SplitState::kInit) {
      partitionWriter_->setRowFormat(false);
      // evict cached first
      ARROW_ASSIGN_OR_RAISE(
          auto cached, evictCachedPayloadNoMerge(size - reclaimed));
      reclaimed += cached;
      if (reclaimed < size) {
        ARROW_ASSIGN_OR_RAISE(
            auto evicted, evictPartitionBuffersMinSize(size - reclaimed));
        reclaimed += evicted;
      }
    } else { // triggered by self
      requestSpill_ = true;
    }
    *actual = reclaimed;
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriterV2::preAllocPartitionBuffers(
    std::vector<uint32_t>& partition2RowCount,
    std::vector<uint32_t>& partitionUsed) {
  for (auto& pid : partitionUsed) {
    auto pRowNum = partition2RowCount[pid];
    RETURN_NOT_OK(allocatePartitionBuffer(pid, pRowNum));
    batchNumRows_[pid].push_back(pRowNum);
  }
  return arrow::Status::OK();
}

// for debug only when string len is negative or extremely large
void BoltShuffleWriterV2::checkLengthBuffer(uint32_t binaryIdx, uint32_t pid) {
  for (auto col = 0; col < binaryColumnIndices_.size(); ++col) {
    const auto& dst = partitionBinaryAddrsVector_[col];
    for (auto i = 0; i < numPartitions_; ++i) {
      if (dst[i].size() > 0) {
        for (auto j = 0; j < dst[i].size(); ++j) {
          const auto& buf = dst[i][j];
          uint32_t* lens = (uint32_t*)(buf.lengthPtr);
          auto tmprows = buf.valueCapacity / sizeof(uint32_t);
          for (auto k = 0; k < tmprows; ++k) {
            if (lens[k] > 1000000 || lens[k] < 0) {
              std::stringstream ss;
              ss << "[";
              for (auto f = 0; f < tmprows; ++f) {
                ss << lens[f] << ",";
              }
              ss << "]";
              LOG(ERROR) << __FUNCTION__ << ": stringLen = " << lens[k]
                         << ", valueOffset = " << buf.valueOffset
                         << "detail = " << ss.str()
                         << ", current alloc pid = " << pid
                         << ", binaryIdx = " << binaryIdx
                         << ", error pid = " << i << ", error col = " << col
                         << ", number BinaryBuf = " << dst[i].size();

              if (batchNumRows_[i].size()) {
                std::stringstream ss1;
                ss1 << "[";
                for (auto tt = 0; tt < batchNumRows_[i].size(); ++tt) {
                  ss1 << batchNumRows_[i][tt] << ",";
                }
                ss1 << "]";
                LOG(ERROR) << __FUNCTION__ << ":error batchNumRows_ "
                           << ss1.str() << ", partitionBufferBase_ = "
                           << partitionBufferBase_[i]
                           << ", partitionBufferBaseInBatches_ = "
                           << partitionBufferBaseInBatches_[i];
              } else {
                LOG(ERROR) << __FUNCTION__ << ":error batchNumRows_ empty";
              }

              if (batchNumRows_[pid].size()) {
                std::stringstream ss1;
                ss1 << "[";
                for (auto tt = 0; tt < batchNumRows_[pid].size(); ++tt) {
                  ss1 << batchNumRows_[pid][tt] << ",";
                }
                ss1 << "]";
                LOG(ERROR) << __FUNCTION__ << ":current batchNumRows_ "
                           << ss1.str() << ", partitionBufferBase_ = "
                           << partitionBufferBase_[pid]
                           << ", partitionBufferBaseInBatches_ = "
                           << partitionBufferBaseInBatches_[pid];
              } else {
                LOG(ERROR) << __FUNCTION__ << ":current batchNumRows_ empty";
              }
              BOLT_FAIL("checkLength failed");
            }
          }
        }
      }
    }
  }
}

void BoltShuffleWriterV2::checkNullValue(
    int32_t col,
    uint32_t pid,
    uint32_t numRows) {
  uint8_t* validityAddr = partitionValidityAddrs_[col][pid];
  if (validityAddr == nullptr) {
    BOLT_FAIL(
        "checkNullValue Failed, validityAddr = nullptr, col = {}, pid = {}, numRows = {}, base = {}, baseinbatch = {}",
        col,
        pid,
        numRows,
        partitionBufferBase_[pid],
        partitionBufferBaseInBatches_[pid]);
  } else {
    if (!bytedance::bolt::bits::isBitNull((uint64_t*)validityAddr, numRows)) {
      BOLT_FAIL(
          "checkNullValue Failed, validityAddr = nullptr, col = {}, pid = {}, numRows = {}, base = {}, baseinbatch = {}",
          col,
          pid,
          numRows,
          partitionBufferBase_[pid],
          partitionBufferBaseInBatches_[pid]);
    }
  }
}

} // namespace bytedance::bolt::shuffle::sparksql
