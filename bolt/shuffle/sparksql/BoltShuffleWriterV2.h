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

#pragma once
#include "BoltShuffleWriter.h"
namespace bytedance::bolt::shuffle::sparksql {
class InternalBuffer {
 public:
  InternalBuffer() : data_(nullptr), size_(0) {}
  InternalBuffer(uint8_t* data, uint64_t size) : data_(data), size_(size) {}
  uint8_t* data() const {
    return data_;
  };
  uint64_t size() const {
    return size_;
  };

 private:
  uint8_t* data_{nullptr};
  uint64_t size_{0};
};

class BufferPool {
 public:
  static constexpr int32_t kDefaultBufferSize = 8 << 20;
  static constexpr int32_t kDefaultBufferAlignment = 64;

  BufferPool() {}
  explicit BufferPool(ShuffleMemoryPool* pool) : pool_(pool) {}

  arrow::Status allocateFixed(uint64_t size, uint8_t** out) {
    if (bytesInBuffer_ >= size) { // enough memory in current buffer
      *out = startOfBuffer_;
      startOfBuffer_ += size;
      bytesInBuffer_ -= size;
    } else {
      if (size > kDefaultBufferSize) [[unlikely]] { // large buffer
        return allocateLargeBuffer(size, out);
      } else {
        return allocateFromNextBuffer(size, out);
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status
  allocateFixedAligned(uint64_t size, uint8_t** out, int32_t alignment) {
    BOLT_DCHECK_EQ(
        __builtin_popcount(alignment), 1, "Alignment can only be power of 2");
    // for simplicity
    BOLT_CHECK(alignment <= kDefaultBufferAlignment);

    if (bytesInBuffer_ >= size) {
      auto paddingBytes = alignmentPadding(startOfBuffer_, alignment);
      auto alignedBytes = size + paddingBytes;
      if (bytesInBuffer_ >= alignedBytes) {
        *out = startOfBuffer_ + paddingBytes;
        startOfBuffer_ += alignedBytes;
        bytesInBuffer_ -= alignedBytes;
      } else {
        return allocateFromNextBuffer(size, out);
      }
    } else {
      if (size > kDefaultBufferSize) [[unlikely]] { // large buffer
        return allocateLargeBuffer(size, out);
      } else {
        return allocateFromNextBuffer(size, out);
      }
    }
    return arrow::Status::OK();
  }

  uint64_t shrink() {
    uint64_t shrunkByte = 0;
    if (buffers_.size() > 0 && currentBufferIndex_ < buffers_.size() - 1) {
      for (auto index = currentBufferIndex_ + 1; index < buffers_.size();
           ++index) {
        pool_->Free(
            buffers_[index], kDefaultBufferSize, kDefaultBufferAlignment);
        buffers_[index] = nullptr;
      }
      auto shrunkByte =
          kDefaultBufferSize * (buffers_.size() - currentBufferIndex_ - 1);
      LOG(INFO) << "BufferPool::shrink() : shrunkByte = " << shrunkByte;
      reservedBytes_ -= shrunkByte;

      buffers_.erase(
          buffers_.begin() + currentBufferIndex_ + 1, buffers_.end());
    }
    return shrunkByte;
  }

  void clear() {
    for (auto i = 0; i < buffers_.size(); ++i) {
      pool_->Free(buffers_[i], kDefaultBufferSize, kDefaultBufferAlignment);
    }
    reservedBytes_ -= kDefaultBufferSize * buffers_.size();
    buffers_.clear();

    releaseLargeBuffers();
    assert(reservedBytes_ == 0);

    currentBufferIndex_ = -1;
    startOfBuffer_ = nullptr;
    bytesInBuffer_ = 0;
  }

  void reset() {
    releaseLargeBuffers();

    if (!buffers_.empty()) {
      currentBufferIndex_ = 0;
      bytesInBuffer_ = kDefaultBufferSize;
      startOfBuffer_ = buffers_[0];
    }
  }

  const uint64_t reservedBytes() {
    return reservedBytes_;
  }

  int32_t alignmentPadding(void* addr, int32_t alignment) {
    // % to be optimized
    auto padding = reinterpret_cast<uintptr_t>(addr) % alignment;
    return padding == 0 ? 0 : alignment - padding;
  }

 private:
  void releaseLargeBuffers() {
    for (auto i = 0; i < largeBuffers_.size(); ++i) {
      auto size = largeBuffers_[i].size();
      reservedBytes_ -= size;
      pool_->Free(largeBuffers_[i].data(), size, kDefaultBufferAlignment);
    }
    largeBuffers_.clear();
  }

  inline arrow::Status allocateLargeBuffer(uint64_t size, uint8_t** out) {
    BOLT_CHECK(pool_->Allocate(size, kDefaultBufferAlignment, out).ok());
    largeBuffers_.emplace_back(InternalBuffer{*out, size});
    reservedBytes_ += size;
    return arrow::Status::OK();
  }

  inline arrow::Status allocateFromNextBuffer(uint64_t size, uint8_t** out) {
    if (currentBufferIndex_ == buffers_.size() - 1) { // need new buffer
      BOLT_CHECK(
          pool_->Allocate(kDefaultBufferSize, kDefaultBufferAlignment, out)
              .ok());
      buffers_.push_back(*out);
      reservedBytes_ += kDefaultBufferSize;
    }
    ++currentBufferIndex_;
    *out = buffers_[currentBufferIndex_];
    startOfBuffer_ = (uint8_t*)(*out) + size;
    bytesInBuffer_ = kDefaultBufferSize - size;
    return arrow::Status::OK();
  }

  std::vector<uint8_t*> buffers_;
  std::vector<InternalBuffer> largeBuffers_;
  int32_t currentBufferIndex_ = -1;
  uint64_t bytesInBuffer_{0};
  uint64_t reservedBytes_{0};
  uint8_t* startOfBuffer_{nullptr};
  ShuffleMemoryPool* pool_{nullptr};
};

class BoltShuffleWriterV2 final : public BoltShuffleWriter {
 public:
  virtual ~BoltShuffleWriterV2() {
    bufferPool_.clear();
    assembleBufferPool_.clear();
  }

  arrow::Status split(bytedance::bolt::RowVectorPtr rv, int64_t memLimit);

  arrow::Status stop();

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  BoltShuffleWriterV2(
      ShuffleWriterOptions options,
      bytedance::bolt::memory::MemoryPool* boltPool,
      arrow::MemoryPool* pool)
      : BoltShuffleWriter(std::move(options), boltPool, pool) {
    bufferPool_ = BufferPool(partitionBufferPool_.get());
    assemblePool_ = std::make_shared<ShuffleMemoryPool>(spillArrowPool_);
    assembleBufferPool_ = BufferPool(assemblePool_.get());
  }

 private:
  void checkLengthBuffer(uint32_t col, uint32_t pid);

  void checkNullValue(int32_t col, uint32_t pid, uint32_t numRows);

  arrow::Status splitExtremelyLargeBatch(
      bytedance::bolt::RowVectorPtr& rv,
      int64_t memLimit,
      uint64_t flatSize,
      uint64_t maxBatchBytes);

  arrow::Status splitSingleBatch(int64_t memLimit);

  arrow::Status splitBatches(int64_t memLimit);

  arrow::Status initPartitions();

  virtual arrow::Status tryEvict(
      int64_t memLimit = std::numeric_limits<int64_t>::max()) override;

  arrow::Result<bool> sequentialEvictAllPartitions();

  arrow::Status doSplit(
      const bytedance::bolt::RowVector& rv,
      bool doAlloc,
      bool doEvict,
      int64_t memLimit);

  arrow::Status splitRowVector(const bytedance::bolt::RowVector& rv);

  arrow::Status initFixedColumnSize(const bytedance::bolt::RowVector& rv);

  arrow::Status initFromRowVector(const bytedance::bolt::RowVector& rv);

  arrow::Status allocateValidityBuffer(
      const uint32_t col,
      const uint32_t partitionId,
      const int32_t bytesNeeded);

  arrow::Status allocatePartitionBuffer(uint32_t partitionId, uint32_t newSize);

  arrow::Status splitBinaryType(
      uint32_t binaryIdx,
      const bytedance::bolt::FlatVector<bytedance::bolt::StringView>& src,
      std::vector<std::vector<BinaryBuf>>& dst);

  arrow::Status splitBinaryArray(const bytedance::bolt::RowVector& rv);

  arrow::Status evictPartitionBuffers(
      uint32_t partitionId,
      Evict::type evictType);

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
  assembleBuffersGeneral(uint32_t partitionId, bool& mayUseRowVectorMode);

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
  assembleBuffersOneBatch(uint32_t partitionId);

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
  assembleBuffersRowVectorMode(uint32_t partitionId);

  arrow::Status resetValidityBuffer(uint32_t partitionId);

  arrow::Status evictFullPartitions();

  arrow::Result<int64_t> evictPartitionBuffersMinSize(int64_t /*size*/);

  arrow::Result<int64_t> evictCachedPayloadNoMerge(int64_t size);

  arrow::Status preAllocPartitionBuffers(
      std::vector<uint32_t>& partition2RowCount,
      std::vector<uint32_t>& partitionUsed);

  arrow::Status allocateassembledBuffer(uint64_t size, uint8_t** out) {
    return assembleBufferPool_.allocateFixed(size, out);
  }

  arrow::Status allocateBuffer(uint64_t size, uint8_t** out) {
    return bufferPool_.allocateFixed(size, out);
  }

  arrow::Status
  allocateBufferAligned(uint64_t size, uint8_t** out, int32_t alignment) {
    return bufferPool_.allocateFixedAligned(size, out, alignment);
  }

  inline void releaseBufferPoolMemory() {
    bufferPool_.clear();
  }

  inline uint64_t shrinkBufferPoolMemory() {
    return bufferPool_.shrink();
  }

  inline void resetBufferPoolMemory() {
    // keep memory reserved
    return bufferPool_.reset();
  }

  // member
  std::vector<std::vector<std::shared_ptr<arrow::ResizableBuffer>>>
      partitionValidityBuffers_;

  std::vector<std::vector<std::vector<uint8_t*>>>
      partitionFixedWidthValueAddrsVector_;

  std::vector<std::vector<std::vector<BinaryBuf>>> partitionBinaryAddrsVector_;

  BufferPool bufferPool_;

  bool requestSpill_ = false;

  //[partition][n-th batch rowNum]
  std::vector<std::vector<uint32_t>> batchNumRows_;

  // buffer pool for assembleBuffers
  std::shared_ptr<ShuffleMemoryPool> assemblePool_;
  BufferPool assembleBufferPool_;
  std::vector<std::vector<std::unique_ptr<arrow::ResizableBuffer>>>
      partitionBooleanValueBuffers_;
  std::map<uint32_t, bool> fullBatch_;
  std::vector<uint16_t> fixedColValueSize_;
  std::vector<uint8_t> needAlignmentBitmap_;
  std::vector<uint32_t> partitionBytesPerBatch_;
  std::vector<bool> isValidityBufferRowVectorMode_;
  int64_t rowVectorModeCompress_{0};

  // for assemble batches
  std::vector<bytedance::bolt::RowVectorPtr> batches_;
  int32_t numRowsInBatches_{0};
  uint64_t numBytesInBatches_{0};
  std::vector<uint32_t> combinedPartition2RowCount_;
  std::vector<uint32_t> combinedPartitionUsed_;
  int64_t combinedVectorNumber_{0};
  int64_t combineVectorTimes_{0};
  uint64_t maxBatchBytes_{200UL << 20};

  // The PrestoVectorSerializer has a limit of INT32_MAX for the size after
  // serialization. In the case of hasComplexType_, limit total size of combined
  // batches to maxCachedBytesWithComplexType_ because this data may be
  // concentrated in one partition.
  const int32_t maxCombinedBytesWithComplexType_ =
      static_cast<int32_t>(INT32_MAX * 0.7);

  // maxComplexTypePageSize_ is accuracy bytes after serialization
  // set maxComplexTypePageSize_ less than maxCombinedBytesWithComplexType_
  // because maxCombinedBytesWithComplexType_ is usually estimated larger
  const int32_t maxComplexTypePageSize_{1UL << 30};
}; // class BoltShuffleWriterV2

} // namespace bytedance::bolt::shuffle::sparksql
