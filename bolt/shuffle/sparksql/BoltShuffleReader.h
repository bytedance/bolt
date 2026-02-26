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

#pragma once

#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::shuffle::sparksql {

class RowBufferPool final {
 public:
  RowBufferPool(arrow::MemoryPool* pool) : pool_(pool) {}

  ~RowBufferPool() {
    release();
  }
  void getRowBuffer(uint8_t** buf, int32_t& bufSize, int32_t minSize) {
    bufSize = std::max(minSize, BUFFER_SIZE);
    if (bufSize > BUFFER_SIZE) {
      auto arrowBuf =
          arrow::AllocateResizableBuffer(bufSize, pool_).ValueOrDie();
      *buf = arrowBuf->mutable_data();
      largeBuffers_.push_back(std::move(arrowBuf));
    } else {
      if (nextIndex_ == rowBuffers_.size()) {
        auto arrowBuf =
            arrow::AllocateResizableBuffer(bufSize, pool_).ValueOrDie();
        rowBuffers_.push_back(std::move(arrowBuf));
      }
      *buf = rowBuffers_[nextIndex_++]->mutable_data();
    }
    usedBytes_ += bufSize;
  }

  void reset() {
    largeBuffers_.clear();
    nextIndex_ = 0;
    usedBytes_ = 0;
  }

  void release() {
    rowBuffers_.clear();
    reset();
  }

  const int32_t bytesUsed() const {
    return usedBytes_;
  }

  const int32_t reservedBytes() const {
    int32_t reserved = rowBuffers_.size() * BUFFER_SIZE;
    for (auto const& buf : largeBuffers_) {
      reserved += buf->size();
    }
    return reserved;
  }

 private:
  std::vector<std::shared_ptr<arrow::Buffer>> rowBuffers_;
  std::vector<std::shared_ptr<arrow::Buffer>> largeBuffers_;
  const int32_t BUFFER_SIZE =
      std::max(ZSTD_DStreamOutSize(), (size_t)(1024 * 1024));
  int32_t nextIndex_{0};
  int32_t usedBytes_{0};
  arrow::MemoryPool* pool_;
};

class BoltColumnarBatchDeserializer {
 public:
  BoltColumnarBatchDeserializer(
      std::shared_ptr<arrow::io::InputStream> in,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<Codec>& codec,
      const bytedance::bolt::RowTypePtr& rowType,
      int32_t batchSize,
      int32_t shuffleBatchByteSize,
      arrow::MemoryPool* memoryPool,
      bytedance::bolt::memory::MemoryPool* boltPool,
      std::vector<bool>* isValidityBuffer,
      bool hasComplexType,
      uint64_t& deserializeTime,
      uint64_t& decompressTime,
      bool isRowFormat = false,
      AdaptiveParallelZstdCodec* zstdCodec = nullptr,
      RowBufferPool* rowBufferPool = nullptr,
      ShuffleRowToColumnarConverter* row2ColConverter = nullptr);

  bytedance::bolt::RowVectorPtr next();

 private:
  bytedance::bolt::RowVectorPtr nextFromRows();
  FLATTEN bool isCompositeRowVectorLayout(int64_t& bytes);

  std::shared_ptr<arrow::io::BufferedInputStream> in_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Codec> codec_;
  bytedance::bolt::RowTypePtr rowType_;
  int32_t batchSize_;
  int32_t shuffleBatchByteSize_;
  arrow::MemoryPool* memoryPool_;
  bytedance::bolt::memory::MemoryPool* boltPool_;
  std::vector<bool>* isValidityBuffer_;
  bool hasComplexType_;

  uint64_t& deserializeTime_;
  uint64_t& decompressTime_;

  std::unique_ptr<InMemoryPayload> merged_{nullptr};
  bool reachEos_{false};

  // for row format shuffle read
  const bool isRowFormat_{false};
  AdaptiveParallelZstdCodec* zstdCodec_{nullptr};
  RowBufferPool* rowBufferPool_{nullptr};
  uint8_t* partialRow_{nullptr};
  int32_t partialRowSize_{0};
  ShuffleRowToColumnarConverter* row2ColConverter_{nullptr};
  std::unique_ptr<arrow::ResizableBuffer> tailBuffer_{nullptr};

  // for CompositeRowVector shuffle reader
  RowVectorLayout vectorLayout_{RowVectorLayout::kInvalid};
  std::optional<uint8_t> payloadType_;
  ByteBuffer readAheadBuffer_;
};

class BoltColumnarBatchDeserializerFactory {
 public:
  BoltColumnarBatchDeserializerFactory(
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<Codec>& codec,
      const bytedance::bolt::RowTypePtr& rowType,
      int32_t batchSize,
      int32_t shuffleBatchByteSize,
      arrow::MemoryPool* memoryPool,
      bytedance::bolt::memory::MemoryPool* boltPool,
      bool checksumEnabled = true);

  std::unique_ptr<BoltColumnarBatchDeserializer> createDeserializer(
      std::shared_ptr<arrow::io::InputStream> in);

  arrow::MemoryPool* getPool();

  int64_t getDecompressTime();

  int64_t getDeserializeTime();

  void setNumPartitions(int32_t numPartitions) {
    numPartitions_ = numPartitions;
  }

  void setShuffleWriterType(int32_t writerType) {
    switch (writerType) {
      case 0:
        shuffleWriterType_ = ShuffleWriterType::Adaptive;
        break;
      case 1:
        shuffleWriterType_ = ShuffleWriterType::V1;
        break;
      case 2:
        shuffleWriterType_ = ShuffleWriterType::V2;
        break;
      case 3:
        shuffleWriterType_ = ShuffleWriterType::RowBased;
        break;
      default:
        BOLT_CHECK(
            false,
            "Illegal ShuffleWriterType writerType = " +
                std::to_string(writerType));
    }
  }

  void setpartitioningShortName(const std::string& name) {
    partitioningShortName_ = name;
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Codec> codec_;
  bytedance::bolt::RowTypePtr rowType_;
  int32_t batchSize_;
  int32_t shuffleBatchByteSize_;
  int32_t numPartitions_{0};
  ShuffleWriterType shuffleWriterType_{ShuffleWriterType::V1};
  std::string partitioningShortName_;
  arrow::MemoryPool* memoryPool_;
  bytedance::bolt::memory::MemoryPool* boltPool_;

  std::vector<bool> isValidityBuffer_;
  bool hasComplexType_{false};

  uint64_t deserializeTime_{0};
  uint64_t decompressTime_{0};

  void initFromSchema();
  // for rowbased shuffle
  std::shared_ptr<AdaptiveParallelZstdCodec> zstdCodec_{nullptr};
  std::shared_ptr<RowBufferPool> rowBufferPool_{nullptr};
  std::shared_ptr<ShuffleRowToColumnarConverter> row2ColConverter_{nullptr};
  bool checksumEnabled_{true};
};

class BoltShuffleReader {
 public:
  BoltShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options,
      arrow::MemoryPool* pool,
      bytedance::bolt::memory::MemoryPool* boltPool);

  ~BoltShuffleReader() {}

  // FIXME iterator should be unique_ptr or un-copyable singleton
  std::unique_ptr<BoltColumnarBatchDeserializer> readStream(
      std::shared_ptr<arrow::io::InputStream> in) {
    return factory_->createDeserializer(in);
  }

  arrow::Status close() {
    return arrow::Status::OK();
  }

  int64_t getDecompressTime() const {
    return factory_->getDecompressTime();
  }

  int64_t getIpcTime() const {
    return ipcTime_;
  }

  int64_t getDeserializeTime() const {
    return factory_->getDeserializeTime();
  }

  arrow::MemoryPool* getPool() const {
    return factory_->getPool();
  }

 private:
  arrow::MemoryPool* pool_;
  int64_t decompressTime_ = 0;
  int64_t ipcTime_ = 0;
  int64_t deserializeTime_ = 0;

  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<BoltColumnarBatchDeserializerFactory> factory_;
};

} // namespace bytedance::bolt::shuffle::sparksql
