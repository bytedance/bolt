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

#include <arrow/memory_pool.h>
#include <arrow/type.h>

#include "bolt/buffer/Buffer.h"
#include "bolt/row/CompactRow.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::shuffle::sparksql {
static const uint32_t kSizeOfRowHeader = sizeof(int32_t);

class RowInternalBuffer;
using RowInternalBufferPtr = std::shared_ptr<RowInternalBuffer>;

class RowInternalBuffer final {
 public:
  RowInternalBuffer(
      uint8_t* ptr,
      int64_t size,
      bytedance::bolt::memory::MemoryPool* pool)
      : data_(ptr), size_(size), pool_(pool) {}

  ~RowInternalBuffer() {
    freeToPool();
  }

  static RowInternalBufferPtr allocate(
      int64_t size,
      bytedance::bolt::memory::MemoryPool* pool) {
    return std::make_shared<RowInternalBuffer>(
        (uint8_t*)pool->allocate(size), size, pool);
  }

  void freeToPool() {
    if (data_) {
      pool_->free(data_, size_);
    }
  }

  uint8_t* mutable_data() {
    return data_;
  }

 private:
  uint8_t* data_{nullptr};
  int64_t size_{0};
  bytedance::bolt::memory::MemoryPool* pool_;
};

class ShuffleColumnarToRowConverter {
 public:
  explicit ShuffleColumnarToRowConverter(
      const bytedance::bolt::RowTypePtr& rowType,
      bytedance::bolt::memory::MemoryPool* boltPool)
      : boltPool_(boltPool) {
    init(rowType);
  }

  class RowVectorWithStats {
    friend class ShuffleColumnarToRowConverter;

   public:
    int64_t getTotalMemorySize() {
      return totalMemorySize;
    }

   private:
    std::shared_ptr<bytedance::bolt::row::CompactRow> compactRow;
    int64_t numRows;
    int64_t totalMemorySize;
  };

  RowVectorWithStats getWithStats(
      const bytedance::bolt::RowVectorPtr& rowVector);

  void convert(
      const RowVectorWithStats& rowVector,
      const std::vector<uint32_t>& indexes,
      std::vector<std::vector<uint8_t*>>& sortedRows,
      std::vector<int64_t>& partitionBytes);

  void reset() {
    boltBuffers_.clear();
    totalBufferSize_ = 0;
  }

  const int64_t totalBufferSize() const {
    return totalBufferSize_;
  }

  const size_t averageRowSize() const {
    return averageRowSize_;
  }

 private:
  void init(const bytedance::bolt::RowTypePtr& rowType);

  int32_t fixedRowSize_ = 0;
  uint8_t* bufferAddress_;
  int64_t totalBufferSize_{0};
  size_t averageRowSize_{0};
  bytedance::bolt::memory::MemoryPool* boltPool_;
  std::vector<RowInternalBufferPtr> boltBuffers_;
};

class ShuffleRowToRowConverter {
 public:
  explicit ShuffleRowToRowConverter(
      bytedance::bolt::memory::MemoryPool* boltPool)
      : boltPool_(boltPool) {}

  void convert(
      const bytedance::bolt::CompositeRowVectorPtr& rowVector,
      const std::vector<uint32_t>& indexes,
      std::vector<std::vector<uint8_t*>>& sortedRows);

  void reset() {
    boltBuffers_.clear();
  }

 private:
  uint8_t* bufferAddress_;
  bytedance::bolt::memory::MemoryPool* boltPool_;
  std::vector<RowInternalBufferPtr> boltBuffers_;
};

} // namespace bytedance::bolt::shuffle::sparksql
