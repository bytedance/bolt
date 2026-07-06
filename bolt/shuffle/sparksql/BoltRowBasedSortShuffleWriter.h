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

#include "BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
namespace bytedance::bolt::shuffle::sparksql {
class BoltRowBasedSortShuffleWriter final : public BoltShuffleWriter {
 public:
  virtual ~BoltRowBasedSortShuffleWriter() {}

  arrow::Status split(bytedance::bolt::RowVectorPtr rv, int64_t memLimit)
      override;

  arrow::Status stop() override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  BoltRowBasedSortShuffleWriter(
      ShuffleWriterOptions options,
      bytedance::bolt::memory::MemoryPool* boltPool,
      arrow::MemoryPool* pool)
      : BoltShuffleWriter(std::move(options), boltPool, pool) {}

  std::string toString() const override {
    return "BoltRowBasedSortShuffleWriter";
  }

 private:
  arrow::Status init() override;

  arrow::Status initFromRowVector(
      const bytedance::bolt::RowVector& rv) override;

  const int64_t totalBufferSize() const {
    return rowConverter_->totalBufferSize();
  }

  virtual arrow::Status tryEvict(
      int64_t memLimit = std::numeric_limits<int64_t>::max()) override;

  bytedance::bolt::RowTypePtr getStrippedRowVectorType(
      const bytedance::bolt::RowVector& rv);

  std::unique_ptr<ShuffleColumnarToRowConverter> rowConverter_{nullptr};

  bool isInitialized_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql
