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

#include <memory>
#include "arrow/memory_pool.h"
#include "bolt/common/memory/MemoryPool.h"
namespace bytedance::bolt::shuffle::sparksql {

/*
 * This pool use bolt's MemoryPool to allocate buffer, compatible with
 * ArrowMemoryPool, should be deprecated after shuffle writer move to bolt.
 */
class BoltArrowMemoryPool final
    : public arrow::MemoryPool,
      public std::enable_shared_from_this<BoltArrowMemoryPool> {
 public:
  explicit BoltArrowMemoryPool(bytedance::bolt::memory::MemoryPool* pool)
      : pool_(pool) {}

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out)
      override;

  arrow::Status Reallocate(
      int64_t oldSize,
      int64_t newSize,
      int64_t alignment,
      uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  int64_t bytes_allocated() const override;

  int64_t total_bytes_allocated() const override;

  int64_t num_allocations() const override;

  std::string backend_name() const override;

 private:
  bytedance::bolt::memory::MemoryPool* pool_;
  int64_t bytesAllocated_ = 0; // Track bytes allocated by this pool, not the
                               // total bytes allocated by bolt pool.
};

} // namespace bytedance::bolt::shuffle::sparksql
