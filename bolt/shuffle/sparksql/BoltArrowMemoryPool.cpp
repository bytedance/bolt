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

#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
namespace bytedance::bolt::shuffle::sparksql {

arrow::Status
BoltArrowMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  *out = static_cast<uint8_t*>(pool_->allocate(size, alignment));
  bytesAllocated_ += size;
  return arrow::Status::OK();
}

arrow::Status BoltArrowMemoryPool::Reallocate(
    int64_t oldSize,
    int64_t newSize,
    int64_t alignment,
    uint8_t** ptr) {
  *ptr = static_cast<uint8_t*>(
      pool_->reallocate(static_cast<void*>(*ptr), oldSize, newSize, alignment));
  bytesAllocated_ += (newSize - oldSize);
  return arrow::Status::OK();
}

void BoltArrowMemoryPool::Free(
    uint8_t* buffer,
    int64_t size,
    int64_t alignment) {
  pool_->free(static_cast<void*>(buffer), size, alignment);
  bytesAllocated_ -= size;
}

int64_t BoltArrowMemoryPool::bytes_allocated() const {
  return bytesAllocated_;
}

int64_t BoltArrowMemoryPool::total_bytes_allocated() const {
  BOLT_FAIL("Not implement");
}

int64_t BoltArrowMemoryPool::num_allocations() const {
  BOLT_FAIL("Not implement");
}

std::string BoltArrowMemoryPool::backend_name() const {
  return "bolt arrow allocator";
}

} // namespace bytedance::bolt::shuffle::sparksql
