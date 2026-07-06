/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <paimon/memory/memory_pool.h>
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/core/ExpressionEvaluator.h"

namespace bytedance::bolt::connector::paimon {

class BoltPaimonMemoryPool : public ::paimon::MemoryPool {
 public:
  explicit BoltPaimonMemoryPool(
      memory::MemoryPool* const pool,
      core::ExpressionEvaluator* const expressionEvaluator = nullptr)
      : pool_(pool), expressionEvaluator_(expressionEvaluator) {}

  memory::MemoryPool* getBoltPool() const {
    return pool_;
  }

  core::ExpressionEvaluator* getExpressionEvaluator() const {
    return expressionEvaluator_;
  }

  void* Malloc(uint64_t size, uint64_t alignment) override {
    if (alignment == 0) {
      return pool_->allocate(static_cast<int64_t>(size));
    }

    return pool_->allocate(
        static_cast<int64_t>(size), static_cast<uint32_t>(alignment));
  }

  void* Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment)
      override {
    if (alignment == 0) {
      return pool_->reallocate(
          p, static_cast<int64_t>(old_size), static_cast<int64_t>(new_size));
    }

    return pool_->reallocate(
        p,
        static_cast<int64_t>(old_size),
        static_cast<int64_t>(new_size),
        static_cast<uint8_t>(alignment));
  }

  void Free(void* p, uint64_t size) override {
    pool_->free(p, static_cast<int64_t>(size));
  }

  void Free(void* p, uint64_t size, uint64_t alignment) override {
    pool_->free(p, static_cast<int64_t>(size), static_cast<uint8_t>(alignment));
  }

  uint64_t CurrentUsage() const override {
    return static_cast<uint64_t>(pool_->usedBytes());
  }

  uint64_t MaxMemoryUsage() const override {
    return static_cast<uint64_t>(pool_->peakBytes());
  }

 private:
  memory::MemoryPool* const pool_;
  core::ExpressionEvaluator* const expressionEvaluator_;
};

} // namespace bytedance::bolt::connector::paimon
