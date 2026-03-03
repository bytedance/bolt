/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <paimon/memory/memory_pool.h>
#include "bolt/common/memory/MemoryPool.h"

namespace bytedance::bolt::connector::paimon {

class BoltPaimonMemoryPool : public ::paimon::MemoryPool {
 public:
  explicit BoltPaimonMemoryPool(std::shared_ptr<memory::MemoryPool> pool)
      : pool_(std::move(pool)) {}

  memory::MemoryPool* getBoltPool() const {
    return pool_.get();
  }

  std::shared_ptr<memory::MemoryPool> getBoltPoolShared() const {
    return pool_;
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
  std::shared_ptr<memory::MemoryPool> pool_;
};

} // namespace bytedance::bolt::connector::paimon
