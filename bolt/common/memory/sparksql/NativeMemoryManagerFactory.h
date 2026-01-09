/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <arrow/memory_pool.h>
#include <cstdint>
#include <map>
#include <memory>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/sparksql/AllocationListener.h"
#include "bolt/common/memory/sparksql/MemoryAllocator.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/MemoryUsageStats.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
namespace bytedance::bolt::memory::sparksql {

using BoltMemoryPool = bytedance::bolt::memory::MemoryPool;
using BoltMemoryPoolPtr = std::shared_ptr<BoltMemoryPool>;

class MemoryManager;
class BoltMemoryManager;
using MemoryManagerPtr = std::shared_ptr<MemoryManager>;
using BoltMemoryManagerPtr = std::shared_ptr<BoltMemoryManager>;
using InternalBoltMemoryManager = bytedance::bolt::memory::MemoryManager;

class BoltMemoryManagerHolder;
using BoltMemoryManagerHolderPtr = std::shared_ptr<BoltMemoryManagerHolder>;
using BoltMemoryManagerHolderWeakPtr = std::weak_ptr<BoltMemoryManagerHolder>;

class ListenableArbitrator final : public bolt::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListenerPtr& listener);

  std::string kind() const override;

  void shutdown() override {}

  void addPool(const std::shared_ptr<MemoryPool>& pool) override {
    BOLT_CHECK_EQ(pool->capacity(), 0);

    std::unique_lock guard{mutex_};
    BOLT_CHECK_EQ(candidates_.count(pool.get()), 0);
    candidates_.emplace(pool.get(), pool->weak_from_this());
  }

  void removePool(MemoryPool* pool) override {
    BOLT_CHECK_EQ(pool->reservedBytes(), 0);
    shrinkCapacity(pool, pool->capacity());

    std::unique_lock guard{mutex_};
    const auto ret = candidates_.erase(pool);
    BOLT_CHECK_EQ(ret, 1);
  }

  bool growCapacity(BoltMemoryPool* pool, uint64_t targetBytes) override;

  // shrinkCapacity with targetBytes, will reclaim from candidate pool
  uint64_t shrinkCapacity(
      uint64_t targetBytes,
      bool allowSpill = true,
      bool allowAbort = false) override;

  // shrinkCapacity for specific pool, only shrink from this pool
  uint64_t shrinkCapacity(BoltMemoryPool* pool, uint64_t targetBytes) override;

  MemoryArbitrator::Stats stats() const override;

  std::string toString() const override;

 private:
  bool growPoolLocked(BoltMemoryPool* pool, uint64_t bytes);

  uint64_t releaseMemoryLocked(BoltMemoryPool* pool, uint64_t bytes);

  AllocationListenerPtr listener_;
  std::recursive_mutex mutex_;
  inline static std::string kind_ = "GLUTEN";

  std::unordered_map<
      bolt::memory::MemoryPool*,
      std::weak_ptr<bolt::memory::MemoryPool>>
      candidates_;

  const uint64_t memoryReclaimMaxWaitMs_;
};

class ArbitratorFactoryRegister final {
 public:
  explicit ArbitratorFactoryRegister(AllocationListenerPtr& listener);

  virtual ~ArbitratorFactoryRegister();

  const std::string& getKind() const;

 private:
  std::string kind_;
  AllocationListenerPtr listener_;
};

class ArrowMemoryPool final : public arrow::MemoryPool {
 public:
  explicit ArrowMemoryPool(MemoryAllocatorPtr& allocator);

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
  MemoryAllocatorPtr allocator_;
};

class MemoryManager {
 public:
  MemoryManager() = default;

  virtual ~MemoryManager() = default;

  virtual arrow::MemoryPool* getArrowMemoryPool() = 0;

  virtual MemoryUsageStatsPtr collectMemoryUsageStats() const = 0;

  virtual int64_t shrink(int64_t size) = 0;

  // Hold this memory manager. The underlying memory pools will be released as
  // lately as this memory manager gets destroyed. Which means, a call to this
  // function would make sure the memory blocks directly or indirectly managed
  // by this manager, be guaranteed safe to access during the period that this
  // manager is alive.
  virtual void hold() = 0;
};

class BoltMemoryManager final : public MemoryManager {
 public:
  BoltMemoryManager(
      const std::string& name,
      MemoryAllocatorPtr& allocator,
      AllocationListenerPtr& listener,
      const std::unordered_map<std::string, std::string>& conf);

  ~BoltMemoryManager() override;
  BoltMemoryManager(const BoltMemoryManager&) = delete;
  BoltMemoryManager(BoltMemoryManager&&) = delete;
  BoltMemoryManager& operator=(const BoltMemoryManager&) = delete;
  BoltMemoryManager& operator=(BoltMemoryManager&&) = delete;

  BoltMemoryPoolPtr getAggregateMemoryPool() const;

  BoltMemoryPoolPtr getLeafMemoryPool() const;

  InternalBoltMemoryManager* getMemoryManager() const;

  arrow::MemoryPool* getArrowMemoryPool() override;

  MemoryUsageStatsPtr collectMemoryUsageStats() const override;

  int64_t shrink(int64_t size) override;

  void hold() override;

  std::string treeMemoryUsage();

 private:
  bool tryDestructSafe();

  static void holdInternal(
      std::vector<BoltMemoryPoolPtr>& heldBoltPools,
      const BoltMemoryPool* pool);

  static MemoryUsageStatsPtr collectMemoryUsageStatsInternal(
      const BoltMemoryPool* pool);

  std::string name_;

  int64_t maxWaitTime_{0};

  // This is a listenable allocator used for arrow.
  MemoryAllocatorPtr glutenAlloc_;
  AllocationListenerPtr listener_;
  std::unique_ptr<arrow::MemoryPool> arrowPool_;

  std::unique_ptr<InternalBoltMemoryManager> boltMemoryManager_;
  BoltMemoryPoolPtr boltAggregatePool_;
  BoltMemoryPoolPtr boltLeafPool_;
  std::vector<BoltMemoryPoolPtr> heldBoltPools_;
};

struct BoltMemoryManagerHolderKey {
  std::string name;
  int64_t taskId;
  int64_t memoryManagerHandle;

  bool operator<(const BoltMemoryManagerHolderKey& rhs) const {
    return std::tie(name, taskId, memoryManagerHandle) <
        std::tie(rhs.name, rhs.taskId, rhs.memoryManagerHandle);
  }
};

class BoltMemoryManagerHolder final {
 public:
  ~BoltMemoryManagerHolder() = default;

  static BoltMemoryManagerHolder* create(
      const std::string& name,
      AllocationListenerPtr& listener,
      MemoryTargetPtr& target,
      const std::unordered_map<std::string, std::string>& sessionConf);

  MemoryUsageStatsPtr collectMemoryUsage();

  int64_t shrink(int64_t size);

  void hold();

  BoltMemoryManagerPtr getManager();

  void appendSpiller(SpillerPtr& spiller);

  int64_t usage();

 private:
  BoltMemoryManagerHolder(
      const std::string& name,
      MemoryTargetPtr& target,
      MemoryManagerPtr& manager);

  std::string name_;
  MemoryManagerPtr manager_;
  MemoryTargetPtr target_;
};

struct NativeMemoryManagerFactoryParam {
  std::string name;
  bool memoryIsolation;
  int64_t conservativeTaskOffHeapMemorySize;
  double overAcquiredRatio;
  TaskMemoryManagerWeakPtr taskMemoryManager;
  std::unordered_map<std::string, std::string> sessionConf;
};

class NativeMemoryManagerFactory final {
 public:
  static BoltMemoryManagerHolder* contextInstance(
      const NativeMemoryManagerFactoryParam& param);

  static BoltMemoryManagerHolder* create(
      const NativeMemoryManagerFactoryParam& param,
      const std::list<SpillerPtr>& spillers);

 private:
  static BoltMemoryManagerHolder* createNativeMemoryManager(
      const NativeMemoryManagerFactoryParam& param,
      const std::list<SpillerPtr>& spillers);
};

} // namespace bytedance::bolt::memory::sparksql
