/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <atomic>
#include <chrono>
#include <limits>
#include <list>
#include <memory>
#include <string>

#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <glog/logging.h>

#include <bolt/common/base/Exceptions.h>
#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/flags/BoltFlags.h"
#include "bolt/common/memory/Allocation.h"
#include "bolt/common/memory/MemoryAllocator.h"
#include "bolt/common/memory/MemoryPool.h"
#include "folly/CPortability.h"
#include "folly/GLog.h"
#include "folly/Likely.h"
#include "folly/Random.h"
#include "folly/SharedMutex.h"

namespace bytedance::bolt::memory {
#define BOLT_MEM_LOG_PREFIX "[MEM] "
#define BOLT_MEM_LOG(severity) LOG(severity) << BOLT_MEM_LOG_PREFIX
#define BOLT_MEM_LOG_EVERY_MS(severity, ms) \
  FB_LOG_EVERY_MS(severity, ms) << BOLT_MEM_LOG_PREFIX

#define BOLT_MEM_ALLOC_ERROR(errorMessage)                          \
  _BOLT_THROW(                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kMemAllocError.c_str(),        \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

/// 'MemoryManager' is responsible for creating allocator, arbitrator and
/// managing the memory pools.
class MemoryManager {
 public:
  struct ManagedPoolState {
    enum class Kind {
      kOnlySystemPools,
      kHasOutstandingPools,
      kInvalidSystemState,
    };

    Kind kind{Kind::kInvalidSystemState};
    size_t poolCount{0};
    std::string diagnostic;
  };

  struct Options {
    Options() {}
    /// Specifies the default memory allocation alignment.
    uint16_t alignment{MemoryAllocator::kMaxAlignment};

    /// If true, enable memory usage tracking in the default memory pool.
    bool trackDefaultUsage{
        FLAGS_bolt_enable_memory_usage_track_in_default_memory_pool};

    /// If true, check the memory pool and usage leaks on destruction.
    ///
    /// TODO: deprecate this flag after all the existing memory leak use cases
    /// have been fixed.
    bool checkUsageLeak{FLAGS_bolt_memory_leak_check_enabled};

    /// Terminates the process and generates a core file on an allocation
    /// failure
    bool coreOnAllocationFailureEnabled{false};

    /// Disables the memory manager's tracking on memory pools.
    bool disableMemoryPoolTracking{false};

    /// ================== 'MemoryAllocator' settings ==================

    /// Specifies the max memory allocation capacity in bytes enforced by
    /// MemoryAllocator, default unlimited.
    int64_t allocatorCapacity{kMaxMemory};

    /// If true, uses MmapAllocator for memory allocation which manages the
    /// physical memory allocation on its own through std::mmap techniques. If
    /// false, use MallocAllocator which delegates the memory allocation to
    /// std::malloc.
    bool useMmapAllocator{false};

    /// Number of pages in the largest size class in MmapAllocator.
    int32_t largestSizeClassPages{256};

    /// If true, allocations larger than largest size class size will be
    /// delegated to ManagedMmapArena. Otherwise a system mmap call will be
    /// issued for each such allocation.
    ///
    /// NOTE: this only applies for MmapAllocator.
    bool useMmapArena{false};

    /// Used to determine MmapArena capacity. The ratio represents
    /// 'allocatorCapacity' to single MmapArena capacity ratio.
    ///
    /// NOTE: this only applies for MmapAllocator.
    int32_t mmapArenaCapacityRatio{10};

    /// If not zero, reserve 'smallAllocationReservePct'% of space from
    /// 'allocatorCapacity' for ad hoc small allocations. And those allocations
    /// are delegated to std::malloc. If 'maxMallocBytes' is 0, this value will
    /// be disregarded.
    ///
    /// NOTE: this only applies for MmapAllocator.
    uint32_t smallAllocationReservePct{0};

    /// The allocation threshold less than which an allocation is delegated to
    /// std::malloc(). If it is zero, then we don't delegate any allocation
    /// std::malloc, and 'smallAllocationReservePct' will be automatically set
    /// to 0 disregarding any passed in value.
    ///
    /// NOTE: this only applies for MmapAllocator.
    int32_t maxMallocBytes{3072};

    /// The memory allocations with size smaller than this threshold check the
    /// capacity with local sharded counter to reduce the lock contention on the
    /// global allocation counter. The sharded local counters reserve/release
    /// memory capacity from the global counter in batch. With this
    /// optimization, we don't have to update the global counter for each
    /// individual small memory allocation. If it is zero, then this
    /// optimization is disabled. The default is 1MB.
    ///
    /// NOTE: this only applies for MallocAllocator.
    uint32_t allocationSizeThresholdWithReservation{1 << 20};

    /// ================== 'MemoryArbitrator' settings =================

    /// Memory capacity available for query/task memory pools. This capacity
    /// setting should be equal or smaller than 'allocatorCapacity'. The
    /// difference between 'allocatorCapacity' and 'arbitratorCapacity' is
    /// reserved for system usage such as cache and spilling.
    ///
    /// NOTE:
    /// - if 'arbitratorCapacity' is greater than 'allocatorCapacity', the
    /// behavior will be equivalent to as if they are equal, meaning no
    /// reservation capacity for system usage.
    int64_t arbitratorCapacity{kMaxMemory};

    /// The string kind of memory arbitrator used in the memory manager.
    ///
    /// NOTE: the arbitrator will only be created if its kind is set explicitly.
    /// Otherwise MemoryArbitrator::create returns a nullptr.
    std::string arbitratorKind{};

    /// Provided by the query system to validate the state after a memory pool
    /// enters arbitration if not null. For instance, Prestissimo provides
    /// callback to check if a memory arbitration request is issued from a
    /// driver thread, then the driver should be put in suspended state to avoid
    /// the potential deadlock when reclaim memory from the task of the request
    /// memory pool.
    MemoryArbitrationStateCheckCB arbitrationStateCheckCb{nullptr};

    /// Additional configs that are arbitrator implementation specific.
    std::unordered_map<std::string, std::string> extraArbitratorConfigs{};

    /// Provides the customized get preferred size function for memory pool
    /// allocation. It returns the actual allocation size for a given input
    /// size. If not set, uses the memory pool's default get preferred size
    /// function.
    std::function<size_t(size_t)> getPreferredSize{nullptr};

    /// It is used to control the threshold of printing the allocation stack
    /// when
    /// memory is allocated. When the threshold is 0, it means turning off the
    /// printing stack function.
    std::string poolRegex{};
    int64_t singleAllocationThreshold{0};
    int64_t accumulativeAllocationThreshold{0};

    /// If true, the memory manager will use the memory pool for gluten
    bool useMemoryPoolForGluten{false};
    bool enableDynamicMemoryQuotaManager{false};
  };

  explicit MemoryManager(const Options& options = Options{});

  ~MemoryManager();

  /// Creates process-wide memory manager using specified options. Throws if
  /// memory manager has already been created by an easier call.
  static void initialize(const Options& options);

  /// Returns process-wide memory manager. Throws if 'initialize' hasn't been
  /// called yet.
  static MemoryManager* getInstance();

  /// Returns true if the memory manager has been set.
  static bool testInstance();

  /// Used by test to override the process-wide memory manager.
  static MemoryManager& testingSetInstance(const Options& options);

  /// Returns the memory capacity of this memory manager which puts a hard cap
  /// on memory usage, and any allocation that exceeds this capacity throws.
  int64_t capacity() const;

  /// Returns the memory allocation alignment of this memory manager.
  uint16_t alignment() const;

  /// Creates a root memory pool with specified 'name' and 'maxCapacity'. If
  /// 'name' is missing, the memory manager generates a default name internally
  /// to ensure uniqueness.
  std::shared_ptr<MemoryPool> addRootPool(
      const std::string& name = "",
      int64_t maxCapacity = kMaxMemory,
      std::unique_ptr<MemoryReclaimer> reclaimer = nullptr,
      const std::optional<MemoryPool::DebugOptions>& poolDebugOpts =
          std::nullopt);

  /// Creates a leaf memory pool for direct memory allocation use with specified
  /// 'name'. If 'name' is missing, the memory manager generates a default name
  /// internally to ensure uniqueness. The leaf memory pool is created as the
  /// child of the memory manager's default root memory pool. If 'threadSafe' is
  /// true, then we track its memory usage in a non-thread-safe mode to reduce
  /// its cpu cost.
  std::shared_ptr<MemoryPool> addLeafPool(
      const std::string& name = "",
      bool threadSafe = true);

  /// Invoked to shrink alive pools to free 'targetBytes' capacity. The function
  /// returns the actual freed memory capacity in bytes. If 'targetBytes' is
  /// zero, then try to reclaim all the memory from the alive pools. If
  /// 'allowSpill' is true, it reclaims the used memory by spilling. If
  /// 'allowAbort' is true, it reclaims the used memory by aborting the queries
  /// with the most memory usage. If both are true, it first reclaims the used
  /// memory by spilling and then abort queries to reach the reclaim target.
  uint64_t shrinkPools(
      uint64_t targetBytes = 0,
      bool allowSpill = true,
      bool allowAbort = false);

  /// Returns the current total memory usage under this memory manager.
  int64_t getTotalBytes() const;

  /// Returns the number of alive memory pools allocated from addRootPool() and
  /// addLeafPool().
  ///
  /// NOTE: this doesn't count the memory manager's internal default root and
  /// leaf memory pools.
  size_t numPools() const;

  /// Invokes 'visitor' on each direct child of the internal system root pool
  /// without exposing the root pool itself.
  void visitSystemRootChildren(
      const std::function<bool(MemoryPool*)>& visitor) const;

  /// Inspects whether only the internal system pools remain alive. The result
  /// doesn't prevent pools from being created or destroyed after this call.
  ManagedPoolState inspectManagedPoolState() const;

  MemoryAllocator* allocator();

  MemoryArbitrator* arbitrator();

  /// Returns debug string of this memory manager. If 'detail' is true, it
  /// returns the detailed tree memory usage from all the top level root memory
  /// pools.
  std::string toString(bool detail = false) const;

  std::string treeMemoryUsage() const;

  /// Returns the memory manger's internal default root memory pool for testing
  /// purpose and legacy use cases.
  MemoryPool& deprecatedSysRootPool() const {
    return *sysRoot_;
  }

  /// Returns the process wide leaf memory pool used for disk spilling.
  MemoryPool* spillPool() {
    return spillPool_.get();
  }

  /// Returns the process wide leaf memory pool used for ssd cache.
  MemoryPool* cachePool() {
    return cachePool_.get();
  }

  /// Returns the process wide leaf memory pool used for query tracing.
  MemoryPool* tracePool() const {
    return tracePool_.get();
  }

 private:
  std::shared_ptr<MemoryPoolImpl> createRootPool(
      std::string poolName,
      std::unique_ptr<MemoryReclaimer>& reclaimer,
      MemoryPool::Options& options);

  void dropPool(MemoryPool* pool);

  //  Returns the shared references to all the alive memory pools in 'pools_'.
  std::vector<std::shared_ptr<MemoryPool>> getAlivePools() const;

  const std::shared_ptr<MemoryAllocator> allocator_;

  // If not null, used to arbitrate the memory capacity among 'pools_'.
  const std::unique_ptr<MemoryArbitrator> arbitrator_;
  const uint16_t alignment_;
  const bool checkUsageLeak_;
  const bool coreOnAllocationFailureEnabled_;
  const bool disableMemoryPoolTracking_;
  const std::function<size_t(size_t)> getPreferredSize_;

  // Used to print stack when allocate memory
  std::string poolRegex_;
  int64_t singleAllocationThreshold_;
  int64_t accumulativeAllocationThreshold_;
  bool useMemoryPoolForGluten_{false};
  bool enableDynamicMemoryQuotaManager_{false};

  // The destruction callback set for the allocated root memory pools which are
  // tracked by 'pools_'. It is invoked on the root pool destruction and removes
  // the pool from 'pools_'.
  const MemoryPoolImpl::DestructionCallback poolDestructionCb_;

  const std::shared_ptr<MemoryPool> sysRoot_;
  const std::shared_ptr<MemoryPool> spillPool_;
  const std::shared_ptr<MemoryPool> cachePool_;
  const std::shared_ptr<MemoryPool> tracePool_;
  mutable folly::SharedMutex mutex_;
  // All user root pools allocated from 'this'.
  std::unordered_map<std::string, std::weak_ptr<MemoryPool>> pools_;
};

/// Initializes the process-wide memory manager based on the specified
/// 'options'.
///
/// NOTE: user should only call this once on query system startup. Otherwise,
/// the function throws.
void initializeMemoryManager(const MemoryManager::Options& options);

/// Returns the process-wide memory manager.
///
/// NOTE: user should have already initialized memory manager by calling.
/// Otherwise, the function throws.
MemoryManager* memoryManager();

/// Returns the system-wide memory pool for spilling memory usage.
memory::MemoryPool* spillMemoryPool();

/// Returns true if the provided 'pool' is the spilling memory pool.
bool isSpillMemoryPool(memory::MemoryPool* pool);

/// Returns the system-wide memory pool for tracing memory usage.
memory::MemoryPool* traceMemoryPool();

FOLLY_ALWAYS_INLINE int32_t alignmentPadding(void* address, int32_t alignment) {
  auto extra = reinterpret_cast<uintptr_t>(address) % alignment;
  return extra == 0 ? 0 : alignment - extra;
}
} // namespace bytedance::bolt::memory
