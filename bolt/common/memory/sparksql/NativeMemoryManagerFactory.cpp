/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include <arrow/memory_pool.h>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryArbitrator.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/sparksql/AllocationListener.h"
#include "bolt/common/memory/sparksql/ConfigurationResolver.h"
#include "bolt/common/memory/sparksql/MemoryAllocator.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/MemoryUsageStats.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/WeakPtrHelper.h"
namespace bytedance::bolt::memory::sparksql {

ListenableArbitrator::ListenableArbitrator(
    const Config& config,
    AllocationListenerPtr& listener)
    : MemoryArbitrator(config), listener_(listener) {}

std::string ListenableArbitrator::kind() const {
  return kind_;
}

bool ListenableArbitrator::growCapacity(
    bolt::memory::MemoryPool* pool,
    uint64_t targetBytes) {
  std::lock_guard<std::recursive_mutex> l(mutex_);
  return growPoolLocked(pool, targetBytes);
}

uint64_t ListenableArbitrator::shrinkCapacity(
    bolt::memory::MemoryPool* pool,
    uint64_t targetBytes) {
  std::lock_guard<std::recursive_mutex> l(mutex_);
  return releaseMemoryLocked(pool, targetBytes);
}

MemoryArbitrator::Stats ListenableArbitrator::stats() const {
  MemoryArbitrator::Stats stats; // no-op
  return stats;
}

std::string ListenableArbitrator::toString() const {
  return fmt::format(
      "ARBITRATOR[{}] CAPACITY {} {}",
      kind_,
      bolt::succinctBytes(capacity()),
      stats().toString());
}

bool ListenableArbitrator::growPoolLocked(
    bolt::memory::MemoryPool* pool,
    uint64_t bytes) {
  // We should pass bytes as parameter "reservationBytes" when calling ::grow.
  const uint64_t freeBytes = pool->freeBytes();
  if (freeBytes >= bytes) {
    bool reserved = pool->grow(0, bytes);
    BOLT_CHECK(reserved);
    return true;
  }
  const int64_t reserveSize = listener_->allocationChanged(bytes);
  if (reserveSize >= bytes) {
    auto beforeCapacity = pool->capacity();
    bool ret = pool->grow(reserveSize, bytes);
    BOLT_CHECK(
        ret,
        "{} failed to grow {} bytes, current state {}",
        pool->name(),
        bolt::succinctBytes(bytes),
        pool->toString());
    return ret;
  } else {
    listener_->allocationChanged(-reserveSize);
    return false;
  }
}

uint64_t ListenableArbitrator::releaseMemoryLocked(
    bolt::memory::MemoryPool* pool,
    uint64_t bytes) {
  uint64_t freeBytes = pool->shrink(0);
  listener_->allocationChanged(-freeBytes);
  return freeBytes;
}

ArbitratorFactoryRegister::ArbitratorFactoryRegister(
    AllocationListenerPtr& listener)
    : listener_(listener) {
  static std::atomic_uint32_t id{0UL};
  kind_ = "BOTL_FOR_GLUTEN_ARBITRATOR_FACTORY_" + std::to_string(id++);
  bolt::memory::MemoryArbitrator::registerFactory(
      kind_,
      [this](const bolt::memory::MemoryArbitrator::Config& config)
          -> std::unique_ptr<bolt::memory::MemoryArbitrator> {
        return std::make_unique<ListenableArbitrator>(config, listener_);
      });
}

ArbitratorFactoryRegister::~ArbitratorFactoryRegister() {
  bolt::memory::MemoryArbitrator::unregisterFactory(kind_);
}

const std::string& ArbitratorFactoryRegister::getKind() const {
  return kind_;
}

ArrowMemoryPool::ArrowMemoryPool(MemoryAllocatorPtr& allocator)
    : allocator_(allocator) {}

arrow::Status
ArrowMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  if (!allocator_->allocateAligned(
          alignment, size, reinterpret_cast<void**>(out))) {
    return arrow::Status::Invalid(
        "WrappedMemoryPool: Error allocating " + std::to_string(size) +
        " bytes");
  }
  return arrow::Status::OK();
}

arrow::Status ArrowMemoryPool::Reallocate(
    int64_t oldSize,
    int64_t newSize,
    int64_t alignment,
    uint8_t** ptr) {
  if (!allocator_->reallocateAligned(
          *ptr, alignment, oldSize, newSize, reinterpret_cast<void**>(ptr))) {
    return arrow::Status::Invalid(
        "WrappedMemoryPool: Error reallocating " + std::to_string(newSize) +
        " bytes");
  }
  return arrow::Status::OK();
}

void ArrowMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  allocator_->free(buffer, size);
}

int64_t ArrowMemoryPool::bytes_allocated() const {
  // fixme use self accountant
  return allocator_->getBytes();
}

int64_t ArrowMemoryPool::total_bytes_allocated() const {
  BOLT_NYI("Not implement");
}

int64_t ArrowMemoryPool::num_allocations() const {
  BOLT_NYI("Not implement");
}

std::string ArrowMemoryPool::backend_name() const {
  return "gluten arrow allocator";
}

BoltMemoryManager::BoltMemoryManager(
    const std::string& name,
    MemoryAllocatorPtr& allocator,
    AllocationListenerPtr& listener,
    const std::unordered_map<std::string, std::string>& conf)
    : name_(name), listener_(listener) {
  const std::string poolRegex = ConfigurationResolver::getStringParamFromConf(
      conf,
      ConfigurationResolver::kBacktraceAllocationPoolRegex,
      ConfigurationResolver::kBacktraceAllocationPoolRegexDefaultValue);
  const int64_t singleAllocationThreshold =
      ConfigurationResolver::getIntParamFromConf(
          conf,
          ConfigurationResolver::kBacktraceAllocationSingleThreshold,
          ConfigurationResolver::
              kBacktraceAllocationSingleThresholdDefaultValue);
  const int64_t accumulativeAllocationThreshold =
      ConfigurationResolver::getIntParamFromConf(
          conf,
          ConfigurationResolver::kBacktraceAllocationAccumulativeThreshold,
          ConfigurationResolver::
              kBacktraceAllocationAccumulativeThresholdDefaultValue);
  maxWaitTime_ = ConfigurationResolver::getIntParamFromConf(
      conf,
      ConfigurationResolver::kBoltMemoryManagerMaxWaitTimeWhenFree,
      ConfigurationResolver::kBoltMemoryManagerMaxWaitTimeWhenFreeDefaultValue);

  const bool enableDynamicMemoryQuotaManager =
      ConfigurationResolver::getBoolParamFromConf(
          conf,
          ConfigurationResolver::kDynamicMemoryQuotaManager,
          ConfigurationResolver::kDynamicMemoryQuotaManagerDefaultValue);

  // user-defined Allocator, used in gluten only
  glutenAlloc_ = std::make_shared<ListenableMemoryAllocator>(
      allocator,
      listener_,
      poolRegex,
      singleAllocationThreshold,
      accumulativeAllocationThreshold);
  arrowPool_ = std::make_unique<ArrowMemoryPool>(glutenAlloc_);

  ArbitratorFactoryRegister afr(listener_);
  bolt::memory::MemoryManager::Options mmOptions;
  mmOptions.alignment = bolt::memory::MemoryAllocator::kMaxAlignment;
  mmOptions.trackDefaultUsage = true; // memory usage tracking
  mmOptions.checkUsageLeak = true; // leak check
  // mmOptions.debugEnabled = false; // debug
  mmOptions.coreOnAllocationFailureEnabled = false;
  mmOptions.allocatorCapacity = bolt::memory::kMaxMemory;
  mmOptions.arbitratorKind = afr.getKind();
  // mmOptions.memoryPoolInitCapacity = 0;
  // mmOptions.memoryPoolTransferCapacity = 32 << 20;
  // mmOptions.memoryReclaimWaitMs = 0;
  mmOptions.poolRegex = poolRegex;
  mmOptions.singleAllocationThreshold = singleAllocationThreshold;
  mmOptions.accumulativeAllocationThreshold = accumulativeAllocationThreshold;
  mmOptions.useMemoryPoolForGluten = true; /* This code only for gluten */
  mmOptions.enableDynamicMemoryQuotaManager = enableDynamicMemoryQuotaManager;
  boltMemoryManager_ = std::make_unique<bolt::memory::MemoryManager>(mmOptions);

  boltAggregatePool_ = boltMemoryManager_->addRootPool(
      name_ + "_root",
      bolt::memory::kMaxMemory, // the 3rd capacity
      bytedance::bolt::memory::MemoryReclaimer::create());

  boltLeafPool_ = boltAggregatePool_->addLeafChild(name_ + "_default_leaf");
}

BoltMemoryManager::~BoltMemoryManager() {
  uint32_t accumulatedWaitMs = 0UL;
  for (int32_t tryCount = 0; accumulatedWaitMs < maxWaitTime_; tryCount++) {
    if (tryDestructSafe()) {
      if (tryCount > 0) {
        LOG(INFO)
            << "All the outstanding memory resources successfully released. ";
      }
      break;
    }
    uint32_t waitMs = 50 *
        static_cast<uint32_t>(pow(1.5, tryCount)); // 50ms, 75ms, 112.5ms ...
    LOG(INFO)
        << "There are still outstanding Bolt memory allocations in manager "
        << name_ << ". Waiting for " << waitMs
        << " ms to let possible async tasks done... ";
    usleep(waitMs * 1000);
    accumulatedWaitMs += waitMs;
  }
}

BoltMemoryPoolPtr BoltMemoryManager::getAggregateMemoryPool() const {
  return boltAggregatePool_;
}

BoltMemoryPoolPtr BoltMemoryManager::getLeafMemoryPool() const {
  return boltLeafPool_;
}

InternalBoltMemoryManager* BoltMemoryManager::getMemoryManager() const {
  return boltMemoryManager_.get();
}

arrow::MemoryPool* BoltMemoryManager::getArrowMemoryPool() {
  return arrowPool_.get();
}

MemoryUsageStatsPtr BoltMemoryManager::collectMemoryUsageStats() const {
  return collectMemoryUsageStatsInternal(boltAggregatePool_.get());
}

int64_t BoltMemoryManager::shrink(int64_t size) {
  InternalBoltMemoryManager* manager = boltMemoryManager_.get();
  BoltMemoryPool* pool = boltAggregatePool_.get();
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Shrink[" + poolName + "]: "};
  VLOG(2) << logPrefix << "Trying to shrink " << size << " bytes of data...";
  VLOG(2) << logPrefix << "Pool has reserved " << pool->currentBytes() << "/"
          << pool->root()->reservedBytes() << "/" << pool->root()->capacity()
          << "/" << pool->root()->maxCapacity() << " bytes.";
  VLOG(2) << logPrefix << "Shrinking...";
  const uint64_t oldCapacity = pool->capacity();
  manager->arbitrator()->shrinkCapacity(pool, 0);
  const uint64_t newCapacity = pool->capacity();
  int64_t shrunken = oldCapacity - newCapacity;
  VLOG(2) << logPrefix << shrunken << " bytes released from shrinking.";
  return shrunken;
}

void BoltMemoryManager::hold() {
  holdInternal(heldBoltPools_, boltAggregatePool_.get());
}

std::string BoltMemoryManager::treeMemoryUsage() {
  std::string str;
  std::string arrowUsed = succinctBytes(arrowPool_->bytes_allocated());
  str += fmt::format(
      "ArrowMemoryPool usage {} reserved {} peak {}\n",
      arrowUsed,
      arrowUsed,
      arrowUsed);
  str += boltMemoryManager_->treeMemoryUsage();
  return str;
}

bool BoltMemoryManager::tryDestructSafe() {
  // Bolt memory pools considered safe to destruct when no alive allocations.
  for (const auto& pool : heldBoltPools_) {
    if (pool && pool->currentBytes() != 0) {
      LOG(INFO) << pool->name() << " still hold " << pool->currentBytes()
                << " bytes, can't be freed immediately.\n"
                << pool->treeMemoryUsage();
      return false;
    }
  }
  if (boltLeafPool_ && boltLeafPool_->currentBytes() != 0) {
    LOG(INFO) << boltLeafPool_->name() << " still hold "
              << boltLeafPool_->currentBytes()
              << " bytes, can't be freed immediately.\n"
              << boltLeafPool_->treeMemoryUsage();
    return false;
  }
  if (boltAggregatePool_ && boltAggregatePool_->currentBytes() != 0) {
    LOG(INFO) << boltAggregatePool_->name() << " still hold "
              << boltAggregatePool_->currentBytes()
              << " bytes, can't be freed immediately.\n"
              << boltAggregatePool_->treeMemoryUsage();
    return false;
  }
  heldBoltPools_.clear();
  boltLeafPool_.reset();
  boltAggregatePool_.reset();

  // Bolt memory manager considered safe to destruct when no alive pools.
  if (boltMemoryManager_) {
    if (boltMemoryManager_->numPools() > 3) {
      LOG(INFO) << "memory::MemoryManager still hold "
                << boltMemoryManager_->numPools() << " pools, expect 2.\n"
                << boltMemoryManager_->toString(true);
      return false;
    }
    if (boltMemoryManager_->numPools() == 3) {
      // Assert the pool is spill pool
      int32_t spillPoolCount = 0;
      int32_t cachePoolCount = 0;
      int32_t tracePoolCount = 0;
      boltMemoryManager_->deprecatedSysRootPool().visitChildren(
          [&](bolt::memory::MemoryPool* child) -> bool {
            if (child == boltMemoryManager_->spillPool()) {
              spillPoolCount++;
            }
            if (child == boltMemoryManager_->cachePool()) {
              cachePoolCount++;
            }
            if (child == boltMemoryManager_->tracePool()) {
              tracePoolCount++;
            }
            return true;
          });
      BOLT_CHECK(
          spillPoolCount == 1,
          "Illegal pool count state: spillPoolCount: " +
              std::to_string(spillPoolCount));
      BOLT_CHECK(
          cachePoolCount == 1,
          "Illegal pool count state: cachePoolCount: " +
              std::to_string(cachePoolCount));
      BOLT_CHECK(
          tracePoolCount == 1,
          "Illegal pool count state: tracePoolCount: " +
              std::to_string(tracePoolCount));
    }
    if (boltMemoryManager_->numPools() < 3) {
      BOLT_CHECK(false, "Unreachable code");
    }
  }

  boltMemoryManager_.reset();

  // Applies similar rule for Arrow memory pool.
  if (arrowPool_ && arrowPool_->bytes_allocated() != 0) {
    LOG(INFO) << "Arrow pool still hold " << arrowPool_->bytes_allocated()
              << " bytes, can't be freed immediately.";
    return false;
  }
  arrowPool_.reset();

  // Successfully destructed.
  return true;
}

void BoltMemoryManager::holdInternal(
    std::vector<BoltMemoryPoolPtr>& heldBoltPools,
    const BoltMemoryPool* pool) {
  pool->visitChildren([&](bolt::memory::MemoryPool* child) -> bool {
    auto shared = child->shared_from_this();
    heldBoltPools.push_back(shared);
    holdInternal(heldBoltPools, child);
    return true;
  });
}

MemoryUsageStatsPtr BoltMemoryManager::collectMemoryUsageStatsInternal(
    const BoltMemoryPool* pool) {
  MemoryUsageStatsPtr stats = std::make_shared<MemoryUsageStats>();
  stats->current = pool->currentBytes();
  stats->peak = pool->peakBytes();
  // walk down root and all children
  pool->visitChildren([&](bolt::memory::MemoryPool* pool) -> bool {
    stats->children.emplace(
        pool->name(), collectMemoryUsageStatsInternal(pool));
    return true;
  });
  return stats;
}

BoltMemoryManagerHolder::BoltMemoryManagerHolder(
    const std::string& name,
    MemoryTargetPtr& target,
    MemoryManagerPtr& manager) {
  name_ = name;
  manager_ = manager;
  target_ = target;
}

BoltMemoryManagerHolder* BoltMemoryManagerHolder::create(
    const std::string& name,
    AllocationListenerPtr& listener,
    MemoryTargetPtr& target,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  auto defaultAllocator =
      DefaultMemoryAllocatorGetter::defaultMemoryAllocator();
  MemoryManagerPtr manager = std::make_shared<BoltMemoryManager>(
      name, defaultAllocator, listener, sessionConf);
  return new BoltMemoryManagerHolder(name, target, manager);
}

MemoryUsageStatsPtr BoltMemoryManagerHolder::collectMemoryUsage() {
  return manager_->collectMemoryUsageStats();
}

int64_t BoltMemoryManagerHolder::shrink(int64_t size) {
  return manager_->shrink(size);
}

void BoltMemoryManagerHolder::hold() {
  manager_->hold();
}

BoltMemoryManagerPtr BoltMemoryManagerHolder::getManager() {
  return std::dynamic_pointer_cast<BoltMemoryManager>(manager_);
}

void BoltMemoryManagerHolder::appendSpiller(SpillerPtr& spiller) {
  auto target = std::dynamic_pointer_cast<TreeMemoryTarget>(target_);
  BOLT_CHECK(target != nullptr, "dynamic_pointer_cast failed");
  target->appendSpiller(spiller);
}

int64_t BoltMemoryManagerHolder::usage() {
  auto manager = getManager();
  return manager->getAggregateMemoryPool()->reservedBytes() +
      manager->getArrowMemoryPool()->bytes_allocated();
}

namespace {

class StatsCollectRecorder final : public MemoryUsageRecorder {
 public:
  StatsCollectRecorder() = default;

  ~StatsCollectRecorder() override = default;

  void inc(int64_t bytes) override {
    // no op
  }

  int64_t peak() override {
    BOLT_NYI("Not Yet Implemented");
  }

  int64_t current() override {
    BOLT_NYI("Not Yet Implemented");
  }

  MemoryUsageStatsPtr toStats() override {
    return holder_->collectMemoryUsage();
  }

  void setManagerHolder(BoltMemoryManagerHolder* holder) {
    holder_ = holder;
  }

 private:
  BoltMemoryManagerHolder* holder_;
};

} // namespace

BoltMemoryManagerHolder* NativeMemoryManagerFactory::contextInstance(
    const NativeMemoryManagerFactoryParam& param) {
  std::list<SpillerPtr> empty{};
  return createNativeMemoryManager(param, empty);
}

BoltMemoryManagerHolder* NativeMemoryManagerFactory::create(
    const NativeMemoryManagerFactoryParam& param,
    const std::list<SpillerPtr>& spillers) {
  return createNativeMemoryManager(param, spillers);
}

BoltMemoryManagerHolder* NativeMemoryManagerFactory::createNativeMemoryManager(
    const NativeMemoryManagerFactoryParam& param,
    const std::list<SpillerPtr>& spillers) {
  // Normal Target
  std::shared_ptr<ShrinkSpiller> shrinkSpiller =
      std::make_shared<ShrinkSpiller>();
  // prepare spiller
  std::list<SpillerPtr> selfSpillers = spillers;
  selfSpillers.emplace_back(shrinkSpiller);

  std::shared_ptr<StatsCollectRecorder> collector =
      std::make_shared<StatsCollectRecorder>();
  std::map<std::string, MemoryUsageStatsBuilderPtr> virtualChildren{
      {"single", collector}};

  MemoryTargetPtr target = MemoryTargetBuilder::newConsumer(
      param.taskMemoryManager,
      param.name,
      selfSpillers,
      virtualChildren,
      param.memoryIsolation,
      param.conservativeTaskOffHeapMemorySize);

  // overAccquire Target
  auto overAccquireSpillers =
      std::list<SpillerPtr>{std::make_shared<OverAccquireSpiller>()};
  std::map<std::string, MemoryUsageStatsBuilderPtr> emptyVirtualChildren{};
  const std::string overTargetPrefix = "OverAccquire.";
  MemoryTargetPtr overTarget = MemoryTargetBuilder::newConsumer(
      param.taskMemoryManager,
      overTargetPrefix + param.name,
      overAccquireSpillers,
      emptyVirtualChildren,
      param.memoryIsolation,
      param.conservativeTaskOffHeapMemorySize);

  MemoryTargetPtr resultTarget = MemoryTargetBuilder::overAcquire(
      target, overTarget, param.overAcquiredRatio);

  SimpleMemoryUsageRecorderPtr recorderInManagedAllocationListener =
      std::make_shared<SimpleMemoryUsageRecorder>();
  AllocationListenerPtr listener = std::make_shared<ManagedAllocationListener>(
      param.name, resultTarget, recorderInManagedAllocationListener);

  BoltMemoryManagerHolder* ans = BoltMemoryManagerHolder::create(
      param.name,
      listener,
      target /* overTarget doesn't need export to user*/,
      param.sessionConf);
  shrinkSpiller->setManagerHolder(ans);
  collector->setManagerHolder(ans);
  return ans;
}

} // namespace bytedance::bolt::memory::sparksql