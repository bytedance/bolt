#include "bolt/common/memory/bm/tests/SparkListenableArbitratorContext.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/sparksql/ConfigurationResolver.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <limits>
#include <set>
#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

int64_t nextTaskAttemptId() {
  static std::atomic<int64_t> id{0};
  return id++;
}

class AutomaticReclaimSpiller final : public sparksql::Spiller {
 public:
  explicit AutomaticReclaimSpiller(std::function<int64_t(int64_t)> spill)
      : spill_(std::move(spill)) {}

  int64_t spill(sparksql::MemoryTargetWeakPtr /*self*/, int64_t size) override {
    if (size <= 0) {
      return 0;
    }
    return spill_(size);
  }

  const std::set<sparksql::SpillerPhase>& applicablePhases() override {
    return sparksql::SpillerHelper::phaseSetAll();
  }

 private:
  std::function<int64_t(int64_t)> spill_;
};

} // namespace

SparkListenableArbitratorContext::SparkListenableArbitratorContext(
    SparkListenableArbitratorContextOptions options)
    : options_(std::move(options)) {
  if (options_.initializeExecutionMemoryPool) {
    initializeExecutionMemoryPool(
        options_.memoryLimitBytes,
        options_.minMemoryMaxWaitMs,
        options_.maxTaskNumber);
  }

  taskMemoryManager_ = std::make_shared<sparksql::TaskMemoryManager>(
      sparksql::ExecutionMemoryPool::instance(), nextTaskAttemptId());

  options_.sessionConf.emplace(
      sparksql::ConfigurationResolver::kDynamicMemoryQuotaManager, "false");

  sparksql::NativeMemoryManagerFactoryParam param{
      .name = options_.name,
      .memoryIsolation = options_.memoryIsolation,
      .conservativeTaskOffHeapMemorySize = options_.memoryLimitBytes,
      .overAcquiredRatio = options_.overAcquiredRatio,
      .taskMemoryManager = taskMemoryManager_,
      .sessionConf = options_.sessionConf};
  holder_ = sparksql::NativeMemoryManagerFactory::contextInstance(param);
  BOLT_CHECK_NOT_NULL(holder_);
}

SparkListenableArbitratorContext::~SparkListenableArbitratorContext() {
  delete holder_;
  holder_ = nullptr;

  sparksql::MemoryTargetBuilder::invalidate(
      taskMemoryManager_, options_.memoryIsolation, options_.memoryLimitBytes);
  sparksql::ExecutionMemoryPool::instance()->releaseAllMemoryForTask(
      taskMemoryManager_->getTaskAttemptId());
}

std::shared_ptr<MemoryPool> SparkListenableArbitratorContext::rootPool() const {
  BOLT_CHECK_NOT_NULL(holder_);
  auto pool = holder_->getManager()->getAggregateMemoryPool();
  BOLT_CHECK_NOT_NULL(pool);
  return pool->shared_from_this();
}

void SparkListenableArbitratorContext::installAutomaticReclaimSpill() {
  BOLT_CHECK_NOT_NULL(holder_);
  sparksql::SpillerPtr spiller = std::make_shared<AutomaticReclaimSpiller>(
      [this](int64_t size) { return spillFixedSize(size); });
  holder_->appendSpiller(spiller);
}

SparkListenableArbitratorContextStats SparkListenableArbitratorContext::stats()
    const {
  std::lock_guard<std::mutex> lock(statsMutex_);
  return stats_;
}

void SparkListenableArbitratorContext::initializeExecutionMemoryPool(
    int64_t memoryLimitBytes,
    int64_t minMemoryMaxWaitMs,
    int32_t maxTaskNumber) {
  BOLT_CHECK_GT(memoryLimitBytes, 0);
  BOLT_CHECK_GT(maxTaskNumber, 0);
  if (!sparksql::ExecutionMemoryPool::inited()) {
    sparksql::ExecutionMemoryPool::init(
        true, memoryLimitBytes, maxTaskNumber, {}, minMemoryMaxWaitMs);
    return;
  }
  BOLT_CHECK_GE(
      sparksql::ExecutionMemoryPool::instance()->maxTaskNumber(),
      maxTaskNumber,
      "ExecutionMemoryPool is already initialized with maxTaskNumber {}, "
      "which is smaller than requested maxTaskNumber {}",
      sparksql::ExecutionMemoryPool::instance()->maxTaskNumber(),
      maxTaskNumber);
  sparksql::ExecutionMemoryPool::testingResetPoolSize(memoryLimitBytes);
}

int64_t SparkListenableArbitratorContext::spillFixedSize(int64_t size) {
  BOLT_CHECK_NOT_NULL(holder_);
  BOLT_CHECK_GE(size, 0);
  if (size == 0) {
    return 0;
  }

  const auto start = std::chrono::steady_clock::now();
  {
    std::lock_guard<std::mutex> lock(statsMutex_);
    ++stats_.automaticSpillTriggers;
    stats_.automaticSpillRequestedBytes += static_cast<uint64_t>(size);
  }

  auto manager = holder_->getManager();
  BOLT_CHECK_NOT_NULL(manager);
  auto aggregatePool = manager->getAggregateMemoryPool();
  BOLT_CHECK_NOT_NULL(aggregatePool);
  MemoryReclaimer::Stats reclaimStats;
  uint64_t reclaimed = 0;
  {
    ScopedMemoryArbitrationContext arbitrationContext{aggregatePool.get()};
    reclaimed = aggregatePool->reclaim(
        static_cast<uint64_t>(size),
        static_cast<uint64_t>(options_.minMemoryMaxWaitMs),
        reclaimStats);
  }
  {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.automaticSpillReclaimedBytes += reclaimed;
  }
  const auto shrunken = manager->shrink(size);
  {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.automaticSpillShrunkenBytes += static_cast<uint64_t>(shrunken);
  }
  const auto remaining = size - shrunken;
  if (remaining <= 0) {
    const auto elapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    {
      std::lock_guard<std::mutex> lock(statsMutex_);
      stats_.automaticSpillReturnedBytes += static_cast<uint64_t>(shrunken);
      stats_.automaticSpillTimeUs += elapsedUs;
    }
    return shrunken;
  }

  const auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
  {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.automaticSpillReturnedBytes += static_cast<uint64_t>(shrunken);
    stats_.automaticSpillTimeUs += elapsedUs;
  }
  return shrunken;
}

} // namespace bytedance::bolt::memory::bm
