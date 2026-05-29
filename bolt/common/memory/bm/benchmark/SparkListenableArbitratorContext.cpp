#include "bolt/common/memory/bm/benchmark/SparkListenableArbitratorContext.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/sparksql/ConfigurationResolver.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"

#include <glog/logging.h>

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

void initializeExecutionMemoryPool(
    int64_t memoryLimitBytes,
    int64_t minMemoryMaxWaitMs) {
  BOLT_CHECK_GT(memoryLimitBytes, 0);
  if (!sparksql::ExecutionMemoryPool::inited()) {
    sparksql::ExecutionMemoryPool::init(
        true, memoryLimitBytes, 1, {}, minMemoryMaxWaitMs);
    return;
  }
  sparksql::ExecutionMemoryPool::testingResetPoolSize(memoryLimitBytes);
}

class AutomaticReclaimSpiller final : public sparksql::Spiller {
 public:
  explicit AutomaticReclaimSpiller(std::function<int64_t(int64_t)> spill)
      : spill_(std::move(spill)) {}

  int64_t spill(sparksql::MemoryTargetWeakPtr /*self*/, int64_t size) override {
    if (size <= 0) {
      return 0;
    }
    VLOG(1) << "BM sort automatic spiller invoked, requested_bytes=" << size;
    const auto reclaimed = spill_(size);
    VLOG(1) << "BM sort automatic spiller finished, requested_bytes=" << size
            << ", returned_bytes=" << reclaimed;
    return reclaimed;
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
  initializeExecutionMemoryPool(
      options_.memoryLimitBytes, options_.minMemoryMaxWaitMs);

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
  sparksql::SpillerPtr spiller =
      std::make_shared<AutomaticReclaimSpiller>([this](int64_t size) {
        return spillFixedSize(size);
      });
  holder_->appendSpiller(spiller);
  VLOG(1) << "BM sort installed automatic reclaim spiller";
}

SparkListenableArbitratorContextStats
SparkListenableArbitratorContext::stats() const {
  return stats_;
}

int64_t SparkListenableArbitratorContext::spillFixedSize(int64_t size) {
  BOLT_CHECK_NOT_NULL(holder_);
  BOLT_CHECK_GE(size, 0);
  if (size == 0) {
    return 0;
  }

  const auto start = std::chrono::steady_clock::now();
  ++stats_.automaticSpillTriggers;
  stats_.automaticSpillRequestedBytes += static_cast<uint64_t>(size);

  auto manager = holder_->getManager();
  BOLT_CHECK_NOT_NULL(manager);
  VLOG(1) << "BM sort spillFixedSize begin, requested_bytes=" << size
          << ", aggregate_pool=" << manager->getAggregateMemoryPool()->toString();
  const auto shrunken = manager->shrink(size);
  stats_.automaticSpillShrunkenBytes += static_cast<uint64_t>(shrunken);
  const auto remaining = size - shrunken;
  VLOG(1) << "BM sort spillFixedSize after shrink, requested_bytes=" << size
          << ", shrunken_bytes=" << shrunken
          << ", remaining_bytes=" << remaining
          << ", aggregate_pool=" << manager->getAggregateMemoryPool()->toString();
  if (remaining <= 0) {
    stats_.automaticSpillReturnedBytes += static_cast<uint64_t>(shrunken);
    stats_.automaticSpillTimeUs +=
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    VLOG(1) << "BM sort spillFixedSize end after shrink, returned_bytes="
            << shrunken;
    return shrunken;
  }

  const auto reclaimed =
      manager->getMemoryManager()->shrinkPools(static_cast<uint64_t>(remaining));
  stats_.automaticSpillReclaimedBytes += reclaimed;
  const auto cappedReclaimed = std::min<uint64_t>(
      reclaimed, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
  const auto returned = shrunken + static_cast<int64_t>(cappedReclaimed);
  stats_.automaticSpillReturnedBytes += static_cast<uint64_t>(returned);
  stats_.automaticSpillTimeUs +=
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - start)
          .count();
  VLOG(1) << "BM sort spillFixedSize end, requested_bytes=" << size
          << ", shrunken_bytes=" << shrunken
          << ", reclaimed_bytes=" << reclaimed
          << ", returned_bytes=" << returned
          << ", aggregate_pool=" << manager->getAggregateMemoryPool()->toString();
  return returned;
}

} // namespace bytedance::bolt::memory::bm
