#include "bolt/common/memory/bm/benchmark/SparkListenableArbitratorContext.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/sparksql/ConfigurationResolver.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"

#include <atomic>
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

uint64_t SparkListenableArbitratorContext::reclaimThroughArbitrator(
    uint64_t targetBytes) {
  BOLT_CHECK_NOT_NULL(holder_);
  return holder_->getManager()->getMemoryManager()->shrinkPools(targetBytes);
}

} // namespace bytedance::bolt::memory::bm
