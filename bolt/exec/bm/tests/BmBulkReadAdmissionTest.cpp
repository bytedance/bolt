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

#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/Task.h"

#include <limits>

namespace bytedance::bolt::exec::bm {
namespace {

using memory::sparksql::ExecutionMemoryPool;
constexpr int64_t kMiB = int64_t{1} << 20;
constexpr int64_t kTask = 101;

class TaskListener final : public memory::sparksql::AllocationListener {
 public:
  explicit TaskListener(ExecutionMemoryPool& tasks) : tasks_(tasks) {}

  int64_t allocationChanged(int64_t bytes) override {
    return bytes > 0 ? tasks_.acquireMemory(bytes, kTask)
                     : -tasks_.releaseMemory(-bytes, kTask);
  }

  int64_t getUsedBytes() override {
    return tasks_.getMemoryUsageForTask(kTask);
  }

 private:
  ExecutionMemoryPool& tasks_;
};

class BmBulkReadAdmissionTest : public BmRowContainerDiskIoTest {
 protected:
  void useTaskPool(int64_t baseBytes) {
    bufferManager_.reset();
    root_.reset();
    memory::sparksql::DynamicMemoryQuotaManagerOption option{};
    option.enable = true;
    option.quotaTriggerRatio = 0;
    option.rssMinRatio = 1000000;
    option.rssMaxRatio = 0;
    option.extendMinRatio = 6;
    option.extendMaxRatio = 6;
    option.extendScaleRatio = 1;
    option.sampleRatio = 1;
    option.sampleSize = std::numeric_limits<int64_t>::max();
    option.changeThresholdRatio = 0;
    option.logPrintFreq = 0;
    if (!ExecutionMemoryPool::inited()) {
      ExecutionMemoryPool::init(true, baseBytes, 1, option, 10000);
    } else {
      ExecutionMemoryPool::testingResetPoolSize(baseBytes);
    }
    tasks_ = ExecutionMemoryPool::instance();
    listener_ = std::make_shared<TaskListener>(*tasks_);
    registration_ =
        std::make_unique<memory::sparksql::ArbitratorFactoryRegister>(
            listener_);
    memory::MemoryManager::Options options;
    options.useMemoryPoolForGluten = true;
    options.arbitratorKind = registration_->getKind();
    manager_ = std::make_unique<memory::MemoryManager>(options);
    root_ = manager_->addRootPool(
        "bulk-read-admission",
        memory::kMaxMemory,
        memory::MemoryReclaimer::create());
    probe_ = root_->addLeafChild("probe");
    resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  }

  std::shared_ptr<Task> makeTask() {
    core::PlanFragment fragment;
    fragment.planNode = std::make_shared<core::ValuesNode>(
        "values", std::vector<RowVectorPtr>{makeInput()});
    auto queryCtx = core::QueryCtx::create();
    queryCtx->testingOverrideMemoryPool(root_);
    return Task::create(
        "Gluten_Stage_0_TID_101_ATTEMPT_0_VTID_0",
        std::move(fragment),
        0,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel);
  }

  void TearDown() override {
    bufferManager_.reset();
    probe_.reset();
    root_.reset();
    manager_.reset();
    registration_.reset();
    listener_.reset();
    if (tasks_) {
      tasks_->releaseAllMemoryForTask(kTask);
    }
  }

  std::shared_ptr<ExecutionMemoryPool> tasks_;
  memory::sparksql::AllocationListenerPtr listener_;
  std::unique_ptr<memory::sparksql::ArbitratorFactoryRegister> registration_;
  std::unique_ptr<memory::MemoryManager> manager_;
  std::shared_ptr<memory::MemoryPool> probe_;
};

TEST_F(BmBulkReadAdmissionTest, ProbeCannotBorrowAdditionalQuota) {
  useTaskPool(4 * kMiB);
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      memory::bm::MemoryTag::kTesting,
      4 * kMiB,
      4 * kMiB);
  storeAll(container, makeInput());
  const auto segment = container.spillActiveSegment();
  auto task = makeTask();
  DriverCtx driverCtx(task, 0, 0, 0, 0);
  ScopedDriverThreadContext driverContext(&driverCtx);
  ASSERT_FALSE(container.canBulkRead({&segment, 1}));
  EXPECT_EQ(tasks_->configuredPoolSize(), tasks_->poolSize());
  // The scoped guard is gone after canBulkRead returns. An unguarded probe
  // can borrow quota even while the same Driver context is still active.
  EXPECT_TRUE(bufferManager_->MaybeReserve(8 * kMiB));
  bufferManager_->ReleaseUnusedReservation();
  EXPECT_GT(tasks_->poolSize(), tasks_->configuredPoolSize());
}

TEST_F(BmBulkReadAdmissionTest, UnguardedAdmissionPreservesBorrowing) {
  useTaskPool(4 * kMiB);
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      memory::bm::MemoryTag::kTesting);
  storeAll(container, makeInput());
  const auto segment = container.spillActiveSegment();
  EXPECT_TRUE(container.canBulkRead({&segment, 1}));
  EXPECT_GT(tasks_->poolSize(), tasks_->configuredPoolSize());
}

TEST_F(BmBulkReadAdmissionTest, GuardCanReusePreviouslyBorrowedCapacity) {
  useTaskPool(4 * kMiB);
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      memory::bm::MemoryTag::kTesting,
      4 * kMiB,
      4 * kMiB);
  storeAll(container, makeInput());
  const auto segment = container.spillActiveSegment();
  ASSERT_TRUE(probe_->maybeReserve(16 * kMiB));
  probe_->release();
  // Like HashBuild, the guard does not revoke historical root capacity.
  auto task = makeTask();
  DriverCtx driverCtx(task, 0, 0, 0, 0);
  ScopedDriverThreadContext driverContext(&driverCtx);
  EXPECT_TRUE(container.canBulkRead({&segment, 1}));
}

} // namespace
} // namespace bytedance::bolt::exec::bm
