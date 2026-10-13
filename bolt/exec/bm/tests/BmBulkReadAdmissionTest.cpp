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
    memory::sparksql::DynamicMemoryQuotaManagerOption option{
        .enable = true,
        .quotaTriggerRatio = 0,
        .rssMinRatio = 1000000,
        .rssMaxRatio = 0,
        .extendMinRatio = 6,
        .extendMaxRatio = 6,
        .extendScaleRatio = 1,
        .sampleRatio = 1,
        .sampleSize = std::numeric_limits<int64_t>::max(),
        .changeThresholdRatio = 0,
        .logPrintFreq = 0};
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
      4 * kMiB,
      kTask);
  storeAll(container, makeInput());
  const auto segment = container.spillActiveSegment();
  ASSERT_FALSE(container.canBulkRead({&segment, 1}));
  EXPECT_EQ(tasks_->configuredPoolSize(), tasks_->poolSize());
  // The scoped guard is gone after canBulkRead returns. An unguarded probe
  // can borrow quota using the same real arbitrator and task pool.
  EXPECT_TRUE(bufferManager_->MaybeReserve(8 * kMiB));
  bufferManager_->ReleaseUnusedReservation();
  EXPECT_GT(tasks_->poolSize(), tasks_->configuredPoolSize());
}

TEST_F(BmBulkReadAdmissionTest, MissingTaskIdPreservesBorrowing) {
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
      4 * kMiB,
      kTask);
  storeAll(container, makeInput());
  const auto segment = container.spillActiveSegment();
  ASSERT_TRUE(probe_->maybeReserve(16 * kMiB));
  probe_->release();
  // Like HashBuild, the guard does not revoke historical root capacity.
  EXPECT_TRUE(container.canBulkRead({&segment, 1}));
}

} // namespace
} // namespace bytedance::bolt::exec::bm
