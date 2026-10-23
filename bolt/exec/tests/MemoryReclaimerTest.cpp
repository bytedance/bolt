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

#include "bolt/exec/MemoryReclaimer.h"

#include <functional>
#include "bolt/buffer/Buffer.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::memory;

namespace {

class ReclaimTestOperator : public Operator {
 public:
  explicit ReclaimTestOperator(DriverCtx* ctx)
      : Operator(ctx, ROW({}, {}), 0, "0", "ReclaimTest") {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr /*input*/) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return true;
  }

  bool canReclaim() const override {
    return reclaimEnabled;
  }

  void reclaim(
      uint64_t /*targetBytes*/,
      memory::MemoryReclaimer::Stats& /*stats*/) override {
    onReclaim();
  }

  std::unique_ptr<memory::MemoryReclaimer> makeReclaimer() {
    return Operator::MemoryReclaimer::create(operatorCtx()->driverCtx(), this);
  }

  bool reclaimEnabled{true};
  std::function<void()> onReclaim;
};

class ExternalPoolReclaimTestOperator : public ReclaimTestOperator {
 public:
  explicit ExternalPoolReclaimTestOperator(DriverCtx* ctx)
      : ReclaimTestOperator(ctx),
        externalPool_(pool()->parent()->addLeafChild("external")) {}

  memory::MemoryPool* externalReclaimPool() const override {
    return externalPool_.get();
  }

 private:
  const std::shared_ptr<memory::MemoryPool> externalPool_;
};

} // namespace

class MemoryReclaimerTest : public OperatorTestBase {
 protected:
  MemoryReclaimerTest() : pool_(memory::memoryManager()->addLeafPool()) {
    const auto seed =
        std::chrono::system_clock::now().time_since_epoch().count();
    rng_.seed(seed);
    LOG(INFO) << "Random seed: " << seed;

    rowType_ = ROW({"c0", "c1"}, {{INTEGER(), VARCHAR()}});
    VectorFuzzer fuzzer{{}, pool_.get()};

    std::vector<RowVectorPtr> values = {fuzzer.fuzzRow(rowType_)};
    core::PlanFragment fakePlanFragment;
    const core::PlanNodeId id{"0"};
    fakePlanFragment.planNode = std::make_shared<core::ValuesNode>(id, values);

    fakeTask_ = Task::create(
        "MemoryReclaimerTest",
        std::move(fakePlanFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        Task::ExecutionMode::kParallel);
  }

  void SetUp() override {}

  void TearDown() override {}

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(4)};

  const std::shared_ptr<memory::MemoryPool> pool_;
  RowTypePtr rowType_;
  std::shared_ptr<Task> fakeTask_;
  folly::Random::DefaultGenerator rng_;
};

TEST_F(MemoryReclaimerTest, enterArbitrationTest) {
  for (const auto& underDriverContext : {false, true}) {
    SCOPED_TRACE(fmt::format("underDriverContext: {}", underDriverContext));

    auto reclaimer = exec::MemoryReclaimer::create();
    auto driver = Driver::testingCreate(
        std::make_unique<DriverCtx>(fakeTask_, 0, 0, 0, 0));
    fakeTask_->testingIncrementThreads();
    if (underDriverContext) {
      driver->state().setThread();
      ScopedDriverThreadContext scopedDriverThreadCtx{driver->driverCtx()};
      reclaimer->enterArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_TRUE(driver->state().suspended());
      reclaimer->leaveArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().suspended());
    } else {
      reclaimer->enterArbitration();
      ASSERT_FALSE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().suspended());
      reclaimer->leaveArbitration();
    }
  }
}

TEST_F(MemoryReclaimerTest, abortTest) {
  for (const auto& leafPool : {false, true}) {
    const std::string testName = fmt::format("leafPool: {}", leafPool);
    SCOPED_TRACE(testName);
    auto rootPool = memory::memoryManager()->addRootPool(
        testName, kMaxMemory, exec::MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    if (leafPool) {
      auto leafPool = rootPool->addLeafChild(
          "leafAbortTest", true, exec::MemoryReclaimer::create());
      try {
        BOLT_FAIL("abortTest error");
      } catch (const BoltRuntimeError& e) {
        leafPool->abort(std::current_exception());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(leafPool->aborted());
    } else {
      auto aggregatePool = rootPool->addAggregateChild(
          "nonLeafAbortTest", exec::MemoryReclaimer::create());
      try {
        BOLT_FAIL("abortTest error");
      } catch (const BoltRuntimeError& e) {
        aggregatePool->abort(std::current_exception());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(aggregatePool->aborted());
    }
  }
}

TEST_F(MemoryReclaimerTest, operatorWithoutExternalPool) {
  auto driver =
      Driver::testingCreate(std::make_unique<DriverCtx>(fakeTask_, 0, 0, 0, 0));
  ReclaimTestOperator op(driver->driverCtx());
  auto reclaimer = op.makeReclaimer();
  auto sibling = op.pool()->parent()->addLeafChild("unrelated");
  ASSERT_TRUE(sibling->maybeReserve(8 << 20));
  const auto siblingReservation = sibling->reservedBytes();
  fakeTask_->requestPause().wait();
  ScopedMemoryArbitrationContext arbitrationContext;

  // Exercise the default nullptr hook. Unrelated leaves must not contribute
  // to either the candidate estimate or the reclaimed byte count.
  for (bool releaseReservation : {false, true}) {
    SCOPED_TRACE(releaseReservation);
    auto allocation = AlignedBuffer::allocate<char>(1 << 20, op.pool());
    ASSERT_TRUE(op.pool()->maybeReserve(8 << 20));
    const auto before = op.pool()->reservedBytes();
    uint64_t candidate{0};
    ASSERT_TRUE(reclaimer->reclaimableBytes(*op.pool(), candidate));
    EXPECT_EQ(candidate, before);

    op.reclaimEnabled = false;
    ASSERT_FALSE(reclaimer->reclaimableBytes(*op.pool(), candidate));
    EXPECT_EQ(candidate, 0);
    memory::MemoryReclaimer::Stats stats;
    EXPECT_EQ(reclaimer->reclaim(op.pool(), 0, 0, stats), 0);
    EXPECT_EQ(op.pool()->reservedBytes(), before);
    op.reclaimEnabled = true;

    op.onReclaim = [&]() {
      allocation.reset();
      if (releaseReservation) {
        op.pool()->release();
      }
    };
    const auto reclaimed = reclaimer->reclaim(op.pool(), 0, 0, stats);
    EXPECT_EQ(reclaimed, before - op.pool()->reservedBytes());
    EXPECT_EQ(stats.reclaimedBytes, reclaimed);
    if (releaseReservation) {
      EXPECT_EQ(reclaimed, before);
    } else {
      // The legacy path does not release idle reservation on the operator's
      // behalf. Adding an optional external pool must preserve this behavior.
      EXPECT_GT(op.pool()->reservedBytes(), 0);
      EXPECT_LT(reclaimed, before);
    }
    EXPECT_EQ(sibling->reservedBytes(), siblingReservation);
    op.pool()->release();
  }
  uint64_t candidate{1};
  EXPECT_TRUE(reclaimer->reclaimableBytes(*op.pool(), candidate));
  EXPECT_EQ(candidate, 0);
  sibling->release();
}

TEST_F(MemoryReclaimerTest, operatorWithExternalPool) {
  auto driver =
      Driver::testingCreate(std::make_unique<DriverCtx>(fakeTask_, 0, 0, 0, 0));
  ExternalPoolReclaimTestOperator op(driver->driverCtx());
  auto reclaimer = op.makeReclaimer();
  auto* external = op.externalReclaimPool();
  fakeTask_->requestPause().wait();
  ScopedMemoryArbitrationContext arbitrationContext;
  for (bool allocateFromOperator : {false, true}) {
    SCOPED_TRACE(allocateFromOperator);
    BufferPtr allocation;
    if (allocateFromOperator) {
      allocation = AlignedBuffer::allocate<char>(1 << 20, op.pool());
      ASSERT_TRUE(op.pool()->maybeReserve(8 << 20));
    }
    auto externalAllocation = AlignedBuffer::allocate<char>(2 << 20, external);
    ASSERT_TRUE(external->maybeReserve(8 << 20));
    const auto before = op.pool()->reservedBytes() + external->reservedBytes();
    uint64_t candidate{0};
    ASSERT_TRUE(reclaimer->reclaimableBytes(*op.pool(), candidate));
    EXPECT_EQ(candidate, before);
    EXPECT_GT(candidate, 0);

    op.onReclaim = [&]() {
      allocation.reset();
      externalAllocation.reset();
    };
    memory::MemoryReclaimer::Stats stats;
    EXPECT_EQ(reclaimer->reclaim(op.pool(), 0, 0, stats), before);
    EXPECT_EQ(stats.reclaimedBytes, before);
    EXPECT_EQ(op.pool()->reservedBytes(), 0);
    EXPECT_EQ(external->reservedBytes(), 0);
  }
}

TEST_F(MemoryReclaimerTest, externalPoolGrowthOffsetsReclaimedBytes) {
  auto driver =
      Driver::testingCreate(std::make_unique<DriverCtx>(fakeTask_, 0, 0, 0, 0));
  ExternalPoolReclaimTestOperator op(driver->driverCtx());
  auto reclaimer = op.makeReclaimer();
  auto* external = op.externalReclaimPool();
  auto allocation = AlignedBuffer::allocate<char>(1 << 20, op.pool());
  BufferPtr externalAllocation;
  const auto before = op.pool()->reservedBytes();
  op.onReclaim = [&]() {
    allocation.reset();
    externalAllocation = AlignedBuffer::allocate<char>(8 << 20, external);
  };
  fakeTask_->requestPause().wait();
  ScopedMemoryArbitrationContext arbitrationContext;
  memory::MemoryReclaimer::Stats stats;
  EXPECT_EQ(reclaimer->reclaim(op.pool(), 0, 0, stats), 0);
  EXPECT_EQ(stats.reclaimedBytes, 0);
  EXPECT_EQ(op.pool()->reservedBytes(), 0);
  EXPECT_GT(external->reservedBytes(), before);
}

TEST(ReclaimableSectionGuard, basic) {
  tsan_atomic<bool> nonReclaimableSection{false};
  {
    memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
    ASSERT_TRUE(nonReclaimableSection);
    {
      memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
      ASSERT_FALSE(nonReclaimableSection);
      {
        memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_TRUE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
      }
      ASSERT_FALSE(nonReclaimableSection);
    }
    ASSERT_TRUE(nonReclaimableSection);
  }
  ASSERT_FALSE(nonReclaimableSection);
  nonReclaimableSection = true;
  {
    memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
    ASSERT_FALSE(nonReclaimableSection);
    {
      memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
      ASSERT_TRUE(nonReclaimableSection);
      {
        memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_FALSE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_TRUE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
      }
      ASSERT_TRUE(nonReclaimableSection);
    }
    ASSERT_FALSE(nonReclaimableSection);
  }
  ASSERT_TRUE(nonReclaimableSection);
}
