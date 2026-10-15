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

#include "bolt/exec/bm/BmStreamingWindowBuild.h"
#include "bolt/exec/bm/BmAggregateWindow.h"
#include "bolt/exec/bm/BmWindowPartition.h"

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryArbitrator.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/sparksql/ConfigurationResolver.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"

#include <folly/String.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>

using namespace bytedance::bolt::exec::test;

namespace bytedance::bolt::exec {
namespace {

class BmStreamingWindowBuildTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    bytedance::bolt::window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
  }
};

class BmStreamingWindowBuildDiskIoTest : public BmStreamingWindowBuildTest {
 public:
  void SetUp() override {
    try {
      memory::bm::diskIoScheduler().ensureReady();
    } catch (const std::exception& e) {
      if (std::string(e.what()).find("io_uring_queue_init failed") !=
          std::string::npos) {
        GTEST_SKIP() << e.what();
      }
      throw;
    }
    BmStreamingWindowBuildTest::SetUp();
  }
};

class PlainWindowPartition final : public WindowPartition {
 public:
  PlainWindowPartition()
      : WindowPartition(
            std::vector<column_index_t>{},
            std::vector<std::pair<column_index_t, core::SortOrder>>{}) {}
};

struct AutoSpillTestStats {
  uint64_t triggers{0};
  uint64_t requestedBytes{0};
  uint64_t reclaimedBytes{0};
  uint64_t shrunkenBytes{0};
  uint64_t returnedBytes{0};
};

int64_t nextAutoSpillTaskAttemptId() {
  static std::atomic<int64_t> id{0};
  return id++;
}

class AutoReclaimSpiller final : public memory::sparksql::Spiller {
 public:
  explicit AutoReclaimSpiller(std::function<int64_t(int64_t)> spill)
      : spill_(std::move(spill)) {}

  int64_t spill(memory::sparksql::MemoryTargetWeakPtr /*self*/, int64_t size)
      override {
    if (size <= 0) {
      return 0;
    }
    return spill_(size);
  }

  const std::set<memory::sparksql::SpillerPhase>& applicablePhases() override {
    return memory::sparksql::SpillerHelper::phaseSetAll();
  }

 private:
  std::function<int64_t(int64_t)> spill_;
};

class AutoSpillTestContext {
 public:
  AutoSpillTestContext(std::string name, int64_t memoryLimitBytes)
      : memoryLimitBytes_(memoryLimitBytes) {
    initializeExecutionMemoryPool(memoryLimitBytes_);
    taskMemoryManager_ = std::make_shared<memory::sparksql::TaskMemoryManager>(
        memory::sparksql::ExecutionMemoryPool::instance(),
        nextAutoSpillTaskAttemptId());

    std::unordered_map<std::string, std::string> sessionConf;
    sessionConf.emplace(
        memory::sparksql::ConfigurationResolver::kDynamicMemoryQuotaManager,
        "false");
    memory::sparksql::NativeMemoryManagerFactoryParam param{
        .name = std::move(name),
        .memoryIsolation = false,
        .conservativeTaskOffHeapMemorySize = memoryLimitBytes_,
        .overAcquiredRatio = 0,
        .taskMemoryManager = taskMemoryManager_,
        .sessionConf = std::move(sessionConf)};
    holder_ =
        memory::sparksql::NativeMemoryManagerFactory::contextInstance(param);
    BOLT_CHECK_NOT_NULL(holder_);

    memory::sparksql::SpillerPtr spiller = std::make_shared<AutoReclaimSpiller>(
        [this](int64_t size) { return spillFixedSize(size); });
    holder_->appendSpiller(spiller);
  }

  ~AutoSpillTestContext() {
    delete holder_;
    holder_ = nullptr;

    memory::sparksql::MemoryTargetBuilder::invalidate(
        taskMemoryManager_, false, memoryLimitBytes_);
    if (memory::sparksql::ExecutionMemoryPool::inited()) {
      memory::sparksql::ExecutionMemoryPool::instance()
          ->releaseAllMemoryForTask(taskMemoryManager_->getTaskAttemptId());
    }
  }

  std::shared_ptr<memory::MemoryPool> rootPool() const {
    BOLT_CHECK_NOT_NULL(holder_);
    auto pool = holder_->getManager()->getAggregateMemoryPool();
    BOLT_CHECK_NOT_NULL(pool);
    return pool->shared_from_this();
  }

  AutoSpillTestStats stats() const {
    std::lock_guard<std::mutex> lock(statsMutex_);
    return stats_;
  }

 private:
  static void initializeExecutionMemoryPool(int64_t memoryLimitBytes) {
    BOLT_CHECK_GT(memoryLimitBytes, 0);
    constexpr int64_t minMemoryMaxWaitMs = 10'000;
    constexpr int32_t maxTaskNumber = 1;
    if (!memory::sparksql::ExecutionMemoryPool::inited()) {
      memory::sparksql::ExecutionMemoryPool::init(
          true, memoryLimitBytes, maxTaskNumber, {}, minMemoryMaxWaitMs);
      return;
    }
    BOLT_CHECK_GE(
        memory::sparksql::ExecutionMemoryPool::instance()->maxTaskNumber(),
        maxTaskNumber);
    memory::sparksql::ExecutionMemoryPool::testingResetPoolSize(
        memoryLimitBytes);
  }

  int64_t spillFixedSize(int64_t size) {
    BOLT_CHECK_NOT_NULL(holder_);
    BOLT_CHECK_GT(size, 0);
    {
      std::lock_guard<std::mutex> lock(statsMutex_);
      ++stats_.triggers;
      stats_.requestedBytes += static_cast<uint64_t>(size);
    }

    auto manager = holder_->getManager();
    BOLT_CHECK_NOT_NULL(manager);
    auto aggregatePool = manager->getAggregateMemoryPool();
    BOLT_CHECK_NOT_NULL(aggregatePool);

    memory::MemoryReclaimer::Stats reclaimStats;
    uint64_t reclaimed = 0;
    {
      memory::ScopedMemoryArbitrationContext arbitrationContext{
          aggregatePool.get()};
      reclaimed = aggregatePool->reclaim(
          static_cast<uint64_t>(size), 10'000, reclaimStats);
    }
    const auto shrunken = manager->shrink(size);

    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.reclaimedBytes += reclaimed;
    stats_.shrunkenBytes += static_cast<uint64_t>(shrunken);
    stats_.returnedBytes += static_cast<uint64_t>(shrunken);
    return shrunken;
  }

  const int64_t memoryLimitBytes_;
  mutable std::mutex statsMutex_;
  AutoSpillTestStats stats_;
  std::shared_ptr<memory::sparksql::TaskMemoryManager> taskMemoryManager_;
  memory::sparksql::BoltMemoryManagerHolder* holder_{nullptr};
};

std::shared_ptr<memory::bm::BufferManager> createWindowTestBufferManager(
    memory::MemoryPool* root,
    const std::string& name) {
  memory::bm::BufferManagerConfig config;
  config.poolName = name;
  config.spillStoreConfig.fileAllocatorConfig =
      memory::bm::test::ValidConfigWithDirectory(
          memory::bm::test::UniqueTempDir(name));
  return memory::bm::BufferManager::Create(*root, std::move(config));
}

std::shared_ptr<Task> runBmStreamingWindow(
    const core::PlanNodePtr& plan,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    const std::string& spillDirectory) {
  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  return AssertQueryBuilder(plan, duckDbQueryRunner)
      .config(core::QueryConfig::kBufferManagerEnabled, "true")
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWindowSpillEnabled, "true")
      .spillDirectory(spillDirectory)
      .assertResults(duckDbSql);
}

std::shared_ptr<Task> makeDirectWindowTask(
    const core::PlanNodePtr& plan,
    const std::string& spillDirectory,
    std::shared_ptr<memory::MemoryPool> queryPool = nullptr) {
  core::PlanFragment fragment;
  fragment.planNode = plan;
  auto queryCtx = core::QueryCtx::create();
  queryCtx->testingOverrideConfigUnsafe({
      {core::QueryConfig::kBufferManagerEnabled, "true"},
      {core::QueryConfig::kSpillEnabled, "true"},
      {core::QueryConfig::kWindowSpillEnabled, "true"},
      {core::QueryConfig::kBmStreamingWindowBuildEnabled, "true"},
  });
  if (queryPool != nullptr) {
    queryCtx->testingOverrideMemoryPool(std::move(queryPool));
  }
  return Task::create(
      "bm-streaming-window-direct",
      std::move(fragment),
      0,
      std::move(queryCtx),
      Task::ExecutionMode::kParallel,
      Consumer{},
      0,
      common::SpillDiskOptions{spillDirectory, true});
}

struct AutoSpillWindowRunStats {
  AutoSpillTestStats autoSpill;
  memory::bm::BufferManagerStats bufferManager;
  uint64_t outputReclaimedBytes{0};
};

RowVectorPtr expectedSortWindowResults(
    memory::MemoryPool* pool,
    const RowVectorPtr& data,
    const std::vector<std::string>& functions) {
  auto plan = PlanBuilder().values({data}).window(functions).planNode();
  TestWindowInjection windowInjection(WindowBuildType::kSortWindowBuild);
  return AssertQueryBuilder(plan).copyResults(pool);
}

void runAutoSpillWindow(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& input,
    const RowVectorPtr& expected,
    AutoSpillTestContext& context,
    AutoSpillWindowRunStats* runStats) {
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);
  ASSERT_NE(nullptr, expected);
  ASSERT_NE(nullptr, runStats);

  auto spillDirectory = TempDirectoryPath::create();
  auto task =
      makeDirectWindowTask(plan, spillDirectory->path, context.rootPool());
  ASSERT_NE(nullptr, task->bufferManager());

  auto driverCtx = std::make_unique<DriverCtx>(task, 0, 0, 0, 0);
  auto* driverCtxRaw = driverCtx.get();
  auto driver = Driver::testingCreate(std::move(driverCtx));
  task->testingIncrementThreads();
  driver->state().setThread();
  auto leaveDriverGuard =
      folly::makeGuard([&]() { task->leave(driver->state(), nullptr); });
  ScopedDriverThreadContext scopedDriverThreadContext{driver->driverCtx()};

  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  Window window(0, driverCtxRaw, windowNode);
  window.initialize();

  for (const auto& batch : input) {
    ASSERT_TRUE(window.needsInput());
    window.addInput(batch);
  }

  runStats->autoSpill = context.stats();
  EXPECT_GT(runStats->autoSpill.triggers, 0);
  EXPECT_GT(runStats->autoSpill.returnedBytes, 0);
  EXPECT_GT(task->bufferManager()->stats().spillWriteBytes, 0);

  window.noMoreInput();
  vector_size_t outputRows = 0;
  for (auto attempts = 0; attempts < 128 && !window.isFinished(); ++attempts) {
    Operator::NonReclaimableSectionGuard guard(&window);
    auto output = window.getOutput();
    if (output == nullptr) {
      continue;
    }

    runStats->outputReclaimedBytes += task->bufferManager()->Reclaim(
        task->bufferManager()->reclaimableBytes());

    ASSERT_LE(outputRows + output->size(), expected->size());
    for (auto row = 0; row < output->size(); ++row) {
      ASSERT_TRUE(output->equalValueAt(expected.get(), row, outputRows + row))
          << "Mismatch at output row " << outputRows + row;
    }
    outputRows += output->size();
  }

  EXPECT_EQ(expected->size(), outputRows);
  EXPECT_TRUE(window.isFinished());
  runStats->bufferManager = task->bufferManager()->stats();
  EXPECT_GT(runStats->bufferManager.spillReadCount, 0);
  EXPECT_GT(runStats->outputReclaimedBytes, 0);
}

std::string selectAllWithWindowFunctions(
    const RowVectorPtr& data,
    const std::vector<std::string>& functions) {
  auto rowType = asRowType(data->type());
  return fmt::format(
      "SELECT {}, {} FROM tmp",
      folly::join(", ", rowType->names()),
      folly::join(", ", functions));
}

std::shared_ptr<Task> runBmStreamingWindow(
    const RowVectorPtr& data,
    const core::PlanNodePtr& plan,
    const std::vector<std::string>& functions,
    DuckDbQueryRunner& duckDbQueryRunner,
    const std::string& spillDirectory) {
  return runBmStreamingWindow(
      plan,
      selectAllWithWindowFunctions(data, functions),
      duckDbQueryRunner,
      spillDirectory);
}

TEST_F(BmStreamingWindowBuildTest, rowNumber) {
  const vector_size_t size = 100;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 5; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      });

  createDuckDbTable({data});

  auto plan =
      PlanBuilder()
          .values(split(data, 7))
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kBufferManagerEnabled, "true")
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWindowSpillEnabled, "true")
      .spillDirectory(spillDirectory->path)
      .assertResults(
          "SELECT *, row_number() over (partition by p order by s) FROM tmp");
}

TEST_F(
    BmStreamingWindowBuildTest,
    doesNotExposePendingPartitionWhileCurrentPartitionLives) {
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5}),
          makeFlatVector<int16_t>({0, 0, 1, 1, 2, 2}),
          makeFlatVector<int32_t>({0, 1, 0, 1, 0, 1}),
      });
  auto plan =
      PlanBuilder(pool())
          .values({data})
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  auto root = memory::memoryManager()->addRootPool(
      "bm-streaming-window-single-partition-state",
      64 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager =
      createWindowTestBufferManager(root.get(), root->name() + "-bm");
  tsan_atomic<bool> nonReclaimableSection{false};
  window::BmStreamingWindowBuild build(
      windowNode,
      pool(),
      nullptr,
      &nonReclaimableSection,
      0,
      false,
      std::move(bufferManager));

  build.addInput(data);
  ASSERT_TRUE(build.hasNextPartition());

  auto first = build.nextPartition();
  ASSERT_EQ(2, first->numRows());
  EXPECT_FALSE(build.hasNextPartition());
  EXPECT_FALSE(build.needsInput());

  first.reset();
  ASSERT_TRUE(build.hasNextPartition());
  auto second = build.nextPartition();
  ASSERT_EQ(2, second->numRows());
  EXPECT_FALSE(build.hasNextPartition());

  second.reset();
  EXPECT_FALSE(build.hasNextPartition());
  EXPECT_TRUE(build.needsInput());
  build.noMoreInput();
  ASSERT_TRUE(build.hasNextPartition());
  auto third = build.nextPartition();
  ASSERT_EQ(2, third->numRows());
  third.reset();

  EXPECT_FALSE(build.hasNextPartition());
  EXPECT_FALSE(build.needsInput());
}

TEST_F(BmStreamingWindowBuildTest, acceptsIgnorePeerFlag) {
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({0, 1, 2}),
          makeFlatVector<int16_t>({0, 0, 0}),
          makeFlatVector<int32_t>({0, 1, 2}),
      });
  auto plan =
      PlanBuilder(pool())
          .values({data})
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  auto root = memory::memoryManager()->addRootPool(
      "bm-streaming-window-ignore-peer",
      64 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager =
      createWindowTestBufferManager(root.get(), root->name() + "-bm");
  tsan_atomic<bool> nonReclaimableSection{false};
  window::BmStreamingWindowBuild build(
      windowNode,
      pool(),
      nullptr,
      &nonReclaimableSection,
      0,
      false,
      std::move(bufferManager));

  EXPECT_NO_THROW(build.setIgnorePeer(true));
  EXPECT_NO_THROW(build.setIgnorePeer(false));
}

TEST_F(BmStreamingWindowBuildTest, nonAggregateFunctions) {
  const vector_size_t size = 96;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 24; }),
          makeFlatVector<int32_t>(
              size, [](auto row) { return (row % 24) / 3; }),
      });

  createDuckDbTable({data});

  std::vector<std::string> functions{
      "rank() over (partition by p order by s)",
      "dense_rank() over (partition by p order by s)",
      "percent_rank() over (partition by p order by s)",
      "cume_dist() over (partition by p order by s)",
      "ntile(4) over (partition by p order by s)",
      "lead(d, 1, -1) over (partition by p order by s)",
      "lag(d, 1, -1) over (partition by p order by s)",
      "first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)",
      "last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)"};
  auto plan = PlanBuilder()
                  .values(split(data, 11))
                  .streamingWindow(functions)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kBufferManagerEnabled, "true")
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWindowSpillEnabled, "true")
      .spillDirectory(spillDirectory->path)
      .assertResults(
          fmt::format("SELECT *, {} FROM tmp", folly::join(", ", functions)));
}

TEST_F(BmStreamingWindowBuildTest, ignoreNullsValueFunctions) {
  const vector_size_t size = 48;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row; },
              [](auto row) { return row % 5 == 0 || row % 11 == 0; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 12; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 12; }),
      });

  createDuckDbTable({data});

  std::vector<std::string> functions{
      "first_value(d IGNORE NULLS) over (partition by p order by s rows between unbounded preceding and unbounded following)",
      "last_value(d IGNORE NULLS) over (partition by p order by s rows between unbounded preceding and unbounded following)",
      "nth_value(d, 2 IGNORE NULLS) over (partition by p order by s rows between unbounded preceding and unbounded following)",
      "lead(d, 1, -1 IGNORE NULLS) over (partition by p order by s)",
      "lag(d, 1, -1 IGNORE NULLS) over (partition by p order by s)"};
  auto plan = PlanBuilder()
                  .values(split(data, 7))
                  .streamingWindow(functions)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kBufferManagerEnabled, "true")
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWindowSpillEnabled, "true")
      .spillDirectory(spillDirectory->path)
      .assertResults(
          fmt::format("SELECT *, {} FROM tmp", folly::join(", ", functions)));
}

TEST_F(BmStreamingWindowBuildTest, aggregate) {
  const vector_size_t size = 120;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row % 17; },
              [](auto row) { return row % 19 == 0; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 6; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 6; }),
      });

  createDuckDbTable({data});

  auto runningSum =
      "sum(d) over (partition by p order by s rows between unbounded preceding and current row)";
  auto boundedSum =
      "sum(d) over (partition by p order by s rows between 1 preceding and 1 following)";
  auto plan = PlanBuilder()
                  .values(split(data, 9))
                  .streamingWindow({runningSum, boundedSum})
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kBufferManagerEnabled, "true")
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWindowSpillEnabled, "true")
      .spillDirectory(spillDirectory->path)
      .assertResults(
          fmt::format("SELECT *, {}, {} FROM tmp", runningSum, boundedSum));
}

TEST_F(BmStreamingWindowBuildTest, sharesEntirePartitionAggregateInput) {
  constexpr vector_size_t size = 512;
  auto data = makeRowVector(
      {"d0", "d1", "d2", "d3", "p", "s"},
      {
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row % 17; },
              [](auto row) { return row % 19 == 0; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row * 3; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 31; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 43; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  const std::string frame =
      "partition by p order by s rows between unbounded preceding and "
      "unbounded following";
  const std::vector<std::string> functions{
      fmt::format("count(d0) over ({})", frame),
      fmt::format("sum(d1) over ({})", frame),
      fmt::format("min(d2) over ({})", frame),
      fmt::format("max(d3) over ({})", frame)};
  auto plan = PlanBuilder()
                  .values(split(data, 8))
                  .streamingWindow(functions)
                  .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);
  for (const auto& function : windowNode->windowFunctions()) {
    EXPECT_EQ(
        core::WindowNode::BoundType::kUnboundedPreceding,
        function.frame.startType);
    EXPECT_EQ(
        core::WindowNode::BoundType::kUnboundedFollowing,
        function.frame.endType);
  }

#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
#endif
  auto spillDirectory = TempDirectoryPath::create();
  runBmStreamingWindow(
      plan,
      fmt::format("SELECT *, {} FROM tmp", folly::join(", ", functions)),
      duckDbQueryRunner_,
      spillDirectory->path);

#ifndef NDEBUG
  const auto stats = window::bmAggregateWindowTestStats();
  EXPECT_EQ(0, stats.sharedScanCalls);
  EXPECT_EQ(0, stats.materializedRows);
  EXPECT_EQ(size, stats.inputAggregationRows);
  EXPECT_EQ(1, stats.inputAggregationPartitions);
  EXPECT_EQ(functions.size(), stats.inputAggregationFunctions);
#endif
}

TEST_F(BmStreamingWindowBuildTest, unsupportedInputAggregateUsesSharedScan) {
  constexpr vector_size_t size = 2053;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {makeFlatVector<int64_t>(size, [](auto row) { return row % 17; }),
       makeFlatVector<int32_t>(size, [](auto /*row*/) { return 0; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row; })});
  createDuckDbTable({data});
  const std::string frame =
      "partition by p order by s rows between unbounded preceding and "
      "unbounded following";
  for (const bool single : {true, false}) {
    SCOPED_TRACE(single);
    std::vector<std::string> functions;
    std::vector<std::string> expected;
    if (!single) {
      functions.push_back(fmt::format("sum(d) over ({})", frame));
      expected.push_back(functions.back());
    }
    functions.push_back(fmt::format("max_by(d, s) over ({})", frame));
    expected.push_back(fmt::format("arg_max(d, s) over ({})", frame));
    auto plan = PlanBuilder()
                    .values(split(data, 8))
                    .streamingWindow(functions)
                    .planNode();
#ifndef NDEBUG
    window::resetBmAggregateWindowTestStats();
#endif
    auto spillDirectory = TempDirectoryPath::create();
    runBmStreamingWindow(
        plan,
        fmt::format("SELECT *, {} FROM tmp", folly::join(", ", expected)),
        duckDbQueryRunner_,
        spillDirectory->path);
#ifndef NDEBUG
    const auto stats = window::bmAggregateWindowTestStats();
    EXPECT_EQ(0, stats.inputAggregationRows);
    EXPECT_EQ(1, stats.sharedScanCalls);
    EXPECT_EQ(functions.size(), stats.sharedScanFunctions);
    EXPECT_EQ(size, stats.materializedRows);
#endif
  }
}

TEST_F(
    BmStreamingWindowBuildTest,
    inputAggregateResultsAreIndependentPerPartition) {
  constexpr vector_size_t size = 4096;
  std::string value;
  auto data = makeRowVector(
      {"d", "v", "p", "s"},
      {makeFlatVector<int64_t>(
           size,
           [](auto row) { return row % 17; },
           [](auto row) { return row < 683 || row % 3 == 0; }),
       makeFlatVector<StringView>(
           size,
           [&value](auto row) {
             value = fmt::format("long-string-partition-{}-{}", row / 683, row);
             return StringView(value);
           },
           [](auto row) { return row < 683 || row % 5 == 0; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row / 683; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row; })});
  createDuckDbTable({data});
  const std::string frame =
      "partition by p order by s rows between unbounded preceding and "
      "unbounded following";
  for (const bool single : {true, false}) {
    SCOPED_TRACE(single);
    std::vector<std::string> functions{fmt::format("sum(d) over ({})", frame)};
    if (!single) {
      for (const auto* call :
           {"count(d)",
            "min(d)",
            "max(d)",
            "avg(d)",
            "max(v)",
            "count(1)",
            "count(*)"}) {
        functions.push_back(fmt::format("{} over ({})", call, frame));
      }
    }
    auto plan = PlanBuilder()
                    .values(split(data, 12))
                    .streamingWindow(functions)
                    .planNode();
#ifndef NDEBUG
    window::resetBmAggregateWindowTestStats();
#endif
    auto spillDirectory = TempDirectoryPath::create();
    runBmStreamingWindow(
        plan,
        fmt::format("SELECT *, {} FROM tmp", folly::join(", ", functions)),
        duckDbQueryRunner_,
        spillDirectory->path);
#ifndef NDEBUG
    const auto stats = window::bmAggregateWindowTestStats();
    EXPECT_EQ(size, stats.inputAggregationRows);
    EXPECT_EQ(6, stats.inputAggregationPartitions);
    EXPECT_EQ(6 * functions.size(), stats.inputAggregationFunctions);
    EXPECT_EQ(0, stats.sharedScanCalls);
    EXPECT_EQ(0, stats.materializedRows);
#endif
  }
}

TEST_F(
    BmStreamingWindowBuildDiskIoTest,
    inputAggregateInlineHourSurvivesWindowSpill) {
  constexpr vector_size_t kRows = 131072;
  for (const bool allNull : {false, true}) {
    SCOPED_TRACE(allNull);
    auto data = makeRowVector(
        {"hour", "payload", "id"},
        {makeFlatVector<std::string>(
             kRows,
             [](auto row) { return fmt::format("{:02}", row % 24); },
             [allNull](auto row) { return allNull || row % 7 == 0; }),
         makeFlatVector<std::string>(
             kRows, [](auto row) { return std::string(256, 'a' + row % 26); }),
         makeFlatVector<int64_t>(kRows, [](auto row) { return row; })});
    auto plan =
        PlanBuilder(pool())
            .values({data})
            .streamingWindow({
                // Match Spark's full-partition normalization of OVER().
                "max(hour) over (rows between unbounded preceding and unbounded following)",
            })
            .planNode();
    auto node = std::dynamic_pointer_cast<const core::WindowNode>(plan);
    ASSERT_NE(nullptr, node);
    ASSERT_EQ(
        core::WindowNode::BoundType::kUnboundedFollowing,
        node->windowFunctions().front().frame.endType);
    auto root = memory::memoryManager()->addRootPool(
        "bm-inline-hour-spill", 128 << 20, memory::MemoryReclaimer::create());
    auto directory = TempDirectoryPath::create();
    auto task = makeDirectWindowTask(plan, directory->path, root);
    auto config = task->queryCtx()->queryConfig().rawConfigsCopy();
    config[core::QueryConfig::kPreferredOutputBatchRows] = "8192";
    config[core::QueryConfig::kMaxOutputBatchRows] = "8192";
    task->queryCtx()->testingOverrideConfigUnsafe(std::move(config));
    auto driverCtx = std::make_unique<DriverCtx>(task, 0, 0, 0, 0);
    auto* context = driverCtx.get();
    auto driver = Driver::testingCreate(std::move(driverCtx));
    TestWindowInjection injection(WindowBuildType::kBmStreamingWindowBuild);
    Window window(0, context, node);
    window.initialize();
#ifndef NDEBUG
    window::resetBmAggregateWindowTestStats();
    window::resetBmWindowPartitionTestStats();
#endif
    window.addInput(data);
    // The physical rows span several 4 MiB row blocks. Finalize and spill them
    // before output, exercising cached results across chunk and batch
    // boundaries.
    memory::MemoryReclaimer::Stats reclaimStats;
    window.reclaim(1, reclaimStats);
    ASSERT_GT(task->bufferManager()->stats().spillWriteCount, 0);
    window.noMoreInput();
    // Select WindowRead under temporary admission pressure, then remove that
    // pressure so the unrelated resident re-pin reservation issue is excluded.
    auto pressurePool = root->addLeafChild("force-window-read");
    const auto pressureBytes = root->freeBytes() - (40 << 20);
    auto* pressure = pressurePool->allocate(pressureBytes);
    auto pressureGuard = folly::makeGuard([&] {
      if (pressure != nullptr) {
        pressurePool->free(pressure, pressureBytes);
      }
    });
    vector_size_t offset = 0;
    while (offset < kRows) {
      Operator::NonReclaimableSectionGuard guard(&window);
      auto output = window.getOutput();
      ASSERT_NE(nullptr, output);
      if (pressure != nullptr) {
        pressurePool->free(pressure, pressureBytes);
        pressure = nullptr;
      }
      ASSERT_LE(output->size(), 8192);
      for (vector_size_t row = 0; row < output->size(); ++row) {
        for (column_index_t column = 0; column < 3; ++column) {
          ASSERT_TRUE(output->childAt(column)->equalValueAt(
              data->childAt(column).get(), row, offset + row));
        }
        ASSERT_EQ(allNull, output->childAt(3)->isNullAt(row));
        if (!allNull) {
          ASSERT_EQ(
              "23",
              output->childAt(3)
                  ->asFlatVector<StringView>()
                  ->valueAt(row)
                  .str());
        }
      }
      offset += output->size();
    }
    EXPECT_EQ(kRows, offset);
    EXPECT_TRUE(window.isFinished());
#ifndef NDEBUG
    const auto stats = window::bmAggregateWindowTestStats();
    EXPECT_EQ(kRows, stats.inputAggregationRows);
    EXPECT_EQ(1, stats.inputAggregationPartitions);
    EXPECT_EQ(1, stats.inputAggregationFunctions);
    EXPECT_EQ(0, stats.materializedRows);
#endif
    EXPECT_GT(task->bufferManager()->stats().spillReadCount, 0);
#ifndef NDEBUG
    EXPECT_GT(window::bmWindowPartitionTestStats().loadRowsCalls, 1);
#endif
  }
}

TEST_F(BmStreamingWindowBuildTest, smallPartitionStringResultMemoryComparison) {
  constexpr vector_size_t kRows = 4096;
  auto values = makeFlatVector<std::string>(
      kRows, [](auto row) { return std::string(64, 'a' + row % 26); });
  auto data = makeRowVector(
      {"p", "v"},
      {makeFlatVector<int64_t>(kRows, [](auto row) { return row; }), values});
  auto plan =
      PlanBuilder(pool())
          .values({data})
          .streamingWindow({
              "max(v) over (partition by p rows between unbounded preceding and unbounded following)",
          })
          .planNode();
  auto node = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  for (const bool bm : {false, true}) {
    SCOPED_TRACE(bm);
    auto root = memory::memoryManager()->addRootPool(
        "window-small-partition-results",
        256 << 20,
        memory::MemoryReclaimer::create());
    auto directory = TempDirectoryPath::create();
    auto task = makeDirectWindowTask(plan, directory->path, root);
    auto config = task->queryCtx()->queryConfig().rawConfigsCopy();
    config[core::QueryConfig::kPreferredOutputBatchRows] = "128";
    config[core::QueryConfig::kMaxOutputBatchRows] = "128";
    task->queryCtx()->testingOverrideConfigUnsafe(std::move(config));
    auto driverCtx = std::make_unique<DriverCtx>(task, 0, 0, 0, 0);
    auto* context = driverCtx.get();
    auto driver = Driver::testingCreate(std::move(driverCtx));
    TestWindowInjection injection(
        bm ? WindowBuildType::kBmStreamingWindowBuild
           : WindowBuildType::kSpillableWindowBuild);
    Window window(0, context, node);
    window.initialize();
#ifndef NDEBUG
    window::resetBmAggregateWindowTestStats();
#endif
    window.addInput(data);
    // Record the queue before a small output batch can drain it. This is a
    // diagnostic comparison, not a regression bound or a claim of bounded
    // memory.
    RecordProperty(
        bm ? "bm_input_phase_bytes" : "spillable_input_phase_bytes",
        root->currentBytes());
    window.noMoreInput();
    vector_size_t offset = 0;
    while (offset < kRows) {
      Operator::NonReclaimableSectionGuard guard(&window);
      auto output = window.getOutput();
      ASSERT_NE(nullptr, output);
      ASSERT_LE(output->size(), 128);
      for (vector_size_t row = 0; row < output->size(); ++row) {
        ASSERT_TRUE(output->childAt(0)->equalValueAt(
            data->childAt(0).get(), row, offset + row));
        ASSERT_TRUE(
            output->childAt(1)->equalValueAt(values.get(), row, offset + row));
        ASSERT_TRUE(
            output->childAt(2)->equalValueAt(values.get(), row, offset + row));
      }
      offset += output->size();
    }
    EXPECT_EQ(kRows, offset);
    EXPECT_TRUE(window.isFinished());
    RecordProperty(
        bm ? "bm_peak_bytes" : "spillable_peak_bytes", root->peakBytes());
#ifndef NDEBUG
    if (bm) {
      EXPECT_EQ(
          kRows, window::bmAggregateWindowTestStats().inputAggregationRows);
      EXPECT_EQ(
          kRows,
          window::bmAggregateWindowTestStats().inputAggregationPartitions);
    }
#endif
  }
}

TEST_F(BmStreamingWindowBuildDiskIoTest, inputAggregateSurvivesExplicitSpill) {
  constexpr vector_size_t size = 16384;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeFlatVector<int32_t>(size, [](auto /*row*/) { return 0; }),
       makeFlatVector<int32_t>(size, [](auto row) { return row; })});
  auto plan =
      PlanBuilder(pool())
          .values({data})
          .streamingWindow(
              {"sum(d) over (partition by p rows between unbounded preceding and unbounded following)"})
          .planNode();
  auto node = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, node);
  auto root = memory::memoryManager()->addRootPool(
      "bm-input-aggregation-spill",
      64 << 20,
      memory::MemoryReclaimer::create());
  auto manager = createWindowTestBufferManager(root.get(), root->name());
  tsan_atomic<bool> nonReclaimable{false};
  common::SpillConfig spillConfig;
  window::BmStreamingWindowBuild build(
      node, pool(), &spillConfig, &nonReclaimable, 0, false, manager);
  HashStringAllocator allocator(pool());
  std::vector<std::unique_ptr<WindowFunction>> functions;
  functions.push_back(window::createBmAggregateWindowFunction(
      "sum",
      {{BIGINT(), nullptr, 0}},
      BIGINT(),
      false,
      pool(),
      &allocator,
      core::QueryConfig({})));
#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
#endif
  build.setInputAggregation(window::prepareBmAggregateWindowFunctions(
      node->windowFunctions(), functions, pool()));
  for (const auto& input : split(data, 8)) {
    build.addInput(input);
    build.spill();
  }
  build.noMoreInput();
  auto partition = build.nextPartition();
  functions.front()->resetPartition(partition.get());
  auto starts = AlignedBuffer::allocate<vector_size_t>(17, pool());
  auto ends = AlignedBuffer::allocate<vector_size_t>(17, pool());
  std::fill_n(starts->asMutable<vector_size_t>(), 17, 0);
  std::fill_n(ends->asMutable<vector_size_t>(), 17, size - 1);
  auto result = BaseVector::create(BIGINT(), 17, pool());
  const auto readsBefore = manager->stats().spillReadCount;
  functions.front()->apply(
      nullptr, nullptr, starts, ends, SelectivityVector(17), 0, result);
  for (vector_size_t i = 0; i < 17; ++i) {
    EXPECT_EQ(
        int64_t{size} * (size - 1) / 2,
        result->asFlatVector<int64_t>()->valueAt(i));
  }
  EXPECT_EQ(readsBefore, manager->stats().spillReadCount);
#ifndef NDEBUG
  EXPECT_EQ(size, window::bmAggregateWindowTestStats().inputAggregationRows);
  EXPECT_EQ(0, window::bmAggregateWindowTestStats().materializedRows);
#endif
  EXPECT_GT(build.windowSpilledStats()->spilledBytes, 0);
}

TEST_F(BmStreamingWindowBuildTest, sortNullAndRankingFunctionMatrix) {
  constexpr vector_size_t size = 72;
  auto data = makeRowVector(
      {"d", "p", "s", "t"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(
              size,
              [](auto row) { return row % 6; },
              [](auto row) { return row % 17 == 0; }),
          makeFlatVector<int32_t>(
              size,
              [](auto row) { return (row * 7) % 11; },
              [](auto row) { return row % 10 == 0; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return (row * 5) % 13; },
              [](auto row) { return row % 19 == 0; }),
      });

  createDuckDbTable({data});

  struct SortCase {
    std::vector<std::string> orderByClauses;
    std::string overClause;
  };
  const std::vector<SortCase> cases{
      {{"p NULLS FIRST",
        "s ASC NULLS LAST",
        "t DESC NULLS FIRST",
        "d NULLS FIRST"},
       "partition by p order by s asc nulls last, t desc nulls first, d"},
      {{"p NULLS FIRST",
        "s DESC NULLS FIRST",
        "t ASC NULLS LAST",
        "d NULLS FIRST"},
       "partition by p order by s desc nulls first, t asc nulls last, d"}};

  for (const auto& sortCase : cases) {
    SCOPED_TRACE(sortCase.overClause);
    std::vector<std::string> functions{
        fmt::format("row_number() over ({})", sortCase.overClause),
        fmt::format("rank() over ({})", sortCase.overClause),
        fmt::format("dense_rank() over ({})", sortCase.overClause),
        fmt::format("percent_rank() over ({})", sortCase.overClause),
        fmt::format("cume_dist() over ({})", sortCase.overClause),
        fmt::format("ntile(4) over ({})", sortCase.overClause)};
    auto plan = PlanBuilder()
                    .values(split(data, 9))
                    .orderBy(sortCase.orderByClauses, false)
                    .streamingWindow(functions)
                    .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    runBmStreamingWindow(
        data, plan, functions, duckDbQueryRunner_, spillDirectory->path);
  }
}

TEST_F(BmStreamingWindowBuildTest, valueFunctionNullAndFrameMatrix) {
  constexpr vector_size_t size = 80;
  auto data = makeRowVector(
      {"d", "p", "s", "v", "off", "def"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(
              size,
              [](auto row) { return row % 5; },
              [](auto row) { return row % 23 == 0; }),
          makeFlatVector<int32_t>(
              size,
              [](auto row) { return (row * 3) % 17; },
              [](auto row) { return row % 13 == 0; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row * 10 + 7; },
              [](auto row) { return row % 4 == 0 || row % 9 == 0; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
          makeFlatVector<int64_t>(size, [](auto row) { return -1 - row; }),
      });

  createDuckDbTable({data});

  const std::vector<std::string> orderByClauses{
      "p NULLS FIRST", "s DESC NULLS FIRST", "d NULLS FIRST"};
  const std::string overClause =
      "partition by p order by s desc nulls first, d";
  std::vector<std::string> functions{
      fmt::format(
          "first_value(v) over ({} rows between 2 preceding and 1 following)",
          overClause),
      fmt::format(
          "last_value(v) over ({} rows between unbounded preceding and current row)",
          overClause),
      fmt::format(
          "nth_value(v, off) over ({} rows between unbounded preceding and unbounded following)",
          overClause),
      fmt::format(
          "first_value(v IGNORE NULLS) over ({} rows between 2 preceding and 2 following)",
          overClause),
      fmt::format(
          "last_value(v IGNORE NULLS) over ({} rows between unbounded preceding and unbounded following)",
          overClause),
      fmt::format(
          "nth_value(v, off IGNORE NULLS) over ({} rows between unbounded preceding and unbounded following)",
          overClause),
      fmt::format("lead(v, off, def) over ({})", overClause),
      fmt::format("lag(v, off, def) over ({})", overClause),
      fmt::format("lead(v, off, def IGNORE NULLS) over ({})", overClause),
      fmt::format("lag(v, off, def IGNORE NULLS) over ({})", overClause)};

  auto plan = PlanBuilder()
                  .values(split(data, 8))
                  .orderBy(orderByClauses, false)
                  .streamingWindow(functions)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  runBmStreamingWindow(
      data, plan, functions, duckDbQueryRunner_, spillDirectory->path);
}

TEST_F(BmStreamingWindowBuildTest, aggregateRowsFrameMatrix) {
  constexpr vector_size_t size = 96;
  auto data = makeRowVector(
      {"d", "p", "s", "v", "pre", "fol", "one"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(
              size,
              [](auto row) { return row / 12; },
              [](auto row) { return row % 29 == 0; }),
          makeFlatVector<int32_t>(
              size, [](auto row) { return (row * 5) % 23; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return (row * 11) % 37; },
              [](auto row) { return row % 7 == 0; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
          makeFlatVector<int64_t>(
              size, [](auto row) { return (row + 1) % 3 + 1; }),
          makeFlatVector<int64_t>(size, [](auto /*row*/) { return 1; }),
      });

  createDuckDbTable({data});

  const std::vector<std::string> orderByClauses{
      "p NULLS FIRST", "s ASC NULLS LAST", "d NULLS FIRST"};
  const std::string overClause = "partition by p order by s, d";
  std::vector<std::string> functions{
      fmt::format(
          "sum(v) over ({} rows between unbounded preceding and current row)",
          overClause),
      fmt::format(
          "count(v) over ({} rows between unbounded preceding and unbounded following)",
          overClause),
      fmt::format(
          "sum(one) over ({} rows between 2 preceding and 2 following)",
          overClause),
      fmt::format(
          "avg(v) over ({} rows between pre preceding and fol following)",
          overClause),
      fmt::format(
          "min(v) over ({} rows between current row and unbounded following)",
          overClause),
      fmt::format(
          "max(v) over ({} rows between unbounded preceding and 1 preceding)",
          overClause)};

  auto plan = PlanBuilder()
                  .values(split(data, 12))
                  .orderBy(orderByClauses, false)
                  .streamingWindow(functions)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  runBmStreamingWindow(
      data, plan, functions, duckDbQueryRunner_, spillDirectory->path);
}

TEST_F(BmStreamingWindowBuildTest, rangeFrameAscDescMatrix) {
  constexpr vector_size_t size = 90;
  auto data = makeRowVector(
      {"d", "p", "s", "v", "pre_asc", "fol_asc", "pre_desc", "fol_desc"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 30; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 30; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return (row * 17) % 101; },
              [](auto row) { return row % 11 == 0; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return static_cast<int64_t>(row % 30) - 2; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return static_cast<int64_t>(row % 30) + 3; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return static_cast<int64_t>(row % 30) + 2; }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return static_cast<int64_t>(row % 30) - 3; }),
      });

  createDuckDbTable({data});

  struct RangeCase {
    std::vector<std::string> orderByClauses;
    std::vector<std::string> boltFunctions;
    std::vector<std::string> duckFunctions;
  };
  const std::vector<RangeCase> cases{
      {{"p NULLS FIRST", "s ASC NULLS LAST"},
       {"sum(v) over (partition by p order by s range between pre_asc preceding and fol_asc following)",
        "count(v) over (partition by p order by s range between pre_asc preceding and current row)"},
       {"sum(v) over (partition by p order by s range between 2 preceding and 3 following)",
        "count(v) over (partition by p order by s range between 2 preceding and current row)"}},
      {{"p NULLS FIRST", "s DESC NULLS LAST"},
       {"sum(v) over (partition by p order by s desc range between pre_desc preceding and fol_desc following)",
        "count(v) over (partition by p order by s desc range between pre_desc preceding and current row)"},
       {"sum(v) over (partition by p order by s desc range between 2 preceding and 3 following)",
        "count(v) over (partition by p order by s desc range between 2 preceding and current row)"}}};

  for (const auto& rangeCase : cases) {
    SCOPED_TRACE(folly::join(", ", rangeCase.boltFunctions));
    auto plan = PlanBuilder()
                    .values(split(data, 10))
                    .orderBy(rangeCase.orderByClauses, false)
                    .streamingWindow(rangeCase.boltFunctions)
                    .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    runBmStreamingWindow(
        plan,
        selectAllWithWindowFunctions(data, rangeCase.duckFunctions),
        duckDbQueryRunner_,
        spillDirectory->path);
  }
}

TEST_F(BmStreamingWindowBuildTest, canBeSelectedByQueryConfig) {
  const vector_size_t size = 256;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 17; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 16; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 16; }),
      });

  createDuckDbTable({data});

  auto boundedSum =
      "sum(d) over (partition by p order by s rows between 2 preceding and 2 following)";
  auto plan = PlanBuilder()
                  .values(split(data, 23))
                  .streamingWindow({boundedSum})
                  .planNode();

#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
#endif
  auto spillDirectory = TempDirectoryPath::create();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kBufferManagerEnabled, "true")
      .config(core::QueryConfig::kBmStreamingWindowBuildEnabled, "true")
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWindowSpillEnabled, "true")
      .spillDirectory(spillDirectory->path)
      .assertResults(fmt::format("SELECT *, {} FROM tmp", boundedSum));

#ifndef NDEBUG
  EXPECT_GT(window::bmAggregateWindowTestStats().materializeCalls, 0);
#endif
}

TEST_F(BmStreamingWindowBuildTest, aggregateReusesMaterializedChunks) {
  const vector_size_t size = 512;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 17; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  auto runningSum =
      "sum(d) over (partition by p order by s rows between unbounded preceding and current row)";
  auto boundedSum =
      "sum(d) over (partition by p order by s rows between 64 preceding and 64 following)";
  auto plan = PlanBuilder()
                  .values(split(data, 8))
                  .streamingWindow({runningSum, boundedSum})
                  .planNode();

#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
#endif
  auto spillDirectory = TempDirectoryPath::create();
  runBmStreamingWindow(
      plan,
      fmt::format("SELECT *, {}, {} FROM tmp", runningSum, boundedSum),
      duckDbQueryRunner_,
      spillDirectory->path);

#ifndef NDEBUG
  const auto stats = window::bmAggregateWindowTestStats();
  EXPECT_LE(stats.materializeCalls, 4)
      << "Sliding aggregate frames should reuse chunk materialization instead "
         "of rematerializing for each output row.";
  EXPECT_EQ(size * 2, stats.materializedRows);
  EXPECT_EQ(size, stats.maxMaterializedRows);
#endif
}

TEST_F(BmStreamingWindowBuildTest, aggregateUsesReverseIncrementalForSuffix) {
  const vector_size_t size = 512;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row % 17; },
              [](auto row) { return row % 23 == 0; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  auto suffixSum =
      "sum(d) over (partition by p order by s rows between current row and unbounded following)";
  auto plan = PlanBuilder()
                  .values(split(data, 8))
                  .streamingWindow({suffixSum})
                  .planNode();

#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
#endif
  auto spillDirectory = TempDirectoryPath::create();
  runBmStreamingWindow(
      plan,
      fmt::format("SELECT *, {} FROM tmp", suffixSum),
      duckDbQueryRunner_,
      spillDirectory->path);

#ifndef NDEBUG
  const auto stats = window::bmAggregateWindowTestStats();
  EXPECT_GT(stats.reverseIncrementalCalls, 0);
  EXPECT_EQ(size, stats.reverseIncrementalRows);
  EXPECT_EQ(size, stats.materializedRows);
#endif
}

TEST_F(BmStreamingWindowBuildTest, aggregateWindowRequiresBmPartition) {
  HashStringAllocator stringAllocator(pool());
  std::vector<WindowFunctionArg> args{{BIGINT(), nullptr, column_index_t{0}}};
  const core::QueryConfig config{
      std::unordered_map<std::string, std::string>{}};
  auto function = window::createBmAggregateWindowFunction(
      "count", args, BIGINT(), false, pool(), &stringAllocator, config);
  ASSERT_NE(function, nullptr);

  PlainWindowPartition partition;
  EXPECT_THROW(function->resetPartition(&partition), BoltRuntimeError);
}

TEST_F(
    BmStreamingWindowBuildTest,
    collectionAndByExtremeAggregatesUseBmWindowFunction) {
  HashStringAllocator stringAllocator(pool());
  const core::QueryConfig config{
      std::unordered_map<std::string, std::string>{}};
  const std::vector<WindowFunctionArg> collectionArgs{
      {BIGINT(), nullptr, column_index_t{0}}};
  for (const auto* name : {"collect_list", "collect_set"}) {
    auto function = window::createBmAggregateWindowFunction(
        name,
        collectionArgs,
        ARRAY(BIGINT()),
        false,
        pool(),
        &stringAllocator,
        config);
    EXPECT_NE(nullptr, function) << name;
  }

  const std::vector<WindowFunctionArg> byExtremeArgs{
      {VARCHAR(), nullptr, column_index_t{0}},
      {BIGINT(), nullptr, column_index_t{1}}};
  for (const auto* name : {"max_by", "min_by"}) {
    auto function = window::createBmAggregateWindowFunction(
        name,
        byExtremeArgs,
        VARCHAR(),
        false,
        pool(),
        &stringAllocator,
        config);
    EXPECT_NE(nullptr, function) << name;
  }
}

TEST_F(BmStreamingWindowBuildTest, percentileUsesGenericWindowFunction) {
  HashStringAllocator stringAllocator(pool());
  const core::QueryConfig config{
      std::unordered_map<std::string, std::string>{}};
  const std::vector<WindowFunctionArg> args{
      {BIGINT(), nullptr, column_index_t{0}}};
  auto function = window::createBmAggregateWindowFunction(
      "percentile", args, BIGINT(), false, pool(), &stringAllocator, config);
  EXPECT_EQ(nullptr, function);
}

TEST_F(BmStreamingWindowBuildTest, featureFlagAcceptsComplexInput) {
  const vector_size_t size = 32;
  auto data = makeRowVector(
      {"a", "p", "s"},
      {
          makeArrayVector<int64_t>(
              std::vector<std::vector<int64_t>>(size, {1, 2, 3})),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 8; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 8; }),
      });

  auto plan =
      PlanBuilder()
          .values(split(data, 4))
          .streamingWindow(
              {"sum(s) over (partition by p order by s rows between 2 preceding and 2 following)"})
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  std::shared_ptr<Task> task;
  auto result =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kBufferManagerEnabled, "true")
          .config(core::QueryConfig::kBmStreamingWindowBuildEnabled, "true")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->path)
          .copyResults(pool(), task);
  ASSERT_NE(nullptr, result);
  ASSERT_NE(nullptr, task);
  ASSERT_NE(nullptr, task->bufferManager());
  EXPECT_GT(task->bufferManager()->stats().allocatedBlocks, 0);
}

TEST_F(BmStreamingWindowBuildTest, mapPartitionKeyUsesCanonicalOrder) {
  auto maps = makeMapVector<int64_t, int64_t>({
      {{2, 20}, {1, 10}},
      {{1, 10}, {2, 20}},
      {{3, 30}},
      {{3, 30}},
  });
  auto data =
      makeRowVector({"m", "s"}, {maps, makeFlatVector<int32_t>({0, 1, 0, 1})});
  auto plan =
      PlanBuilder()
          .values(split(data, 2))
          .streamingWindow({"row_number() over (partition by m order by s)"})
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  auto result = AssertQueryBuilder(plan)
                    .config(core::QueryConfig::kBufferManagerEnabled, "true")
                    .config(core::QueryConfig::kSpillEnabled, "true")
                    .config(core::QueryConfig::kWindowSpillEnabled, "true")
                    .spillDirectory(spillDirectory->path)
                    .copyResults(pool());
  auto* rowNumbers =
      result->childAt(data->childrenSize())->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, rowNumbers);
  ASSERT_EQ(4, rowNumbers->size());
  EXPECT_EQ(1, rowNumbers->valueAt(0));
  EXPECT_EQ(2, rowNumbers->valueAt(1));
  EXPECT_EQ(1, rowNumbers->valueAt(2));
  EXPECT_EQ(2, rowNumbers->valueAt(3));
}

TEST_F(BmStreamingWindowBuildTest, doesNotSpillDuringStoreWithoutReclaim) {
  const vector_size_t size = 512;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 13; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  auto plan =
      PlanBuilder(pool())
          .values({data})
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  auto root = memory::memoryManager()->addRootPool(
      "bm-streaming-window-no-auto-spill",
      64 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager =
      createWindowTestBufferManager(root.get(), root->name() + "-bm");
  tsan_atomic<bool> nonReclaimableSection{false};
  common::SpillConfig spillConfig;
  window::BmStreamingWindowBuild build(
      windowNode,
      pool(),
      &spillConfig,
      &nonReclaimableSection,
      1,
      false,
      std::move(bufferManager));

  build.addInput(data);

  EXPECT_FALSE(build.windowSpilledStats().has_value());
}

TEST_F(BmStreamingWindowBuildDiskIoTest, windowReclaimSpillsDuringStore) {
  constexpr vector_size_t size = 4'096;
  constexpr vector_size_t inputRowsPerBatch = 512;
  constexpr vector_size_t payloadBytes = 4'096;
  constexpr int64_t memoryLimitBytes = 48LL << 20;

  auto data = makeRowVector(
      {"d", "payload", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 13; }),
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(payloadBytes, 'a' + row % 26);
              }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  auto plan =
      PlanBuilder(pool())
          .values(split(data, size / inputRowsPerBatch))
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  AutoSpillTestContext context{
      "bm-streaming-window-store-auto-spill", memoryLimitBytes};

  auto spillDirectory = TempDirectoryPath::create();
  auto task =
      makeDirectWindowTask(plan, spillDirectory->path, context.rootPool());
  ASSERT_NE(nullptr, task->bufferManager());

  auto driverCtx = std::make_unique<DriverCtx>(task, 0, 0, 0, 0);
  auto* driverCtxRaw = driverCtx.get();
  auto driver = Driver::testingCreate(std::move(driverCtx));

  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  Window window(0, driverCtxRaw, windowNode);
  window.initialize();

  auto batches = split(data, size / inputRowsPerBatch);
  for (const auto& batch : batches) {
    window.addInput(batch);
  }

  const auto contextStats = context.stats();
  EXPECT_GT(contextStats.triggers, 0);
  EXPECT_GT(contextStats.returnedBytes, 0);
  EXPECT_GT(task->bufferManager()->stats().spillWriteBytes, 0);

  window.noMoreInput();

  vector_size_t outputRows = 0;
  for (auto i = 0; i < 16 && !window.isFinished(); ++i) {
    auto output = window.getOutput();
    if (output == nullptr) {
      continue;
    }
    auto rowNumbers = output->childAt(4)->as<FlatVector<int64_t>>();
    ASSERT_NE(nullptr, rowNumbers);
    for (auto row = 0; row < output->size(); ++row) {
      EXPECT_EQ(outputRows + row + 1, rowNumbers->valueAt(row));
    }
    outputRows += output->size();
  }
  EXPECT_EQ(size, outputRows);
  EXPECT_TRUE(window.isFinished());
}

TEST_F(
    BmStreamingWindowBuildDiskIoTest,
    leadLagReadSpilledRowsAcrossChunksAndOutputReclaim) {
  constexpr vector_size_t size = 20'480;
  constexpr vector_size_t inputRowsPerBatch = 4'096;
  constexpr vector_size_t payloadBytes = 4'096;
  constexpr int64_t memoryLimitBytes = 96LL << 20;

  auto data = makeRowVector(
      {"v", "payload", "p", "s", "off", "def"},
      {
          makeFlatVector<std::string>(
              size,
              [](auto row) { return fmt::format("value-{}", row); },
              [](auto row) { return row % 7 == 0; }),
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(payloadBytes, 'a' + row % 26);
              }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
          makeFlatVector<int64_t>(
              size,
              [size](auto row) {
                constexpr std::array<int64_t, 5> offsets{
                    0, 1, 2, 1'024, size + 1};
                return offsets[row % offsets.size()];
              }),
          makeFlatVector<std::string>(
              size, [](auto row) { return fmt::format("default-{}", row); }),
      });
  const std::vector<std::string> functions{
      "lead(v, off, def) over (partition by p order by s)",
      "lag(v, off, def) over (partition by p order by s)",
      "lead(v, 2, def IGNORE NULLS) over (partition by p order by s)",
      "lag(v, 2, def IGNORE NULLS) over (partition by p order by s)"};

  auto expected = expectedSortWindowResults(pool(), data, functions);
  auto input = split(data, size / inputRowsPerBatch);
  auto plan = PlanBuilder().values(input).streamingWindow(functions).planNode();

  AutoSpillTestContext context{
      "bm-streaming-window-lead-lag-auto-spill", memoryLimitBytes};
#ifndef NDEBUG
  window::resetBmWindowPartitionTestStats();
#endif
  AutoSpillWindowRunStats runStats;
  runAutoSpillWindow(plan, input, expected, context, &runStats);

#ifndef NDEBUG
  const auto partitionStats = window::bmWindowPartitionTestStats();
  EXPECT_GT(partitionStats.loadRowsCalls, 0);
  EXPECT_GT(partitionStats.loadedRows, size);
  EXPECT_LE(partitionStats.maxLoadedRows, 4'096);
#endif
  EXPECT_GT(runStats.bufferManager.reclaimedBytes, 0);
}

TEST_F(
    BmStreamingWindowBuildDiskIoTest,
    complexValueReadsSpilledRowsAcrossChunksAndOutputReclaim) {
  constexpr vector_size_t size = 20'480;
  constexpr vector_size_t inputRowsPerBatch = 4'096;
  constexpr int64_t memoryLimitBytes = 96LL << 20;

  std::vector<std::vector<std::string>> values;
  std::vector<std::vector<std::string>> payloads;
  values.reserve(size);
  payloads.reserve(size);
  for (vector_size_t row = 0; row < size; ++row) {
    values.push_back({fmt::format("value-{}", row)});
    payloads.push_back({std::string(4'096, 'a' + row % 26)});
  }

  auto data = makeRowVector(
      {"v", "payload", "p", "s"},
      {
          makeArrayVector<std::string>(values),
          makeArrayVector<std::string>(payloads),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });
  const std::vector<std::string> functions{
      "lead(v, 1) over (partition by p order by s)"};
  auto expected = expectedSortWindowResults(pool(), data, functions);
  auto input = split(data, size / inputRowsPerBatch);
  auto plan = PlanBuilder().values(input).streamingWindow(functions).planNode();
  AutoSpillTestContext context{
      "bm-streaming-window-complex-value-auto-spill", memoryLimitBytes};
  AutoSpillWindowRunStats stats;
  runAutoSpillWindow(plan, input, expected, context, &stats);
}

TEST_F(
    BmStreamingWindowBuildDiskIoTest,
    rankingFunctionsReadSpilledRowsDuringOutputReclaim) {
  constexpr vector_size_t size = 8'192;
  constexpr vector_size_t inputRowsPerBatch = 4'096;
  constexpr vector_size_t payloadBytes = 8'192;
  constexpr int64_t memoryLimitBytes = 64LL << 20;

  auto data = makeRowVector(
      {"d", "payload", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(payloadBytes, 'a' + row % 26);
              }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row / 31; }),
      });
  const std::vector<std::string> functions{
      "row_number() over (partition by p order by s)",
      "rank() over (partition by p order by s)",
      "dense_rank() over (partition by p order by s)",
      "percent_rank() over (partition by p order by s)",
      "cume_dist() over (partition by p order by s)",
      "ntile(4) over (partition by p order by s)"};

  std::vector<VectorPtr> expectedChildren = data->children();
  expectedChildren.push_back(
      makeFlatVector<int64_t>(size, [](auto row) { return row + 1; }));
  expectedChildren.push_back(makeFlatVector<int64_t>(
      size, [](auto row) { return (row / 31) * 31 + 1; }));
  expectedChildren.push_back(
      makeFlatVector<int64_t>(size, [](auto row) { return row / 31 + 1; }));
  expectedChildren.push_back(makeFlatVector<double>(size, [size](auto row) {
    return static_cast<double>((row / 31) * 31) / (size - 1);
  }));
  expectedChildren.push_back(makeFlatVector<double>(size, [size](auto row) {
    return static_cast<double>(std::min((row / 31 + 1) * 31, size)) / size;
  }));
  expectedChildren.push_back(makeFlatVector<int64_t>(
      size, [size](auto row) { return row / (size / 4) + 1; }));
  auto expected = makeRowVector(expectedChildren);
  auto input = split(data, size / inputRowsPerBatch);
  auto plan = PlanBuilder().values(input).streamingWindow(functions).planNode();

  AutoSpillTestContext context{
      "bm-streaming-window-ranking-auto-spill", memoryLimitBytes};
#ifndef NDEBUG
  window::resetBmWindowPartitionTestStats();
#endif
  AutoSpillWindowRunStats runStats;
  runAutoSpillWindow(plan, input, expected, context, &runStats);

#ifndef NDEBUG
  const auto partitionStats = window::bmWindowPartitionTestStats();
  EXPECT_GT(partitionStats.loadRowsCalls, 0);
  EXPECT_GE(partitionStats.loadedRows, size);
  EXPECT_LE(partitionStats.maxLoadedRows, 4'096);
#endif
  EXPECT_GT(runStats.bufferManager.reclaimedBytes, 0);
}

TEST_F(
    BmStreamingWindowBuildDiskIoTest,
    coreAggregatesReadSpilledRowsDuringOutputReclaim) {
  constexpr vector_size_t size = 8'192;
  constexpr vector_size_t inputRowsPerBatch = 4'096;
  constexpr vector_size_t payloadBytes = 8'192;
  constexpr int64_t memoryLimitBytes = 64LL << 20;

  auto data = makeRowVector(
      {"v", "payload", "p", "s"},
      {
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row % 101; },
              [](auto row) { return row % 17 == 0; }),
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(payloadBytes, 'a' + row % 26);
              }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });
  const std::string frame =
      " over (partition by p order by s rows between unbounded preceding and current row)";
  const std::vector<std::string> functions{
      "sum(v)" + frame,
      "count(v)" + frame,
      "max(v)" + frame,
      "min(v)" + frame,
      "avg(v)" + frame};

  auto expected = expectedSortWindowResults(pool(), data, functions);
  auto input = split(data, size / inputRowsPerBatch);
  auto plan = PlanBuilder().values(input).streamingWindow(functions).planNode();

  AutoSpillTestContext context{
      "bm-streaming-window-core-aggregate-auto-spill", memoryLimitBytes};
#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
  window::resetBmWindowPartitionTestStats();
#endif
  AutoSpillWindowRunStats runStats;
  runAutoSpillWindow(plan, input, expected, context, &runStats);

#ifndef NDEBUG
  const auto aggregateStats = window::bmAggregateWindowTestStats();
  EXPECT_GT(aggregateStats.materializeCalls, 0);
  EXPECT_GE(aggregateStats.materializedRows, size);
  EXPECT_LE(aggregateStats.maxMaterializedRows, 4'096);

  const auto partitionStats = window::bmWindowPartitionTestStats();
  EXPECT_GT(partitionStats.loadRowsCalls, 0);
  EXPECT_GT(partitionStats.loadedRows, size);
  EXPECT_LE(partitionStats.maxLoadedRows, 4'096);
#endif
  EXPECT_GT(runStats.bufferManager.reclaimedBytes, 0);
}

TEST_F(
    BmStreamingWindowBuildDiskIoTest,
    nextPartitionSpillsActiveSegmentForSpilledPartition) {
  auto firstBatch = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({0, 1, 2}),
          makeFlatVector<int16_t>({0, 0, 0}),
          makeFlatVector<int32_t>({0, 1, 2}),
      });
  auto secondBatch = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({3, 4, 5}),
          makeFlatVector<int16_t>({0, 0, 0}),
          makeFlatVector<int32_t>({3, 4, 5}),
      });

  auto plan =
      PlanBuilder(pool())
          .values({firstBatch, secondBatch})
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  auto root = memory::memoryManager()->addRootPool(
      "bm-streaming-window-spill-active-before-return",
      64 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager =
      createWindowTestBufferManager(root.get(), root->name() + "-bm");
  tsan_atomic<bool> nonReclaimableSection{false};
  common::SpillConfig spillConfig;
  window::BmStreamingWindowBuild build(
      windowNode,
      pool(),
      &spillConfig,
      &nonReclaimableSection,
      0,
      false,
      std::move(bufferManager));

  build.addInput(firstBatch);
  build.spill();
  ASSERT_TRUE(build.windowSpilledStats().has_value());
  EXPECT_EQ(3, build.windowSpilledStats()->spilledRows);

  build.addInput(secondBatch);
  build.noMoreInput();

  ASSERT_TRUE(build.hasNextPartition());
  auto partition = std::dynamic_pointer_cast<window::BmWindowPartition>(
      build.nextPartition());
  ASSERT_NE(nullptr, partition);
  EXPECT_FALSE(partition->hasResidentRows());
  ASSERT_TRUE(build.windowSpilledStats().has_value());
  EXPECT_EQ(6, build.windowSpilledStats()->spilledRows);
}

TEST_F(
    BmStreamingWindowBuildDiskIoTest,
    peerMetadataSurvivesStoreSpillAndCrossBatchPeer) {
  auto firstBatch = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({0, 1, 2}),
          makeFlatVector<int16_t>({0, 0, 0}),
          makeFlatVector<int32_t>({1, 1, 2}),
      });
  auto secondBatch = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({3, 4, 5}),
          makeFlatVector<int16_t>({0, 0, 0}),
          makeFlatVector<int32_t>({2, 3, 3}),
      });

  auto plan = PlanBuilder(pool())
                  .values({firstBatch, secondBatch})
                  .streamingWindow({"rank() over (partition by p order by s)"})
                  .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  auto root = memory::memoryManager()->addRootPool(
      "bm-streaming-window-peer-metadata-after-spill",
      64 << 20,
      memory::MemoryReclaimer::create());
  auto bufferManager =
      createWindowTestBufferManager(root.get(), root->name() + "-bm");
  tsan_atomic<bool> nonReclaimableSection{false};
  common::SpillConfig spillConfig;
  window::BmStreamingWindowBuild build(
      windowNode,
      pool(),
      &spillConfig,
      &nonReclaimableSection,
      0,
      false,
      std::move(bufferManager));

  build.setIgnorePeer(false);
  build.addInput(firstBatch);
  build.spill();
  build.addInput(secondBatch);
  build.noMoreInput();

  auto partition = std::dynamic_pointer_cast<window::BmWindowPartition>(
      build.nextPartition());
  ASSERT_NE(nullptr, partition);
  ASSERT_FALSE(partition->hasResidentRows());

  std::vector<vector_size_t> peerStarts(6);
  std::vector<vector_size_t> peerEnds(6);
#ifndef NDEBUG
  window::resetBmWindowPartitionTestStats();
#endif
  partition->computePeerBuffers(0, 6, 0, 0, peerStarts.data(), peerEnds.data());

  EXPECT_EQ((std::vector<vector_size_t>{0, 0, 2, 2, 4, 4}), peerStarts);
  EXPECT_EQ((std::vector<vector_size_t>{1, 1, 3, 3, 5, 5}), peerEnds);

#ifndef NDEBUG
  const auto stats = window::bmWindowPartitionTestStats();
  EXPECT_EQ(0, stats.loadRowsCalls);
  EXPECT_EQ(0, stats.loadedRows);
#endif
}

TEST_F(BmStreamingWindowBuildTest, canReclaimThroughWindowDuringBmOutputStage) {
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({0, 1, 2, 3}),
          makeFlatVector<int16_t>({0, 0, 0, 0}),
          makeFlatVector<int32_t>({0, 1, 2, 3}),
      });

  auto plan =
      PlanBuilder(pool())
          .values({data})
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  auto spillDirectory = TempDirectoryPath::create();
  auto task = makeDirectWindowTask(plan, spillDirectory->path);
  auto driverCtx = std::make_unique<DriverCtx>(task, 0, 0, 0, 0);
  auto* driverCtxRaw = driverCtx.get();
  auto driver = Driver::testingCreate(std::move(driverCtx));

  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  Window window(0, driverCtxRaw, windowNode);
  window.initialize();
  ASSERT_TRUE(window.canReclaim());

  window.addInput(data);
  window.noMoreInput();

  EXPECT_TRUE(window.canReclaim());
}

TEST_F(BmStreamingWindowBuildDiskIoTest, rangeSumReadsSpilledSinglePartition) {
  constexpr vector_size_t size = 8'192;
  constexpr vector_size_t inputRowsPerBatch = 1'024;
  constexpr int64_t framePreceding = 8;
  constexpr int64_t frameFollowing = 8;
  constexpr int32_t payloadColumns = 128;

  std::vector<std::string> names{"d", "p", "s", "pre", "fol"};
  std::vector<VectorPtr> children{
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(
          size,
          [framePreceding](auto row) {
            return static_cast<int64_t>(row) - framePreceding;
          }),
      makeFlatVector<int64_t>(
          size,
          [frameFollowing](auto row) {
            return static_cast<int64_t>(row) + frameFollowing;
          }),
  };
  for (auto column = 0; column < payloadColumns; ++column) {
    names.push_back(folly::to<std::string>("payload_", column));
    children.push_back(makeFlatVector<int64_t>(
        size, [column](auto row) { return row + column; }));
  }
  auto data = makeRowVector(names, children);
  const auto sumChannel = data->childrenSize();

  auto rangeSum =
      "sum(d) over (partition by p order by s range between pre preceding and fol following)";
  auto plan = PlanBuilder(pool())
                  .values(split(data, size / inputRowsPerBatch))
                  .streamingWindow({rangeSum})
                  .planNode();
  auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(plan);
  ASSERT_NE(nullptr, windowNode);

  AutoSpillTestContext context{
      "bm-streaming-window-range-auto-spill", 24LL << 20};

  auto spillDirectory = TempDirectoryPath::create();
  auto task =
      makeDirectWindowTask(plan, spillDirectory->path, context.rootPool());
  ASSERT_NE(nullptr, task->bufferManager());

  auto driverCtx = std::make_unique<DriverCtx>(task, 0, 0, 0, 0);
  auto* driverCtxRaw = driverCtx.get();
  auto driver = Driver::testingCreate(std::move(driverCtx));

  TestWindowInjection windowInjection(WindowBuildType::kBmStreamingWindowBuild);
  Window window(0, driverCtxRaw, windowNode);
  window.initialize();

  auto batches = split(data, size / inputRowsPerBatch);
  for (const auto& batch : batches) {
    window.addInput(batch);
  }
  const auto contextStats = context.stats();
  EXPECT_GT(contextStats.triggers, 0);
  EXPECT_GT(contextStats.returnedBytes, 0);
  EXPECT_GT(task->bufferManager()->stats().spillWriteBytes, 0);

#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
  window::resetBmWindowPartitionTestStats();
#endif
  window.noMoreInput();

  auto expectedSum = [](int64_t first, int64_t last) {
    return (first + last) * (last - first + 1) / 2;
  };

  vector_size_t outputRows = 0;
  uint64_t reclaimedOutputBytes = 0;
  const auto reclaimedBeforeOutput =
      task->bufferManager()->stats().reclaimedBytes;
  for (auto attempts = 0; attempts < 64 && !window.isFinished(); ++attempts) {
    // Match Driver's protection during getOutput. Expired scoped batches
    // must be reclaimable through BM without calling Window's reclaimer.
    Operator::NonReclaimableSectionGuard guard(&window);
    auto output = window.getOutput();
    if (output == nullptr) {
      continue;
    }

    {
      memory::MemoryReclaimer::Stats stats;
      memory::ScopedMemoryArbitrationContext contextGuard(
          context.rootPool().get());
      reclaimedOutputBytes += context.rootPool()->reclaim(
          task->bufferManager()->reclaimableBytes(), 0, stats);
    }
    auto sums = output->childAt(sumChannel)->as<FlatVector<int64_t>>();
    ASSERT_NE(nullptr, sums);
    for (auto row = 0; row < output->size(); ++row) {
      const auto inputRow = static_cast<int64_t>(outputRows + row);
      const auto first = std::max<int64_t>(0, inputRow - framePreceding);
      const auto last = std::min<int64_t>(size - 1, inputRow + frameFollowing);
      ASSERT_FALSE(sums->isNullAt(row));
      ASSERT_EQ(expectedSum(first, last), sums->valueAt(row))
          << "row " << inputRow;
    }
    outputRows += output->size();
  }

  EXPECT_EQ(size, outputRows);
  EXPECT_TRUE(window.isFinished());

#ifndef NDEBUG
  const auto aggregateStats = window::bmAggregateWindowTestStats();
  EXPECT_GT(aggregateStats.materializeCalls, 0);
  EXPECT_GE(aggregateStats.materializedRows, size);
  EXPECT_LE(aggregateStats.maxMaterializedRows, 4'096);

  const auto partitionStats = window::bmWindowPartitionTestStats();
  EXPECT_GT(partitionStats.loadRowsCalls, 0);
  EXPECT_GT(partitionStats.loadedRows, size);
#endif
  EXPECT_GT(reclaimedOutputBytes, 0);
  EXPECT_GT(
      task->bufferManager()->stats().reclaimedBytes, reclaimedBeforeOutput);
#ifndef NDEBUG
  EXPECT_LE(partitionStats.maxLoadedRows, 4'096);
#endif
}

TEST_F(BmStreamingWindowBuildTest, materializesArgsInChunks) {
  const vector_size_t size = 9'000;
  constexpr vector_size_t inputRowsPerBatch = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<std::string>(
              size, [](auto row) { return std::string(128, 'a' + row % 26); }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  auto runningCount =
      "count(d) over (partition by p order by s rows between unbounded preceding and current row)";
  auto plan = PlanBuilder()
                  .values(split(data, size / inputRowsPerBatch))
                  .streamingWindow({runningCount})
                  .planNode();

#ifndef NDEBUG
  window::resetBmAggregateWindowTestStats();
#endif
  auto spillDirectory = TempDirectoryPath::create();
  runBmStreamingWindow(
      plan,
      fmt::format("SELECT *, {} FROM tmp", runningCount),
      duckDbQueryRunner_,
      spillDirectory->path);

#ifndef NDEBUG
  const auto stats = window::bmAggregateWindowTestStats();
  ASSERT_GE(stats.materializeCalls, 3);
  ASSERT_EQ(size, stats.materializedRows);
  ASSERT_LE(stats.maxMaterializedRows, 4'096);
#endif
}

TEST_F(BmStreamingWindowBuildTest, reducesPeakMemory) {
  const vector_size_t size = 8'192;
  constexpr vector_size_t inputRowsPerBatch = 1'024;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<std::string>(
              size,
              [](auto row) { return std::string(4'096, 'a' + row % 26); }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  auto fullPartitionCount =
      "count(d) over (partition by p order by s rows between unbounded preceding and unbounded following)";

  auto run = [&](WindowBuildType buildType) {
    core::PlanNodeId windowId;
    auto plan = PlanBuilder()
                    .values(split(data, size / inputRowsPerBatch))
                    .streamingWindow({fullPartitionCount})
                    .capturePlanNodeId(windowId)
                    .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    TestWindowInjection windowInjection(buildType);
    auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                    .config(core::QueryConfig::kBufferManagerEnabled, "true")
                    .config(core::QueryConfig::kSpillEnabled, "true")
                    .config(core::QueryConfig::kWindowSpillEnabled, "true")
                    .spillDirectory(spillDirectory->path)
                    .assertResults(fmt::format(
                        "SELECT *, {} FROM tmp", fullPartitionCount));
    return exec::toPlanStats(task->taskStats()).at(windowId).peakMemoryBytes;
  };

  const auto streamingPeak = run(WindowBuildType::kSortWindowBuild);
  const auto bmPeak = run(WindowBuildType::kBmStreamingWindowBuild);

  ASSERT_GT(streamingPeak, 0);
  ASSERT_GT(bmPeak, 0);
  ASSERT_LT(bmPeak, streamingPeak)
      << "streamingPeak=" << streamingPeak << ", bmPeak=" << bmPeak;
}

} // namespace
} // namespace bytedance::bolt::exec
