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

#include "bolt/exec/bm/BmAggregateWindow.h"
#include "bolt/exec/bm/BmStreamingWindowBuild.h"
#include "bolt/exec/bm/BmWindowPartition.h"

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryArbitrator.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
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
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <set>
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
    taskMemoryManager_ =
        std::make_shared<memory::sparksql::TaskMemoryManager>(
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
    holder_ = memory::sparksql::NativeMemoryManagerFactory::contextInstance(
        param);
    BOLT_CHECK_NOT_NULL(holder_);

    memory::sparksql::SpillerPtr spiller =
        std::make_shared<AutoReclaimSpiller>(
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
  auto plan = PlanBuilder(pool())
                  .values({data})
                  .streamingWindow(
                      {"row_number() over (partition by p order by s)"})
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
  auto plan = PlanBuilder(pool())
                  .values({data})
                  .streamingWindow(
                      {"row_number() over (partition by p order by s)"})
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
          makeFlatVector<int32_t>(size, [](auto row) { return (row % 24) / 3; }),
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
      .assertResults(fmt::format(
          "SELECT *, {} FROM tmp", folly::join(", ", functions)));
}

TEST_F(BmStreamingWindowBuildTest, ignoreNullsValueFunctions) {
  const vector_size_t size = 48;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(
              size, [](auto row) { return row; }, [](auto row) {
                return row % 5 == 0 || row % 11 == 0;
              }),
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
      .assertResults(fmt::format(
          "SELECT *, {} FROM tmp", folly::join(", ", functions)));
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
          makeFlatVector<int64_t>(size, [](auto row) {
            return static_cast<int64_t>(row % 30) - 2;
          }),
          makeFlatVector<int64_t>(size, [](auto row) {
            return static_cast<int64_t>(row % 30) + 3;
          }),
          makeFlatVector<int64_t>(size, [](auto row) {
            return static_cast<int64_t>(row % 30) + 2;
          }),
          makeFlatVector<int64_t>(size, [](auto row) {
            return static_cast<int64_t>(row % 30) - 3;
          }),
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

  auto runningSum =
      "sum(d) over (partition by p order by s rows between unbounded preceding and current row)";
  auto plan = PlanBuilder()
                  .values(split(data, 23))
                  .streamingWindow({runningSum})
                  .planNode();

  window::resetBmAggregateWindowTestStats();
  auto spillDirectory = TempDirectoryPath::create();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kBufferManagerEnabled, "true")
      .config(core::QueryConfig::kBmStreamingWindowBuildEnabled, "true")
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWindowSpillEnabled, "true")
      .spillDirectory(spillDirectory->path)
      .assertResults(fmt::format("SELECT *, {} FROM tmp", runningSum));

  EXPECT_GT(window::bmAggregateWindowTestStats().materializeCalls, 0);
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

TEST_F(BmStreamingWindowBuildTest, featureFlagFallsBackForUnsupportedTypes) {
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
          .streamingWindow({"row_number() over (partition by p order by s)"})
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  ASSERT_NO_THROW(AssertQueryBuilder(plan)
                      .config(core::QueryConfig::kBufferManagerEnabled, "true")
                      .config(
                          core::QueryConfig::kBmStreamingWindowBuildEnabled,
                          "true")
                      .config(core::QueryConfig::kSpillEnabled, "true")
                      .config(core::QueryConfig::kWindowSpillEnabled, "true")
                      .spillDirectory(spillDirectory->path)
                      .copyResults(pool()));
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

TEST_F(BmStreamingWindowBuildTest, windowReclaimSpillsDuringStore) {
  constexpr vector_size_t size = 4'096;
  constexpr vector_size_t inputRowsPerBatch = 512;
  constexpr vector_size_t payloadBytes = 4'096;
  constexpr int64_t memoryLimitBytes = 48LL << 20;

  auto data = makeRowVector(
      {"d", "payload", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 13; }),
          makeFlatVector<std::string>(size, [](auto row) {
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
    BmStreamingWindowBuildTest,
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

  auto plan = PlanBuilder(pool())
                  .values({firstBatch, secondBatch})
                  .streamingWindow(
                      {"row_number() over (partition by p order by s)"})
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
  auto partition =
      std::dynamic_pointer_cast<window::BmWindowPartition>(build.nextPartition());
  ASSERT_NE(nullptr, partition);
  EXPECT_FALSE(partition->hasResidentRows());
  ASSERT_TRUE(build.windowSpilledStats().has_value());
  EXPECT_EQ(6, build.windowSpilledStats()->spilledRows);
}

TEST_F(
    BmStreamingWindowBuildTest,
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

  auto partition =
      std::dynamic_pointer_cast<window::BmWindowPartition>(build.nextPartition());
  ASSERT_NE(nullptr, partition);
  ASSERT_FALSE(partition->hasResidentRows());

  std::vector<vector_size_t> peerStarts(6);
  std::vector<vector_size_t> peerEnds(6);
  window::resetBmWindowPartitionTestStats();
  partition->computePeerBuffers(
      0,
      6,
      0,
      0,
      peerStarts.data(),
      peerEnds.data());

  EXPECT_EQ((std::vector<vector_size_t>{0, 0, 2, 2, 4, 4}), peerStarts);
  EXPECT_EQ((std::vector<vector_size_t>{1, 1, 3, 3, 5, 5}), peerEnds);

  const auto stats = window::bmWindowPartitionTestStats();
  EXPECT_EQ(0, stats.loadRowsCalls);
  EXPECT_EQ(0, stats.loadedRows);
}

TEST_F(BmStreamingWindowBuildTest, canReclaimThroughWindowDuringBmOutputStage) {
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({0, 1, 2, 3}),
          makeFlatVector<int16_t>({0, 0, 0, 0}),
          makeFlatVector<int32_t>({0, 1, 2, 3}),
      });

  auto plan = PlanBuilder(pool())
                  .values({data})
                  .streamingWindow(
                      {"row_number() over (partition by p order by s)"})
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

TEST_F(BmStreamingWindowBuildTest, rangeSumReadsSpilledSinglePartition) {
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

  window::resetBmAggregateWindowTestStats();
  window::resetBmWindowPartitionTestStats();
  window.noMoreInput();

  auto expectedSum = [](int64_t first, int64_t last) {
    return (first + last) * (last - first + 1) / 2;
  };

  vector_size_t outputRows = 0;
  for (auto attempts = 0; attempts < 64 && !window.isFinished(); ++attempts) {
    auto output = window.getOutput();
    if (output == nullptr) {
      continue;
    }

    auto sums = output->childAt(sumChannel)->as<FlatVector<int64_t>>();
    ASSERT_NE(nullptr, sums);
    for (auto row = 0; row < output->size(); ++row) {
      const auto inputRow = static_cast<int64_t>(outputRows + row);
      const auto first = std::max<int64_t>(0, inputRow - framePreceding);
      const auto last =
          std::min<int64_t>(size - 1, inputRow + frameFollowing);
      ASSERT_FALSE(sums->isNullAt(row));
      ASSERT_EQ(expectedSum(first, last), sums->valueAt(row)) << "row "
                                                              << inputRow;
    }
    outputRows += output->size();
  }

  EXPECT_EQ(size, outputRows);
  EXPECT_TRUE(window.isFinished());

  const auto aggregateStats = window::bmAggregateWindowTestStats();
  EXPECT_GT(aggregateStats.materializeCalls, 0);
  EXPECT_GT(aggregateStats.materializedRows, size);
  EXPECT_LE(aggregateStats.maxMaterializedRows, 4'096);

  const auto partitionStats = window::bmWindowPartitionTestStats();
  EXPECT_GT(partitionStats.loadRowsCalls, 0);
  EXPECT_GT(partitionStats.loadedRows, size);
  EXPECT_GT(partitionStats.reclaimReadChunksCalls, 0);
  EXPECT_LE(partitionStats.maxLoadedRows, 4'096);
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

  auto fullPartitionCount =
      "count(d) over (partition by p order by s rows between unbounded preceding and unbounded following)";
  auto plan = PlanBuilder()
                  .values(split(data, size / inputRowsPerBatch))
                  .streamingWindow({fullPartitionCount})
                  .planNode();

  window::resetBmAggregateWindowTestStats();
  auto spillDirectory = TempDirectoryPath::create();
  runBmStreamingWindow(
      plan,
      fmt::format("SELECT *, {} FROM tmp", fullPartitionCount),
      duckDbQueryRunner_,
      spillDirectory->path);

  const auto stats = window::bmAggregateWindowTestStats();
  ASSERT_GE(stats.materializeCalls, 3);
  ASSERT_EQ(size, stats.materializedRows);
  ASSERT_LE(stats.maxMaterializedRows, 4'096);
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
