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

#include <boost/algorithm/string.hpp>
#include <common/memory/HashStringAllocator.h>
#include <core/PlanNode.h>
#include <fmt/format.h>
#include <re2/re2.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ranges>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/cudf/tests/CudfResource.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/Spill.h"
#include "bolt/exec/Spiller.h"
#include "bolt/exec/tests/utils/ArbitratorTestUtil.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/exec/tests/utils/WithGPUParamInterface.h"
#include "bolt/serializers/ArrowSerializer.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "folly/experimental/EventCount.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::common::testutil;
using namespace bytedance::bolt::core;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::exec::test {
namespace {
// Returns aggregated spilled stats by 'task'.
common::SpillStats spilledStats(const exec::Task& task) {
  common::SpillStats spilledStats;
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledStats.spilledInputBytes += op.spilledInputBytes;
      spilledStats.spilledBytes += op.spilledBytes;
      spilledStats.spilledRows += op.spilledRows;
      spilledStats.spilledPartitions += op.spilledPartitions;
      spilledStats.spilledFiles += op.spilledFiles;
    }
  }
  return spilledStats;
}

void abortPool(memory::MemoryPool* pool) {
  try {
    BOLT_FAIL("Manual MemoryPool Abortion");
  } catch (const BoltException& error) {
    pool->abort(std::current_exception());
  }
}
} // namespace

class OrderByTest : public OperatorTestBase, public WithGPUParamInterface<> {
 protected:
  static void SetUpTestCase() {
    FLAGS_bolt_testing_enable_arbitration = true;
    OperatorTestBase::SetUpTestCase();
    TestValue::enable();
  }

  void SetUp() override {
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      this->registerVectorSerde();
    }
    rng_.seed(123);

    rowType_ = ROW(
        {{"c0", INTEGER()},
         {"c1", INTEGER()},
         {"c2", VARCHAR()},
         {"c3", VARCHAR()}});
    fuzzerOpts_.vectorSize = 1024;
    fuzzerOpts_.nullRatio = 0;
    fuzzerOpts_.stringVariableLength = false;
    fuzzerOpts_.stringLength = 1024;
    fuzzerOpts_.allowLazyVector = false;

#if defined BOLT_HAS_CUDF && BOLT_HAS_CUDF == 1
    if (GetParam().useGPU) {
      bolt::cudf::test::CudfResource::getInstance().initialize();
    }
#endif
  }

  void TearDown() override {
#if defined BOLT_HAS_CUDF && BOLT_HAS_CUDF == 1
    if (GetParam().useGPU) {
      bolt::cudf::test::CudfResource::getInstance().finalize();
    }
#endif
  }

  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key,
      const bool useGPU = false) {
    core::PlanNodeId orderById;
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);
    auto plan =
        PlanBuilder()
            .values(input)
            .orderBy({fmt::format("{} ASC NULLS LAST", key)}, false, useGPU)
            .capturePlanNodeId(orderById)
            .planNode();
    runTest(
        plan,
        orderById,
        fmt::format("SELECT * FROM tmp ORDER BY {} NULLS LAST", key),
        {keyIndex});

    plan =
        PlanBuilder()
            .values(input)
            .orderBy({fmt::format("{} DESC NULLS FIRST", key)}, false, useGPU)
            .planNode();
    runTest(
        plan,
        orderById,
        fmt::format("SELECT * FROM tmp ORDER BY {} DESC NULLS FIRST", key),
        {keyIndex});
  }

  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key,
      const std::string& filter,
      const bool useGPU = false) {
    core::PlanNodeId orderById;
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);
    auto plan =
        PlanBuilder()
            .values(input)
            .filter(filter)
            .orderBy({fmt::format("{} ASC NULLS LAST", key)}, false, useGPU)
            .capturePlanNodeId(orderById)
            .planNode();
    runTest(
        plan,
        orderById,
        fmt::format(
            "SELECT * FROM tmp WHERE {} ORDER BY {} NULLS LAST", filter, key),
        {keyIndex});

    plan =
        PlanBuilder()
            .values(input)
            .filter(filter)
            .orderBy({fmt::format("{} DESC NULLS FIRST", key)}, false, useGPU)
            .capturePlanNodeId(orderById)
            .planNode();
    runTest(
        plan,
        orderById,
        fmt::format(
            "SELECT * FROM tmp WHERE {} ORDER BY {} DESC NULLS FIRST",
            filter,
            key),
        {keyIndex});
  }

  void testTwoKeys(
      const std::vector<RowVectorPtr>& input,
      const std::string& key1,
      const std::string& key2,
      const bool useGPU = false) {
    auto rowType = input[0]->type()->asRow();
    auto keyIndices = {rowType.getChildIdx(key1), rowType.getChildIdx(key2)};

    std::vector<core::SortOrder> sortOrders = {
        core::kAscNullsLast, core::kDescNullsFirst};
    std::vector<std::string> sortOrderSqls = {"NULLS LAST", "DESC NULLS FIRST"};

    for (int i = 0; i < sortOrders.size(); i++) {
      for (int j = 0; j < sortOrders.size(); j++) {
        core::PlanNodeId orderById;
        auto plan = PlanBuilder()
                        .values(input)
                        .orderBy(
                            {fmt::format("{} {}", key1, sortOrderSqls[i]),
                             fmt::format("{} {}", key2, sortOrderSqls[j])},
                            false,
                            useGPU)
                        .capturePlanNodeId(orderById)
                        .planNode();
        runTest(
            plan,
            orderById,
            fmt::format(
                "SELECT * FROM tmp ORDER BY {} {}, {} {}",
                key1,
                sortOrderSqls[i],
                key2,
                sortOrderSqls[j]),
            keyIndices);
      }
    }
  }

  void testMultiKeys(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& keys,
      const std::vector<core::SortOrder>& sortOrders,
      const bool useGPU = false) {
    auto rowType = input[0]->type()->asRow();

    auto indices =
        keys | std::views::transform([&rowType](const std::string& key) {
          return rowType.getChildIdx(key);
        });

    auto orders =
        sortOrders | std::views::transform([](const core::SortOrder& o) {
          return o.toString();
        });

    std::vector<std::string> orderFlags(orders.begin(), orders.end());
    std::vector<std::string> orderKeys;
    for (auto i = 0; i < keys.size(); ++i) {
      orderKeys.emplace_back(fmt::format(" {} {} ", keys[i], orderFlags[i]));
    }
    // C++ 23
    // for (auto &&[k, f] : std::views::zip(keys, orders)) {
    //   std::cout << fmt::format(" {} {} ", k, f) << "\n";
    // }
    core::PlanNodeId orderById;
    auto plan = PlanBuilder()
                    .values(input)
                    .orderBy(orderKeys, false, useGPU)
                    .capturePlanNodeId(orderById)
                    .planNode();

    std::vector<uint32_t> keyIndices(indices.begin(), indices.end());
    runTest(
        plan,
        orderById,
        fmt::format(
            "SELECT * FROM tmp ORDER BY {}",
            boost::algorithm::join(orderKeys, ", ")),
        keyIndices);
  }

  void runTest(
      core::PlanNodePtr planNode,
      const core::PlanNodeId& orderById,
      const std::string& duckDbSql,
      const std::vector<uint32_t>& sortingKeys) {
    {
      SCOPED_TRACE("run without spilling");
      assertQueryOrdered(planNode, duckDbSql, sortingKeys);
    }
    if (GetParam().useGPU) {
      // Exiting function. The current GPU operators lack support for spilling.
      return;
    }
    {
      SCOPED_TRACE("run with spilling");
      auto spillDirectory = exec::test::TempDirectoryPath::create();
      auto queryCtx = core::QueryCtx::create(executor_.get());
      TestScopedSpillInjection scopedSpillInjection(100);
      queryCtx->testingOverrideConfigUnsafe({
          {core::QueryConfig::kSpillEnabled, "true"},
          {core::QueryConfig::kOrderBySpillEnabled, "true"},
          {core::QueryConfig::kJitLevel, "-1"},
      });
      CursorParameters params;
      params.planNode = planNode;
      params.queryCtx = queryCtx;
      params.spillDirectory = spillDirectory->path;
      auto task = assertQueryOrdered(params, duckDbSql, sortingKeys);
      auto inputRows = toPlanStats(task->taskStats()).at(orderById).inputRows;
      if (checkSpillStats_) {
        const uint64_t peakSpillMemoryUsage =
            memory::spillMemoryPool()->stats().peakBytes;
        ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
        if (inputRows > 0) {
          EXPECT_LT(0, spilledStats(*task).spilledInputBytes);
          EXPECT_LT(0, spilledStats(*task).spilledBytes);
          EXPECT_EQ(1, spilledStats(*task).spilledPartitions);
          EXPECT_LT(0, spilledStats(*task).spilledFiles);
          EXPECT_GE(inputRows, spilledStats(*task).spilledRows);
          EXPECT_LT(0, spilledStats(*task).spilledRows);
          ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
          if (memory::spillMemoryPool()->trackUsage()) {
            ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
            ASSERT_GE(
                memory::spillMemoryPool()->stats().peakBytes,
                peakSpillMemoryUsage);
          }
        } else {
          EXPECT_EQ(0, spilledStats(*task).spilledInputBytes);
          EXPECT_EQ(0, spilledStats(*task).spilledBytes);
        }
      }
      OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
    }
  }

  static void reclaimAndRestoreCapacity(
      const Operator* op,
      uint64_t targetBytes,
      memory::MemoryReclaimer::Stats& reclaimerStats) {
    const auto oldCapacity = op->pool()->capacity();
    op->pool()->reclaim(targetBytes, 0, reclaimerStats);
    dynamic_cast<memory::MemoryPoolImpl*>(op->pool())
        ->testingSetCapacity(oldCapacity);
  }

  folly::Random::DefaultGenerator rng_;
  memory::MemoryReclaimer::Stats reclaimerStats_;
  RowTypePtr rowType_;
  VectorFuzzer::Options fuzzerOpts_;

  bool checkSpillStats_{true};
};

TEST_P(OrderByTest, selectiveFilter) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c2 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  // c0 values are unique across batches
  testSingleKey(vectors, "c0", "c0 % 333 = 0", GetParam().useGPU);

  // c1 values are unique only within a batch
  testSingleKey(vectors, "c1", "c1 % 333 = 0", GetParam().useGPU);
}

TEST_P(OrderByTest, singleKey) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 2; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0", GetParam().useGPU);

  // parser doesn't support "is not null" expression, hence, using c0 % 2 >= 0
  testSingleKey(vectors, "c0", "c0 % 2 >= 0", GetParam().useGPU);

  core::PlanNodeId orderById;
  auto plan = PlanBuilder()
                  .values(vectors)
                  .orderBy({"c0 DESC NULLS LAST"}, false, GetParam().useGPU)
                  .capturePlanNodeId(orderById)
                  .planNode();
  runTest(
      plan, orderById, "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST", {0});

  plan = PlanBuilder()
             .values(vectors)
             .orderBy({"c0 ASC NULLS FIRST"}, false, GetParam().useGPU)
             .capturePlanNodeId(orderById)
             .planNode();
  runTest(plan, orderById, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST", {0});
}

TEST_P(OrderByTest, multipleKeys) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 2; ++i) {
    // c0: half of rows are null, a quarter is 0 and remaining quarter is 1
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [](vector_size_t row) { return row % 4; }, nullEvery(2, 1));
    auto c1 = makeFlatVector<int32_t>(
        batchSize, [](vector_size_t row) { return row + 10; }, nullptr); //
    auto c2 = makeFlatVector<double>(
        batchSize,
        [](vector_size_t row) { return row * 0.1; },
        nullEvery(11)); //
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  // testTwoKeys(vectors, "c0", "c1");

  const std::vector<std::vector<SortOrder>> orders{
      {kAscNullsLast, kAscNullsLast},
      {kAscNullsLast, kAscNullsFirst},
      {kAscNullsFirst, kAscNullsFirst},
      {kAscNullsFirst, kAscNullsLast},
      {kDescNullsLast, kDescNullsLast},
      {kDescNullsLast, kDescNullsFirst},
      {kDescNullsFirst, kDescNullsFirst},
      {kDescNullsFirst, kDescNullsLast},
  };
  for (const auto& order : orders) {
    testMultiKeys(vectors, {"c0", "c1"}, order, GetParam().useGPU);
  }
}

TEST_P(OrderByTest, DISABLED_moreThan3Keys) {
  constexpr vector_size_t batchSize = 8192;
  std::vector<RowVectorPtr> vectors;
  std::vector<std::string> rawStrs;
  for (auto j = 0; j < 16; ++j) {
    for (auto i = 0; i < batchSize / 16; ++i) {
      rawStrs.emplace_back("ssssssssss" + std::to_string(i)); // "ssssssssss" +
    }
  }

  for (int32_t i = 0; i < 1; ++i) {
    // c0: half of rows are null, a quarter is 0 and remaining quarter is 1
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [](vector_size_t row) { return row % 7; }, nullEvery(2, 1));

    auto c1 = makeFlatVector<StringView>(
        batchSize,
        [&i](vector_size_t row) {
          return StringView::makeInline(
              fmt::format("{:_<5}{}", row % 8, "str" + std::to_string(i)));
        },
        nullEvery(5));

    auto c2 = makeFlatVector<StringView>(
        batchSize,
        [&rawStrs](vector_size_t row) { return StringView(rawStrs[row]); },
        nullEvery(7));

    auto c3 = makeFlatVector<double>(
        batchSize,
        [](vector_size_t row) { return (row + 10) * 1.1; },
        nullEvery(11));

    vectors.emplace_back(makeRowVector({c0, c1, c2, c3}));
  }
  createDuckDbTable(vectors);

  const std::vector<std::vector<SortOrder>> orders{
      {kAscNullsLast, kAscNullsLast, kAscNullsLast, kAscNullsLast},
      {kAscNullsFirst, kAscNullsFirst, kAscNullsFirst, kAscNullsFirst},
      {kDescNullsLast, kDescNullsLast, kDescNullsLast, kDescNullsLast},
      {kDescNullsFirst, kDescNullsFirst, kDescNullsFirst, kDescNullsFirst},
      {kAscNullsLast, kAscNullsFirst, kDescNullsLast, kDescNullsFirst},
  };

  checkSpillStats_ = false;
  auto guard = folly::makeGuard([&]() { checkSpillStats_ = true; });

  for (const auto& order : orders) {
    testMultiKeys(vectors, {"c0", "c1", "c2", "c3"}, order, GetParam().useGPU);
  }
}

TEST_P(OrderByTest, moreThan8Keys) {
  vector_size_t batchSize = 1024;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 16; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [](vector_size_t row) { return row % 5; }, nullEvery(3, 1));

    auto c1 = makeFlatVector<int128_t>(
        batchSize,
        [](vector_size_t row) { return HugeInt::build(row % 16, row % 8); },
        nullEvery(7),
        DECIMAL(20, 2));

    auto c2 = makeFlatVector<StringView>(
        batchSize,
        [&i](vector_size_t row) {
          return StringView::makeInline(
              fmt::format("{:_<5}{}", row % 64, "str" + std::to_string(i)));
        },
        nullEvery(11));

    auto c3 = makeFlatVector<int32_t>(
        batchSize, [](vector_size_t row) { return row % 128; }, nullEvery(5));

    auto c4 = makeFlatVector<double>(
        batchSize,
        [](vector_size_t row) { return (row % 64 + 10) * 1.0; },
        nullEvery(13));

    auto c5 = makeFlatVector<int64_t>(
        batchSize,
        [](vector_size_t row) { return row % 128 + 20; },
        nullEvery(7));

    auto c6 = makeFlatVector<float>(
        batchSize,
        [](vector_size_t row) { return (float)(row % 64 + 30); },
        nullEvery(5));

    auto c7 = makeFlatVector<bool>(
        batchSize,
        [](vector_size_t row) { return row % 2 == 0; },
        nullEvery(13));

    auto c8 = makeFlatVector<int32_t>(
        batchSize, [](vector_size_t row) { return row % 64; }, nullEvery(17));

    auto c9 = makeFlatVector<int8_t>(
        batchSize, [](vector_size_t row) { return row; }, nullEvery(23));

    vectors.emplace_back(
        makeRowVector({c0, c1, c2, c3, c4, c5, c6, c7, c8, c9}));
  }
  createDuckDbTable(vectors);

  const std::vector<std::vector<SortOrder>> orders{
      {kAscNullsLast,
       kAscNullsLast,
       kAscNullsLast,
       kAscNullsLast,
       kAscNullsLast,
       kAscNullsLast,
       kAscNullsLast,
       kAscNullsLast,
       kAscNullsLast},
      {kDescNullsFirst,
       kDescNullsFirst,
       kDescNullsFirst,
       kDescNullsFirst,
       kDescNullsFirst,
       kDescNullsFirst,
       kDescNullsFirst,
       kDescNullsFirst,
       kDescNullsFirst},
      {kDescNullsLast,
       kDescNullsLast,
       kDescNullsLast,
       kDescNullsLast,
       kDescNullsLast,
       kAscNullsFirst,
       kAscNullsFirst,
       kAscNullsFirst,
       kAscNullsFirst},
  };

  checkSpillStats_ = false;
  auto guard = folly::makeGuard([&]() { checkSpillStats_ = true; });

  for (const auto& order : orders) {
    testMultiKeys(
        vectors,
        {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
        order,
        GetParam().useGPU);
  }
}

TEST_P(OrderByTest, multiBatchResult) {
  vector_size_t batchSize = 5000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c1, c1, c1, c1}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0", GetParam().useGPU);
}

/// A String generator for unit test.
/// 1. Prefix len varies from 0 to 12;
/// 2. generate valid and random binary strings with random length
/// 3. some corner cases.
class TestStringGenerator {
 public:
  TestStringGenerator() {
    // Starts with '0x01', 0x00 is ignored,
    // since it does not work in duckdb
    for (uint8_t i = 0x01; i <= 0x7F; i++) {
      first_values.push_back(i);
    }
    for (uint8_t i = 0xC2; i <= 0xF4; i++) {
      first_values.push_back(i);
    }

    for (uint8_t i = 0x80; i <= 0xBF; i++) {
      trailing_values.push_back(i);
    }
  }

  std::vector<std::string> genRandUtf8WithVariablePrefix(size_t n = 10000) {
    std::vector<std::string> result;
    std::srand(std::time(0));

    constexpr size_t MAX_PREFIX_LEN{12};
    size_t cnt{0};
    result = cornerStrs;
    cnt += cornerStrs.size();

    // a little bit tricky...
    // Refer to:
    // https://stackoverflow.com/questions/1477294/generate-random-utf-8-string-in-python
    auto random_utf8 = [&]() {
      std::string valid_utf8;
      auto first = first_values[(uint32_t)std::rand() % first_values.size()];

      if (first <= 0x7F) {
        valid_utf8.append(1, first);
      } else if (first <= 0xDF) {
        valid_utf8.append(1, first);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
      } else if (first == 0xE0) {
        valid_utf8.append(1, first);
        valid_utf8.append(1, 0xA1); // byte_range(0xA0, 0xBF
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
      } else if (first == 0xED) {
        valid_utf8.append(1, first);
        valid_utf8.append(1, 0x81); // byte_range(0x80, 0x9F)
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
      } else if (first <= 0xEF) {
        valid_utf8.append(1, first);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
      } else if (first == 0xF0) {
        valid_utf8.append(1, first);
        valid_utf8.append(1, 0x91); // byte_range(0x90, 0xBF)
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
      } else if (first <= 0xF3) {
        valid_utf8.append(1, first);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
      } else if (first == 0xF4) {
        valid_utf8.append(1, first);
        valid_utf8.append(1, 0x81); // byte_range(0x80, 0x8F)
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
        valid_utf8.append(
            1, trailing_values[(uint32_t)std::rand() % trailing_values.size()]);
      }

      return valid_utf8;
    };

    for (; cnt < n; ++cnt) {
      for (size_t pre = 0; pre <= MAX_PREFIX_LEN && (cnt < n); ++pre) {
        std::string str;
        str.append(pre, 'a');
        uint32_t rnd = (uint32_t)std::rand();
        auto numUtf8char = rnd % 5;
        for (auto i = 0; i < numUtf8char; ++i) {
          str.append(random_utf8());
        }
        result.emplace_back(str);
      }
    }
    return result;
  }

 private:
  std::vector<uint8_t> first_values;
  std::vector<uint8_t> trailing_values;

  // corner cases
  std::vector<std::string> cornerStrs{
      "    qier",
      "    卡卡",
      "aaaabbbbqier",
      "aaaabbbb卡卡",
      "aaaabbbbcccqier",
      "aaaabbbbccc卡卡",
      "aaaabbbbccccqier",
      "aaaabbbbcccc卡卡",
      "用户7889977832889",
      "用户9978511894858",
      "用户7889977832889",
      "用户9978511894858",
      "κόσμε",
      "若朴",
      "若冲",
      "日本語",
      "にほんご",
      "∮ E⋅da = Q,  ",
      "n → ∞, ∑ f(i) = ∏ g(i)",
      "แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช",
      "ሰማይ አይታረስ ንጉሥ አይከሰስ።",
      "",
      "",
      "\0",
      " ",
      " ",
      "中文",
      "上海",
      "abcd", // 4
      "abcde",
      "abcD",
      "abcde",
      "abcD",
      "abcdefgh", // 8
      "abcdefghijkl", // 12
      "abcdefghijklM", // 13
      "abcd中文",
      "abcd上海",
      "abcdefgh中文",
      "abcdefgh上海",
      "abcdefghijkl",
      "abcdefghijklM",
  };
};

TEST_P(OrderByTest, Strings) {
  TestStringGenerator dataGen;
  constexpr vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;

  std::vector<std::string> rawStrs =
      dataGen.genRandUtf8WithVariablePrefix(batchSize);

  for (int32_t i = 0; i < 8; ++i) {
    auto c0 = makeFlatVector<StringView>(
        batchSize,
        [&rawStrs](vector_size_t row) {
          // construct 'dirty' StringView. if string len < 4, set inline part as
          // dirty
          auto&& s = StringView(rawStrs[row % rawStrs.size()]);
          if (s.size() <= 4) {
            int64_t* p = (int64_t*)(&s);
            *(p + 1) = std::rand();
            return s;
          }
          return s;
        },
        nullptr);
    auto c1 = makeFlatVector<StringView>(
        batchSize,
        [&rawStrs](vector_size_t row) {
          return StringView(rawStrs[row % rawStrs.size()]);
        },
        nullptr);

    auto c2 = makeFlatVector<StringView>(
        batchSize,
        [&rawStrs](vector_size_t row) {
          return StringView(rawStrs[row % rawStrs.size()]);
        },
        nullEvery(17));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c1", GetParam().useGPU);
}

TEST_P(OrderByTest, nonContigousStringInRowCmp) {
  std::vector<RowVectorPtr> vectors;

  std::vector<std::string> rawStrs;

  // > Run 64K.
  std::srand(std::time(nullptr));
  for (auto i = 0; i < 1024; ++i) {
    int rnd = std::rand();
    std::string s(rnd % 11, 'c');
    rawStrs.emplace_back(s + std::to_string(i));

    // some special characters
    if (i % 64 == 0) {
      std::string s1 =
          R"(第一章】道可道，非常道；名可名，非常名。无名天地之始，有名万物之母。故常无欲，以观其妙；常
有欲，以观其徼（jiào）。此两者同出而异名，同谓之玄，玄之又玄，众妙之门。〖译文〗)" +
          std::to_string(rnd % 17);
      std::string s2 =
          "再不续就当你死在外面了椿惚！！！ [朋友]" + std::to_string(rnd % 11);
      std::string s3 = "日本の；日本人の；日本語の；日系の（Jap., Jpnと略す）";
      std::string s4 = R"(<a href="file">C:\Program Files\</a>)";

      rawStrs.emplace_back(s1);
      rawStrs.emplace_back(s2);
      rawStrs.emplace_back(s3);
      rawStrs.emplace_back(s4);

      rawStrs.emplace_back("a\0\0");
      rawStrs.emplace_back("aaaa\0\0");
      rawStrs.emplace_back("aaaa");
      rawStrs.emplace_back("\0aaaaaaa\0\0");
      rawStrs.emplace_back("aaaaaaaaaaaa");
      rawStrs.emplace_back("aaaaaaaaaaaa\0\0");
    }
  }

  // Now, append some supper long strings.
  constexpr size_t sz = 16 * memory::AllocationTraits::kPageSize;
  rawStrs.emplace_back(sz / 4, 'c');
  rawStrs.emplace_back(sz, 'c');
  rawStrs.emplace_back(sz + 1, 'c');
  rawStrs.emplace_back(2 * sz, 'c');
  rawStrs.emplace_back(2 * sz + 1, 'c');

  for (int32_t i = 0; i < 1; ++i) {
    auto c0 = makeFlatVector<StringView>(
        rawStrs.size(),
        [&rawStrs](vector_size_t row) { return StringView(rawStrs[row]); },
        nullptr);
    vectors.emplace_back(makeRowVector({c0}));
  }
  createDuckDbTable(vectors);

  const std::vector<std::vector<SortOrder>> orders{
      {kAscNullsLast}, {kDescNullsLast}};

  checkSpillStats_ = false;
  auto guard = folly::makeGuard([&]() { checkSpillStats_ = true; });

  for (const auto& order : orders) {
    testMultiKeys(vectors, {"c0"}, order, GetParam().useGPU);
  }
}

TEST_P(OrderByTest, floatingPointKeysCornerCases) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;

  //  NaN: 0111111111111000000000000000000000000000000000000000000000000000
  // -Inf: 1111111111110000000000000000000000000000000000000000000000000000
  // +Inf: 0111111111110000000000000000000000000000000000000000000000000000
  // -Max: 1111111111101111111111111111111111111111111111111111111111111111
  // +Max: 0111111111101111111111111111111111111111111111111111111111111111

  for (int32_t i = 0; i < 8; ++i) {
    auto c0 = makeFlatVector<double>(
        {4.94066e-324,
         0.0,
         -0.0,
         1.0,
         -1.0,
         -std::numeric_limits<double>::infinity(),
         // std::numeric_limits<double>::quiet_NaN(), // DuckDB has different
         // behavior
         std::numeric_limits<double>::infinity(),
         std::numeric_limits<double>::min(),
         std::numeric_limits<double>::max()});

    vectors.push_back(makeRowVector({c0}));
  }
  createDuckDbTable(vectors);

  const std::vector<std::vector<SortOrder>> orders{
      {kAscNullsLast},
      {kAscNullsFirst},
      {kDescNullsFirst},
      {kDescNullsFirst},
  };
  for (const auto& order : orders) {
    testMultiKeys(vectors, {"c0"}, order, GetParam().useGPU);
  }
}

TEST_P(OrderByTest, unknown) {
  vector_size_t size = 1'000;
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row % 7; }),
      BaseVector::createNullConstant(UNKNOWN(), size, pool()),
  });

  // Exclude "UNKNOWN" column as DuckDB doesn't understand UNKNOWN type
  createDuckDbTable(
      {makeRowVector({vector->childAt(0)}),
       makeRowVector({vector->childAt(0)})});

  core::PlanNodeId orderById;
  auto plan = PlanBuilder()
                  .values({vector, vector})
                  .orderBy({"c0 DESC NULLS LAST"}, false, GetParam().useGPU)
                  .capturePlanNodeId(orderById)
                  .planNode();
  if (GetParam().useGPU) {
    // "UNKNOWN" type scalar column is not supported by Arrow export.
    // Arrow will throw an error. This test checks if it complains the input.
    // CUDF implementation uses Arrow.
    ASSERT_THROW(
        runTest(
            plan,
            orderById,
            "SELECT *, null FROM tmp ORDER BY c0 DESC NULLS LAST",
            {0}),
        ::bytedance::bolt::BoltRuntimeError);
  } else {
    runTest(
        plan,
        orderById,
        "SELECT *, null FROM tmp ORDER BY c0 DESC NULLS LAST",
        {0});
  }
}

TEST_P(OrderByTest, constantColumn) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> input;
  for (int32_t i = 0; i < 2; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) {
          return std::hash<int>{}(row) % (batchSize * batchSize);
        },
        nullEvery(7));
    auto c1 = BaseVector::createConstant(INTEGER(), 7, batchSize, pool());
    input.push_back(makeRowVector({c0, c1}));
  }
  createDuckDbTable(input);
  testSingleKey(input, "c0", GetParam().useGPU);
}

/// Verifies output batch rows of OrderBy
TEST_P(OrderByTest, outputBatchRows) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  struct {
    int numRowsPerBatch;
    int preferredOutBatchBytes;
    int maxOutBatchRows;
    int expectedOutputVectors;

    // TODO: add output size check with spilling enabled
    std::string debugString() const {
      return fmt::format(
          "numRowsPerBatch:{}, preferredOutBatchBytes:{}, maxOutBatchRows:{}, expectedOutputVectors:{}",
          numRowsPerBatch,
          preferredOutBatchBytes,
          maxOutBatchRows,
          expectedOutputVectors);
    }
  } testSettings[] = {
      {1024, 1, 100, 1024},
      // estimated size per row is ~2092, set preferredOutBatchBytes to 20920,
      // so each batch has 10 rows, so it would return 100 batches
      {1000, 20920, 100, 100},
      // same as above, but maxOutBatchRows is 1, so it would return 1000
      // batches
      {1000, 20920, 1, 1000}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const vector_size_t batchSize = testData.numRowsPerBatch;
    std::vector<RowVectorPtr> rowVectors;
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(11));
    std::vector<VectorPtr> vectors;
    vectors.push_back(c0);
    for (int i = 0; i < 256; ++i) {
      vectors.push_back(c1);
    }
    rowVectors.push_back(makeRowVector(vectors));
    createDuckDbTable(rowVectors);

    core::PlanNodeId orderById;
    auto plan = PlanBuilder()
                    .values(rowVectors)
                    .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                    .capturePlanNodeId(orderById)
                    .planNode();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchBytes,
          std::to_string(testData.preferredOutBatchBytes)},
         {core::QueryConfig::kMaxOutputBatchRows,
          std::to_string(testData.maxOutBatchRows)}});
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = queryCtx;
    auto task = assertQueryOrdered(
        params, "SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST", {0});
    EXPECT_EQ(
        testData.expectedOutputVectors,
        toPlanStats(task->taskStats()).at(orderById).outputVectors);
  }
}

TEST_P(OrderByTest, spill) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 1024, 48 << 20);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId orderNodeId;
  const auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors)
          .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
          .capturePlanNodeId(orderNodeId)
          .planNode();

  const auto expectedResult = AssertQueryBuilder(plan).copyResults(pool_.get());

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task = AssertQueryBuilder(plan)
                  .spillDirectory(spillDirectory->path)
                  .config(core::QueryConfig::kSpillEnabled, true)
                  .config(core::QueryConfig::kOrderBySpillEnabled, true)
                  // Set a small capacity to trigger threshold based spilling
                  .config(QueryConfig::kOrderBySpillMemoryThreshold, 32 << 20)
                  .assertResults(expectedResult);
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& planStats = taskStats.at(orderNodeId);
  ASSERT_GT(planStats.spilledBytes, 0);
  ASSERT_GT(planStats.spilledRows, 0);
  ASSERT_GT(planStats.spilledBytes, 0);
  ASSERT_GT(planStats.spilledInputBytes, 0);
  ASSERT_EQ(planStats.spilledPartitions, 1);
  ASSERT_GT(planStats.spilledFiles, 0);
  ASSERT_GT(planStats.customStats["spillRuns"].count, 0);
  ASSERT_GT(planStats.customStats["spillFillTime"].sum, 0);
  ASSERT_GT(planStats.customStats["spillSortTime"].sum, 0);
  ASSERT_GT(planStats.customStats["spillSerializationTime"].sum, 0);
  ASSERT_GT(planStats.customStats["spillFlushTime"].sum, 0);
  ASSERT_EQ(
      planStats.customStats["spillSerializationTime"].count,
      planStats.customStats["spillFlushTime"].count);
  ASSERT_GT(planStats.customStats[Operator::kSpillWrites].sum, 0);
  ASSERT_GT(planStats.customStats["spillWriteTime"].sum, 0);
  ASSERT_EQ(
      planStats.customStats[Operator::kSpillWrites].count,
      planStats.customStats["spillWriteTime"].count);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_P(OrderByTest, spillWithArrowSerde) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kArrow)) {
    serializer::arrowserde::ArrowVectorSerde::registerNamedVectorSerde();
  }

  const vector_size_t batchSize = 2'048;
  std::vector<RowVectorPtr> input;
  for (int i = 0; i < 2; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(11));
    auto c1 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row % 7; }, nullEvery(17));
    input.push_back(makeRowVector({c0, c1}));
  }
  createDuckDbTable(input);

  core::PlanNodeId orderById;
  auto plan = PlanBuilder()
                  .values(input)
                  .orderBy({"c0 ASC NULLS LAST"}, false)
                  .capturePlanNodeId(orderById)
                  .planNode();

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());
  TestScopedSpillInjection scopedSpillInjection(100);
  bool sawArrowSerde = false;
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::SpillState::appendToPartition",
      std::function<void(exec::SpillState*)>([&](exec::SpillState* state) {
        const auto kind = state->testingSpillSerdeKind();
        if (kind.has_value()) {
          EXPECT_EQ(VectorSerde::Kind::kArrow, kind.value());
          sawArrowSerde = true;
        }
      }));
  queryCtx->testingOverrideConfigUnsafe({
      {core::QueryConfig::kSpillEnabled, "true"},
      {core::QueryConfig::kOrderBySpillEnabled, "true"},
      {core::QueryConfig::kSinglePartitionSpillSerdeKind, "Arrow"},
      {core::QueryConfig::kSpillNumPartitionBits, "0"},
      {core::QueryConfig::kJitLevel, "-1"},
  });
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  params.spillDirectory = spillDirectory->path;
  auto task = assertQueryOrdered(
      params, "SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST", {0});
  auto& planStats = toPlanStats(task->taskStats()).at(orderById);
  ASSERT_GT(planStats.spilledBytes, 0);
  ASSERT_GT(planStats.spilledInputBytes, 0);
  ASSERT_GT(planStats.spilledRows, 0);
  ASSERT_EQ(planStats.spilledPartitions, 1);
#ifndef NDEBUG
  ASSERT_TRUE(sawArrowSerde);
#endif
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_P(OrderByTest, spillWithMemoryLimit) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  constexpr int32_t kNumRows = 2000;
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({}, pool());
  const int32_t numBatches = 5;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }
  struct {
    uint64_t orderByMemLimit;
    bool expectSpill;

    std::string debugString() const {
      return fmt::format(
          "orderByMemLimit:{}, expectSpill:{}", orderByMemLimit, expectSpill);
    }
  } testSettings[] = {// Memory limit is disabled so spilling is not triggered.
                      {0, false},
                      // Memory limit is too small so always trigger spilling.
                      {1, true},
                      // Memory limit is too large so spilling is not triggered.
                      {1'000'000'000, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), kMaxBytes));
    auto results =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());
    auto task =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kOrderBySpillEnabled, true)
            .config(
                QueryConfig::kOrderBySpillMemoryThreshold,
                testData.orderByMemLimit)
            .assertResults(results);

    auto stats = task->taskStats().pipelineStats;
    ASSERT_EQ(
        testData.expectSpill, stats[0].operatorStats[1].spilledInputBytes > 0);
    ASSERT_EQ(testData.expectSpill, stats[0].operatorStats[1].spilledBytes > 0);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
}

DEBUG_ONLY_TEST_P(OrderByTest, reclaimDuringInputProcessing) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  struct {
    // 0: trigger reclaim with some input processed.
    // 1: trigger reclaim after all the inputs processed.
    int triggerCondition;
    bool spillEnabled;
    bool expectedReclaimable;

    std::string debugString() const {
      return fmt::format(
          "triggerCondition {}, spillEnabled {}, expectedReclaimable {}",
          triggerCondition,
          spillEnabled,
          expectedReclaimable);
    }
  } testSettings[] = {
      {0, true, true}, {1, true, true}, {0, false, false}, {1, false, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<int> numInputs{0};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
          ++numInputs;
          if (testData.triggerCondition == 0) {
            if (numInputs != 2) {
              return;
            }
          }
          if (testData.triggerCondition == 1) {
            if (numInputs != numBatches) {
              return;
            }
          }
          ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, testData.expectedReclaimable);
          if (testData.expectedReclaimable) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      if (testData.spillEnabled) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kOrderBySpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
    ASSERT_EQ(reclaimable, testData.expectedReclaimable);
    if (testData.expectedReclaimable) {
      ASSERT_GT(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }

    if (testData.expectedReclaimable) {
      reclaimAndRestoreCapacity(
          op,
          folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
          reclaimerStats_);
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      reclaimerStats_.reset();
      ASSERT_EQ(op->pool()->currentBytes(), 0);
    } else {
      BOLT_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
              reclaimerStats_),
          "");
    }

    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    if (testData.expectedReclaimable) {
      ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 1);
    } else {
      ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    }
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_P(OrderByTest, reclaimDuringReserve) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    const size_t size = i == 0 ? 100 : 40000;
    VectorFuzzer fuzzer({.vectorSize = size}, pool());
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
  auto expectedResult =
      AssertQueryBuilder(
          PlanBuilder()
              .values(batches)
              .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
              .planNode())
          .queryCtx(queryCtx)
          .copyResults(pool_.get());

  folly::EventCount driverWait;
  auto driverWaitKey = driverWait.prepareWait();
  folly::EventCount testWait;
  auto testWaitKey = testWait.prepareWait();

  Operator* op;
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "OrderBy") {
          ASSERT_FALSE(testOp->canReclaim());
          return;
        }
        op = testOp;
      })));

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            ASSERT_TRUE(op != nullptr);
            const std::string re(".*OrderBy");
            if (!RE2::FullMatch(pool->name(), re)) {
              return;
            }
            if (!injectOnce.exchange(false)) {
              return;
            }
            ASSERT_TRUE(op->canReclaim());
            uint64_t reclaimableBytes{0};
            const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
            ASSERT_TRUE(reclaimable);
            ASSERT_GT(reclaimableBytes, 0);
            auto* driver = op->testingOperatorCtx()->driver();
            SuspendedSection suspendedSection(driver);
            testWait.notify();
            driverWait.wait(driverWaitKey);
          })));

  std::thread taskThread([&]() {
    AssertQueryBuilder(
        PlanBuilder()
            .values(batches)
            .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
            .planNode())
        .queryCtx(queryCtx)
        .spillDirectory(tempDirectory->path)
        .config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kOrderBySpillEnabled, true)
        .maxDrivers(1)
        .assertResults(expectedResult);
  });

  testWait.wait(testWaitKey);
  ASSERT_TRUE(op != nullptr);
  auto task = op->testingOperatorCtx()->task();
  auto taskPauseWait = task->requestPause();
  taskPauseWait.wait();

  uint64_t reclaimableBytes{0};
  const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
  ASSERT_TRUE(op->canReclaim());
  ASSERT_TRUE(reclaimable);
  ASSERT_GT(reclaimableBytes, 0);

  reclaimAndRestoreCapacity(
      op,
      folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
      reclaimerStats_);
  ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
  ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
  reclaimerStats_.reset();
  ASSERT_EQ(op->pool()->currentBytes(), 0);

  driverWait.notify();
  Task::resume(task);

  taskThread.join();

  auto stats = task->taskStats().pipelineStats;
  ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
  ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 1);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_P(OrderByTest, reclaimDuringAllocation) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  const std::vector<bool> enableSpillings = {false, true};
  for (bool enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), kMaxBytes));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    Operator* op;
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
        })));

    std::atomic<bool> injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              ASSERT_TRUE(op != nullptr);
              const std::string re(".*OrderBy");
              if (!RE2::FullMatch(pool->name(), re)) {
                return;
              }
              if (!injectOnce.exchange(false)) {
                return;
              }
              ASSERT_EQ(op->canReclaim(), enableSpilling);
              uint64_t reclaimableBytes{0};
              const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
              ASSERT_EQ(reclaimable, enableSpilling);
              if (enableSpilling) {
                ASSERT_GE(reclaimableBytes, 0);
              } else {
                ASSERT_EQ(reclaimableBytes, 0);
              }
              auto* driver = op->testingOperatorCtx()->driver();
              SuspendedSection suspendedSection(driver);
              testWait.notify();
              driverWait.wait(driverWaitKey);
            })));

    std::thread taskThread([&]() {
      if (enableSpilling) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kOrderBySpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);
    if (enableSpilling) {
      ASSERT_GE(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }

    BOLT_ASSERT_THROW(
        op->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
            reclaimerStats_),
        "");

    driverWait.notify();
    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
    ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{0});
}

DEBUG_ONLY_TEST_P(OrderByTest, reclaimDuringOutputProcessing) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  const std::vector<bool> enableSpillings = {false, true};
  for (bool enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<bool> injectOnce{true};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
          if (!injectOnce.exchange(false)) {
            return;
          }
          ASSERT_EQ(op->canReclaim(), enableSpilling);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, enableSpilling);
          if (enableSpilling) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      if (enableSpilling) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kOrderBySpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);

    if (enableSpilling) {
      ASSERT_GT(reclaimableBytes, 0);
      reclaimerStats_.reset();
      reclaimAndRestoreCapacity(op, reclaimableBytes, reclaimerStats_);
      ASSERT_EQ(reclaimerStats_.reclaimedBytes, reclaimableBytes);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
      BOLT_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
              reclaimerStats_),
          "");
    }

    Task::resume(task);
    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
    ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 0);
}

DEBUG_ONLY_TEST_P(OrderByTest, abortDuringOutputProcessing) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<bool> injectOnce{true};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            return;
          }
          op = testOp;
          if (!injectOnce.exchange(false)) {
            return;
          }
          auto* driver = op->testingOperatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testWait.notify();
          driverWait.wait(driverWaitKey);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          BOLT_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    std::thread taskThread([&]() {
      BOLT_ASSERT_THROW(
          AssertQueryBuilder(
              PlanBuilder()
                  .values(batches)
                  .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                  .planNode())
              .queryCtx(queryCtx)
              .maxDrivers(1)
              .assertResults(expectedResult),
          "");
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    testData.abortFromRootMemoryPool ? abortPool(queryCtx->pool())
                                     : abortPool(op->pool());
    ASSERT_TRUE(op->pool()->aborted());
    ASSERT_TRUE(queryCtx->pool()->aborted());
    ASSERT_EQ(queryCtx->pool()->currentBytes(), 0);
    driverWait.notify();
    taskThread.join();
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_P(OrderByTest, abortDuringInputgProcessing) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<int> numInputs{0};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            return;
          }
          op = testOp;
          ++numInputs;
          if (numInputs != 2) {
            return;
          }
          auto* driver = op->testingOperatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testWait.notify();
          driverWait.wait(driverWaitKey);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          // Simulate the memory abort by memory arbitrator.
          BOLT_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    std::thread taskThread([&]() {
      BOLT_ASSERT_THROW(
          AssertQueryBuilder(
              PlanBuilder()
                  .values(batches)
                  .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                  .planNode())
              .queryCtx(queryCtx)
              .maxDrivers(1)
              .assertResults(expectedResult),
          "");
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    testData.abortFromRootMemoryPool ? abortPool(queryCtx->pool())
                                     : abortPool(op->pool());
    ASSERT_TRUE(op->pool()->aborted());
    ASSERT_TRUE(queryCtx->pool()->aborted());
    ASSERT_EQ(queryCtx->pool()->currentBytes(), 0);
    driverWait.notify();
    taskThread.join();
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_P(OrderByTest, spillWithNoMoreOutput) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 1024, 4 << 20);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId orderNodeId;
  const auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors)
          .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
          .capturePlanNodeId(orderNodeId)
          .planNode();

  const auto expectedResult = AssertQueryBuilder(plan).copyResults(pool_.get());

  std::atomic_int numOutputs{0};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "OrderBy") {
          return;
        }
        if (!op->testingNoMoreInput()) {
          return;
        }
        if (++numOutputs != 2) {
          return;
        }
        ASSERT_TRUE(!op->isFinished());
        op->reclaim(1'000'000'000, reclaimerStats_);
        ASSERT_EQ(reclaimerStats_.reclaimedBytes, 0);
      })));

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task =
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          // Set output buffer size to extreme large to read all the
          // output rows in one vector.
          .config(QueryConfig::kPreferredOutputBatchRows, 1'000'000'000)
          .config(QueryConfig::kMaxOutputBatchRows, 1'000'000'000)
          .config(QueryConfig::kPreferredOutputBatchBytes, 1'000'000'000)
          .config(QueryConfig::kMaxSpillBytes, 1)
          .maxDrivers(1)
          .assertResults(expectedResult);
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& planStats = taskStats.at(orderNodeId);
  ASSERT_EQ(planStats.spilledBytes, 0);
  ASSERT_EQ(planStats.spilledRows, 0);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_P(OrderByTest, maxSpillBytes) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 1024, 15 << 20);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId orderNodeId;
  const auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors)
          .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
          .capturePlanNodeId(orderNodeId)
          .planNode();
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());

  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {16 << 20, true}, {0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    try {
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->path)
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          // Set a small capacity to trigger threshold based spilling
          .config(QueryConfig::kOrderBySpillMemoryThreshold, 5 << 20)
          .config(QueryConfig::kMaxSpillBytes, testData.maxSpilledBytes)
          .config(QueryConfig::kRowBasedSpillMode, "")
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const BoltRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find(
              "Query exceeded per-query local spill limit of 16.00MB"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), bytedance::bolt::error_code::kSpillLimitExceeded);
    }
  }
}

DEBUG_ONLY_TEST_P(OrderByTest, reclaimFromOrderBy) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  std::vector<RowVectorPtr> vectors = createVectors(8, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  std::atomic_int numInputs{0};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "OrderBy") {
          return;
        }
        if (++numInputs != 5) {
          return;
        }
        auto* driver = op->testingOperatorCtx()->driver();
        SuspendedSection suspendedSection(driver);
        memory::testingRunArbitration();
      })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  core::PlanNodeId orderById;
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          .plan(PlanBuilder()
                    .values(vectors)
                    .orderBy({"c0 ASC NULLS LAST"}, false)
                    .capturePlanNodeId(orderById)
                    .planNode())
          .assertResults("SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST");
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& planStats = taskStats.at(orderById);
  ASSERT_GT(planStats.spilledBytes, 0);
  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_P(OrderByTest, reclaimFromEmptyOrderBy) {
  if (GetParam().useGPU) {
    GTEST_SKIP() << "GPU OrderBy does not support spilling\n";
  }

  const std::vector<RowVectorPtr> vectors =
      createVectors(8, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "OrderBy") {
          return;
        }

        if (!injectOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
      })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          .plan(PlanBuilder()
                    .values(vectors)
                    .orderBy({"c0 ASC NULLS LAST"}, false)
                    .planNode())
          .assertResults("SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST");
  // Verify no spill has been triggered.
  const auto stats = task->taskStats().pipelineStats;
  ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
  ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
}

INSTANTIATE_GPU_TEST_SUITE_P(OrderByTestOnCPUOrGPU, OrderByTest);

} // namespace bytedance::bolt::exec::test
