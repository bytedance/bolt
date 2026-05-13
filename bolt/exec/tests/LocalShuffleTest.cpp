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

#include <common/memory/HashStringAllocator.h>
#include <core/PlanNode.h>
#include <re2/re2.h>

#include <fmt/format.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/ArbitratorTestUtil.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/vector/VectorPrinter.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "folly/experimental/EventCount.h"

#include <boost/algorithm/string.hpp>
#include <algorithm>
#include <cstdint>
#include <cstdlib>

#include <ranges>
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::common::testutil;
using namespace bytedance::bolt::core;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;
namespace bytedance::bolt::exec::test {
class LocalShuffleTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      this->registerVectorSerde();
    }
  }

  RowVectorPtr createVector(size_t startRowIdx, size_t batchSize) {
    auto c0 = makeFlatVector<int32_t>(
        batchSize,
        [startRowIdx](auto row) { return startRowIdx + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<int32_t>(
        batchSize,
        [startRowIdx](auto row) { return (startRowIdx + row) * 10; },
        nullEvery(7));
    auto c2 = makeFlatVector<StringView>(
        batchSize,
        [startRowIdx](auto row) {
          return StringView::makeInline(
              fmt::format("c2-{}", startRowIdx + row));
        },
        nullEvery(11));
    auto c3 = makeFlatVector<StringView>(
        batchSize,
        [startRowIdx](auto row) {
          return StringView::makeInline(
              fmt::format("c3-{}", (startRowIdx + row) * 10));
        },
        nullEvery(13));
    return makeRowVector({c0, c1, c2, c3});
  }

  std::vector<RowVectorPtr> createSeqVectors(
      size_t batchSize,
      size_t totalRows) {
    std::vector<RowVectorPtr> vectors;
    for (size_t i = 0; i < totalRows; i += batchSize) {
      size_t currentBatchSize = std::min(batchSize, totalRows - i);
      vectors.push_back(createVector(i, currentBatchSize));
    }
    return vectors;
  }

  std::vector<RowVectorPtr>
  runQuery(std::vector<RowVectorPtr> input, size_t poolSize, size_t seed) {
    core::PlanNodeId localShuffleId;
    CursorParameters params;
    auto builder = PlanBuilder().values(input).localShuffle(seed);
    params.planNode = builder.capturePlanNodeId(localShuffleId).planNode();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kLocalShufflePoolSize, std::to_string(poolSize)},
         {core::QueryConfig::kPreferredOutputBatchRows, "8192"}});
    params.queryCtx = queryCtx;
    auto cursorResult = readCursor(params, [](Task*) {});
    std::vector<RowVectorPtr> copies;
    if (cursorResult.second.empty()) {
      return copies;
    }

    auto totalCount = 0;
    for (const auto& result : cursorResult.second) {
      auto copy =
          BaseVector::create<RowVector>(result->type(), result->size(), pool());
      copy->copy(result.get(), 0, 0, result->size());
      copies.push_back(copy);
    }
    return copies;
  }

  std::vector<std::vector<int32_t>>
  predictShuffle(size_t poolSize, size_t totalRows, int64_t seed) {
    std::mt19937_64 gen(seed);
    std::vector<std::vector<int32_t>> result;
    for (int32_t i = 0; i < totalRows; i += poolSize) {
      std::vector<int32_t> batch;
      for (int32_t j = i; j < i + poolSize && j < totalRows; ++j) {
        batch.push_back(j);
      }
      std::shuffle(batch.begin(), batch.end(), gen);
      result.push_back(batch);
    }

    return result;
  }
  void runTest(
      size_t poolSize,
      size_t inputBatchSize,
      size_t totalRows,
      int64_t seed) {
    int64_t calculatedPoolSize = std::max(
        poolSize,
        inputBatchSize * ((poolSize + inputBatchSize - 1) / inputBatchSize));
    auto expectedResults = predictShuffle(calculatedPoolSize, totalRows, seed);
    std::vector<RowVectorPtr> results = runQuery(
        createSeqVectors(inputBatchSize, totalRows), calculatedPoolSize, seed);
    EXPECT_EQ(results.size(), expectedResults.size());
    for (int i = 0; i < results.size(); ++i) {
      EXPECT_EQ(results[i]->size(), expectedResults[i].size());
      const auto& c0 = DecodedVector(*results[i]->childAt(0));
      const auto& c1 = DecodedVector(*results[i]->childAt(1));
      const auto& c2 = DecodedVector(*results[i]->childAt(2));
      const auto& c3 = DecodedVector(*results[i]->childAt(3));
      for (int j = 0; j < results[i]->size(); ++j) {
        int32_t expected = expectedResults[i][j];
        if (expected % inputBatchSize % 5 == 0) {
          EXPECT_TRUE(c0.isNullAt(j))
              << fmt::format("expected is null at {}", j);
        } else {
          EXPECT_EQ(expected, c0.valueAt<int32_t>(j)) << fmt::format(
              "expected {} but got {} at {}",
              expected,
              c0.valueAt<int32_t>(j),
              j);
        }
        if (expected % inputBatchSize % 7 == 0) {
          EXPECT_TRUE(c1.isNullAt(j))
              << fmt::format("expected is null at {}", j);
        } else {
          EXPECT_EQ(expected * 10, c1.valueAt<int32_t>(j)) << fmt::format(
              "expected {} but got {} at {}",
              expected * 10,
              c1.valueAt<int32_t>(j),
              j);
        }
        if (expected % inputBatchSize % 11 == 0) {
          EXPECT_TRUE(c2.isNullAt(j))
              << fmt::format("expected is null at {}", j);
        } else {
          EXPECT_EQ(fmt::format("c2-{}", expected), c2.valueAt<StringView>(j))
              << fmt::format(
                     "expected {} but got {} at {}",
                     fmt::format("c2-{}", expected),
                     c2.valueAt<StringView>(j),
                     j);
        }
        if (expected % inputBatchSize % 13 == 0) {
          EXPECT_TRUE(c3.isNullAt(j))
              << fmt::format("expected is null at {}", j);
        } else {
          EXPECT_EQ(
              fmt::format("c3-{}", expected * 10),
              c3.valueAt<StringView>(j).str())
              << fmt::format(
                     "expected {} but got {} at {}",
                     fmt::format("c3-{}", expected * 10),
                     c3.valueAt<StringView>(j).str(),
                     j);
        }
      }
    }
  }
};

TEST_F(LocalShuffleTest, singleBatchLessThanPoolSize) {
  runTest(1024, 2048, 999, 123);
}

TEST_F(LocalShuffleTest, singleBatchEqualToPoolSize) {
  runTest(1024, 2048, 1024, 123);
}

TEST_F(LocalShuffleTest, singleBatchGreaterThanPoolSize) {
  runTest(1024, 2048, 2048, 123);
}

TEST_F(LocalShuffleTest, multipleBatchesLessThanPoolSize) {
  runTest(2048, 1024, 2047, 123);
}

TEST_F(LocalShuffleTest, multipleBatchesEqualPoolSize) {
  runTest(2048, 1024, 2048, 123);
}

TEST_F(LocalShuffleTest, multipleBatchesGreaterThanPoolSize) {
  runTest(2048, 1024, 2049, 123);
}

TEST_F(LocalShuffleTest, empytyBatches) {
  runTest(2048, 1024, 0, 123);
}

TEST_F(LocalShuffleTest, random) {
  auto input1 = createSeqVectors(1024, 2049);
  auto input2 = createSeqVectors(1024, 2049);

  core::PlanNodeId localShuffleSortId1;
  CursorParameters params1;
  auto builder1 = PlanBuilder().values(input1).localShuffle(123).orderBy(
      {"c0", "c1", "c2", "c3"}, false);
  params1.planNode = builder1.capturePlanNodeId(localShuffleSortId1).planNode();
  auto queryCtx1 = core::QueryCtx::create(executor_.get());
  queryCtx1->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kLocalShufflePoolSize, "2048"},
       {core::QueryConfig::kPreferredOutputBatchRows, "8192"}});
  params1.queryCtx = queryCtx1;
  auto cursorResult1 = readCursor(params1, [](Task*) {});

  core::PlanNodeId localShuffleSortId2;
  CursorParameters params2;
  auto builder2 = PlanBuilder().values(input2).localShuffle(456).orderBy(
      {"c0", "c1", "c2", "c3"}, false);
  params2.planNode = builder2.capturePlanNodeId(localShuffleSortId2).planNode();
  auto queryCtx2 = core::QueryCtx::create(executor_.get());
  queryCtx2->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kLocalShufflePoolSize, "2048"},
       {core::QueryConfig::kPreferredOutputBatchRows, "8192"}});
  params2.queryCtx = queryCtx2;
  auto cursorResult2 = readCursor(params2, [](Task*) {});

  EXPECT_EQ(cursorResult1.second.size(), cursorResult2.second.size());
  for (int i = 0; i < cursorResult1.second.size(); ++i) {
    EXPECT_EQ(cursorResult1.second[i]->size(), cursorResult2.second[i]->size());
    const auto& c0_1 = DecodedVector(*cursorResult1.second[i]->childAt(0));
    const auto& c1_1 = DecodedVector(*cursorResult1.second[i]->childAt(1));
    const auto& c2_1 = DecodedVector(*cursorResult1.second[i]->childAt(2));
    const auto& c3_1 = DecodedVector(*cursorResult1.second[i]->childAt(3));
    const auto& c0_2 = DecodedVector(*cursorResult2.second[i]->childAt(0));
    const auto& c1_2 = DecodedVector(*cursorResult2.second[i]->childAt(1));
    const auto& c2_2 = DecodedVector(*cursorResult2.second[i]->childAt(2));
    const auto& c3_2 = DecodedVector(*cursorResult2.second[i]->childAt(3));
    for (int j = 0; j < cursorResult1.second[i]->size(); ++j) {
      if (c0_1.isNullAt(j)) {
        EXPECT_TRUE(c0_2.isNullAt(j));
      } else {
        EXPECT_EQ(c0_1.valueAt<int32_t>(j), c0_2.valueAt<int32_t>(j));
      }
      if (c1_1.isNullAt(j)) {
        EXPECT_TRUE(c1_2.isNullAt(j));
      } else {
        EXPECT_EQ(c1_1.valueAt<int32_t>(j), c1_2.valueAt<int32_t>(j));
      }
      if (c2_1.isNullAt(j)) {
        EXPECT_TRUE(c2_2.isNullAt(j));
      } else {
        EXPECT_EQ(
            c2_1.valueAt<StringView>(j).str(),
            c2_2.valueAt<StringView>(j).str());
      }
      if (c3_1.isNullAt(j)) {
        EXPECT_TRUE(c3_2.isNullAt(j));
      } else {
        EXPECT_EQ(
            c3_1.valueAt<StringView>(j).str(),
            c3_2.valueAt<StringView>(j).str());
      }
    }
  }
}

} // namespace bytedance::bolt::exec::test
