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

#include <gtest/gtest.h>
#include <atomic>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/process/ProcessBase.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HashTableSimdTestUtil.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using bytedance::bolt::core::QueryConfig;

namespace bytedance::bolt::exec::test {

class HashTableSimdAggregationTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
  }
};

namespace {
void assertAggregationMatchesScalar(
    const core::PlanNodePtr& plan,
    memory::MemoryPool* pool) {
  for (bool adaptive : {true, false}) {
    assertQueryResultsEqualWithSimdHashTable(
        plan,
        pool,
        "HashAggregation",
        {{core::QueryConfig::kHashAdaptivityEnabled,
          adaptive ? "true" : "false"}});
  }
}
} // namespace

TEST_F(HashTableSimdAggregationTest, varcharSingleKey) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // 8 input batches with a deliberately diverse VARCHAR key palette.
  auto palette = hashTableSimdVarcharValues();

  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 8; ++b) {
    constexpr int32_t kRowsPerBatch = 1'000;
    batches.push_back(makeRowVector(
        {"k", "v"},
        {makeFlatVector<std::string>(
             kRowsPerBatch,
             [&](auto row) { return palette[(row + b * 7) % palette.size()]; },
             [b](auto row) { return (row + b) % 17 == 0; }),
         makeFlatVector<int64_t>(
             kRowsPerBatch, [b](auto row) { return (row * 31 + b) % 997; })}));
  }

  auto plan =
      PlanBuilder()
          .values(batches)
          .singleAggregation(
              {"k"},
              {"sum(v) AS s", "min(v) AS mn", "max(v) AS mx", "count(v) AS c"})
          .planNode();
  assertAggregationMatchesScalar(plan, pool());
}

TEST_F(HashTableSimdAggregationTest, mixedKeyTypes) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // 3-key group-by mixing types: BIGINT + VARCHAR + DOUBLE.
  // Every dimension carries adversarial values:
  //   - BIGINT: includes INT64_MIN, INT64_MAX, 0.
  //   - VARCHAR: empty + same-prefix + multi-chunk + inline-boundary.
  //   - DOUBLE: +-0, +-Inf, canonical NaN, denormal.
  std::vector<int64_t> bigintPalette = {
      std::numeric_limits<int64_t>::min(),
      std::numeric_limits<int64_t>::min() + 1,
      -42,
      -1,
      0,
      1,
      42,
      std::numeric_limits<int64_t>::max() - 1,
      std::numeric_limits<int64_t>::max(),
  };
  std::vector<std::string> varcharPalette = {
      "",
      "x",
      "abcd",
      std::string(13, 'M'),
      std::string("PRFX") + std::string(60, 'A'),
      std::string("PRFX") + std::string(60, 'B'),
      std::string(16 * 1024, 'Y'),
  };
  std::vector<double> doublePalette = {
      0.0,
      -0.0,
      1.0,
      -1.0,
      std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::quiet_NaN(),
      std::numeric_limits<double>::denorm_min(),
      std::numeric_limits<double>::min(),
      std::numeric_limits<double>::max(),
  };

  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 6; ++b) {
    constexpr int32_t kRowsPerBatch = 800;
    batches.push_back(makeRowVector(
        {"k1", "k2", "k3", "v"},
        {makeFlatVector<int64_t>(
             kRowsPerBatch,
             [&](auto row) {
               return bigintPalette[(row * 3 + b) % bigintPalette.size()];
             },
             [b](auto row) { return (row + b) % 23 == 0; }),
         makeFlatVector<std::string>(
             kRowsPerBatch,
             [&](auto row) {
               return varcharPalette[(row * 5 + b) % varcharPalette.size()];
             },
             [b](auto row) { return (row + b) % 19 == 0; }),
         makeFlatVector<double>(
             kRowsPerBatch,
             [&](auto row) {
               return doublePalette[(row * 7 + b) % doublePalette.size()];
             },
             [b](auto row) { return (row + b) % 29 == 0; }),
         makeFlatVector<int64_t>(
             kRowsPerBatch, [](auto row) { return row % 113; })}));
  }

  auto plan = PlanBuilder()
                  .values(batches)
                  .singleAggregation(
                      {"k1", "k2", "k3"}, {"sum(v) AS s", "count(v) AS c"})
                  .planNode();
  assertAggregationMatchesScalar(plan, pool());
}

TEST_F(HashTableSimdAggregationTest, distinct) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // Distinct (group-by with no aggregates). VARCHAR with diverse sizes.
  std::vector<std::string> palette = {
      "",
      "a",
      "ab",
      "abc",
      "abcd",
      "abcde",
      std::string(11, 'L'),
      std::string(12, 'M'),
      std::string(13, 'N'),
      std::string(64, 'O'),
      std::string("PRFX") + std::string(60, 'P'),
      std::string("PRFX") + std::string(60, 'Q'),
      std::string(16 * 1024, 'R'),
  };
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 5; ++b) {
    constexpr int32_t kRowsPerBatch = 1'200;
    batches.push_back(makeRowVector(
        {"k"},
        {makeFlatVector<std::string>(
            kRowsPerBatch,
            [&](auto row) { return palette[(row + b * 11) % palette.size()]; },
            [b](auto row) { return (row + b) % 37 == 0; })}));
  }
  auto plan =
      PlanBuilder().values(batches).singleAggregation({"k"}, {}).planNode();
  assertAggregationMatchesScalar(plan, pool());
}

TEST_F(HashTableSimdAggregationTest, manyBatchesForRehash) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // Many small batches with steadily growing distinct keys (each batch
  // adds previously-unseen keys) to force multiple mid-stream rehashes
  // while SIMD layout is active.
  std::vector<RowVectorPtr> batches;
  constexpr int32_t kBatches = 32;
  constexpr int32_t kRowsPerBatch = 257; // not multiple of 8/16.
  for (int32_t b = 0; b < kBatches; ++b) {
    batches.push_back(makeRowVector(
        {"k", "v"},
        {makeFlatVector<std::string>(
             kRowsPerBatch,
             [b](auto row) {
               // Every batch introduces fresh keys; some collide with
               // earlier batches via the modulo so we exercise both
               // insert-new and probe-existing.
               return fmt::format(
                   "PRFX_{}_{}", row + b * 100, (row * b * 17) % 257);
             }),
         makeFlatVector<int64_t>(
             kRowsPerBatch, [b](auto row) { return (row * 13 + b) % 991; })}));
  }
  auto plan =
      PlanBuilder()
          .values(batches)
          .singleAggregation(
              {"k"},
              {"sum(v) AS s", "min(v) AS mn", "max(v) AS mx", "count(*) AS c"})
          .planNode();
  assertAggregationMatchesScalar(plan, pool());
}

TEST_F(HashTableSimdAggregationTest, nullKeyMix) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // Heavy null pattern in keys: every 2nd row is null on key1, every
  // 3rd on key2. Tests the deselectRowsWithNulls path and SIMD probe
  // with sparse selection.
  std::vector<RowVectorPtr> batches;
  for (int32_t b = 0; b < 6; ++b) {
    constexpr int32_t kRows = 1'009;
    batches.push_back(makeRowVector(
        {"k1", "k2", "v"},
        {makeFlatVector<int64_t>(
             kRows,
             [b](auto row) { return (row * 7 + b * 31) % 53; },
             [](auto row) { return row % 2 == 0; }),
         makeFlatVector<std::string>(
             kRows,
             [b](auto row) { return fmt::format("v_{}", (row + b) % 41); },
             [](auto row) { return row % 3 == 0; }),
         makeFlatVector<int64_t>(kRows, [](auto row) { return row % 100; })}));
  }
  auto plan = PlanBuilder()
                  .values(batches)
                  .singleAggregation({"k1", "k2"}, {"sum(v) AS s"})
                  .planNode();
  assertAggregationMatchesScalar(plan, pool());
}

TEST_F(HashTableSimdAggregationTest, withSpill) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // SIMD + spill: aggregation builds a large hash table, then spills.
  // Verifies that SIMD layout is correctly serialized + deserialized
  // and that the final result matches the scalar path.
  std::vector<RowVectorPtr> batches;
  constexpr int32_t kBatches = 20;
  for (int32_t b = 0; b < kBatches; ++b) {
    constexpr int32_t kRows = 4'000;
    batches.push_back(makeRowVector(
        {"k", "v"},
        {makeFlatVector<std::string>(
             kRows,
             [b](auto row) {
               return fmt::format("PRFX_{}_{}_{}", row, b, (row * 7919) % 4093);
             }),
         makeFlatVector<int64_t>(
             kRows, [b](auto row) { return row + b * 1000; })}));
  }

  auto runWithConfig = [&](bool enableSimd) {
    auto tempDir = exec::test::TempDirectoryPath::create();
    auto plan = PlanBuilder()
                    .values(batches)
                    .singleAggregation({"k"}, {"sum(v) AS s"})
                    .planNode();
    std::shared_ptr<Task> task;
    auto result =
        AssertQueryBuilder(plan)
            .spillDirectory(tempDir->getPath())
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kAggregationSpillEnabled, "true")
            .config(core::QueryConfig::kSpillStartPartitionBit, "29")
            .config(
                core::QueryConfig::kSimdHashTableEnabled,
                enableSimd ? "true" : "false")
            // Force spill via the testing spill percentage (bolt-specific).
            .config(QueryConfig::kTestingSpillPct, "100")
            .copyResults(pool(), task);
    EXPECT_GT(toPlanStats(task->taskStats()).at(plan->id()).spilledBytes, 0);
    return result;
  };
  auto scalarResult = runWithConfig(false);
  auto simdResult = runWithConfig(true);
  ASSERT_TRUE(test::assertEqualResults(
      std::vector<RowVectorPtr>{simdResult},
      std::vector<RowVectorPtr>{scalarResult}));
}

TEST_F(HashTableSimdAggregationTest, encodedKeys) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // Operator-level encoded input: dictionary + constant key columns.
  // HashTableTest covers VectorHasher encodings directly; this verifies
  // the same cases survive the full Aggregation operator pipeline with
  // SIMD explicitly enabled.
  std::vector<std::string> palette = {
      "",
      "a",
      "abcd",
      std::string(12, 'I'),
      std::string(13, 'J'),
      std::string("PRFX") + std::string(60, 'A'),
      std::string("PRFX") + std::string(60, 'B'),
      std::string(16 * 1024, 'K'),
  };

  std::vector<RowVectorPtr> batches;
  for (int32_t b = 0; b < 7; ++b) {
    constexpr int32_t kRows = 777;
    auto baseKey = makeFlatVector<std::string>(
        palette.size() * 3,
        [&](auto row) { return palette[(row + b) % palette.size()]; },
        [b](auto row) { return (row + b) % 29 == 0; });
    auto indices =
        makeIndices(kRows, [b](auto row) { return (row * 37 + b * 11) % 24; });
    auto dictionaryKey =
        BaseVector::wrapInDictionary(nullptr, indices, kRows, baseKey);

    auto constantBase = makeFlatVector<std::string>({"", "constant"});
    auto constantEmptyKey = BaseVector::wrapInConstant(kRows, 0, constantBase);

    batches.push_back(makeRowVector(
        {"dict_k", "const_k", "v"},
        {dictionaryKey,
         constantEmptyKey,
         makeFlatVector<int64_t>(
             kRows, [b](auto row) { return (row * 13 + b) % 997; })}));
  }

  auto plan = PlanBuilder()
                  .values(batches)
                  .singleAggregation(
                      {"dict_k", "const_k"},
                      {"sum(v) AS s", "min(v) AS mn", "max(v) AS mx"})
                  .planNode();
  assertAggregationMatchesScalar(plan, pool());
}

TEST_F(HashTableSimdAggregationTest, emptyStringFromTpcdsPattern) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // TPC-DS time_dim.t_meal_time legitimately contains empty strings for
  // non-meal hours. This is a focused operator-level regression for the
  // empty-inline StringView bug: many empty keys must collapse to a single
  // group in SIMD just as in scalar.
  std::vector<std::string> mealByHour = {
      "",          "",          "",          "",          "", "",
      "breakfast", "breakfast", "breakfast", "breakfast", "", "",
      "lunch",     "lunch",     "lunch",     "",          "", "dinner",
      "dinner",    "dinner",    "",          "",          "", "",
  };

  std::vector<RowVectorPtr> batches;
  for (int32_t b = 0; b < 9; ++b) {
    constexpr int32_t kRows = 1'003;
    batches.push_back(makeRowVector(
        {"meal_time", "v"},
        {makeFlatVector<std::string>(
             kRows,
             [&](auto row) {
               return mealByHour[(row * 17 + b) % mealByHour.size()];
             }),
         makeFlatVector<int64_t>(
             kRows, [b](auto row) { return (row + b) % 31; })}));
  }

  auto plan =
      PlanBuilder()
          .values(batches)
          .singleAggregation(
              {"meal_time"},
              {"count(*) AS c", "sum(v) AS s", "min(v) AS mn", "max(v) AS mx"})
          .planNode();
  assertAggregationMatchesScalar(plan, pool());
}

TEST_F(HashTableSimdAggregationTest, forcedNormalizedKey) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // Operator-level aggregation that naturally selects kNormalizedKey:
  // two finite BIGINT ranges whose product exceeds kArrayHashMaxSize, but
  // still fits in normalized-key encoding. This exercises
  // groupNormalizedKeyProbeSimd with SIMD enabled.
  std::vector<RowVectorPtr> batches;
  for (int32_t b = 0; b < 8; ++b) {
    constexpr int32_t kRows = 1'027;
    batches.push_back(makeRowVector(
        {"k1", "k2", "v"},
        {makeFlatVector<int64_t>(
             kRows,
             [b](auto row) {
               return static_cast<int64_t>((row * 131 + b * 17) % 4097) - 2048;
             }),
         makeFlatVector<int64_t>(
             kRows,
             [b](auto row) {
               return static_cast<int64_t>((row * 257 + b * 31) % 4099) - 2049;
             }),
         makeFlatVector<int64_t>(
             kRows, [b](auto row) { return (row * 19 + b) % 1009; })}));
  }

  auto plan =
      PlanBuilder()
          .values(batches)
          .singleAggregation(
              {"k1", "k2"}, {"sum(v) AS s", "count(*) AS c", "min(v) AS mn"})
          .planNode();
  std::atomic<BaseHashTable::HashMode> mode{BaseHashTable::HashMode::kArray};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::HashTable::setHashMode",
      std::function<void(void*)>([&](void* newMode) {
        mode = *reinterpret_cast<BaseHashTable::HashMode*>(newMode);
      }));
  auto scalarResult =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kSimdHashTableEnabled, "false")
          .copyResults(pool());
#ifndef NDEBUG
  EXPECT_EQ(mode, BaseHashTable::HashMode::kNormalizedKey);
  mode = BaseHashTable::HashMode::kArray;
#endif
  auto simdResult =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kSimdHashTableEnabled, "true")
          .copyResults(pool());
#ifndef NDEBUG
  EXPECT_EQ(mode, BaseHashTable::HashMode::kNormalizedKey);
#endif
  ASSERT_TRUE(test::assertEqualResults(
      std::vector<RowVectorPtr>{simdResult},
      std::vector<RowVectorPtr>{scalarResult}));
}

} // namespace bytedance::bolt::exec::test
