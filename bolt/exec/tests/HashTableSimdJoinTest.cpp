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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/process/ProcessBase.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HashTableSimdTestUtil.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

namespace bytedance::bolt::exec::test {

class HashTableSimdJoinTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  // Build vectors share the adversarial VARCHAR palette but only a
  // subset, so probe-side rows split between hits and misses.
  RowVectorPtr makeVarcharBuildVector(int32_t numRows) {
    auto palette = hashTableSimdVarcharValues();
    return makeRowVector(
        {"u_k", "u_v"},
        {makeFlatVector<std::string>(
             numRows,
             [&](auto row) { return palette[row % (palette.size() - 2)]; }),
         makeFlatVector<int64_t>(
             numRows, [](auto row) { return row * 31 % 991; })});
  }

  // Probe vectors include every palette entry, so the last two
  // (different multi-chunk and a non-existent key) miss.
  RowVectorPtr makeVarcharProbeVector(int32_t numRows) {
    auto palette = hashTableSimdVarcharValues();
    palette.push_back(std::string(16 * 1024, 'A')); // miss.
    palette.push_back("zzzz_no_match"); // miss.
    return makeRowVector(
        {"t_k", "t_v"},
        {makeFlatVector<std::string>(
             numRows,
             [&](auto row) { return palette[row % palette.size()]; },
             [](auto row) { return row % 41 == 0; }),
         makeFlatVector<int64_t>(numRows, [](auto row) { return row + 1; })});
  }
};

TEST_F(HashTableSimdJoinTest, inner) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(2'000)};
  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 4; ++b) {
    probeVectors.push_back(makeVarcharProbeVector(1'500));
  }
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v", "u_v"},
                      core::JoinType::kInner)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, leftOuter) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(1'200)};
  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 3; ++b) {
    probeVectors.push_back(makeVarcharProbeVector(1'700));
  }
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v", "u_v"},
                      core::JoinType::kLeft)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, rightOuter) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(2'500)};
  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 2; ++b) {
    probeVectors.push_back(makeVarcharProbeVector(1'200));
  }
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v", "u_k", "u_v"},
                      core::JoinType::kRight)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, antiJoin) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(900)};
  std::vector<RowVectorPtr> probeVectors{makeVarcharProbeVector(1'500)};
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v"},
                      core::JoinType::kAnti)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, leftSemiFilter) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(1'100)};
  std::vector<RowVectorPtr> probeVectors{makeVarcharProbeVector(1'300)};
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v"},
                      core::JoinType::kLeftSemiFilter)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, multiKeyMixedTypes) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // Two-key join (BIGINT + VARCHAR). VARCHAR carries the adversarial
  // diversity. BIGINT covers extremes.
  std::vector<int64_t> bigintPalette = {
      std::numeric_limits<int64_t>::min(),
      -1,
      0,
      1,
      42,
      std::numeric_limits<int64_t>::max(),
  };
  auto varcharPalette = hashTableSimdVarcharValues();

  auto buildVector = makeRowVector(
      {"u_k1", "u_k2", "u_v"},
      {makeFlatVector<int64_t>(
           1'500,
           [&](auto row) { return bigintPalette[row % bigintPalette.size()]; }),
       makeFlatVector<std::string>(
           1'500,
           [&](auto row) {
             return varcharPalette[(row * 3) % varcharPalette.size()];
           }),
       makeFlatVector<int64_t>(
           1'500, [](auto row) { return row * 7 % 1009; })});

  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 3; ++b) {
    probeVectors.push_back(makeRowVector(
        {"t_k1", "t_k2", "t_v"},
        {makeFlatVector<int64_t>(
             1'300,
             [&](auto row) {
               return bigintPalette[(row + b) % bigintPalette.size()];
             },
             [](auto row) { return row % 47 == 0; }),
         makeFlatVector<std::string>(
             1'300,
             [&](auto row) {
               return varcharPalette[(row + b * 5) % varcharPalette.size()];
             },
             [](auto row) { return row % 53 == 0; }),
         makeFlatVector<int64_t>(1'300, [](auto row) { return row; })}));
  }
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k1", "t_k2"},
                      {"u_k1", "u_k2"},
                      PlanBuilder(idGen).values({buildVector}).planNode(),
                      "",
                      {"t_k1", "t_k2", "t_v", "u_v"},
                      core::JoinType::kInner)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, hotKeyHighDuplicateBuild) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  // Build side is dominated by one hot key (~2000 duplicates) plus a
  // smaller tail of unique keys; probe-side picks the hot key for ~50%
  // of rows so the SIMD survivor compaction must walk a long bucket
  // chain. Sized to keep Cartesian output manageable.
  auto buildVector = makeRowVector(
      {"u_k", "u_v"},
      {makeFlatVector<std::string>(
           2'000,
           [](auto row) {
             // 80% hot key, 20% tail.
             return row % 5 == 0 ? fmt::format("tail_{}", row) : "HOT_KEY";
           }),
       makeFlatVector<int64_t>(2'000, [](auto row) { return row; })});

  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 2; ++b) {
    probeVectors.push_back(makeRowVector(
        {"t_k", "t_v"},
        {makeFlatVector<std::string>(
             400,
             [b](auto row) {
               int r = (row + b) % 10;
               if (r < 5) {
                 return std::string("HOT_KEY"); // ~50% hot.
               }
               if (r < 8) {
                 return fmt::format("tail_{}", row * 5);
               }
               return fmt::format("miss_{}", row);
             }),
         makeFlatVector<int64_t>(
             400, [](auto row) { return row * 13 % 991; })}));
  }
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values({buildVector}).planNode(),
                      "",
                      {"t_k", "t_v", "u_v"},
                      core::JoinType::kInner)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, parallelBuild) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::HashTable::setHashMode",
      std::function<void(void*)>([](void* mode) {
        *reinterpret_cast<BaseHashTable::HashMode*>(mode) =
            BaseHashTable::HashMode::kHash;
      }));
  // Multi-driver execution: triggers the parallel HashBuild SIMD path
  // through HashTable::parallelJoinBuild().
  std::vector<RowVectorPtr> buildVectors;
  for (int b = 0; b < 8; ++b) {
    buildVectors.push_back(makeRowVector(
        {"u_k", "u_v"},
        {makeFlatVector<std::string>(
             2'000,
             [b](auto row) {
               return fmt::format("PRFX_{}_{}", row, (row * b * 17) % 257);
             }),
         makeFlatVector<int64_t>(
             2'000, [b](auto row) { return row + b * 1000; })}));
  }
  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 4; ++b) {
    probeVectors.push_back(makeRowVector(
        {"t_k", "t_v"},
        {makeFlatVector<std::string>(
             1'500,
             [b](auto row) {
               return fmt::format("PRFX_{}_{}", row, (row * b * 17) % 257);
             }),
         makeFlatVector<int64_t>(
             1'500, [](auto row) { return row * 31 % 1013; })}));
  }
  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors, true)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors, true).planNode(),
                      "",
                      {"t_k", "t_v", "u_v"},
                      core::JoinType::kInner)
                  .planNode();

  auto runWithDrivers = [&](bool enableSimd) {
    return AssertQueryBuilder(plan)
        .config(
            core::QueryConfig::kSimdHashTableEnabled,
            enableSimd ? "true" : "false")
        .config(
            core::QueryConfig::kMinTableRowsForParallelJoinBuild,
            "1000") // force parallel build path
        .maxDrivers(4)
        .copyResults(pool_.get());
  };
  auto scalarResult = runWithDrivers(false);
  auto simdResult = runWithDrivers(true);
  ASSERT_TRUE(test::assertEqualResults(
      std::vector<RowVectorPtr>{simdResult},
      std::vector<RowVectorPtr>{scalarResult}));
}

TEST_F(HashTableSimdJoinTest, fullOuter) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(1'300)};
  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 3; ++b) {
    probeVectors.push_back(makeVarcharProbeVector(1'100));
  }

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v", "u_k", "u_v"},
                      core::JoinType::kFull)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, leftSemiProject) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(1'000)};
  std::vector<RowVectorPtr> probeVectors{makeVarcharProbeVector(1'500)};

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v", "match"},
                      core::JoinType::kLeftSemiProject)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, rightSemiFilter) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(1'600)};
  std::vector<RowVectorPtr> probeVectors;
  for (int b = 0; b < 2; ++b) {
    probeVectors.push_back(makeVarcharProbeVector(1'000));
  }

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"u_k", "u_v"},
                      core::JoinType::kRightSemiFilter)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, rightSemiProject) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeVarcharBuildVector(1'400)};
  std::vector<RowVectorPtr> probeVectors{makeVarcharProbeVector(1'100)};

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"u_k", "u_v", "match"},
                      core::JoinType::kRightSemiProject)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, nullAwareAntiJoinWithNulls) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  std::vector<RowVectorPtr> buildVectors{makeRowVector(
      {"u_k", "u_v"},
      {makeNullableFlatVector<std::string>(
           {"", "a", std::nullopt, std::string(13, 'N'), "zz"}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50})})};
  std::vector<RowVectorPtr> probeVectors{makeRowVector(
      {"t_k", "t_v"},
      {makeNullableFlatVector<std::string>(
           {"", "a", "miss", std::nullopt, std::string(13, 'N')}),
       makeFlatVector<int64_t>({1, 2, 3, 4, 5})})};

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k"},
                      {"u_k"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_v"},
                      core::JoinType::kAnti,
                      true /*nullAware*/)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

TEST_F(HashTableSimdJoinTest, encodedProbeAndBuildKeys) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  auto makeEncodedJoinInput = [&](const std::string& keyName,
                                  const std::string& constName,
                                  const std::string& payloadName,
                                  int32_t rows,
                                  int32_t salt,
                                  bool includeMisses) {
    auto palette = hashTableSimdVarcharValues();
    if (includeMisses) {
      palette.push_back(std::string(16 * 1024, 'A'));
      palette.push_back("miss_key");
    }
    auto baseKey = makeFlatVector<std::string>(
        palette.size() * 4,
        [&](auto row) { return palette[(row + salt) % palette.size()]; },
        [salt](auto row) { return (row + salt) % 31 == 0; });
    auto indices = makeIndices(
        rows, [salt](auto row) { return (row * 41 + salt * 13) % 32; });
    auto dictionaryKey =
        BaseVector::wrapInDictionary(nullptr, indices, rows, baseKey);
    auto constantBase = makeFlatVector<std::string>({"", "constant"});
    auto constantEmptyKey = BaseVector::wrapInConstant(rows, 0, constantBase);
    return makeRowVector(
        {keyName, constName, payloadName},
        {dictionaryKey,
         constantEmptyKey,
         makeFlatVector<int64_t>(
             rows, [salt](auto row) { return (row * 17 + salt) % 1009; })});
  };

  std::vector<RowVectorPtr> buildVectors;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t b = 0; b < 3; ++b) {
    buildVectors.push_back(makeEncodedJoinInput(
        "u_k", "u_const", "u_v", 420, b, false /*includeMisses*/));
    probeVectors.push_back(makeEncodedJoinInput(
        "t_k", "t_const", "t_v", 450, b + 7, true /*includeMisses*/));
  }

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(idGen)
                  .values(probeVectors)
                  .hashJoin(
                      {"t_k", "t_const"},
                      {"u_k", "u_const"},
                      PlanBuilder(idGen).values(buildVectors).planNode(),
                      "",
                      {"t_k", "t_const", "t_v", "u_v"},
                      core::JoinType::kInner)
                  .planNode();
  assertQueryResultsEqualWithSimdHashTable(plan, pool_.get(), "HashJoin");
}

} // namespace bytedance::bolt::exec::test
