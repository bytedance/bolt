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

#include "bolt/connectors/hive/bytelake/BytelakeHashDedup.h"

#include <algorithm>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/hive/bytelake/BytelakeScanSpecUtil.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "gtest/gtest.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;

class BytelakeHashDedupTest : public testing::Test,
                              public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  // Common schema: 1 VARCHAR PK ("pk") + 1 BIGINT precombine ("ts")
  // + BOOLEAN isDeleted ("deleted"). 3 columns total.
  BytelakeKeyOnlySchema simpleSchema() {
    auto full = ROW({"pk", "ts", "deleted"}, {VARCHAR(), BIGINT(), BOOLEAN()});
    return makeBytelakeKeyOnlySchema(full, /*pk=*/{0}, /*pc=*/{1}, /*vk=*/2);
  }

  // Helper: build a key batch matching simpleSchema().
  RowVectorPtr makeBatch(
      std::vector<std::string> pks,
      std::vector<int64_t> tss,
      std::vector<bool> deletes) {
    return makeRowVector({
        makeFlatVector<std::string>(pks),
        makeFlatVector<int64_t>(tss),
        makeFlatVector<bool>(deletes),
    });
  }

  // Sort a per-file losers vector for stable comparison.
  static std::vector<uint32_t> sorted(std::vector<uint32_t> v) {
    std::sort(v.begin(), v.end());
    return v;
  }
};

TEST_F(BytelakeHashDedupTest, singlePkSingleRow) {
  // One row → one winner, zero losers.
  BytelakeHashDedup dedup(simpleSchema(), /*numFiles=*/1);
  auto batch = makeBatch({"a"}, {100}, {false});
  dedup.addBatch(batch, /*fileIdx=*/0, /*batchStartRow=*/0);

  EXPECT_EQ(dedup.numWinners(), 1u);
  auto losers = std::move(dedup).takeLosers();
  ASSERT_EQ(losers.size(), 1u);
  EXPECT_TRUE(losers[0].empty());
}

TEST_F(BytelakeHashDedupTest, samePkDifferentPrecombineKeepsMax) {
  // Two rows same PK, different precombine — higher wins.
  BytelakeHashDedup dedup(simpleSchema(), 1);
  auto batch = makeBatch({"a", "a"}, {100, 200}, {false, false});
  dedup.addBatch(batch, 0, 0);

  EXPECT_EQ(dedup.numWinners(), 1u);
  auto losers = std::move(dedup).takeLosers();
  // Row 0 (precombine=100) is the loser; row 1 (precombine=200) wins.
  EXPECT_EQ(losers[0], (std::vector<uint32_t>{0}));
}

TEST_F(BytelakeHashDedupTest, samePkTieKeepsFirstSeen) {
  // Equal precombine: first-seen wins, second-seen becomes loser.
  BytelakeHashDedup dedup(simpleSchema(), 1);
  auto batch = makeBatch({"a", "a"}, {100, 100}, {false, false});
  dedup.addBatch(batch, 0, 0);

  EXPECT_EQ(dedup.numWinners(), 1u);
  auto losers = std::move(dedup).takeLosers();
  EXPECT_EQ(losers[0], (std::vector<uint32_t>{1}));
}

TEST_F(BytelakeHashDedupTest, multipleBatchesSameFile) {
  // Batch 1 (file 0, startRow=0): ("a",10) ("b",20)
  // Batch 2 (file 0, startRow=2): ("a",30) ("b",15)
  // After all:
  //   PK="a" winner = file 0 row 2 (precombine=30); row 0 is loser
  //   PK="b" winner = file 0 row 1 (precombine=20); row 3 is loser
  BytelakeHashDedup dedup(simpleSchema(), 1);
  dedup.addBatch(makeBatch({"a", "b"}, {10, 20}, {false, false}), 0, 0);
  dedup.addBatch(makeBatch({"a", "b"}, {30, 15}, {false, false}), 0, 2);

  EXPECT_EQ(dedup.numWinners(), 2u);
  auto losers = std::move(dedup).takeLosers();
  EXPECT_EQ(sorted(losers[0]), (std::vector<uint32_t>{0, 3}));
}

TEST_F(BytelakeHashDedupTest, crossFileDedup) {
  // File 0 row 0: PK="a" precombine=100 → initial winner
  // File 1 row 0: PK="a" precombine=200 → wins; file 0 row 0 displaced
  BytelakeHashDedup dedup(simpleSchema(), 2);
  dedup.addBatch(makeBatch({"a"}, {100}, {false}), /*fileIdx=*/0, 0);
  dedup.addBatch(makeBatch({"a"}, {200}, {false}), /*fileIdx=*/1, 0);

  EXPECT_EQ(dedup.numWinners(), 1u);
  auto losers = std::move(dedup).takeLosers();
  ASSERT_EQ(losers.size(), 2u);
  EXPECT_EQ(losers[0], (std::vector<uint32_t>{0})); // file 0 row 0 displaced
  EXPECT_TRUE(losers[1].empty()); // file 1 row 0 won
}

TEST_F(BytelakeHashDedupTest, batchStartRowAccumulates) {
  // Confirm rowInFile arithmetic uses batchStartRow correctly.
  // Batch starting at row 100 — losers should report rowInFile >= 100.
  BytelakeHashDedup dedup(simpleSchema(), 1);
  dedup.addBatch(makeBatch({"a", "a"}, {1, 2}, {false, false}), 0, 100);

  auto losers = std::move(dedup).takeLosers();
  // Row 0 in batch = rowInFile 100; loser since row 1 (rowInFile 101) wins.
  EXPECT_EQ(losers[0], (std::vector<uint32_t>{100}));
}

TEST_F(BytelakeHashDedupTest, distinctPksAllWinners) {
  // 3 distinct PKs → 3 winners, zero losers.
  BytelakeHashDedup dedup(simpleSchema(), 1);
  dedup.addBatch(
      makeBatch({"a", "b", "c"}, {1, 2, 3}, {false, false, false}), 0, 0);

  EXPECT_EQ(dedup.numWinners(), 3u);
  auto losers = std::move(dedup).takeLosers();
  EXPECT_TRUE(losers[0].empty());
}

TEST_F(BytelakeHashDedupTest, emptyBatchIsNoop) {
  BytelakeHashDedup dedup(simpleSchema(), 1);
  dedup.addBatch(makeBatch({}, {}, {}), 0, 0);
  EXPECT_EQ(dedup.numWinners(), 0u);
}

TEST_F(BytelakeHashDedupTest, deleteWinnerAddedToLosers) {
  // Hudi Merge-on-Read: if the winning row is a delete tombstone (isDeleted=true),
  // takeLosers folds its origin into the skip list. Combined with the
  // actual losers, the resulting bitmap drops the entire PK from output.
  BytelakeHashDedup dedup(simpleSchema(), 1);
  dedup.addBatch(makeBatch({"a", "a"}, {100, 200}, {false, true}), 0, 0);

  EXPECT_EQ(dedup.numWinners(), 1u);
  auto losers = std::move(dedup).takeLosers();
  // Row 0 lost to row 1; row 1 won but is a tombstone — both skipped.
  EXPECT_EQ(sorted(losers[0]), (std::vector<uint32_t>{0, 1}));
}

TEST_F(BytelakeHashDedupTest, multiColumnPk) {
  // Composite PK = (BIGINT, VARCHAR). Same PK only when both columns match.
  auto full =
      ROW({"pk1", "pk2", "ts", "deleted"},
          {BIGINT(), VARCHAR(), BIGINT(), BOOLEAN()});
  auto schema =
      makeBytelakeKeyOnlySchema(full, /*pk=*/{0, 1}, /*pc=*/{2}, /*vk=*/3);
  BytelakeHashDedup dedup(schema, 1);

  // Rows:
  //   (1, "a", 10, F)  → PK=(1,"a") winner
  //   (1, "b", 20, F)  → PK=(1,"b") winner (different PK)
  //   (2, "a", 30, F)  → PK=(2,"a") winner (different PK)
  //   (1, "a", 50, F)  → same PK as row 0; precombine=50 > 10 → row 0 displaced
  auto batch = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 1}),
      makeFlatVector<std::string>({"a", "b", "a", "a"}),
      makeFlatVector<int64_t>({10, 20, 30, 50}),
      makeFlatVector<bool>({false, false, false, false}),
  });
  dedup.addBatch(batch, 0, 0);

  EXPECT_EQ(dedup.numWinners(), 3u);
  auto losers = std::move(dedup).takeLosers();
  EXPECT_EQ(losers[0], (std::vector<uint32_t>{0}));
}

TEST_F(BytelakeHashDedupTest, multiColumnPrecombine) {
  // Composite precombine = (BIGINT, VARCHAR). Compared in column order.
  auto full =
      ROW({"pk", "ts", "commit_time", "deleted"},
          {VARCHAR(), BIGINT(), VARCHAR(), BOOLEAN()});
  auto schema =
      makeBytelakeKeyOnlySchema(full, /*pk=*/{0}, /*pc=*/{1, 2}, /*vk=*/3);
  BytelakeHashDedup dedup(schema, 1);

  // All same PK="a", different (ts, commit_time):
  //   row 0: (100, "20260514") → initial winner
  //   row 1: (100, "20260515") → same ts, commit_time > → wins, row 0 loser
  //   row 2: (50,  "20260601") → ts < → loses
  //   row 3: (200, "20260101") → ts > → wins, row 1 displaced
  auto batch = makeRowVector({
      makeFlatVector<std::string>({"a", "a", "a", "a"}),
      makeFlatVector<int64_t>({100, 100, 50, 200}),
      makeFlatVector<std::string>(
          {"20260514", "20260515", "20260601", "20260101"}),
      makeFlatVector<bool>({false, false, false, false}),
  });
  dedup.addBatch(batch, 0, 0);

  EXPECT_EQ(dedup.numWinners(), 1u);
  auto losers = std::move(dedup).takeLosers();
  // Final winner is row 3. Losers (in any order): {0, 1, 2}.
  EXPECT_EQ(sorted(losers[0]), (std::vector<uint32_t>{0, 1, 2}));
}

TEST_F(BytelakeHashDedupTest, fileIdxOutOfRangeRejects) {
  BytelakeHashDedup dedup(simpleSchema(), /*numFiles=*/2);
  // fileIdx=2 is out of range (valid range is [0, 2)).
  EXPECT_ANY_THROW(
      dedup.addBatch(makeBatch({"a"}, {1}, {false}), /*fileIdx=*/2, 0));
}
