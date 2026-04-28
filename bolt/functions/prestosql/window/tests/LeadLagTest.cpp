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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/functions/lib/window/tests/WindowTestBase.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::window::test {
namespace {

// Test parameter is function name: lead or lag.
class LeadLagTest : public WindowTestBase,
                    public testing::WithParamInterface<std::string> {
 protected:
  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
  }

  std::string fn(const std::string& params) {
    return fmt::format("{}({})", GetParam(), params);
  }

  bool isLag() {
    return GetParam() == "lag";
  }

  RowVectorPtr appendColumn(
      const RowVectorPtr& rowVector,
      const VectorPtr& newColumn) {
    std::vector<VectorPtr> columns = rowVector->children();
    columns.push_back(newColumn);
    return makeRowVector(columns);
  }
};

TEST_P(LeadLagTest, offset) {
  // largeOffset is larger than std::numeric_limits<int32_t>::max()
  // and is a negative number when cast to int32.
  int64_t largeOffset = (int64_t)std::numeric_limits<int32_t>::max() * 2;
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({1, 2, 3, 1, 2}),
      // Offsets with nulls.
      makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt, 2}),
      // Large offsets.
      makeFlatVector<int64_t>(
          {largeOffset, largeOffset, largeOffset, largeOffset, largeOffset}),
      // Default values.
      makeNullableFlatVector<int64_t>({std::nullopt, 99, 99, 99, std::nullopt}),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  // Default offset.
  assertResults(fn("c0"));

  // Constant offset.
  assertResults(fn("c0, 2"));

  // Large offset.
  assertResults(fn("c0, c3"));

  // Large && CONSTANT offset.
  assertResults(fn(fmt::format("c0, {}", largeOffset)));

  // Constant null offset. DuckDB returns incorrect results for this case. It
  // treats null offset as 0.
  auto queryInfo =
      buildWindowQuery({data}, fn("c0, null::bigint"), "order by c0", "");

  auto expected =
      appendColumn(data, makeAllNullFlatVector<int64_t>(data->size()));
  assertQuery(queryInfo.planNode, expected);

  // Variable offsets.
  assertResults(fn("c0, c1"));

  // Variable offsets with nulls.
  queryInfo = buildWindowQuery({data}, fn("c0, c2"), "order by c0", "");

  // This query hits UBSAN failure in DuckDB (probably due to null offset).
  std::vector<std::optional<int64_t>> expectedWindow;
  if (isLag()) {
    expectedWindow = {
        std::nullopt, std::nullopt, std::nullopt, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, std::nullopt, std::nullopt, std::nullopt};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));

  assertQuery(queryInfo.planNode, expected);

  // Out of range offsets return default value(99 here, constant case), whereas
  // null offsets return null.
  queryInfo = buildWindowQuery({data}, fn("c0, c2, 99"), "order by c0", "");
  if (isLag()) {
    expectedWindow = {99, 99, 99, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, 99, std::nullopt, 99};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));
  assertQuery(queryInfo.planNode, expected);

  // Out of range offsets return default value(null here, constant null case),
  // whereas null offsets return null.
  queryInfo =
      buildWindowQuery({data}, fn("c0, c2, null::bigint"), "order by c0", "");
  if (isLag()) {
    expectedWindow = {
        std::nullopt, std::nullopt, std::nullopt, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, std::nullopt, std::nullopt, std::nullopt};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));
  assertQuery(queryInfo.planNode, expected);

  // Out of range offsets return default value(c4 here, nullable offset
  // variable case), whereas null offsets return null.
  queryInfo = buildWindowQuery({data}, fn("c0, c2, c4"), "order by c0", "");
  if (isLag()) {
    expectedWindow = {std::nullopt, 99, 99, std::nullopt, 3};
  } else {
    expectedWindow = {2, 4, 99, std::nullopt, std::nullopt};
  }
  expected =
      appendColumn(data, makeNullableFlatVector<int64_t>(expectedWindow));
  assertQuery(queryInfo.planNode, expected);
}

TEST_P(LeadLagTest, ignoreNullsInt64Offset) {
  // The offset is bigger than int32:max() and it is also a positive number
  // if cast to int32. With only such a special number we can trigger
  // some tricky bug.
  int64_t largeOffset = (int64_t)std::numeric_limits<uint32_t>::max() + 2;
  auto data = makeRowVector(
      {// Values.
       makeNullableFlatVector<int64_t>({1, std::nullopt, 3, 4, 5}),
       // Offsets.
       makeFlatVector<int64_t>(
           {largeOffset, largeOffset, largeOffset, largeOffset, largeOffset})});

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  // Test the large offset which is a column reference.
  assertResults(fn("c0, c1 IGNORE NULLS"));

  // Test the large offset which is a CONSTANT.
  assertResults(fn(fmt::format("c0, {} IGNORE NULLS", largeOffset)));
}

TEST_P(LeadLagTest, zeroOffset) {
  auto data = makeRowVector({
      // Values with null.
      makeNullableFlatVector<int32_t>(
          {1, std::nullopt, 2, std::nullopt, std::nullopt}),
      // Values without null.
      makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({0, 0, 0, 0, 0}),
  });
  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");
    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  assertResults(fn("c0, 0"));
  assertResults(fn("c0, c2"));
  assertResults(fn("c0, 0 IGNORE NULLS"));
  assertResults(fn("c0, c2 IGNORE NULLS"));

  assertResults(fn("c1, 0"));
  assertResults(fn("c1, c2"));
  assertResults(fn("c1, 0 IGNORE NULLS"));
  assertResults(fn("c1, c2 IGNORE NULLS"));
}

TEST_P(LeadLagTest, defaultValue) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Default values.
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      // Default values with nulls.
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, std::nullopt, 50}),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  // Constant non-null default value.
  assertResults(fn("c0, 2, 100"));
  assertResults(fn("c0, 22, 100"));

  // Constant null default value.
  assertResults(fn("c0, 2, null::bigint"));

  // Variable default values.
  assertResults(fn("c0, 2, c1"));
  assertResults(fn("c0, 22, c1"));

  // Variable default values with nulls.
  assertResults(fn("c0, 2, c2"));
  assertResults(fn("c0, 22, c2"));
}

TEST_P(LeadLagTest, constantTargetValue) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Default values.
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      // Default values with nulls.
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, std::nullopt, 50}),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  assertResults(fn("4, 1, 100"));
  assertResults(fn("4, 1, 100 IGNORE NULLS"));

  // Constant non-null default value.
  assertResults(fn("5, 2, 100"));
  assertResults(fn("55, 22, 100 IGNORE NULLS"));

  // Constant null default value.
  assertResults(fn("5, 2, null::bigint"));
  assertResults(fn("5, 2, null::bigint IGNORE NULLS"));

  // Variable default values.
  assertResults(fn("5, 2, c1"));
  assertResults(fn("55, 22, c1"));
  assertResults(fn("55, 22, c1 IGNORE NULLS"));

  // Variable default values with nulls.
  assertResults(fn("5, 2, c2"));
  assertResults(fn("55, 22, c2"));
  assertResults(fn("55, 22, c2 IGNORE NULLS"));
}

TEST_P(LeadLagTest, constantTargetVector) {
  const vector_size_t size = 5;
  auto flatVector = makeFlatVector<int64_t>(
      size, [](auto row) { return row; }, nullEvery(7));

  auto constVector = std::dynamic_pointer_cast<ConstantVector<int64_t>>(
      BaseVector::wrapInConstant(size, 2, flatVector));

  auto data = makeRowVector({
      // Values.
      constVector,
      // Default values.
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      // Default values with nulls.
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, std::nullopt, 50}),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");

    SCOPED_TRACE(queryInfo.functionSql);
    assertQuery(queryInfo.planNode, queryInfo.querySql);
  };

  assertResults(fn("c0, 1, 100"));
  assertResults(fn("c0, 1, 100 IGNORE NULLS"));

  // Constant non-null default value.
  assertResults(fn("c0, 2, 100"));
  assertResults(fn("c0, 22, 100"));

  // Constant null default value.
  assertResults(fn("c0, 2, null::bigint"));
  assertResults(fn("c0, 2, null::bigint IGNORE NULLS"));

  // Variable default values.
  assertResults(fn("c0, 2, c1"));
  assertResults(fn("c0, 22, c1"));
  assertResults(fn("c0, 22, c1 IGNORE NULLS"));

  // Variable default values with nulls.
  assertResults(fn("c0, 2, c2"));
  assertResults(fn("c0, 22, c2"));
  assertResults(fn("c0, 22, c2 IGNORE NULLS"));
}

// Make sure resultOffset passed to LagFunction::apply is handled correctly.
TEST_P(LeadLagTest, smallPartitions) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
      // Small partitions. 5 rows each.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row / 5; }),
      // Default values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row * 10; }),
  });

  createDuckDbTable({data});

  // Single-row partitions.
  auto queryInfo = buildWindowQuery({data}, fn("c0"), "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, fn("c0, 1, 100"), "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery({data}, fn("c0, 2, c2"), "partition by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  // Small partitions.
  queryInfo =
      buildWindowQuery({data}, fn("c0"), "partition by c1 order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery(
      {data}, fn("c0, 1, 100"), "partition by c1 order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);

  queryInfo = buildWindowQuery(
      {data}, fn("c0, 2, c2"), "partition by c1 order by c0", "");
  assertQuery(queryInfo.planNode, queryInfo.querySql);
}

// Make sure partitionOffset logic in LagFunction::apply works correctly.
TEST_P(LeadLagTest, largePartitions) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
      // Offsets with nulls.
      makeFlatVector<int64_t>(
          10'000, [](auto row) { return 1 + row % 5; }, nullEvery(7)),
      // Default values.
      makeFlatVector<int64_t>(10'000, [](auto row) { return row * 10; }),
  });

  createDuckDbTable({data});

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "order by c0", "");
    SCOPED_TRACE(queryInfo.functionSql);
    AssertQueryBuilder(queryInfo.planNode, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(queryInfo.querySql);
  };

  assertResults(fn("c0"));
  assertResults(fn("c0, 5"));
  assertResults(fn("c0, 5, 100"));
  assertResults(fn("c0, 50000, 100"));

  // This query hits UBSAN failure in DuckDB (probably due to null offset).
  auto queryInfo = buildWindowQuery({data}, fn("c0, c1"), "order by c0", "");

  VectorPtr expectedWindow;
  if (isLag()) {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        [](auto row) { return row - (1 + row % 5); },
        [](auto row) { return row < 5 || row % 7 == 0; });
  } else {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        [](auto row) { return row + (1 + row % 5); },
        [](auto row) { return row >= 9'997 || row % 7 == 0; });
  }

  {
    SCOPED_TRACE(queryInfo.functionSql);
    AssertQueryBuilder(queryInfo.planNode)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(appendColumn(data, expectedWindow));
  }

  assertResults(fn("c0, 50000, c2"));

  // This query hits UBSAN failure in DuckDB (probably due to null offset).
  queryInfo = buildWindowQuery({data}, fn("c0, c1, c2"), "order by c0", "");

  if (isLag()) {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        // Default values.
        [](auto row) {
          auto defaultValue = row < 5;
          return defaultValue ? row * 10 : row - (1 + row % 5);
        },
        nullEvery(7));
  } else {
    expectedWindow = makeFlatVector<int64_t>(
        data->size(),
        // Default values.
        [](auto row) {
          auto defaultValue = row >= 9'997;
          return defaultValue ? row * 10 : row + (1 + row % 5);
        },
        nullEvery(7));
  }

  {
    SCOPED_TRACE(queryInfo.functionSql);
    AssertQueryBuilder(queryInfo.planNode)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(appendColumn(data, expectedWindow));
  }
}

TEST_P(LeadLagTest, invalidOffset) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      // Offsets.
      makeFlatVector<int64_t>({1, 0, -2, 2, 4}),
  });

  auto copyResults = [&](const std::string& sql) {
    auto queryInfo = buildWindowQuery({data}, sql, "", "");
    AssertQueryBuilder(queryInfo.planNode).copyResults(pool());
  };

  BOLT_ASSERT_THROW(
      copyResults(fn("c0, -1")), "(-1 vs. 0) Offset must be at least 0");
  BOLT_ASSERT_THROW(
      copyResults(fn("c0, c1")), "(-2 vs. 0) Offset must be at least 0");
}

// Verify that lag function doesn't take frames into account. It operates on the
// whole partition instead.
TEST_P(LeadLagTest, emptyFrames) {
  auto data = makeRowVector({
      // Values.
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  createDuckDbTable({data});

  static const std::string kEmptyFrame =
      "rows between 100 preceding AND 90 preceding";

  // DuckDB results are incorrect. It returns NULL for empty frames.
  std::vector<std::optional<int64_t>> expectedWindow;

  auto assertResults = [&](const std::string& functionSql) {
    auto queryInfo = buildWindowQuery({data}, functionSql, "", kEmptyFrame);
    auto expected = makeRowVector({
        data->childAt(0),
        makeNullableFlatVector<int64_t>(expectedWindow),
    });
    assertQuery(queryInfo.planNode, expected);
  };

  if (isLag()) {
    expectedWindow = {std::nullopt, 1, 2, 3, 4};
    assertResults(fn("c0"));

    expectedWindow = {std::nullopt, std::nullopt, 1, 2, 3};
    assertResults(fn("c0, 2"));

    expectedWindow = {100, 100, 1, 2, 3};
    assertResults(fn("c0, 2, 100"));
  } else {
    expectedWindow = {2, 3, 4, 5, std::nullopt};
    assertResults(fn("c0"));

    expectedWindow = {3, 4, 5, std::nullopt, std::nullopt};
    assertResults(fn("c0, 2"));

    expectedWindow = {3, 4, 5, 100, 100};
    assertResults(fn("c0, 2, 100"));
  }
}

BOLT_INSTANTIATE_TEST_SUITE_P(LagTest, LeadLagTest, ::testing::Values("lag"));

BOLT_INSTANTIATE_TEST_SUITE_P(LeadTest, LeadLagTest, ::testing::Values("lead"));

// Regression test for an O(N^2) slowdown observed when LAG runs over a query
// shape like:
//
//   lag(varchar_col, 1, default_varchar_col) over (partition by k order by ...)
//
// against many small (here single-row) partitions whose VARCHAR values are
// stored out-of-line -- i.e. each value is longer than 12 bytes, so it does
// not fit in StringView's inline storage and must point into an external
// string buffer. Production hit this with timestamp-shaped strings such as
// '2026-01-01 23:59:59' (19 bytes).
//
// Mechanism the fix is guarding against:
//
//   1. For each partition, LeadLagFunction::setDefaultValue extracts the
//      partition's default-value rows into the reusable member vector
//      `defaultValues_`. extractColumn allocates a string buffer inside
//      `defaultValues_` to hold those bytes.
//
//   2. setDefaultValue then calls `result->copy(defaultValues_, ...)`. For
//      same-pool VARCHAR copies this routes through
//      FlatVector::acquireSharedStringBuffers, which makes `result` take a
//      shared ref to every buffer currently in
//      `defaultValues_->stringBuffers_`.
//
//   3. On the next partition, the buffer in `defaultValues_` is no longer
//      unique (refcount==2: held by both `defaultValues_` and `result`), so
//      getBufferWithSpace cannot reuse it. It allocates a new buffer and
//      appends it to `defaultValues_->stringBuffers_` -- the old buffer is
//      never removed because it is still referenced by `result`.
//
//   4. `defaultValues_->stringBuffers_` therefore grows by one entry per
//      partition. Each subsequent `result->copy(defaultValues_, ...)` iterates
//      that whole list inside acquireSharedStringBuffers, giving O(N^2) work
//      across N partitions.
//
// The fix calls `defaultValues_->prepareForReuse()` before each extract, which
// drops the now-non-unique buffer from `defaultValues_` (the bytes it owns are
// kept alive via the ref still held by `result`). That keeps
// `defaultValues_->stringBuffers_` bounded at one entry, returning the per-call
// acquireSharedStringBuffers walk to O(1).
//
// What this test asserts:
//
//   The Window operator emits output in batches of `numRowsPerOutput_` rows
//   (~1024 by default). Within one batch, `result->stringBuffers_` accumulates
//   the union of all distinct buffers it has acquired from `defaultValues_`.
//   Because acquireSharedStringBuffers copies *every* entry of
//   `defaultValues_->stringBuffers_`, in the broken version that union grows
//   monotonically across batches: batch i's `result` ends up with roughly
//   i * (rows-per-batch) buffers. We empirically observed on the broken
//   version: 11264, 21504, 31744, 41984, 50000.
//
//   With the fix, `defaultValues_->stringBuffers_` is bounded at 1, so the
//   only buffers a batch's `result` acquires are the ones produced by the
//   partitions in that very batch -- bounded by the batch row count
//   (~1024 in our case).
//
//   So `maxBatchBuffers` distinguishes the two cleanly: it scales with kSize
//   when broken and with rows-per-batch when fixed. The threshold below
//   (kSize / 10 = 5000) sits comfortably above ~1024 (a few batches' worth of
//   slack) and far below kSize (50000), so it tolerates batch-sizing changes
//   while still firing on regressions.
TEST_F(LeadLagTest, manySingleRowVarcharPartitions) {
  // Each row is its own partition, so callApplyForPartitionRows runs kSize
  // times against the LAG function -- amplifying the per-partition O(N) walk
  // in the broken version into a clearly visible O(N^2) total.
  constexpr vector_size_t kSize = 50'000;

  // Materialize the value/default strings into long-lived std::strings so the
  // StringViews we hand to makeFlatVector point at stable memory while
  // FlatVector::set copies them into its own string buffer. (The
  // StringView(std::string&&) constructor is deleted precisely to catch the
  // dangling-temporary mistake, so we cannot use fmt::format directly inline.)
  std::vector<std::string> valueStrings(kSize);
  std::vector<std::string> defaultStrings(kSize);
  for (vector_size_t row = 0; row < kSize; ++row) {
    // 19 chars -- exceeds StringView's 12-byte inline limit, forcing each
    // value to live in an external buffer. This is what makes the
    // acquireSharedStringBuffers path relevant.
    valueStrings[row] = fmt::format(
        "2026-01-01 {:02d}:{:02d}:{:02d}",
        (row / 3600) % 24,
        (row / 60) % 60,
        row % 60);
    defaultStrings[row] = fmt::format(
        "2026-12-31 {:02d}:{:02d}:{:02d}",
        (row / 3600) % 24,
        (row / 60) % 60,
        row % 60);
  }

  // Schema: c0 = unique partition key, c1 = LAG value column,
  // c2 = LAG per-row default (variable -- routes setDefaultValue through the
  // `defaultValueIndex_` branch where the `defaultValues_` accumulation bug
  // lives, not the constant-default branch).
  auto data = makeRowVector({
      makeFlatVector<int64_t>(kSize, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          kSize, [&](auto row) { return StringView(valueStrings[row]); }),
      makeFlatVector<StringView>(
          kSize, [&](auto row) { return StringView(defaultStrings[row]); }),
  });

  auto queryInfo = buildWindowQuery(
      {data}, "lag(c1, 1, c2)", "partition by c0 order by c1 desc", "");

  // We must observe the *raw* per-batch FlatVector that the Window operator
  // emits. AssertQueryBuilder::copyResults concatenates batches into a fresh
  // RowVector created in the test's pool; that copy goes through
  // FlatVector<StringView>::copyRanges' cross-pool path, which writes via
  // setStringViewValue and re-chunks into the result's own 32 KB buffers --
  // hiding the per-batch buffer count we want to assert on.
  //
  // Driving with TaskCursor + `copyResult=false` skips the queue-side copy
  // and hands us the operator's own RowVector, so `lagColumn->stringBuffers()`
  // reports exactly what the Window operator produced.
  exec::test::CursorParameters params;
  params.planNode = queryInfo.planNode;
  params.copyResult = false;
  auto cursor = exec::test::TaskCursor::create(params);

  auto start = std::chrono::steady_clock::now();
  size_t totalRows = 0;
  size_t maxBatchBuffers = 0;
  size_t numBatches = 0;
  while (cursor->moveNext()) {
    auto batch = cursor->current();
    // Output schema is the input columns followed by one column per window
    // function. We have a single LAG, so it's the last child.
    auto* lagColumn = batch->childAt(batch->childrenSize() - 1)
                          ->asUnchecked<FlatVector<StringView>>();
    maxBatchBuffers =
        std::max(maxBatchBuffers, lagColumn->stringBuffers().size());

    // Spot-check correctness while we're here: every row sits in its own
    // size-1 partition with offset=1, so LAG always falls back to the per-row
    // default (c2 = defaultStrings[row]).
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      ASSERT_FALSE(lagColumn->isNullAt(i));
      ASSERT_EQ(
          lagColumn->valueAt(i), StringView(defaultStrings[totalRows + i]));
    }
    totalRows += batch->size();
    ++numBatches;
  }
  auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

  ASSERT_EQ(totalRows, kSize);
  // Diagnostic line; not part of the assertion, but useful when triaging
  // future regressions to know whether the slowdown shows up in elapsed
  // time, in buffer count, or both.
  std::cout << "[manySingleRowVarcharPartitions] rows=" << kSize
            << " batches=" << numBatches
            << " maxBatchBuffers=" << maxBatchBuffers
            << " elapsed=" << elapsedMs << "ms" << std::endl;

  // The actual regression check. See the long comment above for why this
  // metric distinguishes the broken and fixed code:
  //
  //   broken: maxBatchBuffers grows toward kSize (we measured 50000)
  //   fixed:  maxBatchBuffers stays at ~rows-per-batch (~1024)
  //
  // kSize/10 (== 5000) leaves a comfortable margin around the fixed value
  // while staying well under the broken value, so the test won't go flaky if
  // the default output batch size shifts modestly.
  EXPECT_LT(maxBatchBuffers, static_cast<size_t>(kSize / 10))
      << "lag-column stringBuffers grew across batches (cumulative): "
      << maxBatchBuffers << " >= " << kSize / 10
      << ". This indicates defaultValues_->stringBuffers_ is accumulating "
      << "across partitions -- check that LeadLagFunction::setDefaultValue "
      << "and setConstantTargetValue still call defaultValues_->prepareForReuse() "
      << "before extractColumn.";
}

// DuckDB has errors in IGNORE NULLS logic for empty
// frames (tested above). So using non-empty frames.
inline const std::vector<std::string> kIgnoreNullsFrames = {
    "range current row",
    "range between unbounded preceding and current row",

    "range between unbounded preceding and unbounded following",

    "rows between 5 preceding and unbounded following",
    "rows between unbounded preceding and 5 following",

    "rows between 1 preceding and 5 following",

    "rows between c2 preceding and unbounded following",
    "rows between unbounded preceding and c2 following",
    "rows between c2 preceding and c2 following",
};

inline const std::vector<std::string> kIgnoreNullsPartitionClauses = {
    "partition by c0 order by c1 desc, c2",
    "partition by c0 order by c1 desc nulls first, c2",
    "partition by c0 order by c1 asc, c2",
    "partition by c0 order by c1 asc nulls first, c2",
};

TEST_F(LeadLagTest, ignoreNulls) {
  auto size = 40;
  auto input = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
       makeFlatVector<int64_t>(
           size, [](auto row) { return row % 7; }, nullEvery(8)),
       makeFlatVector<int64_t>(size, [](auto row) { return row % 6 + 1; }),
       // All null values.
       makeAllNullFlatVector<int64_t>(size)});
  // c1 has null values, so used for the values argument.
  const std::vector<std::string> kFunctionsList = {
      "lead(c1, 2 IGNORE NULLS)",
      "lag(c1, 2 IGNORE NULLS)",
      "lead(c1, c2 IGNORE NULLS)",
      "lag(c1, c2 IGNORE NULLS)",
      "lead(c1, 2, 5 IGNORE NULLS)",
      "lag(c1, 2, 5 IGNORE NULLS)",
      "lead(c1, 2, c2 IGNORE NULLS)",
      "lag(c1, 2, c2 IGNORE NULLS)",
      // All null values with IGNORE NULLS specified return default
      // value.
      "lead(c3, 2, 99 IGNORE NULLS)",
      "lag(c3, 2, 99 IGNORE NULLS)",
  };

  bool createTable = true;
  for (auto fn : kFunctionsList) {
    WindowTestBase::testWindowFunction(
        {input},
        fn,
        kIgnoreNullsPartitionClauses,
        kIgnoreNullsFrames,
        createTable);
    createTable = false;
  }
}

} // namespace
} // namespace bytedance::bolt::window::test
