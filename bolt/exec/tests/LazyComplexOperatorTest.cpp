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

// All operator-level lazy-complex-encoding tests live in this single file.
// One test per integrated operator (plus shared helpers). New operator
// integrations should add their TEST_F here.
//
// Structure:
//   - RowContainer (foundation)  -- storage-layer tests
//   - Window                     -- SortWindowBuild + RowsStreamingWindowBuild
//   + spill
//   - OrderBy (SortBuffer)       -- non-hybrid lazy path
//   - Sort + Window pipeline     -- end-to-end chained operator test
//   - (future) HashBuild/Probe, TopN, HashAggregation, etc. — add here

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/row/CompactRowLazyCodec.h"
#include "bolt/vector/LazyComplexCodec.h"
#include "bolt/vector/LazyComplexVector.h"
#include "bolt/vector/SelectivityVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/ScopedActiveLazyFormat.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;
using bytedance::bolt::test::assertEqualVectors;

namespace bytedance::bolt::exec {
namespace {

// ============================================================================
// Shared fixture
// ============================================================================

class LazyComplexOperatorTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    window::prestosql::registerAllWindowFunctions();
  }

  // ---- Schemas --------------------------------------------------------------

  // Simple schema: k (bigint, sort key, no nulls) + v1 (array<real>) +
  // v2 (map<varchar, array<integer>>).
  RowTypePtr simpleSchema() const {
    return ROW(
        {"k", "v1", "v2"},
        {BIGINT(), ARRAY(REAL()), MAP(VARCHAR(), ARRAY(INTEGER()))});
  }

  // Wide schema: 3 bigint sort keys + 4 complex payload types — stresses
  // the chained-Window pipeline.
  RowTypePtr wideSchema() const {
    return ROW(
        {"k1", "k2", "k3", "v1", "v2", "v3", "v4"},
        {BIGINT(),
         BIGINT(),
         BIGINT(),
         ARRAY(BIGINT()),
         ARRAY(DOUBLE()),
         MAP(VARCHAR(), ARRAY(REAL())),
         ROW({BIGINT(), ARRAY(BIGINT()), MAP(INTEGER(), INTEGER())})});
  }

  // ---- Batch builders -------------------------------------------------------

  std::vector<RowVectorPtr>
  makeSimpleBatches(int numBatches, int batchSize, int seed = 99) {
    VectorFuzzer::Options opts;
    opts.vectorSize = batchSize;
    opts.nullRatio = 0.05;
    opts.containerLength = 6;
    VectorFuzzer fuzzer(opts, pool(), /*seed=*/seed);

    VectorFuzzer::Options keyOpts = opts;
    keyOpts.nullRatio = 0.0;
    VectorFuzzer keyFuzzer(keyOpts, pool(), /*seed=*/seed);

    std::vector<RowVectorPtr> out;
    out.reserve(numBatches);
    for (int i = 0; i < numBatches; ++i) {
      auto base = fuzzer.fuzzInputRow(simpleSchema());
      auto k = keyFuzzer.fuzzFlat(BIGINT(), batchSize);
      out.push_back(makeRowVector(
          simpleSchema()->names(), {k, base->childAt(1), base->childAt(2)}));
    }
    return out;
  }

  std::vector<RowVectorPtr>
  makeWideBatches(int numBatches, int batchSize, int seed = 42) {
    VectorFuzzer::Options opts;
    opts.vectorSize = batchSize;
    opts.nullRatio = 0.05;
    opts.containerLength = 8;
    VectorFuzzer fuzzer(opts, pool(), /*seed=*/seed);

    VectorFuzzer::Options keyOpts = opts;
    keyOpts.nullRatio = 0.0;
    VectorFuzzer keyFuzzer(keyOpts, pool(), /*seed=*/seed);

    std::vector<RowVectorPtr> out;
    out.reserve(numBatches);
    for (int i = 0; i < numBatches; ++i) {
      auto base = fuzzer.fuzzInputRow(wideSchema());
      auto k1 = keyFuzzer.fuzzFlat(BIGINT(), batchSize);
      auto k2 = keyFuzzer.fuzzFlat(BIGINT(), batchSize);
      auto k3 = keyFuzzer.fuzzFlat(BIGINT(), batchSize);
      out.push_back(makeRowVector(
          wideSchema()->names(),
          {k1,
           k2,
           k3,
           base->childAt(3),
           base->childAt(4),
           base->childAt(5),
           base->childAt(6)}));
    }
    return out;
  }

  // ---- Small direct-container helpers --------------------------------------

  std::unique_ptr<RowContainer> makeRowContainer(
      std::vector<TypePtr> keys,
      std::vector<TypePtr> payload) {
    return std::make_unique<RowContainer>(
        keys,
        /*nullableKeys*/ true,
        /*accumulators*/ std::vector<Accumulator>{},
        payload,
        /*hasNext*/ false,
        /*isJoinBuild*/ false,
        /*hasProbedFlag*/ false,
        /*hasNormalizedKey*/ false,
        /*useListRowIndex*/ false,
        pool());
  }

  VectorPtr makeLazyComplexResult(const TypePtr& type, vector_size_t numRows) {
    auto values =
        AlignedBuffer::allocate<StringView>(numRows > 0 ? numRows : 1, pool());
    auto flat = std::make_shared<FlatVector<StringView>>(
        pool(),
        VARBINARY(),
        /*nulls=*/nullptr,
        numRows,
        values,
        std::vector<BufferPtr>{});
    return std::make_shared<LazyComplexVector>(pool(), type, flat);
  }

  // ---- Decode helper --------------------------------------------------------

  void decodeInPlace(std::vector<RowVectorPtr>& batches) {
    for (auto& batch : batches) {
      batch = decodeLazyColumns(batch, pool());
    }
  }
};

// ============================================================================
// RowContainer foundation
// ============================================================================

TEST_F(LazyComplexOperatorTest, rowContainerStoreAndExtractLazy) {
  bolt::test::ScopedActiveLazyFormat scopedCodec("compact_row");
  auto container = makeRowContainer({BIGINT()}, {ARRAY(BIGINT())});

  EXPECT_FALSE(container->isLazyComplex(0)); // key — not lazy
  EXPECT_TRUE(container->isLazyComplex(1)); // payload complex — lazy

  auto input = makeRowVector({
      makeFlatVector<int64_t>({10, 20, 30}),
      makeArrayVector<int64_t>({{1, 2}, {}, {3, 4, 5}}),
  });
  container->store(input);

  std::vector<char*> rowPointers(input->size());
  RowContainerIterator iter;
  auto n = container->listRows(&iter, input->size(), rowPointers.data());
  ASSERT_EQ(n, input->size());

  VectorPtr result = makeLazyComplexResult(ARRAY(BIGINT()), n);
  container->extractColumn(rowPointers.data(), n, /*columnIndex=*/1, 0, result);
  ASSERT_EQ(result->encoding(), VectorEncoding::Simple::LAZY_COMPLEX);

  SelectivityVector all(n);
  auto decoded = result->asUnchecked<LazyComplexVector>()->decode(all, pool());
  assertEqualVectors(input->childAt(1), decoded);
}

TEST_F(LazyComplexOperatorTest, rowContainerLazyStoreIsBytePassthrough) {
  bolt::test::ScopedActiveLazyFormat scopedCodec("compact_row");
  auto container = makeRowContainer({BIGINT()}, {ARRAY(BIGINT())});

  auto original = makeArrayVector<int64_t>({{1, 2}, {3, 4}});
  row::CompactRowLazyCodec codec;
  auto lazy = codec.encode(original, pool());
  auto row =
      makeRowVector({makeFlatVector<int64_t>({100, 200}), VectorPtr(lazy)});
  container->store(row);

  std::vector<char*> rowPointers(2);
  RowContainerIterator iter;
  container->listRows(&iter, 2, rowPointers.data());

  VectorPtr result = makeLazyComplexResult(ARRAY(BIGINT()), 2);
  container->extractColumn(rowPointers.data(), 2, /*columnIndex=*/1, 0, result);
  auto* lazyOut = result->asUnchecked<LazyComplexVector>();
  for (vector_size_t i = 0; i < 2; ++i) {
    EXPECT_EQ(lazyOut->valueAt(i), lazy->valueAt(i));
  }
}

TEST_F(LazyComplexOperatorTest, rowContainerSkipsComplexKey) {
  bolt::test::ScopedActiveLazyFormat scopedCodec("compact_row");
  auto container =
      makeRowContainer({ARRAY(BIGINT())}, {BIGINT(), ARRAY(BIGINT())});
  EXPECT_FALSE(container->isLazyComplex(0)); // complex key — not lazy
  EXPECT_FALSE(container->isLazyComplex(1)); // bigint payload — not complex
  EXPECT_TRUE(container->isLazyComplex(2)); // complex payload — lazy
}

// ============================================================================
// OrderBy (SortBuffer)
// ============================================================================

TEST_F(LazyComplexOperatorTest, orderByComplexPayload) {
  auto batches = makeSimpleBatches(/*numBatches=*/6, /*batchSize=*/128);
  auto plan = PlanBuilder()
                  .values(batches)
                  .orderBy({"k ASC NULLS LAST"}, /*isPartial=*/false)
                  .planNode();

  auto reference = AssertQueryBuilder(plan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(plan).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

TEST_F(LazyComplexOperatorTest, orderByMultipleOutputBatches) {
  // Small output batch size forces SortBuffer to produce multiple output
  // batches from one sort — exercises the lazy fresh-allocate-per-batch path.
  auto batches = makeSimpleBatches(/*numBatches=*/16, /*batchSize=*/256);
  auto plan = PlanBuilder()
                  .values(batches)
                  .orderBy({"k ASC NULLS LAST"}, /*isPartial=*/false)
                  .planNode();

  auto reference =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kPreferredOutputBatchRows, "256")
          .copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kPreferredOutputBatchRows, "256")
          .readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

TEST_F(LazyComplexOperatorTest, orderBySpillRowVectorRoundTrip) {
  // Forces spill via the kRowVector path (PrestoSerde) with lazy active.
  // Exercises Spiller::initLazyMetadata's VARBINARY translation on write,
  // SpillReadFile::rewrapLazyChildren on read.
  auto batches = makeSimpleBatches(/*numBatches=*/8, /*batchSize=*/256);
  auto plan = PlanBuilder()
                  .values(batches)
                  .orderBy({"k ASC NULLS LAST"}, /*isPartial=*/false)
                  .planNode();

  auto reference = AssertQueryBuilder(plan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  TestScopedSpillInjection scopedSpill(/*spillPct=*/100);
  auto spillDir = TempDirectoryPath::create();

  std::shared_ptr<Task> task;
  auto lazyBatches =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kOrderBySpillEnabled, "true")
          .config(core::QueryConfig::kRowBasedSpillMode, "disable")
          .spillDirectory(spillDir->getPath())
          .maxDrivers(1)
          .readBatches(task);

  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);

  const auto& taskStats = task->taskStats();
  uint64_t orderBySpilledBytes = 0;
  for (const auto& pipelineStats : taskStats.pipelineStats) {
    for (const auto& opStats : pipelineStats.operatorStats) {
      if (opStats.operatorType == "OrderBy") {
        orderBySpilledBytes += opStats.spilledBytes;
      }
    }
  }
  EXPECT_GT(orderBySpilledBytes, 0)
      << "OrderBy did not actually spill — test would not exercise the path";
}

// ============================================================================
// Window — SortWindowBuild + RowsStreamingWindowBuild
// ============================================================================

TEST_F(LazyComplexOperatorTest, windowRowsStreamingBuild) {
  // Pre-sorted input → RowsStreamingWindowBuild with needSort=false.
  auto batches = makeSimpleBatches(/*numBatches=*/4, /*batchSize=*/128);
  auto buildPlan = [&]() {
    return PlanBuilder()
        .values(batches)
        .orderBy({"k ASC NULLS LAST"}, /*isPartial=*/false)
        .window({"row_number() over (order by k)"})
        .planNode();
  };

  auto reference = AssertQueryBuilder(buildPlan()).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  TestWindowInjection windowInjection(
      WindowBuildType::kRowStreamingWindowBuild);

  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(buildPlan()).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

TEST_F(LazyComplexOperatorTest, orderByThenWindow) {
  // The production Sort→Window pipeline.
  auto batches = makeSimpleBatches(/*numBatches=*/6, /*batchSize=*/128);

  auto referencePlan = PlanBuilder()
                           .values(batches)
                           .orderBy({"k ASC NULLS LAST"}, /*isPartial=*/false)
                           .window({"row_number() over (order by k)"})
                           .planNode();
  auto reference = AssertQueryBuilder(referencePlan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  auto lazyPlan = PlanBuilder()
                      .values(batches)
                      .orderBy({"k ASC NULLS LAST"}, /*isPartial=*/false)
                      .window({"row_number() over (order by k)"})
                      .planNode();
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(lazyPlan).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// Window — three chained SortWindowBuild with spill (covers
// SpillableWindowBuild)
// ============================================================================

TEST_F(LazyComplexOperatorTest, threeChainedWindowsSpillBaselinePasses) {
  auto batches = makeWideBatches(/*numBatches=*/8, /*batchSize=*/256);
  auto referencePlan = PlanBuilder()
                           .values(batches)
                           .window({"row_number() over (order by k1)"})
                           .window({"row_number() over (order by k2)"})
                           .window({"row_number() over (order by k3)"})
                           .planNode();
  auto reference = AssertQueryBuilder(referencePlan).copyResults(pool());

  auto spillDir = TempDirectoryPath::create();
  auto testPlan = PlanBuilder()
                      .values(batches)
                      .window({"row_number() over (order by k1)"})
                      .window({"row_number() over (order by k2)"})
                      .window({"row_number() over (order by k3)"})
                      .planNode();
  TestScopedSpillInjection scopedSpill(/*spillPct=*/100);
  TestWindowInjection windowInjection(WindowBuildType::kSortWindowBuild);
  auto task = AssertQueryBuilder(testPlan)
                  .config(core::QueryConfig::kSpillEnabled, "true")
                  .config(core::QueryConfig::kWindowSpillEnabled, "true")
                  .config(
                      core::QueryConfig::kRowBasedSpillMode,
                      core::QueryConfig::kDefaultRowBasedSpillMode)
                  .spillDirectory(spillDir->getPath())
                  .maxDrivers(1)
                  .assertResults(reference);

  const auto& taskStats = task->taskStats();
  int windowSpillOps = 0;
  for (const auto& pipelineStats : taskStats.pipelineStats) {
    for (const auto& opStats : pipelineStats.operatorStats) {
      if (opStats.operatorType == "Window" && opStats.spilledBytes > 0) {
        ++windowSpillOps;
      }
    }
  }
  EXPECT_EQ(windowSpillOps, 3);
}

TEST_F(LazyComplexOperatorTest, threeChainedWindowsSpillWithLazy) {
  auto batches = makeWideBatches(/*numBatches=*/8, /*batchSize=*/256);
  auto referencePlan = PlanBuilder()
                           .values(batches)
                           .window({"row_number() over (order by k1)"})
                           .window({"row_number() over (order by k2)"})
                           .window({"row_number() over (order by k3)"})
                           .planNode();
  auto reference = AssertQueryBuilder(referencePlan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  TestScopedSpillInjection injection(100);
  TestWindowInjection windowInjection(WindowBuildType::kSortWindowBuild);

  auto spillDir = TempDirectoryPath::create();
  auto testPlan = PlanBuilder()
                      .values(batches)
                      .window({"row_number() over (order by k1)"})
                      .window({"row_number() over (order by k2)"})
                      .window({"row_number() over (order by k3)"})
                      .planNode();

  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(testPlan)
                         .config(core::QueryConfig::kSpillEnabled, "true")
                         .config(core::QueryConfig::kWindowSpillEnabled, "true")
                         .config(
                             core::QueryConfig::kRowBasedSpillMode,
                             core::QueryConfig::kDefaultRowBasedSpillMode)
                         .spillDirectory(spillDir->getPath())
                         .maxDrivers(1)
                         .readBatches(task);

  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);

  const auto& taskStats = task->taskStats();
  int windowSpillOps = 0;
  for (const auto& pipelineStats : taskStats.pipelineStats) {
    for (const auto& opStats : pipelineStats.operatorStats) {
      if (opStats.operatorType == "Window" && opStats.spilledBytes > 0) {
        ++windowSpillOps;
      }
    }
  }
  EXPECT_EQ(windowSpillOps, 3);
}

// ============================================================================
// FilterProject — selective decode on expression-referenced cols; passthrough
// for identity projections
// ============================================================================

TEST_F(LazyComplexOperatorTest, filterProjectSelectiveDecode) {
  // Plan: SELECT k, cardinality(v1) AS n1, v2 FROM t
  //   — k is a passthrough column (identity projection, primitive).
  //   — v1 is referenced by cardinality(), so it must be decoded.
  //   — v2 is an identity projection, should pass through as lazy.
  auto batches = makeSimpleBatches(/*nBatches=*/3, /*batchSize=*/64);

  auto plan = PlanBuilder()
                  .values(batches)
                  .project({"k", "cardinality(v1) as n1", "v2"})
                  .planNode();

  auto reference = AssertQueryBuilder(plan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(plan).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// TopN — priority-queue sort: non-key complex payload encoded lazily in
// RowContainer; sort key stays primitive
// ============================================================================

TEST_F(LazyComplexOperatorTest, topNComplexPayload) {
  auto batches = makeSimpleBatches(/*nBatches=*/3, /*batchSize=*/64);

  auto plan = PlanBuilder()
                  .values(batches)
                  .topN({"k"}, /*count=*/32, /*isPartial=*/false)
                  .planNode();

  auto reference = AssertQueryBuilder(plan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(plan).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// TopNRowNumber — partitioned TopN: dependent complex payload encoded lazily
// ============================================================================

TEST_F(LazyComplexOperatorTest, topNRowNumberComplexPayload) {
  // Partition by a derived column so sorting key (k) is distinct from
  // partition key. Payload v1/v2 are complex — they land in data_ as
  // dependents and get lazy-encoded.
  auto batches = makeSimpleBatches(/*nBatches=*/3, /*batchSize=*/64);

  auto plan = PlanBuilder()
                  .values(batches)
                  .project({"k % 4 as p", "k", "v1", "v2"})
                  .topNRowNumber(
                      /*partitionKeys=*/{"p"},
                      /*sortingKeys=*/{"k"},
                      /*limit=*/3,
                      /*generateRowNumber=*/false)
                  .planNode();

  auto reference = AssertQueryBuilder(plan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(plan).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// HashAggregation — Case 2+1: decode input before grouping / aggregation,
// re-encode complex output columns for the next stage
// ============================================================================

TEST_F(LazyComplexOperatorTest, hashAggregationComplexInputAndOutput) {
  // Plan: SELECT k, array_agg(v1) AS v1s FROM t GROUP BY k
  //   — v1 is a lazy array<real> input (decoded before aggregation).
  //   — v1s is array<array<real>> output, re-encoded to lazy on the way out.
  auto batches = makeSimpleBatches(/*nBatches=*/3, /*batchSize=*/64);

  auto plan = PlanBuilder()
                  .values(batches)
                  .singleAggregation({"k"}, {"array_agg(v1) as v1s"})
                  .planNode();

  auto reference = AssertQueryBuilder(plan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(plan).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// StreamingAggregation — same Case 2+1 pattern, sorted input
// ============================================================================

TEST_F(LazyComplexOperatorTest, streamingAggregationComplexInputAndOutput) {
  // Input clustered on k (generated in order), so streaming aggregation is
  // valid. array_agg(v1) produces an array<array<real>> output.
  auto batches = makeSimpleBatches(/*nBatches=*/3, /*batchSize=*/64);

  auto plan = PlanBuilder()
                  .values(batches)
                  .orderBy({"k"}, /*isPartial=*/false)
                  .partialStreamingAggregation({"k"}, {"array_agg(v1) as v1s"})
                  .planNode();

  auto reference = AssertQueryBuilder(plan).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(plan).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// NestedLoopJoin — Case 3 passthrough: lazy-to-lazy replication in output
// ============================================================================

TEST_F(LazyComplexOperatorTest, nestedLoopJoinLazyPassthrough) {
  // Cross-join a tiny probe batch against a small build batch. Both sides
  // carry complex payload columns. The lazy-aware output allocation in the
  // probe means build-side complex columns are copied byte-for-byte between
  // LazyComplexVector slots.
  auto probeBatches = makeSimpleBatches(
      /*nBatches=*/1,
      /*batchSize=*/8,
      /*seed=*/11);
  auto buildRaw = makeSimpleBatches(
      /*nBatches=*/1,
      /*batchSize=*/4,
      /*seed=*/22);

  auto renameBuild = [&](const RowVectorPtr& r) {
    return makeRowVector({"k_b", "v1_b", "v2_b"}, r->children());
  };
  std::vector<RowVectorPtr> buildBatches;
  for (const auto& b : buildRaw) {
    buildBatches.push_back(renameBuild(b));
  }

  auto makePlan = [&]() {
    auto pnidGen = std::make_shared<core::PlanNodeIdGenerator>();
    auto buildPlan = PlanBuilder(pnidGen).values(buildBatches).planNode();
    return PlanBuilder(pnidGen)
        .values(probeBatches)
        .nestedLoopJoin(buildPlan, /*outputLayout=*/{"k", "v1", "v1_b", "v2_b"})
        .planNode();
  };

  auto reference = AssertQueryBuilder(makePlan()).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(makePlan()).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// MergeJoin — sorted inner join, complex payload passes through lazy output
// ============================================================================

TEST_F(LazyComplexOperatorTest, mergeJoinLazyPassthrough) {
  auto probeBatches = makeSimpleBatches(
      /*nBatches=*/2,
      /*batchSize=*/64,
      /*seed=*/33);
  auto buildRaw = makeSimpleBatches(
      /*nBatches=*/2,
      /*batchSize=*/64,
      /*seed=*/77);

  auto renameBuild = [&](const RowVectorPtr& r) {
    return makeRowVector({"k_b", "v1_b", "v2_b"}, r->children());
  };
  std::vector<RowVectorPtr> buildBatches;
  for (const auto& b : buildRaw) {
    buildBatches.push_back(renameBuild(b));
  }

  auto makePlan = [&]() {
    auto pnidGen = std::make_shared<core::PlanNodeIdGenerator>();
    auto buildPlan = PlanBuilder(pnidGen)
                         .values(buildBatches)
                         .orderBy({"k_b"}, /*isPartial=*/false)
                         .planNode();
    return PlanBuilder(pnidGen)
        .values(probeBatches)
        .orderBy({"k"}, /*isPartial=*/false)
        .mergeJoin(
            /*leftKeys=*/{"k"},
            /*rightKeys=*/{"k_b"},
            /*build=*/buildPlan,
            /*filter=*/"",
            /*outputLayout=*/{"k", "v1", "v1_b", "v2_b"})
        .planNode();
  };

  auto reference = AssertQueryBuilder(makePlan()).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(makePlan()).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

// ============================================================================
// HashJoin — HashBuild payload lazy-encoded; HashProbe emits LazyComplexVector
// build-side output
// ============================================================================

TEST_F(LazyComplexOperatorTest, hashJoinLazyBuildSidePayload) {
  // Build side: complex payload carried through the join as build-side output.
  // Join key is a bigint (k). Right side has array<real> + map<varchar,
  // array<integer>> payload.
  constexpr int kProbeBatches = 4;
  constexpr int kBuildBatches = 4;
  constexpr int kBatchSize = 128;

  auto probeBatches = makeSimpleBatches(kProbeBatches, kBatchSize, /*seed=*/11);
  auto buildBatches = makeSimpleBatches(kBuildBatches, kBatchSize, /*seed=*/22);

  // Rename build-side columns to avoid name collision.
  auto renameBuild = [&](const RowVectorPtr& r) {
    return makeRowVector({"k_build", "v1_build", "v2_build"}, r->children());
  };
  std::vector<RowVectorPtr> buildRenamed;
  buildRenamed.reserve(buildBatches.size());
  for (const auto& b : buildBatches) {
    buildRenamed.push_back(renameBuild(b));
  }

  auto makeJoinPlan = [&]() {
    auto pnidGen = std::make_shared<core::PlanNodeIdGenerator>();
    auto buildPlan = PlanBuilder(pnidGen).values(buildRenamed).planNode();
    return PlanBuilder(pnidGen)
        .values(probeBatches)
        .hashJoin(
            /*leftKeys=*/{"k"},
            /*rightKeys=*/{"k_build"},
            /*build=*/buildPlan,
            /*filter=*/"",
            /*outputLayout=*/
            {"k", "v1", "v2", "v1_build", "v2_build"})
        .planNode();
  };

  auto reference = AssertQueryBuilder(makeJoinPlan()).copyResults(pool());

  bolt::test::ScopedActiveLazyFormat lazyActivation("compact_row");
  std::shared_ptr<Task> task;
  auto lazyBatches = AssertQueryBuilder(makeJoinPlan()).readBatches(task);
  decodeInPlace(lazyBatches);
  assertEqualResults({reference}, lazyBatches);
}

} // namespace
} // namespace bytedance::bolt::exec
