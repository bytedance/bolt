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

#include <core/PlanNode.h>
#include <core/QueryConfig.h>
#include <exec/tests/utils/OperatorTestBase.h>
#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>
#include <vector>

#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    99887766,
    "Seed for random input dataset generator");
DEFINE_bool(bolt_benchmark_enable_spill, false, "enable hash agg spill");
DEFINE_string(bolt_benchmark_row_based_spill_mode, "", "row based spill mode");
DEFINE_string(bolt_benchmark_temp_file_path, "", "file path of input file");
DEFINE_int64(bolt_benchmark_iterations, 1, "run count of each benchmark");
DEFINE_int32(bolt_benchmark_jit_level, 0, "jit level");
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::common::testutil;

static constexpr int32_t kNumVectors = 4000;
static constexpr int32_t kRowsPerVector = 1024;

namespace {

class VariousAggregatesBenchmark : public HiveConnectorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::setMemoryLimit(1ULL << 34);
    OperatorTestBase::SetUpTestCase();
    functions::aggregate::sparksql::registerAggregateFunctions("");
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit VariousAggregatesBenchmark() {
    HiveConnectorTestBase::SetUp();

    // inputType_ = ROW(
    //     {{"bool", BOOLEAN()},
    //      {"i8", TINYINT()},
    //      {"i16", SMALLINT()},
    //      {"i32", INTEGER()},
    //      {"i64", BIGINT()},
    //      //  {"i128", HUGEINT()},
    //      {"f32", REAL()},
    //      {"f64", DOUBLE()},
    //      {"ts", TIMESTAMP()},
    //      {"array", ARRAY(INTEGER())},
    //      {"map", MAP(INTEGER(), VARCHAR())},
    //      //  {"struct",
    //      //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
    //      {"bool_dict", BOOLEAN()},
    //      {"i8_dict", TINYINT()},
    //      {"i16_dict", SMALLINT()},
    //      {"i32_dict", INTEGER()},
    //      {"i64_dict", BIGINT()},
    //      //  {"i128_dict", HUGEINT()},
    //      {"f32_dict", REAL()},
    //      {"f64_dict", DOUBLE()},
    //      {"ts_dict", TIMESTAMP()},
    //      {"array_dict", ARRAY(INTEGER())},
    //      {"map_dict", MAP(INTEGER(), VARCHAR())},
    //      //  {"struct_dict",
    //      //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
    //      {"bool_halfnull", BOOLEAN()},
    //      {"i8_halfnull", TINYINT()},
    //      {"i16_halfnull", SMALLINT()},
    //      {"i32_halfnull", INTEGER()},
    //      {"i64_halfnull", BIGINT()},
    //      //  {"i128_halfnull", HUGEINT()},
    //      {"f32_halfnull", REAL()},
    //      {"f64_halfnull", DOUBLE()},
    //      {"ts_halfnull", TIMESTAMP()},
    //      {"array_halfnull", ARRAY(INTEGER())},
    //      {"map_halfnull", MAP(INTEGER(), VARCHAR())},
    //      //  {"struct_halfnull",
    //      //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
    //      {"bool_halfnull_dict", BOOLEAN()},
    //      {"i8_halfnull_dict", TINYINT()},
    //      {"i16_halfnull_dict", SMALLINT()},
    //      {"i32_halfnull_dict", INTEGER()},
    //      {"i64_halfnull_dict", BIGINT()},
    //      //  {"i128_halfnull_dict", HUGEINT()},
    //      {"f32_halfnull_dict", REAL()},
    //      {"f64_halfnull_dict", DOUBLE()},
    //      {"ts_halfnull_dict", TIMESTAMP()},
    //      {"array_halfnull_dict", ARRAY(INTEGER())},
    //      {"map_halfnull_dict", MAP(INTEGER(), VARCHAR())},
    //      //  {"struct_halfnull_dict",
    //      //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
    //      {"str", VARCHAR()},
    //      {"str_dict", VARCHAR()},
    //      {"str_inline", VARCHAR()},
    //      {"str_inline_dict", VARCHAR()},
    //      {"str_halfnull", VARCHAR()},
    //      {"str_halfnull_dict", VARCHAR()},
    //      {"str_inline_halfnull", VARCHAR()},
    //      {"str_inline_halfnull_dict", VARCHAR()}});

    inputType_ = ROW({{"i32", INTEGER()}, {"i64", BIGINT()}});

    if (FLAGS_bolt_benchmark_temp_file_path.empty()) {
      VectorFuzzer::Options opts;
      opts.vectorSize = kRowsPerVector;
      opts.nullRatio = 0;
      VectorFuzzer fuzzer(opts, pool(), FLAGS_bolt_benchmark_fuzzer_seed);
      auto getDictVector = [&](TypePtr type) {
        int dictCount = rand() % kRowsPerVector;
        dictCount = std::max(dictCount, 3);
        return fuzzer.fuzzDictionary(
            fuzzer.fuzzFlat(type, dictCount), opts.vectorSize);
      };
      std::vector<RowVectorPtr> vectors;
      for (auto i = 0; i < kNumVectors; ++i) {
        std::vector<VectorPtr> children;
#if 0
        // Generate random values without nulls.
        children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));
        children.emplace_back(fuzzer.fuzzFlat(TINYINT()));
        children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));
        children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
        children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
        // children.emplace_back(fuzzer.fuzzFlat(HUGEINT()));
        children.emplace_back(fuzzer.fuzzFlat(REAL()));
        children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));
        children.emplace_back(fuzzer.fuzzFlat(TIMESTAMP()));
        children.emplace_back(fuzzer.fuzzFlat(ARRAY(INTEGER())));
        children.emplace_back(fuzzer.fuzzFlat(MAP(INTEGER(), VARCHAR())));
        // std::vector<VectorPtr> structChildren;
        // structChildren.emplace_back(fuzzer.fuzzFlat(BIGINT()));
        // structChildren.emplace_back(fuzzer.fuzzFlat(REAL()));
        // structChildren.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        // children.emplace_back(makeRowVector({"a", "b", "c"},
        // structChildren));
        children.emplace_back(getDictVector(BOOLEAN()));
        children.emplace_back(getDictVector(TINYINT()));
        children.emplace_back(getDictVector(SMALLINT()));
        children.emplace_back(getDictVector(INTEGER()));
        children.emplace_back(getDictVector(BIGINT()));
        // children.emplace_back(getDictVector(HUGEINT()));
        children.emplace_back(getDictVector(REAL()));
        children.emplace_back(getDictVector(DOUBLE()));
        children.emplace_back(getDictVector(TIMESTAMP()));
        children.emplace_back(getDictVector(ARRAY(INTEGER())));
        children.emplace_back(getDictVector(MAP(INTEGER(), VARCHAR())));
        // structChildren.clear();
        // structChildren.emplace_back(getDictVector(BIGINT()));
        // structChildren.emplace_back(getDictVector(REAL()));
        // structChildren.emplace_back(getDictVector(VARCHAR()));
        // children.emplace_back(makeRowVector({"a", "b", "c"},
        // structChildren));

        // Generate random values with nulls.
        opts.nullRatio = 0.5; // 50%
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));
        children.emplace_back(fuzzer.fuzzFlat(TINYINT()));
        children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));
        children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
        children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
        // children.emplace_back(fuzzer.fuzzFlat(HUGEINT()));
        children.emplace_back(fuzzer.fuzzFlat(REAL()));
        children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));
        children.emplace_back(fuzzer.fuzzFlat(TIMESTAMP()));
        children.emplace_back(fuzzer.fuzzFlat(ARRAY(INTEGER())));
        children.emplace_back(fuzzer.fuzzFlat(MAP(INTEGER(), VARCHAR())));
        // structChildren.clear();
        // structChildren.emplace_back(fuzzer.fuzzFlat(BIGINT()));
        // structChildren.emplace_back(fuzzer.fuzzFlat(REAL()));
        // structChildren.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        // children.emplace_back(makeRowVector({"a", "b", "c"},
        // structChildren));
        children.emplace_back(getDictVector(BOOLEAN()));
        children.emplace_back(getDictVector(TINYINT()));
        children.emplace_back(getDictVector(SMALLINT()));
        children.emplace_back(getDictVector(INTEGER()));
        children.emplace_back(getDictVector(BIGINT()));
        // children.emplace_back(getDictVector(HUGEINT()));
        children.emplace_back(getDictVector(REAL()));
        children.emplace_back(getDictVector(DOUBLE()));
        children.emplace_back(getDictVector(TIMESTAMP()));
        children.emplace_back(getDictVector(ARRAY(INTEGER())));
        children.emplace_back(getDictVector(MAP(INTEGER(), VARCHAR())));
        // structChildren.clear();
        // structChildren.emplace_back(getDictVector(BIGINT()));
        // structChildren.emplace_back(getDictVector(REAL()));
        // structChildren.emplace_back(getDictVector(VARCHAR()));
        // children.emplace_back(makeRowVector({"a", "b", "c"},
        // structChildren));

        opts.nullRatio = 0;
        opts.stringLength = 100;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        children.emplace_back(getDictVector(VARCHAR()));

        opts.stringLength = StringView::kInlineSize;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        children.emplace_back(getDictVector(VARCHAR()));

        opts.nullRatio = 0.5; // 50%
        opts.stringLength = 100;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        children.emplace_back(getDictVector(VARCHAR()));

        opts.stringLength = StringView::kInlineSize;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        children.emplace_back(getDictVector(VARCHAR()));
#else
        children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
        children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
#endif
        vectors.emplace_back(makeRowVector(inputType_->names(), children));
      }

      filePath_ = TempFilePath::create()->path;
      writeToFile(filePath_, vectors);
      createDuckDbTable("t", vectors);
      std::cout << filePath_ << std::endl;
    } else {
      filePath_ = FLAGS_bolt_benchmark_temp_file_path;
    }
  }

  ~VariousAggregatesBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  VectorPtr copyIntToBigint(const VectorPtr& source) {
    return copy<int32_t, int64_t>(source, BIGINT());
  }

  template <typename T, typename U>
  VectorPtr copy(const VectorPtr& source, const TypePtr& targetType) {
    auto flatSource = source->asFlatVector<T>();
    auto flatTarget = BaseVector::create<FlatVector<U>>(
        targetType, flatSource->size(), pool_.get());
    for (auto i = 0; i < flatSource->size(); ++i) {
      if (flatSource->isNullAt(i)) {
        flatTarget->setNull(i, true);
      } else {
        flatTarget->set(i, flatSource->valueAt(i));
      }
    }
    return flatTarget;
  }

  core::JoinType getLogicalJoinType(const std::string& joinType) {
    if (joinType == "left") {
      return core::JoinType::kLeft;
    } else if (joinType == "right") {
      return core::JoinType::kRight;
    } else if (joinType == "full") {
      return core::JoinType::kFull;
    } else if (joinType == "leftSemiFilter") {
      return core::JoinType::kLeftSemiFilter;
    } else if (joinType == "rightSemiFilter") {
      return core::JoinType::kRightSemiFilter;
    } else {
      return core::JoinType::kInner;
    }
  }

  std::vector<std::string> joinOutputColumns(const std::string& joinType) {
    if (joinType == "leftSemiFilter") {
      return {"l_key", "l_i64"};
    } else if (joinType == "rightSemiFilter") {
      return {"r_key", "r_i64"};
    } else {
      return {"l_key", "l_i64", "r_key", "r_i64"};
    }
  }

  void runSMJ(const std::string& joinType, const std::string& join) {
    folly::BenchmarkSuspender suspender;

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId leftScanId;
    core::PlanNodeId rightScanId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputType_)
                    .capturePlanNodeId(leftScanId)
                    .project({"i32 as l_key", "i64 as l_i64"})
                    .orderBy({"l_key", "l_i64"}, false)
                    .mergeJoin(
                        {"l_key"},
                        {"r_key"},
                        PlanBuilder(planNodeIdGenerator)
                            .tableScan(inputType_)
                            .capturePlanNodeId(rightScanId)
                            .project({"i32 as r_key", "i64 as r_i64"})
                            .orderBy({"r_key", "r_i64"}, false)
                            .planNode(),
                        "l_i64 - r_i64 < 3000",
                        joinOutputColumns(join),
                        getLogicalJoinType(join))
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan);

    task->addSplit(leftScanId, exec::Split(makeHiveConnectorSplit(filePath_)));
    task->addSplit(rightScanId, exec::Split(makeHiveConnectorSplit(filePath_)));

    task->noMoreSplits(leftScanId);
    task->noMoreSplits(rightScanId);

    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }
    std::cout << joinType << " " << join
              << " result res num = " << numResultRows << std::endl;
    folly::doNotOptimizeAway(numResultRows);
  }

  void runHJ(const std::string& joinType, const std::string& join) {
    folly::BenchmarkSuspender suspender;

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId leftScanId;
    core::PlanNodeId rightScanId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputType_)
                    .capturePlanNodeId(leftScanId)
                    .project({"i32 as l_key", "i64 as l_i64"})
                    .hashJoin(
                        {"l_key"},
                        {"r_key"},
                        PlanBuilder(planNodeIdGenerator)
                            .tableScan(inputType_)
                            .capturePlanNodeId(rightScanId)
                            .project({"i32 as r_key", "i64 as r_i64"})
                            .planNode(),
                        "l_i64 - r_i64 < 3000",
                        joinOutputColumns(join),
                        getLogicalJoinType(join))
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan);

    task->addSplit(leftScanId, exec::Split(makeHiveConnectorSplit(filePath_)));
    task->addSplit(rightScanId, exec::Split(makeHiveConnectorSplit(filePath_)));

    task->noMoreSplits(leftScanId);
    task->noMoreSplits(rightScanId);

    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }
    std::cout << joinType << " " << join
              << " result res num = " << numResultRows << std::endl;
    folly::doNotOptimizeAway(numResultRows);
  }

  std::shared_ptr<exec::Task> makeTask(core::PlanFragment plan) {
    auto queryCtx = core::QueryCtx::create(executor_.get());
    std::unordered_map<std::string, std::string> configs;
    if (FLAGS_bolt_benchmark_enable_spill) {
      configs[core::QueryConfig::kSpillEnabled] = "true";
      configs[core::QueryConfig::kAggregationSpillEnabled] = "true";
      configs[core::QueryConfig::kAggregationSpillMemoryThreshold] = "10000000";
      configs[core::QueryConfig::kRowBasedSpillMode] =
          FLAGS_bolt_benchmark_row_based_spill_mode;
    }
    configs[core::QueryConfig::kJitLevel] =
        std::to_string(FLAGS_bolt_benchmark_jit_level);
    // configs[core::QueryConfig::kMaxOutputBatchRows] = "1024";
    queryCtx->testingOverrideConfigUnsafe(std::move(configs));
    auto task = exec::Task::create(
        "t",
        std::move(plan),
        0,
        queryCtx,
        exec::Task::ExecutionMode::kParallel);
    task->setSpillDirectory("/tmp/" + task->uuid(), false);
    return task;
  }

 private:
  RowTypePtr inputType_;
  std::string filePath_;
}; // namespace

std::unique_ptr<VariousAggregatesBenchmark> benchmark;

void doRun(
    uint32_t,
    const std::string& phyJoinType,
    const std::string& logicalJoinType) {
  for (int i = 0; i < FLAGS_bolt_benchmark_iterations; i++) {
    if (phyJoinType == "SMJ") {
      benchmark->runSMJ(phyJoinType, logicalJoinType);
    } else {
      benchmark->runHJ(phyJoinType, logicalJoinType);
    }
  }
}

BENCHMARK_NAMED_PARAM(doRun, SMJ, "SMJ", "");
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, HJ, "HJ", "");

BENCHMARK_NAMED_PARAM(doRun, SMJ_left, "SMJ", "left");
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, HJ_left, "HJ", "left");

BENCHMARK_NAMED_PARAM(doRun, SMJ_right, "SMJ", "right");
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, HJ_right, "HJ", "right");

BENCHMARK_NAMED_PARAM(doRun, SMJ_full, "SMJ", "full");
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, HJ_full, "HJ", "full");

BENCHMARK_NAMED_PARAM(doRun, SMJ_leftSemiFilter, "SMJ", "leftSemiFilter");
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    HJ_leftSemiFilter,
    "HJ",
    "leftSemiFilter");

BENCHMARK_NAMED_PARAM(doRun, SMJ_rightSemiFilter, "SMJ", "rightSemiFilter");
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    HJ_rightSemiFilter,
    "HJ",
    "rightSemiFilter");

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  VariousAggregatesBenchmark::SetUpTestCase();
  benchmark = std::make_unique<VariousAggregatesBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  VariousAggregatesBenchmark::TearDownTestCase();
  return 0;
}
