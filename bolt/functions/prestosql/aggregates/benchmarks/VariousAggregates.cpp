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

#include <core/QueryConfig.h>
#include <exec/tests/utils/OperatorTestBase.h>
#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>

#include "bolt/common/testutil/TempFilePath.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_bool(enable_spill, false, "enable hash agg spill");
DEFINE_string(row_based_spill_mode, "", "row based spill mode");
DEFINE_string(temp_file_path, "", "file path of input file");
DEFINE_string(agg_func, "count(1)", "agg functions, split by ':'");
DEFINE_string(agg_key, "i8", "aggregation keys");
DEFINE_int64(iterations, 1, "run count of each benchmark");
DEFINE_bool(jit_row_eq_vectors, false, "enable jit row_eq_vectors");
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;

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

    inputType_ = ROW(
        {{"bool", BOOLEAN()},
         {"i8", TINYINT()},
         {"i16", SMALLINT()},
         {"i32", INTEGER()},
         {"i64", BIGINT()},
         //  {"i128", HUGEINT()},
         {"f32", REAL()},
         {"f64", DOUBLE()},
         {"ts", TIMESTAMP()},
         {"array", ARRAY(INTEGER())},
         {"map", MAP(INTEGER(), VARCHAR())},
         //  {"struct",
         //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"bool_dict", BOOLEAN()},
         {"i8_dict", TINYINT()},
         {"i16_dict", SMALLINT()},
         {"i32_dict", INTEGER()},
         {"i64_dict", BIGINT()},
         //  {"i128_dict", HUGEINT()},
         {"f32_dict", REAL()},
         {"f64_dict", DOUBLE()},
         {"ts_dict", TIMESTAMP()},
         {"array_dict", ARRAY(INTEGER())},
         {"map_dict", MAP(INTEGER(), VARCHAR())},
         //  {"struct_dict",
         //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"bool_halfnull", BOOLEAN()},
         {"i8_halfnull", TINYINT()},
         {"i16_halfnull", SMALLINT()},
         {"i32_halfnull", INTEGER()},
         {"i64_halfnull", BIGINT()},
         //  {"i128_halfnull", HUGEINT()},
         {"f32_halfnull", REAL()},
         {"f64_halfnull", DOUBLE()},
         {"ts_halfnull", TIMESTAMP()},
         {"array_halfnull", ARRAY(INTEGER())},
         {"map_halfnull", MAP(INTEGER(), VARCHAR())},
         //  {"struct_halfnull",
         //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"bool_halfnull_dict", BOOLEAN()},
         {"i8_halfnull_dict", TINYINT()},
         {"i16_halfnull_dict", SMALLINT()},
         {"i32_halfnull_dict", INTEGER()},
         {"i64_halfnull_dict", BIGINT()},
         //  {"i128_halfnull_dict", HUGEINT()},
         {"f32_halfnull_dict", REAL()},
         {"f64_halfnull_dict", DOUBLE()},
         {"ts_halfnull_dict", TIMESTAMP()},
         {"array_halfnull_dict", ARRAY(INTEGER())},
         {"map_halfnull_dict", MAP(INTEGER(), VARCHAR())},
         //  {"struct_halfnull_dict",
         //   ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"str", VARCHAR()},
         {"str_dict", VARCHAR()},
         {"str_inline", VARCHAR()},
         {"str_inline_dict", VARCHAR()},
         {"str_halfnull", VARCHAR()},
         {"str_halfnull_dict", VARCHAR()},
         {"str_inline_halfnull", VARCHAR()},
         {"str_inline_halfnull_dict", VARCHAR()}});

    if (FLAGS_temp_file_path.empty()) {
      VectorFuzzer::Options opts;
      opts.vectorSize = kRowsPerVector;
      opts.nullRatio = 0;
      VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);
      auto getDictVector = [&](TypePtr type) {
        int dictCount = rand() % kRowsPerVector;
        dictCount = std::max(dictCount, 3);
        return fuzzer.fuzzDictionary(
            fuzzer.fuzzFlat(type, dictCount), opts.vectorSize);
      };
      std::vector<RowVectorPtr> vectors;
      for (auto i = 0; i < kNumVectors; ++i) {
        std::vector<VectorPtr> children;

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

        vectors.emplace_back(makeRowVector(inputType_->names(), children));
      }

      filePath_ = ::bytedance::bolt::test::TempFilePath::create()->path;
      writeToFile(filePath_, vectors);
      std::cout << filePath_ << std::endl;
    } else {
      filePath_ = FLAGS_temp_file_path;
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

  std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(' ');
    if (first == std::string::npos)
      return "";
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, (last - first + 1));
  }

  std::vector<std::string> split(const std::string& str) {
    std::vector<std::string> result;
    std::stringstream ss(str);
    std::string item;

    while (std::getline(ss, item, ':')) {
      auto tmp = trim(item);
      if (tmp.size() >= 2) {
        result.push_back(tmp);
      }
    }

    return result;
  }

  void run(const std::string& key, const std::string& aggregate) {
    folly::BenchmarkSuspender suspender;

    std::vector<std::string> keys, aggregates;
    if (!aggregate.empty()) {
      aggregates = split(aggregate);
    }
    if (!key.empty()) {
      keys = split(key);
    }

    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .partialAggregation(keys, aggregates)
                    .finalAggregation()
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan);

    task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath_)));
    task->noMoreSplits("0");

    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }
    std::cout << "result #= " << numResultRows << std::endl;
    folly::doNotOptimizeAway(numResultRows);
  }

  void runStream(const std::string& key, const std::string& aggregate) {
    folly::BenchmarkSuspender suspender;

    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .orderBy({key}, false)
                    .streamingAggregation(
                        {key},
                        split(aggregate),
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan);

    task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath_)));
    task->noMoreSplits("0");

    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    folly::doNotOptimizeAway(numResultRows);
  }

  std::shared_ptr<exec::Task> makeTask(core::PlanFragment plan) {
    auto queryCtx = core::QueryCtx::create(executor_.get());
    std::unordered_map<std::string, std::string> configs;
    if (FLAGS_enable_spill) {
      configs[core::QueryConfig::kSpillEnabled] = "true";
      configs[core::QueryConfig::kAggregationSpillEnabled] = "true";
      configs[core::QueryConfig::kAggregationSpillMemoryThreshold] = "10000000";
      configs[core::QueryConfig::kRowBasedSpillMode] =
          FLAGS_row_based_spill_mode;
    }
    configs[core::QueryConfig::kJitLevel] =
        FLAGS_jit_row_eq_vectors ? "-1" : "0";
    // configs[core::QueryConfig::kMaxOutputBatchRows] = "1024";
    queryCtx->testingOverrideConfigUnsafe(std::move(configs));
    auto task = exec::Task::create(
        "t", std::move(plan), 0, queryCtx, exec::Task::ExecutionMode::kSerial);
    task->setSpillDirectory("/tmp/" + task->uuid(), false);
    return task;
  }

 private:
  RowTypePtr inputType_;
  std::string filePath_;
};

std::unique_ptr<VariousAggregatesBenchmark> benchmark;

void doRun(uint32_t, const std::string& key, const std::string& aggregate) {
  for (int i = 0; i < FLAGS_iterations; i++) {
    benchmark->run(key, aggregate);
  }
}

BENCHMARK_NAMED_PARAM(doRun, group_by, FLAGS_agg_key, FLAGS_agg_func);

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
