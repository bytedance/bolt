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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>

#include "bolt/common/testutil/TempFilePath.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_bool(enable_spill, false, "enable hash agg spill");
DEFINE_string(row_based_spill_mode, "", "row based spill mode");
DEFINE_string(temp_file_path, "", "file path of input file");
DEFINE_int64(aggregate_count, 1, "aggregate count of each benchmark");
DEFINE_int64(key_count, 1, "aggregate count of each benchmark");
DEFINE_int64(k_array_size, 17, "group number of k_array");
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;

static constexpr int32_t kNumVectors = 1'000;
static constexpr int32_t kRowsPerVector = 10'000;

namespace {

class SimpleAggregatesBenchmark : public HiveConnectorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit SimpleAggregatesBenchmark() {
    HiveConnectorTestBase::SetUp();

    inputType_ = ROW(
        {{"k_array", INTEGER()},
         {"k_norm", INTEGER()},
         {"k_hash", INTEGER()},
         {"i32", INTEGER()},
         {"i64", BIGINT()},
         {"f32", REAL()},
         {"f64", DOUBLE()},
         {"i32_halfnull", INTEGER()},
         {"i64_halfnull", BIGINT()},
         {"f32_halfnull", REAL()},
         {"f64_halfnull", DOUBLE()},
         {"str", VARCHAR()},
         {"str_inline", VARCHAR()}});

    if (FLAGS_temp_file_path.empty()) {
      VectorFuzzer::Options opts;
      opts.vectorSize = kRowsPerVector;
      opts.nullRatio = 0;
      VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

      std::vector<RowVectorPtr> vectors;
      for (auto i = 0; i < kNumVectors; ++i) {
        std::vector<VectorPtr> children;

        // Generate key with a small number of unique values from a small range
        // (0-16).
        children.emplace_back(makeFlatVector<int32_t>(
            kRowsPerVector,
            [](auto row) { return rand() % FLAGS_k_array_size; }));

        // Generate key with a small number of unique values from a large range
        // (300 total values).
        children.emplace_back(
            makeFlatVector<int32_t>(kRowsPerVector, [](auto row) {
              if (row % 3 == 0) {
                return std::numeric_limits<int32_t>::max() - row % 100;
              } else if (row % 3 == 1) {
                return row % 100;
              } else {
                return std::numeric_limits<int32_t>::min() + row % 100;
              }
            }));

        // Generate key with many unique values from a large range (500K total
        // values).
        children.emplace_back(fuzzer.fuzzFlat(INTEGER()));

        // Generate random values without nulls.
        children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
        // fuzzer.fuzzFlat(BIGINT()) generates very large number causing sum()
        // to overflow.
        children.emplace_back(copyIntToBigint(children.back()));
        children.emplace_back(fuzzer.fuzzFlat(REAL()));
        children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));

        // Generate random values with nulls.

        opts.nullRatio = 0.5; // 50%
        fuzzer.setOptions(opts);

        children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
        children.emplace_back(copyIntToBigint(children.back()));
        children.emplace_back(fuzzer.fuzzFlat(REAL()));
        children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));

        opts.nullRatio = 0;
        opts.stringLength = 100;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));

        opts.stringLength = StringView::kInlineSize;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));

        vectors.emplace_back(makeRowVector(inputType_->names(), children));
      }

      filePath_ = ::bytedance::bolt::test::TempFilePath::create()->path;
      writeToFile(filePath_, vectors);
      std::cout << filePath_ << std::endl;
    } else {
      filePath_ = FLAGS_temp_file_path;
    }
  }

  ~SimpleAggregatesBenchmark() override {
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

  void run(const std::string& key, const std::string& aggregate) {
    folly::BenchmarkSuspender suspender;

    std::vector<std::string> keys(FLAGS_key_count, key), aggregates;
    if (!aggregate.empty()) {
      aggregates.resize(FLAGS_aggregate_count, aggregate);
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

    folly::doNotOptimizeAway(numResultRows);
  }

  void runStream(const std::string& key, const std::string& aggregate) {
    folly::BenchmarkSuspender suspender;

    auto plan =
        PlanBuilder()
            .tableScan(inputType_)
            .orderBy({key}, false)
            .streamingAggregation(
                {key},
                std::vector<std::string>(FLAGS_aggregate_count, aggregate),
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

std::unique_ptr<SimpleAggregatesBenchmark> benchmark;

void doRun(uint32_t, const std::string& key, const std::string& aggregate) {
  benchmark->run(key, aggregate);
}

void doRunStream(
    uint32_t,
    const std::string& key,
    const std::string& aggregate) {
  benchmark->runStream(key, aggregate);
}

#define AGG_BENCHMARKS(_func_, _name_, _key_)      \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_INTEGER_##_key_,                    \
      #_key_,                                      \
      fmt::format("{}(i32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_BIGINT_##_key_,                     \
      #_key_,                                      \
      fmt::format("{}(i64)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_REAL_##_key_,                       \
      #_key_,                                      \
      fmt::format("{}(f32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_DOUBLE_##_key_,                     \
      #_key_,                                      \
      fmt::format("{}(f64)", (#_name_)));          \
  BENCHMARK_DRAW_LINE();                           \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_INTEGER_NULLS_##_key_,              \
      #_key_,                                      \
      fmt::format("{}(i32_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_BIGINT_NULLS_##_key_,               \
      #_key_,                                      \
      fmt::format("{}(i64_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_REAL_NULLS_##_key_,                 \
      #_key_,                                      \
      fmt::format("{}(f32_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      _func_,                                      \
      _name_##_DOUBLE_NULLS_##_key_,               \
      #_key_,                                      \
      fmt::format("{}(f64_halfnull)", (#_name_))); \
  BENCHMARK_DRAW_LINE();                           \
  BENCHMARK_DRAW_LINE();

// distinct aggregate.
BENCHMARK_NAMED_PARAM(doRun, distinct_k_array, "k_array", "");
BENCHMARK_NAMED_PARAM(doRun, distinct_k_norm, "k_norm", "");
BENCHMARK_NAMED_PARAM(doRun, distinct_k_hash, "k_hash", "");
BENCHMARK_DRAW_LINE();

// Count(1) aggregate.
BENCHMARK_NAMED_PARAM(doRun, count_k_array, "k_array", "count(1)");
BENCHMARK_NAMED_PARAM(doRun, count_k_norm, "k_norm", "count(1)");
BENCHMARK_NAMED_PARAM(doRun, count_k_hash, "k_hash", "count(1)");
BENCHMARK_DRAW_LINE();

// Count aggregate.
AGG_BENCHMARKS(doRun, count, k_array)
AGG_BENCHMARKS(doRun, count, k_norm)
AGG_BENCHMARKS(doRun, count, k_hash)
BENCHMARK_DRAW_LINE();

// Sum aggregate.
AGG_BENCHMARKS(doRun, sum, k_array)
AGG_BENCHMARKS(doRun, sum, k_norm)
AGG_BENCHMARKS(doRun, sum, k_hash)
BENCHMARK_DRAW_LINE();

// Avg aggregate.
AGG_BENCHMARKS(doRun, avg, k_array)
AGG_BENCHMARKS(doRun, avg, k_norm)
AGG_BENCHMARKS(doRun, avg, k_hash)
BENCHMARK_DRAW_LINE();

// Min aggregate.
AGG_BENCHMARKS(doRun, min, k_array)
AGG_BENCHMARKS(doRun, min, k_norm)
AGG_BENCHMARKS(doRun, min, k_hash)
BENCHMARK_DRAW_LINE();

// Max aggregate.
AGG_BENCHMARKS(doRun, max, k_array)
AGG_BENCHMARKS(doRun, max, k_norm)
AGG_BENCHMARKS(doRun, max, k_hash)
BENCHMARK_DRAW_LINE();

// Stddev aggregate.
AGG_BENCHMARKS(doRun, stddev, k_array)
AGG_BENCHMARKS(doRun, stddev, k_norm)
AGG_BENCHMARKS(doRun, stddev, k_hash)
BENCHMARK_DRAW_LINE();

// max(str) aggregate.
BENCHMARK_NAMED_PARAM(doRun, max_string_k_array, "k_array", "max(str)");
BENCHMARK_NAMED_PARAM(doRun, max_string_k_norm, "k_norm", "max(str)");
BENCHMARK_NAMED_PARAM(doRun, max_string_k_hash, "k_hash", "max(str)");
BENCHMARK_DRAW_LINE();

// max(str_inline) aggregate.
BENCHMARK_NAMED_PARAM(
    doRun,
    max_string_inline_k_array,
    "k_array",
    "max(str_inline)");
BENCHMARK_NAMED_PARAM(
    doRun,
    max_string_inline_k_norm,
    "k_norm",
    "max(str_inline)");
BENCHMARK_NAMED_PARAM(
    doRun,
    max_string_inline_k_hash,
    "k_hash",
    "max(str_inline)");
BENCHMARK_DRAW_LINE();

// max(str) aggregate.
BENCHMARK_NAMED_PARAM(doRunStream, max_string_k_array, "k_array", "max(str)");
BENCHMARK_NAMED_PARAM(doRunStream, max_string_k_norm, "k_norm", "max(str)");
BENCHMARK_NAMED_PARAM(doRunStream, max_string_k_hash, "k_hash", "max(str)");
BENCHMARK_NAMED_PARAM(
    doRunStream,
    max_i32_k_str_inline,
    "str_inline",
    "max(i32)");
BENCHMARK_NAMED_PARAM(doRunStream, max_i32_k_str, "str", "max(i32)");
BENCHMARK_NAMED_PARAM(
    doRunStream,
    max_str_inline_k_i64,
    "i64",
    "max(str_inline)");
BENCHMARK_DRAW_LINE();

// max(str_inline) aggregate.
BENCHMARK_NAMED_PARAM(
    doRunStream,
    max_string_inline_k_array,
    "k_array",
    "max(str_inline)");
BENCHMARK_NAMED_PARAM(
    doRunStream,
    max_string_inline_k_norm,
    "k_norm",
    "max(str_inline)");
BENCHMARK_NAMED_PARAM(
    doRunStream,
    max_string_inline_k_hash,
    "k_hash",
    "max(str_inline)");
BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  SimpleAggregatesBenchmark::SetUpTestCase();
  benchmark = std::make_unique<SimpleAggregatesBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  SimpleAggregatesBenchmark::TearDownTestCase();
  return 0;
}
