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

#include "bolt/benchmarks/clickbench/ClickBenchBenchmark.h"

#include <chrono>

#include <folly/Benchmark.h>

#include "bolt/benchmarks/QueryBenchmarkBase.h"
#include "bolt/benchmarks/clickbench/ClickBenchQueryBuilder.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

DEFINE_string(
    bolt_benchmark_clickbench_data_path,
    "",
    "Path to hits.parquet or a directory containing ClickBench Parquet files");
DEFINE_int32(
    bolt_benchmark_clickbench_query,
    1,
    "ClickBench query number to run (1-43); use 0 for the Folly suite");

namespace {

class ClickBenchBenchmark : public QueryBenchmarkBase {
 public:
  void initialize() override {
    QueryBenchmarkBase::initialize();
    BOLT_USER_CHECK(
        !FLAGS_bolt_benchmark_clickbench_data_path.empty(),
        "--bolt_benchmark_clickbench_data_path is required");
    queryBuilder_ = std::make_unique<ClickBenchQueryBuilder>();
    queryBuilder_->initialize(FLAGS_bolt_benchmark_clickbench_data_path);
  }

  void shutdown() {
    queryBuilder_.reset();
    QueryBenchmarkBase::shutdown();
  }

  void runMain(std::ostream& out, RunStats& runStats) override {
    if (FLAGS_bolt_benchmark_clickbench_query == 0) {
      folly::runBenchmarks();
      return;
    }

    const auto started = std::chrono::steady_clock::now();
    auto plan =
        queryBuilder_->getQueryPlan(FLAGS_bolt_benchmark_clickbench_query);
    auto [cursor, results] = run(plan);
    BOLT_USER_CHECK_NOT_NULL(
        cursor, "ClickBench q{} failed", FLAGS_bolt_benchmark_clickbench_query);
    const auto task = cursor->task();
    ensureTaskCompletion(task.get());
    if (FLAGS_bolt_benchmark_include_results) {
      printResults(results, out);
    }

    const auto stats = task->taskStats();
    for (const auto& pipeline : stats.pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        if (op.operatorType == "TableScan") {
          runStats.rawInputBytes += op.rawInputBytes;
        }
      }
    }
    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - started);
    out << fmt::format(
               "CLICKBENCH_QUERY q{}\n"
               "CLICKBENCH_SECONDS {:.9f}\n"
               "CLICKBENCH_RAW_INPUT_BYTES {}\n"
               "CLICKBENCH_PEAK_MEMORY_BYTES {}\n",
               FLAGS_bolt_benchmark_clickbench_query,
               elapsed.count(),
               runStats.rawInputBytes,
               task->pool()->peakBytes())
        << std::flush;
  }

  void registerBenchmarks() {
    for (int32_t queryId = 1; queryId <= 43; ++queryId) {
      folly::addBenchmark(
          __FILE__, fmt::format("q{}", queryId), [this, queryId]() {
            run(queryBuilder_->getQueryPlan(queryId));
            return 1;
          });
    }
  }

 private:
  std::unique_ptr<ClickBenchQueryBuilder> queryBuilder_;
};

} // namespace

int clickBenchBenchmarkMain() {
  ClickBenchBenchmark benchmark;
  benchmark.initialize();
  benchmark.registerBenchmarks();
  if (FLAGS_bolt_benchmark_test_flags_file.empty()) {
    RunStats stats;
    benchmark.runMain(std::cout, stats);
  } else {
    benchmark.runAllCombinations();
  }
  benchmark.shutdown();
  return 0;
}
