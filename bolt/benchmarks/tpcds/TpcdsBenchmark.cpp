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
 * 2026-08-28.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/benchmarks/tpcds/TpcdsBenchmark.h"

#include <folly/Benchmark.h>

#include "bolt/benchmarks/QueryBenchmarkBase.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/PartitionFunction.h"
#include "bolt/exec/tests/utils/TpcdsQueryBuilder.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

DEFINE_string(
    bolt_benchmark_tpcds_data_path,
    "",
    "Root of the TPC-DS Parquet data. Each table must have its own "
    "subdirectory.");
DEFINE_string(
    bolt_benchmark_tpcds_plan_path,
    "",
    "Directory containing serialized Bolt plans named after SQL files, for "
    "example q1.json and q14a.json. Uppercase Q1.json is also accepted.");
DEFINE_string(
    bolt_benchmark_tpcds_query_path,
    "",
    "Directory containing the 103 TPC-DS SQL files. If set, the query "
    "manifest is validated before execution.");
DEFINE_string(
    bolt_benchmark_tpcds_query,
    "q1",
    "Query name to run in verbose mode, including a/b suffixes.");
DEFINE_bool(
    bolt_benchmark_tpcds_run_query,
    true,
    "Run --bolt_benchmark_tpcds_query once and print execution statistics.");

namespace {

class TpcdsBenchmark : public QueryBenchmarkBase {
 public:
  void initialize() override {
    QueryBenchmarkBase::initialize();

    constexpr std::string_view kPrestoPrefix = "presto.default.";
    functions::prestosql::registerAllScalarFunctions(
        std::string{kPrestoPrefix});
    aggregate::prestosql::registerAllAggregateFunctions(
        std::string{kPrestoPrefix});
    window::prestosql::registerAllWindowFunctions(std::string{kPrestoPrefix});

    Type::registerSerDe();
    common::Filter::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    exec::registerPartitionFunctionSerDe();

    BOLT_USER_CHECK(
        !FLAGS_bolt_benchmark_tpcds_data_path.empty(),
        "--bolt_benchmark_tpcds_data_path is required");
    BOLT_USER_CHECK(
        !FLAGS_bolt_benchmark_tpcds_plan_path.empty(),
        "--bolt_benchmark_tpcds_plan_path is required");
    if (!FLAGS_bolt_benchmark_tpcds_query_path.empty()) {
      TpcdsPlanLoader::validateQueryDirectory(
          FLAGS_bolt_benchmark_tpcds_query_path);
    }

    queryBuilder_ = std::make_unique<TpcdsQueryBuilder>(
        dwio::common::toFileFormat(FLAGS_bolt_benchmark_data_format),
        ioExecutor_.get());
    queryBuilder_->initialize(FLAGS_bolt_benchmark_tpcds_data_path);
    pool_ = memory::memoryManager()->addLeafPool("TpcdsBenchmark");
  }

  void shutdown() {
    if (queryBuilder_ != nullptr) {
      queryBuilder_->shutdown();
      queryBuilder_.reset();
    }
    pool_.reset();
    QueryBenchmarkBase::shutdown();
  }

  void runMain(std::ostream& out, RunStats& runStats) override {
    if (!FLAGS_bolt_benchmark_tpcds_run_query) {
      folly::runBenchmarks();
      return;
    }

    auto plan = getPlan(FLAGS_bolt_benchmark_tpcds_query);
    auto [cursor, results] = run(plan);
    BOLT_USER_CHECK_NOT_NULL(cursor, "TPC-DS query failed");
    auto task = cursor->task();
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
    out << fmt::format(
               "Execution time: {}\n"
               "Splits total: {}, finished: {}\n"
               "Memory pool peak bytes: {}\n",
               succinctMillis(
                   stats.executionEndTimeMs - stats.executionStartTimeMs),
               stats.numTotalSplits,
               stats.numFinishedSplits,
               task->pool()->peakBytes())
        << printPlanWithStats(
               *plan.plan,
               stats,
               FLAGS_bolt_benchmark_include_custom_stats,
               true)
        << std::endl;
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>> listSplits(
      const std::string& path,
      int32_t numSplitsPerFile,
      const TpchPlan& plan) override {
    auto splits = QueryBenchmarkBase::listSplits(path, numSplitsPerFile, plan);
    std::vector<std::shared_ptr<connector::ConnectorSplit>> result;
    result.reserve(splits.size());
    for (const auto& split : splits) {
      auto hiveSplit =
          std::dynamic_pointer_cast<connector::hive::HiveConnectorSplit>(split);
      BOLT_CHECK_NOT_NULL(hiveSplit);
      result.push_back(
          connector::hive::HiveConnectorSplitBuilder(hiveSplit->filePath)
              .connectorId(queryBuilder_->connectorId())
              .fileFormat(hiveSplit->fileFormat)
              .start(hiveSplit->start)
              .length(hiveSplit->length)
              .build());
    }
    return result;
  }

  void registerBenchmarks() {
    for (const auto& queryName : TpcdsPlanLoader::queryNames()) {
      folly::addBenchmark(__FILE__, queryName, [this, queryName]() {
        run(getPlan(queryName));
        return 1;
      });
    }
  }

 private:
  TpchPlan getPlan(const std::string& queryName) {
    return queryBuilder_->getQueryPlan(
        queryName, FLAGS_bolt_benchmark_tpcds_plan_path, pool_.get());
  }

  std::unique_ptr<TpcdsQueryBuilder> queryBuilder_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

} // namespace

int tpcdsBenchmarkMain() {
  TpcdsBenchmark benchmark;
  benchmark.initialize();
  benchmark.registerBenchmarks();
  if (FLAGS_bolt_benchmark_test_flags_file.empty()) {
    RunStats ignored;
    benchmark.runMain(std::cout, ignored);
  } else {
    benchmark.runAllCombinations();
  }
  benchmark.shutdown();
  return 0;
}
