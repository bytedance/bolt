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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>

#include "bolt/exec/Operator.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/type/Type.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_string(temp_file_path, "", "file path of input file");
// DEFINE_bool(enable_window_spill, true, "enable window spill");
DEFINE_string(row_based_spill_mode, "", "row based spill mode");
DEFINE_int64(fuzzer_seed, 42, "Seed for random input dataset generator");
DEFINE_bool(enable_log, false, "enable log");
DEFINE_int32(window_injection_type, 1, "Type of injections for window");
DEFINE_bool(input_fuzz, true, "input fuzz");
DEFINE_int32(warmup_rounds, 0, "nums of warmup rounds");
// DEFINE_bool(merge_sort_window, false, "enable merge sort window");
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec::test;

int32_t kNumVectors = 2000;
int32_t kRowsPerVector = 1000;
int32_t kNumWarmupRounds = FLAGS_warmup_rounds;

namespace {

struct SortWindowBenchmarkParam {
  std::string caseName;
  std::vector<std::string> partitionKeys;
  std::vector<std::string> sortKeys;
  std::string windowFunction;
  bool mergeSortAndWindow = false;
  bool enableWindowSpill = false;

  SortWindowBenchmarkParam(
      const std::string& caseName,
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortKeys,
      const std::string& windowFunction,
      const bool mergeSortAndWindow,
      const bool enableWindowSpill)
      : caseName(caseName),
        partitionKeys(partitionKeys),
        sortKeys(sortKeys),
        windowFunction(windowFunction),
        mergeSortAndWindow(mergeSortAndWindow),
        enableWindowSpill(enableWindowSpill) {}
};

struct SortWindowBenchmarkResult {
  std::string caseName;
  double sortTotalTimeMs = 0;
  double windowTotalTimeMs = 0;
  uint64_t sortSpillBytes = 0;
  uint64_t windowSpillBytes = 0;
  uint64_t sortSpillRows = 0;
  uint64_t windowSpillRows = 0;
  double sortSpillTotalTime = 0;
  double windowSpillTotalTime = 0;
  double sortOutputTime = 0;
  double sortColToRowTime = 0;
  double sortInSortTime = 0;
  double windowAddInputTime = 0;
  double windowComputeWindowFunctionTime = 0;
  double windowExtractColumnTime = 0;
  double windowOutputTime = 0;
  double windowHasNextPartitionTime = 0;
  double windowLoadFromSpillTime = 0;
  double windowSpillTime = 0;
  double buildPartitionTime = 0;

  static constexpr const char* fmtStr =
      "{:<55}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}{:<15}";

  std::string toString() const {
    return fmt::format(
        fmtStr,
        caseName,
        sortTotalTimeMs,
        windowTotalTimeMs,
        sortSpillBytes,
        sortSpillRows,
        windowSpillBytes,
        windowSpillRows,
        sortSpillTotalTime,
        windowSpillTotalTime,
        sortOutputTime,
        sortColToRowTime,
        sortInSortTime,
        windowAddInputTime,
        windowComputeWindowFunctionTime,
        windowExtractColumnTime,
        windowOutputTime,
        windowHasNextPartitionTime,
        windowLoadFromSpillTime,
        windowSpillTime,
        buildPartitionTime);
  }

  static std::string title() {
    std::string title = fmt::format(
        fmtStr,
        "caseName",
        "sort time(ms)",
        "window time(ms)",
        "sortSpill(B)",
        "sortSpillRows",
        "winSpill(B)",
        "winSpillRows",
        "sSpillTime",
        "wSpillTime",
        "sOut",
        "sC2R",
        "sSort",
        "wInput",
        "wCompu",
        "wExtCol",
        "wOutput",
        "wHasNext",
        "wLoad",
        "wSpill",
        "wBuild");
    std::string delim = std::string(title.size(), '=');
    return delim + "\n" + title + "\n" + delim;
  }
};

class SortWindowBenchmark : public HiveConnectorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    window::prestosql::registerAllWindowFunctions();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  SortWindowBenchmark(RowTypePtr inputType) : inputType_(inputType) {
    HiveConnectorTestBase::SetUp();
    if (FLAGS_enable_log) {
      std::cout << "inputType_->names():"
                << folly::join(",", inputType_->names()) << std::endl;
      std::cout << "inputType_->children():"
                << folly::join(",", inputType_->children()) << std::endl;
    }

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0.1;

    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);
    std::vector<RowVectorPtr> vectors;
    for (auto i = 0; i < kNumVectors; ++i) {
      RowVectorPtr input;
      if (FLAGS_input_fuzz == true) {
        input = makeRowVector(
            {"int32", "string", "int64", "double", "data"},
            {
                fuzzer.fuzz(INTEGER()),
                fuzzer.fuzz(VARCHAR()),
                fuzzer.fuzz(BIGINT()),
                fuzzer.fuzz(DOUBLE()),
                fuzzer.fuzz(INTEGER()),
            });
      } else {
        input = makeRowVector(
            {"int32", "string", "int64", "double", "data"},
            {makeFlatVector<int32_t>(
                 kRowsPerVector, [](auto row) { return row % 23; }),
             makeFlatVector<std::string>(
                 kRowsPerVector,
                 [](auto row) { return std::string(1001, 'a' + (row % 26)); }),
             makeFlatVector<int64_t>(
                 kRowsPerVector, [](auto row) { return row % 11; }),
             makeFlatVector<double>(
                 kRowsPerVector, [](auto row) { return row % 10; }),
             makeFlatVector<int32_t>(
                 kRowsPerVector, [](auto row) { return row % 23; })});
      }
      vectors.emplace_back(input);
    }

    filePath_ = FLAGS_temp_file_path.empty() ? TempFilePath::create()->path
                                             : FLAGS_temp_file_path;

    writeToFile(filePath_, vectors);

    if (FLAGS_enable_log) {
      for (size_t i = 0; i < vectors.size(); ++i) {
        std::cout << "[log by jy][vectors dump] Vector " << i << ":\n"
                  << vectors[i]->toString(0, vectors[i]->size())
                  << ", filePath_ = " << filePath_ << "\n\n";
      }
    }
  }

  ~SortWindowBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  SortWindowBenchmarkResult run(const SortWindowBenchmarkParam& param) {
    if (FLAGS_enable_log) {
      std::cout << "[log by jy]" << __FUNCTION__ << std::endl;
      std::cout << "[log by jy]param.caseName: " << param.caseName
                << ", param.partitionKeys: "
                << folly::join(", ", param.partitionKeys)
                << ", param.sortKeys: " << folly::join(", ", param.sortKeys)
                << ", param.windowFunction: " << param.windowFunction
                << std::endl;
    }
    folly::BenchmarkSuspender suspender;

    core::PlanFragment plan;

    std::vector<std::string> orderByKeys;
    for (auto partitionKey : param.partitionKeys) {
      orderByKeys.push_back(partitionKey);
    }
    for (auto sortKey : param.sortKeys) {
      orderByKeys.push_back(sortKey);
    }

    if (param.mergeSortAndWindow) {
      plan = PlanBuilder()
                 .tableScan(inputType_)
                 .window({param.windowFunction})
                 .planFragment();
    } else {
      plan = PlanBuilder()
                 .tableScan(inputType_)
                 .orderBy(orderByKeys, false)
                 .streamingWindow({param.windowFunction})
                 .planFragment();
    }

    auto task = makeTask(param, plan);
    // task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath_)));
    // task->noMoreSplits("0");

    suspender.dismiss();

    vector_size_t totalRows = 0;
    int batchCount = 0;
    while (auto result = task->next()) {
      if (FLAGS_enable_log) {
        std::cout << "[log by jy][result dump] Batch " << batchCount++ << " ("
                  << result->size() << " rows):\n";
        std::cout << result->toString(0, result->size()) << "\n\n";
      }
      totalRows += result->size();
    }

    SortWindowBenchmarkResult result;
    result.caseName = param.caseName;

    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        if (op.operatorType == "OrderBy") {
          bytedance::bolt::exec::OperatorStats orderByStats = op;
          CpuWallTiming timing;
          timing.add(orderByStats.addInputTiming);
          timing.add(orderByStats.getOutputTiming);
          timing.add(orderByStats.finishTiming);

          result.sortTotalTimeMs = timing.wallNanos / 1000'000.0;
          result.sortSpillRows = orderByStats.spilledRows;
          result.sortSpillBytes = orderByStats.spilledBytes;
          result.sortOutputTime = orderByStats.sortOutputTime / 1'000'000.0;
          result.sortColToRowTime = orderByStats.sortColToRowTime / 1'000'000.0;
          result.sortInSortTime = orderByStats.sortInSortTime / 1'000'000.0;
          // result.sortSpillTotalTime =
          //     orderByStats.runtimeStats["spillTotalTime"].sum / 1000'000.0;
          result.sortSpillTotalTime = orderByStats.spillTotalTime / 1'000'000.0;

          // result.sortSpillSortTimeMs =
          //     orderByStats.runtimeStats["spillSortTime"].sum / 1000'000.0;
          // result.sortSpillFillTimeMs =
          //     orderByStats.runtimeStats["spillFillTime"].sum / 1000'000.0;
          // result.sortSpillConvertTimeMs =
          //     orderByStats.runtimeStats["spillConvertTime"].sum / 1000'000.0;
          // result.sortSpillSerializeTimeMs =
          //     orderByStats.runtimeStats["spillSerializationTime"].sum /
          //     1000'000.0;
          // result.sortSpillFlushTimeMs =
          //     orderByStats.runtimeStats["spillFlushTime"].sum / 1000'000.0;
          // result.sortSpillWrites =
          // orderByStats.runtimeStats["spillWrites"].sum;
          // result.sortSpillWriteTimeMs =
          //     orderByStats.runtimeStats["spillWriteTime"].sum / 1000'000.0;
          // result.sortSpillReadTimeMs =
          //     orderByStats.runtimeStats["spillReadTotalTime"].sum /
          //     1000'000.0;
        }
        if (op.operatorType == "Window") {
          bytedance::bolt::exec::OperatorStats windowStats = op;
          CpuWallTiming timing;
          timing.add(windowStats.addInputTiming);
          timing.add(windowStats.getOutputTiming);
          timing.add(windowStats.finishTiming);
          result.windowTotalTimeMs = timing.wallNanos / 1000'000.0;

          result.windowSpillBytes = windowStats.spilledBytes;
          result.windowSpillRows = windowStats.spilledRows;
          result.windowSpillTotalTime =
              windowStats.spillTotalTime / 1'000'000.0;
          result.windowAddInputTime =
              windowStats.windowAddInputTime / 1'000'000.0;
          result.windowComputeWindowFunctionTime =
              windowStats.windowComputeWindowFunctionTime / 1'000'000.0;
          result.windowExtractColumnTime =
              windowStats.windowExtractColumnTime / 1'000'000.0;
          result.windowOutputTime = windowStats.windowOutputTime / 1'000'000.0;
          result.windowHasNextPartitionTime =
              windowStats.windowHasNextPartitionTime / 1'000'000.0;
          result.windowLoadFromSpillTime =
              windowStats.windowLoadFromSpillTime / 1'000'000.0;
          result.windowSpillTime = windowStats.windowSpillTime / 1'000'000.0;
          result.buildPartitionTime =
              windowStats.buildPartitionTime / 1'000'000.0;
        }
      }
    }

    folly::doNotOptimizeAway(totalRows);
    return result;
  }

  std::shared_ptr<exec::Task> makeTask(
      const SortWindowBenchmarkParam& param,
      const core::PlanFragment& plan) {
    auto queryCtx = core::QueryCtx::create(executor_.get());
    std::unordered_map<std::string, std::string> configs;
    if (param.enableWindowSpill) {
      configs[core::QueryConfig::kSpillEnabled] = "true";
      configs[core::QueryConfig::kWindowSpillEnabled] = "true";
      configs[core::QueryConfig::kRowBasedSpillMode] = "compression";
      configs[core::QueryConfig::kJitLevel] = "-1";
      // configs[core::QueryConfig::kPreferredOutputBatchBytes] = "256";
      // configs[core::QueryConfig::kMaxOutputBatchRows] = "3";
      configs[core::QueryConfig::kTestingSpillPct] = "100";
    }
    queryCtx->testingOverrideConfigUnsafe(std::move(configs));
    auto task = exec::Task::create(
        "t", std::move(plan), 0, queryCtx, exec::Task::ExecutionMode::kSerial);
    task->setSpillDirectory("/tmp/" + task->uuid(), false);

    task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath_)));
    task->noMoreSplits("0");

    return task;
  }

 private:
  RowTypePtr inputType_;
  std::string filePath_;
};

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  SortWindowBenchmark::SetUpTestCase();
  std::unique_ptr<SortWindowBenchmark> benchmark;
  std::vector<SortWindowBenchmarkParam> params;
  std::vector<SortWindowBenchmarkResult> results;

  BENCHMARK_SUSPEND {
    std::vector<TypePtr> listOfScalarTypes = {
        INTEGER(),
        BOOLEAN(),
        TINYINT(),
        SMALLINT(),
        BIGINT(),
        HUGEINT(),
        REAL(),
        DOUBLE(),
        TIMESTAMP(),
        VARCHAR(),
        VARBINARY()};

    auto rowTypeDefinition = ROW({
        {"int32", INTEGER()},
        {"string", VARCHAR()},
        {"int64", BIGINT()},
        {"double", DOUBLE()},
        {"data", INTEGER()},
    });

    benchmark = std::make_unique<SortWindowBenchmark>(rowTypeDefinition);
    auto colNames = rowTypeDefinition->names();
    auto colTypes = rowTypeDefinition->children();
    std::vector<std::string> aggFunctions = {
        // "sum", "avg", "min", "max", "count"};
        "sum"};
    // Window function signature is not supported for
    // rank(INTEGER) and row_number(INTEGER)
    std::vector<std::string> partitionKeys = {
        colNames[0], colNames[1], colNames[2], colNames[3]};
    std::vector<std::string> sortKeys = {
        colNames[0], colNames[1], colNames[2], colNames[3]};
    for (const auto& aggFunc : aggFunctions) {
      for (const auto& partitionKey : partitionKeys) {
        for (const auto& sortKey : sortKeys) {
          if (partitionKey == sortKey) {
            continue;
          }
          for (bool enableWindowSpill : {false, true}) {
            // enable/disable merge sort and window
            for (bool mergeSortAndWindow : {false, true}) {
              std::string caseName = fmt::format(
                  "{}_P_{}_S_{}_Merge[{}]_Spill[{}]",
                  aggFunc,
                  partitionKey,
                  sortKey,
                  mergeSortAndWindow,
                  enableWindowSpill);
              params.emplace_back(SortWindowBenchmarkParam(
                  caseName,
                  {partitionKey},
                  {sortKey},
                  (FLAGS_window_injection_type < 3
                       ? fmt::format(
                             "{}({}) over (partition by {} order by {})",
                             aggFunc,
                             "data",
                             partitionKey,
                             sortKey)
                       : fmt::format(
                             "{}({}) over (partition by {} order by {} ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)",
                             aggFunc,
                             "data",
                             partitionKey,
                             sortKey)),
                  mergeSortAndWindow,
                  enableWindowSpill));
            }
          }
        }
      }
    }
  }

  if (FLAGS_enable_log) {
    std::cout << "Total number of micro-benchmarks" << params.size()
              << std::endl;
  }

  bytedance::bolt::exec::WindowBuildType windowBuildType;
  switch (FLAGS_window_injection_type) {
    case 1:
      windowBuildType =
          bytedance::bolt::exec::WindowBuildType::kSortWindowBuild;
      break;
    case 2:
      windowBuildType =
          bytedance::bolt::exec::WindowBuildType::kRowStreamingWindowBuild;
      break;
    case 3:
      windowBuildType =
          bytedance::bolt::exec::WindowBuildType::kSpillableWindowBuild;
      break;
  }
  bool hasWarmUp = false;
  for (auto& param : params) {
    folly::addBenchmark(__FILE__, param.caseName, [&]() {
      bytedance::bolt::exec::TestScopedSpillInjection scopedSpillInjection(
          param.enableWindowSpill ? 100 : 0);
      bytedance::bolt::exec::TestWindowInjection windowInjection(
          windowBuildType);

      // Let's warmup for a while
      if (!hasWarmUp) {
        BENCHMARK_SUSPEND {
          for (size_t i = 0; i < kNumWarmupRounds; i++) {
            auto tmpRes = benchmark->run(param);
            folly::doNotOptimizeAway(tmpRes);
          }
        }
        hasWarmUp = true;
      }
      results.emplace_back(benchmark->run(param));
      return 1;
    });
  }

  folly::runBenchmarks();
  benchmark.reset();

  std::cout << "\n" << SortWindowBenchmarkResult::title() << std::endl;
  auto it = results.begin();
  auto caseName = it->caseName;
  double tmpSortTime = 0;
  double tmpWindowTime = 0;
  uint64_t tmpSortSpill = 0;
  uint64_t tmpWindowSpill = 0;
  uint64_t tmpSortSpillRows = 0;
  uint64_t tmpWindowSpillRows = 0;
  double tmpSortSpillTotalTime = 0;
  double tmpWindowSpillTotalTime = 0;
  double tmpSortOutputTime = 0;
  double tmpSortColToRowTime = 0;
  double tmpSortInSortTime = 0;
  double tmpWindowAddInputTime = 0;
  double tmpWindowComputeWindowFunctionTime = 0;
  double tmpWindowExtractColumnTime = 0;
  double tmpWindowOutputTime = 0;
  double tmpWindowHasNextPartitionTime = 0;
  double tmpWindowLoadFromSpillTime = 0;
  double tmpWindowSpillTime = 0;
  double tmpBuildPartitionTime = 0;
  uint64_t tmpCnt = 0;
  for (const auto& result : results) {
    // std::cout << result.toString() << std::endl;
    if (caseName != result.caseName) {
      SortWindowBenchmarkResult tmpResult{
          caseName,
          std::round(tmpSortTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowTime / tmpCnt * 1000) / 1000,
          tmpSortSpill / tmpCnt,
          tmpWindowSpill / tmpCnt,
          tmpSortSpillRows / tmpCnt,
          tmpWindowSpillRows / tmpCnt,
          std::round(tmpSortSpillTotalTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowSpillTotalTime / tmpCnt * 1000) / 1000,
          std::round(tmpSortOutputTime / tmpCnt * 1000) / 1000,
          std::round(tmpSortColToRowTime / tmpCnt * 1000) / 1000,
          std::round(tmpSortInSortTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowAddInputTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowComputeWindowFunctionTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowExtractColumnTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowOutputTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowHasNextPartitionTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowLoadFromSpillTime / tmpCnt * 1000) / 1000,
          std::round(tmpWindowSpillTime / tmpCnt * 1000) / 1000,
          std::round(tmpBuildPartitionTime / tmpCnt * 1000) / 1000,
      };
      std::cout << tmpResult.toString() << std::endl;
      caseName = result.caseName;
      tmpSortTime = 0;
      tmpWindowTime = 0;
      tmpSortSpill = 0;
      tmpWindowSpill = 0;
      tmpSortSpillRows = 0;
      tmpWindowSpillRows = 0;
      tmpSortSpillTotalTime = 0;
      tmpWindowSpillTotalTime = 0;
      tmpSortOutputTime = 0;
      tmpSortColToRowTime = 0;
      tmpSortInSortTime = 0;
      tmpWindowAddInputTime = 0;
      tmpWindowComputeWindowFunctionTime = 0;
      tmpWindowExtractColumnTime = 0;
      tmpWindowOutputTime = 0;
      tmpWindowHasNextPartitionTime = 0;
      tmpWindowLoadFromSpillTime = 0;
      tmpWindowSpillTime = 0;
      tmpBuildPartitionTime = 0;
      tmpCnt = 0;
    }
    tmpSortTime += result.sortTotalTimeMs;
    tmpWindowTime += result.windowTotalTimeMs;
    tmpSortSpill += result.sortSpillBytes;
    tmpWindowSpill += result.windowSpillBytes;
    tmpSortSpillRows += result.sortSpillRows;
    tmpWindowSpillRows += result.windowSpillRows;
    tmpSortSpillTotalTime += result.sortSpillTotalTime;
    tmpWindowSpillTotalTime += result.windowSpillTotalTime;
    tmpSortOutputTime += result.sortOutputTime;
    tmpSortColToRowTime += result.sortColToRowTime;
    tmpSortInSortTime += result.sortInSortTime;
    tmpWindowAddInputTime += result.windowAddInputTime;
    tmpWindowComputeWindowFunctionTime +=
        result.windowComputeWindowFunctionTime;
    tmpWindowExtractColumnTime += result.windowExtractColumnTime;
    tmpWindowOutputTime += result.windowOutputTime;
    tmpWindowHasNextPartitionTime += result.windowHasNextPartitionTime;
    tmpWindowLoadFromSpillTime += result.windowLoadFromSpillTime;
    tmpWindowSpillTime += result.windowSpillTime;
    tmpBuildPartitionTime += result.buildPartitionTime;
    tmpCnt++;
  }

  SortWindowBenchmarkResult tmpResult{
      caseName,
      std::round(tmpSortTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowTime / tmpCnt * 1000) / 1000,
      tmpSortSpill / tmpCnt,
      tmpWindowSpill / tmpCnt,
      tmpSortSpillRows / tmpCnt,
      tmpWindowSpillRows / tmpCnt,
      std::round(tmpSortSpillTotalTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowSpillTotalTime / tmpCnt * 1000) / 1000,
      std::round(tmpSortOutputTime / tmpCnt * 1000) / 1000,
      std::round(tmpSortColToRowTime / tmpCnt * 1000) / 1000,
      std::round(tmpSortInSortTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowAddInputTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowComputeWindowFunctionTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowExtractColumnTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowOutputTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowHasNextPartitionTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowLoadFromSpillTime / tmpCnt * 1000) / 1000,
      std::round(tmpWindowSpillTime / tmpCnt * 1000) / 1000,
      std::round(tmpBuildPartitionTime / tmpCnt * 1000) / 1000,
  };
  std::cout << tmpResult.toString() << std::endl;

  SortWindowBenchmark::TearDownTestCase();

  return 0;
}
