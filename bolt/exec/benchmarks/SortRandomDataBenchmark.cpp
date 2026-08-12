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
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_string(bolt_benchmark_temp_file_path, "", "file path of input file");
DEFINE_bool(bolt_benchmark_enable_sort_spill, false, "enable sort spill");
DEFINE_string(bolt_benchmark_row_based_spill_mode, "", "row based spill mode");
DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    99887766,
    "Seed for random input dataset generator");
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec::test;

static constexpr int32_t kNumVectors = 1'000;
static constexpr int32_t kRowsPerVector = 10'000;

namespace {

struct SortBenchmarkParam {
  std::string caseName;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  SortBenchmarkParam(
      const std::string& caseName,
      const std::vector<std::string>& keys,
      const std::vector<std::string>& values)
      : caseName(caseName), keys(keys), values(values) {}
};

struct SortBenchmarkResult {
  std::string caseName;
  double totalOrderByTimeMs;
  uint64_t spillRow;
  uint64_t spillBytes;
  double spillSortTimeMs;
  double spillFillTimeMs;
  double spillConvertTimeMs;
  double spillSerializeTimeMs;
  double spillFlushTimeMs;
  uint64_t spillWrites;
  double spillWriteTimeMs;
  double spillReadTimeMs;

  static constexpr const char* fmtStr =
      "{:<30}{:<20}{:<20}{:<20}{:<20}{:<20}{:<20}{:<20}{:<20}{:<20}{:<20}{:<20}";

  std::string toString() const {
    return fmt::format(
        fmtStr,
        caseName,
        totalOrderByTimeMs,
        spillRow,
        spillBytes,
        spillSortTimeMs,
        spillFillTimeMs,
        spillConvertTimeMs,
        spillSerializeTimeMs,
        spillFlushTimeMs,
        spillWrites,
        spillWriteTimeMs,
        spillReadTimeMs);
  }

  static std::string title() {
    std::string title = fmt::format(
        fmtStr,
        "caseName",
        "totalOrderByTimeMs",
        "spillRow",
        "spillBytes",
        "spillSortTimeMs",
        "spillFillTimeMs",
        "spillConvertTimeMs",
        "spillSerialTimeMs",
        "spillFlushTimeMs",
        exec::Operator::kSpillWrites,
        "spillWriteTimeMs",
        "spillReadTimeMs");
    std::string delim = std::string(title.size(), '=');
    return delim + "\n" + title + "\n" + delim;
  }
};

class SortRandomDataBenchmark : public HiveConnectorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit SortRandomDataBenchmark() {
    HiveConnectorTestBase::SetUp();

    inputType_ = ROW(
        {{"i32", INTEGER()},
         {"i32_thin", INTEGER()},
         {"i64", BIGINT()},
         {"i64_thin", BIGINT()},
         {"f32", REAL()},
         {"f32_thin", REAL()},
         {"f64", DOUBLE()},
         {"f64_thin", DOUBLE()},
         {"str", VARCHAR()},
         {"str_thin", VARCHAR()},
         {"str_inline", VARCHAR()},
         {"str_inline_thin", VARCHAR()}});

    if (FLAGS_bolt_benchmark_temp_file_path.empty()) {
      VectorFuzzer::Options opts;
      opts.vectorSize = kRowsPerVector;
      opts.nullRatio = 0;
      VectorFuzzer fuzzer(opts, pool(), FLAGS_bolt_benchmark_fuzzer_seed);

      int thinCount = 3;
      auto getThinVector = [&](TypePtr type) {
        return fuzzer.fuzzDictionary(
            fuzzer.fuzzFlat(type, thinCount), opts.vectorSize);
      };

      std::vector<RowVectorPtr> vectors;
      for (auto i = 0; i < kNumVectors; ++i) {
        std::vector<VectorPtr> children;

        // Generate random values without nulls.
        children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
        children.emplace_back(getThinVector(INTEGER()));
        children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
        children.emplace_back(getThinVector(BIGINT()));
        children.emplace_back(fuzzer.fuzzFlat(REAL()));
        children.emplace_back(getThinVector(REAL()));
        children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));
        children.emplace_back(getThinVector(DOUBLE()));

        opts.stringLength = 50;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        children.emplace_back(getThinVector(VARCHAR()));

        opts.stringLength = StringView::kInlineSize;
        fuzzer.setOptions(opts);
        children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
        children.emplace_back(getThinVector(VARCHAR()));

        vectors.emplace_back(makeRowVector(inputType_->names(), children));
      }

      filePath_ = TempFilePath::create()->path;
      writeToFile(filePath_, vectors);
      std::cout << filePath_ << std::endl;
    } else {
      filePath_ = FLAGS_bolt_benchmark_temp_file_path;
    }
  }

  ~SortRandomDataBenchmark() override {
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

  SortBenchmarkResult run(const SortBenchmarkParam& param) {
    folly::BenchmarkSuspender suspender;

    std::vector<std::string> orderByKeys;
    std::vector<std::string> projectExprs;
    std::vector<std::string> scanNames;
    std::vector<TypePtr> scanTypes;
    for (const auto& name : param.keys) {
      std::string alias = name + std::to_string(projectExprs.size());
      projectExprs.push_back(name + " as " + alias);
      orderByKeys.push_back(alias);
      if (std::find(scanNames.begin(), scanNames.end(), name) ==
          scanNames.end()) {
        scanNames.push_back(name);
        scanTypes.push_back(inputType_->findChild(name));
      }
    }
    for (const auto& name : param.values) {
      std::string alias = name + std::to_string(projectExprs.size());
      projectExprs.push_back(name + " as " + alias);
      if (std::find(scanNames.begin(), scanNames.end(), name) ==
          scanNames.end()) {
        scanNames.push_back(name);
        scanTypes.push_back(inputType_->findChild(name));
      }
    }

    auto plan = PlanBuilder()
                    .tableScan(ROW(std::move(scanNames), std::move(scanTypes)))
                    .project(projectExprs)
                    .orderBy(orderByKeys, false)
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan);

    task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath_)));
    task->noMoreSplits("0");

    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    SortBenchmarkResult result;
    result.caseName = param.caseName;
    bytedance::bolt::exec::OperatorStats orderByStats;
    for (auto piplineStat : task->taskStats().pipelineStats) {
      for (auto operatorStat : piplineStat.operatorStats) {
        if (operatorStat.operatorType == "OrderBy") {
          orderByStats = operatorStat;
        }
      }
    }
    CpuWallTiming timing;
    timing.add(orderByStats.addInputTiming);
    timing.add(orderByStats.getOutputTiming);
    timing.add(orderByStats.finishTiming);
    result.totalOrderByTimeMs = timing.wallNanos / 1000'000.0;
    result.spillRow = orderByStats.spilledRows;
    result.spillBytes = orderByStats.spilledBytes;
    result.spillSortTimeMs =
        orderByStats.runtimeStats["spillSortTime"].sum / 1000'000.0;
    result.spillFillTimeMs =
        orderByStats.runtimeStats["spillFillTime"].sum / 1000'000.0;
    result.spillConvertTimeMs =
        orderByStats.runtimeStats["spillConvertTime"].sum / 1000'000.0;
    result.spillSerializeTimeMs =
        orderByStats.runtimeStats["spillSerializationTime"].sum / 1000'000.0;
    result.spillFlushTimeMs =
        orderByStats.runtimeStats["spillFlushTime"].sum / 1000'000.0;
    result.spillWrites =
        orderByStats.runtimeStats[exec::Operator::kSpillWrites].sum;
    result.spillWriteTimeMs =
        orderByStats.runtimeStats["spillWriteTime"].sum / 1000'000.0;
    result.spillReadTimeMs =
        orderByStats.runtimeStats["spillReadTotalTime"].sum / 1000'000.0;

    folly::doNotOptimizeAway(numResultRows);
    return result;
  }

  std::shared_ptr<exec::Task> makeTask(core::PlanFragment plan) {
    auto queryCtx = core::QueryCtx::create(executor_.get());
    std::unordered_map<std::string, std::string> configs;
    if (FLAGS_bolt_benchmark_enable_sort_spill) {
      configs[core::QueryConfig::kSpillEnabled] = "true";
      configs[core::QueryConfig::kOrderBySpillEnabled] = "true";
      configs[core::QueryConfig::kOrderBySpillMemoryThreshold] = "10000000";
      configs[core::QueryConfig::kRowBasedSpillMode] =
          FLAGS_bolt_benchmark_row_based_spill_mode;
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

std::vector<std::string> getThinKeys(int n, std::string key) {
  auto v = std::vector<std::string>(n - 1, key + "_thin");
  v.push_back(key);
  return v;
}
} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  SortRandomDataBenchmark::SetUpTestCase();

  std::unique_ptr<SortRandomDataBenchmark> benchmark =
      std::make_unique<SortRandomDataBenchmark>();

  std::vector<SortBenchmarkParam> params;
  std::vector<SortBenchmarkResult> results;

  for (auto typeName : {"i64", "str_inline", "str"}) {
    // only keys
    for (int keyCount = 1; keyCount <= 8; keyCount++) {
      std::string caseName = fmt::format("sort_{}_{}k0v", typeName, keyCount);
      params.emplace_back(
          caseName,
          getThinKeys(keyCount, typeName),
          std::vector<std::string>());
    }
    // more values
    for (int valueCount = 1; valueCount <= 7; valueCount++) {
      std::string caseName = fmt::format("sort_{}_1k{}v", typeName, valueCount);
      params.emplace_back(
          caseName,
          std::vector<std::string>({typeName}),
          std::vector<std::string>(valueCount, typeName));
    }
  }

  for (auto& param : params) {
    folly::addBenchmark(
        __FILE__, param.caseName, [param, &benchmark, &results]() {
          results.push_back(benchmark->run(param));
          return 1;
        });
  }
  folly::runBenchmarks();
  benchmark.reset();

  std::cout << SortBenchmarkResult::title() << std::endl;
  for (auto result : results) {
    std::cout << result.toString() << std::endl;
  }
  SortRandomDataBenchmark::TearDownTestCase();
  return 0;
}
