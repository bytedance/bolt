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

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

DEFINE_int32(window_benchmark_num_vectors, 8, "Number of input vectors");
DEFINE_int32(window_benchmark_rows_per_vector, 1024, "Rows per input vector");
DEFINE_int32(
    window_benchmark_partition_rows,
    4096,
    "Rows per partition in generated sorted input");
DEFINE_int32(
    window_benchmark_string_bytes,
    1024,
    "Bytes per VARCHAR value in generated input");
DEFINE_bool(
    window_benchmark_include_bm_slow,
    false,
    "Deprecated no-op. BmStreamingWindowBuild spill is driven by operator "
    "reclaim, not TestScopedSpillInjection; use a dedicated reclaim benchmark "
    "for BM slow-path measurements.");

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;

namespace {

enum class PayloadProfile : uint8_t {
  kFixed,
  kNullable,
  kLargeString,
};

const char* payloadProfileName(PayloadProfile profile) {
  switch (profile) {
    case PayloadProfile::kFixed:
      return "fixed";
    case PayloadProfile::kNullable:
      return "nullable";
    case PayloadProfile::kLargeString:
      return "large_string";
  }
  BOLT_UNREACHABLE();
}

struct InputProfile {
  int32_t partitionRows{4096};
  int32_t peerGroupRows{1};
  PayloadProfile payload{PayloadProfile::kFixed};

  std::string key() const {
    return fmt::format(
        "{}:{}:{}", partitionRows, peerGroupRows, payloadProfileName(payload));
  }
};

struct BenchmarkCase {
  std::string name;
  std::vector<std::string> windowFunctions;
  InputProfile input;
};

struct BenchmarkResult {
  std::string caseName;
  std::string buildName;
  uint64_t iterations{1};
  uint64_t outputRows{0};
  uint64_t wallUs{0};
  uint64_t peakMemoryBytes{0};
  uint64_t spilledBytes{0};
  uint64_t spilledRows{0};
  double addInputMs{0};
  double computeMs{0};
  double extractMs{0};
  double outputMs{0};

  static constexpr const char* kFormat =
      "{:<32} {:<30} {:>6} {:>8} {:>12} {:>9} {:>14} {:>10} {:>10} {:>10} {:>9} {:>10} {:>10} {:>9}";

  static std::string title() {
    return fmt::format(
        kFormat,
        "case",
        "build",
        "runs",
        "rows",
        "avg_wall_us",
        "speedup",
        "max_peak_bytes",
        "mem_reduct",
        "spill_mb",
        "spill_rows",
        "add_ms",
        "compute_ms",
        "extract_ms",
        "output_ms");
  }

  uint64_t avgWallUs() const {
    return iterations == 0 ? 0 : wallUs / iterations;
  }

  double avgAddInputMs() const {
    return iterations == 0 ? 0 : addInputMs / iterations;
  }

  double avgComputeMs() const {
    return iterations == 0 ? 0 : computeMs / iterations;
  }

  double avgExtractMs() const {
    return iterations == 0 ? 0 : extractMs / iterations;
  }

  double avgOutputMs() const {
    return iterations == 0 ? 0 : outputMs / iterations;
  }

  std::string toString(const BenchmarkResult* baseline = nullptr) const {
    const auto avgWall = avgWallUs();
    const auto speedup = baseline == nullptr || avgWall == 0
        ? std::string("-")
        : fmt::format("{:.2f}x", baseline->avgWallUs() * 1.0 / avgWall);
    const auto memoryReduction = baseline == nullptr || peakMemoryBytes == 0
        ? std::string("-")
        : fmt::format(
              "{:.2f}x", baseline->peakMemoryBytes * 1.0 / peakMemoryBytes);
    return fmt::format(
        kFormat,
        caseName,
        buildName,
        iterations,
        outputRows,
        avgWall,
        speedup,
        peakMemoryBytes,
        memoryReduction,
        fmt::format("{:.2f}", spilledBytes / 1024.0 / 1024.0),
        spilledRows,
        fmt::format("{:.3f}", avgAddInputMs()),
        fmt::format("{:.3f}", avgComputeMs()),
        fmt::format("{:.3f}", avgExtractMs()),
        fmt::format("{:.3f}", avgOutputMs()));
  }
};

void recordResult(
    std::vector<BenchmarkResult>& results,
    BenchmarkResult sample) {
  auto it = std::find_if(results.begin(), results.end(), [&](const auto& row) {
    return row.caseName == sample.caseName && row.buildName == sample.buildName;
  });
  if (it == results.end()) {
    results.push_back(std::move(sample));
    return;
  }

  ++it->iterations;
  it->outputRows = sample.outputRows;
  it->wallUs += sample.wallUs;
  it->peakMemoryBytes = std::max(it->peakMemoryBytes, sample.peakMemoryBytes);
  it->spilledBytes = std::max(it->spilledBytes, sample.spilledBytes);
  it->spilledRows = std::max(it->spilledRows, sample.spilledRows);
  it->addInputMs += sample.addInputMs;
  it->computeMs += sample.computeMs;
  it->extractMs += sample.extractMs;
  it->outputMs += sample.outputMs;
}

class StreamingWindowBuildBenchmark : public VectorTestBase {
 public:
  StreamingWindowBuildBenchmark() = default;

  BenchmarkResult run(
      const BenchmarkCase& benchmarkCase,
      WindowBuildType buildType,
      const std::string& buildName) {
    const auto& data = inputForProfile(benchmarkCase.input);
    core::PlanNodeId windowId;
    auto plan = PlanBuilder()
                    .values(data)
                    .streamingWindow(benchmarkCase.windowFunctions)
                    .capturePlanNodeId(windowId)
                    .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    TestWindowInjection windowInjection(buildType);

    AssertQueryBuilder query(plan);
    query.config(core::QueryConfig::kPreferredOutputBatchRows, 1024)
        .config(core::QueryConfig::kMaxOutputBatchRows, 1024)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, 1 << 20);

    if (buildType == WindowBuildType::kBmStreamingWindowBuild) {
      query.config(core::QueryConfig::kBufferManagerEnabled, "true")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->path);
    }

    std::shared_ptr<Task> task;
    const auto startUs = getCurrentTimeMicro();
    const auto outputRows = query.runWithoutResults(task);
    const auto wallUs = getCurrentTimeMicro() - startUs;

    const auto planStats = toPlanStats(task->taskStats());
    const auto& stats = planStats.at(windowId);

    BenchmarkResult result;
    result.caseName = benchmarkCase.name;
    result.buildName = buildName;
    result.outputRows = outputRows;
    result.wallUs = wallUs;
    result.peakMemoryBytes = stats.peakMemoryBytes;
    result.spilledBytes = stats.spilledBytes;
    result.spilledRows = stats.spilledRows;
    result.addInputMs = stats.windowAddInputTime / 1'000'000.0;
    result.computeMs = stats.windowComputeWindowFunctionTime / 1'000'000.0;
    result.extractMs = stats.windowExtractColumnTime / 1'000'000.0;
    result.outputMs = stats.windowOutputTime / 1'000'000.0;
    return result;
  }

 private:
  const std::vector<RowVectorPtr>& inputForProfile(InputProfile profile) {
    profile.partitionRows = std::max(1, profile.partitionRows);
    profile.peerGroupRows = std::max(1, profile.peerGroupRows);
    auto [it, inserted] = inputsByProfile_.try_emplace(profile.key());
    if (inserted) {
      it->second = makeSortedInput(profile);
    }
    return it->second;
  }

  std::vector<RowVectorPtr> makeSortedInput(const InputProfile& profile) {
    std::vector<RowVectorPtr> vectors;
    vectors.reserve(FLAGS_window_benchmark_num_vectors);

    const auto stringBytes = profile.payload == PayloadProfile::kLargeString
        ? FLAGS_window_benchmark_string_bytes
        : 16;
    const auto nullable = profile.payload == PayloadProfile::kNullable;
    std::string scratch;
    for (auto vector = 0; vector < FLAGS_window_benchmark_num_vectors;
         ++vector) {
      const auto base = vector * FLAGS_window_benchmark_rows_per_vector;
      vectors.push_back(makeRowVector(
          {"d", "x", "v", "p", "s", "off", "def"},
          {
              makeFlatVector<int64_t>(
                  FLAGS_window_benchmark_rows_per_vector,
                  [base](auto row) {
                    return static_cast<int64_t>((base + row) % 17);
                  },
                  [base, nullable](auto row) {
                    return nullable && (base + row) % 7 == 0;
                  }),
              makeFlatVector<double>(
                  FLAGS_window_benchmark_rows_per_vector,
                  [base](auto row) {
                    return 0.25 + static_cast<double>((base + row) % 31);
                  },
                  [base, nullable](auto row) {
                    return nullable && (base + row) % 11 == 0;
                  }),
              makeFlatVector<StringView>(
                  FLAGS_window_benchmark_rows_per_vector,
                  [&](auto row) {
                    scratch.assign(stringBytes, 'a' + (base + row) % 26);
                    return StringView(scratch);
                  },
                  [base, nullable](auto row) {
                    return nullable && (base + row) % 13 == 0;
                  }),
              makeFlatVector<int32_t>(
                  FLAGS_window_benchmark_rows_per_vector,
                  [base, partitionRows = profile.partitionRows](auto row) {
                    return (base + row) / partitionRows;
                  }),
              makeFlatVector<int64_t>(
                  FLAGS_window_benchmark_rows_per_vector,
                  [base,
                   partitionRows = profile.partitionRows,
                   peerGroupRows = profile.peerGroupRows](auto row) {
                    const auto rowInPartition = (base + row) % partitionRows;
                    return static_cast<int64_t>(rowInPartition / peerGroupRows);
                  }),
              makeFlatVector<int64_t>(
                  FLAGS_window_benchmark_rows_per_vector,
                  [base](auto row) {
                    return static_cast<int64_t>((base + row) % 3) + 1;
                  }),
              makeFlatVector<int64_t>(
                  FLAGS_window_benchmark_rows_per_vector,
                  [base](auto row) {
                    return -1000 - static_cast<int64_t>(base + row);
                  }),
          }));
    }
    return vectors;
  }

  std::unordered_map<std::string, std::vector<RowVectorPtr>> inputsByProfile_;
};

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  filesystems::registerLocalFileSystem();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  window::prestosql::registerAllWindowFunctions();
  parse::registerTypeResolver();

  StreamingWindowBuildBenchmark benchmark;
  std::vector<BenchmarkResult> results;

  const auto totalRows = FLAGS_window_benchmark_num_vectors *
      FLAGS_window_benchmark_rows_per_vector;
  const auto defaultPartitionRows =
      std::max(1, FLAGS_window_benchmark_partition_rows);
  const auto unalignedPartitionRows =
      std::max(1, FLAGS_window_benchmark_rows_per_vector + 1);
  const auto widePeerRows = std::min(defaultPartitionRows, 128);

  const InputProfile defaultFixed{
      defaultPartitionRows, 1, PayloadProfile::kFixed};
  const InputProfile widePeer{
      defaultPartitionRows, widePeerRows, PayloadProfile::kFixed};
  const InputProfile fullPeer{
      defaultPartitionRows, defaultPartitionRows, PayloadProfile::kFixed};
  const InputProfile manyTiny{4, 1, PayloadProfile::kFixed};
  const InputProfile unaligned{
      unalignedPartitionRows, 1, PayloadProfile::kFixed};
  const InputProfile singleLarge{totalRows, 1, PayloadProfile::kFixed};
  const InputProfile nullable{
      defaultPartitionRows, 1, PayloadProfile::kNullable};
  const InputProfile largeString{
      defaultPartitionRows, 1, PayloadProfile::kLargeString};

  std::vector<BenchmarkCase> cases{
      {"row_number_default",
       {"row_number() over (partition by p order by s)"},
       defaultFixed},
      {"rank_unique_peer",
       {"rank() over (partition by p order by s)"},
       defaultFixed},
      {"rank_128_peer", {"rank() over (partition by p order by s)"}, widePeer},
      {"rank_full_peer", {"rank() over (partition by p order by s)"}, fullPeer},
      {"many_tiny_row_number",
       {"row_number() over (partition by p order by s)"},
       manyTiny},
      {"unaligned_running_sum",
       {"sum(d) over (partition by p order by s rows between unbounded preceding and current row)"},
       unaligned},
      {"single_part_running_sum",
       {"sum(d) over (partition by p order by s rows between unbounded preceding and current row)"},
       singleLarge},
      {"reverse_running_sum",
       {"sum(d) over (partition by p order by s rows between current row and unbounded following)"},
       defaultFixed},
      {"bounded_rows_64_sum",
       {"sum(d) over (partition by p order by s rows between 64 preceding and 64 following)"},
       defaultFixed},
      {"bounded_range_col_sum",
       {"sum(d) over (partition by p order by s range between off preceding and off following)"},
       defaultFixed},
      {"lead_lag_nullable",
       {"lead(d, off, def) over (partition by p order by s)",
        "lag(d, off, def) over (partition by p order by s)"},
       nullable},
      {"ignore_nulls_values",
       {"first_value(d IGNORE NULLS) over (partition by p order by s rows between 2 preceding and 2 following)",
        "last_value(d IGNORE NULLS) over (partition by p order by s rows between unbounded preceding and unbounded following)",
        "nth_value(d, off IGNORE NULLS) over (partition by p order by s rows between unbounded preceding and unbounded following)",
        "lead(d, off, def IGNORE NULLS) over (partition by p order by s)",
        "lag(d, off, def IGNORE NULLS) over (partition by p order by s)"},
       nullable},
      {"multi_function_mix",
       {"row_number() over (partition by p order by s)",
        "rank() over (partition by p order by s)",
        "lead(d, 1, def) over (partition by p order by s)",
        "sum(d) over (partition by p order by s rows between unbounded preceding and current row)",
        "count(v) over (partition by p order by s rows between unbounded preceding and unbounded following)"},
       defaultFixed},
      {"multi_agg_types",
       {"sum(d) over (partition by p order by s rows between unbounded preceding and current row)",
        "avg(x) over (partition by p order by s rows between unbounded preceding and current row)",
        "min(d) over (partition by p order by s rows between unbounded preceding and unbounded following)",
        "max(d) over (partition by p order by s rows between unbounded preceding and unbounded following)"},
       defaultFixed},
      {"count_large_varchar_full",
       {"count(v) over (partition by p order by s rows between unbounded preceding and unbounded following)"},
       largeString}};

  for (const auto& benchmarkCase : cases) {
    folly::addBenchmark(
        __FILE__,
        benchmarkCase.name + "_streaming",
        [&benchmark, &benchmarkCase, &results](unsigned int iters) {
          for (auto i = 0U; i < iters; ++i) {
            recordResult(
                results,
                benchmark.run(
                    benchmarkCase,
                    WindowBuildType::kSortWindowBuild,
                    "StreamingWindowBuild"));
          }
          return iters;
        });
    folly::addBenchmark(
        __FILE__,
        benchmarkCase.name + "_bm_streaming",
        [&benchmark, &benchmarkCase, &results](unsigned int iters) {
          for (auto i = 0U; i < iters; ++i) {
            recordResult(
                results,
                benchmark.run(
                    benchmarkCase,
                    WindowBuildType::kBmStreamingWindowBuild,
                    "BmStreamingWindowBuild"));
          }
          return iters;
        });
    folly::addBenchmark(__FILE__, "-", [](unsigned int) { return 0; });
  }

  if (FLAGS_window_benchmark_include_bm_slow) {
    std::cerr
        << "Warning: --window_benchmark_include_bm_slow is a no-op for "
           "BmStreamingWindowBuild. BM spill is triggered through operator "
           "reclaim/MemoryArbitrator, not TestScopedSpillInjection.\n";
  }

  folly::runBenchmarks();

  std::unordered_map<std::string, const BenchmarkResult*> baselines;
  for (const auto& result : results) {
    if (result.buildName == "StreamingWindowBuild") {
      baselines[result.caseName] = &result;
    }
  }

  std::cout << "\nInput: vectors=" << FLAGS_window_benchmark_num_vectors
            << ", rows_per_vector=" << FLAGS_window_benchmark_rows_per_vector
            << ", total_rows=" << totalRows
            << ", default_partition_rows=" << defaultPartitionRows
            << ", unaligned_partition_rows=" << unalignedPartitionRows
            << ", string_bytes=" << FLAGS_window_benchmark_string_bytes
            << ", cases=" << cases.size() << ", include_bm_slow="
            << (FLAGS_window_benchmark_include_bm_slow ? "true" : "false")
            << "\n";

  std::cout << "\n" << BenchmarkResult::title() << "\n";
  for (const auto& result : results) {
    const auto baseline = baselines.find(result.caseName);
    std::cout << result.toString(
                     baseline == baselines.end() ? nullptr : baseline->second)
              << "\n";
  }

  return 0;
}
