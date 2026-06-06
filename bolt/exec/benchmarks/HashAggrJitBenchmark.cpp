/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <limits>

#include "bolt/core/QueryConfig.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::test;

DEFINE_int32(hashaggr_jit_benchmark_batches, 20, "Number of input batches.");
DEFINE_int32(hashaggr_jit_benchmark_batch_size, 10000, "Rows per input batch.");
DEFINE_int32(hashaggr_jit_benchmark_groups, 10000, "Number of distinct groups.");

namespace {

struct HashAggrJitBenchmarkCase {
  std::shared_ptr<const core::PlanNode> plan;
};

enum class AggregationPlanKind {
  Single,
  Partial,
  PartialFinal,
};

class HashAggrJitBenchmark : public VectorTestBase {
 public:
  void addBenchmark(const std::string& name, int32_t width) {
    auto rows = makeRows(width);
    std::vector<std::string> sums;
    std::vector<std::string> avgs;
    std::vector<std::string> mins;
    std::vector<std::string> counts;
    sums.reserve(width);
    avgs.reserve(width);
    mins.reserve(width);
    counts.reserve(width);
    for (auto i = 0; i < width; ++i) {
      sums.push_back(fmt::format("spark_sum(c{})", i + 1));
      avgs.push_back(fmt::format("spark_avg(c{})", i + 1));
      mins.push_back(fmt::format("min(c{})", i + 1));
      counts.push_back(fmt::format("count(c{})", i + 1));
    }

    addCase(name + "_sum", rows, sums);
    addCase(name + "_avg", rows, avgs);
    addCase(name + "_min", rows, mins);
    addCase(name + "_count", rows, counts);
    addCase(name + "_merge_sum", rows, sums, AggregationPlanKind::PartialFinal);
    addCase(name + "_merge_avg", rows, avgs, AggregationPlanKind::PartialFinal);
    addCase(name + "_merge_min", rows, mins, AggregationPlanKind::PartialFinal);
    addCase(name + "_merge_count", rows, counts, AggregationPlanKind::PartialFinal);
  }

  void addDecimalBenchmark(const std::string& name, int32_t width) {
    auto rows = makeDecimalRows(width);
    std::vector<std::string> sums;
    std::vector<std::string> avgs;
    for (auto i = 0; i < width; ++i) {
      sums.push_back(fmt::format("spark_sum(c{})", i + 1));
      avgs.push_back(fmt::format("spark_avg(c{})", i + 1));
    }
    addCase(name + "_decimal_sum", rows, sums);
    addCase(name + "_decimal_avg", rows, avgs);
  }

  void addFloatingPointMinMaxBenchmark(const std::string& name, int32_t width) {
    auto rows = makeDoubleRows(width);
    std::vector<std::string> mins;
    std::vector<std::string> maxs;
    for (auto i = 0; i < width; ++i) {
      mins.push_back(fmt::format("min(c{})", i + 1));
      maxs.push_back(fmt::format("max(c{})", i + 1));
    }
    addCase(name + "_double_min", rows, mins);
    addCase(name + "_double_max", rows, maxs);
  }

  void addHighCardinalityExtractBenchmark(const std::string& name, int32_t width) {
    auto rows = makeHighCardinalityRows(width);
    std::vector<std::string> avgs;
    std::vector<std::string> sums;
    avgs.reserve(width);
    sums.reserve(width);
    for (auto i = 0; i < width; ++i) {
      avgs.push_back(fmt::format("spark_avg(c{})", i + 1));
      sums.push_back(fmt::format("spark_sum(c{})", i + 1));
    }
    addCase(name + "_partial_avg_extract", rows, avgs, AggregationPlanKind::Partial);
    addCase(name + "_partial_sum_extract", rows, sums, AggregationPlanKind::Partial);
  }

 private:
  std::vector<RowVectorPtr> makeRows(int32_t width) {
    std::vector<std::string> names;
    std::vector<VectorPtr> children;
    names.reserve(width + 1);
    children.reserve(width + 1);
    names.push_back("c0");
    children.push_back(makeFlatVector<int64_t>(
        FLAGS_hashaggr_jit_benchmark_batch_size,
        [](vector_size_t row) { return row % FLAGS_hashaggr_jit_benchmark_groups; }));

    for (auto column = 0; column < width; ++column) {
      names.push_back(fmt::format("c{}", column + 1));
      children.push_back(makeFlatVector<int64_t>(
          FLAGS_hashaggr_jit_benchmark_batch_size,
          [column](vector_size_t row) {
            return static_cast<int64_t>((row + 17 * column) & 0xffff);
          }));
    }

    auto batch = makeRowVector(names, children);
    std::vector<RowVectorPtr> rows;
    rows.reserve(FLAGS_hashaggr_jit_benchmark_batches);
    for (auto i = 0; i < FLAGS_hashaggr_jit_benchmark_batches; ++i) {
      rows.push_back(batch);
    }
    return rows;
  }

  std::vector<RowVectorPtr> makeDecimalRows(int32_t width) {
    std::vector<std::string> names;
    std::vector<VectorPtr> children;
    names.push_back("c0");
    children.push_back(makeFlatVector<int64_t>(
        FLAGS_hashaggr_jit_benchmark_batch_size,
        [](vector_size_t row) { return row % FLAGS_hashaggr_jit_benchmark_groups; }));
    for (auto column = 0; column < width; ++column) {
      names.push_back(fmt::format("c{}", column + 1));
      children.push_back(makeFlatVector<int64_t>(
          FLAGS_hashaggr_jit_benchmark_batch_size,
          [column](vector_size_t row) {
            return static_cast<int64_t>((row + 13 * column) % 100000);
          },
          nullptr,
          DECIMAL(12, 2)));
    }
    auto batch = makeRowVector(names, children);
    return std::vector<RowVectorPtr>(FLAGS_hashaggr_jit_benchmark_batches, batch);
  }

  std::vector<RowVectorPtr> makeDoubleRows(int32_t width) {
    std::vector<std::string> names;
    std::vector<VectorPtr> children;
    names.push_back("c0");
    children.push_back(makeFlatVector<int64_t>(
        FLAGS_hashaggr_jit_benchmark_batch_size,
        [](vector_size_t row) { return row % FLAGS_hashaggr_jit_benchmark_groups; }));
    for (auto column = 0; column < width; ++column) {
      names.push_back(fmt::format("c{}", column + 1));
      children.push_back(makeFlatVector<double>(
          FLAGS_hashaggr_jit_benchmark_batch_size,
          [column](vector_size_t row) {
            if ((row + column) % 997 == 0) {
              return std::numeric_limits<double>::quiet_NaN();
            }
            return static_cast<double>((row + 17 * column) & 0xffff);
          }));
    }
    auto batch = makeRowVector(names, children);
    return std::vector<RowVectorPtr>(FLAGS_hashaggr_jit_benchmark_batches, batch);
  }

  std::vector<RowVectorPtr> makeHighCardinalityRows(int32_t width) {
    std::vector<RowVectorPtr> rows;
    rows.reserve(FLAGS_hashaggr_jit_benchmark_batches);
    for (auto batchIndex = 0; batchIndex < FLAGS_hashaggr_jit_benchmark_batches; ++batchIndex) {
      std::vector<std::string> names;
      std::vector<VectorPtr> children;
      names.reserve(width + 1);
      children.reserve(width + 1);
      names.push_back("c0");
      children.push_back(makeFlatVector<int64_t>(
          FLAGS_hashaggr_jit_benchmark_batch_size,
          [batchIndex](vector_size_t row) {
            return static_cast<int64_t>(batchIndex) *
                    FLAGS_hashaggr_jit_benchmark_batch_size +
                row;
          }));
      for (auto column = 0; column < width; ++column) {
        names.push_back(fmt::format("c{}", column + 1));
        children.push_back(makeFlatVector<int64_t>(
            FLAGS_hashaggr_jit_benchmark_batch_size,
            [batchIndex, column](vector_size_t row) {
              return static_cast<int64_t>(batchIndex * 97 + column * 17 + row);
            }));
      }
      rows.push_back(makeRowVector(names, children));
    }
    return rows;
  }

  std::shared_ptr<const core::PlanNode> makePlan(
      const std::vector<RowVectorPtr>& rows,
      const std::vector<std::string>& aggregates,
      AggregationPlanKind planKind = AggregationPlanKind::Single) {
    exec::test::PlanBuilder builder;
    builder.values(rows);
    switch (planKind) {
      case AggregationPlanKind::Single:
        builder.singleAggregation({"c0"}, aggregates);
        break;
      case AggregationPlanKind::Partial:
        builder.partialAggregation({"c0"}, aggregates);
        break;
      case AggregationPlanKind::PartialFinal:
        builder.partialAggregation({"c0"}, aggregates).finalAggregation();
        break;
    }
    return builder.planNode();
  }

  void run(const std::shared_ptr<const core::PlanNode>& plan, bool enableJit) {
    exec::test::AssertQueryBuilder(plan)
        .config(core::QueryConfig::kHashAggrJitEnabled, enableJit ? "true" : "false")
        .config(core::QueryConfig::kHashAggrJitMinFuseWidth, "4")
        .config(core::QueryConfig::kHashAggrJitMaxFuseWidth, "16")
        .copyResults(pool_.get());
  }

  void addCase(
      const std::string& name,
      const std::vector<RowVectorPtr>& rows,
      const std::vector<std::string>& aggregates,
      AggregationPlanKind planKind = AggregationPlanKind::Single) {
    auto testCase = std::make_unique<HashAggrJitBenchmarkCase>();
    testCase->plan = makePlan(rows, aggregates, planKind);
    // Warm up both paths so the benchmark compares steady-state execution and
    // doesn't charge one-time plan setup / JIT compilation to the first sample.
    run(testCase->plan, false);
    run(testCase->plan, true);
    auto* testCasePtr = testCase.get();
    folly::addBenchmark(__FILE__, name + "_nojit", [this, testCasePtr]() {
      run(testCasePtr->plan, false);
      return 1;
    });
    folly::addBenchmark(__FILE__, name + "_jit", [this, testCasePtr]() {
      run(testCasePtr->plan, true);
      return 1;
    });
    folly::addBenchmark(__FILE__, "-", []() { return 0; });
    cases_.push_back(std::move(testCase));
  }

  std::vector<std::unique_ptr<HashAggrJitBenchmarkCase>> cases_;
};

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  aggregate::prestosql::registerAllAggregateFunctions();
  functions::aggregate::sparksql::registerAggregateFunctions("spark_", false);
  parse::registerTypeResolver();

  HashAggrJitBenchmark benchmark;
  benchmark.addBenchmark("width4", 4);
  benchmark.addBenchmark("width8", 8);
  benchmark.addBenchmark("width16", 16);
  benchmark.addBenchmark("width32", 32);
  benchmark.addDecimalBenchmark("width8", 8);
  benchmark.addFloatingPointMinMaxBenchmark("width8", 8);
  benchmark.addHighCardinalityExtractBenchmark("width8_high_card", 8);

  folly::runBenchmarks();
  return 0;
}
