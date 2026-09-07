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

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/core/QueryConfig.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;

DEFINE_int32(aggregation_benchmark_rows, 10'000'000, "Number of input rows");
DEFINE_int32(
    aggregation_benchmark_groups,
    1'000'000,
    "Number of distinct groups");
DEFINE_int32(
    aggregation_benchmark_batch_rows,
    10'000,
    "Number of rows per input batch");

namespace {

constexpr int32_t kNumKeys = 6;
constexpr int32_t kNumValues = 30;
constexpr int32_t kNumColumns = kNumKeys + kNumValues;

class HashAggregationStringAllocationBenchmark : public VectorTestBase {
 public:
  HashAggregationStringAllocationBenchmark() {
    BOLT_CHECK_GT(FLAGS_aggregation_benchmark_rows, 0);
    BOLT_CHECK_GT(FLAGS_aggregation_benchmark_groups, 0);
    BOLT_CHECK_LE(
        FLAGS_aggregation_benchmark_groups, FLAGS_aggregation_benchmark_rows);
    BOLT_CHECK_GT(FLAGS_aggregation_benchmark_batch_rows, 0);
    makeData();
    makePlan();
  }

  int64_t run(bool useMonoAlloc) {
    std::shared_ptr<Task> task;
    auto result = AssertQueryBuilder(plan_)
                      .maxDrivers(1)
                      .config("use_mono_alloc", useMonoAlloc ? "true" : "false")
                      .copyResults(pool_.get(), task);
    BOLT_CHECK_NOT_NULL(task);
    BOLT_CHECK_EQ(result->size(), FLAGS_aggregation_benchmark_groups);
    BOLT_CHECK_EQ(result->childrenSize(), kNumColumns);
    folly::doNotOptimizeAway(result);
    return task->pool()->peakBytes();
  }

 private:
  static std::string makeString(int64_t value, int32_t column, int32_t length) {
    auto result = fmt::format("{:010d}{:02d}", value, column);
    result.append(length - result.size(), 'a' + column % 26);
    return result;
  }

  void makeData() {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(kNumColumns);
    types.reserve(kNumColumns);
    for (auto column = 0; column < kNumKeys; ++column) {
      names.push_back(fmt::format("k{}", column + 1));
      types.push_back(VARCHAR());
    }
    for (auto column = 0; column < kNumValues; ++column) {
      names.push_back(fmt::format("c{}", column + 1));
      types.push_back(VARCHAR());
    }
    rowType_ = ROW(std::move(names), std::move(types));

    for (int32_t batchStart = 0; batchStart < FLAGS_aggregation_benchmark_rows;
         batchStart += FLAGS_aggregation_benchmark_batch_rows) {
      const auto batchSize = std::min(
          FLAGS_aggregation_benchmark_batch_rows,
          FLAGS_aggregation_benchmark_rows - batchStart);
      std::vector<VectorPtr> children;
      children.reserve(kNumColumns);
      for (auto column = 0; column < kNumColumns; ++column) {
        const auto length = 20 + column % 11;
        children.push_back(makeFlatVector<std::string>(
            batchSize, [batchStart, column, length](auto row) {
              const auto inputRow = batchStart + row;
              const auto value = column < kNumKeys
                  ? inputRow % FLAGS_aggregation_benchmark_groups
                  : inputRow;
              return makeString(value, column, length);
            }));
      }
      batches_.push_back(makeRowVector(rowType_->names(), children));
    }
  }

  void makePlan() {
    std::vector<std::string> groupingKeys;
    groupingKeys.reserve(kNumKeys);
    for (auto column = 0; column < kNumKeys; ++column) {
      groupingKeys.push_back(fmt::format("k{}", column + 1));
    }

    std::vector<std::string> aggregates;
    aggregates.reserve(kNumValues);
    for (auto column = 0; column < kNumValues; ++column) {
      aggregates.push_back(fmt::format("max(c{})", column + 1));
    }

    plan_ = PlanBuilder()
                .values(batches_)
                .singleAggregation(groupingKeys, aggregates)
                .planNode();
  }

  RowTypePtr rowType_;
  std::vector<RowVectorPtr> batches_;
  core::PlanNodePtr plan_;
};

HashAggregationStringAllocationBenchmark& benchmark() {
  static auto instance =
      std::make_unique<HashAggregationStringAllocationBenchmark>();
  return *instance;
}

BENCHMARK(Aggregation_HashStringAllocator) {
  folly::BenchmarkSuspender suspender;
  auto& instance = benchmark();
  suspender.dismiss();
  const auto peakBytes = instance.run(false);
  suspender.rehire();
  fmt::print("poolPeakBytes={}\n", peakBytes);
}

BENCHMARK_RELATIVE(Aggregation_MonotonicMemoryResource) {
  folly::BenchmarkSuspender suspender;
  auto& instance = benchmark();
  suspender.dismiss();
  const auto peakBytes = instance.run(true);
  suspender.rehire();
  fmt::print("poolPeakBytes={}\n", peakBytes);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::Options options;
  options.useMmapAllocator = false;
  options.allocatorCapacity = 64UL << 30;
#ifdef BOLT_AGG_BENCHMARK_HAS_ALIGNED_BUF_ALLOC_STRATEGY
  options.enableAlignedBufAllocStrategy = false;
#endif
  memory::MemoryManager::initialize(options);
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  folly::runBenchmarks();
  return 0;
}
