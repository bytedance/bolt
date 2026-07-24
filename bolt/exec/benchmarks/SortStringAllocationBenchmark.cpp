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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;

DEFINE_int32(sort_benchmark_rows, 1'000'000, "Number of input rows");

namespace {
class SortBenchmark : public VectorTestBase {
 public:
  SortBenchmark() {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (int i = 0; i < 4; ++i) {
      names.push_back(fmt::format("key{}", i + 1));
      types.push_back(VARCHAR());
    }
    for (int i = 0; i < 50; ++i) {
      names.push_back(fmt::format("payload{}", i + 1));
      types.push_back(VARCHAR());
    }
    auto rowType = ROW(std::move(names), std::move(types));
    std::vector<VectorPtr> children;
    for (int i = 0; i < 4; ++i) {
      children.push_back(
          makeFlatVector<std::string>(FLAGS_sort_benchmark_rows, [i](auto row) {
            return fmt::format("key{}_{}", i, (row * 7919 + i * 17) % 100000);
          }));
    }
    for (int i = 0; i < 50; ++i) {
      children.push_back(makeFlatVector<std::string>(
          FLAGS_sort_benchmark_rows,
          [i](auto row) { return fmt::format("payload{}_{}", i, row); }));
    }
    input_ = makeRowVector(rowType->names(), children);
    plan_ = PlanBuilder()
                .values({input_})
                .orderBy({"key1", "key2", "key3", "key4"}, false)
                .planNode();
  }

  void run() {
    auto result =
        AssertQueryBuilder(plan_).maxDrivers(1).copyResults(pool_.get());
    folly::doNotOptimizeAway(result);
    fmt::print("poolPeakBytes={}\n", pool_->peakBytes());
  }

 private:
  RowVectorPtr input_;
  core::PlanNodePtr plan_;
};

BENCHMARK(Sort_Key4Payload50) {
  folly::BenchmarkSuspender suspender;
  static SortBenchmark benchmark;
  suspender.dismiss();
  benchmark.run();
  suspender.rehire();
}
} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  folly::runBenchmarks();
}
