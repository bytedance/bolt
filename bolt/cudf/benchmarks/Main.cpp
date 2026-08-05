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

#include <fmt/core.h>
#include <folly/init/Init.h>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "bolt/cudf/benchmarks/Aggregation.h"
#include "bolt/cudf/benchmarks/BenchmarkBase.h"
#include "bolt/cudf/benchmarks/OrderBy.h"
#include "bolt/tpch/gen/TpchGen.h"

DEFINE_double(
    bolt_benchmark_scale_factor,
    1,
    "The scale factor for an input data set");
DEFINE_string(
    bolt_benchmark_op_type,
    "",
    "The type of operation to run (available: orderby, aggregation)");
DEFINE_int32(bolt_benchmark_batch_size, 0, "Use batches for input data");

namespace bolt::cudf::benchmark {
using CpuBenchmarkPtr = std::unique_ptr<BenchmarkBase<false>>;
using GpuBenchmarkPtr = std::unique_ptr<BenchmarkBase<true>>;
using BenchmarkPair = std::pair<CpuBenchmarkPtr, GpuBenchmarkPtr>;

BenchmarkPair createBenchmarks(
    const std::string& opType,
    std::shared_ptr<std::vector<bytedance::bolt::RowVectorPtr>> input) {
  using BenchmarkCreator = std::function<BenchmarkPair(
      std::shared_ptr<std::vector<bytedance::bolt::RowVectorPtr>>)>;

  const static std::map<std::string, BenchmarkCreator> benchmarkRegistry = {
      {"orderby",
       [](std::shared_ptr<std::vector<bytedance::bolt::RowVectorPtr>> vec) {
         return std::make_pair(
             std::make_unique<OrderByBenchmark<false>>(vec),
             std::make_unique<OrderByBenchmark<true>>(vec));
       }},
      {"aggregation",
       [](std::shared_ptr<std::vector<bytedance::bolt::RowVectorPtr>> vec) {
         return std::make_pair(
             std::make_unique<AggregationBenchmark<false>>(vec),
             std::make_unique<AggregationBenchmark<true>>(vec));
       }}};

  if (auto it = benchmarkRegistry.find(opType); it != benchmarkRegistry.end()) {
    return it->second(input);
  }

  BOLT_USER_FAIL(
      "Unsupported opType '{}'. Available types: orderby, aggregation.",
      opType);
}

void runAndReport(CpuBenchmarkPtr& cpu, GpuBenchmarkPtr& gpu) {
  cpu->addBenchmark();
  gpu->addBenchmark();
  folly::runBenchmarks();

  fmt::print("\n--- CPU Stats ---\n");
  cpu->reportStats();
  fmt::print("\n--- GPU Stats ---\n");
  gpu->reportStats();
}
} // namespace bolt::cudf::benchmark

int main(int argc, char** argv) {
  // Disabled unnecessary serialization of input in value operator
  FLAGS_minloglevel = 3;
  folly::init(&argc, &argv);
  bytedance::bolt::memory::MemoryManager::initialize({});
  auto pool = bytedance::bolt::memory::memoryManager()->addLeafPool();

  size_t totalUsedSize = 0;
  std::vector<bytedance::bolt::RowVectorPtr> batches;
  const bytedance::bolt::vector_size_t maxInputSize =
      6'000'000 * FLAGS_bolt_benchmark_scale_factor;
  const bytedance::bolt::vector_size_t batchSize =
      FLAGS_bolt_benchmark_batch_size == 0 ? maxInputSize
                                           : FLAGS_bolt_benchmark_batch_size;
  for (auto offset = 0; offset < maxInputSize + batchSize;
       offset += batchSize) {
    auto batch = bytedance::bolt::tpch::genTpchLineItem(
        pool.get(),
        std::min(batchSize, maxInputSize - offset),
        offset,
        FLAGS_bolt_benchmark_scale_factor,
        true);
    totalUsedSize += batch->usedSize();
    batches.emplace_back(std::move(batch));
  }

  std::cout << "batchSize=" << batchSize << std::endl;
  std::cout << "totalUsedSize In GiB: " << totalUsedSize / (1 << 30)
            << std::endl;

  auto [cpuBenchmark, gpuBenchmark] = bolt::cudf::benchmark::createBenchmarks(
      FLAGS_bolt_benchmark_op_type,
      std::make_shared<std::vector<bytedance::bolt::RowVectorPtr>>(batches));

  if (!cpuBenchmark || !gpuBenchmark) {
    fmt::print(
        stderr,
        "Error: Unsupported opType '{}'. Available types: orderby, aggregation.\n",
        FLAGS_bolt_benchmark_op_type);
    return 1;
  }

  bolt::cudf::benchmark::runAndReport(cpuBenchmark, gpuBenchmark);

  return 0;
}
