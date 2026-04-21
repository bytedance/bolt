/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
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
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/functions/sparksql/registration/Register.h"

using namespace bytedance;
using namespace bytedance::bolt;

namespace {

constexpr int kVectorSize = 1000;

// Build a RowVectorPtr with one VARCHAR child of N=kVectorSize strings in
// "yyyy-MM-dd HH:mm:ss" format sampled uniformly at random from the given
// inclusive [lowYear, highYear] range. Seed is fixed so benchmark runs
// compare apples-to-apples across binaries.
RowVectorPtr makeDateStringColumn(
    ExpressionBenchmarkBuilder& builder,
    int lowYear,
    int highYear,
    uint64_t seed) {
  std::mt19937_64 rng{seed};
  std::uniform_int_distribution<int> yearDist(lowYear, highYear);
  std::uniform_int_distribution<int> monthDist(1, 12);
  std::uniform_int_distribution<int> dayDist(
      1, 28); // avoid month-length branches
  std::uniform_int_distribution<int> hourDist(0, 23);
  std::uniform_int_distribution<int> minuteDist(0, 59);
  std::uniform_int_distribution<int> secondDist(0, 59);

  std::vector<std::string> rows;
  rows.reserve(kVectorSize);
  for (int i = 0; i < kVectorSize; ++i) {
    rows.push_back(fmt::format(
        "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}",
        yearDist(rng),
        monthDist(rng),
        dayDist(rng),
        hourDist(rng),
        minuteDist(rng),
        secondDist(rng)));
  }
  auto col = builder.vectorMaker().flatVector<std::string>(rows);
  return builder.vectorMaker().rowVector({"c0"}, {col});
}

} // namespace

int main(int argc, char** argv) {
  // Benchmark for Spark unix_timestamp in the Asia/Shanghai session time
  // zone. Two sets:
  //   shanghai_cst  - post-1991 modern timestamps, exercises the CST fast
  //                   path (no DST, no LMT, pure arithmetic).
  //   shanghai_dst  - 1986-1991 non-gap timestamps, exercises the tzdata
  //                   slow path used for all historical DST regimes.
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::sparksql::registerFunctions("");

  ExpressionBenchmarkBuilder benchmarkBuilder;
  benchmarkBuilder.setTimezone("Asia/Shanghai");

  benchmarkBuilder
      .addBenchmarkSet(
          "shanghai_cst",
          makeDateStringColumn(benchmarkBuilder, 2000, 2024, /*seed=*/0xBEEF01))
      .addExpression(
          "unix_timestamp", "unix_timestamp(c0, 'yyyy-MM-dd HH:mm:ss')")
      .disableTesting()
      .withIterations(1000);

  benchmarkBuilder
      .addBenchmarkSet(
          "shanghai_dst",
          makeDateStringColumn(benchmarkBuilder, 1986, 1991, /*seed=*/0xBEEF02))
      .addExpression(
          "unix_timestamp", "unix_timestamp(c0, 'yyyy-MM-dd HH:mm:ss')")
      .disableTesting()
      .withIterations(1000);

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
