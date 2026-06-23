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
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2026-06-16.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"

using namespace bytedance::bolt;

namespace {

class ConfigurableBenchmarkBuilder : public ExpressionBenchmarkBuilder {
 public:
  void setConfig(const std::string& key, const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe({{key, value}});
  }
};

void addArithmeticBenchmarks(
    ExpressionBenchmarkBuilder& builder,
    const std::string& prefix) {
  for (auto vectorSize : {1'024, 4'096, 40'960}) {
    builder
        .addBenchmarkSet(
            fmt::format("{}_arith_batch{}", prefix, vectorSize),
            ROW({"a", "b", "c", "d"}, {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()}))
        .withFuzzerOptions(
            {.vectorSize = static_cast<size_t>(vectorSize), .nullRatio = 0})
        .addExpression("add_ab", "a + b")
        .addExpression("complex_7n", "(a + b) * c + (a - d) * b")
        .addExpression(
            "deep_15n_d8", "((((((a + b) * c - d) + a) * b - c) + d) * a - b)")
        .withIterations(1'000)
        .disableTesting();
  }
}

void addComparisonBenchmarks(
    ExpressionBenchmarkBuilder& builder,
    const std::string& prefix) {
  for (auto vectorSize : {1'024, 4'096, 40'960}) {
    builder
        .addBenchmarkSet(
            fmt::format("{}_cmp_batch{}", prefix, vectorSize),
            ROW({"a", "b"}, {DOUBLE(), DOUBLE()}))
        .withFuzzerOptions(
            {.vectorSize = static_cast<size_t>(vectorSize), .nullRatio = 0})
        .addExpression("eq_ab", "a = b")
        .withIterations(1'000);
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();

  ConfigurableBenchmarkBuilder fastPathOn;
  addArithmeticBenchmarks(fastPathOn, "on");
  addComparisonBenchmarks(fastPathOn, "on");

  ConfigurableBenchmarkBuilder fastPathOff;
  fastPathOff.setConfig(core::QueryConfig::kExprEvalFlatNoNulls, "false");
  addArithmeticBenchmarks(fastPathOff, "off");
  addComparisonBenchmarks(fastPathOff, "off");

  fastPathOn.testBenchmarks();
  fastPathOff.testBenchmarks();
  fastPathOn.registerBenchmarks();
  fastPathOff.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
