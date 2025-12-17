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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <unordered_set>

#include "bolt/exec/fuzzer/AggregationFuzzerOptions.h"
#include "bolt/exec/fuzzer/AggregationFuzzerRunner.h"
#include "bolt/exec/fuzzer/DuckQueryRunner.h"
#include "bolt/exec/fuzzer/PrestoQueryRunner.h"
#include "bolt/exec/fuzzer/TransformResultVerifier.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/fuzzer/ApproxDistinctInputGenerator.h"
#include "bolt/functions/prestosql/fuzzer/ApproxDistinctResultVerifier.h"
#include "bolt/functions/prestosql/fuzzer/ApproxPercentileInputGenerator.h"
#include "bolt/functions/prestosql/fuzzer/ApproxPercentileResultVerifier.h"
#include "bolt/functions/prestosql/fuzzer/ArbitraryResultVerifier.h"
#include "bolt/functions/prestosql/fuzzer/MinMaxInputGenerator.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_string(
    only,
    "",
    "If specified, Fuzzer will only choose functions from "
    "this comma separated list of function names "
    "(e.g: --only \"min\" or --only \"sum,avg\").");

DEFINE_string(
    presto_url,
    "",
    "Presto coordinator URI along with port. If set, we use Presto "
    "source of truth. Otherwise, use DuckDB. Example: "
    "--presto_url=http://127.0.0.1:8080");
namespace bytedance::bolt::exec::test {
namespace {

std::unordered_map<std::string, std::shared_ptr<InputGenerator>>
getCustomInputGenerators() {
  return {
      {"min", std::make_shared<MinMaxInputGenerator>("min")},
      {"min_by", std::make_shared<MinMaxInputGenerator>("min_by")},
      {"max", std::make_shared<MinMaxInputGenerator>("max")},
      {"max_by", std::make_shared<MinMaxInputGenerator>("max_by")},
      {"approx_distinct", std::make_shared<ApproxDistinctInputGenerator>()},
      {"approx_set", std::make_shared<ApproxDistinctInputGenerator>()},
      {"approx_percentile", std::make_shared<ApproxPercentileInputGenerator>()},
  };
}

} // namespace
} // namespace bytedance::bolt::exec::test

int main(int argc, char** argv) {
  bytedance::bolt::aggregate::prestosql::registerAllAggregateFunctions(
      "", false);
  bytedance::bolt::functions::prestosql::registerAllScalarFunctions();
  bytedance::bolt::window::prestosql::registerAllWindowFunctions();
  bytedance::bolt::functions::prestosql::registerInternalFunctions();
  bytedance::bolt::memory::MemoryManager::initialize({});

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  // List of functions that have known bugs that cause crashes or failures.
  static const std::unordered_set<std::string> skipFunctions = {
      "stddev_pop",
      // Lambda functions are not supported yet.
      "reduce_agg",
      "max_data_size_for_stats",
      "map_union_sum",
      "approx_set",
      "min_by",
      "max_by",
      "any_value"};

  using bytedance::bolt::exec::test::ApproxDistinctResultVerifier;
  using bytedance::bolt::exec::test::ApproxPercentileResultVerifier;
  using bytedance::bolt::exec::test::ArbitraryResultVerifier;
  using bytedance::bolt::exec::test::setupReferenceQueryRunner;
  using bytedance::bolt::exec::test::TransformResultVerifier;

  auto makeArrayVerifier = []() {
    return TransformResultVerifier::create("\"$internal$canonicalize\"({})");
  };

  auto makeMapVerifier = []() {
    return TransformResultVerifier::create(
        "\"$internal$canonicalize\"(map_keys({}))");
  };

  // Functions whose results verification should be skipped. These can be
  // order-dependent functions whose results depend on the order of input rows,
  // or functions that return complex-typed results containing floating-point
  // fields. For some functions, the result can be transformed to a value that
  // can be verified. If such transformation exists, it can be specified to be
  // used for results verification. If no transformation is specified, results
  // are not verified.
  static const std::unordered_map<
      std::string,
      std::shared_ptr<bytedance::bolt::exec::test::ResultVerifier>>
      customVerificationFunctions = {
          // Order-dependent functions.
          {"approx_distinct", std::make_shared<ApproxDistinctResultVerifier>()},
          {"approx_set", nullptr},
          {"approx_percentile",
           std::make_shared<ApproxPercentileResultVerifier>()},
          {"arbitrary", std::make_shared<ArbitraryResultVerifier>()},
          {"any_value", nullptr},
          {"array_agg", makeArrayVerifier()},
          {"set_agg", makeArrayVerifier()},
          {"set_union", makeArrayVerifier()},
          {"map_agg", makeMapVerifier()},
          {"map_union", makeMapVerifier()},
          {"map_union_sum", makeMapVerifier()},
          {"max_by", nullptr},
          {"max", nullptr},
          {"min_by", nullptr},
          {"multimap_agg",
           TransformResultVerifier::create(
               "transform_values({}, (k, v) -> \"$internal$canonicalize\"(v))")},
          // Semantically inconsistent functions
          {"skewness", nullptr},
          {"kurtosis", nullptr},
          {"entropy", nullptr},
          {"max_data_size_for_stats", nullptr},
          {"sum_data_size_for_stats", nullptr},
      };

  using Runner = bytedance::bolt::exec::test::AggregationFuzzerRunner;
  using Options = bytedance::bolt::exec::test::AggregationFuzzerOptions;

  Options options;
  options.onlyFunctions = FLAGS_only;
  options.skipFunctions = skipFunctions;
  options.customVerificationFunctions = customVerificationFunctions;
  options.customInputGenerators =
      bytedance::bolt::exec::test::getCustomInputGenerators();
  options.timestampPrecision =
      bytedance::bolt::VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  return Runner::run(
      initialSeed,
      setupReferenceQueryRunner(FLAGS_presto_url, "aggregation_fuzzer"),
      options);
}
