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
#include "bolt/exec/fuzzer/TransformResultVerifier.h"
#include "bolt/functions/sparksql/aggregates/Register.h"

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

int main(int argc, char** argv) {
  bytedance::bolt::functions::aggregate::sparksql::registerAggregateFunctions(
      "", false);

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});

  // TODO: List of the functions that at some point crash or fail and need to
  // be fixed before we can enable. Constant argument of bloom_filter_agg cause
  // fuzzer test fail.
  std::unordered_set<std::string> skipFunctions = {
      "bloom_filter_agg",
      // The following functions cause crashes and need to be fixed. They are
      // temporarily skipped to keep the fuzzer running.
      "percentile_merge_extract_array_double",
      "percentile_merge_extract_double",
      "percentile_merge_extract",
      "percentile_merge",
      "percentile_approx",
      "percentile_partial",
      "percentile",
      "aggregate_map_sum",
      "first_ignore_null",
      "last",
      "last_ignore_null",
      "first"};

  using bytedance::bolt::exec::test::TransformResultVerifier;

  auto makeArrayVerifier = []() {
    return TransformResultVerifier::create("\"$internal$canonicalize\"({})");
  };

  // The results of the following functions depend on the order of input
  // rows. For some functions, the result can be transformed to a value that
  // doesn't depend on the order of inputs. If such transformation exists, it
  // can be specified to be used for results verification. If no transformation
  // is specified, results are not verified.
  std::unordered_map<
      std::string,
      std::shared_ptr<bytedance::bolt::exec::test::ResultVerifier>>
      customVerificationFunctions = {
          {"last", nullptr},
          {"last_ignore_null", nullptr},
          {"first", nullptr},
          {"first_ignore_null", nullptr},
          {"max_by", nullptr},
          {"min_by", nullptr},
          {"skewness", nullptr},
          {"kurtosis", nullptr},
          {"collect_list", makeArrayVerifier()},
          {"collect_set", makeArrayVerifier()},
          // Nested nulls are handled as values in Spark. But nested nulls
          // comparison always generates null in DuckDB.
          {"min", nullptr},
          {"max", nullptr},
      };

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  auto duckQueryRunner =
      std::make_unique<bytedance::bolt::exec::test::DuckQueryRunner>();
  duckQueryRunner->disableAggregateFunctions({
      "max_by",
      "min_by",
      // The skewness functions of Bolt and DuckDB use different
      // algorithms.
      "skewness",
      // Spark's kurtosis uses Pearson's formula for calculating the kurtosis
      // coefficient. Meanwhile, DuckDB employs the sample kurtosis calculation
      // formula. The results from the two methods are completely different.
      "kurtosis",
      // When all data in a group are null, Spark returns an empty array while
      // DuckDB returns null.
      "collect_list",
  });

  using Runner = bytedance::bolt::exec::test::AggregationFuzzerRunner;
  using Options = bytedance::bolt::exec::test::AggregationFuzzerOptions;

  Options options;
  options.onlyFunctions = FLAGS_only;
  options.skipFunctions = skipFunctions;
  options.customVerificationFunctions = customVerificationFunctions;
  return Runner::run(initialSeed, std::move(duckQueryRunner), options);
}
