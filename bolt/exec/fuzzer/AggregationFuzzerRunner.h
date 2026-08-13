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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#pragma once

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/fuzzer/AggregationFuzzer.h"
#include "bolt/exec/fuzzer/AggregationFuzzerOptions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::exec::test {

/// AggregationFuzzerRunner leverages AggregationFuzzer and VectorFuzzer to
/// automatically generate and execute aggregation tests. It works by:
///
///  1. Taking an initial set of available function signatures.
///  2. Generating a random query plan based on the available function
///     signatures.
///  3. Generating a random set of input data (vector), with a variety of
///     encodings and data layouts.
///  4. Executing a variety of logically equivalent query plans and
///     asserting results are the same.
///  5. Rinse and repeat.
///
/// The common usage pattern is as following:
///
///  $ ./bolt_aggregate_fuzzer_test --bolt_fuzzer_steps 10000
///
/// The important flags that control AggregateFuzzer's behavior are:
///
///  --bolt_fuzzer_steps: how many iterations to run.
///  --bolt_fuzzer_duration_sec: alternatively, for how many seconds it should
///  run (takes
///          precedence over --bolt_fuzzer_steps).
///  --bolt_fuzzer_seed: pass a deterministic seed to reproduce the behavior
///  (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --bolt_fuzzer_only: restrict the functions to fuzz.
///  --bolt_fuzzer_batch_size: size of input vector batches generated.
///
/// e.g:
///
///  $ ./bolt_aggregation_fuzzer_test \
///         --bolt_fuzzer_steps 10000 \
///         --bolt_fuzzer_seed 123 \
///         --v=1 \
///         --bolt_fuzzer_only "min,max"
class AggregationFuzzerRunner {
 public:
  static int run(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const AggregationFuzzerOptions& options) {
    return runFuzzer(
        seed, std::nullopt, std::move(referenceQueryRunner), options);
  }

  static int runRepro(
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
    return runFuzzer(0, planPath, std::move(referenceQueryRunner), {});
  }

 protected:
  static int runFuzzer(
      size_t seed,
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const AggregationFuzzerOptions& options) {
    auto signatures = bytedance::bolt::exec::getAggregateFunctionSignatures();
    if (signatures.empty()) {
      LOG(ERROR) << "No aggregate functions registered.";
      exit(1);
    }

    auto filteredSignatures = bolt::fuzzer::filterSignatures(
        signatures, options.onlyFunctions, options.skipFunctions);
    if (filteredSignatures.empty()) {
      LOG(ERROR)
          << "No aggregate functions left after filtering using 'only' and 'skip' lists.";
      exit(1);
    }

    bytedance::bolt::parse::registerTypeResolver();
    bytedance::bolt::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();
    bytedance::bolt::filesystems::registerLocalFileSystem();

    bytedance::bolt::exec::test::aggregateFuzzer(
        filteredSignatures,
        seed,
        options.customVerificationFunctions,
        options.customInputGenerators,
        options.timestampPrecision,
        options.queryConfigs,
        planPath,
        std::move(referenceQueryRunner));
    // Calling gtest here so that it can be recognized as tests in CI systems.
    return RUN_ALL_TESTS();
  }
};

} // namespace bytedance::bolt::exec::test
