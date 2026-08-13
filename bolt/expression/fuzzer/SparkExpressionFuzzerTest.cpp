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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bolt/exec/fuzzer/FuzzerFlags.h"
#include "bolt/expression/fuzzer/FuzzerRunner.h"
#include "bolt/functions/sparksql/registration/Register.h"

using bytedance::bolt::fuzzer::FuzzerRunner;

int main(int argc, char** argv) {
  bytedance::bolt::fuzzer::setSparkExpressionFuzzerFlagDefaults();
  bytedance::bolt::functions::sparksql::registerFunctions("");

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  // The following list are the Spark UDFs that hit issues
  // For rlike you need the following combo in the only list:
  // rlike, md5 and upper
  std::unordered_set<std::string> skipFunctions = {
      "regexp_extract",
      "rlike",
      "chr",
      "replace",
      "might_contain",
      "now",
      // The following UDFs cause crashes or hangs and need to be fixed. They
      // are temporarily skipped to keep the fuzzer running.
      "round",
      "json_tuple_with_codegen",
      "map_filter_keys"};

  // Required by spark_partition_id function.
  std::unordered_map<std::string, std::string> queryConfigs = {
      {bytedance::bolt::core::QueryConfig::kSparkPartitionId, "123"}};

  return FuzzerRunner::run(FLAGS_bolt_fuzzer_seed, skipFunctions, queryConfigs);
}
