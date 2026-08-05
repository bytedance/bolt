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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <unordered_set>

#include "bolt/exec/fuzzer/FuzzerFlags.h"
#include "bolt/expression/fuzzer/FuzzerRunner.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"

using bytedance::bolt::fuzzer::FuzzerRunner;

int main(int argc, char** argv) {
  bytedance::bolt::functions::prestosql::registerAllScalarFunctions();

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});

  // TODO: List of the functions that at some point crash or fail and need to
  // be fixed before we can enable.
  // This list can include a mix of function names and function signatures.
  // Use function name to exclude all signatures of a given function from
  // testing. Use function signature to exclude only a specific signature.
  std::unordered_set<std::string> skipFunctions = {
      // Fuzzer and the underlying engine are confused about cardinality(HLL)
      // (since HLL is a user defined type), and end up trying to use
      // cardinality passing a VARBINARY (since HLL's implementation uses an
      // alias to VARBINARY).
      "cardinality",
      "element_at",
      "width_bucket",
      // Fuzzer cannot generate valid 'comparator' lambda.
      "array_sort(array(T),constant function(T,T,bigint)) -> array(T)",
      "round",
      "regexp_extract",
      "regexp_extract_all",
      "regexp_like",
      "regexp_replace",
      "regexp_split",
      "bytedance_regexp_extract",
      "bytedance_regexp_extract_all",
      "minus",
      "plus",
      // date_format and format_datetime throw BoltRuntimeError when input
      // timestamp is out of the supported range.
      "date_format",
      "bytedance_date_format",
      "format_datetime",
      // from_unixtime can generate timestamps out of the supported range that
      // make other functions throw BoltRuntimeErrors.
      "from_unixtime",
      "bytedance_from_unixtime",
      "aeolus_from_unixtime",
  };
  size_t initialSeed =
      FLAGS_bolt_fuzzer_seed == 0 ? std::time(nullptr) : FLAGS_bolt_fuzzer_seed;
  return FuzzerRunner::run(initialSeed, skipFunctions, {{}});
}
