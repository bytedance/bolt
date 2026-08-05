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

#include <gflags/gflags.h>

#include "bolt/functions/CoverageUtil.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"

DEFINE_bool(
    bolt_presto_coverage_all,
    false,
    "Generate coverage map for all Presto functions");
DEFINE_bool(
    bolt_presto_coverage_most_used,
    false,
    "Generate coverage map for a subset of most-used Presto functions");
using namespace bytedance::bolt;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Register all simple and vector scalar functions.
  functions::prestosql::registerAllScalarFunctions();

  // Register Presto aggregate functions.
  aggregate::prestosql::registerAllAggregateFunctions();

  // Register Presto window functions.
  window::prestosql::registerAllWindowFunctions();

  if (FLAGS_bolt_presto_coverage_all) {
    functions::printCoverageMapForAll();
  } else if (FLAGS_bolt_presto_coverage_most_used) {
    functions::printCoverageMapForMostUsed();
  } else {
    const std::unordered_set<std::string> linkBlockList = {
        "checked_divide",
        "checked_minus",
        "checked_modulus",
        "checked_multiply",
        "checked_negate",
        "checked_plus",
        "in",
        "modulus",
        "not"};
    functions::printBoltFunctions(linkBlockList);
  }

  return 0;
}
