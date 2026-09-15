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

#include "bolt/common/process/ThreadDebugInfo.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/Udf.h"

#include <folly/Unit.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

// Only build if address sanitizer is not enabled since this test initiated a
// segfault which should be captured by the signal handler but gets caught by
// the address sanitizer handler instead which print a different error log.

// For clang
#if defined(__has_feature)
// gcc may have the __SANITIZE_ADDRESS__ defined but not the
// __has_feature(address_sanitizer), check for either
#if __has_feature(address_sanitizer) || defined(__SANITIZE_ADDRESS__)
#define IS_BUILDING_WITH_ASAN
#endif
#endif
// For GCC
#if defined(__SANITIZE_ADDRESS__) && __SANITIZE_ADDRESS__
#define IS_BUILDING_WITH_ASAN
#endif

// Ensure the test class name has "DeathTest" as a prefix to ensure this runs in
// a single thread since the ASSERT_DEATH forks the process.
class ThreadDebugInfoDeathTest : public OperatorTestBase {};

namespace {

template <typename T>
struct InduceSegFaultFunction {
  template <typename TResult, typename TInput>
  void call(TResult& out, const TInput& in) {
    int* nullpointer = nullptr;
    *nullpointer = 6;
  }
};
} // namespace

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, withinSeperateDriverThread) {
  auto vector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  registerFunction<InduceSegFaultFunction, int64_t, int64_t>({"segFault"});
  auto op = PlanBuilder().values({vector}).project({"segFault(c0)"}).planNode();
#ifndef IS_BUILDING_WITH_ASAN
  ASSERT_DEATH(
      (assertQuery(op, vector)),
      ".*Fatal signal handler. Query Id= TaskCursorQuery_0 Task Id= test_cursor_1.*");
#endif
}

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, withinQueryCompilation) {
  auto vector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  registerFunction<InduceSegFaultFunction, int64_t, int64_t>({"segFault"});
  // Call expression with a constant to trigger the constant folding during
  // compilation.
  auto op = PlanBuilder().values({vector}).project({"segFault(1)"}).planNode();

#ifndef IS_BUILDING_WITH_ASAN
  ASSERT_DEATH(
      (assertQuery(op, vector)),
      ".*Fatal signal handler. Query Id= TaskCursorQuery_0 Task Id= test_cursor_1.*");
#endif
}

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, withinTheCallingThread) {
  auto vector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  registerFunction<InduceSegFaultFunction, int64_t, int64_t>({"segFault"});
  auto plan =
      PlanBuilder().values({vector}).project({"segFault(c0)"}).planFragment();

  auto queryCtx = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig({}),
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      cache::AsyncDataCache::getInstance(),
      nullptr,
      nullptr,
      "TaskCursorQuery_0");

#ifndef IS_BUILDING_WITH_ASAN
  ASSERT_DEATH(
      (exec::Task::create(
           "single.execution.task.0",
           std::move(plan),
           0,
           queryCtx,
           exec::Task::ExecutionMode::kSerial)
           ->next()),
      ".*Fatal signal handler. Query Id= TaskCursorQuery_0 Task Id= single.execution.task.0.*");
#endif
}

DEBUG_ONLY_TEST_F(ThreadDebugInfoDeathTest, noThreadContextSet) {
  int* nullpointer = nullptr;
#ifndef IS_BUILDING_WITH_ASAN
  ASSERT_DEATH((*nullpointer = 6), ".*ThreadDebugInfo object not found.*");
#endif
  folly::compiler_must_not_elide(nullpointer);
}
