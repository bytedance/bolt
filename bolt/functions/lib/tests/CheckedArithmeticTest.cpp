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

#include "bolt/common/base/SparkCompatibility.h"

#include <cstdint>
#include "bolt/functions/lib/CheckedArithmetic.h"
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;

class CheckedArithmeticTest : public functions::test::FunctionBaseTest {};

TEST_F(CheckedArithmeticTest, DefaultModulusCompatibility) {
  functions::CheckedModulusFunction<int32_t> modulus;
  int32_t result;
  if (::bytedance::bolt::kSparkCompatible) {
    EXPECT_FALSE(modulus.call(result, 1, 0));
  } else {
    EXPECT_THROW(modulus.call(result, 1, 0), BoltUserError);
  }
}

TEST_F(CheckedArithmeticTest, Mod) {
  auto firstVector = makeFlatVector<int32_t>({10, 20, 30});
  auto secondVector = makeFlatVector<int32_t>({0, 1, 2});
  auto expected = makeNullableFlatVector<int32_t>({std::nullopt, 0, 0});
  if (!::bytedance::bolt::kSparkCompatible) {
    // When any number mod 0, presto's logic throws an exception
    EXPECT_THROW(
        evaluate<SimpleVector<int32_t>>(
            "mod(c0, c1)", makeRowVector({firstVector, secondVector})),
        BoltUserError);
  } else {
    auto result = evaluate<SimpleVector<int32_t>>(
        "mod(c0, c1)", makeRowVector({firstVector, secondVector}));
    assertEqualVectors(expected, result);
  }
}
