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

#include <optional>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::functions::test;

namespace {

class WidthBucketArrayTest : public FunctionBaseTest {
 protected:
  static constexpr double kInf = std::numeric_limits<double>::infinity();
  static constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
};

TEST_F(WidthBucketArrayTest, success) {
  VectorPtr binsVector;
  auto testWidthBucketArray = [&](const double operand,
                                  const std::vector<int64_t>& expected) {
    auto result = evaluate<SimpleVector<int64_t>>(
        "width_bucket(c0, c1)",
        makeRowVector({
            makeConstant(operand, binsVector->size()),
            binsVector,
        }));

    assertEqualVectors(makeFlatVector<int64_t>(expected), result);

    // Encode the bins array in a dictionary repeating each row of the
    // original array twice.
    auto newSize = binsVector->size() * 2;
    auto binsIndices = makeIndices(newSize, [](auto row) { return row / 2; });

    // Flatten the constant operand to avoid peeling of encodings.
    auto operandFlat = flatten(makeConstant(operand, newSize));
    auto dictResult = evaluate<SimpleVector<int64_t>>(
        "width_bucket(c0, c1)",
        makeRowVector({
            operandFlat,
            wrapInDictionary(binsIndices, newSize, binsVector),
        }));
    auto dictExpected = wrapInDictionary(
        binsIndices, newSize, makeFlatVector<int64_t>(expected));
    assertEqualVectors(dictExpected, dictResult);
  };

  {
    binsVector = makeArrayVector<double>({{0.0, 2.0, 4.0}, {0.0}});
    testWidthBucketArray(3.14, {2, 1});
    testWidthBucketArray(kInf, {3, 1});
    testWidthBucketArray(-1, {0, 0});
  }

  {
    binsVector = makeArrayVector<int64_t>({{0, 2, 4}, {0}});
    testWidthBucketArray(3.14, {2, 1});
    testWidthBucketArray(kInf, {3, 1});
    testWidthBucketArray(-1, {0, 0});
  }
}

TEST_F(WidthBucketArrayTest, failure) {
  auto testFailure = [&](const double operand,
                         const std::vector<std::vector<double>>& bins,
                         const std::string& expected_message) {
    auto binsVector = makeArrayVector<double>(bins);
    BOLT_ASSERT_THROW(
        evaluate<SimpleVector<int64_t>>(
            "width_bucket(c0, c1)",
            makeRowVector(
                {makeConstant(operand, binsVector->size()), binsVector})),
        expected_message);
  };

  testFailure(0, {{}}, "Bins cannot be an empty array");
  testFailure(kNan, {{0}}, "Operand cannot be NaN");
  testFailure(1, {{0, kInf}}, "Bin value must be finite");
  testFailure(1, {{0, kNan}}, "Bin values are not sorted in ascending order");
  testFailure(2, {{1, 0}}, "Bin values are not sorted in ascending order");
}

TEST_F(WidthBucketArrayTest, successForConstantArray) {
  auto testWidthBucketArray = [&](const double operand,
                                  const std::string& bins,
                                  const int64_t expected) {
    auto expression = fmt::format("width_bucket(c0, {})", bins);
    auto oneRowResult = evaluate<SimpleVector<int64_t>>(
        expression, makeRowVector({makeConstant(operand, 1)}));
    assertEqualVectors(
        makeFlatVector<int64_t>(std::vector<int64_t>({expected})),
        oneRowResult);

    auto twoRowsResult = evaluate<SimpleVector<int64_t>>(
        expression, makeRowVector({makeConstant(operand, 2)}));
    assertEqualVectors(
        makeFlatVector<int64_t>(std::vector<int64_t>({expected, expected})),
        twoRowsResult);
  };

  testWidthBucketArray(3.14, "ARRAY[0.0, 2.0, 4.0]", 2);
  testWidthBucketArray(kInf, "ARRAY[0.0, 2.0, 4.0]", 3);
  testWidthBucketArray(-1, "ARRAY[0.0, 2.0, 4.0]", 0);
  testWidthBucketArray(3.14, "ARRAY[0.0]", 1);
  testWidthBucketArray(kInf, "ARRAY[0.0]", 1);
  testWidthBucketArray(-1, "ARRAY[0.0]", 0);
}

TEST_F(WidthBucketArrayTest, failureForConstant) {
  auto testFailure = [&](const double operand,
                         const std::string& bins,
                         const std::string& expected_message) {
    BOLT_ASSERT_THROW(
        evaluate<SimpleVector<int64_t>>(
            fmt::format("width_bucket(c0, {})", bins),
            makeRowVector({makeConstant(operand, 1)})),
        expected_message);
  };

  // TODO: Add tests for empty bin and bins that contains infinity(), nan()
  //       once corresponding casting and non-constant array literal element is
  //       supported.
  testFailure(kNan, "ARRAY[0.0]", "Operand cannot be NaN");
  testFailure(
      2, "ARRAY[1.0, 0.0]", "Bin values are not sorted in ascending order");
}

TEST_F(WidthBucketArrayTest, makeWidthBucketArrayNoThrow) {
  auto test = [&](const std::string& sql) {
    auto typed = makeTypedExpr(sql, ROW({DOUBLE(), ARRAY(BIGINT())}));
    ASSERT_NO_THROW(ExprSet exprSet({typed}, &execCtx_));
  };

  test("width_bucket(0.0, array_constructor(null::BIGINT))");
  test("width_bucket(0.0, array_constructor()::BIGINT[])");
  test("width_bucket(0.0, array_constructor(1,0))");
}
} // namespace
