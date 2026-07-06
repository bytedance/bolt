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

#include <optional>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"

namespace bytedance::bolt::functions::flinksql::test {
namespace {

class ElementAtTest : public FlinkFunctionBaseTest {};

} // namespace

/// Flink element_at(array, index) semantics:
///
/// 1. index is 1-based.
/// 2. Constant index < 1 (zero or negative) → throws an exception.
/// 3. Non-constant index < 1 (zero or negative) → returns NULL.
/// 4. Index out of bounds (constant or non-constant) → returns NULL.
/// 5. Valid index → returns the corresponding element.

TEST_F(ElementAtTest, constantValidIndex) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}});
  EXPECT_EQ(
      evaluate<SimpleVector<int64_t>>(
          "element_at(C0, 1)", makeRowVector({arrayVector}))
          ->valueAt(0),
      10);
  EXPECT_EQ(
      evaluate<SimpleVector<int64_t>>(
          "element_at(C0, 3)", makeRowVector({arrayVector}))
          ->valueAt(0),
      30);
}

TEST_F(ElementAtTest, constantZeroIndexThrows) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}});
  BOLT_ASSERT_THROW(
      evaluate<SimpleVector<int64_t>>(
          "element_at(C0, 0)", makeRowVector({arrayVector})),
      "SQL array indices start at 1");
}

TEST_F(ElementAtTest, constantZeroIndexThrowsForAllRows) {
  auto arrayVector = makeArrayVector<int64_t>({
      {10, 20, 30},
      {40, 50, 60},
      {70, 80, 90},
  });
  BOLT_ASSERT_THROW(
      evaluate<SimpleVector<int64_t>>(
          "element_at(C0, 0)", makeRowVector({arrayVector})),
      "SQL array indices start at 1");
}

TEST_F(ElementAtTest, constantNegativeIndexThrows) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}});
  BOLT_ASSERT_THROW(
      evaluate<SimpleVector<int64_t>>(
          "element_at(C0, -1)", makeRowVector({arrayVector})),
      "SQL array indices start at 1");
}

TEST_F(ElementAtTest, constantOutOfBoundsReturnsNull) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}});
  auto result = evaluate<SimpleVector<int64_t>>(
      "element_at(C0, 99)", makeRowVector({arrayVector}));
  EXPECT_TRUE(result->isNullAt(0));
}

TEST_F(ElementAtTest, nonConstantZeroIndexReturnsNull) {
  // Build a flat array column and an index column where index = 0 at runtime.
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}, {40, 50}});
  auto indexVector = makeFlatVector<int64_t>({0, 0});
  auto result = evaluate<SimpleVector<int64_t>>(
      "element_at(C0, C1)", makeRowVector({arrayVector, indexVector}));
  EXPECT_TRUE(result->isNullAt(0));
  EXPECT_TRUE(result->isNullAt(1));
}

TEST_F(ElementAtTest, runtimeConstantZeroIndexReturnsNull) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}, {40, 50}});
  auto zeroIndexBase = makeFlatVector<int64_t>({0});
  auto indexVector = BaseVector::wrapInConstant(2, 0, zeroIndexBase);

  auto result = evaluate<SimpleVector<int64_t>>(
      "element_at(C0, C1)", makeRowVector({arrayVector, indexVector}));

  EXPECT_TRUE(result->isNullAt(0));
  EXPECT_TRUE(result->isNullAt(1));
}

TEST_F(ElementAtTest, nonConstantNegativeIndexReturnsNull) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}, {40, 50}});
  auto indexVector = makeFlatVector<int64_t>({-1, -2});
  auto result = evaluate<SimpleVector<int64_t>>(
      "element_at(C0, C1)", makeRowVector({arrayVector, indexVector}));
  EXPECT_TRUE(result->isNullAt(0));
  EXPECT_TRUE(result->isNullAt(1));
}

TEST_F(ElementAtTest, nonConstantOutOfBoundsReturnsNull) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}});
  auto indexVector = makeFlatVector<int64_t>({99});
  auto result = evaluate<SimpleVector<int64_t>>(
      "element_at(C0, C1)", makeRowVector({arrayVector, indexVector}));
  EXPECT_TRUE(result->isNullAt(0));
}

TEST_F(ElementAtTest, nonConstantMixedRows) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 20, 30}, {40, 50}, {60}});
  auto indexVector = makeFlatVector<int64_t>({2, 0, -1});
  auto result = evaluate<SimpleVector<int64_t>>(
      "element_at(C0, C1)", makeRowVector({arrayVector, indexVector}));
  EXPECT_EQ(result->valueAt(0), 20); // valid: 2nd element
  EXPECT_TRUE(result->isNullAt(1)); // index 0 → null
  EXPECT_TRUE(result->isNullAt(2)); // index -1 → null
}

} // namespace bytedance::bolt::functions::flinksql::test
