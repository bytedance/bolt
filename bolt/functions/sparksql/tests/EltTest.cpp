/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
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

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class EltTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> elt(
      std::optional<int32_t> index,
      std::optional<std::string> input0,
      std::optional<std::string> input1) {
    return evaluateOnce<std::string>("elt(c0, c1, c2)", index, input0, input1);
  }

  std::optional<std::string> elt3(
      std::optional<int32_t> index,
      std::optional<std::string> input0,
      std::optional<std::string> input1,
      std::optional<std::string> input2) {
    return evaluateOnce<std::string>(
        "elt(c0, c1, c2, c3)", index, input0, input1, input2);
  }
};

TEST_F(EltTest, inRange) {
  EXPECT_EQ("a", elt3(1, "a", "b", "c"));
  EXPECT_EQ("b", elt3(2, "a", "b", "c"));
  EXPECT_EQ("c", elt3(3, "a", "b", "c"));
}

TEST_F(EltTest, singleInput) {
  EXPECT_EQ(
      "only",
      evaluateOnce<std::string>(
          "elt(c0, c1)",
          std::optional<int32_t>(1),
          std::optional<std::string>("only")));
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<std::string>(
          "elt(c0, c1)",
          std::optional<int32_t>(2),
          std::optional<std::string>("only")));
}

// Spark returns NULL for out-of-range indices instead of throwing. This is
// where elt() differs from element_at(), which throws on index 0 and treats
// negative indices as offsets from the end of the array.
TEST_F(EltTest, indexOutOfRange) {
  EXPECT_EQ(std::nullopt, elt3(4, "a", "b", "c"));
  EXPECT_EQ(std::nullopt, elt(0, "a", "b"));
  EXPECT_EQ(std::nullopt, elt(-1, "a", "b"));
  EXPECT_EQ(std::nullopt, elt(-2, "a", "b"));
  EXPECT_EQ(std::nullopt, elt(std::numeric_limits<int32_t>::min(), "a", "b"));
  EXPECT_EQ(std::nullopt, elt(std::numeric_limits<int32_t>::max(), "a", "b"));
}

TEST_F(EltTest, nullIndex) {
  EXPECT_EQ(std::nullopt, elt(std::nullopt, "a", "b"));
}

TEST_F(EltTest, nullInput) {
  // The selected input is NULL.
  EXPECT_EQ(std::nullopt, elt(2, "a", std::nullopt));
  // A NULL input that is not selected must not affect the result.
  EXPECT_EQ("a", elt(1, "a", std::nullopt));
  EXPECT_EQ("b", elt(2, std::nullopt, "b"));
}

TEST_F(EltTest, emptyString) {
  EXPECT_EQ("", elt(1, "", "b"));
  EXPECT_EQ("", elt(2, "a", ""));
}

TEST_F(EltTest, longStrings) {
  // Strings longer than 12 bytes are not inlined in StringView, so they
  // exercise the shared string buffer path.
  const std::string long0(100, 'x');
  const std::string long1(200, 'y');
  EXPECT_EQ(long0, elt(1, long0, long1));
  EXPECT_EQ(long1, elt(2, long0, long1));
}

TEST_F(EltTest, unicode) {
  EXPECT_EQ("你好", elt(1, "你好", "世界"));
  EXPECT_EQ("世界", elt(2, "你好", "世界"));
}

// Multiple rows with a different index per row, verifying that rows are
// dispatched to the correct input.
TEST_F(EltTest, vectorizedFlatIndex) {
  auto index =
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, 0, std::nullopt, 2, 1});
  auto input0 = makeFlatVector<std::string>(
      {"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7"});
  auto input1 = makeFlatVector<std::string>(
      {"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7"});
  auto input2 = makeFlatVector<std::string>(
      {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"});

  auto result = evaluate(
      "elt(c0, c1, c2, c3)", makeRowVector({index, input0, input1, input2}));

  auto expected = makeNullableFlatVector<std::string>(
      {"a0",
       "b1",
       "c2",
       std::nullopt, // index 4 is out of range
       std::nullopt, // index 0 is out of range
       std::nullopt, // NULL index
       "b6",
       "a7"});
  assertEqualVectors(expected, result);
}

// Nulls in the non-selected inputs must not leak into the result.
TEST_F(EltTest, vectorizedWithNullInputs) {
  auto index = makeFlatVector<int32_t>({1, 2, 1, 2});
  auto input0 = makeNullableFlatVector<std::string>(
      {"a0", std::nullopt, std::nullopt, "a3"});
  auto input1 = makeNullableFlatVector<std::string>(
      {std::nullopt, "b1", "b2", std::nullopt});

  auto result =
      evaluate("elt(c0, c1, c2)", makeRowVector({index, input0, input1}));

  auto expected = makeNullableFlatVector<std::string>(
      {"a0", "b1", std::nullopt, std::nullopt});
  assertEqualVectors(expected, result);
}

// A constant index exercises the decoded (non-identity) mapping path.
TEST_F(EltTest, constantIndex) {
  auto index = makeConstant<int32_t>(2, 4);
  auto input0 = makeFlatVector<std::string>({"a0", "a1", "a2", "a3"});
  auto input1 = makeFlatVector<std::string>({"b0", "b1", "b2", "b3"});

  auto result =
      evaluate("elt(c0, c1, c2)", makeRowVector({index, input0, input1}));

  assertEqualVectors(
      makeFlatVector<std::string>({"b0", "b1", "b2", "b3"}), result);
}

// A constant input must be copied correctly for the rows that select it.
TEST_F(EltTest, constantInput) {
  auto index = makeFlatVector<int32_t>({1, 2, 1, 2});
  auto input0 = makeFlatVector<std::string>({"a0", "a1", "a2", "a3"});
  auto input1 = makeConstant<StringView>("const"_sv, 4);

  auto result =
      evaluate("elt(c0, c1, c2)", makeRowVector({index, input0, input1}));

  assertEqualVectors(
      makeFlatVector<std::string>({"a0", "const", "a2", "const"}), result);
}

// A dictionary-encoded input must be resolved through its indices.
TEST_F(EltTest, dictionaryInput) {
  auto index = makeFlatVector<int32_t>({1, 2, 2, 1});
  auto input0 = makeFlatVector<std::string>({"a0", "a1", "a2", "a3"});
  auto base = makeFlatVector<std::string>({"d0", "d1"});
  // Row i of the dictionary reads base[1 - i % 2], i.e. "d1", "d0", "d1", "d0".
  auto input1 = wrapInDictionary(makeIndices({1, 0, 1, 0}), 4, base);

  auto result =
      evaluate("elt(c0, c1, c2)", makeRowVector({index, input0, input1}));

  // Row 0 -> input0[0], row 1 -> dict[1] = "d0", row 2 -> dict[2] = "d1",
  // row 3 -> input0[3].
  assertEqualVectors(
      makeFlatVector<std::string>({"a0", "d0", "d1", "a3"}), result);
}

TEST_F(EltTest, varbinary) {
  auto index = makeFlatVector<int32_t>({1, 2});
  auto input0 = makeFlatVector<std::string>({"a0", "a1"}, VARBINARY());
  auto input1 = makeFlatVector<std::string>({"b0", "b1"}, VARBINARY());

  auto result =
      evaluate("elt(c0, c1, c2)", makeRowVector({index, input0, input1}));

  assertEqualVectors(
      makeFlatVector<std::string>({"a0", "b1"}, VARBINARY()), result);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
