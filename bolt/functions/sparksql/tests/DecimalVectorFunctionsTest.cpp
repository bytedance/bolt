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

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
namespace {

class DecimalVectorFunctionsTest : public SparkFunctionBaseTest {
 protected:
  template <TypeKind KIND>
  void testDecimalExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    using EvalType = typename bolt::TypeTraits<KIND>::NativeType;
    auto result =
        evaluate<SimpleVector<EvalType>>(expression, makeRowVector(input));
    bolt::test::assertEqualVectors(expected, result);
    testOpDictVectors<EvalType>(expression, expected, input);
  }

  template <typename T>
  void testOpDictVectors(
      const std::string& operation,
      const VectorPtr& expected,
      const std::vector<VectorPtr>& flatVector) {
    // Dictionary vectors as arguments.
    auto newSize = flatVector[0]->size() * 2;
    std::vector<VectorPtr> dictVectors;
    for (auto i = 0; i < flatVector.size(); ++i) {
      auto indices = makeIndices(newSize, [&](int row) { return row / 2; });
      dictVectors.push_back(
          VectorTestBase::wrapInDictionary(indices, newSize, flatVector[i]));
    }
    auto resultIndices = makeIndices(newSize, [&](int row) { return row / 2; });
    auto expectedResultDictionary =
        VectorTestBase::wrapInDictionary(resultIndices, newSize, expected);
    auto actual =
        evaluate<SimpleVector<T>>(operation, makeRowVector(dictVectors));
    bolt::test::assertEqualVectors(expectedResultDictionary, actual);
  }
};

TEST_F(DecimalVectorFunctionsTest, ceil) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({3, 6, -9, 0}, DECIMAL(2, 0))},
      "ceil(c0)",
      {makeFlatVector<int64_t>({234, 552, -999, 0}, DECIMAL(3, 2))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1, 1, 0, 0}, DECIMAL(1, 0))},
      "ceil(c0)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, -999999999999999999, 0},
          DECIMAL(19, 19))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>(std::vector<int64_t>{101}, DECIMAL(4, 0))},
      "ceil(c0)",
      {makeFlatVector<int128_t>(
          std::vector<int128_t>{
              int128_t(100100100100100100) * 100000000000 + 10010010010},
          DECIMAL(29, 26))});
}

TEST_F(DecimalVectorFunctionsTest, floor) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1, 5, -10, 0}, DECIMAL(2, 0))},
      "floor(c0)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 2))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 0))},
      "floor(c0)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 0))});
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({0, 0, -1, 0}, DECIMAL(1, 0))},
      "floor(c0)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, -999999999999999999, 0},
          DECIMAL(19, 19))});
}

TEST_F(DecimalVectorFunctionsTest, abs) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({123, 552, 999, 0}, DECIMAL(3, 2))},
      "abs(c0)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 2))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, 999999999999999999, 0},
          DECIMAL(19, 19))},
      "abs(c0)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, 999999999999999999, 0},
          DECIMAL(19, 19))});
}

TEST_F(DecimalVectorFunctionsTest, negative) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({-123, -552, 999, 0}, DECIMAL(3, 2))},
      "negative(c0)",
      {makeFlatVector<int64_t>({123, 552, -999, 0}, DECIMAL(3, 2))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {-1234567890123456789, -5000000000000000000, 999999999999999999, 0},
          DECIMAL(19, 19))},
      "negative(c0)",
      {makeFlatVector<int128_t>(
          {1234567890123456789, 5000000000000000000, -999999999999999999, 0},
          DECIMAL(19, 19))});
}

TEST_F(DecimalVectorFunctionsTest, ceilFloor) {
  auto input =
      makeFlatVector<int64_t>({111111, 123456, 123478}, DECIMAL(15, 2));

  {
    auto expected = makeFlatVector<int64_t>({1112, 1235, 1235}, DECIMAL(14, 0));
    auto result =
        evaluate<SimpleVector<int64_t>>("ceil(c0)", makeRowVector({input}));
    bolt::test::assertEqualVectors(result, expected);
  }

  {
    auto expected = makeFlatVector<int64_t>({1111, 1234, 1234}, DECIMAL(14, 0));
    auto result =
        evaluate<SimpleVector<int64_t>>("floor(c0)", makeRowVector({input}));
    bolt::test::assertEqualVectors(result, expected);
  }
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
