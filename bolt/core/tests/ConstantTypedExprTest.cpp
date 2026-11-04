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

#include <gtest/gtest.h>
#include <bit>

#include "bolt/core/Expressions.h"
#include "bolt/functions/prestosql/types/HyperLogLogType.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::core::test {

TEST(ConstantTypedExprTest, null) {
  auto makeNull = [](const TypePtr& type) {
    return std::make_shared<ConstantTypedExpr>(
        type, variant::null(type->kind()));
  };

  EXPECT_FALSE(*makeNull(BIGINT()) == *makeNull(DOUBLE()));
  EXPECT_FALSE(*makeNull(ARRAY(BIGINT())) == *makeNull(ARRAY(DOUBLE())));
  EXPECT_FALSE(
      *makeNull(MAP(BIGINT(), INTEGER())) ==
      *makeNull(MAP(BIGINT(), DOUBLE())));

  EXPECT_FALSE(*makeNull(JSON()) == *makeNull(VARCHAR()));
  EXPECT_FALSE(*makeNull(VARCHAR()) == *makeNull(JSON()));

  EXPECT_FALSE(*makeNull(ARRAY(JSON())) == *makeNull(ARRAY(VARCHAR())));
  EXPECT_FALSE(*makeNull(ARRAY(VARCHAR())) == *makeNull(ARRAY(JSON())));
  EXPECT_FALSE(
      *makeNull(MAP(BIGINT(), JSON())) == *makeNull(MAP(BIGINT(), VARCHAR())));
  EXPECT_FALSE(
      *makeNull(MAP(BIGINT(), VARCHAR())) == *makeNull(MAP(BIGINT(), JSON())));

  EXPECT_FALSE(*makeNull(HYPERLOGLOG()) == *makeNull(VARBINARY()));
  EXPECT_FALSE(*makeNull(VARBINARY()) == *makeNull(HYPERLOGLOG()));

  EXPECT_FALSE(
      *makeNull(TIMESTAMP_WITH_TIME_ZONE()) ==
      *makeNull(ROW({BIGINT(), SMALLINT()})));
  EXPECT_FALSE(
      *makeNull(ROW({BIGINT(), SMALLINT()})) ==
      *makeNull(TIMESTAMP_WITH_TIME_ZONE()));

  EXPECT_TRUE(*makeNull(DOUBLE()) == *makeNull(DOUBLE()));
  EXPECT_TRUE(*makeNull(ARRAY(DOUBLE())) == *makeNull(ARRAY(DOUBLE())));

  EXPECT_TRUE(*makeNull(JSON()) == *makeNull(JSON()));
  EXPECT_TRUE(
      *makeNull(MAP(VARCHAR(), JSON())) == *makeNull(MAP(VARCHAR(), JSON())));

  EXPECT_FALSE(*makeNull(JSON()) == *makeNull(VARCHAR()));
  EXPECT_FALSE(
      *makeNull(ROW({"a", "b"}, {INTEGER(), REAL()})) ==
      *makeNull(ROW({"x", "y"}, {INTEGER(), REAL()})));
}

TEST(ConstantTypedExprTest, floatingPointBits) {
  const auto check = []<typename T>(T nan, T otherNan) {
    const auto type = CppToType<T>::create();
    const auto equal = [&](T left, T right) {
      return ConstantTypedExpr(type, variant(left)) ==
          ConstantTypedExpr(type, variant(right));
    };
    EXPECT_FALSE(equal(T(0.0), T(-0.0)));
    EXPECT_TRUE(equal(T(-0.0), T(-0.0)));
    EXPECT_TRUE(equal(nan, nan));
    EXPECT_FALSE(equal(nan, -nan));
    EXPECT_FALSE(equal(nan, otherNan));
  };
  check(
      std::bit_cast<float>(uint32_t{0x7fc00001}),
      std::bit_cast<float>(uint32_t{0x7fc00002}));
  check(
      std::bit_cast<double>(uint64_t{0x7ff8000000000001}),
      std::bit_cast<double>(uint64_t{0x7ff8000000000002}));
}

class ConstantVectorExprTest : public testing::Test,
                               public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(ConstantVectorExprTest, nestedValuesAndEncodings) {
  const auto check = [&]<typename T>(T nan) {
    auto values = makeNullableFlatVector<T>(
        {T(0.0), T(-0.0), nan, nan, -nan, std::nullopt});
    std::vector<VectorPtr> vectors{
        values,
        makeArrayVector({0, 1, 2, 3, 4, 5}, values),
        makeRowVector({values}),
        makeMapVector(
            {0, 1, 2, 3, 4, 5},
            values,
            makeFlatVector<int32_t>({1, 1, 1, 1, 1, 1}))};
    for (const auto& vector : vectors) {
      auto constant = [&](vector_size_t row) {
        return ConstantTypedExpr(BaseVector::wrapInConstant(1, row, vector));
      };
      EXPECT_FALSE(constant(0) == constant(1));
      EXPECT_TRUE(constant(1) == constant(1));
      EXPECT_TRUE(constant(2) == constant(3));
      EXPECT_FALSE(constant(2) == constant(4));
      EXPECT_TRUE(constant(5) == constant(5));
      auto dictionary = wrapInDictionary(makeIndices({3, 1, 5}), 3, vector);
      EXPECT_TRUE(constant(2) == ConstantTypedExpr(dictionary));
    }
  };
  check(std::bit_cast<float>(uint32_t{0x7fc00001}));
  check(std::bit_cast<double>(uint64_t{0x7ff8000000000001}));
}

TEST_F(ConstantVectorExprTest, projectedRowFields) {
  const auto type = ROW({DOUBLE(), BIGINT()});
  const auto row = [&](std::vector<VectorPtr> children) {
    return ConstantTypedExpr(std::make_shared<RowVector>(
        pool(), type, nullptr, 1, std::move(children)));
  };
  auto left = row({nullptr, makeFlatVector<int64_t>({1})});
  EXPECT_TRUE(left == row({nullptr, makeFlatVector<int64_t>({1})}));
  EXPECT_FALSE(
      left == row({makeFlatVector<double>({0}), makeFlatVector<int64_t>({1})}));
  EXPECT_FALSE(left == row({nullptr}));
  EXPECT_FALSE(row({nullptr}) == left);
}

TEST_F(ConstantVectorExprTest, mapEntryOrder) {
  auto maps = makeMapVector<int32_t, int32_t>(
      {{{1, 10}, {2, 20}},
       {{2, 20}, {1, 10}},
       {{1, 10}, {2, 20}},
       {{1, 99}, {2, 20}}});
  auto constant = [&](vector_size_t row) {
    return ConstantTypedExpr(BaseVector::wrapInConstant(1, row, maps));
  };
  EXPECT_FALSE(constant(0) == constant(1));
  EXPECT_TRUE(constant(0) == constant(2));
  EXPECT_FALSE(constant(0) == constant(3));
  EXPECT_EQ(constant(0).hash(), constant(2).hash());
}

} // namespace bytedance::bolt::core::test
