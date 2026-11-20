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

#include <string>

#include <bolt/core/QueryConfig.h>
#include <gtest/gtest.h>
#include <optional>
#include "bolt/common/base/SparkCompatibility.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "bolt/type/Timestamp.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::functions::test;

namespace bytedance::bolt::functions::sparksql::test {
class SizeTest : public SparkFunctionBaseTest {
 protected:
  std::function<vector_size_t(vector_size_t /* row */)> sizeAt =
      [](vector_size_t row) { return 1 + row % 7; };

  void testLegacySizeOfNull(VectorPtr vector, vector_size_t numRows) {
    auto result = evaluate<SimpleVector<int32_t>>(
        "size(c0, true)", makeRowVector({vector}));
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (vector->isNullAt(i)) {
        EXPECT_EQ(result->valueAt(i), -1) << "at " << i;
      } else {
        EXPECT_EQ(result->valueAt(i), sizeAt(i)) << "at " << i;
      }
    }
  }

  void testSize(VectorPtr vector, vector_size_t numRows) {
    auto result = evaluate<SimpleVector<int32_t>>(
        "size(c0, false)", makeRowVector({vector}));
    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_EQ(result->isNullAt(i), vector->isNullAt(i)) << "at " << i;
    }
  }

  void testSizeWithSparkQueryConfig(
      VectorPtr vector,
      vector_size_t numRows,
      bool legacySizeOfNull,
      bool ansiEnabled,
      bool expectedLegacySizeOfNull) {
    queryCtx_->testingOverrideConfigUnsafe({
        {"spark.sql.legacy.sizeOfNull", std::to_string(legacySizeOfNull)},
        {"spark.sql.ansi.enabled", std::to_string(ansiEnabled)},
    });
    auto result =
        evaluate<SimpleVector<int32_t>>("size(c0)", makeRowVector({vector}));
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (!vector->isNullAt(i)) {
        EXPECT_EQ(result->valueAt(i), sizeAt(i)) << "at " << i;
      } else if (expectedLegacySizeOfNull) {
        EXPECT_FALSE(result->isNullAt(i)) << "at " << i;
        EXPECT_EQ(result->valueAt(i), -1) << "at " << i;
      } else {
        EXPECT_TRUE(result->isNullAt(i)) << "at " << i;
      }
    }
  }

  template <typename T>
  int32_t testArraySize(const std::vector<std::optional<T>>& input) {
    auto row = makeRowVector({makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{input})});
    return evaluateOnce<int32_t>("size(c0, false)", row).value();
  }

  static inline vector_size_t valueAt(vector_size_t idx) {
    return idx + 1;
  }
};

// Ensure that out is set to -1 for null input if legacySizeOfNull = true.
TEST_F(SizeTest, legacySizeOfNull) {
  vector_size_t numRows = 100;
  auto arrayVector =
      makeArrayVector<int64_t>(numRows, sizeAt, valueAt, nullptr);
  testLegacySizeOfNull(arrayVector, numRows);
  arrayVector =
      makeArrayVector<int64_t>(numRows, sizeAt, valueAt, nullEvery(5));
  testLegacySizeOfNull(arrayVector, numRows);
  auto mapVector = makeMapVector<int64_t, int64_t>(
      numRows, sizeAt, valueAt, valueAt, nullptr);
  testLegacySizeOfNull(mapVector, numRows);
  mapVector = makeMapVector<int64_t, int64_t>(
      numRows, sizeAt, valueAt, valueAt, nullEvery(5));
  testLegacySizeOfNull(mapVector, numRows);
}

// Ensure that out is set to null for null input if legacySizeOfNull = false.
TEST_F(SizeTest, size) {
  vector_size_t numRows = 100;
  auto arrayVector =
      makeArrayVector<int64_t>(numRows, sizeAt, valueAt, nullEvery(1));
  testSize(arrayVector, numRows);
  auto mapVector = makeMapVector<int64_t, int64_t>(
      numRows, sizeAt, valueAt, valueAt, nullEvery(1));
  testSize(mapVector, numRows);
}

TEST_F(SizeTest, oneArgumentUsesSparkLegacySizeOfNullConfig) {
  constexpr vector_size_t kNumRows = 100;
  auto arrayVector =
      makeArrayVector<int64_t>(kNumRows, sizeAt, valueAt, nullEvery(5));
  auto mapVector = makeMapVector<int64_t, int64_t>(
      kNumRows, sizeAt, valueAt, valueAt, nullEvery(5));

  for (const bool legacySizeOfNull : {true, false}) {
    testSizeWithSparkQueryConfig(
        arrayVector,
        kNumRows,
        legacySizeOfNull,
        /*ansiEnabled=*/false,
        /*expectedLegacySizeOfNull=*/legacySizeOfNull);
    testSizeWithSparkQueryConfig(
        mapVector,
        kNumRows,
        legacySizeOfNull,
        /*ansiEnabled=*/false,
        /*expectedLegacySizeOfNull=*/legacySizeOfNull);
  }
}

TEST_F(SizeTest, ansiAffectsLegacySizeOfNullOnlyInSparkCompatibleBuild) {
  constexpr vector_size_t kNumRows = 100;
  auto arrayVector =
      makeArrayVector<int64_t>(kNumRows, sizeAt, valueAt, nullEvery(5));
  auto mapVector = makeMapVector<int64_t, int64_t>(
      kNumRows, sizeAt, valueAt, valueAt, nullEvery(5));

  testSizeWithSparkQueryConfig(
      arrayVector,
      kNumRows,
      /*legacySizeOfNull=*/true,
      /*ansiEnabled=*/true,
      /*expectedLegacySizeOfNull=*/!::bytedance::bolt::kSparkCompatible);
  testSizeWithSparkQueryConfig(
      mapVector,
      kNumRows,
      /*legacySizeOfNull=*/true,
      /*ansiEnabled=*/true,
      /*expectedLegacySizeOfNull=*/!::bytedance::bolt::kSparkCompatible);
}

TEST_F(SizeTest, boolean) {
  EXPECT_EQ(testArraySize<bool>({true, false}), 2);
  EXPECT_EQ(testArraySize<bool>({true}), 1);
  EXPECT_EQ(testArraySize<bool>({}), 0);
  EXPECT_EQ(testArraySize<bool>({true, false, true, std::nullopt}), 4);
}

TEST_F(SizeTest, smallint) {
  EXPECT_EQ(testArraySize<int8_t>({}), 0);
  EXPECT_EQ(testArraySize<int8_t>({1}), 1);
  EXPECT_EQ(testArraySize<int8_t>({std::nullopt}), 1);
  EXPECT_EQ(testArraySize<int8_t>({std::nullopt, 1}), 2);
}

TEST_F(SizeTest, real) {
  EXPECT_EQ(testArraySize<float>({}), 0);
  EXPECT_EQ(testArraySize<float>({1.1}), 1);
  EXPECT_EQ(testArraySize<float>({std::nullopt}), 1);
  EXPECT_EQ(testArraySize<float>({std::nullopt, 1.1}), 2);
}

TEST_F(SizeTest, varchar) {
  EXPECT_EQ(testArraySize<std::string>({"red", "blue"}), 2);
  EXPECT_EQ(
      testArraySize<std::string>({std::nullopt, "blue", "yellow", "orange"}),
      4);
  EXPECT_EQ(testArraySize<std::string>({}), 0);
  EXPECT_EQ(testArraySize<std::string>({std::nullopt}), 1);
}

TEST_F(SizeTest, integer) {
  EXPECT_EQ(testArraySize<int32_t>({1, 2}), 2);
}

TEST_F(SizeTest, timestamp) {
  auto ts = [](int64_t micros) { return Timestamp::fromMicros(micros); };
  EXPECT_EQ(testArraySize<Timestamp>({}), 0);
  EXPECT_EQ(testArraySize<Timestamp>({std::nullopt}), 1);
  EXPECT_EQ(testArraySize<Timestamp>({ts(0), ts(1)}), 2);
}

} // namespace bytedance::bolt::functions::sparksql::test
