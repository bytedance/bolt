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

#include <cstdint>
#include <limits>
#include <optional>

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class BitmapTest : public SparkFunctionBaseTest {
 protected:
  std::optional<int64_t> bitPosition(std::optional<int64_t> value) {
    return evaluateOnce<int64_t>("bitmap_bit_position(c0)", value);
  }

  std::optional<int64_t> bitPositionFromInt(std::optional<int32_t> value) {
    return evaluateOnce<int64_t>(
        "bitmap_bit_position(cast(c0 as bigint))", value);
  }
};

TEST_F(BitmapTest, bitPositionPositive) {
  EXPECT_EQ(bitPosition(1), 0);
  EXPECT_EQ(bitPosition(2), 1);
  EXPECT_EQ(bitPosition(32768), 32767);
  EXPECT_EQ(bitPosition(32769), 0);
  EXPECT_EQ(bitPosition(65536), 32767);
  EXPECT_EQ(bitPosition(65537), 0);
  EXPECT_EQ(bitPosition(std::numeric_limits<int64_t>::max()), 32766);
}

TEST_F(BitmapTest, bitPositionNonPositive) {
  EXPECT_EQ(bitPosition(0), 0);
  EXPECT_EQ(bitPosition(-1), 1);
  EXPECT_EQ(bitPosition(-32767), 32767);
  EXPECT_EQ(bitPosition(-32768), 0);
  EXPECT_EQ(bitPosition(-32769), 1);
  EXPECT_EQ(bitPosition(std::numeric_limits<int64_t>::min()), 0);
  EXPECT_EQ(bitPosition(std::numeric_limits<int64_t>::min() + 1), 32767);
}

TEST_F(BitmapTest, bitPositionNullAndCast) {
  EXPECT_EQ(bitPosition(std::nullopt), std::nullopt);
  EXPECT_EQ(bitPositionFromInt(32769), 0);
  EXPECT_EQ(bitPositionFromInt(std::nullopt), std::nullopt);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
