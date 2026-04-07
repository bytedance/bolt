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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class LuhnCheckTest : public SparkFunctionBaseTest {
 protected:
  std::optional<bool> luhnCheck(const std::optional<std::string>& str) {
    return evaluateOnce<bool>("luhn_check(c0)", str);
  }
};

TEST_F(LuhnCheckTest, luhnCheck) {
  EXPECT_EQ(luhnCheck("4111111111111111"), true);
  EXPECT_EQ(luhnCheck("5500000000000004"), true);
  EXPECT_EQ(luhnCheck("340000000000009"), true);
  EXPECT_EQ(luhnCheck("6011000000000004"), true);
  EXPECT_EQ(luhnCheck("6011000000000005"), false);
  EXPECT_EQ(luhnCheck("378282246310006"), false);
  EXPECT_EQ(luhnCheck("0"), true);
  EXPECT_EQ(luhnCheck("4111111111111111    "), false);
  EXPECT_EQ(luhnCheck("4111111 111111111"), false);
  EXPECT_EQ(luhnCheck(" 4111111111111111"), false);
  EXPECT_EQ(luhnCheck(""), false);
  EXPECT_EQ(luhnCheck("  "), false);
  EXPECT_EQ(luhnCheck("510B105105105106"), false);
  EXPECT_EQ(luhnCheck("ABCDED"), false);
  EXPECT_EQ(luhnCheck(std::nullopt), std::nullopt);
  EXPECT_EQ(luhnCheck("6011111111111117"), true);
  EXPECT_EQ(luhnCheck("6011111111111118"), false);
  EXPECT_EQ(luhnCheck("123.456"), false);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
