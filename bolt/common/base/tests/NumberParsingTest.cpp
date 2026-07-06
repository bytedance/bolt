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

#include "bolt/common/base/NumberParsing.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::test {
namespace {

TEST(NumberParsingTest, extractNumberAfterPrefix) {
  EXPECT_EQ(extractNumberAfterPrefix("query_ATTEMPT_4", "ATTEMPT_"), 4);
  EXPECT_EQ(extractNumberAfterPrefix("ATTEMPT_9_suffix", "ATTEMPT_"), 9);
  EXPECT_EQ(extractNumberAfterPrefix("ATTEMPT_123", "ATTEMPT_"), 123);

  EXPECT_EQ(extractNumberAfterPrefix("query", "ATTEMPT_"), std::nullopt);
  EXPECT_EQ(extractNumberAfterPrefix("ATTEMPT_", "ATTEMPT_"), std::nullopt);
  EXPECT_EQ(extractNumberAfterPrefix("ATTEMPT_abc", "ATTEMPT_"), std::nullopt);
}

TEST(NumberParsingTest, extractNumberBetween) {
  EXPECT_EQ(
      extractNumberBetween("query_0_TID_123_ATTEMPT_4", "_TID_", "_ATTEMPT_"),
      123);

  EXPECT_EQ(
      extractNumberBetween("query_0_ATTEMPT_4", "_TID_", "_ATTEMPT_"),
      std::nullopt);
  EXPECT_EQ(
      extractNumberBetween("query_0_TID__ATTEMPT_4", "_TID_", "_ATTEMPT_"),
      std::nullopt);
  EXPECT_EQ(
      extractNumberBetween("query_0_TID_abc_ATTEMPT_4", "_TID_", "_ATTEMPT_"),
      std::nullopt);
}

} // namespace
} // namespace bytedance::bolt::test
