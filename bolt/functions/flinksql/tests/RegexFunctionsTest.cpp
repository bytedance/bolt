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

#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"

namespace bytedance::bolt::functions::flinksql::test {
namespace {

class FlinkRegexFunctionsTest : public FlinkFunctionBaseTest {};

TEST_F(FlinkRegexFunctionsTest, invalidRegexReturnsFalse) {
  EXPECT_EQ(
      false,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"**"}),
          })));

  EXPECT_EQ(
      false,
      evaluateOnce<bool>(
          "rlike(c0, '**')",
          makeRowVector({makeNullableFlatVector(
              std::vector<std::optional<std::string>>{"abc"})})));
}

TEST_F(FlinkRegexFunctionsTest, validRegexStillWorks) {
  EXPECT_EQ(
      true,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc123"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"[0-9]+"}),
          })));

  EXPECT_EQ(
      false,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"[0-9]+"}),
          })));

  EXPECT_EQ(
      true,
      evaluateOnce<bool>(
          "rlike(c0, '[0-9]+')",
          makeRowVector({makeNullableFlatVector(
              std::vector<std::optional<std::string>>{"abc123"})})));
}

TEST_F(FlinkRegexFunctionsTest, nullInputPreservesNullSemantics) {
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{std::nullopt}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"[0-9]+"}),
          })));

  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<bool>(
          "rlike(c0, c1)",
          makeRowVector({
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{"abc"}),
              makeNullableFlatVector(
                  std::vector<std::optional<std::string>>{std::nullopt}),
          })));
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
