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

#include "bolt/functions/flinksql/registration/Register.h"
#include "bolt/functions/prestosql/tests/CastBaseTest.h"
#include "bolt/parse/TypeResolver.h"

namespace bytedance::bolt::functions::flinksql::test {

class FlinkCastExprTest : public functions::test::CastBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    flinksql::registerFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(FlinkCastExprTest, decimalToVarcharPreservesPlainScale) {
  const auto decimalType = DECIMAL(10, 8);

  testCast<int64_t, StringView>(
      "varchar",
      {0, 1, 100000000, -1, std::nullopt},
      {"0.00000000", "0.00000001", "1.00000000", "-0.00000001", std::nullopt},
      decimalType,
      VARCHAR());

  testTryCast<int64_t, StringView>(
      "varchar",
      {0, 1, 100000000, -1, std::nullopt},
      {"0.00000000", "0.00000001", "1.00000000", "-0.00000001", std::nullopt},
      decimalType,
      VARCHAR());
}

TEST_F(FlinkCastExprTest, longDecimalToVarcharPreservesPlainScale) {
  const auto decimalType = DECIMAL(30, 10);

  testCast<int128_t, StringView>(
      "varchar",
      {0, 1, 10000000000LL, -1, std::nullopt},
      {"0.0000000000",
       "0.0000000001",
       "1.0000000000",
       "-0.0000000001",
       std::nullopt},
      decimalType,
      VARCHAR());

  testTryCast<int128_t, StringView>(
      "varchar",
      {0, 1, 10000000000LL, -1, std::nullopt},
      {"0.0000000000",
       "0.0000000001",
       "1.0000000000",
       "-0.0000000001",
       std::nullopt},
      decimalType,
      VARCHAR());
}

TEST_F(FlinkCastExprTest, nonDecimalToVarcharUsesDefaultCast) {
  testCast<int64_t, StringView>(
      "varchar", {0, 12, -34, std::nullopt}, {"0", "12", "-34", std::nullopt});
}

} // namespace bytedance::bolt::functions::flinksql::test
