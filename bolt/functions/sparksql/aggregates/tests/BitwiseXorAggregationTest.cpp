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

#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
namespace bytedance::bolt::functions::aggregate::sparksql::test {

namespace {

class BitwiseXorAggregationTest : public aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("");
    allowInputShuffle();
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {BIGINT(), TINYINT(), SMALLINT(), INTEGER(), BIGINT()})};
};

TEST_F(BitwiseXorAggregationTest, bitwiseXor) {
  auto vectors = makeVectors(rowType_, 100, 10);
  createDuckDbTable(vectors);

  // Global aggregation.
  testAggregations(
      vectors,
      {},
      {"bit_xor(c1)", "bit_xor(c2)", "bit_xor(c3)", "bit_xor(c4)"},
      "SELECT bit_xor(c1), bit_xor(c2), bit_xor(c3), bit_xor(c4) FROM tmp");

  // Group by aggregation.
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 10", "c1", "c2", "c3", "c4"});
      },
      {"p0"},
      {"bit_xor(c1)", "bit_xor(c2)", "bit_xor(c3)", "bit_xor(c4)"},
      "SELECT c0 % 10, bit_xor(c1), bit_xor(c2), bit_xor(c3), bit_xor(c4) FROM tmp GROUP BY 1");
}

} // namespace
} // namespace bytedance::bolt::functions::aggregate::sparksql::test
