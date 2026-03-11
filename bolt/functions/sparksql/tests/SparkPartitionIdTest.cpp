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

class SparkPartitionIdTest : public SparkFunctionBaseTest {
 protected:
  void testSparkPartitionId(int32_t partitionId, int32_t vectorSize) {
    setSparkPartitionId(partitionId);
    auto result =
        evaluate("spark_partition_id()", makeRowVector(ROW({}), vectorSize));
    ASSERT_TRUE(result->isConstantEncoding());
    bolt::test::assertEqualVectors(
        makeConstant(partitionId, vectorSize), result);
  }
};

TEST_F(SparkPartitionIdTest, basic) {
  testSparkPartitionId(0, 1);
  testSparkPartitionId(100, 1);
  testSparkPartitionId(0, 100);
  testSparkPartitionId(100, 100);
}

TEST_F(SparkPartitionIdTest, error) {
  auto rowVector = makeRowVector(ROW({}), 1);

  queryCtx_->testingOverrideConfigUnsafe({{}});
  BOLT_ASSERT_THROW(
      evaluate("spark_partition_id()", rowVector),
      "Spark partition id is not set");

  setSparkPartitionId(-1);
  BOLT_ASSERT_THROW(
      evaluate("spark_partition_id()", rowVector),
      "Invalid Spark partition id");
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
