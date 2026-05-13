/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::connector::hive::test {
namespace {

using bytedance::bolt::exec::test::PlanBuilder;

/// Covers the Hive-specific HivePartitionFunctionSpec branch of
/// PartitionedOutputNode::toString. The connector-agnostic parts of
/// PartitionedOutputNode rendering live in
/// bolt/exec/tests/PlanNodeToStringTest.cpp.
class HivePartitionFunctionPlanNodeToStringTest
    : public ::testing::Test,
      public bolt::test::VectorTestBase {
 public:
  HivePartitionFunctionPlanNodeToStringTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    data_ = makeRowVector(
        {makeFlatVector<int16_t>({0, 1, 2, 3, 4}),
         makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
         makeFlatVector<int64_t>({0, 1, 2, 3, 4})});
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  RowVectorPtr data_;
};

TEST_F(HivePartitionFunctionPlanNodeToStringTest, partitionedOutput) {
  auto hiveSpec = std::make_shared<HivePartitionFunctionSpec>(
      4,
      std::vector<int>{0, 1, 0, 1},
      std::vector<column_index_t>{1, 2},
      std::vector<VectorPtr>{});

  auto plan = PlanBuilder()
                  .values({data_})
                  .partitionedOutput({"c1", "c2"}, 2, false, hiveSpec)
                  .planNode();
  ASSERT_EQ("-- PartitionedOutput[1]\n", plan->toString(false, false, true));
  ASSERT_EQ(
      "-- PartitionedOutput[1][partitionFunction: HIVE((1, 2) buckets: 4) with 2 partitions] -> c0:SMALLINT, c1:INTEGER, c2:BIGINT\n",
      plan->toString(true, false, true));
}

} // namespace
} // namespace bytedance::bolt::connector::hive::test
