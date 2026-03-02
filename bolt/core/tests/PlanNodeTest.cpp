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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/core/PlanNode.h"

using namespace ::bytedance::bolt;
using namespace ::bytedance::bolt::core;

namespace {
class PlanNodeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(PlanNodeTest, findFirstNode) {
  auto rowType = ROW({"name1"}, {BIGINT()});

  std::shared_ptr<connector::ConnectorTableHandle> tableHandle;
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  std::shared_ptr<PlanNode> tableScan3 =
      std::make_shared<TableScanNode>("3", rowType, tableHandle, assignments);
  std::shared_ptr<PlanNode> tableScan2 =
      std::make_shared<TableScanNode>("2", rowType, tableHandle, assignments);

  std::vector<FieldAccessTypedExprPtr> sortingKeys;
  std::vector<SortOrder> sortingOrders;
  std::shared_ptr<PlanNode> localMerge1 = std::make_shared<LocalMergeNode>(
      "1",
      sortingKeys,
      sortingOrders,
      std::vector<PlanNodePtr>{tableScan2, tableScan3});

  std::vector<std::string> names;
  std::vector<TypedExprPtr> projections;
  std::shared_ptr<PlanNode> project0 =
      std::make_shared<ProjectNode>("0", names, projections, localMerge1);

  EXPECT_EQ(
      tableScan3.get(),
      PlanNode::findFirstNode(project0.get(), [](const PlanNode* node) {
        return node->id() == "3";
      }));

  EXPECT_EQ(
      project0.get(),
      PlanNode::findFirstNode(project0.get(), [](const PlanNode* node) {
        return node->name() == "Project";
      }));

  EXPECT_EQ(
      nullptr,
      PlanNode::findFirstNode(project0.get(), [](const PlanNode* node) {
        return node->name() == "Unknown";
      }));
}

TEST_F(PlanNodeTest, sortOrder) {
  struct {
    SortOrder order1;
    SortOrder order2;
    int expectedEqual;

    std::string debugString() const {
      return fmt::format(
          "order1 {} order2 {} expectedEqual {}",
          order1.toString(),
          order2.toString(),
          expectedEqual);
    }
  } testSettings[] = {
      {{true, true}, {true, true}, true},
      {{true, false}, {true, false}, true},
      {{false, true}, {false, true}, true},
      {{false, false}, {false, false}, true},
      {{true, true}, {true, false}, false},
      {{true, true}, {false, false}, false},
      {{true, true}, {false, true}, false},
      {{true, false}, {false, false}, false},
      {{true, false}, {false, true}, false},
      {{false, true}, {false, false}, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.expectedEqual) {
      ASSERT_EQ(testData.order1, testData.order2);
    } else {
      ASSERT_NE(testData.order1, testData.order2);
    }
  }
}

TEST_F(PlanNodeTest, duplicateSortKeys) {
  auto sortingKeys = std::vector<FieldAccessTypedExprPtr>{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"),
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c1"),
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"),
  };
  auto sortingOrders =
      std::vector<SortOrder>{{true, true}, {false, false}, {true, true}};
  BOLT_ASSERT_USER_THROW(
      std::make_shared<OrderByNode>(
          "orderBy", sortingKeys, sortingOrders, false, nullptr),
      "Duplicate sorting keys are not allowed: c0");
}
} // namespace
