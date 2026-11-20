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
 * 2026-08-28.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <algorithm>
#include <filesystem>
#include <fstream>

#include <folly/json.h>
#include <gtest/gtest.h>

#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/exec/PartitionFunction.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/exec/tests/utils/TpcdsPlanLoader.h"

namespace bytedance::bolt::exec::test {
namespace {

class TpcdsPlanLoaderTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  void writePlan(const std::string& name, const core::PlanNodePtr& plan) {
    std::ofstream output(std::filesystem::path(directory_->path) / name);
    output << folly::toJson(plan->serialize());
  }

  std::shared_ptr<TempDirectoryPath> directory_ = TempDirectoryPath::create();
};

TEST_F(TpcdsPlanLoaderTest, queryManifest) {
  EXPECT_EQ(TpcdsPlanLoader::queryNames().size(), 103);
  EXPECT_NE(
      std::find(
          TpcdsPlanLoader::queryNames().begin(),
          TpcdsPlanLoader::queryNames().end(),
          "q14a"),
      TpcdsPlanLoader::queryNames().end());
  EXPECT_NE(
      std::find(
          TpcdsPlanLoader::queryNames().begin(),
          TpcdsPlanLoader::queryNames().end(),
          "q14b"),
      TpcdsPlanLoader::queryNames().end());
}

TEST_F(TpcdsPlanLoaderTest, loadsVariantAndStripsPartitionedOutput) {
  auto serializedPlan =
      PlanBuilder()
          .tableScan("store_sales", ROW({"ss_item_sk"}, {INTEGER()}))
          .partitionedOutput({}, 1)
          .planNode();
  writePlan("q14a.json", serializedPlan);

  TpcdsPlanLoader loader(directory_->path);
  auto plan = loader.loadPlan("Q14A");
  EXPECT_EQ(plan.planName, "Q14A");
  EXPECT_EQ(plan.plan->name(), "TableScan");
  const auto scans = TpcdsPlanLoader::collectTableScanNodes(plan.plan);
  ASSERT_EQ(scans.size(), 1);
  EXPECT_EQ(scans.front()->tableHandle()->name(), "store_sales");
}

TEST_F(TpcdsPlanLoaderTest, acceptsPrestoUppercaseFileName) {
  auto serializedPlan =
      PlanBuilder().values({}).partitionedOutput({}, 1).planNode();
  writePlan("Q1.json", serializedPlan);

  TpcdsPlanLoader loader(directory_->path);
  EXPECT_EQ(loader.loadPlan("q1").plan->name(), "Values");
}

TEST_F(TpcdsPlanLoaderTest, rejectsUnknownOrMissingQuery) {
  TpcdsPlanLoader loader(directory_->path);
  EXPECT_THROW(loader.loadPlan("q100"), BoltUserError);
  EXPECT_THROW(loader.loadPlan("q2"), BoltUserError);
}

} // namespace
} // namespace bytedance::bolt::exec::test
