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

#include <memory>
#include <thread>
#include <type_traits>

#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/torch/Operator.h"
#include "bolt/torch/tests/utils.hpp"

using namespace ::bytedance::bolt::torch::testing;
using namespace ::bytedance::bolt;
using ::bytedance::bolt::exec::test::OperatorTestBase;
using ::bytedance::bolt::exec::test::PlanBuilder;

std::pair<std::shared_ptr<exec::Operator>, std::unique_ptr<exec::DriverCtx>>
MakeTorchOperator(
    std::shared_ptr<::bytedance::bolt::torch::TorchNode> plan_node) {
  constexpr int32_t kOperatorId = 1;
  auto output_type = plan_node->outputType();
  constexpr auto kTaskId = "TorchTestTask";
  constexpr int kDestinationPartition = 0;
  constexpr int kDriverId = 0;
  constexpr int kPipelineId = 0;
  constexpr uint32_t kSplitGroupId = 0;
  constexpr uint32_t kPartitionId = 0;

  auto script = plan_node->moduleScript();
  auto query_context = core::QueryCtx::create(nullptr, core::QueryConfig{{}});
  core::PlanFragment planFragment{std::move(plan_node)};

  auto task = exec::Task::create(
      kTaskId,
      std::move(planFragment),
      kDestinationPartition,
      std::move(query_context),
      exec::Task::ExecutionMode::kParallel);

  auto driver_context = std::make_unique<exec::DriverCtx>(
      std::move(task), kDriverId, kPipelineId, kSplitGroupId, kPartitionId);
  return std::make_pair(
      std::make_shared<::bytedance::bolt::torch::TorchOperator>(
          script,
          output_type,
          kOperatorId,
          driver_context.get(),
          "TorchOperator"),
      std::move(driver_context));
}

struct TorchOperatorTest : public TorchTest, public OperatorTestBase {
  static void SetUpTestCase() {
    ::bytedance::bolt::torch::testing::TorchTest::SetUpTestCase();
    OperatorTestBase::SetUpTestCase();
  }
  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }
};

TEST_F(TorchOperatorTest, TestConcurrentScriptExecution) {
  const ::bytedance::bolt::RowVectorPtr input = MakeTable();
  const auto expected_output = MultiplyByTwo(input);
  auto plan_node =
      PlanBuilder()
          .values({input})
          .torch(kMultiplyByTwoTorchScript, asRowType(expected_output->type()))
          .planNode();
  auto const_torch_node =
      std::dynamic_pointer_cast<const ::bytedance::bolt::torch::TorchNode>(
          plan_node);
  auto torch_node =
      std::const_pointer_cast<::bytedance::bolt::torch::TorchNode>(
          const_torch_node);
  auto [bolt_operator, driver_context] = MakeTorchOperator(torch_node);

  // Test initial conditions
  {
    // The bolt operator needs at least one tensor input.
    ASSERT_TRUE(bolt_operator->needsInput());

    // The newly created bolt_operator does not have outputs.
    ASSERT_EQ(bolt_operator->getOutput(), nullptr);

    // The newly created bolt_operator is not finished.
    ASSERT_FALSE(bolt_operator->isFinished());
  }

  // Run the operator lifecycle by concurrently adding inputs
  // and consuming outputs.
  {
    constexpr size_t num_tasks = 32;
    std::vector<std::thread> threads;
    std::vector<RowVectorPtr> outputs(num_tasks, nullptr);
    // Concurrently queue inputs to process
    for (size_t i = 0; i < num_tasks; i++) {
      threads.emplace_back([&]() { bolt_operator->addInput(input); });
    }
    // Concurrently wait for all outputs.
    for (size_t i = 0; i < num_tasks; i++) {
      threads.emplace_back([&, i]() {
        RowVectorPtr output = nullptr;
        // output may be null if consumer threads are scheduled faster than
        // producer threads.
        while (output == nullptr) {
          output = bolt_operator->getOutput();
        }
        outputs[i] = output;
      });
    }
    // Wait for all threads to finish.
    for (auto& t : threads) {
      t.join();
    }
    // Assess results are valid:
    for (auto output : outputs) {
      ASSERT_TRUE(::bytedance::bolt::exec::test::assertEqualResults(
          {expected_output}, {output}));
    }
  }

  // Test final conditions
  {
    // The operator return a nullptr when there is no more result to produce.
    ASSERT_EQ(bolt_operator->getOutput(), nullptr);

    // The operator is not finished as long as noMoreInput_ is not called.
    ASSERT_FALSE(bolt_operator->isFinished());

    // The operator is finished after processing all inputs and
    // noMoreInput() is called.
    bolt_operator->noMoreInput();
    ASSERT_TRUE(bolt_operator->isFinished());
  }
}

TEST_F(TorchOperatorTest, TestOperatorBase) {
  const ::bytedance::bolt::RowVectorPtr input = MakeTable();
  const auto expected_output = MultiplyByTwo(input);
  auto plan_node =
      PlanBuilder()
          .values({input})
          .torch(kMultiplyByTwoTorchScript, asRowType(expected_output->type()))
          .planNode();
  assertQuery(plan_node, expected_output);
}
