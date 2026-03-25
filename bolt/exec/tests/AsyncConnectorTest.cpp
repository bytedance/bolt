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

#include <folly/experimental/FunctionScheduler.h>
#include "bolt/connectors/Connector.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector;
using namespace bytedance::bolt::test;
namespace bytedance::bolt::exec::test {

namespace {

const std::string kTestConnectorId = "test";

class TestTableHandle : public connector::ConnectorTableHandle {
 public:
  TestTableHandle() : connector::ConnectorTableHandle(kTestConnectorId) {}

  std::string toString() const override {
    BOLT_NYI();
  }

  const std::string& name() const override {
    BOLT_NYI();
  }
};

class TestSplit : public connector::ConnectorSplit {
 public:
  explicit TestSplit(uint32_t delayMs)
      : connector::ConnectorSplit(kTestConnectorId), delayMs_{delayMs} {
    scheduler_.start();
  }

  ~TestSplit() override {
    scheduler_.shutdown();
  }

  ContinueFuture touch() {
    if (delayMs_ == 0) {
      return ContinueFuture::makeEmpty();
    }

    auto [promise, future] = makeBoltContinuePromiseContract();

    promise_ = std::move(promise);
    scheduler_.addFunction(
        [&]() { promise_.setValue(); }, std::chrono::milliseconds(delayMs_));

    return std::move(future);
  }

 private:
  const uint32_t delayMs_;
  folly::FunctionScheduler scheduler_;
  bolt::ContinuePromise promise_;
};

class TestDataSource : public connector::DataSource {
 public:
  explicit TestDataSource(memory::MemoryPool* pool) : pool_{pool} {}

  void addSplit(std::shared_ptr<connector::ConnectorSplit> split) override {
    auto testSplit = std::dynamic_pointer_cast<TestSplit>(split);
    BOLT_CHECK_NOT_NULL(testSplit);
    future_ = testSplit->touch();
    needSplit_ = false;
  }

  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future)
      override {
    if (future_.valid()) {
      future = std::move(future_);
      return std::nullopt;
    }

    if (needSplit_) {
      return nullptr;
    }

    needSplit_ = true;
    auto data =
        BaseVector::create<FlatVector<int64_t>>({BIGINT()}, size, pool_);
    for (auto i = 0; i < size; i++) {
      data->set(i, i);
    }

    return std::make_shared<RowVector>(
        pool_,
        ROW({"a"}, {BIGINT()}),
        nullptr,
        size,
        std::vector<VectorPtr>{data});
  }

  void addDynamicFilter(
      column_index_t /* outputChannel */,
      const std::shared_ptr<common::Filter>& /* filter */) override {
    BOLT_NYI();
  }

  uint64_t getCompletedBytes() override {
    return 0;
  }

  uint64_t getCompletedRows() override {
    return 0;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  memory::MemoryPool* pool_;
  bool needSplit_{true};
  ContinueFuture future_{ContinueFuture::makeEmpty()};
};

class TestConnector : public connector::Connector {
 public:
  TestConnector(const std::string& id) : connector::Connector(id) {}

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr& /* outputType */,
      const std::shared_ptr<ConnectorTableHandle>& /* tableHandle */,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /* columnHandles */,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& /* queryConfig */) override {
    return std::make_unique<TestDataSource>(connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<connector::DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/,
      const core::QueryConfig& /*queryConfig*/) override final {
    BOLT_NYI();
  }
};

class TestConnectorFactory : public connector::ConnectorFactory {
 public:
  static constexpr const char* kTestConnectorName = "test";

  TestConnectorFactory() : connector::ConnectorFactory(kTestConnectorName) {}

  std::shared_ptr<connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /* executor */) override {
    return std::make_shared<TestConnector>(id);
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* executor = nullptr) override {
    std::shared_ptr<const config::ConfigBase> convertedConfig;
    convertedConfig = config == nullptr
        ? nullptr
        : std::make_shared<config::ConfigBase>(config->valuesCopy());
    return newConnector(id, convertedConfig, executor);
  }
};
} // namespace

class AsyncConnectorTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    connector::registerConnectorFactory(
        std::make_shared<TestConnectorFactory>());
    auto testConnector =
        connector::getConnectorFactory(TestConnectorFactory::kTestConnectorName)
            ->newConnector(
                kTestConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()),
                nullptr);
    connector::registerConnector(testConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kTestConnectorId);
    OperatorTestBase::TearDown();
  }
};

TEST_F(AsyncConnectorTest, basic) {
  auto tableHandle = std::make_shared<TestTableHandle>();
  core::PlanNodeId scanId;
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(ROW({"a"}, {BIGINT()}))
                  .tableHandle(tableHandle)
                  .endTableScan()
                  .capturePlanNodeId(scanId)
                  .singleAggregation({}, {"min(a)"})
                  .planNode();

  // Run without a delay and verify blocked time for table scan is zero.
  {
    auto task = assertQuery(plan, {std::make_shared<TestSplit>(0)}, "SELECT 0");
    auto stats = toPlanStats(task->taskStats());
    const auto& scanStats = stats.at(scanId);
    ASSERT_EQ(scanStats.blockedWallNanos, 0);
  }

  // Run with a delay and verify blocked time for table scan is non-zero.
  {
    auto task =
        assertQuery(plan, {std::make_shared<TestSplit>(100)}, "SELECT 0");
    auto stats = toPlanStats(task->taskStats());
    const auto& scanStats = stats.at(scanId);
    ASSERT_GT(scanStats.blockedWallNanos, 0);
  }
}

} // namespace bytedance::bolt::exec::test
