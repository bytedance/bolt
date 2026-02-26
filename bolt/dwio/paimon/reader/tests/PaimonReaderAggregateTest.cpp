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

#include <limits>
#include <map>
#include <optional>
#include <string>

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <bolt/dwio/common/BufferedInput.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "folly/Range.h"

#include "bolt/common/base/Fs.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/PaimonConnectorSplit.h"
#include "bolt/connectors/hive/PaimonConstants.h"
#include "bolt/connectors/hive/paimon_merge_engines/PaimonRowKind.h"
#include "bolt/dwio/paimon/reader/tests/PaimonTestUtils.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include <folly/init/Init.h>
#include <algorithm>

using namespace bytedance::bolt;
// using exec::test::HiveConnectorTestBase;
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector;
using namespace bytedance::bolt::connector::hive;
namespace bytedance::bolt::dwio::paimon::test {
namespace {

class PaimonReaderAggregateTest : public testing::Test,
                                  public bytedance::bolt::test::VectorTestBase {
 protected:
  static const std::string kHiveConnectorId;

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetTests");
    leafPool_ = rootPool_->addLeafChild("ParquetTests");

    auto hiveConnector =
        connector::getConnectorFactory(connector::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kHiveConnectorId);
    leafPool_.reset();
    rootPool_.reset();
  }

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
  getIdentityAssignment(RowTypePtr rowType) {
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments;

    for (int i = 0; i < rowType->size(); i++) {
      auto& colType = rowType->childAt(i);
      auto& colName = rowType->nameOf(i);
      assignments[colName] =
          std::make_shared<connector::hive::HiveColumnHandle>(
              colName,
              HiveColumnHandle::ColumnType::kRegular,
              colType,
              colType);
    }

    return assignments;
  }

  std::vector<std::string> getFilePaths(std::string directoryPath) {
    std::vector<std::string> filePaths;
    auto fs = filesystems::getFileSystem(directoryPath, nullptr);

    for (const auto& entry : fs->list(directoryPath)) {
      if (!fs->isDirectory(entry)) {
        filePaths.push_back(entry);
      }
    }

    return filePaths;
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
};

const std::string PaimonReaderAggregateTest::kHiveConnectorId = "test-hive";

static std::unordered_map<std::string, RuntimeMetric> getTableScanRuntimeStats(
    const std::shared_ptr<bytedance::bolt::exec::Task>& task) {
  return task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
}

TEST_F(PaimonReaderAggregateTest, sumNoPrimaryKey) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW(
      {{"a", INTEGER()},
       {"b", TINYINT()},
       {"c", SMALLINT()},
       {"d", INTEGER()},
       {"e", BIGINT()},
       {"f", REAL()},
       {"g", DOUBLE()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       vectorMaker_.flatVector<int8_t>({3, 4}),
       vectorMaker_.flatVector<int16_t>({5, 6}),
       vectorMaker_.flatVector<int32_t>({7, 8}),
       vectorMaker_.flatVector<int64_t>({9, 10}),
       vectorMaker_.flatVector<float>({11, 12}),
       vectorMaker_.flatVector<double>({13, 14})},
      {0, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {
          vectorMaker_.flatVector<int32_t>({1, 2}),
          vectorMaker_.flatVector<int8_t>({23, 24}),
          vectorMaker_.flatVector<int16_t>({25, 26}),
          vectorMaker_.flatVector<int32_t>({27, 28}),
          vectorMaker_.flatVector<int64_t>({29, 30}),
          vectorMaker_.flatVector<float>({31, 32}),
          vectorMaker_.flatVector<double>({33, 34}),
      },
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kAggregateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {"fields.b.aggregate-function", "sum"},
      {"fields.c.aggregate-function", "sum"},
      {"fields.d.aggregate-function", "sum"},
      {"fields.e.aggregate-function", "sum"},
      {"fields.f.aggregate-function", "sum"},
      {"fields.g.aggregate-function", "sum"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

  rowType = ROW(
      {{"b", TINYINT()},
       {"c", SMALLINT()},
       {"d", INTEGER()},
       {"e", BIGINT()},
       {"f", REAL()},
       {"g", DOUBLE()}});
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(rowType, tableHandle, assignments)
                              .capturePlanNodeId(scanNodeId)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  auto filePaths = getFilePaths(tempDir->getPath());

  std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
  for (auto filePath : filePaths) {
    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
    hiveSplits.push_back(hiveSplit);
  }

  auto connectorSplit = std::make_shared<connector::hive::PaimonConnectorSplit>(
      kHiveConnectorId, hiveSplits);

  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});

  readTask->noMoreSplits(scanNodeId);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int8_t>({26, 28}),
          vectorMaker_.flatVector<int16_t>({30, 32}),
          vectorMaker_.flatVector<int32_t>({34, 36}),
          vectorMaker_.flatVector<int64_t>({38, 40}),
          vectorMaker_.flatVector<float>({42, 44}),
          vectorMaker_.flatVector<double>({46, 48}),
      });

  auto result = readTask->next();
  bytedance::bolt::test::assertEqualVectors(expected, result);

  result = readTask->next();
  BOLT_CHECK_NULL(result);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderAggregateTest, sumWithPrimaryKey) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW(
      {{"a", INTEGER()},
       {"b", TINYINT()},
       {"c", SMALLINT()},
       {"d", INTEGER()},
       {"e", BIGINT()},
       {"f", REAL()},
       {"g", DOUBLE()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {
          vectorMaker_.flatVector<int32_t>({1, 2}),
          vectorMaker_.flatVector<int8_t>({3, 4}),
          vectorMaker_.flatVector<int16_t>({5, 6}),
          vectorMaker_.flatVector<int32_t>({7, 8}),
          vectorMaker_.flatVector<int64_t>({9, 10}),
          vectorMaker_.flatVector<float>({11, 12}),
          vectorMaker_.flatVector<double>({13, 14}),
      },
      {0, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       vectorMaker_.flatVector<int8_t>({23, 24}),
       vectorMaker_.flatVector<int16_t>({25, 26}),
       vectorMaker_.flatVector<int32_t>({27, 28}),
       vectorMaker_.flatVector<int64_t>({29, 30}),
       vectorMaker_.flatVector<float>({31, 32}),
       vectorMaker_.flatVector<double>({33, 34})},
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kAggregateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {"fields.b.aggregate-function", "sum"},
      {"fields.c.aggregate-function", "sum"},
      {"fields.d.aggregate-function", "sum"},
      {"fields.e.aggregate-function", "sum"},
      {"fields.f.aggregate-function", "sum"},
      {"fields.g.aggregate-function", "sum"},
      {"fields.h.aggregate-function", "sum"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

  rowType = ROW({
      {"a", INTEGER()},
      {"b", TINYINT()},
      {"c", SMALLINT()},
      {"d", INTEGER()},
      {"e", BIGINT()},
      {"f", REAL()},
      {"g", DOUBLE()},
  });
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(rowType, tableHandle, assignments)
                              .capturePlanNodeId(scanNodeId)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  auto filePaths = getFilePaths(tempDir->getPath());

  std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
  for (auto filePath : filePaths) {
    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
    hiveSplits.push_back(hiveSplit);
  }

  auto connectorSplit = std::make_shared<connector::hive::PaimonConnectorSplit>(
      kHiveConnectorId, hiveSplits);

  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});

  readTask->noMoreSplits(scanNodeId);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 2}),
          vectorMaker_.flatVector<int8_t>({26, 28}),
          vectorMaker_.flatVector<int16_t>({30, 32}),
          vectorMaker_.flatVector<int32_t>({34, 36}),
          vectorMaker_.flatVector<int64_t>({38, 40}),
          vectorMaker_.flatVector<float>({42, 44}),
          vectorMaker_.flatVector<double>({46, 48}),
      });

  auto result = readTask->next();
  bytedance::bolt::test::assertEqualVectors(expected, result);

  result = readTask->next();
  BOLT_CHECK_NULL(result);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderAggregateTest, listAgg) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       vectorMaker_.flatVector<StringView>({"Hello", "World"})},
      {0, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       vectorMaker_.flatVector<StringView>({"Puff", "Daddy"})},
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kAggregateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {"fields.b.aggregate-function",
       connector::paimon::kAggregateListAggName}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

  rowType = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(rowType, tableHandle, assignments)
                              .capturePlanNodeId(scanNodeId)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  auto filePaths = getFilePaths(tempDir->getPath());

  std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
  for (auto filePath : filePaths) {
    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
    hiveSplits.push_back(hiveSplit);
  }

  auto connectorSplit = std::make_shared<connector::hive::PaimonConnectorSplit>(
      kHiveConnectorId, hiveSplits);

  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});

  readTask->noMoreSplits(scanNodeId);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 2}),
          vectorMaker_.flatVector<StringView>({"Hello,Puff", "World,Daddy"})});

  auto result = readTask->next();
  bytedance::bolt::test::assertEqualVectors(expected, result);

  result = readTask->next();
  BOLT_CHECK_NULL(result);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderAggregateTest, listAggDelimiter) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       vectorMaker_.flatVector<StringView>({"Hello", "World"})},
      {0, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       vectorMaker_.flatVector<StringView>({"Puff", "Daddy"})},
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kAggregateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {"fields.b.aggregate-function", connector::paimon::kAggregateListAggName},
      {"fields.b.list-agg-delimiter", ";"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

  rowType = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(rowType, tableHandle, assignments)
                              .capturePlanNodeId(scanNodeId)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  auto filePaths = getFilePaths(tempDir->getPath());

  std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
  for (auto filePath : filePaths) {
    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
    hiveSplits.push_back(hiveSplit);
  }

  auto connectorSplit = std::make_shared<connector::hive::PaimonConnectorSplit>(
      kHiveConnectorId, hiveSplits);

  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});

  readTask->noMoreSplits(scanNodeId);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 2}),
          vectorMaker_.flatVector<StringView>({"Hello;Puff", "World;Daddy"})});

  auto result = readTask->next();
  bytedance::bolt::test::assertEqualVectors(expected, result);

  result = readTask->next();
  BOLT_CHECK_NULL(result);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderAggregateTest, collectWithPrimaryKey) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW(
      {{"a", INTEGER()},
       {"b", ARRAY(TINYINT())},
       {"c", ARRAY(SMALLINT())},
       {"d", ARRAY(INTEGER())},
       {"e", ARRAY(BIGINT())},
       {"f", ARRAY(REAL())},
       {"g", ARRAY(DOUBLE())}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       makeArrayVector<int8_t>({{2, 3, 4}, {3, 6}}, TINYINT()),
       makeArrayVector<int16_t>({{2, 3, 4}, {3, 6}}, SMALLINT()),
       makeArrayVector<int32_t>({{2, 3, 4}, {3, 6}}, INTEGER()),
       makeArrayVector<int64_t>({{2, 3, 4}, {3, 6}}, BIGINT()),
       makeArrayVector<float>({{2, 3, 4}, {3, 6}}, REAL()),
       makeArrayVector<double>({{2, 3, 4}, {3, 6}}, DOUBLE())},
      {0, 1},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       makeArrayVector<int8_t>({{12, 13, 3, 14}, {3, 6, 16}}, TINYINT()),
       makeArrayVector<int16_t>({{12, 13, 3, 14}, {3, 6, 16}}, SMALLINT()),
       makeArrayVector<int32_t>({{12, 13, 3, 14}, {3, 6, 16}}, INTEGER()),
       makeArrayVector<int64_t>({{12, 13, 3, 14}, {3, 6, 16}}, BIGINT()),
       makeArrayVector<float>({{12, 13, 3, 14}, {3, 6, 16}}, REAL()),
       makeArrayVector<double>({{12, 13, 3, 14}, {3, 6, 16}}, DOUBLE())},
      {2, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kAggregateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {"fields.b.aggregate-function", "collect"},
      {"fields.c.aggregate-function", "collect"},
      {"fields.d.aggregate-function", "collect"},
      {"fields.e.aggregate-function", "collect"},
      {"fields.f.aggregate-function", "collect"},
      {"fields.g.aggregate-function", "collect"},
      {"fields.h.aggregate-function", "collect"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

  rowType = ROW(
      {{"a", INTEGER()},
       {"b", ARRAY(TINYINT())},
       {"c", ARRAY(SMALLINT())},
       {"d", ARRAY(INTEGER())},
       {"e", ARRAY(BIGINT())},
       {"f", ARRAY(REAL())},
       {"g", ARRAY(DOUBLE())}});
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(rowType, tableHandle, assignments)
                              .capturePlanNodeId(scanNodeId)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  auto filePaths = getFilePaths(tempDir->getPath());

  std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
  for (auto filePath : filePaths) {
    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
    hiveSplits.push_back(hiveSplit);
  }

  auto connectorSplit = std::make_shared<connector::hive::PaimonConnectorSplit>(
      kHiveConnectorId, hiveSplits);

  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});

  readTask->noMoreSplits(scanNodeId);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 2}),
          makeArrayVector<int8_t>(
              {{2, 3, 4, 12, 13, 3, 14}, {3, 6, 3, 6, 16}}, TINYINT()),
          makeArrayVector<int16_t>(
              {{2, 3, 4, 12, 13, 3, 14}, {3, 6, 3, 6, 16}}, SMALLINT()),
          makeArrayVector<int32_t>(
              {{2, 3, 4, 12, 13, 3, 14}, {3, 6, 3, 6, 16}}, INTEGER()),
          makeArrayVector<int64_t>(
              {{2, 3, 4, 12, 13, 3, 14}, {3, 6, 3, 6, 16}}, BIGINT()),
          makeArrayVector<float>(
              {{2, 3, 4, 12, 13, 3, 14}, {3, 6, 3, 6, 16}}, REAL()),
          makeArrayVector<double>(
              {{2, 3, 4, 12, 13, 3, 14}, {3, 6, 3, 6, 16}}, DOUBLE())});

  auto result = readTask->next();
  bytedance::bolt::test::assertEqualVectors(expected, result);

  result = readTask->next();
  BOLT_CHECK_NULL(result);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderAggregateTest, collectDistinctWithPrimaryKey) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW(
      {{"a", INTEGER()},
       {"b", ARRAY(TINYINT())},
       {"c", ARRAY(SMALLINT())},
       {"d", ARRAY(INTEGER())},
       {"e", ARRAY(BIGINT())},
       {"f", ARRAY(REAL())},
       {"g", ARRAY(DOUBLE())}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       makeArrayVector<int8_t>({{2, 3, 4}, {3, 6}}, TINYINT()),
       makeArrayVector<int16_t>({{2, 3, 4}, {3, 6}}, SMALLINT()),
       makeArrayVector<int32_t>({{2, 3, 4}, {3, 6}}, INTEGER()),
       makeArrayVector<int64_t>({{2, 3, 4}, {3, 6}}, BIGINT()),
       makeArrayVector<float>({{2, 3, 4}, {3, 6}}, REAL()),
       makeArrayVector<double>({{2, 3, 4}, {3, 6}}, DOUBLE())},
      {0, 1},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({1, 2}),
       makeArrayVector<int8_t>({{12, 13, 3, 14}, {3, 6, 16}}, TINYINT()),
       makeArrayVector<int16_t>({{12, 13, 3, 14}, {3, 6, 16}}, SMALLINT()),
       makeArrayVector<int32_t>({{12, 13, 3, 14}, {3, 6, 16}}, INTEGER()),
       makeArrayVector<int64_t>({{12, 13, 3, 14}, {3, 6, 16}}, BIGINT()),
       makeArrayVector<float>({{12, 13, 3, 14}, {3, 6, 16}}, REAL()),
       makeArrayVector<double>({{12, 13, 3, 14}, {3, 6, 16}}, DOUBLE())},
      {2, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kAggregateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {"fields.b.distinct", "true"},
      {"fields.c.distinct", "true"},
      {"fields.d.distinct", "true"},
      {"fields.e.distinct", "true"},
      {"fields.f.distinct", "true"},
      {"fields.g.distinct", "true"},
      {"fields.h.distinct", "true"},
      {"fields.b.aggregate-function", "collect"},
      {"fields.c.aggregate-function", "collect"},
      {"fields.d.aggregate-function", "collect"},
      {"fields.e.aggregate-function", "collect"},
      {"fields.f.aggregate-function", "collect"},
      {"fields.g.aggregate-function", "collect"},
      {"fields.h.aggregate-function", "collect"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

  rowType = ROW(
      {{"a", INTEGER()},
       {"b", ARRAY(TINYINT())},
       {"c", ARRAY(SMALLINT())},
       {"d", ARRAY(INTEGER())},
       {"e", ARRAY(BIGINT())},
       {"f", ARRAY(REAL())},
       {"g", ARRAY(DOUBLE())}});
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(rowType, tableHandle, assignments)
                              .capturePlanNodeId(scanNodeId)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  auto filePaths = getFilePaths(tempDir->getPath());

  std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
  for (auto filePath : filePaths) {
    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
    hiveSplits.push_back(hiveSplit);
  }

  auto connectorSplit = std::make_shared<connector::hive::PaimonConnectorSplit>(
      kHiveConnectorId, hiveSplits);

  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});

  readTask->noMoreSplits(scanNodeId);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 2}),
          makeArrayVector<int8_t>(
              {{2, 3, 4, 12, 13, 14}, {3, 6, 16}}, TINYINT()),
          makeArrayVector<int16_t>(
              {{2, 3, 4, 12, 13, 14}, {3, 6, 16}}, SMALLINT()),
          makeArrayVector<int32_t>(
              {{2, 3, 4, 12, 13, 14}, {3, 6, 16}}, INTEGER()),
          makeArrayVector<int64_t>(
              {{2, 3, 4, 12, 13, 14}, {3, 6, 16}}, BIGINT()),
          makeArrayVector<float>({{2, 3, 4, 12, 13, 14}, {3, 6, 16}}, REAL()),
          makeArrayVector<double>(
              {{2, 3, 4, 12, 13, 14}, {3, 6, 16}}, DOUBLE())});

  auto result = readTask->next();
  bytedance::bolt::test::assertEqualVectors(expected, result);

  result = readTask->next();
  BOLT_CHECK_NULL(result);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

} // namespace
} // namespace bytedance::bolt::dwio::paimon::test
