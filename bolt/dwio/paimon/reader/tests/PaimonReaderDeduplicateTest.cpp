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

class PaimonReaderDeduplicateTest
    : public testing::Test,
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

const std::string PaimonReaderDeduplicateTest::kHiveConnectorId = "test-hive";

static std::unordered_map<std::string, RuntimeMetric> getTableScanRuntimeStats(
    const std::shared_ptr<bytedance::bolt::exec::Task>& task) {
  return task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
}

TEST_F(PaimonReaderDeduplicateTest, insertAllUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  3       0                +I
  2       2  4       3                +I

  _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  5       4                +I
  2       2  6       6                +I
  -------------------------------------
  1       1  5       4                +I
  2       2  6       6                +I      */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {0, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
          vectorMaker_.flatVector<int32_t>({5, 6})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicNoUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
   _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  3       4                +I
  2       2  4       3                +I

  _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  5       1                +I
  2       2  6       2                +I
  -------------------------------------
  1       1  3       4                +I
  2       2  4       3                +I      */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {1, 2},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
          vectorMaker_.flatVector<int32_t>({3, 4})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicFirstUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  3       4                +I
  2       2  4       3                +I

  _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  5       5                +I
  2       2  6       2                +I
  -------------------------------------
  1       1  5       5                +I
  2       2  4       3                +I    */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {5, 2},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
          vectorMaker_.flatVector<int32_t>({5, 4})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicLastUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  3       4                +I
  2       2  4       3                +I

  _KEY_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1       1  5       1                +I
  2       2  6       5                +I
  -------------------------------------
  1       1  3       4                +I
  2       2  6       5                +I    */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {1, 5},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
          vectorMaker_.flatVector<int32_t>({3, 6})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
}

TEST_F(PaimonReaderDeduplicateTest, differentPrimaryKeys) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  PK  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  1   1  3       4                +I
  2   2  4       3                +I

  PK  a  b _SEQUENCE_NUMBER  _VALUE_KIND
  2   2  5       1                +I
  3   3  6       5                +I
  -------------------------------------
  1   1  3       4                +I
  2   2  4       1                +I
  3   3  6       5                +I   */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{2, 3}, {5, 6}},
      {1, 5},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
      3,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 2, 3}),
          vectorMaker_.flatVector<int32_t>({3, 4, 6})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, multiplePrimaryKeys) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_a  _KEY_b   a   b  c _SEQUENCE_NUMBER  _VALUE_KIND
  1        3       1   3  5    4                +I
  2        4       2   4  6    3                +I
  _KEY_a  _KEY_b   a   b  c _SEQUENCE_NUMBER  _VALUE_KIND
  1        2       1   2  9    1                +I
  1        3       1   3  7    8                +I
  -------------------------------------------------------
  1        2       1   2  9    1                +I
  1        3       1   3  7    8                +I
  2        4       2   4  6    3                +I  */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}, {"c", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0, 1},
      rowType,
      {{1, 2}, {3, 4}, {5, 6}},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0, 1},
      rowType,
      {{1, 1}, {2, 3}, {9, 7}},
      {1, 8},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a,b"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
      3,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 1, 2}),
          vectorMaker_.flatVector<int32_t>({2, 3, 4}),
          vectorMaker_.flatVector<int32_t>({9, 7, 6})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, multiplePrimaryKeysReverse) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_b  _KEY_a   a   b  c _SEQUENCE_NUMBER  _VALUE_KIND
  3        1       1   3  5    4                +I
  4        2       2   4  6    3                +I
  _KEY_b  _KEY_a   a   b  c _SEQUENCE_NUMBER  _VALUE_KIND
  2        1       1   2  9    1                +I
  3        1       1   3  7    8                +I
  -------------------------------------------------------
  2        1       1   2  9    1                +I
  3        1       1   3  7    8                +I
  4        2       2   4  6    3                +I   */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}, {"c", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {1, 0},
      rowType,
      {{1, 2}, {3, 4}, {5, 6}},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {1, 0},
      rowType,
      {{1, 1}, {2, 3}, {9, 7}},
      {1, 8},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "b,a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
      3,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1, 1, 2}),
          vectorMaker_.flatVector<int32_t>({2, 3, 4}),
          vectorMaker_.flatVector<int32_t>({9, 7, 6})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicIgnoreDeleteAll) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  3       0                -D
    2   2  4       3                -D

  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  5       4                -D
    2   2  6       2                -D
  -------------------------------------
                 Empty                 */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {0, 3},
      {int8_t(PaimonRowKind::DELETE), int8_t(PaimonRowKind::DELETE)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {4, 2},
      {int8_t(PaimonRowKind::DELETE), int8_t(PaimonRowKind::DELETE)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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

  while (auto result = readTask->next()) {
    BOLT_UNREACHABLE("All deleted produced results");
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicIgnoreLastDelete) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following:
  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  3       0                +I
    2   2  4       3                +I

  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  5       4                +I
    2   2  6       6                -D
  -------------------------------------
    1   1  5       4                +I
    2   2  4       3                +I  */

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {0, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::DELETE)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
          vectorMaker_.flatVector<int32_t>({5, 4})});

  while (auto result = readTask->next()) {
    LOG(INFO) << "Hive reader result:";
    for (vector_size_t i = 0; i < result->size(); ++i) {
      LOG(INFO) << result->toString(i);
    }
    // bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicIgnoreNonLastDelete) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});

  /* Merging following:
  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  3       0                -D
    2   2  4       3                +I

  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  5       4                +I
    2   2  6       6                +I
  -------------------------------------
    1   1  5       4                +I
    2   2  6       6                +I  */
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {0, 3},
      {int8_t(PaimonRowKind::DELETE), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
          vectorMaker_.flatVector<int32_t>({5, 6})});

  while (auto result = readTask->next()) {
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicNoIgnoreLastDelete) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following:
  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  3       0                +I
    2   2  4       3                -D

  PK_a  a  b _SEQUENCE_NUMBER  _VALUE_KIND
    1   1  5       4                +I
    2   2  6       6                +I
  -------------------------------------
    1   1  5       4                +I
    2   2  6       6                +I  */
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {3, 4}},
      {0, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {{1, 2}, {5, 6}},
      {4, 6},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::DELETE)});

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "false"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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
      1,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({1}),
          vectorMaker_.flatVector<int32_t>({5})});

  auto result = readTask->next();
  bytedance::bolt::test::assertEqualVectors(expected, result);

  result = readTask->next();
  BOLT_CHECK_NULL(result);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicMultipleBatchesAllUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  int len = 20000;

  std::vector<int> a1(len);
  std::iota(a1.begin(), a1.end(), 1);

  std::vector<int> b1(len);
  std::iota(b1.begin(), b1.end(), 1);

  std::vector<int64_t> seqNum(len);
  std::iota(seqNum.begin(), seqNum.end(), 1);
  std::transform(seqNum.begin(), seqNum.end(), seqNum.begin(), [](int x) {
    return 2 * x;
  });

  std::vector<int8_t> valueKind(
      len, static_cast<int8_t>(PaimonRowKind::INSERT));

  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {a1, b1},
      seqNum,
      valueKind);

  std::vector<int> a2(len);
  std::iota(a2.begin(), a2.end(), 1);

  std::vector<int> b2(len);
  std::iota(b2.begin(), b2.end(), len + 1);

  std::vector<int64_t> seqNum2(len);
  std::iota(seqNum2.begin(), seqNum2.end(), 1);
  std::transform(seqNum2.begin(), seqNum2.end(), seqNum2.begin(), [](int x) {
    return (2 * x + 1);
  });

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {a2, b2},
      seqNum2,
      valueKind);

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "false"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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

  std::vector<int> a3(len);
  std::iota(a3.begin(), a3.end(), 1);

  std::vector<int> b3(len);
  std::iota(b3.begin(), b3.end(), len + 1);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      len,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>(a3),
          vectorMaker_.flatVector<int32_t>(b3)});

  auto combined = BaseVector::create(rowType, 0, leafPool_.get());
  while (auto result = readTask->next()) {
    combined->append(result.get());
  }

  bytedance::bolt::test::assertEqualVectors(expected, combined);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicMultipleBatchesNoUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  int len = 20000;

  std::vector<int> a1(len);
  std::iota(a1.begin(), a1.end(), 1);

  std::vector<int> b1(len);
  std::iota(b1.begin(), b1.end(), 1);

  std::vector<int64_t> seqNum(len);
  std::iota(seqNum.begin(), seqNum.end(), 1);
  std::transform(seqNum.begin(), seqNum.end(), seqNum.begin(), [](int x) {
    return 2 * x;
  });

  std::vector<int8_t> valueKind(
      len, static_cast<int8_t>(PaimonRowKind::INSERT));

  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {a1, b1},
      seqNum,
      valueKind);

  std::vector<int> a2(len);
  std::iota(a2.begin(), a2.end(), 1);

  std::vector<int> b2(len);
  std::iota(b2.begin(), b2.end(), len + 1);

  std::vector<int64_t> seqNum2(len);
  std::iota(seqNum2.begin(), seqNum2.end(), 1);
  std::transform(seqNum2.begin(), seqNum2.end(), seqNum2.begin(), [](int x) {
    return (2 * x - 1);
  });

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {a2, b2},
      seqNum2,
      valueKind);

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "false"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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

  std::vector<int> a3(len);
  std::iota(a3.begin(), a3.end(), 1);

  std::vector<int> b3(len);
  std::iota(b3.begin(), b3.end(), 1);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      len,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>(a3),
          vectorMaker_.flatVector<int32_t>(b3)});

  auto combined = BaseVector::create(rowType, 0, leafPool_.get());
  while (auto result = readTask->next()) {
    combined->append(result.get());
  }

  bytedance::bolt::test::assertEqualVectors(expected, combined);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderDeduplicateTest, basicMultipleBatches) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following:
 _KEY_a  _SEQUENCE_NUMBER  _VALUE_KIND      a      b
      1                 2          +I       1  10001
      2                 4          +I       2  10002
      3                 6          +I       3  10003
      4                 8          +I       4  10004
      5                10          +I       5  10005
...       ...               ...          ...    ...    ...
  19996             39992          +I   19996  29996
  19997             39994          +I   19997  29997
  19998             39996          +I   19998  29998
  19999             39998          +I   19999  29999
  20000             40000          +I   20000  30000

_KEY_a  _SEQUENCE_NUMBER  _VALUE_KIND      a      b
 10001             20001          +I   10001  50001
 10002             20005          +I   10002  50002
 10003             20005          +I   10003  50003
 10004             20009          +I   10004  50004
 10005             20009          +I   10005  50005
...       ...               ...          ...    ...    ...
 29996             59993          +I   29996  69996
 29997             59993          +I   29997  69997
 29998             59997          +I   29998  69998
 29999             59997          +I   29999  69999
 30000             60001          +I   30000  70000
-----------------------------------------------------------
 _KEY_a  _SEQUENCE_NUMBER  _VALUE_KIND      a      b
     1                 2            0      1  10001
     2                 4            0      2  10002
     3                 6            0      3  10003
     4                 8            0      4  10004
     5                10            0      5  10005
...      ...               ...          ...    ...    ...
  9996             19992            0   9996  19996
  9997             19994            0   9997  19997
  9998             19996            0   9998  19998
  9999             19998            0   9999  19999
 10000             20000            0  10000  20000

 10001             20002            0  10001  20001
 10002             20005            0  10002  50002
 10003             20006            0  10003  20003
 10004             20009            0  10004  50004
 ........
 19997             39993            0  19997  59997
 19998             39996            0  19998  29998
 19999             39997            0  19999  59999
 20000             40000            0  20000  30000

 20001             40001            0  20001  60001
 20002             40005            0  20002  60002
 20003             40005            0  20003  60003
 20004             40009            0  20004  60004
 20005             40009            0  20005  60005
...       ...               ...          ...    ...    ...
 29996             59993            0  29996  69996
 29997             59993            0  29997  69997
 29998             59997            0  29998  69998
 29999             59997            0  29999  69999
 30000             60001            0  30000  70000 */
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  int len = 4000;

  std::vector<int> a1(len);
  std::iota(a1.begin(), a1.end(), 1);

  std::vector<int> b1(len);
  std::iota(b1.begin(), b1.end(), len / 2 + 1);

  std::vector<int64_t> seqNum(len);
  std::iota(seqNum.begin(), seqNum.end(), 1);
  std::transform(seqNum.begin(), seqNum.end(), seqNum.begin(), [](int x) {
    return 2 * x;
  });

  std::vector<int8_t> valueKind(
      len, static_cast<int8_t>(PaimonRowKind::INSERT));

  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {a1, b1},
      seqNum,
      valueKind);

  std::vector<int> a2(len);
  std::iota(a2.begin(), a2.end(), len / 2 + 1);

  std::vector<int> b2(len);
  std::iota(b2.begin(), b2.end(), 5 * len / 2 + 1);

  std::vector<int64_t> seqNum2(len);
  std::iota(seqNum2.begin(), seqNum2.end(), len / 2 + 1);
  std::transform(seqNum2.begin(), seqNum2.end(), seqNum2.begin(), [](int x) {
    return x % 2 ? (2 * x - 1) : (2 * x + 1);
  });

  auto fileRowType2 = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor,
      {0},
      rowType,
      {a2, b2},
      seqNum2,
      valueKind);

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "false"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      fileRowType,
      tableParameters);

  auto assignments = getIdentityAssignment(fileRowType);

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

  std::vector<int> a3(3 * len / 2);
  std::iota(a3.begin(), a3.end(), 1);

  std::vector<int> b3(3 * len / 2);
  for (int i = 0; i < 3 * len / 2; i++) {
    if (i < len / 2) {
      b3[i] = i + 1 + len / 2;
    } else if (i >= len) {
      b3[i] = 4 * len / 2 + 1 + i;
    } else if (i % 2 == 0) {
      b3[i] = i + 1 + len / 2;
    } else {
      b3[i] = 4 * len / 2 + 1 + i;
    }
  }

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      rowType,
      BufferPtr(nullptr),
      3 * len / 2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>(a3),
          vectorMaker_.flatVector<int32_t>(b3)});

  auto combined = BaseVector::create(rowType, 0, leafPool_.get());

  while (auto result = readTask->next()) {
    combined->append(result.get());
  }

  bytedance::bolt::test::assertEqualVectors(expected, combined);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

} // namespace
} // namespace bytedance::bolt::dwio::paimon::test
