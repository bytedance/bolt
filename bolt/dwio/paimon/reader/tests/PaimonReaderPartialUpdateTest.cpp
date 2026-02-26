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

class PaimonReaderPartialUpdateTest
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

  void createPaimonFile(
      std::string outputDirectoryPath,
      std::shared_ptr<folly::Executor> executor,
      const std::vector<std::string>& primaryKeyNames,
      RowVectorPtr rowVector) {
    auto writerPlanFragment =
        exec::test::PlanBuilder()
            .values({rowVector})
            .orderBy(primaryKeyNames, false)
            .tableWrite(outputDirectoryPath, dwio::common::FileFormat::PARQUET)
            .planFragment();

    auto writeTask = exec::Task::create(
        "my_write_task",
        writerPlanFragment,
        0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kSerial);

    while (auto result = writeTask->next())
      ;
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

const std::string PaimonReaderPartialUpdateTest::kHiveConnectorId = "test-hive";

static std::unordered_map<std::string, RuntimeMetric> getTableScanRuntimeStats(
    const std::shared_ptr<bytedance::bolt::exec::Task>& task) {
  return task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
}

TEST_F(PaimonReaderPartialUpdateTest, nonNullUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_k  k   a      b      c      _SEQUENCE_NUMBER  _VALUE_KIND
    1     1  23.0   10     NULL         1                +I
    1     1  NULL  NULL   'foobar'      2                +I
    1     1  25.2  NULL    NULL         3                +I
  */

  auto rowType = ROW({
      {"_KEY_k", BIGINT()},
      {"k", BIGINT()},
      {"a", DOUBLE()},
      {"b", BIGINT()},
      {"c", VARCHAR()},
      {"_SEQUENCE_NUMBER", BIGINT()},
      {"_VALUE_KIND", TINYINT()},
  });

  bytedance::bolt::test::VectorMaker maker{leafPool_.get()};

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int64_t>({1}),
           maker.flatVector<int64_t>({1}),
           maker.flatVector<double>({23.0}),
           maker.flatVector<int64_t>({10}),
           maker.flatVectorNullable<StringView>({std::nullopt}),
           maker.flatVector<int64_t>({1}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int64_t>({1}),
           maker.flatVector<int64_t>({1}),
           maker.flatVectorNullable<double>({std::nullopt}),
           maker.flatVectorNullable<int64_t>({std::nullopt}),
           maker.flatVector<StringView>({"foobar"}),
           maker.flatVector<int64_t>({2}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int64_t>({1}),
           maker.flatVector<int64_t>({1}),
           maker.flatVector<double>({25.2}),
           maker.flatVectorNullable<int64_t>({std::nullopt}),
           maker.flatVectorNullable<StringView>({std::nullopt}),
           maker.flatVector<int64_t>({3}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kPartialUpdateMergeEngine},
      {connector::paimon::kPrimaryKey, "k"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      rowType,
      tableParameters);

  auto assignments = getIdentityAssignment(rowType);

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

  auto expected = maker.rowVector(
      rowType->names(),
      {maker.flatVector<int64_t>({1}),
       maker.flatVector<int64_t>({1}),
       maker.flatVector<double>({25.2}),
       maker.flatVector<int64_t>({10}),
       maker.flatVector<StringView>({"foobar"}),
       maker.flatVector<int64_t>({3}),
       maker.flatVector<int8_t>({static_cast<int8_t>(PaimonRowKind::INSERT)})});

  int nBatch = 0;
  while (auto result = readTask->next()) {
    nBatch++;
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  BOLT_CHECK(nBatch > 0);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderPartialUpdateTest, sequenceGroupUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_k  k   a      b      g_1  c   d   g_2 _SEQUENCE_NUMBER  _VALUE_KIND
    1     1   1      1      1    1   1    1    1                 +I
    1     1   2      2      2    2   2   null  2                 +I
    1     1   3      3      1    3   3    3    3                 +I
    -----------------------------------------------------------------
    1.    1   2      2      2    3   3    3    3                 +I
  */

  auto rowType = ROW({
      {"_KEY_k", INTEGER()},
      {"k", INTEGER()},
      {"a", INTEGER()},
      {"b", INTEGER()},
      {"g_1", INTEGER()},
      {"c", INTEGER()},
      {"d", INTEGER()},
      {"g_2", INTEGER()},
      {"_SEQUENCE_NUMBER", BIGINT()},
      {"_VALUE_KIND", TINYINT()},
  });

  bytedance::bolt::test::VectorMaker maker{leafPool_.get()};

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int64_t>({1}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int64_t>({2}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),

           maker.flatVector<int64_t>({3}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kPartialUpdateMergeEngine},
      {connector::paimon::kPrimaryKey, "k"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("g_1") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "a,b"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("g_2") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "c,d"}};

  auto rowType2 = ROW(
      {{"k", INTEGER()},
       {"a", INTEGER()},
       {"b", INTEGER()},
       {"g_1", INTEGER()},
       {"c", INTEGER()},
       {"d", INTEGER()},
       {"g_2", INTEGER()}});

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      rowType2,
      tableParameters);

  auto assignments = getIdentityAssignment(rowType2);

  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(rowType2, tableHandle, assignments)
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

  auto expected = maker.rowVector(
      rowType2->names(),
      {maker.flatVector<int32_t>({1}),
       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({3})});

  int nBatch = 0;
  while (auto result = readTask->next()) {
    nBatch++;
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  BOLT_CHECK(nBatch > 0);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderPartialUpdateTest, multipleSequenceGroupUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_k  k   a      b      g_1  c   d   g_2 g_3 _SEQUENCE_NUMBER  _VALUE_KIND
    1     1   1      1      1    1   1    1   1       1                 +I
    1     1   2      2      2    2   2    1  null     2                 +I
    1     1   3      3      1    3   3    3    3      3                 +I
  */

  auto rowType = ROW({
      {"_KEY_k", INTEGER()},
      {"k", INTEGER()},
      {"a", INTEGER()},
      {"b", INTEGER()},
      {"g_1", INTEGER()},
      {"c", INTEGER()},
      {"d", INTEGER()},
      {"g_2", INTEGER()},
      {"g_3", INTEGER()},
      {"_SEQUENCE_NUMBER", BIGINT()},
      {"_VALUE_KIND", TINYINT()},
  });

  bytedance::bolt::test::VectorMaker maker{leafPool_.get()};

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int64_t>({1}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({1}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int64_t>({2}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),

           maker.flatVector<int64_t>({3}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kPartialUpdateMergeEngine},
      {connector::paimon::kPrimaryKey, "k"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("g_1") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "a,b"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("g_2") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "c,d"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      rowType,
      tableParameters);

  auto assignments = getIdentityAssignment(rowType);

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

  auto expected = maker.rowVector(
      rowType->names(),
      {maker.flatVector<int32_t>({1}),
       maker.flatVector<int32_t>({1}),

       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({2}),

       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int64_t>({3}),
       maker.flatVector<int8_t>({static_cast<int8_t>(PaimonRowKind::INSERT)})});

  int nBatch = 0;
  while (auto result = readTask->next()) {
    nBatch++;
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  BOLT_CHECK(nBatch > 0);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderPartialUpdateTest, sequenceGroupAggregateUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_k  k   a      b       c      d    _SEQUENCE_NUMBER  _VALUE_KIND
    1     1   1      1      null  null        1               +I
    1     1  null   null     1      1         2               +I
    1     1   2      2       null  null       3               +I
    1     1  null   null     2       2        4               +I
  */

  auto rowType = ROW({
      {"_KEY_k", INTEGER()},
      {"k", INTEGER()},
      {"a", INTEGER()},
      {"b", INTEGER()},
      {"c", INTEGER()},
      {"d", INTEGER()},
      {"_SEQUENCE_NUMBER", BIGINT()},
      {"_VALUE_KIND", TINYINT()},
  });

  bytedance::bolt::test::VectorMaker maker{leafPool_.get()};

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int64_t>({1}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int64_t>({2}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int64_t>({3}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),

           maker.flatVector<int64_t>({4}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kPartialUpdateMergeEngine},
      {connector::paimon::kPrimaryKey, "k"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("a") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "b"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("b") +
           connector::paimon::kAggregateEngineKeyPostfix,
       "first_value"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("c") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "d"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("d") +
           connector::paimon::kAggregateEngineKeyPostfix,
       "sum"},
  };

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      rowType,
      tableParameters);

  auto assignments = getIdentityAssignment(rowType);

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

  auto expected = maker.rowVector(
      rowType->names(),
      {maker.flatVector<int32_t>({1}),
       maker.flatVector<int32_t>({1}),

       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({1}),
       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({3}),

       maker.flatVector<int64_t>({4}),
       maker.flatVector<int8_t>({static_cast<int8_t>(PaimonRowKind::INSERT)})});

  int nBatch = 0;
  while (auto result = readTask->next()) {
    nBatch++;
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  BOLT_CHECK(nBatch > 0);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderPartialUpdateTest, multipleSequenceGroupAggregateUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_k  k   a      b   g_1    c      g_2     g_3  _SEQUENCE_NUMBER _VALUE_KIND
    1     1   1      1    1    '1'     1        1         1              +I
    1     1   2      2    2    '2'     null     2         2              +I
    1     1   3      3    2    '3'     3        1         3              +I
  */

  auto rowType = ROW({
      {"_KEY_k", INTEGER()},
      {"k", INTEGER()},
      {"a", INTEGER()},
      {"b", INTEGER()},
      {"g_1", INTEGER()},
      {"c", VARCHAR()},
      {"g_2", INTEGER()},
      {"g_3", INTEGER()},
      {"_SEQUENCE_NUMBER", BIGINT()},
      {"_VALUE_KIND", TINYINT()},
  });

  bytedance::bolt::test::VectorMaker maker{leafPool_.get()};

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),

           maker.flatVector<StringView>({"1"}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int64_t>({1}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),

           maker.flatVector<int32_t>({2}),

           maker.flatVector<StringView>({"2"}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVector<int32_t>({2}),

           maker.flatVector<int64_t>({2}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({3}),

           maker.flatVector<int32_t>({2}),

           maker.flatVector<StringView>({"3"}),

           maker.flatVector<int32_t>({3}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int64_t>({3}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kPartialUpdateMergeEngine},
      {connector::paimon::kPrimaryKey, "k"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("a") +
           connector::paimon::kAggregateEngineKeyPostfix,
       "sum"},

      {connector::paimon::kPartialUpdateKeyPrefix + std::string("g_1,g_3") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "a"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("g_2") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "c"},
  };

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      rowType,
      tableParameters);

  auto assignments = getIdentityAssignment(rowType);

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

  auto expected = maker.rowVector(
      rowType->names(),
      {maker.flatVector<int32_t>({1}),
       maker.flatVector<int32_t>({1}),

       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({2}),
       maker.flatVector<StringView>({"3"}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({2}),

       maker.flatVector<int64_t>({3}),
       maker.flatVector<int8_t>({static_cast<int8_t>(PaimonRowKind::INSERT)})});

  int nBatch = 0;
  while (auto result = readTask->next()) {
    nBatch++;
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  BOLT_CHECK(nBatch > 0);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderPartialUpdateTest, sequenceGroupDefaultAggregateUpdate) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /* Merging following
  _KEY_k  k   a      b     c     d   _SEQUENCE_NUMBER   _VALUE_KIND
    1     1   1      1    null  null         1              +I
    1     1   null   null   1    1           2              +I
    1     1   2      2    null  null         3              +I
    1     1   null   null   2    2           4              +I
  */

  auto rowType = ROW({
      {"_KEY_k", INTEGER()},
      {"k", INTEGER()},
      {"a", INTEGER()},
      {"b", INTEGER()},
      {"c", INTEGER()},
      {"d", INTEGER()},
      {"_SEQUENCE_NUMBER", BIGINT()},
      {"_VALUE_KIND", TINYINT()},
  });

  bytedance::bolt::test::VectorMaker maker{leafPool_.get()};

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int64_t>({1}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int64_t>({2}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int64_t>({3}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int32_t>({1}),
           maker.flatVector<int32_t>({1}),

           maker.flatVectorNullable<int32_t>({std::nullopt}),
           maker.flatVectorNullable<int32_t>({std::nullopt}),

           maker.flatVector<int32_t>({2}),
           maker.flatVector<int32_t>({2}),

           maker.flatVector<int64_t>({4}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kPartialUpdateMergeEngine},
      {connector::paimon::kPrimaryKey, "k"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("d") +
           connector::paimon::kAggregateEngineKeyPostfix,
       "sum"},
      {connector::paimon::kPartialUpdateDefaultAggregateFunctionKey, "sum"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("a") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "b"},
      {connector::paimon::kPartialUpdateKeyPrefix + std::string("c") +
           connector::paimon::kPartialUpdateSequenceGroup,
       "d"},
  };

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      rowType,
      tableParameters);

  auto assignments = getIdentityAssignment(rowType);

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

  auto expected = maker.rowVector(
      rowType->names(),
      {maker.flatVector<int32_t>({1}),
       maker.flatVector<int32_t>({1}),

       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({3}),
       maker.flatVector<int32_t>({2}),
       maker.flatVector<int32_t>({3}),

       maker.flatVector<int64_t>({4}),
       maker.flatVector<int8_t>({static_cast<int8_t>(PaimonRowKind::INSERT)})});

  int nBatch = 0;
  while (auto result = readTask->next()) {
    nBatch++;
    bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  BOLT_CHECK(nBatch > 0);
  EXPECT_GT(getTableScanRuntimeStats(readTask)["totalMergeTime"].sum, 0);
}

TEST_F(PaimonReaderPartialUpdateTest, uninitializedMemoryTest) {
  filesystems::registerLocalFileSystem();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  auto tempDir = exec::test::TempDirectoryPath::create();

  /*
   * We want to test that if a column is NEVER updated for a PK, it should be
   * NULL. If memory is uninitialized, it might show up as non-NULL with garbage
   * value.
   *
   * Row Schema: k (PK), v1, v2, v3 (never updated)
   *
   * Input 1: k=1, v1=10, v2=NULL, v3=NULL
   * Input 2: k=1, v1=NULL, v2=20, v3=NULL
   *
   * Expected: k=1, v1=10, v2=20, v3=NULL
   */

  auto rowType = ROW({
      {"k", BIGINT()},
      {"v1", BIGINT()},
      {"v2", BIGINT()},
      {"v3", BIGINT()}, // This column will never be updated
      {"_SEQUENCE_NUMBER", BIGINT()},
      {"_VALUE_KIND", TINYINT()},
  });

  bytedance::bolt::test::VectorMaker maker{leafPool_.get()};

  // File 1: update v1
  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int64_t>({1}),
           maker.flatVector<int64_t>({10}),
           maker.flatVectorNullable<int64_t>({std::nullopt}),
           maker.flatVectorNullable<int64_t>({std::nullopt}),
           maker.flatVector<int64_t>({1}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  // File 2: update v2
  createPaimonFile(
      tempDir->getPath(),
      executor,
      {"k"},
      maker.rowVector(
          rowType->names(),
          {maker.flatVector<int64_t>({1}),
           maker.flatVectorNullable<int64_t>({std::nullopt}),
           maker.flatVector<int64_t>({20}),
           maker.flatVectorNullable<int64_t>({std::nullopt}),
           maker.flatVector<int64_t>({2}),
           maker.flatVector<int8_t>(
               {static_cast<int8_t>(PaimonRowKind::INSERT)})}));

  core::PlanNodeId scanNodeId;

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kPartialUpdateMergeEngine},
      {connector::paimon::kPrimaryKey, "k"}};

  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "hive_table",
      true,
      SubfieldFilters{},
      nullptr,
      rowType,
      tableParameters);

  auto assignments = getIdentityAssignment(rowType);

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

  // Expected: k=1, v1=10, v2=20, v3=NULL
  // Since v3 is never updated, it must remain NULL.
  auto expected = maker.rowVector(
      rowType->names(),
      {maker.flatVector<int64_t>({1}),
       maker.flatVector<int64_t>({10}),
       maker.flatVector<int64_t>({20}),
       maker.flatVectorNullable<int64_t>({std::nullopt}), // v3 MUST be NULL
       maker.flatVector<int64_t>({2}), // Last sequence number
       maker.flatVector<int8_t>({static_cast<int8_t>(PaimonRowKind::INSERT)})});

  int nBatch = 0;
  while (auto result = readTask->next()) {
    nBatch++;
    // Check v3 explicitly to be sure
    auto v3 = result->childAt(3);
    EXPECT_TRUE(v3->isNullAt(0))
        << "v3 should be NULL at index 0, but got: " << v3->toString(0);

    bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  BOLT_CHECK(nBatch > 0);
}

} // namespace
} // namespace bytedance::bolt::dwio::paimon::test
