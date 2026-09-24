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

#include "bolt/connectors/paimon/PaimonConnector.h"
#include <folly/Conv.h>
#include <folly/Subprocess.h>
#include <folly/json.h>
#include <gtest/gtest.h>
#include <paimon/defs.h>
#include <paimon/format/file_format_factory.h>
#include <paimon/scan_context.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/plan.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_scan.h>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include "bolt/common/config/Config.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonBoltFileSystem.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonConnectorSplit.h"
#include "bolt/connectors/paimon/PaimonDataSource.h"
#include "bolt/connectors/paimon/PaimonFilterTranslator.h"
#include "bolt/connectors/paimon/PaimonTableHandle.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/Type.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#ifdef BOLT_ENABLE_ORC
#include "bolt/connectors/paimon/PaimonOrcReader.h"
#endif

namespace bytedance::bolt::connector::paimon {

namespace {

std::string pythonExecutableForTests() {
  const char* python = std::getenv("PAIMON_TEST_PYTHON");
  return python != nullptr && python[0] != '\0' ? python : "python3";
}

} // namespace

class PaimonConnectorTest
    : public bytedance::bolt::exec::test::OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    filesystems::registerLocalFileSystem();

    // Create a temporary directory for the test
    tempDir_ = exec::test::TempDirectoryPath::create();
    LOG(INFO) << "Test using temporary directory: " << tempDir_->path;

    // Run create_test_tables.py with the temporary directory.
    // PAIMON_TEST_SCRIPT_DIR and PAIMON_TEST_PYTHON are set by ctest via the
    // test's ENVIRONMENT property. Fall back to paths suitable for running the
    // binary directly from the repository root.
    std::string scriptPath;
    const char* envDir = std::getenv("PAIMON_TEST_SCRIPT_DIR");
    if (envDir && envDir[0] != '\0') {
      scriptPath = std::string(envDir) + "/create_test_tables.py";
    } else {
      scriptPath = "./bolt/connectors/paimon/tests/create_test_tables.py";
    }
    folly::Subprocess process(
        {pythonExecutableForTests(), scriptPath, "--base-path", tempDir_->path},
        folly::Subprocess::Options().usePath());
    const auto status = process.wait();
    CHECK(status.exited() && status.exitStatus() == 0)
        << "Failed to create test tables: " << status.str();
    exec::test::OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    tempDir_.reset();
    exec::test::OperatorTestBase::TearDownTestCase();
  }

  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    // Register the Paimon connector factory
    connector::registerConnectorFactory(
        std::make_shared<PaimonConnectorFactory>());

    // Create and register a connector instance
    auto factory = connector::getConnectorFactory(
        PaimonConnectorFactory::kPaimonConnectorName);
    auto connector = factory->newConnector(
        "paimon_test",
        std::shared_ptr<const config::ConfigBase>{},
        driverExecutor_.get());
    connector::registerConnector(connector);
  }

  void TearDown() override {
    connector::unregisterConnector("paimon_test");
    connector::unregisterConnectorFactory(
        PaimonConnectorFactory::kPaimonConnectorName);
    exec::test::OperatorTestBase::TearDown();
  }

  static std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
};

std::shared_ptr<exec::test::TempDirectoryPath> PaimonConnectorTest::tempDir_ =
    nullptr;

std::vector<std::shared_ptr<PaimonConnectorSplit>> makeConnectorSplits(
    const std::shared_ptr<::paimon::Plan>& paimonPlan,
    const std::shared_ptr<::paimon::MemoryPool>& paimonPool) {
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  const auto paimonSplits = paimonPlan->Splits();
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (const auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }
  return paimonConnectorSplits;
}

TEST_F(PaimonConnectorTest, TestTableScanBasic) {
  // Create Parquet data with unique id
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  auto schema = ROW({"id"}, {BIGINT()});

  const int64_t kRows = 3;
  std::vector<int64_t> ids(kRows);
  std::iota(ids.begin(), ids.end(), 1);
  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  std::vector<VectorPtr> children{idVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();
  std::unique_ptr<::paimon::TableScan> tableScan =
      ::paimon::TableScan::Create(std::move(scanContext)).value();
  std::shared_ptr<::paimon::Plan> paimonPlan = tableScan->CreatePlan().value();

  // Define schema and handles
  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, ReturnedVectorsCanOutliveReaderEof) {
  EnsurePaimonBoltFileSystemRegistered();

  auto rootPool =
      memory::memoryManager()->addRootPool("PaimonDataSourceLifetimeTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto connectorPool = rootPool->addAggregateChild("connector");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  ::paimon::ScanContextBuilder contextBuilder(tablePath);
  auto scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();
  auto tableScan = ::paimon::TableScan::Create(std::move(scanContext)).value();
  auto paimonPlan = tableScan->CreatePlan().value();

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());
  auto sessionProperties = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});
  auto queryCtx = std::make_shared<connector::ConnectorQueryCtx>(
      leafPool.get(),
      connectorPool.get(),
      sessionProperties.get(),
      nullptr,
      nullptr,
      nullptr,
      nullptr,
      "query.PaimonDataSourceLifetimeTest",
      "task.PaimonDataSourceLifetimeTest",
      "planNodeId.PaimonDataSourceLifetimeTest",
      0);

  PaimonDataSource dataSource(
      rowType,
      tableHandle,
      columnHandles,
      queryCtx,
      core::QueryConfig({}),
      std::make_shared<PaimonConfig>(sessionProperties));

  for (const auto& split : makeConnectorSplits(paimonPlan, paimonPool)) {
    dataSource.addSplit(split);
  }

  ContinueFuture future;
  std::vector<RowVectorPtr> outputs;
  for (;;) {
    auto next = dataSource.next(1, future);
    ASSERT_TRUE(next.has_value());
    if (!next.value()) {
      break;
    }
    outputs.push_back(std::move(next).value());
  }

  ASSERT_FALSE(outputs.empty());
  outputs.clear();
}

TEST_F(
    PaimonConnectorTest,
    SessionPropertiesOverrideConnectorConfigWithBaseFallback) {
  EnsurePaimonBoltFileSystemRegistered();

  auto rootPool =
      memory::memoryManager()->addRootPool("PaimonConnectorSessionConfigTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto connectorPool = rootPool->addAggregateChild("connector");

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_session_config_test",
      "test_table",
      "file:" + tempDir_->path + "/test_db.db/basic",
      std::unordered_map<std::string, std::string>{});

  auto baseConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {PaimonConfig::kReadBatchSize, "invalid-base-value"}});
  PaimonConnector paimonConnector(
      "paimon_session_config_test", baseConfig, driverExecutor_.get());

  auto overriddenSession = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {PaimonConfig::kReadBatchSize, "1024"}});
  auto overriddenQueryCtx = std::make_shared<connector::ConnectorQueryCtx>(
      leafPool.get(),
      connectorPool.get(),
      overriddenSession.get(),
      nullptr,
      nullptr,
      nullptr,
      nullptr,
      "query.PaimonConnectorSessionConfigTest.override",
      "task.PaimonConnectorSessionConfigTest.override",
      "planNodeId.PaimonConnectorSessionConfigTest.override",
      0);
  EXPECT_NO_THROW(paimonConnector.createDataSource(
      rowType,
      tableHandle,
      columnHandles,
      overriddenQueryCtx,
      core::QueryConfig({})));

  auto emptySession = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});
  auto fallbackQueryCtx = std::make_shared<connector::ConnectorQueryCtx>(
      leafPool.get(),
      connectorPool.get(),
      emptySession.get(),
      nullptr,
      nullptr,
      nullptr,
      nullptr,
      "query.PaimonConnectorSessionConfigTest.fallback",
      "task.PaimonConnectorSessionConfigTest.fallback",
      "planNodeId.PaimonConnectorSessionConfigTest.fallback",
      0);
  EXPECT_THROW(
      paimonConnector.createDataSource(
          rowType,
          tableHandle,
          columnHandles,
          fallbackQueryCtx,
          core::QueryConfig({})),
      folly::ConversionError);
}

TEST_F(PaimonConnectorTest, QueryConfigOverridesPaimonDataSourceReadDefaults) {
  const auto mergedConnectorConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {PaimonConfig::kNaturalReadSize, "10485760"},
          {PaimonConfig::kCoalesceReads, "true"},
          {PaimonConfig::kReadTimestampUnit, "3"}});
  const PaimonConfig paimonConfig(mergedConnectorConfig);
  const core::QueryConfig queryConfig({
      {PaimonConfig::kNaturalReadSize, "20971520"},
      {PaimonConfig::kCoalesceReads, "false"},
      {PaimonConfig::kReadTimestampUnit, "6"},
  });

  const auto readOptions =
      resolvePaimonDataSourceReadOptions(queryConfig, paimonConfig);

  EXPECT_EQ(readOptions.naturalReadSize, 20971520);
  EXPECT_FALSE(readOptions.coalesceReads);
  EXPECT_EQ(readOptions.readTimestampUnit, 6);

  const auto fallbackOptions =
      resolvePaimonDataSourceReadOptions(core::QueryConfig({}), paimonConfig);
  EXPECT_EQ(fallbackOptions.naturalReadSize, 10485760);
  EXPECT_TRUE(fallbackOptions.coalesceReads);
  EXPECT_EQ(fallbackOptions.readTimestampUnit, 3);
}

TEST_F(
    PaimonConnectorTest,
    QueryConfigOverrideDoesNotParseMalformedConnectorFallback) {
  const auto malformedConnectorConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {PaimonConfig::kNaturalReadSize, "invalid-size"},
          {PaimonConfig::kCoalesceReads, "invalid-bool"},
          {PaimonConfig::kReadTimestampUnit, "invalid-unit"}});
  const PaimonConfig paimonConfig(malformedConnectorConfig);
  const core::QueryConfig queryConfig({
      {PaimonConfig::kNaturalReadSize, "20971520"},
      {PaimonConfig::kCoalesceReads, "false"},
      {PaimonConfig::kReadTimestampUnit, "6"},
  });

  PaimonDataSourceReadOptions readOptions;
  EXPECT_NO_THROW(
      readOptions =
          resolvePaimonDataSourceReadOptions(queryConfig, paimonConfig));
  EXPECT_EQ(readOptions.naturalReadSize, 20971520);
  EXPECT_FALSE(readOptions.coalesceReads);
  EXPECT_EQ(readOptions.readTimestampUnit, 6);
}

TEST_F(PaimonConnectorTest, TestTableScanAppendOnlyMultipleAppend) {
  // Create Parquet data with unique id
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  auto schema = ROW({"id"}, {BIGINT()});

  const int64_t kRows = 6;
  std::vector<int64_t> ids(kRows);
  std::iota(ids.begin(), ids.end(), 4);
  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  std::vector<VectorPtr> children{idVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/append_only_multiple_append";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();
  std::unique_ptr<::paimon::TableScan> tableScan =
      ::paimon::TableScan::Create(std::move(scanContext)).value();
  std::shared_ptr<::paimon::Plan> paimonPlan = tableScan->CreatePlan().value();

  // Define schema and handles
  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, TestTableScanPkNoOverwrite) {
  // Create Parquet data with unique id
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  auto schema = ROW({"id"}, {BIGINT()});

  const int64_t kRows = 6;
  std::vector<int64_t> ids(kRows);
  std::iota(ids.begin(), ids.end(), 10);
  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  std::vector<VectorPtr> children{idVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/pk_no_overwrite";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();
  std::unique_ptr<::paimon::TableScan> tableScan =
      ::paimon::TableScan::Create(std::move(scanContext)).value();
  std::shared_ptr<::paimon::Plan> paimonPlan = tableScan->CreatePlan().value();

  // Define schema and handles
  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, TestTableScanPkWithOverwrite) {
  // PK table with overlapping keys across two writes.
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  const int64_t kRows = 15;
  std::vector<int64_t> ids(kRows);
  std::vector<int64_t> values(kRows);
  std::iota(ids.begin(), ids.end(), 0);
  for (int i = 0; i < kRows; i++) {
    if (i < 5) {
      values[i] = 2 * i;
    } else {
      values[i] = 3 * i;
    }
  }
  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  auto valueVec = mk.flatVector<int64_t>(values);
  std::vector<VectorPtr> children{idVec, valueVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/pk_with_overwrite";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();
  std::unique_ptr<::paimon::TableScan> tableScan =
      ::paimon::TableScan::Create(std::move(scanContext)).value();
  std::shared_ptr<::paimon::Plan> paimonPlan = tableScan->CreatePlan().value();

  // Define schema and handles
  auto rowType = ROW({"id", "value"}, {BIGINT(), BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["value"] =
      std::make_shared<PaimonColumnHandle>("value", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0, c1 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, TestTableScanDataEvolution) {
  // Create Parquet data with unique id and value
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  auto schema =
      ROW({"id", "value", "length"}, {BIGINT(), VARCHAR(), INTEGER()});

  const int64_t kRows = 3;
  std::vector<int64_t> ids = {1, 2, 3};
  std::vector<std::string> values = {"apple", "banana", "cherry"};
  std::vector<int32_t> lengths = {5, 6, 6};
  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  auto valueVec = mk.flatVector<std::string>(values);
  auto lengthVec = mk.flatVector<int32_t>(lengths);
  std::vector<VectorPtr> children{idVec, valueVec, lengthVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/data_evolution";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .AddOption(::paimon::Options::ROW_TRACKING_ENABLED, "true")
          .AddOption(::paimon::Options::DATA_EVOLUTION_ENABLED, "true")
          .Finish()
          .value();
  std::unique_ptr<::paimon::TableScan> tableScan =
      ::paimon::TableScan::Create(std::move(scanContext)).value();
  std::shared_ptr<::paimon::Plan> paimonPlan = tableScan->CreatePlan().value();
  LOG(INFO) << "Table scan type name: " << typeid(tableScan).name();

  // Define schema and handles
  auto rowType =
      ROW({"id", "value", "length"}, {BIGINT(), VARCHAR(), INTEGER()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["value"] =
      std::make_shared<PaimonColumnHandle>("value", VARCHAR());
  columnHandles["length"] =
      std::make_shared<PaimonColumnHandle>("length", INTEGER());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(
          {{::paimon::Options::ROW_TRACKING_ENABLED, "true"},
           {::paimon::Options::DATA_EVOLUTION_ENABLED, "true"}}));

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0, c1, c2 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, TestTableScanPartialUpdate) {
  // Create expected data
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType =
      ROW({"id", "name", "age", "salary"},
          {BIGINT(), VARCHAR(), INTEGER(), DOUBLE()});

  const int64_t kRows = 3;
  std::vector<int64_t> ids = {1, 2, 3};
  std::vector<std::string> names = {"Alice", "Bob", "Charlie"};
  std::vector<int32_t> ages = {30, 35, 40};
  std::vector<double> salaries = {55000.0, 60000.0, 75000.0};

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  auto nameVec = mk.flatVector<std::string>(names);
  auto ageVec = mk.flatVector<int32_t>(ages);
  auto salaryVec = mk.flatVector<double>(salaries);
  std::vector<VectorPtr> children{idVec, nameVec, ageVec, salaryVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/partial_update";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();
  std::unique_ptr<::paimon::TableScan> tableScan =
      ::paimon::TableScan::Create(std::move(scanContext)).value();
  std::shared_ptr<::paimon::Plan> paimonPlan = tableScan->CreatePlan().value();

  // Define schema and handles
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["name"] =
      std::make_shared<PaimonColumnHandle>("name", VARCHAR());
  columnHandles["age"] = std::make_shared<PaimonColumnHandle>("age", INTEGER());
  columnHandles["salary"] =
      std::make_shared<PaimonColumnHandle>("salary", DOUBLE());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0, c1, c2, c3 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, TestTableScanAggregate) {
  // Create expected data
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id", "sales", "price"}, {BIGINT(), BIGINT(), DOUBLE()});

  const int64_t kRows = 3;
  std::vector<int64_t> ids = {1, 2, 3};
  std::vector<int32_t> sales = {3, 3, 3};
  std::vector<double> prices = {15.0, 20.0, 25.0};

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  auto salesVec = mk.flatVector<int32_t>(sales);
  auto priceVec = mk.flatVector<double>(prices);
  std::vector<VectorPtr> children{idVec, salesVec, priceVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/aggregate";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();
  auto tableScanResult = ::paimon::TableScan::Create(std::move(scanContext));
  BOLT_CHECK(
      tableScanResult.ok(),
      "Failed to create table scan: {}",
      tableScanResult.status().ToString());
  const auto& tableScan = tableScanResult.value();
  const auto& scanPlanResult = tableScan->CreatePlan();
  BOLT_CHECK(
      scanPlanResult.ok(),
      "Failed to create plan: {}",
      scanPlanResult.status().ToString());
  std::shared_ptr<::paimon::Plan> paimonPlan = scanPlanResult.value();

  // Define schema and handles
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["sales"] =
      std::make_shared<PaimonColumnHandle>("sales", BIGINT());
  columnHandles["price"] =
      std::make_shared<PaimonColumnHandle>("price", DOUBLE());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0, c1, c2 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, TestTableScanDeduplicate) {
  // Create expected data
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType =
      ROW({"id", "value", "timestamp"}, {BIGINT(), VARCHAR(), BIGINT()});

  std::vector<int64_t> ids = {1, 2, 3, 4};
  std::vector<std::string> values = {
      "v1", "v2_updated", "v3_updated", "v4_updated"};
  std::vector<int64_t> timestamps = {2500, 2500, 3500, 4500};

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto idVec = mk.flatVector<int64_t>(ids);
  auto valueVec = mk.flatVector<std::string>(values);
  auto timestampVec = mk.flatVector<int64_t>(timestamps);
  std::vector<VectorPtr> children{idVec, valueVec, timestampVec};
  auto rowVec = mk.rowVector(children);

  // Build table path using the temporary directory
  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/deduplicate";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt")
          .Finish()
          .value();

  auto tableScanResult = ::paimon::TableScan::Create(std::move(scanContext));
  BOLT_CHECK(
      tableScanResult.ok(),
      "Failed to create table scan: {}",
      tableScanResult.status().ToString());
  const auto& paimonPlanResult = tableScanResult.value()->CreatePlan();
  BOLT_CHECK(
      paimonPlanResult.ok(),
      "Failed to create plan: {}",
      paimonPlanResult.status().ToString());
  std::shared_ptr<::paimon::Plan> paimonPlan = paimonPlanResult.value();

  // Define schema and handles
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["value"] =
      std::make_shared<PaimonColumnHandle>("value", VARCHAR());
  columnHandles["timestamp"] =
      std::make_shared<PaimonColumnHandle>("timestamp", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0, c1, c2 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
  for (auto& paimonSplit : paimonSplits) {
    const auto serialized =
        ::paimon::Split::Serialize(paimonSplit, paimonPool).value();
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

// ===========================================================================
// Filter Pushdown E2E Tests
// ===========================================================================
//
// These tests verify end-to-end filter pushdown: a filter TypedExpr is placed
// on the PaimonTableHandle, translated by PaimonFilterTranslator into a
// paimon::Predicate, pushed down into the Parquet reader via ReadContext,
// and the returned rows match the expected filtered result.
//

/// Helper: build splits for a given table path.
static std::vector<std::string> makeSerializedPaimonSplits(
    const std::string& tablePath,
    const std::shared_ptr<BoltPaimonMemoryPool>& paimonPool,
    const std::unordered_map<std::string, std::string>& extraOptions = {});

static std::vector<std::shared_ptr<connector::ConnectorSplit>> makePaimonSplits(
    const std::string& tablePath,
    const std::shared_ptr<BoltPaimonMemoryPool>& paimonPool,
    const std::unordered_map<std::string, std::string>& extraOptions = {}) {
  const auto serializedSplits =
      makeSerializedPaimonSplits(tablePath, paimonPool, extraOptions);
  std::vector<std::shared_ptr<connector::ConnectorSplit>> result;
  result.reserve(serializedSplits.size());
  for (const auto& serialized : serializedSplits) {
    result.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }
  return result;
}

static std::vector<std::string> makeSerializedPaimonSplits(
    const std::string& tablePath,
    const std::shared_ptr<BoltPaimonMemoryPool>& paimonPool,
    const std::unordered_map<std::string, std::string>& extraOptions) {
  ::paimon::ScanContextBuilder contextBuilder(tablePath);
  contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "bolt");
  for (const auto& [key, value] : extraOptions) {
    contextBuilder.AddOption(key, value);
  }
  auto scanContext = contextBuilder.Finish().value();
  auto tableScan = ::paimon::TableScan::Create(std::move(scanContext)).value();
  const auto paimonPlan = tableScan->CreatePlan().value();

  std::vector<std::string> result;
  const auto paimonSplits = paimonPlan->Splits();
  result.reserve(paimonSplits.size());
  for (const auto& split : paimonSplits) {
    result.push_back(::paimon::Split::Serialize(split, paimonPool).value());
  }
  return result;
}

static void
appendBigEndian(std::string& output, uint64_t value, size_t byteCount) {
  for (size_t i = byteCount; i > 0; --i) {
    output.push_back(static_cast<char>(value >> ((i - 1) * 8)));
  }
}

static uint64_t
readBigEndian(const std::string& input, size_t offset, size_t byteCount) {
  uint64_t value = 0;
  for (size_t i = 0; i < byteCount; ++i) {
    value = (value << 8) | static_cast<unsigned char>(input[offset + i]);
  }
  return value;
}

static std::string withDeletionFile(
    const std::string& split,
    const std::string& path,
    int64_t offset,
    int64_t length,
    int64_t cardinality) {
  // A v8 DataSplit ends with an empty data-deletion-file list followed by
  // isStreaming and rawConvertible. Replace that empty list with the public
  // DeletionFile::SerializeList representation for its sole data file.
  std::string result = split.substr(0, split.size() - 3);
  result.push_back(1);
  appendBigEndian(result, 1, 4);
  result.push_back(1);
  appendBigEndian(result, path.size(), 2);
  result.append(path);
  appendBigEndian(result, offset, 8);
  appendBigEndian(result, length, 8);
  appendBigEndian(result, cardinality, 8);
  result.append(split, split.size() - 2, 2);
  return result;
}

static std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
serializedAppendColumnHandles() {
  return {
      {"id", std::make_shared<PaimonColumnHandle>("id", BIGINT())},
      {"score", std::make_shared<PaimonColumnHandle>("score", BIGINT())},
      {"label", std::make_shared<PaimonColumnHandle>("label", VARCHAR())},
  };
}

static size_t countFilesWithExtension(
    const std::filesystem::path& directory,
    const std::string& extension) {
  size_t count = 0;
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(directory)) {
    if (entry.is_regular_file() && entry.path().extension() == extension) {
      ++count;
    }
  }
  return count;
}

#ifdef BOLT_ENABLE_ORC
static std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
serializedEvolutionColumnHandles() {
  return {
      {"id", std::make_shared<PaimonColumnHandle>("id", BIGINT())},
      {"score", std::make_shared<PaimonColumnHandle>("score", BIGINT())},
      {"label", std::make_shared<PaimonColumnHandle>("label", VARCHAR())},
      {"rank", std::make_shared<PaimonColumnHandle>("rank", INTEGER())},
  };
}

TEST_F(PaimonConnectorTest, SerializedOrcAppendTableQueries) {
  auto format = ::paimon::FileFormatFactory::Get("orc", {});
  ASSERT_TRUE(format.ok()) << format.status().ToString();
  EXPECT_NE(dynamic_cast<PaimonOrcReader*>(format.value().get()), nullptr);

  auto rootPool = memory::memoryManager()->addRootPool("SerializedOrcScan");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  bytedance::bolt::test::VectorMaker mk(leafPool.get());

  const std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/serialized_append_orc";
  EXPECT_EQ(
      countFilesWithExtension(
          tempDir_->path + "/test_db.db/serialized_append_orc", ".orc"),
      2);
  const auto connectorSplits = makePaimonSplits(
      tablePath,
      paimonPool,
      {{::paimon::Options::SOURCE_SPLIT_TARGET_SIZE, "1B"}});
  ASSERT_GE(connectorSplits.size(), 2);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "serialized_append_orc",
      tablePath,
      std::unordered_map<std::string, std::string>{});
  const auto columnHandles = serializedAppendColumnHandles();
  const auto fullType =
      ROW({"id", "score", "label"}, {BIGINT(), BIGINT(), VARCHAR()});
  auto fullPlan = exec::test::PlanBuilder()
                      .tableScan(fullType, tableHandle, columnHandles)
                      .planNode();
  auto fullExpected = mk.rowVector(
      {mk.flatVector<int64_t>({1, 2, 3, 4, 5, 6}),
       mk.flatVector<int64_t>({10, 20, 30, 40, 50, 60}),
       mk.flatVector<std::string>(
           {"alpha", "beta", "gamma", "delta", "epsilon", "zeta"})});
  exec::test::AssertQueryBuilder(fullPlan)
      .splits(connectorSplits)
      .assertResults(fullExpected);

  const auto labelType = ROW({"label"}, {VARCHAR()});
  auto projectionPlan = exec::test::PlanBuilder()
                            .tableScan(labelType, tableHandle, columnHandles)
                            .planNode();
  auto projectionExpected = mk.rowVector({mk.flatVector<std::string>(
      {"alpha", "beta", "gamma", "delta", "epsilon", "zeta"})});
  exec::test::AssertQueryBuilder(projectionPlan)
      .splits(connectorSplits)
      .assertResults(projectionExpected);

  auto filteredHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "serialized_append_orc",
      tablePath,
      std::unordered_map<std::string, std::string>{},
      parseExpr("id >= 5", fullType));
  const auto filteredType = ROW({"id", "label"}, {BIGINT(), VARCHAR()});
  auto filteredPlan =
      exec::test::PlanBuilder()
          .tableScan(filteredType, filteredHandle, columnHandles)
          .planNode();
  auto filteredExpected = mk.rowVector(
      {mk.flatVector<int64_t>({5, 6}),
       mk.flatVector<std::string>({"epsilon", "zeta"})});
  exec::test::AssertQueryBuilder(filteredPlan)
      .splits(connectorSplits)
      .assertResults(filteredExpected);

  auto emptyHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "serialized_append_orc",
      tablePath,
      std::unordered_map<std::string, std::string>{},
      parseExpr("id > 100", fullType));
  auto emptyPlan = exec::test::PlanBuilder()
                       .tableScan(labelType, emptyHandle, columnHandles)
                       .planNode();
  exec::test::AssertQueryBuilder(emptyPlan)
      .splits(connectorSplits)
      .assertEmptyResults();

  auto countPlan = exec::test::PlanBuilder()
                       .tableScan(
                           ROW({}, {}),
                           tableHandle,
                           std::unordered_map<
                               std::string,
                               std::shared_ptr<connector::ColumnHandle>>{})
                       .singleAggregation({}, {"count(1)"})
                       .planNode();
  auto countExpected = mk.rowVector({mk.flatVector<int64_t>({6})});
  exec::test::AssertQueryBuilder(countPlan)
      .splits(connectorSplits)
      .assertResults(countExpected);

  auto filteredCountHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "serialized_append_orc",
      tablePath,
      std::unordered_map<std::string, std::string>{},
      parseExpr("id >= 5", fullType));
  auto filteredCountPlan =
      exec::test::PlanBuilder()
          .tableScan(ROW({}, {}), filteredCountHandle, columnHandles)
          .singleAggregation({}, {"count(1)"})
          .planNode();
  auto filteredCountExpected = mk.rowVector({mk.flatVector<int64_t>({2})});
  exec::test::AssertQueryBuilder(filteredCountPlan)
      .splits(connectorSplits)
      .assertResults(filteredCountExpected);
}

TEST_F(PaimonConnectorTest, SerializedOrcPrimaryKeyUpdatesRemoveOldVersions) {
  auto rootPool =
      memory::memoryManager()->addRootPool("SerializedOrcPrimaryKeyScan");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  bytedance::bolt::test::VectorMaker mk(leafPool.get());

  const std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/serialized_pk_orc";
  EXPECT_EQ(
      countFilesWithExtension(
          tempDir_->path + "/test_db.db/serialized_pk_orc", ".orc"),
      3);
  const auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  ASSERT_FALSE(connectorSplits.empty());

  const auto rowType =
      ROW({"id", "score", "label"}, {BIGINT(), BIGINT(), VARCHAR()});
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "serialized_pk_orc",
      tablePath,
      std::unordered_map<std::string, std::string>{});
  auto plan =
      exec::test::PlanBuilder()
          .tableScan(rowType, tableHandle, serializedAppendColumnHandles())
          .orderBy({"id"}, false)
          .planNode();
  auto expected = mk.rowVector(
      {mk.flatVector<int64_t>({1, 2, 3, 4, 5, 6}),
       mk.flatVector<int64_t>({10, 200, 30, 400, 50, 60}),
       mk.flatVector<std::string>(
           {"one", "two-updated", "three", "four-updated", "five", "six"})});
  exec::test::AssertQueryBuilder(plan)
      .splits(connectorSplits)
      .assertResults(expected);
}

TEST_F(
    PaimonConnectorTest,
    SerializedOrcSchemaEvolutionMapsAddedAndReorderedFields) {
  auto rootPool =
      memory::memoryManager()->addRootPool("SerializedOrcSchemaEvolution");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  bytedance::bolt::test::VectorMaker mk(leafPool.get());

  const std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/serialized_evolution_orc";
  EXPECT_EQ(
      countFilesWithExtension(
          tempDir_->path + "/test_db.db/serialized_evolution_orc", ".orc"),
      2);
  const auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  ASSERT_FALSE(connectorSplits.empty());

  const auto rowType =
      ROW({"score", "rank", "id", "label"},
          {BIGINT(), INTEGER(), BIGINT(), VARCHAR()});
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "serialized_evolution_orc",
      tablePath,
      std::unordered_map<std::string, std::string>{});
  auto plan =
      exec::test::PlanBuilder()
          .tableScan(rowType, tableHandle, serializedEvolutionColumnHandles())
          .orderBy({"id"}, false)
          .planNode();
  auto expected = mk.rowVector(
      {mk.flatVector<int64_t>({10, 20, 30, 40}),
       makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt, 1, 2}),
       mk.flatVector<int64_t>({1, 2, 3, 4}),
       mk.flatVector<std::string>({"alpha", "beta", "gamma", "delta"})});
  exec::test::AssertQueryBuilder(plan)
      .splits(connectorSplits)
      .assertResults(expected);
}

#endif

TEST_F(
    PaimonConnectorTest,
    SerializedDeletionVectorsPreservePhysicalPositionsWithPredicateAndRowTracking) {
  auto rootPool =
      memory::memoryManager()->addRootPool("SerializedDeletionVector");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  bytedance::bolt::test::VectorMaker mk(leafPool.get());

  const std::vector<std::string> formats{
      "parquet",
#ifdef BOLT_ENABLE_ORC
      "orc",
#endif
  };
  for (const auto& format : formats) {
    SCOPED_TRACE(format);
    const std::string tableName = "serialized_dv_" + format;
    const std::string tablePath =
        "file:" + tempDir_->path + "/test_db.db/" + tableName;
    const std::filesystem::path localDeletionVectorPath =
        tempDir_->path + "/test_db.db/" + tableName + "/index/serialized-dv-0";
    const std::string deletionVectorPath =
        "file:" + localDeletionVectorPath.string();
    EXPECT_EQ(
        countFilesWithExtension(
            tempDir_->path + "/test_db.db/" + tableName, "." + format),
        1);
    ASSERT_TRUE(std::filesystem::exists(localDeletionVectorPath));
    const auto deletionVectorSize =
        std::filesystem::file_size(localDeletionVectorPath);
    ASSERT_GT(deletionVectorSize, 9);
    ASSERT_LE(
        deletionVectorSize - 9,
        static_cast<uintmax_t>(std::numeric_limits<int32_t>::max()));
    ASSERT_LE(
        deletionVectorPath.size(),
        static_cast<size_t>(std::numeric_limits<uint16_t>::max()));

    const auto serializedSplits =
        makeSerializedPaimonSplits(tablePath, paimonPool);
    ASSERT_EQ(serializedSplits.size(), 1);
    const auto& serializedSplit = serializedSplits.front();
    ASSERT_GE(serializedSplit.size(), 15);
    // The public C++ API exposes Split serialization but no DataSplit builder.
    // Verify the Java-compatible v8 wire layout before replacing its empty data
    // deletion-file list with the documented one-file representation.
    ASSERT_EQ(readBigEndian(serializedSplit, 8, 4), 8);
    ASSERT_EQ(serializedSplit[serializedSplit.size() - 3], '\0');
    auto originalSplit = ::paimon::Split::Deserialize(
        serializedSplit.data(), serializedSplit.size(), paimonPool);
    ASSERT_TRUE(originalSplit.ok()) << originalSplit.status().ToString();
    auto originalDataSplit =
        std::dynamic_pointer_cast<::paimon::DataSplit>(originalSplit.value());
    ASSERT_NE(originalDataSplit, nullptr);
    ASSERT_EQ(originalDataSplit->GetFileList().size(), 1);
    ASSERT_EQ(originalDataSplit->GetFileList().front().row_count, 9);

    const auto splitWithDeletionFile = withDeletionFile(
        serializedSplit,
        deletionVectorPath,
        /*offset=*/1,
        /*length=*/deletionVectorSize - 9,
        /*cardinality=*/3);
    auto patchedSplit = ::paimon::Split::Deserialize(
        splitWithDeletionFile.data(), splitWithDeletionFile.size(), paimonPool);
    ASSERT_TRUE(patchedSplit.ok()) << patchedSplit.status().ToString();
    std::vector<std::shared_ptr<connector::ConnectorSplit>> connectorSplits;
    connectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test",
        splitWithDeletionFile.data(),
        splitWithDeletionFile.size()));

    auto columnHandles = serializedAppendColumnHandles();
    columnHandles.emplace(
        "_ROW_ID", std::make_shared<PaimonColumnHandle>("_ROW_ID", BIGINT()));
    const auto fullType =
        ROW({"id", "score", "label"}, {BIGINT(), BIGINT(), VARCHAR()});
    auto tableHandle = std::make_shared<PaimonTableHandle>(
        "paimon_test",
        tableName,
        tablePath,
        std::unordered_map<std::string, std::string>{});
    auto allRowsPlan =
        exec::test::PlanBuilder()
            .tableScan(ROW({"id"}, {BIGINT()}), tableHandle, columnHandles)
            .planNode();
    auto allRowsExpected =
        mk.rowVector({mk.flatVector<int64_t>({0, 2, 3, 5, 6, 8})});
    exec::test::AssertQueryBuilder(allRowsPlan)
        .connectorSessionProperty(
            "paimon_test", PaimonConfig::kReadBatchSize, "3")
        .splits(connectorSplits)
        .assertResults(allRowsExpected);

    auto filteredTableHandle = std::make_shared<PaimonTableHandle>(
        "paimon_test",
        tableName,
        tablePath,
        std::unordered_map<std::string, std::string>{},
        parseExpr("id >= 7", fullType));
    auto filteredPlan =
        exec::test::PlanBuilder()
            .tableScan(
                ROW({"label"}, {VARCHAR()}), filteredTableHandle, columnHandles)
            .planNode();
    auto filteredExpected =
        mk.rowVector({mk.flatVector<std::string>({"row-8"})});
    for (const auto& predicateEnabled : {"true", "false"}) {
      exec::test::AssertQueryBuilder(filteredPlan)
          .connectorSessionProperty(
              "paimon_test", PaimonConfig::kReadBatchSize, "3")
          .connectorSessionProperty(
              "paimon_test",
              PaimonConfig::kPredicateFilterEnabled,
              predicateEnabled)
          .splits(connectorSplits)
          .assertResults(filteredExpected);
    }
    // Row IDs are reconstructed by Paimon from each output row's physical
    // position, including after predicate pushdown and DV remove rows.
    auto rowIdPlan = exec::test::PlanBuilder()
                         .tableScan(
                             ROW({"_ROW_ID", "label"}, {BIGINT(), VARCHAR()}),
                             filteredTableHandle,
                             columnHandles)
                         .planNode();
    exec::test::AssertQueryBuilder(rowIdPlan)
        .connectorSessionProperty(
            "paimon_test", PaimonConfig::kReadBatchSize, "3")
        .splits(connectorSplits)
        .assertResults(mk.rowVector(
            {mk.flatVector<int64_t>({8}),
             mk.flatVector<std::string>({"row-8"})}));
    // Without a DV, row tracking still requires original physical positions.
    auto unmodifiedSplits = makePaimonSplits(tablePath, paimonPool);
    exec::test::AssertQueryBuilder(rowIdPlan)
        .connectorSessionProperty(
            "paimon_test", PaimonConfig::kReadBatchSize, "3")
        .splits(unmodifiedSplits)
        .assertResults(mk.rowVector(
            {mk.flatVector<int64_t>({7, 8}),
             mk.flatVector<std::string>({"row-7", "row-8"})}));
    auto sparseHandle = std::make_shared<PaimonTableHandle>(
        "paimon_test",
        tableName,
        tablePath,
        std::unordered_map<std::string, std::string>{},
        parseExpr("id >= 2 AND id <= 6", fullType));
    auto sparsePlan = exec::test::PlanBuilder()
                          .tableScan(
                              ROW({"_ROW_ID", "label"}, {BIGINT(), VARCHAR()}),
                              sparseHandle,
                              columnHandles)
                          .planNode();
    exec::test::AssertQueryBuilder(sparsePlan)
        .connectorSessionProperty(
            "paimon_test", PaimonConfig::kReadBatchSize, "9")
        .splits(connectorSplits)
        .assertResults(mk.rowVector(
            {mk.flatVector<int64_t>({2, 3, 5, 6}),
             mk.flatVector<std::string>(
                 {"row-2", "row-3", "row-5", "row-6"})}));
    auto countPlan =
        exec::test::PlanBuilder()
            .tableScan(ROW({}, {}), filteredTableHandle, columnHandles)
            .singleAggregation({}, {"count(1)"})
            .planNode();
    exec::test::AssertQueryBuilder(countPlan)
        .connectorSessionProperty(
            "paimon_test", PaimonConfig::kReadBatchSize, "3")
        .splits(connectorSplits)
        .assertResults(mk.rowVector({mk.flatVector<int64_t>({1})}));
  }
}

TEST_F(
    PaimonConnectorTest,
    CrossColumnOrPreservesFilterOnlyColumnsWithPredicateFilteringEnabledAndDisabled) {
  auto rootPool =
      memory::memoryManager()->addRootPool("DisabledPredicateResidual");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  bytedance::bolt::test::VectorMaker mk(leafPool.get());

  const auto filterType =
      ROW({"id", "score", "label"}, {BIGINT(), BIGINT(), VARCHAR()});
  const auto outputType = ROW({"label"}, {VARCHAR()});
  const auto filterExpr = parseExpr("id = 2 OR score = 50", filterType);
  const auto translated =
      PaimonFilterTranslator::translate(filterExpr, filterType);
  ASSERT_TRUE(translated.ok()) << translated.reason;
  const auto typed =
      PaimonFilterTranslator::toTypedExpr(translated.value, leafPool.get());
  ASSERT_TRUE(typed.ok()) << typed.reason;
  EXPECT_TRUE(PaimonFilterTranslator::toSubfieldFilters(typed.value).empty());

  const auto expected =
      mk.rowVector({mk.flatVector<std::string>({"beta", "epsilon"})});
  const auto columnHandles = serializedAppendColumnHandles();
  const std::vector<std::string> formats{
      "parquet",
#ifdef BOLT_ENABLE_ORC
      "orc",
#endif
  };
  for (const auto& format : formats) {
    SCOPED_TRACE(format);
    const std::string tableName = "serialized_append_" + std::string(format);
    const std::string tablePath =
        "file:" + tempDir_->path + "/test_db.db/" + tableName;
    const auto connectorSplits = makePaimonSplits(
        tablePath,
        paimonPool,
        {{::paimon::Options::SOURCE_SPLIT_TARGET_SIZE, "1B"}});
    ASSERT_GE(connectorSplits.size(), 2);

    auto tableHandle = std::make_shared<PaimonTableHandle>(
        "paimon_test",
        tableName,
        tablePath,
        std::unordered_map<std::string, std::string>{},
        filterExpr);
    auto plan = exec::test::PlanBuilder()
                    .tableScan(outputType, tableHandle, columnHandles)
                    .planNode();
    for (const auto predicateFilteringEnabled : {"true", "false"}) {
      SCOPED_TRACE(predicateFilteringEnabled);
      exec::test::AssertQueryBuilder(plan)
          .connectorSessionProperty(
              "paimon_test",
              PaimonConfig::kPredicateFilterEnabled,
              predicateFilteringEnabled)
          .splits(connectorSplits)
          .assertResults(expected);
    }
  }
}

TEST_F(PaimonConnectorTest, TablePropertyCannotOverrideBoltFileSystem) {
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>{
          {::paimon::Options::FILE_SYSTEM, "local"}});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(plan, connectorSplits, "SELECT c0 FROM tmp");
}

TEST_F(PaimonConnectorTest, FilterPushdownIntEquality) {
  // Table "basic": id = [1, 2, 3]
  // Filter: id = 2  → expect [2]
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto filterExpr = parseExpr("id = 2", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  // All rows for DuckDB baseline
  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 = 2");
}

TEST_F(PaimonConnectorTest, UnsupportedFilterIsAppliedAsRemainingFilter) {
  // Table "basic": id = [1, 2, 3]
  // Filter: id + 1 = 3 cannot be translated into a paimon Predicate, but it
  // must still be evaluated by the scan as a remaining filter.
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto filterExpr = parseExpr("id + 1 = 3", rowType);
  EXPECT_FALSE(PaimonFilterTranslator::translate(filterExpr, rowType).ok());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 + 1 = 3");
}

TEST_F(
    PaimonConnectorTest,
    UnsupportedFilterCanReadNonOutputColumnAsRemainingFilter) {
  // Table "partial_update": ids [1, 2, 3], names [Alice, Bob, Charlie].
  // The scan outputs only name, while the remaining filter references id.
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto filterRowType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  auto outputType = ROW({"name"}, {VARCHAR()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["name"] =
      std::make_shared<PaimonColumnHandle>("name", VARCHAR());
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/partial_update";
  auto filterExpr = parseExpr("id + 1 = 3", filterRowType);
  EXPECT_FALSE(
      PaimonFilterTranslator::translate(filterExpr, filterRowType).ok());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(outputType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector(
      {mk.flatVector<int64_t>({1, 2, 3}),
       mk.flatVector<std::string>({"Alice", "Bob", "Charlie"})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(plan, connectorSplits, "SELECT c1 FROM tmp WHERE c0 + 1 = 3");
}

TEST_F(PaimonConnectorTest, PartiallyTranslatableFilterKeepsRemainingFilter) {
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto filterExpr = parseExpr("id > 1 AND id + 1 = 3", rowType);
  EXPECT_FALSE(PaimonFilterTranslator::translate(filterExpr, rowType).ok());
  EXPECT_TRUE(
      PaimonFilterTranslator::translate(parseExpr("id > 1", rowType), rowType)
          .ok());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(
      plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 > 1 AND c0 + 1 = 3");
}

TEST_F(PaimonConnectorTest, FilterPushdownIntGreaterThan) {
  // Table "basic": id = [1, 2, 3]
  // Filter: id > 1  → expect [2, 3]
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto filterExpr = parseExpr("id > 1", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 > 1");
}

TEST_F(PaimonConnectorTest, FilterPushdownIntLessThanOrEqual) {
  // Table "basic": id = [1, 2, 3]
  // Filter: id <= 2  → expect [1, 2]
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto filterExpr = parseExpr("id <= 2", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 <= 2");
}

TEST_F(PaimonConnectorTest, FilterPushdownAndTwoColumns) {
  // Table "basic": id = [1, 2, 3]
  // Filter: id > 0 AND id < 3  → expect [1, 2]
  // Tests AND pushdown with two conditions on the SAME column.
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto filterExpr = parseExpr("id > 0 AND id < 3", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(
      plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 > 0 AND c0 < 3");
}

TEST_F(PaimonConnectorTest, FilterPushdownStringEquality) {
  // Table "data_evolution":
  //   id=[1,2,3], value=["apple","banana","cherry"], length=[5,6,6]
  // Filter: value = 'banana'  → expect row id=2
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType =
      ROW({"id", "value", "length"}, {BIGINT(), VARCHAR(), INTEGER()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["value"] =
      std::make_shared<PaimonColumnHandle>("value", VARCHAR());
  columnHandles["length"] =
      std::make_shared<PaimonColumnHandle>("length", INTEGER());

  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/data_evolution";
  auto filterExpr = parseExpr("value = 'banana'", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(
          {{::paimon::Options::ROW_TRACKING_ENABLED, "true"},
           {::paimon::Options::DATA_EVOLUTION_ENABLED, "true"}}),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({
      mk.flatVector<int64_t>({1, 2, 3}),
      mk.flatVector<std::string>({"apple", "banana", "cherry"}),
      mk.flatVector<int32_t>({5, 6, 6}),
  });
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(
      tablePath,
      paimonPool,
      {{::paimon::Options::ROW_TRACKING_ENABLED, "true"},
       {::paimon::Options::DATA_EVOLUTION_ENABLED, "true"}});
  assertQuery(
      plan, connectorSplits, "SELECT c0, c1, c2 FROM tmp WHERE c1 = 'banana'");
}

TEST_F(PaimonConnectorTest, FilterPushdownOrOfEquals) {
  // Table "pk_no_overwrite": id = [10, 11, 12, 13, 14, 15]
  // Filter: id = 11 OR id = 13 OR id = 15  → expect [11, 13, 15]
  // Tests OR pushdown (OR-of-EQUAL on same column is a supported pattern).
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/pk_no_overwrite";
  auto filterExpr = parseExpr("id = 11 OR id = 13 OR id = 15", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows =
      mk.rowVector({mk.flatVector<int64_t>({10, 11, 12, 13, 14, 15})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(
      plan,
      connectorSplits,
      "SELECT c0 FROM tmp WHERE c0 = 11 OR c0 = 13 OR c0 = 15");
}

TEST_F(PaimonConnectorTest, FilterPushdownNotEqual) {
  // Table "basic": id = [1, 2, 3]
  // Filter: id <> 2  → expect [1, 3]
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto filterExpr = parseExpr("id <> 2", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 <> 2");
}

TEST_F(PaimonConnectorTest, FilterPushdownBetween) {
  // Table "append_only_multiple_append": id = [4, 5, 6, 7, 8, 9]
  // Filter: id BETWEEN 5 AND 8  → expect [5, 6, 7, 8]
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/append_only_multiple_append";
  auto filterExpr = parseExpr("id BETWEEN 5 AND 8", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({mk.flatVector<int64_t>({4, 5, 6, 7, 8, 9})});
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQuery(
      plan, connectorSplits, "SELECT c0 FROM tmp WHERE c0 BETWEEN 5 AND 8");
}

TEST_F(PaimonConnectorTest, FilterPushdownAggregateTable) {
  // Table "aggregate": id=[1,2,3], sales=[3,3,3], price=[15.0,20.0,25.0]
  // Filter: price > 15.0  → expect ids 2,3
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id", "sales", "price"}, {BIGINT(), BIGINT(), DOUBLE()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["sales"] =
      std::make_shared<PaimonColumnHandle>("sales", BIGINT());
  columnHandles["price"] =
      std::make_shared<PaimonColumnHandle>("price", DOUBLE());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/aggregate";
  auto filterExpr = parseExpr("price > 15.0", rowType);

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>(),
      filterExpr);

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, false)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  auto allRows = mk.rowVector({
      mk.flatVector<int64_t>({1, 2, 3}),
      mk.flatVector<int64_t>({3, 3, 3}),
      mk.flatVector<double>({15.0, 20.0, 25.0}),
  });
  createDuckDbTable("tmp", {allRows});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  assertQueryOrdered(
      plan,
      connectorSplits,
      "SELECT c0, c1, c2 FROM tmp WHERE c2 > 15.0 ORDER BY c0",
      std::vector<uint32_t>{0});
}

// ===========================================================================
// End-to-end config tests: prefetch & multi-thread row-to-batch
// ===========================================================================

TEST_F(PaimonConnectorTest, PrefetchEnabledReturnsCorrectResults) {
  // Scan the "basic" table (id = [1, 2, 3]) with prefetch enabled via
  // query config overrides.
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>{});

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  createDuckDbTable("tmp", {mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})})});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  exec::test::CursorParameters params;
  params.planNode = plan;
  params.queryConfigs = {
      {PaimonConfig::kPrefetchEnabled, "true"},
      {PaimonConfig::kPrefetchBatchCount, "100"},
      {PaimonConfig::kPrefetchMaxParallel, "2"},
  };
  exec::test::assertQuery(
      params,
      [&](exec::Task* task) {
        for (auto& split : connectorSplits) {
          task->addSplit("0", exec::Split(std::move(split)));
        }
        task->noMoreSplits("0");
      },
      "SELECT c0 FROM tmp ORDER BY c0",
      duckDbQueryRunner_,
      std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, MultiThreadRowToBatchReturnsCorrectResults) {
  // Scan with multi-thread row-to-batch enabled via query config overrides.
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/append_only_multiple_append";
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>{});

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  std::vector<int64_t> ids(6);
  std::iota(ids.begin(), ids.end(), 4);
  createDuckDbTable("tmp", {mk.rowVector({mk.flatVector<int64_t>(ids)})});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  exec::test::CursorParameters params;
  params.planNode = plan;
  params.queryConfigs = {
      {PaimonConfig::kMultiThreadRowToBatch, "true"},
      {PaimonConfig::kRowToBatchThreadNum, "2"},
  };
  exec::test::assertQuery(
      params,
      [&](exec::Task* task) {
        for (auto& split : connectorSplits) {
          task->addSplit("0", exec::Split(std::move(split)));
        }
        task->noMoreSplits("0");
      },
      "SELECT c0 FROM tmp ORDER BY c0",
      duckDbQueryRunner_,
      std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, PrefetchAndMultiThreadCombined) {
  // Enable both prefetch and multi-thread row-to-batch via query config
  // overrides.
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/pk_no_overwrite";
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>{});

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  std::vector<int64_t> ids(6);
  std::iota(ids.begin(), ids.end(), 10);
  createDuckDbTable("tmp", {mk.rowVector({mk.flatVector<int64_t>(ids)})});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  exec::test::CursorParameters params;
  params.planNode = plan;
  params.queryConfigs = {
      {PaimonConfig::kPrefetchEnabled, "true"},
      {PaimonConfig::kPrefetchBatchCount, "100"},
      {PaimonConfig::kPrefetchMaxParallel, "2"},
      {PaimonConfig::kMultiThreadRowToBatch, "true"},
      {PaimonConfig::kRowToBatchThreadNum, "2"},
  };
  exec::test::assertQuery(
      params,
      [&](exec::Task* task) {
        for (auto& split : connectorSplits) {
          task->addSplit("0", exec::Split(std::move(split)));
        }
        task->noMoreSplits("0");
      },
      "SELECT c0 FROM tmp ORDER BY c0",
      duckDbQueryRunner_,
      std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, CustomNaturalReadSizeReturnsCorrectResults) {
  // Set a non-default natural read size via query config and verify it flows
  // through to PaimonReadFile (which uses PaimonIoOptions::naturalReadSize).
  auto rootPool = memory::memoryManager()->addRootPool("Test");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());

  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/basic";
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "test_table",
      tablePath,
      std::unordered_map<std::string, std::string>{});

  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  bytedance::bolt::test::VectorMaker mk(leafPool.get());
  createDuckDbTable("tmp", {mk.rowVector({mk.flatVector<int64_t>({1, 2, 3})})});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);
  exec::test::CursorParameters params;
  params.planNode = plan;
  params.queryConfigs = {
      {PaimonConfig::kNaturalReadSize, "20971520"}, // 20 MB
      {PaimonConfig::kCoalesceReads, "false"},
  };
  exec::test::assertQuery(
      params,
      [&](exec::Task* task) {
        for (auto& split : connectorSplits) {
          task->addSplit("0", exec::Split(std::move(split)));
        }
        task->noMoreSplits("0");
      },
      "SELECT c0 FROM tmp ORDER BY c0",
      duckDbQueryRunner_,
      std::vector<uint32_t>{0});
}

// ---- End-to-end timestamp precision through PlanBuilder
// ----------------------

TEST_F(PaimonConnectorTest, TimestampPrecisionEndToEnd) {
  // Verify kReadTimestampUnit config flows through the full pipeline:
  //   queryConfig → PaimonConfig → PaimonParquetReader → RowReaderOptions
  //
  // Uses the timestamp_precision table (created by create_test_tables.py)
  // which contains nanosecond-precision timestamps. Reading at different
  // kReadTimestampUnit values should produce correctly truncated results.

  auto rootPool = memory::memoryManager()->addRootPool("TsPrecisionE2E");
  auto leafPool = rootPool->addLeafChild("leaf");
  auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool.get());
  bytedance::bolt::test::VectorMaker mk(leafPool.get());

  std::string tablePath =
      "file:" + tempDir_->path + "/test_db.db/timestamp_precision";

  auto rowType = ROW({"id", "ts", "value"}, {BIGINT(), TIMESTAMP(), BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
  columnHandles["ts"] = std::make_shared<PaimonColumnHandle>("ts", TIMESTAMP());
  columnHandles["value"] =
      std::make_shared<PaimonColumnHandle>("value", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test",
      "timestamp_precision",
      tablePath,
      std::unordered_map<std::string, std::string>{});

  auto connectorSplits = makePaimonSplits(tablePath, paimonPool);

  const auto parseTs = [](std::string_view s) {
    return util::fromTimestampString(s.data(), s.size(), nullptr);
  };

  // Expected at millisecond precision: sub-ms digits zeroed.
  auto expectedMilli = mk.rowVector({
      mk.flatVector<int64_t>({1, 2, 3, 4, 5}),
      mk.flatVectorNullable<Timestamp>({
          parseTs("2015-06-01 19:34:56.123000000"),
          parseTs("2023-04-21 09:09:34.567000000"),
          parseTs("2007-12-12 04:27:56.999000000"),
          parseTs("2000-01-01 00:00:00.000000000"),
          parseTs("1999-12-31 23:59:59.999000000"),
      }),
      mk.flatVector<int64_t>({10, 20, 30, 40, 50}),
  });

  // Expected at microsecond precision: sub-us digits preserved.
  auto expectedMicro = mk.rowVector({
      mk.flatVector<int64_t>({1, 2, 3, 4, 5}),
      mk.flatVectorNullable<Timestamp>({
          parseTs("2015-06-01 19:34:56.123456000"),
          parseTs("2023-04-21 09:09:34.567890000"),
          parseTs("2007-12-12 04:27:56.999999000"),
          parseTs("2000-01-01 00:00:00.000001000"),
          parseTs("1999-12-31 23:59:59.999999000"),
      }),
      mk.flatVector<int64_t>({10, 20, 30, 40, 50}),
  });

  // Helper: run a table scan with given kReadTimestampUnit and assert results.
  const auto assertPrecision = [&](const std::string& tsUnit,
                                   const RowVectorPtr& expected) {
    auto plan = exec::test::PlanBuilder()
                    .tableScan(rowType, tableHandle, columnHandles)
                    .planNode();

    createDuckDbTable("tmp", {expected});

    exec::test::CursorParameters params;
    params.planNode = plan;
    params.queryConfigs = {{PaimonConfig::kReadTimestampUnit, tsUnit}};

    exec::test::assertQuery(
        params,
        [&](exec::Task* task) {
          for (auto& split : connectorSplits) {
            task->addSplit("0", exec::Split(std::move(split)));
          }
          task->noMoreSplits("0");
        },
        "SELECT c0, c1, c2 FROM tmp ORDER BY c0",
        duckDbQueryRunner_,
        std::vector<uint32_t>{0, 1, 2});
  };

  // Millisecond precision (default): sub-ms truncated.
  assertPrecision("3", expectedMilli);

  // Microsecond precision: sub-us truncated.
  assertPrecision("6", expectedMicro);
}

} // namespace bytedance::bolt::connector::paimon
