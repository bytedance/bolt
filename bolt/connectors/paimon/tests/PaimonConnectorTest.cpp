/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/connectors/paimon/PaimonConnector.h"
#include <fmt/format.h>
#include <folly/init/Init.h>
#include <folly/json.h>
#include <gtest/gtest.h>
#include <paimon/defs.h>
#include <paimon/scan_context.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/plan.h>
#include <paimon/table/source/table_scan.h>
#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/paimon/PaimonConnectorSplit.h"
#include "bolt/connectors/paimon/PaimonTableHandle.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/Type.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

namespace bytedance::bolt::connector::paimon {

class PaimonConnectorTest
    : public bytedance::bolt::exec::test::OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    // Create a temporary directory for the test
    tempDir_ = exec::test::TempDirectoryPath::create();
    LOG(INFO) << "Test using temporary directory: " << tempDir_->path;

    // Run create_test_tables.py with the temporary directory
    std::string scriptPath =
        "./bolt/connectors/paimon/tests/create_test_tables.py";
    std::string command = scriptPath + " --base-path " + tempDir_->path;
    int exitCode = system(command.c_str());
    CHECK_EQ(exitCode, 0) << "Failed to create test tables";
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

TEST_F(PaimonConnectorTest, TestTableScanBasic) {
  // Create Parquet data with unique id
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
    paimonConnectorSplits.push_back(
        std::make_shared<PaimonConnectorSplit>("paimon_test", paimonSplit));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

TEST_F(PaimonConnectorTest, TestTableScanAppendOnlyMultipleAppend) {
  // Create Parquet data with unique id
  auto rootPool = memory::memoryManager()->addRootPool("PaimonConnectorTest");
  auto leafPool = rootPool->addLeafChild("leaf");
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
    paimonConnectorSplits.push_back(
        std::make_shared<PaimonConnectorSplit>("paimon_test", paimonSplit));
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
    paimonConnectorSplits.push_back(
        std::make_shared<PaimonConnectorSplit>("paimon_test", paimonSplit));
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
    paimonConnectorSplits.push_back(
        std::make_shared<PaimonConnectorSplit>("paimon_test", paimonSplit));
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
    paimonConnectorSplits.push_back(
        std::make_shared<PaimonConnectorSplit>("paimon_test", paimonSplit));
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
    paimonConnectorSplits.push_back(
        std::make_shared<PaimonConnectorSplit>("paimon_test", paimonSplit));
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

  auto rowType =
      ROW({"id", "value", "timestamp"}, {BIGINT(), VARCHAR(), BIGINT()});

  const int64_t kRows = 3;
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
    paimonConnectorSplits.push_back(
        std::make_shared<PaimonConnectorSplit>("paimon_test", paimonSplit));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(
      inputSplits.end(),
      paimonConnectorSplits.begin(),
      paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

} // namespace bytedance::bolt::connector::paimon

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
