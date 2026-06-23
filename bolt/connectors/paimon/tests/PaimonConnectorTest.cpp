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
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonConnectorSplit.h"
#include "bolt/connectors/paimon/PaimonTableHandle.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/TimestampConversion.h"
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

    // Run create_test_tables.py with the temporary directory.
    // PAIMON_TEST_SCRIPT_DIR is set by ctest via the test's ENVIRONMENT
    // property; fall back to relative path when running binary directly.
    std::string scriptPath;
    const char* envDir = std::getenv("PAIMON_TEST_SCRIPT_DIR");
    if (envDir && envDir[0] != '\0') {
      scriptPath = std::string(envDir) + "/create_test_tables.py";
    } else {
      scriptPath = "./bolt/connectors/paimon/tests/create_test_tables.py";
    }
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
      contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
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
static std::vector<std::shared_ptr<connector::ConnectorSplit>> makePaimonSplits(
    const std::string& tablePath,
    const std::shared_ptr<BoltPaimonMemoryPool>& paimonPool,
    const std::unordered_map<std::string, std::string>& extraOptions = {}) {
  ::paimon::ScanContextBuilder contextBuilder(tablePath);
  contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local");
  for (const auto& [key, value] : extraOptions) {
    contextBuilder.AddOption(key, value);
  }
  auto scanContext = contextBuilder.Finish().value();
  auto tableScan = ::paimon::TableScan::Create(std::move(scanContext)).value();
  auto paimonPlan = tableScan->CreatePlan().value();

  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits =
      paimonPlan->Splits();
  std::vector<std::shared_ptr<connector::ConnectorSplit>> result;
  result.reserve(paimonSplits.size());
  for (auto& ps : paimonSplits) {
    auto serialized = ::paimon::Split::Serialize(ps, paimonPool).value();
    result.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", serialized.data(), serialized.length()));
  }
  return result;
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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
