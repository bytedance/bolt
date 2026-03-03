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
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/table_scan.h>
#include <paimon/scan_context.h>
#include <paimon/table/source/plan.h>
#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/paimon/PaimonConnectorSplit.h"
#include "bolt/connectors/paimon/PaimonTableHandle.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/type/Type.h"
#include "bolt/vector/tests/utils/VectorMaker.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

namespace bytedance::bolt::connector::paimon {

class PaimonConnectorTest
    : public bytedance::bolt::exec::test::OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    // Create a temporary directory for the test
    tempDir_ = exec::test::TempDirectoryPath::create();
    LOG(INFO) << "Test using temporary directory: " << tempDir_->path;

    // Run create_test_tables.py with the temporary directory
    std::string scriptPath = "./bolt/connectors/paimon/tests/create_test_tables.py";
    std::string command = "python3 " + scriptPath + " --base-path " + tempDir_->path;
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

std::shared_ptr<exec::test::TempDirectoryPath> PaimonConnectorTest::tempDir_ = nullptr;

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
                           contextBuilder
                               .AddOption(::paimon::Options::FILE_SYSTEM, "local")
                               .Finish()
                               .value();
    std::unique_ptr<::paimon::TableScan> tableScan =
                           ::paimon::TableScan::Create(std::move(scanContext)).value();
    std::shared_ptr<::paimon::Plan> paimonPlan =
                           tableScan->CreatePlan().value();
    paimonPlan->Splits();

  // Define schema and handles
  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test", "test_table", tablePath, std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits = paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
for (auto& paimonSplit : paimonSplits) {
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", paimonSplit));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(inputSplits.end(), paimonConnectorSplits.begin(), paimonConnectorSplits.end());
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
  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/append_only_multiple_append";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
                           contextBuilder
                               .AddOption(::paimon::Options::FILE_SYSTEM, "local")
                               .Finish()
                               .value();
    std::unique_ptr<::paimon::TableScan> tableScan =
                           ::paimon::TableScan::Create(std::move(scanContext)).value();
    std::shared_ptr<::paimon::Plan> paimonPlan =
                           tableScan->CreatePlan().value();
    paimonPlan->Splits();

  // Define schema and handles
  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test", "test_table", tablePath, std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits = paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
for (auto& paimonSplit : paimonSplits) {
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", paimonSplit));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(inputSplits.end(), paimonConnectorSplits.begin(), paimonConnectorSplits.end());
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
  std::string tablePath = "file:" + tempDir_->path + "/test_db.db/pk_no_overwrite";

  ::paimon::ScanContextBuilder contextBuilder(tablePath);

  std::unique_ptr<::paimon::ScanContext> scanContext =
                           contextBuilder
                               .AddOption(::paimon::Options::FILE_SYSTEM, "local")
                               .Finish()
                               .value();
    std::unique_ptr<::paimon::TableScan> tableScan =
                           ::paimon::TableScan::Create(std::move(scanContext)).value();
    std::shared_ptr<::paimon::Plan> paimonPlan =
                           tableScan->CreatePlan().value();
    paimonPlan->Splits();

  // Define schema and handles
  auto rowType = ROW({"id"}, {BIGINT()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());

  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test", "test_table", tablePath, std::unordered_map<std::string, std::string>());

  // Build plan with ORDER BY id
  auto plan = exec::test::PlanBuilder()
                  .tableScan(rowType, tableHandle, columnHandles)
                  .orderBy({"id"}, /*isPartial*/ false)
                  .planNode();

  // Prepare DuckDB expected results
  std::vector<RowVectorPtr> rows{rowVec};
  createDuckDbTable("tmp", rows);
  std::string duckSql = "SELECT c0 FROM tmp ORDER BY c0";
  std::vector<std::shared_ptr<::paimon::Split>> paimonSplits = paimonPlan->Splits();
  std::vector<std::shared_ptr<PaimonConnectorSplit>> paimonConnectorSplits;
  paimonConnectorSplits.reserve(paimonSplits.size());
for (auto& paimonSplit : paimonSplits) {
    paimonConnectorSplits.push_back(std::make_shared<PaimonConnectorSplit>(
        "paimon_test", paimonSplit));
  }

  // Assert query correctness and ordering
  std::vector<std::shared_ptr<connector::ConnectorSplit>> inputSplits;
  inputSplits.insert(inputSplits.end(), paimonConnectorSplits.begin(), paimonConnectorSplits.end());
  assertQueryOrdered(plan, inputSplits, duckSql, std::vector<uint32_t>{0});
}

} // namespace bytedance::bolt::connector::paimon

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
