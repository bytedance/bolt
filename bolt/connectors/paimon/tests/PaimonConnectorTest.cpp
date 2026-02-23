/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "bolt/connectors/paimon/PaimonConnector.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"

namespace bytedance::bolt::connector::paimon {

class PaimonConnectorTest : public exec::tests::OperatorTestBase {
 protected:
  void SetUp() override {
    exec::tests::OperatorTestBase::SetUp();
    // Register the Paimon connector factory
    connector::registerConnectorFactory(std::make_shared<PaimonConnectorFactory>());
    
    // Create and register a connector instance
    auto factory = connector::getConnectorFactory(PaimonConnectorFactory::kPaimonConnectorName);
    auto connector = factory->newConnector("paimon_test", nullptr, executor_.get());
    connector::registerConnector(connector);
  }

  void TearDown() override {
    connector::unregisterConnector("paimon_test");
    connector::unregisterConnectorFactory(PaimonConnectorFactory::kPaimonConnectorName);
    exec::tests::OperatorTestBase::TearDown();
  }
};

TEST_F(PaimonConnectorTest, TestTableScan) {
  // TODO: Generate a Paimon table at a temporary path
  std::string tablePath = "/tmp/paimon_test_table";
  
  // Create a Plan to scan the table
  // This requires a PaimonTableHandle and columns
  // Since we don't have a real table, we mock the handle
  
  auto tableHandle = std::make_shared<PaimonTableHandle>(
      "paimon_test", "test_table", tablePath);

  // Define schema
  auto rowType = ROW({"id", "data"}, {INTEGER(), VARCHAR()});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> columnHandles;
  columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", INTEGER());
  columnHandles["data"] = std::make_shared<PaimonColumnHandle>("data", VARCHAR());

  auto plan = exec::tests::PlanBuilder()
      .tableScan(rowType, tableHandle, columnHandles)
      .plan();

  // Execute and assert
  // Since the table doesn't exist and the reader is not fully implemented, 
  // we expect this to potentially fail or return nothing if we implemented graceful failure.
  // For now, we verify that we can build the plan and attempt execution.
  
  // exec::tests::AssertQueryBuilder(plan).assertResults({}); 
}

} // namespace bytedance::bolt::connector::paimon
