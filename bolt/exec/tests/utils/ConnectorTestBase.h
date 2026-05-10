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
 * Copyright (c) International Business Machines Corporation
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by International Business Machines Corporation.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/ConnectorNames.h"
#include "bolt/connectors/ConnectorObjectFactory.h"
#include "bolt/connectors/ConnectorOptions.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/exec/tests/utils/TempFilePath.h"

DECLARE_string(testing_only_set_scan_exception_mesg_for_prepare);
DECLARE_string(testing_only_set_scan_exception_mesg_for_next);

namespace bytedance::bolt::dwrf {
class Config;
}

namespace bytedance::bolt::exec::test {

// Default connector ID used in tests. Tests that create splits or connector
// objects directly may use connectorId() instead.
constexpr std::string_view kHiveConnectorId = "test-hive";

// Column type values for use with makeColumnHandle's "columnType" option.
// These match the connector's internal ColumnType enum ordering.
constexpr int kColumnTypePartitionKey = 0;
constexpr int kColumnTypeRegular = 1;
constexpr int kColumnTypeSynthesized = 2;
constexpr int kColumnTypeRowIndex = 6;

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

/// Base class for connector-related tests. Takes a connector name and ID so
/// tests can be parameterized over different connectors. Registers the named
/// connector and its object factory in SetUp(). Tests should use
/// connectorObjectFactory() to create connector objects directly.
class ConnectorTestBase : public OperatorTestBase {
 public:
  /// @param connectorName  Connector type name (e.g. "hive", "tpch").
  /// @param connectorId    Connector instance ID registered with the runtime.
  explicit ConnectorTestBase(
      std::string connectorName = connector::kHiveConnectorName,
      std::string connectorId = std::string(kHiveConnectorId));

  virtual ~ConnectorTestBase();

  void SetUp() override;
  void TearDown() override;

  /// Re-creates the connector with a custom config (e.g. to test config knobs).
  void resetConnector(const std::shared_ptr<const config::ConfigBase>& config);

  void writeToFile(const std::string& filePath, RowVectorPtr vector);

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::shared_ptr<dwrf::Config> config = nullptr);

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector);

  using OperatorTestBase::assertQuery;

  /// Assumes plan has a single TableScan node.
  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql);

  static std::vector<std::shared_ptr<TempFilePath>> makeFilePaths(int count);

  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeConnectorSplits(
      const std::shared_ptr<TempDirectoryPath>& directoryPath,
      dwio::common::FileFormat format = dwio::common::FileFormat::DWRF) const;

  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeConnectorSplits(
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths) const;

  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeConnectorSplits(
      const std::string& directoryPath,
      dwio::common::FileFormat format = dwio::common::FileFormat::DWRF) const;

  /// Builds and returns the connector splits from the list of files with one
  /// split per each file.
  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeConnectorSplits(
      const std::vector<std::filesystem::path>& filePaths,
      dwio::common::FileFormat format = dwio::common::FileFormat::DWRF) const;

  /// Split file at path 'filePath' into 'splitCount' splits using this
  /// fixture's connector object factory.
  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeConnectorSplits(
      const std::string& filePath,
      uint32_t splitCount,
      dwio::common::FileFormat format) const;

  /// Static variant for non-fixture contexts.
  static std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeConnectorSplits(
      const std::string& connectorName,
      const std::string& filePath,
      uint32_t splitCount,
      dwio::common::FileFormat format);

  std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max()) const;

  /// Like makeConnectorSplit(path, start, length), but merges the given options
  /// into the factory call. Adds fileFormat=DWRF if not already specified in
  /// options.
  std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
      const std::string& filePath,
      uint64_t start,
      uint64_t length,
      connector::DynamicConnectorOptions options) const;

  std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
      const std::string& filePath,
      int64_t fileSize,
      int64_t fileModifiedTime,
      uint64_t start,
      uint64_t length) const;

  /// Creates a simple regular column handle (no required subfields) via the
  /// connector object factory.
  std::shared_ptr<connector::ColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& type) const;

  /// Creates a default table handle (no subfield filters) via the connector
  /// object factory. Optionally accepts a remaining filter expression.
  std::shared_ptr<connector::ConnectorTableHandle> makeTableHandle(
      const std::string& tableName = "hive_table",
      const core::TypedExprPtr& remainingFilter = nullptr) const;

  /// Returns the ConnectorObjectFactory registered for this test's connector.
  std::shared_ptr<connector::ConnectorObjectFactory> connectorObjectFactory()
      const;

  /// Returns the connector instance ID (e.g. "test-hive").
  const std::string& connectorId() const;

  /// Returns the connector type name (e.g. "hive").
  const std::string& connectorName() const;

 protected:
  std::string connectorName_{connector::kHiveConnectorName};
  std::string connectorId_{kHiveConnectorId};
};

} // namespace bytedance::bolt::exec::test
