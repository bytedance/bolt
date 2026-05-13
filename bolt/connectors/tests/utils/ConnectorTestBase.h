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

#pragma once

#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/ConnectorObjectFactory.h"
#include "bolt/connectors/ConnectorOptions.h"
#include "bolt/core/ITypedExpr.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/type/Type.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::connector::test {

/// Default connector instance ID used when callers don't supply one.
constexpr std::string_view kDefaultConnectorId = "test-connector";

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

/// Callback invoked from ConnectorTestBase::SetUp() to ensure the named
/// connector factory is registered with the runtime. Each connector library
/// exposes one (e.g. connector::hive::registerHiveConnectorFactories,
/// connector::tpch::registerTpchConnectorFactories).
using FactoryRegistrar = std::function<void()>;

/// Registers the named connector instance with the runtime: invokes
/// @c factoryRegistrar (if non-null and the factory isn't already registered),
/// then registers an object factory under @c connectorId and a Connector
/// constructed via the factory using @c config and @c ioExecutor.
///
/// Reusable outside ConnectorTestBase by callers (e.g. the legacy
/// exec::test::HiveConnectorTestBase) that need the same registration
/// semantics without inheriting this fixture.
void registerTestConnector(
    const std::string& connectorName,
    const std::string& connectorId,
    folly::Executor* ioExecutor,
    const std::shared_ptr<const config::ConfigBase>& config,
    const FactoryRegistrar& factoryRegistrar);

/// Reverses registerTestConnector: unregisters the Connector instance and
/// the object factory.
void unregisterTestConnector(
    const std::string& connectorName,
    const std::string& connectorId);

/// Connector-agnostic GTest fixture for connector-driven tests.
///
/// Lifecycle:
///   SetUp() registers
///   (i) standard serdes (Type, Filter, ITypedExpr),
///   (ii) the connector factory via @c factoryRegistrar (if not already
///   registered),
///   (iii) the connector instance under @c connectorId, and
///   (iv) the local file system.
///   TearDown() reverses (iii) and unregisters the connector object factory.
class ConnectorTestBase : public ::testing::Test,
                          public bolt::test::VectorTestBase {
 public:
  /// @param connectorName     Connector type name (e.g. "hive", "tpch").
  /// @param connectorId       Connector instance ID registered with the
  ///                          runtime.
  /// @param factoryRegistrar  Invoked from SetUp() if @c connectorName is not
  ///                          yet registered. May be null when the test has
  ///                          another way of ensuring registration.
  ConnectorTestBase(
      std::string connectorName,
      std::string connectorId,
      FactoryRegistrar factoryRegistrar);

  ~ConnectorTestBase() override;

  void SetUp() override;
  void TearDown() override;

  /// Re-creates the connector with a custom config (e.g. to test config
  /// knobs). Reuses the existing ioExecutor_.
  void resetConnector(const std::shared_ptr<const config::ConfigBase>& config);

  /// Returns the ConnectorObjectFactory registered for this fixture's
  /// connector.
  std::shared_ptr<connector::ConnectorObjectFactory> connectorObjectFactory()
      const;

  const std::string& connectorId() const {
    return connectorId_;
  }

  const std::string& connectorName() const {
    return connectorName_;
  }

  /// Splits @c filePath into @c splitCount contiguous chunks. All splits use
  /// DWRF unless @c format says otherwise.
  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeConnectorSplits(
      const std::string& filePath,
      uint32_t splitCount,
      dwio::common::FileFormat format = dwio::common::FileFormat::DWRF) const;

  /// Single DWRF split for @c filePath covering the full file.
  std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
      const std::string& filePath) const;

  /// Single DWRF split for @c filePath covering [start, start + length).
  std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max()) const;

  /// Single split for @c filePath with explicit options merged in. Defaults
  /// fileFormat to DWRF if not present in @c options.
  std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
      const std::string& filePath,
      uint64_t start,
      uint64_t length,
      connector::DynamicConnectorOptions options) const;

  /// Regular column handle (no required subfields) via the factory.
  std::shared_ptr<connector::ColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& type) const;

  std::shared_ptr<connector::ColumnHandle> makeColumnHandle(
      const std::string& name,
      const TypePtr& type,
      connector::ConnectorOptions options) const;

  /// Default table handle (no subfield filters), optional remaining filter.
  std::shared_ptr<connector::ConnectorTableHandle> makeTableHandle(
      const std::string& tableName = "test_table",
      const core::TypedExprPtr& remainingFilter = nullptr) const;

 protected:
  std::string connectorName_;
  std::string connectorId_;
  FactoryRegistrar factoryRegistrar_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
};

} // namespace bytedance::bolt::connector::test
