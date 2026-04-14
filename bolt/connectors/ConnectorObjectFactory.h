/*
 * Copyright (c) International Business Machines Corporation
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

#include <shared_mutex>
#include <string>

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/ConnectorOptions.h"

namespace bytedance::bolt::connector {

/// Factory interface for constructing connector-specific objects from a
/// generic, serialized representation (ConnectorOptions). Each connector type
/// registers exactly one ConnectorObjectFactory keyed by its connector name.
/// A factory instance is bound to a specific connector instance via
/// connectorId, which is forwarded to objects such as ConnectorSplit and
/// ConnectorTableHandle that require it.
///
/// Subclasses override the makeXXX methods for the object types they support.
/// Unsupported methods throw by default.
class ConnectorObjectFactory {
 public:
  /// @param name        The connector type name (e.g. "hive").
  /// @param connectorId The id of the connector instance this factory serves.
  explicit ConnectorObjectFactory(std::string name, std::string connectorId)
      : name_(std::move(name)), connectorId_(std::move(connectorId)) {}

  virtual ~ConnectorObjectFactory();

  /// Returns the connector type name this factory is registered under.
  const std::string& connectorName() const {
    return name_;
  }

  /// Returns the connector instance id passed at construction time.
  const std::string& connectorId() const {
    return connectorId_;
  }

  /// Deserializes a ConnectorSplit from @p filePath, @p start, @p length, and
  /// connector-specific fields encoded in @p options.
  virtual std::shared_ptr<ConnectorSplit> makeConnectorSplit(
      const std::string& /*filePath*/,
      uint64_t /*start*/,
      uint64_t /*length*/,
      const ConnectorOptions& /*options*/) const {
    BOLT_UNSUPPORTED(
        "makeConnectorSplit not supported by connector", connectorId_);
  }

  /// Deserializes a ColumnHandle for column @p name with data type @p type and
  /// connector-specific metadata in @p options.
  virtual std::shared_ptr<connector::ColumnHandle> makeColumnHandle(
      const std::string& /*name*/,
      const TypePtr& /*type*/,
      const ConnectorOptions& /*options*/) const {
    BOLT_UNSUPPORTED(
        "makeColumnHandle not supported by connector", connectorId_);
  }

  /// Deserializes a ConnectorTableHandle for @p tableName using the provided
  /// @p columnHandles and connector-specific metadata in @p options.
  virtual std::shared_ptr<ConnectorTableHandle> makeTableHandle(
      const std::string& /*tableName*/,
      const std::vector<
          std::shared_ptr<const connector::ColumnHandle>>& /*columnHandles*/,
      const ConnectorOptions& /*options*/) const {
    BOLT_UNSUPPORTED(
        "makeTableHandle not supported by connector", connectorId_);
  }

  /// Deserializes a ConnectorInsertTableHandle from @p inputColumns,
  /// @p locationHandle, and connector-specific metadata in @p options.
  virtual std::shared_ptr<ConnectorInsertTableHandle> makeInsertTableHandle(
      const std::vector<
          std::shared_ptr<const connector::ColumnHandle>>& /*inputColumns*/,
      const std::shared_ptr<const ConnectorLocationHandle>& /*locationHandle*/,
      const ConnectorOptions& /*options*/) const {
    BOLT_UNSUPPORTED(
        "makeInsertTableHandle not supported by connector", connectorId_);
  }

  /// Deserializes a ConnectorLocationHandle with the given @p tableType and
  /// connector-specific metadata in @p options.
  virtual std::shared_ptr<ConnectorLocationHandle> makeLocationHandle(
      ConnectorLocationHandle::TableType /*tableType*/,
      const ConnectorOptions& /*options*/) const {
    BOLT_UNSUPPORTED(
        "makeLocationHandle not supported by connector", connectorId_);
  }

 private:
  const std::string name_;
  const std::string connectorId_;
};

/// Registers @p factory keyed by its connectorName(). Throws if a factory with
/// the same name is already registered.
bool registerConnectorObjectFactory(
    const std::shared_ptr<ConnectorObjectFactory>& factory);

/// Returns true if a ConnectorObjectFactory is registered for @p connectorName.
bool hasConnectorObjectFactory(const std::string& connectorName);

/// Removes the ConnectorObjectFactory registered under @p connectorName.
/// Returns true if a factory was found and removed.
bool unregisterConnectorObjectFactory(const std::string& connectorName);

/// Returns the ConnectorObjectFactory registered under @p connectorName.
/// Throws if none is registered.
std::shared_ptr<ConnectorObjectFactory> getConnectorObjectFactory(
    const std::string& connectorName);

} // namespace bytedance::bolt::connector
