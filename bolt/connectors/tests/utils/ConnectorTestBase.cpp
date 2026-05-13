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

#include "bolt/connectors/tests/utils/ConnectorTestBase.h"

#include <folly/dynamic.h>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/serialization/Serializable.h"
#include "bolt/type/filter/FilterBase.h"

namespace bytedance::bolt::connector::test {

namespace {

std::string toEffectivePath(const std::string& filePath) {
  return filePath.find('/') == 0 ? "file:" + filePath : filePath;
}

uint64_t fileSize(const std::string& filePath) {
  auto file =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  return static_cast<uint64_t>(file->size());
}

} // namespace

void registerTestConnector(
    const std::string& connectorName,
    const std::string& connectorId,
    folly::Executor* ioExecutor,
    const std::shared_ptr<const config::ConfigBase>& config,
    const FactoryRegistrar& factoryRegistrar) {
  if (factoryRegistrar && !connector::hasConnectorFactory(connectorName)) {
    factoryRegistrar();
  }
  auto factory = connector::getConnectorFactory(connectorName);
  factory->registerObjectFactory(connectorId);
  connector::registerConnector(
      factory->newConnector(connectorId, config, ioExecutor));
}

void unregisterTestConnector(
    const std::string& connectorName,
    const std::string& connectorId) {
  connector::unregisterConnector(connectorId);
  connector::unregisterConnectorObjectFactory(connectorName);
}

ConnectorTestBase::ConnectorTestBase(
    std::string connectorName,
    std::string connectorId,
    FactoryRegistrar factoryRegistrar)
    : connectorName_(std::move(connectorName)),
      connectorId_(std::move(connectorId)),
      factoryRegistrar_(std::move(factoryRegistrar)) {}

ConnectorTestBase::~ConnectorTestBase() = default;

void ConnectorTestBase::SetUp() {
  Type::registerSerDe();
  common::Filter::registerSerDe();
  core::ITypedExpr::registerSerDe();
  ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);
  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  registerTestConnector(
      connectorName_,
      connectorId_,
      ioExecutor_.get(),
      emptyConfig,
      factoryRegistrar_);
  filesystems::registerLocalFileSystem();
}

void ConnectorTestBase::TearDown() {
  // Make sure all pending loads finish or are cancelled before unregistering
  // the connector.
  ioExecutor_.reset();
  unregisterTestConnector(connectorName_, connectorId_);
}

void ConnectorTestBase::resetConnector(
    const std::shared_ptr<const config::ConfigBase>& config) {
  connector::unregisterConnector(connectorId_);
  if (factoryRegistrar_ && !connector::hasConnectorFactory(connectorName_)) {
    factoryRegistrar_();
  }
  connector::registerConnector(
      connector::getConnectorFactory(connectorName_)
          ->newConnector(connectorId_, config, ioExecutor_.get()));
}

std::shared_ptr<connector::ConnectorObjectFactory>
ConnectorTestBase::connectorObjectFactory() const {
  return connector::getConnectorObjectFactory(connectorName_);
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
ConnectorTestBase::makeConnectorSplits(
    const std::string& filePath,
    uint32_t splitCount,
    dwio::common::FileFormat format) const {
  const auto size = fileSize(filePath);
  const auto splitSize =
      static_cast<uint64_t>((size + splitCount - 1) / splitCount);
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.reserve(splitCount);
  auto factory = connectorObjectFactory();
  const auto effectivePath = toEffectivePath(filePath);
  for (uint32_t i = 0; i < splitCount; ++i) {
    splits.emplace_back(factory->makeConnectorSplit(
        effectivePath,
        i * splitSize,
        splitSize,
        connector::makeOptions({{"fileFormat", static_cast<int>(format)}})));
  }
  return splits;
}

std::shared_ptr<connector::ConnectorSplit>
ConnectorTestBase::makeConnectorSplit(const std::string& filePath) const {
  return makeConnectorSplit(filePath, 0, fileSize(filePath));
}

std::shared_ptr<connector::ConnectorSplit>
ConnectorTestBase::makeConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length) const {
  return connectorObjectFactory()->makeConnectorSplit(
      toEffectivePath(filePath),
      start,
      length,
      connector::makeOptions(
          {{"fileFormat",
            static_cast<int>(dwio::common::FileFormat::DWRF)}}));
}

std::shared_ptr<connector::ConnectorSplit>
ConnectorTestBase::makeConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length,
    connector::DynamicConnectorOptions options) const {
  if (!options.options.isObject()) {
    options.options = folly::dynamic::object;
  }
  if (!options.options.count("fileFormat")) {
    options.options["fileFormat"] =
        static_cast<int>(dwio::common::FileFormat::DWRF);
  }
  return connectorObjectFactory()->makeConnectorSplit(
      toEffectivePath(filePath), start, length, options);
}

std::shared_ptr<connector::ColumnHandle> ConnectorTestBase::makeColumnHandle(
    const std::string& name,
    const TypePtr& type) const {
  return connectorObjectFactory()->makeColumnHandle(
      name, type, connector::makeOptions({}));
}

std::shared_ptr<connector::ColumnHandle> ConnectorTestBase::makeColumnHandle(
    const std::string& name,
    const TypePtr& type,
    connector::ConnectorOptions options) const {
  return connectorObjectFactory()->makeColumnHandle(name, type, options);
}

std::shared_ptr<connector::ConnectorTableHandle>
ConnectorTestBase::makeTableHandle(
    const std::string& tableName,
    const core::TypedExprPtr& remainingFilter) const {
  auto tableOptions = connector::makeOptions({});
  if (remainingFilter) {
    tableOptions.options["remainingFilter"] =
        ISerializable::serialize(remainingFilter);
  }
  return connectorObjectFactory()->makeTableHandle(tableName, {}, tableOptions);
}

} // namespace bytedance::bolt::connector::test
