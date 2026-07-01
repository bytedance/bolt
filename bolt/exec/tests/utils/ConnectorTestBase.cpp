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

#include "bolt/exec/tests/utils/ConnectorTestBase.h"

#include <folly/dynamic.h>
#include <filesystem>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/serialization/Serializable.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/connectors/ConnectorNames.h"
#include "bolt/core/ITypedExpr.h"
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/dwio/dwrf/common/Config.h"
#include "bolt/dwio/dwrf/reader/DwrfReader.h"
#include "bolt/dwio/dwrf/writer/Writer.h"
#include "bolt/exec/tests/utils/ConnectorTestBootstrap.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/Type.h"
#include "bolt/type/filter/FilterBase.h"

namespace bytedance::bolt::exec::test {
namespace {

class ConnectorDataSource final : public connector::DataSource {
 public:
  explicit ConnectorDataSource(std::unique_ptr<connector::DataSource> delegate)
      : delegate_(std::move(delegate)) {
    BOLT_CHECK_NOT_NULL(delegate_);
  }

  void addSplit(std::shared_ptr<connector::ConnectorSplit> split) override {
    delegate_->addSplit(std::move(split));
  }

  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future)
      override {
    BOLT_TEST_ADJUST("bytedance::bolt::connector::DataSource::next", this);
    return delegate_->next(size, future);
  }

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {
    delegate_->addDynamicFilter(outputChannel, filter);
  }

  uint64_t getCompletedBytes() override {
    return delegate_->getCompletedBytes();
  }

  uint64_t getCompletedRows() override {
    return delegate_->getCompletedRows();
  }

  std::vector<uint64_t> getCompletedBytesReads() override {
    return delegate_->getCompletedBytesReads();
  }

  std::vector<uint64_t> getCompletedCntReads() override {
    return delegate_->getCompletedCntReads();
  }

  std::vector<uint64_t> getCompletedScanTimeReads() override {
    return delegate_->getCompletedScanTimeReads();
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return delegate_->runtimeStats();
  }

  bool allPrefetchIssued() const override {
    return delegate_->allPrefetchIssued();
  }

  bool isFinished() const override {
    return delegate_->isFinished();
  }

  void setFromDataSource(
      std::unique_ptr<connector::DataSource> source) override {
    auto* wrapped = dynamic_cast<ConnectorDataSource*>(source.get());
    BOLT_CHECK_NOT_NULL(wrapped, "Expected ConnectorDataSource");
    delegate_->setFromDataSource(std::move(wrapped->delegate_));
  }

  int64_t estimatedRowSize() override {
    return delegate_->estimatedRowSize();
  }

  void close() override {
    delegate_->close();
  }

 private:
  std::unique_ptr<connector::DataSource> delegate_;
};

class ConnectorWrapper final : public connector::Connector {
 public:
  explicit ConnectorWrapper(std::shared_ptr<connector::Connector> delegate)
      : connector::Connector(delegate->connectorId()),
        delegate_(std::move(delegate)) {
    BOLT_CHECK_NOT_NULL(delegate_);
  }

  const std::shared_ptr<const config::ConfigBase>& connectorConfig()
      const override {
    return delegate_->connectorConfig();
  }

  bool canAddDynamicFilter() const override {
    return delegate_->canAddDynamicFilter();
  }

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& queryConfig) override {
    return std::make_unique<ConnectorDataSource>(delegate_->createDataSource(
        outputType,
        tableHandle,
        columnHandles,
        std::move(connectorQueryCtx),
        queryConfig));
  }

  bool supportsSplitPreload() override {
    return delegate_->supportsSplitPreload();
  }

  bool supportsIndexLookup() const override {
    return delegate_->supportsIndexLookup();
  }

  std::shared_ptr<connector::IndexSource> createIndexSource(
      const RowTypePtr& inputType,
      size_t numJoinKeys,
      const std::vector<std::shared_ptr<const core::ITypedExpr>>&
          joinConditions,
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      connector::ConnectorQueryCtx* connectorQueryCtx) override {
    return delegate_->createIndexSource(
        inputType,
        numJoinKeys,
        joinConditions,
        outputType,
        tableHandle,
        columnHandles,
        connectorQueryCtx);
  }

  std::unique_ptr<connector::DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<connector::ConnectorInsertTableHandle>
          connectorInsertTableHandle,
      connector::ConnectorQueryCtx* connectorQueryCtx,
      connector::CommitStrategy commitStrategy,
      const core::QueryConfig& queryConfig) override {
    return delegate_->createDataSink(
        std::move(inputType),
        std::move(connectorInsertTableHandle),
        connectorQueryCtx,
        commitStrategy,
        queryConfig);
  }

  folly::Executor* FOLLY_NULLABLE executor() const override {
    return delegate_->executor();
  }

 private:
  std::shared_ptr<connector::Connector> delegate_;
};

std::shared_ptr<connector::Connector> newConnectorWrapper(
    const std::string& connectorName,
    const std::string& connectorId,
    const std::shared_ptr<const config::ConfigBase>& config,
    folly::Executor* executor) {
  auto delegate = connector::getConnectorFactory(connectorName)
                      ->newConnector(connectorId, config, executor);
  return std::make_shared<ConnectorWrapper>(std::move(delegate));
}

} // namespace

ConnectorTestBase::ConnectorTestBase(
    std::string connectorName,
    std::string connectorId)
    : connectorName_(std::move(connectorName)),
      connectorId_(std::move(connectorId)) {}

ConnectorTestBase::~ConnectorTestBase() = default;

void ConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();
  Type::registerSerDe();
  common::Filter::registerSerDe();
  core::ITypedExpr::registerSerDe();
  registerConnectorTestFactories(connectorName_);
  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  auto connectorFactory = connector::getConnectorFactory(connectorName_);
  connectorFactory->registerObjectFactory(connectorId_);
  connector::registerConnector(newConnectorWrapper(
      connectorName_, connectorId_, emptyConfig, ioExecutor_.get()));
  filesystems::registerLocalFileSystem();
}

void ConnectorTestBase::TearDown() {
  // Make sure all pending loads are finished or cancelled before unregister
  // connector.
  ioExecutor_.reset();
  connector::unregisterConnector(connectorId_);
  connector::unregisterConnectorObjectFactory(connectorName_);
  OperatorTestBase::TearDown();
}

void ConnectorTestBase::resetConnector(
    const std::shared_ptr<const config::ConfigBase>& config) {
  connector::unregisterConnector(connectorId_);
  registerConnectorTestFactories(connectorName_);
  connector::registerConnector(newConnectorWrapper(
      connectorName_, connectorId_, config, ioExecutor_.get()));
}

void ConnectorTestBase::writeToFile(
    const std::string& filePath,
    RowVectorPtr vector) {
  writeToFile(filePath, std::vector{std::move(vector)});
}

void ConnectorTestBase::writeToFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors,
    std::shared_ptr<dwrf::Config> config) {
  bolt::dwrf::WriterOptions options;
  if (config == nullptr) {
    config = std::make_shared<bytedance::bolt::dwrf::Config>();
  }
  options.config = config;
  options.schema = vectors[0]->type();
  auto localWriteFile = std::make_unique<LocalWriteFile>(filePath, true, false);
  auto sink = std::make_unique<dwio::common::WriteFileSink>(
      std::move(localWriteFile), filePath);
  auto childPool = rootPool_->addAggregateChild("ConnectorTestBase.Writer");
  options.memoryPool = childPool.get();
  bytedance::bolt::dwrf::Writer writer{std::move(sink), options};
  for (const auto& vector : vectors) {
    writer.write(vector);
  }
  writer.close();
}

std::vector<RowVectorPtr> ConnectorTestBase::makeVectors(
    const RowTypePtr& rowType,
    int32_t numVectors,
    int32_t rowsPerVector) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        bolt::test::BatchMaker::createBatch(rowType, rowsPerVector, *pool_));
    vectors.push_back(vector);
  }
  return vectors;
}

std::shared_ptr<exec::Task> ConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(
      plan, makeConnectorSplits(filePaths), duckDbSql);
}

std::vector<std::shared_ptr<TempFilePath>> ConnectorTestBase::makeFilePaths(
    int count) {
  std::vector<std::shared_ptr<TempFilePath>> filePaths;
  filePaths.reserve(count);
  for (auto i = 0; i < count; ++i) {
    filePaths.emplace_back(TempFilePath::create());
  }
  return filePaths;
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
ConnectorTestBase::makeConnectorSplits(
    const std::shared_ptr<TempDirectoryPath>& directoryPath,
    dwio::common::FileFormat format) const {
  return makeConnectorSplits(directoryPath->path, format);
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
ConnectorTestBase::makeConnectorSplits(
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths) const {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.reserve(filePaths.size());
  for (const auto& filePath : filePaths) {
    splits.emplace_back(makeConnectorSplit(
        filePath->path,
        filePath->fileSize(),
        filePath->fileModifiedTime(),
        0,
        std::numeric_limits<uint64_t>::max()));
  }
  return splits;
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
ConnectorTestBase::makeConnectorSplits(
    const std::string& directoryPath,
    dwio::common::FileFormat format) const {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;

  for (const auto& path :
       std::filesystem::recursive_directory_iterator(directoryPath)) {
    if (path.is_regular_file()) {
      splits.emplace_back(
          makeConnectorSplits(path.path().string(), 1, format)[0]);
    }
  }

  return splits;
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
ConnectorTestBase::makeConnectorSplits(
    const std::vector<std::filesystem::path>& filePaths,
    dwio::common::FileFormat format) const { // NOLINT
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.reserve(filePaths.size());
  for (const auto& filePath : filePaths) {
    splits.emplace_back(makeConnectorSplits(filePath.string(), 1, format)[0]);
  }
  return splits;
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
ConnectorTestBase::makeConnectorSplits(
    const std::string& filePath,
    uint32_t splitCount,
    dwio::common::FileFormat format) const {
  return makeConnectorSplits(connectorName_, filePath, splitCount, format);
}

/*static*/
std::vector<std::shared_ptr<connector::ConnectorSplit>>
ConnectorTestBase::makeConnectorSplits(
    const std::string& connectorName,
    const std::string& filePath,
    uint32_t splitCount,
    dwio::common::FileFormat format) {
  auto file =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  const int64_t fileSize = file->size();
  // Take the upper bound.
  const auto splitSize =
      static_cast<uint64_t>((fileSize + splitCount - 1) / splitCount);
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.reserve(splitCount);
  auto factory = connector::getConnectorObjectFactory(connectorName);
  const auto effectivePath =
      filePath.find('/') == 0 ? "file:" + filePath : filePath;
  for (uint32_t i = 0; i < splitCount; i++) {
    splits.emplace_back(factory->makeConnectorSplit(
        effectivePath,
        i * splitSize,
        splitSize,
        connector::makeOptions({{"fileFormat", static_cast<int>(format)}})));
  }
  return splits;
}

std::shared_ptr<connector::ConnectorSplit>
ConnectorTestBase::makeConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length) const {
  const auto effectivePath =
      filePath.find('/') == 0 ? "file:" + filePath : filePath;
  return connector::getConnectorObjectFactory(connectorName_)
      ->makeConnectorSplit(
          effectivePath,
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
  const auto effectivePath =
      filePath.find('/') == 0 ? "file:" + filePath : filePath;
  if (!options.options.isObject()) {
    options.options = folly::dynamic::object;
  }
  if (!options.options.count("fileFormat")) {
    options.options["fileFormat"] =
        static_cast<int>(dwio::common::FileFormat::DWRF);
  }
  return connector::getConnectorObjectFactory(connectorName_)
      ->makeConnectorSplit(effectivePath, start, length, options);
}

std::shared_ptr<connector::ConnectorSplit>
ConnectorTestBase::makeConnectorSplit(
    const std::string& filePath,
    int64_t fileSize,
    int64_t fileModifiedTime,
    uint64_t start,
    uint64_t length) const {
  const auto effectivePath =
      filePath.find('/') == 0 ? "file:" + filePath : filePath;
  connector::DynamicConnectorOptions options;
  options.options = folly::dynamic::object;
  options.options["fileFormat"] =
      static_cast<int>(dwio::common::FileFormat::DWRF);
  folly::dynamic infoColumns = folly::dynamic::object;
  infoColumns["$file_size"] = fmt::format("{}", fileSize);
  infoColumns["$file_modified_time"] = fmt::format("{}", fileModifiedTime);
  options.options["infoColumns"] = infoColumns;
  return connector::getConnectorObjectFactory(connectorName_)
      ->makeConnectorSplit(effectivePath, start, length, options);
}

std::shared_ptr<connector::ColumnHandle> ConnectorTestBase::makeColumnHandle(
    const std::string& name,
    const TypePtr& type) const {
  return connectorObjectFactory()->makeColumnHandle(
      name, type, connector::makeOptions({}));
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

std::shared_ptr<connector::ConnectorObjectFactory>
ConnectorTestBase::connectorObjectFactory() const {
  return connector::getConnectorObjectFactory(connectorName_);
}

const std::string& ConnectorTestBase::connectorId() const {
  return connectorId_;
}

const std::string& ConnectorTestBase::connectorName() const {
  return connectorName_;
}

} // namespace bytedance::bolt::exec::test
