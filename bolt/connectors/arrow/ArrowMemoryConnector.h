/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/ConnectorNames.h"
#include "bolt/connectors/arrow/ArrowMemoryConnectorSplit.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
namespace bytedance::bolt::connector::arrow {

// Arrow column handle only needs the column name and the data type.
class ArrowMemoryColumnHandle : public ColumnHandle {
 public:
  ArrowMemoryColumnHandle(const std::string& name, const TypePtr& dateType)
      : name_(name), dataType_(dateType) {}

  [[nodiscard]] const std::string& getName() const {
    return name_;
  }
  [[nodiscard]] const TypePtr& getDataType() const {
    return dataType_;
  }

 private:
  const std::string name_;
  const TypePtr dataType_;
};

class ArrowMemoryTableHandle : public ConnectorTableHandle {
 public:
  ArrowMemoryTableHandle(std::string connectorId)
      : ConnectorTableHandle(connectorId) {}

  std::string toString() const override {
    return fmt::format("Arrow connector: {}", connectorId());
  }
};

// Each Arrow table consists at least Arrow split. Each split is made up by at
// least one column, which is a pair of ArrowSchema and ArrowArray. Since it is
// Arrow Memory Connector, the length of each split equals to the length of its
// columns. The size of splits can be adjusted while creating the input Arrow
// columns for performance tuning or etc.
class ArrowMemoryDataSource : public DataSource {
 public:
  ArrowMemoryDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      bolt::memory::MemoryPool* FOLLY_NONNULL pool);
  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {
    BOLT_NYI("Dynamic filters not supported by ArrowMemoryConnector.");
  };

  std::optional<RowVectorPtr> next(uint64_t size, bolt::ContinueFuture& future)
      override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    // TODO: Which stats do we want to expose here?
    return {};
  }

  int64_t estimatedRowSize() override {
    return 0;
  };

  void setFinish(bool finish) {
    finish_ = finish;
  };

 private:
  memory::MemoryPool* FOLLY_NONNULL pool_;
  bool finish_ = false;
  std::shared_ptr<ArrowMemoryConnectorSplit> currentSplit_;
  uint64_t completedRows_;
  uint64_t completedBytes_;
  std::mutex mutex_;
  const std::shared_ptr<const RowType> outputType_;
  ArrowOptions options_;
};

class ArrowMemoryConnector : public Connector {
 public:
  ArrowMemoryConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> properties,
      folly::Executor* FOLLY_NULLABLE /*executor*/)
      : Connector(id) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& queryConfig) override;

  //  TODO: Support Data Sink for Arrow Connector. But exportToArrow() is able
  //  to convert Bolt vectors to Arrow.

  std::unique_ptr<DataSink> createDataSink(
      std::shared_ptr<const RowType> /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* FOLLY_NONNULL /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/,
      const core::QueryConfig& /*queryConfig*/) override final {
    BOLT_NYI("ArrowMemoryConnector does not support data sink.");
  }
};

class ArrowMemoryConnectorFactory : public ConnectorFactory {
 public:
  ArrowMemoryConnectorFactory() : ConnectorFactory(kArrowMemoryConnectorName) {}

  explicit ArrowMemoryConnectorFactory(const char* FOLLY_NONNULL connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
    return std::make_shared<ArrowMemoryConnector>(id, config, executor);
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* executor = nullptr) override {
    std::shared_ptr<const config::ConfigBase> convertedConfig;
    convertedConfig = config == nullptr
        ? nullptr
        : std::make_shared<config::ConfigBase>(config->valuesCopy());
    return newConnector(id, convertedConfig, executor);
  }
};

template <typename T>
bool CheckArrowMemoryConnectorFactoryInit() {
  static bool init = bytedance::bolt::connector::registerConnectorFactory(
      std::make_shared<T>());
  return init;
}

} // namespace bytedance::bolt::connector::arrow
