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
 */

#pragma once

#include "bolt/common/config/Config.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/ConnectorNames.h"
#include "bolt/connectors/fuzzer/FuzzerConnectorSplit.h"
#include "bolt/core/Config.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

namespace bytedance::bolt::connector::fuzzer {

/// `FuzzerConnector` is a connector that generates data on-the-fly using
/// VectorFuzzer, based on the expected outputType defined by the client.
///
/// FuzzerConnector doesn't have the concept of table names, but using
/// `FuzzerTableHandle` clients can specify VectorFuzzer options and seed, which
/// are used when instantiating VectorFuzzer.
///
/// FuzzerConnectorSplit lets clients specify how many rows are expected to be
/// generated.

class FuzzerTableHandle : public ConnectorTableHandle {
 public:
  explicit FuzzerTableHandle(
      std::string connectorId,
      VectorFuzzer::Options options,
      size_t fuzzerSeed = 0)
      : ConnectorTableHandle(std::move(connectorId)),
        fuzzerOptions(options),
        fuzzerSeed(fuzzerSeed) {}

  ~FuzzerTableHandle() override {}

  std::string toString() const override {
    return "fuzzer-mock-table";
  }

  const VectorFuzzer::Options fuzzerOptions;
  size_t fuzzerSeed;
};

class FuzzerDataSource : public DataSource {
 public:
  FuzzerDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      bolt::memory::MemoryPool* FOLLY_NONNULL pool);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) override {
    BOLT_NYI("Dynamic filters not supported by FuzzerConnector.");
  }

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

 private:
  const RowTypePtr outputType_;
  std::unique_ptr<VectorFuzzer> vectorFuzzer_;

  // The current split being processed.
  std::shared_ptr<FuzzerConnectorSplit> currentSplit_;

  // How many rows were generated for this split.
  uint64_t splitOffset_{0};
  uint64_t splitEnd_{0};

  size_t completedRows_{0};
  size_t completedBytes_{0};

  memory::MemoryPool* FOLLY_NONNULL pool_;
};

class FuzzerConnector final : public Connector {
 public:
  FuzzerConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /*executor*/)
      : Connector(id) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /*columnHandles*/,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& /* queryConfig */) override final {
    return std::make_unique<FuzzerDataSource>(
        outputType, tableHandle, connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/,
      const core::QueryConfig& /*queryConfig*/) override final {
    BOLT_NYI("FuzzerConnector does not support data sink.");
  }
};

class FuzzerConnectorFactory : public ConnectorFactory {
 public:
  FuzzerConnectorFactory() : ConnectorFactory(kFuzzerConnectorName) {}

  explicit FuzzerConnectorFactory(const char* FOLLY_NONNULL connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<FuzzerConnector>(id, config, executor);
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

} // namespace bytedance::bolt::connector::fuzzer
