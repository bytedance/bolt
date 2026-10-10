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
 * 2026-10-10.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "context.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/dwio/orc/reader/RegisterOrcReader.h"
#include "bolt/dwio/orc/writer/RegisterOrcWriter.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/RegisterParquetWriter.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/functions/sparksql/window/WindowFunctionsRegistration.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/python/Utils.h"

using bytedance::bolt::exec::test::PlanBuilder;

namespace bytedance::bolt::python {

PyBoltContext::PyBoltContext() {
  initMemoryManager();

  auto* memoryManager = bytedance::bolt::memory::MemoryManager::getInstance();
  rootPool_ = memoryManager->addRootPool(
      fmt::format("{}_root_pool", kDefaultContextName),
      memoryManager->capacity());
  leafPool_ =
      rootPool_->addLeafChild(fmt::format("{}_leaf_pool", kDefaultContextName));

  int numThreads = std::max(1, (int)std::thread::hardware_concurrency());
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
      numThreads,
      std::make_shared<folly::NamedThreadFactory>(kDefaultContextName));

  queryCtx_ = bytedance::bolt::core::QueryCtx::create(
      executor_.get(),
      bytedance::bolt::core::QueryConfig{{}},
      std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::config::ConfigBase>>{},
      bytedance::bolt::cache::AsyncDataCache::getInstance(),
      rootPool_,
      nullptr,
      kDefaultContextName);

  execCtx_ = std::make_unique<bytedance::bolt::core::ExecCtx>(
      leafPool_.get(), queryCtx_.get());

  registerAllFunctions();
  registerHiveConnector();
  registerTpchConnector();

  filesystems::registerLocalFileSystem();
  bytedance::bolt::orc::registerOrcWriterFactory();
  bytedance::bolt::parquet::registerParquetWriterFactory();
  bytedance::bolt::orc::registerOrcReaderFactory();
  bytedance::bolt::parquet::registerParquetReaderFactory();

  using bytedance::bolt::dwio::common::FileSink;
  using bytedance::bolt::dwio::common::LocalFileSink;
  FileSink::registerFactory(
      [](const std::string& name,
         const FileSink::Options& options) -> std::unique_ptr<FileSink> {
        return std::make_unique<LocalFileSink>(name, options);
      });
}

void PyBoltContext::cleanup() {
  // Explicitly deregister all vector functions that may contain python
  // function objects from user.
  bytedance::bolt::exec::vectorFunctionFactories().withWLock(
      [&](auto& functionMap) { functionMap.clear(); });
  bytedance::bolt::exec::aggregateFunctions().withWLock(
      [&](auto& functionMap) { functionMap.clear(); });
  bolt::python::cleanup();
}

void PyBoltContext::initMemoryManager() {
  bytedance::bolt::memory::MemoryManager::Options memoryOpt;
  memoryOpt.alignment = bytedance::bolt::memory::MemoryAllocator::kMaxAlignment;
  memoryOpt.trackDefaultUsage = true;
  memoryOpt.checkUsageLeak = true;
  memoryOpt.coreOnAllocationFailureEnabled = false;
  memoryOpt.allocatorCapacity = bytedance::bolt::memory::kMaxMemory;
  bytedance::bolt::memory::MemoryManager::initialize(memoryOpt);
}

void PyBoltContext::registerAllFunctions() {
  bytedance::bolt::parse::registerTypeResolver();
  bytedance::bolt::functions::prestosql::registerAllScalarFunctions();
  bytedance::bolt::functions::sparksql::registerFunctions(kFunctionPrefix);

  bytedance::bolt::aggregate::prestosql::registerAllAggregateFunctions(
      kFunctionPrefix, true /*registerCompanionFunctions*/, true /*overwrite*/);
  bytedance::bolt::functions::aggregate::sparksql::registerAggregateFunctions(
      kFunctionPrefix, true /*registerCompanionFunctions*/, true /*overwrite*/);
  bytedance::bolt::window::prestosql::registerAllWindowFunctions();
  bytedance::bolt::functions::window::sparksql::registerWindowFunctions(
      kFunctionPrefix);
}

void PyBoltContext::registerHiveConnector() {
  if (!connector::isConnectorRegistered(
          std::string(PlanBuilder::kHiveDefaultConnectorId))) {
    std::unordered_map<std::string, std::string> hiveConfig({
        {connector::hive::HiveConfig::kParquetUseColumnNames, "true"},
    });
    auto hiveConnector =
        connector::getConnectorFactory(connector::kHiveConnectorName)
            ->newConnector(
                std::string(PlanBuilder::kHiveDefaultConnectorId),
                std::make_shared<config::ConfigBase>(std::move(hiveConfig)),
                nullptr);
    connector::registerConnector(hiveConnector);
  }
  connector::hive::HiveConnectorFactory().initialize();
}

void PyBoltContext::registerTpchConnector() {
  connector::tpch::CheckTpchConnectorFactoryInit<
      connector::tpch::TpchConnectorFactory>();
  if (!connector::isConnectorRegistered(
          std::string(PlanBuilder::kTpchDefaultConnectorId))) {
    auto tpchConnector =
        connector::getConnectorFactory(
            connector::tpch::TpchConnectorFactory::kTpchConnectorName)
            ->newConnector(
                std::string(PlanBuilder::kTpchDefaultConnectorId),
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()),
                nullptr);
    connector::registerConnector(tpchConnector);
  }
}
} // namespace bytedance::bolt::python
