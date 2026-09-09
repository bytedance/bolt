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
 * 2026-08-28.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/tests/utils/TpcdsQueryBuilder.h"

#include <algorithm>
#include <filesystem>

#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"

namespace bytedance::bolt::exec::test {

TpcdsQueryBuilder::TpcdsQueryBuilder(
    dwio::common::FileFormat format,
    folly::Executor* ioExecutor,
    std::shared_ptr<const config::ConfigBase> connectorConfig)
    : format_(format),
      ioExecutor_(ioExecutor),
      connectorConfig_(
          connectorConfig != nullptr
              ? std::move(connectorConfig)
              : std::make_shared<const config::ConfigBase>(
                    std::unordered_map<std::string, std::string>{})) {}

void TpcdsQueryBuilder::initialize(const std::string& dataPath) {
  tableDataFiles_.clear();

  std::error_code error;
  for (const auto& tableEntry : std::filesystem::directory_iterator(
           dataPath,
           std::filesystem::directory_options::skip_permission_denied,
           error)) {
    if (!tableEntry.is_directory()) {
      continue;
    }

    const auto tableName = tableEntry.path().filename().string();
    if (tableName.empty() || tableName.front() == '.') {
      continue;
    }

    auto& files = tableDataFiles_[tableName];
    std::error_code fileError;
    for (const auto& fileEntry : std::filesystem::directory_iterator(
             tableEntry.path(),
             std::filesystem::directory_options::skip_permission_denied,
             fileError)) {
      const auto filename = fileEntry.path().filename().string();
      if (fileEntry.is_regular_file() && !filename.empty() &&
          filename.front() != '.') {
        files.push_back(fileEntry.path().string());
      }
    }
    BOLT_USER_CHECK(
        !fileError,
        "Failed to scan TPC-DS table directory {}: {}",
        tableEntry.path().string(),
        fileError.message());
    std::sort(files.begin(), files.end());
  }

  BOLT_USER_CHECK(
      !error,
      "Failed to scan TPC-DS data path {}: {}",
      dataPath,
      error.message());
  BOLT_USER_CHECK(
      !tableDataFiles_.empty(),
      "No table subdirectories found in TPC-DS data path: {}",
      dataPath);
}

TpchPlan TpcdsQueryBuilder::getQueryPlan(
    const std::string& queryName,
    const std::string& planDirectory,
    memory::MemoryPool* pool) {
  auto plan = TpcdsPlanLoader(planDirectory, pool).loadPlan(queryName);
  const auto scans = TpcdsPlanLoader::collectTableScanNodes(plan.plan);

  if (!scans.empty() && connectorId_.empty()) {
    connectorId_ = scans.front()->tableHandle()->connectorId();
    if (!connector::isConnectorRegistered(connectorId_)) {
      if (!connector::hasConnectorFactory(connector::kHiveConnectorName)) {
        connector::registerConnectorFactory(
            std::make_shared<connector::hive::HiveConnectorFactory>());
      }
      auto connectorInstance =
          connector::getConnectorFactory(connector::kHiveConnectorName)
              ->newConnector(connectorId_, connectorConfig_, ioExecutor_);
      connector::registerConnector(std::move(connectorInstance));
      ownedConnector_ = true;
    }
  }

  for (const auto& scan : scans) {
    const auto& tableName = scan->tableHandle()->name();
    const auto* files = findDataFiles(tableName);
    BOLT_USER_CHECK(
        files != nullptr,
        "No data files found for TPC-DS table '{}' used by {}",
        tableName,
        queryName);
    plan.dataFiles[scan->id()] = *files;
  }

  plan.dataFileFormat = format_;
  return plan;
}

std::shared_ptr<connector::ConnectorSplit> TpcdsQueryBuilder::makeSplit(
    const std::string& filePath) const {
  return connector::hive::HiveConnectorSplitBuilder(filePath)
      .connectorId(connectorId_.empty() ? kHiveConnectorId : connectorId_)
      .fileFormat(format_)
      .build();
}

void TpcdsQueryBuilder::shutdown() {
  if (ownedConnector_ && !connectorId_.empty()) {
    connector::unregisterConnector(connectorId_);
  }
  ownedConnector_ = false;
  connectorId_.clear();
}

const std::vector<std::string>* TpcdsQueryBuilder::findDataFiles(
    const std::string& tableName) const {
  auto it = tableDataFiles_.find(tableName);
  if (it != tableDataFiles_.end() && !it->second.empty()) {
    return &it->second;
  }

  const auto dot = tableName.rfind('.');
  if (dot != std::string::npos) {
    it = tableDataFiles_.find(tableName.substr(dot + 1));
    if (it != tableDataFiles_.end() && !it->second.empty()) {
      return &it->second;
    }
  }
  return nullptr;
}

} // namespace bytedance::bolt::exec::test
