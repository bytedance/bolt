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

#pragma once

#include <folly/Executor.h>

#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/tests/utils/TpcdsPlanLoader.h"

namespace bytedance::bolt::exec::test {

/// Loads TPC-DS plans and binds their table scans to a Hive-style data tree.
class TpcdsQueryBuilder {
 public:
  explicit TpcdsQueryBuilder(
      dwio::common::FileFormat format = dwio::common::FileFormat::PARQUET,
      folly::Executor* ioExecutor = nullptr,
      std::shared_ptr<const config::ConfigBase> connectorConfig = nullptr);

  /// Discovers data files below dataPath/<table-name>/.
  void initialize(const std::string& dataPath);

  TpchPlan getQueryPlan(
      const std::string& queryName,
      const std::string& planDirectory,
      memory::MemoryPool* pool);

  std::shared_ptr<connector::ConnectorSplit> makeSplit(
      const std::string& filePath) const;

  void shutdown();

  const std::string& connectorId() const {
    return connectorId_;
  }

 private:
  const std::vector<std::string>* findDataFiles(
      const std::string& tableName) const;

  const dwio::common::FileFormat format_;
  folly::Executor* const ioExecutor_;
  const std::shared_ptr<const config::ConfigBase> connectorConfig_;
  std::string connectorId_;
  bool ownedConnector_{false};
  std::unordered_map<std::string, std::vector<std::string>> tableDataFiles_;
};

} // namespace bytedance::bolt::exec::test
