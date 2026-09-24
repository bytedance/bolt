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

#include "bolt/exec/task_manager/Utils.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"

namespace bytedance::bolt {
exec::Split makeSplit(
    const std::string& connectorName,
    const std::string& filePath,
    dwio::common::FileFormat format,
    size_t offset,
    size_t length) {
  BOLT_USER_CHECK(
      connector::isConnectorRegistered(connectorName),
      "Connector {} is not registered",
      connectorName);
  BOLT_USER_CHECK(
      std::dynamic_pointer_cast<connector::hive::HiveConnector>(
          connector::getConnector(connectorName)) != nullptr,
      "Connector {} does not support Hive file splits",
      connectorName);
  return exec::Split(std::make_shared<connector::hive::HiveConnectorSplit>(
      connectorName, filePath, format, offset, length));
}
} // namespace bytedance::bolt
