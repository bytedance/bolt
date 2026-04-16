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

#include <fmt/format.h>
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
namespace bytedance::bolt::connector::hive {

struct PaimonConnectorSplit : public connector::ConnectorSplit {
  explicit PaimonConnectorSplit(
      const std::string& connectorId,
      std::vector<std::shared_ptr<HiveConnectorSplit>> _hiveSplits)
      : connector::ConnectorSplit(connectorId), hiveSplits(_hiveSplits) {
    BOLT_CHECK_GE(hiveSplits.size(), 1, "No splits");
  }

  int64_t splitSizeBytes() const override {
    int64_t total = 0;
    for (const auto& split : hiveSplits) {
      total += split->splitSizeBytes();
    }
    return total;
  }

  std::string toString() const override {
    std::string result;
    for (const auto& split : hiveSplits) {
      if (!result.empty()) {
        result += ", ";
      }
      result += split->toString();
    }

    return fmt::format("[PaimonConnectorSplit: {}]", result);
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "PaimonConnectorSplit";
    obj["connectorId"] = connectorId;

    folly::dynamic hiveSplitsArr = folly::dynamic::array;
    for (const auto& hiveSplit : hiveSplits) {
      BOLT_CHECK_NOT_NULL(hiveSplit, "Null HiveConnectorSplit in hiveSplits");
      hiveSplitsArr.push_back(hiveSplit->serialize());
    }

    obj["hiveSplits"] = hiveSplitsArr;
    return obj;
  }

  static std::shared_ptr<PaimonConnectorSplit> create(
      const folly::dynamic& obj);

  static void registerSerDe();

  std::vector<std::shared_ptr<HiveConnectorSplit>> hiveSplits;
};

} // namespace bytedance::bolt::connector::hive
