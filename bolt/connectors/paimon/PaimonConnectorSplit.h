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

#pragma once

#include <folly/Range.h>
#include <string>
#include "bolt/connectors/Connector.h"

namespace bytedance::bolt::connector::paimon {

struct PaimonConnectorSplit : public connector::ConnectorSplit {
  explicit PaimonConnectorSplit(
      const std::string& connectorId,
      const char* splitBytes,
      size_t length)
      : ConnectorSplit(connectorId), splitBytes_(splitBytes, length) {}

  const std::string splitBytes_;

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["connectorId"] = connectorId;
    obj["splitBytes"] = splitBytes_;
    return obj;
  }
};

} // namespace bytedance::bolt::connector::paimon
