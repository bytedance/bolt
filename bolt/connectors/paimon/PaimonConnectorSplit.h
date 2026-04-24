/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
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
