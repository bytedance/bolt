/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/connectors/Connector.h"

namespace bytedance::bolt::connector::paimon {

struct PaimonConnectorSplit : public connector::ConnectorSplit {
  explicit PaimonConnectorSplit(const std::string& connectorId, std::string serializedSplit)
      : ConnectorSplit(connectorId), serializedSplit(std::move(serializedSplit)) {}

  // The serialized paimon::Split (e.g. JSON or binary)
  std::string serializedSplit;

  folly::dynamic serialize() const override {
      folly::dynamic obj = folly::dynamic::object;
      obj["connectorId"] = connectorId;
      obj["serializedSplit"] = serializedSplit;
      return obj;
  }
};

} // namespace bytedance::bolt::connector::paimon
