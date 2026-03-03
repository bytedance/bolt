/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <folly/Range.h>
#include <paimon/table/source/split.h>
#include "bolt/connectors/Connector.h"

namespace bytedance::bolt::connector::paimon {

struct PaimonConnectorSplit : public connector::ConnectorSplit {
  explicit PaimonConnectorSplit(
      const std::string& connectorId,
      std::shared_ptr<::paimon::Split> split)
      : ConnectorSplit(connectorId), split_(std::move(split)) {}

  std::shared_ptr<::paimon::Split> split_;

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["connectorId"] = connectorId;
    return obj;
  }
};

} // namespace bytedance::bolt::connector::paimon
