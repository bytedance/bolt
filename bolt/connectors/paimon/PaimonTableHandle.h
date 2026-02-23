/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/connectors/Connector.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::connector::paimon {

class PaimonColumnHandle : public ColumnHandle {
 public:
  PaimonColumnHandle(const std::string& name, TypePtr type)
      : name_(name), type_(std::move(type)) {}

  const std::string& name() const {
    return name_;
  }

  const TypePtr& type() const {
    return type_;
  }

  std::string toString() const {
    return name_;
  }

  folly::dynamic serialize() const override {
     return folly::dynamic::object("name", name_);
  }

 private:
  std::string name_;
  TypePtr type_;
};

class PaimonTableHandle : public ConnectorTableHandle {
 public:
  explicit PaimonTableHandle(std::string connectorId, std::string tableName, std::string tablePath)
      : ConnectorTableHandle(std::move(connectorId)),
        tableName_(std::move(tableName)),
        tablePath_(std::move(tablePath)) {}

  const std::string& tableName() const {
    return tableName_;
  }

  const std::string& tablePath() const {
    return tablePath_;
  }

  std::string toString() const override {
    return tableName_;
  }

  folly::dynamic serialize() const override {
      folly::dynamic obj = folly::dynamic::object;
      obj["connectorId"] = connectorId();
      obj["tableName"] = tableName_;
      obj["tablePath"] = tablePath_;
      return obj;
  }

 private:
  std::string tableName_;
  std::string tablePath_;
};

} // namespace bytedance::bolt::connector::paimon
