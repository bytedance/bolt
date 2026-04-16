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
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/connectors/Connector.h"
namespace bytedance::bolt::connector::fuzzer {

struct FuzzerConnectorSplit : public connector::ConnectorSplit {
  explicit FuzzerConnectorSplit(const std::string& connectorId, size_t numRows)
      : ConnectorSplit(connectorId), numRows(numRows) {}

  int64_t splitSizeBytes() const override {
    return 0;
  }

  // Row many rows to generate.
  size_t numRows;
};

} // namespace bytedance::bolt::connector::fuzzer

template <>
struct fmt::formatter<bytedance::bolt::connector::fuzzer::FuzzerConnectorSplit>
    : formatter<std::string> {
  auto format(
      bytedance::bolt::connector::fuzzer::FuzzerConnectorSplit s,
      format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<
    std::shared_ptr<bytedance::bolt::connector::fuzzer::FuzzerConnectorSplit>>
    : formatter<std::string> {
  auto format(
      std::shared_ptr<bytedance::bolt::connector::fuzzer::FuzzerConnectorSplit>
          s,
      format_context& ctx) {
    return formatter<std::string>::format(s->toString(), ctx);
  }
};
