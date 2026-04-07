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

#include <fmt/format.h>
#include "bolt/connectors/Connector.h"
namespace bytedance::bolt::connector::tpch {

struct TpchConnectorSplit : public connector::ConnectorSplit {
  explicit TpchConnectorSplit(
      const std::string& connectorId,
      size_t totalParts = 1,
      size_t partNumber = 0)
      : ConnectorSplit(connectorId),
        totalParts(totalParts),
        partNumber(partNumber) {
    BOLT_CHECK_GE(totalParts, 1, "totalParts must be >= 1");
    BOLT_CHECK_GT(totalParts, partNumber, "totalParts must be > partNumber");
  }

  int64_t splitSizeBytes() const override {
    return 0;
  }

  // In how many parts the generated TPC-H table will be segmented, roughly
  // `rowCount / totalParts`
  size_t totalParts{1};

  // Which of these parts will be read by this split.
  size_t partNumber{0};
};

} // namespace bytedance::bolt::connector::tpch

template <>
struct fmt::formatter<bytedance::bolt::connector::tpch::TpchConnectorSplit>
    : formatter<std::string> {
  auto format(
      bytedance::bolt::connector::tpch::TpchConnectorSplit s,
      format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<
    std::shared_ptr<bytedance::bolt::connector::tpch::TpchConnectorSplit>>
    : formatter<std::string> {
  auto format(
      std::shared_ptr<bytedance::bolt::connector::tpch::TpchConnectorSplit> s,
      format_context& ctx) {
    return formatter<std::string>::format(s->toString(), ctx);
  }
};
