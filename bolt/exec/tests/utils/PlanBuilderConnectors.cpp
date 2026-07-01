/*
 * Copyright (c) International Business Machines Corporation
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

#include "bolt/exec/tests/utils/PlanBuilder.h"

#include "bolt/connectors/tpch/TpchConnector.h"

namespace bytedance::bolt::exec::test {

PlanBuilder& PlanBuilder::tpchTableScan(
    tpch::Table table,
    std::vector<std::string>&& columnNames,
    double scaleFactor,
    const std::string& connectorId) {
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignmentsMap;
  std::vector<TypePtr> outputTypes;

  assignmentsMap.reserve(columnNames.size());
  outputTypes.reserve(columnNames.size());

  for (const auto& columnName : columnNames) {
    assignmentsMap.emplace(
        columnName,
        std::make_shared<connector::tpch::TpchColumnHandle>(columnName));
    outputTypes.emplace_back(resolveTpchColumn(
        table,
        columnName,
        connectorId == connector::tpch::kBoltTpchConnectorId));
  }
  auto rowType = ROW(std::move(columnNames), std::move(outputTypes));
  return TableScanBuilder(*this)
      .filtersAsNode(filtersAsNode_ ? planNodeIdGenerator_ : nullptr)
      .outputType(rowType)
      .tableHandle(std::make_shared<connector::tpch::TpchTableHandle>(
          connectorId, table, scaleFactor))
      .assignments(assignmentsMap)
      .endTableScan();
}

} // namespace bytedance::bolt::exec::test
