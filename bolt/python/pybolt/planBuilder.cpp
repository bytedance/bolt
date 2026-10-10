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
 * 2026-10-10.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/python/pybolt/context.h"
#include "bolt/type/Type.h"

using namespace ::bytedance::bolt;

namespace bytedance::bolt::python {

void addPlanBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::class_<
      exec::test::PlanBuilder,
      std::shared_ptr<exec::test::PlanBuilder>>(
      m, "PlanBuilder", pybind11::module_local(asModuleLocalDefinitions))
      // constructor
      .def(pybind11::init([]() {
        return std::make_shared<exec::test::PlanBuilder>(
            PyBoltContext::getSingletonInstance().pool());
      }))
      .def(
          pybind11::init([](int startId) {
            return std::make_shared<exec::test::PlanBuilder>(
                std::make_shared<core::PlanNodeIdGenerator>(startId),
                PyBoltContext::getSingletonInstance().pool());
          }),
          pybind11::arg("startId"))
      .def(
          pybind11::init([](const core::PlanNodePtr& initialPlanNode) {
            return std::make_shared<exec::test::PlanBuilder>(
                initialPlanNode,
                std::make_shared<core::PlanNodeIdGenerator>(0));
          }),
          pybind11::arg("planNode"))
      .def(
          "tableRead",
          [](exec::test::PlanBuilder& self,
             const std::string_view filename,
             const RowTypePtr& outputType,
             const std::vector<std::string>& partitionKeys,
             std::unordered_map<std::string, std::string> parameters) {
            std::unordered_map<
                std::string,
                std::shared_ptr<connector::ColumnHandle>>
                assignments;
            for (const auto& pk : partitionKeys) {
              auto type = outputType->findChild(pk);
              assignments.insert(
                  {pk,
                   std::make_shared<connector::hive::HiveColumnHandle>(
                       pk,
                       connector::hive::HiveColumnHandle::ColumnType::
                           kPartitionKey,
                       type,
                       type)});
            }
            for (uint32_t i = 0; i < outputType->size(); ++i) {
              const auto& name = outputType->nameOf(i);
              const auto& type = outputType->childAt(i);
              if (assignments.find(name) != assignments.end()) {
                continue;
              }
              assignments.insert(
                  {name,
                   std::make_shared<connector::hive::HiveColumnHandle>(
                       name,
                       connector::hive::HiveColumnHandle::ColumnType::kRegular,
                       type,
                       type)});
            }

            exec::test::PlanBuilder::TableScanBuilder(self)
                .tableName(std::string(filename))
                .assignments(std::move(assignments))
                .outputType(outputType)
                .parameters(std::move(parameters))
                .endTableScan();
            return self;
          },
          pybind11::arg("filename"),
          pybind11::arg("outputType"),
          pybind11::arg("partitionKeys") = pybind11::list(),
          pybind11::arg("parameters") = pybind11::dict())
      .def(
          "tpchTableScan",
          [](exec::test::PlanBuilder& self,
             tpch::Table table,
             std::vector<std::string> columnNames,
             double scaleFactor) {
            self.tpchTableScan(
                table,
                std::move(columnNames),
                scaleFactor,
                std::string(exec::test::PlanBuilder::kTpchDefaultConnectorId));
            return self;
          },
          pybind11::arg("table"),
          pybind11::arg("columnNames"),
          pybind11::arg("scaleFactor") = 1,
          pybind11::return_value_policy::reference_internal)
      .def(
          "values",
          [](exec::test::PlanBuilder& self,
             const std::vector<RowVectorPtr>& values,
             bool parallelizable,
             size_t repeatTimes) {
            return self.values(values, parallelizable, repeatTimes);
          },
          pybind11::arg("values"),
          pybind11::arg("parallelizable") = false,
          pybind11::arg("repeatTimes") = 1,
          pybind11::return_value_policy::reference_internal)
      .def(
          "project",
          [](exec::test::PlanBuilder& self,
             const std::vector<std::string>& projections) {
            return self.project(projections);
          },
          pybind11::arg("projections"),
          pybind11::return_value_policy::reference_internal)
      .def(
          "appendColumns",
          [](exec::test::PlanBuilder& self,
             const std::vector<std::string>& newColumns) {
            return self.appendColumns(newColumns);
          },
          pybind11::arg("newColumns"),
          pybind11::return_value_policy::reference_internal)
      .def(
          "filter",
          [](exec::test::PlanBuilder& self, const std::string& filter) {
            return self.filter(filter);
          },
          pybind11::arg("filter"),
          pybind11::return_value_policy::reference_internal)
      .def(
          "singleAggregation",
          [](exec::test::PlanBuilder& self,
             const std::vector<std::string>& groupingKeys,
             const std::vector<std::string>& aggregates,
             const std::vector<std::string>& masks) {
            return self.singleAggregation(groupingKeys, aggregates, masks);
          },
          pybind11::arg("groupingKeys"),
          pybind11::arg("aggregates"),
          pybind11::arg("masks") = std::vector<std::string>{},
          pybind11::return_value_policy::reference_internal)
      .def(
          "hashJoin",
          [](exec::test::PlanBuilder& self,
             const std::vector<std::string>& leftKeys,
             const std::vector<std::string>& rightKeys,
             const core::PlanNodePtr& build,
             const std::string& filter,
             const std::vector<std::string>& outputLayout,
             core::JoinType joinType,
             bool nullAware) {
            return self.hashJoin(
                leftKeys,
                rightKeys,
                build,
                filter,
                outputLayout,
                joinType,
                nullAware);
          },
          pybind11::arg("leftKeys"),
          pybind11::arg("rightKeys"),
          pybind11::arg("build"),
          pybind11::arg("filter") = "",
          pybind11::arg("outputLayout"),
          pybind11::arg("joinType") = core::JoinType::kInner,
          pybind11::arg("nullAware") = false,
          pybind11::return_value_policy::reference_internal)
      .def(
          "orderBy",
          [](exec::test::PlanBuilder& self,
             const std::vector<std::string>& keys,
             bool isPartial) { return self.orderBy(keys, isPartial); },
          pybind11::arg("keys"),
          pybind11::arg("isPartial"),
          pybind11::return_value_policy::reference_internal)
      .def(
          "limit",
          [](exec::test::PlanBuilder& self,
             int64_t offset,
             int64_t count,
             bool isPartial) { return self.limit(offset, count, isPartial); },
          pybind11::arg("offset"),
          pybind11::arg("count"),
          pybind11::arg("isPartial"),
          pybind11::return_value_policy::reference_internal)
      .def(
          "localShuffle",
          [](exec::test::PlanBuilder& self, int64_t seed) {
            return self.localShuffle(seed);
          },
          pybind11::return_value_policy::reference_internal)
      .def(
          "tableWrite",
          [](exec::test::PlanBuilder& self,
             const std::string& outputDirectoryPath,
             dwio::common::FileFormat fileFormat,
             const std::vector<std::string>& partitionBy,
             int32_t bucketCount,
             const std::vector<std::string>& bucketedBy,
             const std::vector<std::string>& aggregates) {
            if (!bucketedBy.empty() and bucketCount > 0 and
                !partitionBy.empty()) {
              return self.tableWrite(
                  outputDirectoryPath,
                  partitionBy,
                  bucketCount,
                  bucketedBy,
                  fileFormat,
                  aggregates);
            }
            if (!partitionBy.empty()) {
              return self.tableWrite(
                  outputDirectoryPath, partitionBy, fileFormat, aggregates);
            }
            return self.tableWrite(outputDirectoryPath, fileFormat, aggregates);
          },
          pybind11::arg("outputDirectoryPath"),
          pybind11::arg("fileFormat") = dwio::common::FileFormat::DWRF,
          pybind11::arg("partitionBy") = std::vector<std::string>{},
          pybind11::arg("bucketCount") = 0,
          pybind11::arg("bucketedBy") = std::vector<std::string>{},
          pybind11::arg("aggregates") = std::vector<std::string>{},
          pybind11::return_value_policy::reference_internal)
      .def(
          "pythonPlanNode",
          [](exec::test::PlanBuilder& self,
             std::string functionName,
             const RowTypePtr& outputType,
             pybind11::function function,
             pybind11::args args,
             const pybind11::kwargs& kwargs) {
            return self.python(
                std::move(functionName),
                std::move(function),
                std::move(args),
                std::move(kwargs),
                outputType);
          },
          pybind11::arg("functionName"),
          pybind11::arg("outputType"),
          pybind11::arg("function"),
          pybind11::return_value_policy::reference_internal)
      .def("planNode", &exec::test::PlanBuilder::planNode)
      .def("planFragment", &exec::test::PlanBuilder::planFragment);
}
} // namespace bytedance::bolt::python
