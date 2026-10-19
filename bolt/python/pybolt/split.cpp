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

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/PaimonConnectorSplit.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/connectors/tpch/TpchConnectorSplit.h"
#include "bolt/exec/Split.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/python/pybolt/context.h"

using namespace ::bytedance::bolt;
using ::bytedance::bolt::exec::test::PlanBuilder;

namespace bytedance::bolt::python {

void addSplitBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::class_<exec::Split>(
      m, "Split", pybind11::module_local(asModuleLocalDefinitions))
      .def("__repr__", &exec::Split::toString)
      .def("__str__", &exec::Split::toString)
      .def_static(
          "tpch",
          [](size_t totalParts = 1) {
            std::vector<exec::Split> splits;
            splits.reserve(totalParts);

            for (size_t i = 0; i < totalParts; i++) {
              auto split =
                  std::make_shared<connector::tpch::TpchConnectorSplit>(
                      std::string(PlanBuilder::kTpchDefaultConnectorId),
                      totalParts,
                      i);
              splits.emplace_back(
                  std::dynamic_pointer_cast<connector::ConnectorSplit>(split));
            }
            return splits;
          },
          pybind11::arg("numParts") = pybind11::int_(1),
          "Create a set of splits to generate a TPCH table.")
      .def_static(
          "hive",
          [](const std::string_view uri,
             dwio::common::FileFormat fileFormat,
             const std::unordered_map<std::string, std::optional<std::string>>&
                 partitionKeys) -> exec::Split {
            auto split = std::make_shared<connector::hive::HiveConnectorSplit>(
                std::string(PlanBuilder::kHiveDefaultConnectorId),
                std::string(uri),
                fileFormat,
                0,
                std::numeric_limits<uint64_t>::max(),
                partitionKeys);
            return exec::Split(
                std::dynamic_pointer_cast<connector::ConnectorSplit>(split));
          },
          pybind11::arg("uri"),
          pybind11::arg("fileFormat"),
          pybind11::arg("partitionsMap") = pybind11::dict(),
          "Create a single hive split for reading a file.")
      .def_static(
          "paimon",
          [](const std::vector<std::string>& uris,
             dwio::common::FileFormat fileFormat,
             const std::unordered_map<std::string, std::optional<std::string>>&
                 partitionKeys) -> exec::Split {
            std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>>
                hiveSplits;
            hiveSplits.reserve(uris.size());
            for (const auto& uri : uris) {
              hiveSplits.emplace_back(
                  std::make_shared<connector::hive::HiveConnectorSplit>(
                      std::string(PlanBuilder::kHiveDefaultConnectorId),
                      uri,
                      fileFormat,
                      0,
                      std::numeric_limits<uint64_t>::max(),
                      partitionKeys));
            }
            auto paimonSplit =
                std::make_shared<connector::hive::PaimonConnectorSplit>(
                    std::string(PlanBuilder::kHiveDefaultConnectorId),
                    std::move(hiveSplits));
            return exec::Split(
                std::dynamic_pointer_cast<connector::ConnectorSplit>(
                    paimonSplit));
          },
          pybind11::arg("uris"),
          pybind11::arg("fileFormat"),
          pybind11::arg("partitionKeys") = pybind11::dict(),
          "Create a paimon split to read one or multiple files.");
}
} // namespace bytedance::bolt::python
