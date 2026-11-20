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
 * This file was created by ByteDance Ltd. and/or its affiliates.
 *
 * This file is released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 * --------------------------------------------------------------------------
 */

#include <limits>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Split.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/python/pybolt/context.h"
#include "bolt/substrait/BoltExtensions.h"
#include "bolt/substrait/PythonRel.h"
#include "bolt/substrait/SubstraitToBoltPlan.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/substrait/proto/substrait/extensions/extensions.pb.h"

using namespace ::bytedance::bolt;
using ::bytedance::bolt::exec::test::PlanBuilder;

namespace bytedance::bolt::python {

namespace {

template <typename T>
T parseProtobufMessage(
    const pybind11::bytes& serializedBytes,
    const std::string& messageType) {
  T message;
  std::string bytes = serializedBytes;
  if (!message.ParseFromString(bytes)) {
    throw std::runtime_error(
        "Failed to parse " + messageType + " from " +
        std::to_string(bytes.size()) + " bytes");
  }
  return message;
}

template <typename T>
pybind11::bytes serializeProtobufMessage(
    const T& message,
    const std::string& messageType) {
  std::string out;
  if (!message.SerializeToString(&out)) {
    throw std::runtime_error("Failed to serialize " + messageType);
  }
  return pybind11::bytes(out);
}

} // namespace

void addSubstraitBindings( // NOLINT
    pybind11::module& m,
    bool /*asModuleLocalDefinitions*/) {
  m.def(
      "convertSubstraitPlan",
      [](const pybind11::bytes& planBytes)
          -> std::pair<
              std::shared_ptr<core::PlanNode>,
              std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>> {
        auto plan = parseProtobufMessage<::substrait::Plan>(
            planBytes, "Substrait Plan");

        substrait::SubstraitBoltPlanConverter converter(
            PyBoltContext::getSingletonInstance().pool(), false);

        auto planNode = converter.toBoltPlan(plan);

        // Splits are already constructed by the converter preserving grouping.
        std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>
            splitsMap = converter.splitInfos();

        // const_pointer_cast needed because PlanNode pybind11 binding uses
        // std::shared_ptr<PlanNode> (non-const) as the holder type.
        return {
            std::const_pointer_cast<core::PlanNode>(planNode),
            std::move(splitsMap)};
      },
      pybind11::arg("planBytes"),
      "Convert a serialized Substrait Plan to a Bolt PlanNode and splits map.");

  // Build an ExtensionSingleRel that embeds a Python function and arguments.
  // Parameters:
  //  - inputRelBytes: serialized substrait::Rel (parent input)
  //  - function: Python callable
  //  - *args, **kwargs: Python arguments passed to the callable
  //  - outputType: Bolt RowType describing the callable output
  // Returns: serialized substrait::ExtensionSingleRel bytes.
  m.def(
      "createPythonRel",
      [](const pybind11::bytes& inputRelBytes,
         const pybind11::function& function,
         const RowTypePtr& outputType,
         const std::vector<std::string>& extensions,
         const pybind11::args& args,
         const pybind11::kwargs& kwargs) -> pybind11::bytes {
        auto inputRel = parseProtobufMessage<::substrait::Rel>(
            inputRelBytes, "Substrait Rel");

        // Parse optional extensions declarations from serialized bytes.
        std::vector<::substrait::extensions::SimpleExtensionDeclaration> exts;
        exts.reserve(extensions.size());
        for (const auto& eb : extensions) {
          ::substrait::extensions::SimpleExtensionDeclaration decl;
          if (!decl.ParseFromString(eb)) {
            throw std::runtime_error(
                "Failed to parse SimpleExtensionDeclaration from " +
                std::to_string(eb.size()) + " bytes");
          }
          exts.emplace_back(std::move(decl));
        }

        auto rel = substrait::createPythonRel(
            inputRel, function, args, kwargs, outputType, exts);

        return serializeProtobufMessage(rel, "Substrait ExtensionSingleRel");
      },
      pybind11::arg("inputRelBytes"),
      pybind11::arg("function"),
      pybind11::arg("outputType"),
      pybind11::arg("extensions") = std::vector<std::string>{},
      "Create a Substrait ExtensionSingleRel embedding a Python function,"
      " serialized as bytes.");

  // Build an ExtensionSingleRel that encodes a local shuffle with a seed.
  // Parameters:
  //  - inputRelBytes: serialized substrait::Rel (parent input)
  //  - seed: int
  // Returns: serialized substrait::ExtensionSingleRel bytes.
  m.def(
      "createShuffleRel",
      [](const pybind11::bytes& inputRelBytes,
         int64_t seed) -> pybind11::bytes {
        auto inputRel = parseProtobufMessage<::substrait::Rel>(
            inputRelBytes, "Substrait Rel");
        auto rel = substrait::createShuffleRel(inputRel, seed);
        return serializeProtobufMessage(rel, "Substrait ExtensionSingleRel");
      },
      pybind11::arg("inputRelBytes"),
      pybind11::arg("seed"),
      "Create a Substrait ExtensionSingleRel encoding a local shuffle.");

  // Build a WriteRel carrying write options in AdvancedExtension.
  // Returns serialized WriteRel bytes (input not set).
  m.def(
      "createWriteRel",
      [](const std::string& directoryPath,
         const dwio::common::FileFormat fileFormat,
         const std::vector<std::string>& partitionKeys,
         int32_t numBuckets,
         const std::vector<std::string>& bucketKeys) -> pybind11::bytes {
        auto rel = substrait::createWriteRel(
            directoryPath, fileFormat, partitionKeys, numBuckets, bucketKeys);
        return serializeProtobufMessage(rel, "Substrait WriteRel");
      },
      pybind11::arg("directoryPath"),
      pybind11::arg("fileFormat"),
      pybind11::arg("partitionKeys"),
      pybind11::arg("numBuckets"),
      pybind11::arg("bucketKeys"),
      "Create a Substrait WriteRel with directory and partition/bucket options.");

  // Build a ReadRel with PaimonExtensionTable detail.
  // Returns serialized ReadRel bytes.
  m.def(
      "makePaimonExtensionTable",
      [](const RowTypePtr& schema,
         const dwio::common::FileFormat fileFormat,
         const std::vector<exec::Split>& splits,
         const std::unordered_map<std::string, std::string>& parameters)
          -> pybind11::bytes {
        auto rel = substrait::makePaimonExtensionTable(
            schema, fileFormat, splits, parameters);
        return serializeProtobufMessage(rel, "Substrait ReadRel");
      },
      pybind11::arg("schema"),
      pybind11::arg("fileFormat"),
      pybind11::arg("splits"),
      pybind11::arg("parameters") =
          std::unordered_map<std::string, std::string>{},
      "Create a Substrait ReadRel using PaimonExtensionTable and return bytes.");

  // Build a ReadRel with TpchExtensionTable detail.
  // Returns serialized ReadRel bytes.
  m.def(
      "makeTpchExtensionTable",
      [](const std::string& tableName,
         const std::vector<std::string>& columnNames,
         double scaleFactor,
         uint64_t numSplits) -> pybind11::bytes {
        auto rel = substrait::makeTpchExtensionTable(
            tableName,
            columnNames,
            scaleFactor,
            numSplits,
            std::string(PlanBuilder::kTpchDefaultConnectorId));
        return serializeProtobufMessage(rel, "Substrait ReadRel");
      },
      pybind11::arg("tableName"),
      pybind11::arg("columnNames"),
      pybind11::arg("scaleFactor") = 1.0,
      pybind11::arg("numSplits") = 1,
      "Create a Substrait ReadRel using TpchExtensionTable and return bytes.");
}

} // namespace bytedance::bolt::python
