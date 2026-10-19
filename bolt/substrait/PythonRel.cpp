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
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/substrait/PythonRel.h"

#include "bolt/python/PlanNode.h"
#include "bolt/python/Utils.h"
#include "bolt/substrait/BoltToSubstraitType.h"
#include "bolt/substrait/SubstraitParser.h"
#include "bolt/substrait/SubstraitToBoltPlan.h"

using ::bytedance::bolt::python::pickle;
using ::bytedance::bolt::python::unpickle;

namespace bytedance::bolt::substrait {

::substrait::ExtensionSingleRel createPythonRel(
    const ::substrait::Rel& input,
    const pybind11::function& function,
    const pybind11::args& args,
    const pybind11::kwargs& kwargs,
    bolt::RowTypePtr outputType,
    const std::vector<::substrait::extensions::SimpleExtensionDeclaration>&
        extensions) {
  ::substrait::ExtensionSingleRel rel;

  // Set input.
  *rel.mutable_input() = input;

  // Build EmbeddedFunction with pickled Python function and arguments.
  ::substrait::Expression_EmbeddedFunction embedded;

  // Set output type.
  google::protobuf::Arena arena;
  BoltToSubstraitTypeConvertor typeConv(
      std::make_shared<SubstraitExtensionCollector>(extensions));
  *embedded.mutable_output_type() = typeConv.toSubstraitType(arena, outputType);

  // Pickle Python function and store in PythonPickleFunction.
  auto* pyfn = embedded.mutable_python_pickle_function();
  const std::string fnBytes = pickle(function);
  pyfn->set_function(fnBytes);

  // Build args as a Literal.List of binary pickles.
  ::substrait::Expression argsExpr;
  auto* argsList = argsExpr.mutable_literal()->mutable_list();
  for (auto it = args.begin(); it != args.end(); ++it) {
    // Iterator yields a handle; convert to object for our pickle helper.
    pybind11::object obj = pybind11::reinterpret_borrow<pybind11::object>(*it);
    const std::string bytes = pickle(obj);
    auto* lit = argsList->add_values();
    lit->set_binary(bytes);
  }
  *embedded.add_arguments() = std::move(argsExpr);

  // Build kwargs as a Literal.Map of key -> pickled value.
  ::substrait::Expression kwargsExpr;
  auto* m = kwargsExpr.mutable_literal()->mutable_map();
  for (auto it = kwargs.begin(); it != kwargs.end(); ++it) {
    ::substrait::Expression_Literal_Map_KeyValue* kv = m->add_key_values();
    // Key: string literal.
    std::string keyStr = pybind11::reinterpret_borrow<pybind11::str>(it->first);
    kv->mutable_key()->set_string(std::move(keyStr));
    // Value: binary pickle.
    pybind11::object vobj =
        pybind11::reinterpret_borrow<pybind11::object>(it->second);
    const std::string vbytes = pickle(vobj);
    kv->mutable_value()->set_binary(vbytes);
  }
  *embedded.add_arguments() = std::move(kwargsExpr);

  // Pack EmbeddedFunction into detail.
  rel.mutable_detail()->PackFrom(embedded);

  // Propagate output column names via Rel common hint so the converter can
  // set proper names on the resultant PlanNode.
  if (outputType) {
    // Ensure RelCommon exists with a 'direct' emit and hint for output names.
    auto* common = rel.mutable_common();
    common->mutable_direct();
    auto* hint = common->mutable_hint();
    for (const auto& name : outputType->names()) {
      hint->add_output_names(name);
    }
  }

  return rel;
}

bolt::core::PlanNodePtr makePythonPlanNode(
    const std::string& id,
    const ::substrait::ExtensionSingleRel& rel,
    const bolt::core::PlanNodePtr& input) {
  // Reuse the logic above but enforce given input.
  if (!rel.detail().Is<::substrait::Expression_EmbeddedFunction>()) {
    return nullptr;
  }

  ::substrait::Expression_EmbeddedFunction embedded;
  rel.detail().UnpackTo(&embedded);
  if (!embedded.has_python_pickle_function()) {
    return nullptr;
  }

  // Unpickle function and args/kwargs.
  pybind11::function fn =
      unpickle(embedded.python_pickle_function().function());
  pybind11::list pyArgs;
  pybind11::dict pyKwargs;
  if (embedded.arguments_size() >= 1) {
    const auto& argsExpr = embedded.arguments(0);
    if (argsExpr.has_literal() && argsExpr.literal().has_list()) {
      for (const auto& lit : argsExpr.literal().list().values()) {
        if (lit.has_binary()) {
          pyArgs.append(unpickle(lit.binary()));
        }
      }
    }
  }
  if (embedded.arguments_size() >= 2) {
    const auto& kwargsExpr = embedded.arguments(1);
    if (kwargsExpr.has_literal() && kwargsExpr.literal().has_map()) {
      for (const auto& kv : kwargsExpr.literal().map().key_values()) {
        if (!kv.key().has_string()) {
          BOLT_FAIL("Python kwargs key must be a string literal.");
        }
        std::string key = kv.key().string();
        if (kv.value().has_binary()) {
          pyKwargs[pybind11::str(key)] = unpickle(kv.value().binary());
        }
      }
    }
  }

  // Compute output type with optional hint overrides.
  SubstraitParser parser;
  auto outType = parser.parseType(embedded.output_type());
  if (!outType || !outType->isRow()) {
    BOLT_FAIL("Invalid python node output type. Must be a row type.")
  }

  auto rowType = asRowType(outType);
  if (rel.has_common() && rel.common().has_hint() &&
      rel.common().hint().output_names_size() > 0) {
    const auto& hint = rel.common().hint();
    std::vector<std::string> names;
    names.reserve(rowType->size());
    for (const auto& n : hint.output_names()) {
      names.emplace_back(n);
    }
    auto types = rowType->children();
    rowType = ROW(std::move(names), std::move(types));
  }

  std::string functionName;
  try {
    functionName = pybind11::str(fn.attr("__name__"));
  } catch (...) {
    functionName = "python";
  }

  return std::make_shared<::bytedance::bolt::python::PythonNode>(
      id,
      input,
      std::move(functionName),
      std::move(fn),
      std::move(pyArgs),
      std::move(pyKwargs),
      std::move(rowType));
}

} // namespace bytedance::bolt::substrait
