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

#include "bolt/python/PlanNode.h"
#include "bolt/python/Utils.h"

using namespace bytedance::bolt;

namespace bolt::python {
PythonNode::PythonNode(
    const ::bytedance::bolt::core::PlanNodeId& id,
    bytedance::bolt::core::PlanNodePtr source,
    const std::string functionName,
    const pybind11::function function,
    const pybind11::args args,
    const pybind11::kwargs kwargs,
    ::bytedance::bolt::RowTypePtr outputType)
    : core::PlanNode(id),
      functionName_(std::move(functionName)),
      function_(std::move(function)),
      args_(std::move(args)),
      kwargs_(std::move(kwargs)),
      outputType_(std::move(outputType)),
      sources_{std::move(source)} {}

const ::bytedance::bolt::RowTypePtr& PythonNode::outputType() const {
  return outputType_;
}

const std::vector<::bytedance::bolt::core::PlanNodePtr>& PythonNode::sources()
    const {
  return sources_;
}

std::string_view PythonNode::name() const {
  return functionName_;
}

void PythonNode::addDetails(std::stringstream& stream) const {
  stream << functionName_ << "(";
  for (auto arg = args_.begin(); arg != args_.end(); ++arg) {
    std::string argType = pyTry<std::string>([&]() -> std::string {
      return arg->attr("__class__").attr("__name__").cast<pybind11::str>();
    });
    stream << argType;
    if (arg + 1 != args_.end()) {
      stream << ", ";
    }
  }
  size_t i = kwargs_.size() - 1;
  for (auto kwarg = kwargs_.begin(); kwarg != kwargs_.end(); ++kwarg, --i) {
    std::string argName = pyTry<std::string>([&]() -> std::string {
      return pybind11::reinterpret_borrow<pybind11::str>(kwarg->first);
    });
    std::string argType = pyTry<std::string>([&]() {
      return kwarg->second.attr("__class__")
          .attr("__name__")
          .cast<pybind11::str>();
    });
    stream << argName << "=" << argType;
    if (i > 0) {
      stream << ", ";
    }
  }
  stream << ") -> " << outputType_->toString();
}

folly::dynamic PythonNode::serialize() const {
  auto output = core::PlanNode::serialize();
  output.insert(kFunctionName, functionName_);
  output.insert(kFunction, pickle(function_));
  output.insert(kArgs, pickle(args_));
  output.insert(kKwargs, pickle(kwargs_));
  output.insert(kOutputType, outputType_->serialize());
  return output;
}

core::PlanNodePtr PythonNode::create(const folly::dynamic& obj, void* context) {
  core::PlanNodeId node_id = obj[kNodeId].asString();
  auto sources = ISerializable::deserialize<std::vector<core::PlanNode>>(
      obj[kSources], context);
  auto source = std::move(sources[0]);
  std::string functionName = obj[kFunctionName].asString();
  pybind11::function function = unpickle(obj[kFunction].asString());
  pybind11::list args = unpickle(obj[kArgs].asString());
  pybind11::dict kwargs = unpickle(obj[kKwargs].asString());
  auto outputType = asRowType(Type::create(obj[kOutputType]));
  return std::make_shared<PythonNode>(
      node_id,
      std::move(source),
      std::move(functionName),
      std::move(function),
      std::move(args),
      std::move(kwargs),
      std::move(outputType));
}
} // namespace bolt::python
