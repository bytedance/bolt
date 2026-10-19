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

#include <pybind11/pybind11.h>

#include "bolt/core/PlanNode.h"

namespace bytedance::bolt::python {

class PythonNode : public ::bytedance::bolt::core::PlanNode {
 public:
  PythonNode(
      const ::bytedance::bolt::core::PlanNodeId& id,
      bytedance::bolt::core::PlanNodePtr source,
      const std::string functionName,
      const pybind11::function function,
      const pybind11::args args,
      const pybind11::kwargs kwargs,
      ::bytedance::bolt::RowTypePtr outputType);

  const std::string& functionName() const {
    return functionName_;
  }
  const pybind11::function& function() const {
    return function_;
  }
  const pybind11::args& args() const {
    return args_;
  }
  const pybind11::kwargs& kwargs() const {
    return kwargs_;
  }

  // PlanNode override
  const ::bytedance::bolt::RowTypePtr& outputType() const override;
  const std::vector<::bytedance::bolt::core::PlanNodePtr>& sources()
      const override;
  std::string_view name() const override;
  void addDetails(std::stringstream& stream) const override;
  folly::dynamic serialize() const override;
  static ::bytedance::bolt::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  std::string functionName_;
  const pybind11::function function_;
  const pybind11::args args_;
  const pybind11::kwargs kwargs_;
  ::bytedance::bolt::RowTypePtr outputType_;
  std::vector<::bytedance::bolt::core::PlanNodePtr> sources_;

  static inline constexpr std::string_view kFunctionName = "function_name";
  static inline constexpr std::string_view kFunction = "function";
  static inline constexpr std::string_view kArgs = "args";
  static inline constexpr std::string_view kKwargs = "kwargs";
  static inline constexpr std::string_view kOutputType = "output_type";
  static inline constexpr std::string_view kNodeId = "id";
  static inline constexpr std::string_view kSources = "sources";
};
} // namespace bytedance::bolt::python

// Preserve the PythonNode name used by existing planner integrations.
namespace bolt::python {
using PythonNode = ::bytedance::bolt::python::PythonNode;
} // namespace bolt::python
