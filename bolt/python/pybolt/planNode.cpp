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

#include <pybind11/pybind11.h>

#include "bolt/core/PlanNode.h"

using namespace ::bytedance::bolt;

namespace bytedance::bolt::python {

void addPlanNodeBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::class_<core::PlanNode, std::shared_ptr<core::PlanNode>>(
      m, "PlanNode", pybind11::module_local(asModuleLocalDefinitions))
      .def("id", &core::PlanNode::id)
      .def("name", &core::PlanNode::name)
      .def("outputType", &core::PlanNode::outputType)
      .def(
          "toString",
          [](const core::PlanNode& self,
             bool detailed,
             bool recursive,
             bool withId) {
            return self.toString(detailed, recursive, withId);
          },
          pybind11::arg("detailed") = false,
          pybind11::arg("recursive") = false,
          pybind11::arg("withId") = false);
}

} // namespace bytedance::bolt::python
