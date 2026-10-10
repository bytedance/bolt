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

#include "bolt/core/PlanFragment.h"

using namespace ::bytedance::bolt;

namespace bytedance::bolt::python {

void addPlanFragmentBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::class_<core::PlanFragment>(
      m, "PlanFragment", pybind11::module_local(asModuleLocalDefinitions))
      .def(
          pybind11::init<std::shared_ptr<const core::PlanNode>>(),
          pybind11::arg("topNode"))
      .def_readwrite("planNode", &core::PlanFragment::planNode)
      .def(
          "__str__",
          [](const core::PlanFragment& frag) {
            std::ostringstream oss;
            oss << "PlanFragment["
                << "strategy="
                << (frag.executionStrategy ==
                            core::ExecutionStrategy::kUngrouped
                        ? "kUngrouped"
                        : "kGrouped")
                << ", nodes=" << frag.groupedExecutionLeafNodeIds.size() << "]";
            return oss.str();
          })

      .def("__repr__", [](const core::PlanFragment& fragment) {
        return fmt::format(
            "PlanFragment(planNode={}, strategy={}, splitGroups={})",
            fragment.planNode->id(),
            fragment.executionStrategy,
            fragment.numSplitGroups);
      });
}

} // namespace bytedance::bolt::python
