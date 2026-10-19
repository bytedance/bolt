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

#include "bolt/core/PlanNode.h"

using namespace ::bytedance::bolt;

namespace bytedance::bolt::python {

void addSortOrderBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::class_<core::SortOrder>(
      m, "SortOrder", pybind11::module_local(asModuleLocalDefinitions))
      .def(
          pybind11::init<bool, bool>(),
          pybind11::arg("ascending"),
          pybind11::arg("nullsFirst"))

      .def("isAscending", &core::SortOrder::isAscending)
      .def("isNullsFirst", &core::SortOrder::isNullsFirst)

      .def(
          "__repr__",
          [](const core::SortOrder& order) {
            return fmt::format("SortOrder({})", order.toString());
          })
      .def("__str__", &core::SortOrder::toString)

      .def("__eq__", &core::SortOrder::operator==, pybind11::arg("other"))
      .def("__ne__", &core::SortOrder::operator!=, pybind11::arg("other"))
      .def("serialize", &core::SortOrder::serialize)
      .def_static("deserialize", &core::SortOrder::deserialize)
      .def_readonly_static("ASC_NULLS_FIRST", &core::kAscNullsFirst)
      .def_readonly_static("ASC_NULLS_LAST", &core::kAscNullsLast)
      .def_readonly_static("DESC_NULLS_FIRST", &core::kDescNullsFirst)
      .def_readonly_static("DESC_NULLS_LAST", &core::kDescNullsLast);
}

} // namespace bytedance::bolt::python
