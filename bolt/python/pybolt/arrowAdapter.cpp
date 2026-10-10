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

#include "bolt/python/pybolt/context.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::python {

void addArrowAdapterBindings(pybind11::module& m) { // NOLINT
  m.def("exportToArrow", [](const VectorPtr& inputVector) {
    auto arrowArray = std::make_unique<ArrowArray>();
    auto* pool = PyBoltContext::getSingletonInstance().pool();
    bytedance::bolt::exportToArrow(
        inputVector, *arrowArray, pool, ArrowOptions{});

    auto arrowSchema = std::make_unique<ArrowSchema>();
    bytedance::bolt::exportToArrow(inputVector, *arrowSchema, ArrowOptions{});

    pybind11::module arrowModule = pybind11::module::import("pyarrow");
    pybind11::object arrayClass = arrowModule.attr("Array");
    return arrayClass.attr("_import_from_c")(
        reinterpret_cast<uintptr_t>(arrowArray.get()),
        reinterpret_cast<uintptr_t>(arrowSchema.get()));
  });

  m.def("importFromArrow", [](const pybind11::object& inputArrowArray) {
    auto arrowArray = std::make_unique<ArrowArray>();
    auto arrowSchema = std::make_unique<ArrowSchema>();
    inputArrowArray.attr("_export_to_c")(
        reinterpret_cast<uintptr_t>(arrowArray.get()),
        reinterpret_cast<uintptr_t>(arrowSchema.get()));
    auto arrowOpt =
        ArrowOptions{.flattenDictionary = true, .flattenConstant = true};
    auto* pool = PyBoltContext::getSingletonInstance().pool();
    return importFromArrowAsOwner(*arrowSchema, *arrowArray, arrowOpt, pool);
  });
}
} // namespace bytedance::bolt::python
