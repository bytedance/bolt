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
#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include "bolt/core/PlanNode.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"

namespace bytedance::bolt::substrait {

// Build a Substrait ExtensionSingleRel that embeds a Python function
// and its arguments as an Expression.EmbeddedFunction using pickling.
::substrait::ExtensionSingleRel createPythonRel(
    const ::substrait::Rel& input,
    const pybind11::function& function,
    const pybind11::args& args,
    const pybind11::kwargs& kwargs,
    bolt::RowTypePtr outputType,
    const std::vector<::substrait::extensions::SimpleExtensionDeclaration>&
        extensions = {});

// Same as above, but uses provided 'input' PlanNode as the source instead of
// converting the embedded input Rel. ``id`` is the PlanNode id assigned by
// the caller; the converter owns id allocation so multiple invocations of
// the same Python UDF in the same plan get distinct ids.
bolt::core::PlanNodePtr makePythonPlanNode(
    const std::string& id,
    const ::substrait::ExtensionSingleRel& rel,
    const bolt::core::PlanNodePtr& input);

} // namespace bytedance::bolt::substrait
