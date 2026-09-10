/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
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
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "bolt/expression/VectorFunction.h"
namespace bytedance::bolt::functions::sparksql {

/// elt(n, input1, input2, ...) -> input_n
///
/// Returns the n-th input, where n is 1-based. Supported types for the
/// variadic inputs are VARCHAR and VARBINARY.
///
/// Null behavior (matches Spark with spark.sql.ansi.enabled = false):
///   - Returns NULL when n is NULL.
///   - Returns NULL when n is out of the range [1, number of inputs], which
///     includes n <= 0. Note this differs from element_at, which throws on
///     index 0 and treats negative indices as offsets from the end.
///   - Returns NULL when the selected input is itself NULL.
std::shared_ptr<exec::VectorFunction> makeElt(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> eltSignatures();

} // namespace bytedance::bolt::functions::sparksql
