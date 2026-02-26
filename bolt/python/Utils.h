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

#include <fmt/format.h>
#include <pybind11/gil.h>
#include <pybind11/pytypes.h>

#include "bolt/common/base/BoltException.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/vector/ComplexVector.h"

namespace facebook::bolt {
class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;
} // namespace facebook::bolt

namespace bolt::python {
std::vector<std::shared_ptr<bytedance::bolt::exec::FunctionSignature>>
getSignatures(const bytedance::bolt::TypePtr& returnType);

std::vector<std::shared_ptr<bytedance::bolt::exec::FunctionSignature>>
getSignatures(
    const bytedance::bolt::TypePtr& returnType,
    const size_t nArgs,
    const size_t nOptionalArgs);

// Execute a function under the safety of the python GIL and wrap pybind11
// exceptions in a bolt exception with the provided `makeErrorString` message.
//
// Note that it is safe to nest this call (only from a single thread) as the
// GIL can be acquired multiple times within the same thread. The nested
// call will return the first exception encountered (if any).
template <typename ReturnType>
ReturnType pyTry(
    std::function<ReturnType()> function,
    std::function<std::string()> makeErrorString = nullptr) {
  try {
    pybind11::gil_scoped_acquire();
    return function();
  }
  // Short circuit.
  catch (const bytedance::bolt::BoltException&) {
    throw;
  }
  // Python exception
  catch (const pybind11::builtin_exception& e) {
    throw bytedance::bolt::BoltUserError(
        std::current_exception(),
        fmt::format(
            "{}python exception: {}",
            makeErrorString ? makeErrorString() + "\n" : "",
            e.what()),
        false);
  }
}

// Get object name/type  (`__name__') as string with graceful error
// handling.
std::string pyTypeStr(const pybind11::object& obj);

// Get object type name (`__class__.__name__') as string with graceful error
// handling.
std::string pyInstanceTypeStr(const pybind11::object& obj);

// Gracefully handle errors when attempting to evaluate `obj.__str__()`.
std::string toString(const pybind11::object& obj);

// Serialize python object.
std::string pickle(const pybind11::object& obj);

// Deserialize python object.
pybind11::object unpickle(const std::string& bytes);

// Cleanup static python object that need to be deleted before the python
// interpreter.
void cleanup();

// Convert a list of arguments to a python list of arguments while
// handling possible missing argument with existing default value.
pybind11::list asPyArgs(
    const size_t numExpectedArgs,
    const std::vector<bytedance::bolt::VectorPtr>& args,
    const std::vector<pybind11::object>& defaultArgs);

std::shared_ptr<bytedance::bolt::RowVector> combineInputColumns(
    const std::vector<bytedance::bolt::VectorPtr>& args,
    const bytedance::bolt::exec::EvalCtx& context);
} // namespace bolt::python
