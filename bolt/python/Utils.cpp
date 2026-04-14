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

#include "pybind11/pybind11.h"

#include "bolt/python/Utils.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ConstantVector.h"
#include "expression/EvalCtx.h"

using namespace ::bytedance::bolt;

namespace {

std::optional<pybind11::module_> pickleModule(
    pybind11::module_::import("pickle"));

// Cast a vector arg to a ConstantVector and append the matching python
// scalar value to the list of python arguments
template <TypeKind KIND>
void appendConstantValue(pybind11::list& args, BaseVector& v) {
  auto* vec = v.as<ConstantVector<typename TypeTraits<KIND>::DeepCopiedType>>();
  BOLT_CHECK_NOT_NULL(vec);
  bolt::python::pyTry<void>(
      [&]() { args.append(pybind11::cast(vec->value())); },
      [&]() {
        return fmt::format(
            "Failed to convert bolt vector of type: '{}' "
            "to python constant type.",
            v.type()->toString());
      });
}

// Append a VectorPtr to a python list of args with a direct cast
// to the pybolt python binding to VectorPtr.
void appendGenericArg(pybind11::list& args, const VectorPtr& arg) {
  bolt::python::pyTry<void>(
      [&]() { args.append(pybind11::cast(arg)); },
      [&]() {
        return fmt::format(
            "Failed to cast bolt vector of type '{}' to a "
            "python type.",
            arg->type()->toString());
      });
}
} // namespace

namespace bolt::python {
std::vector<std::shared_ptr<exec::FunctionSignature>> getSignatures(
    const TypePtr& returnType) {
  auto builder = exec::FunctionSignatureBuilder();
  builder.returnType(exec::TypeSignature(*returnType).toString());
  builder.argumentType("any");
  builder.variableArity();
  return {builder.build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> getSignatures(
    const TypePtr& returnType,
    const size_t nArgs,
    const size_t nOptionalArgs) {
  BOLT_CHECK_LE(nOptionalArgs, nArgs);

  std::string returnTypeStr = ::exec::TypeSignature(*returnType).toString();
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures.reserve(1 + nOptionalArgs);

  for (size_t n = nArgs - nOptionalArgs; n <= nArgs; n++) {
    auto signature = exec::FunctionSignatureBuilder();
    signature.returnType(returnTypeStr);
    for (size_t i = 0; i < n; i++) {
      auto var = fmt::format("T{}", i);
      signature.typeVariable(var).argumentType(var);
    }
    signatures.emplace_back(signature.build());
  }

  return signatures;
}

// Get object name/type as string without error handling.
std::string pyTypeStr(const pybind11::object& obj) {
  return pyTry<std::string>(
      [&]() { return obj.attr("__name__").cast<pybind11::str>(); });
}

std::string pyInstanceTypeStr(const pybind11::object& obj) {
  return pyTry<std::string>([&]() { return pyTypeStr(obj.attr("__class__")); });
}

std::string toString(const pybind11::object& obj) {
  return pyTry<std::string>(
      [&]() -> std::string {
        return obj.attr("__str__")().cast<pybind11::str>();
      },
      [&]() {
        return fmt::format(
            "Fail to get string representation of {}", pyTypeStr(obj));
      });
}

std::string pickle(const pybind11::object& obj) {
  return pyTry<std::string>([&]() {
    return pickleModule->attr("dumps")(obj).cast<pybind11::bytes>();
  });
}

pybind11::object unpickle(const std::string& bytes) {
  return pyTry<pybind11::object>(
      [&]() { return pickleModule->attr("loads")(pybind11::bytes(bytes)); });
}

void cleanup() {
  pickleModule.reset();
}

pybind11::list asPyArgs(
    const size_t numExpectedArgs,
    const std::vector<VectorPtr>& args,
    const std::vector<pybind11::object>& defaultArgs) {
  BOLT_CHECK_LE(args.size(), numExpectedArgs);
  BOLT_CHECK_GE(args.size() + defaultArgs.size(), numExpectedArgs);

  pybind11::list pyArgs;
  auto arg = args.cbegin();

  // Append arguments without existing default value as is.
  for (; arg != args.cbegin() + (numExpectedArgs - defaultArgs.size()); ++arg) {
    (*arg)->loadedVector();
    appendGenericArg(pyArgs, *arg);
  }

  // Arguments with a default value that is a constant type are expected to
  // be a python scalar by the user python function. However, when providing
  // a scalar value as part of a UDF expression, the value is converted into
  // a constant vector. Therefore, we need to recast the constant vector value
  // into a python scalar value
  //
  // This loop will iterate through arguments provided by the user which also
  // have a default value.
  for (; arg != args.cend(); ++arg) {
    (*arg)->loadedVector();
    if ((*arg)->encoding() == VectorEncoding::Simple::CONSTANT) {
      BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          appendConstantValue, (*arg)->type()->kind(), pyArgs, **arg);
    } else {
      appendGenericArg(pyArgs, *arg);
    }
  }

  // Iterate through remaining arguments that use the default value.
  for (auto defaultArg = defaultArgs.cend() - (numExpectedArgs - args.size());
       defaultArg != defaultArgs.cend();
       ++defaultArg) {
    pyArgs.append(*defaultArg);
  }

  return pyArgs;
}

std::shared_ptr<RowVector> combineInputColumns(
    const std::vector<VectorPtr>& args,
    const exec::EvalCtx& context) {
  std::vector<std::string> names;
  std::vector<VectorPtr> children;

  for (size_t i = 0; i < args.size(); ++i) {
    const auto& arg = args[i];
    if (arg) {
      arg->loadedVector();
      children.push_back(arg);

      std::string name;
      if (context.row()) {
        long long address = (long long)arg.get();
        for (size_t j = 0; j < context.row()->childrenSize(); ++j) {
          auto cur = context.row()->children()[j];
          if ((long long)cur.get() == address) {
            name = context.row()->type()->asRow().nameOf(j);
            break;
          }
        }
      }

      if (name.empty()) {
        name = fmt::format("col_{}", i);
      }
      names.push_back(name);
    }
  }

  if (children.empty()) {
    return RowVector::createEmpty({}, context.pool());
  }

  vector_size_t length = children[0]->size();

  std::vector<TypePtr> types;
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  auto rowType = ROW(move(names), move(types));

  return std::make_shared<RowVector>(
      context.pool(), rowType, nullptr, length, children, 0);
}
} // namespace bolt::python
