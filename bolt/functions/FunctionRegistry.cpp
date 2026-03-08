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

#include "bolt/functions/FunctionRegistry.h"

#include <boost/algorithm/string.hpp>
#include <algorithm>
#include <iterator>
#include <optional>
#include <sstream>
#include "bolt/common/base/Exceptions.h"
#include "bolt/core/SimpleFunctionMetadata.h"
#include "bolt/expression/FunctionCallToSpecialForm.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/expression/SignatureBinder.h"
#include "bolt/expression/SimpleFunctionRegistry.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt {
namespace {

void populateVectorFunctionSignatures(FunctionSignatureMap& map) {
  auto vectorFunctions = exec::vectorFunctionFactories();
  vectorFunctions.withRLock([&map](const auto& locked) {
    for (const auto& it : locked) {
      const auto& allSignatures = it.second.signatures;
      auto& curSignatures = map[it.first];
      curSignatures.insert(
          std::end(curSignatures), allSignatures.begin(), allSignatures.end());
    }
  });
}

} // namespace

FunctionSignatureMap getFunctionSignatures() {
  const auto& simpleFunctions = exec::simpleFunctions();
  FunctionSignatureMap result = simpleFunctions.getFunctionSignatureMap();
  populateVectorFunctionSignatures(result);
  return result;
}

void clearFunctionRegistry() {
  exec::mutableSimpleFunctions().clearRegistry();
  exec::vectorFunctionFactories().withWLock(
      [](auto& functionMap) { functionMap.clear(); });
}

std::shared_ptr<const Type> resolveFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  // Check if this is a simple function.
  if (auto returnType = resolveSimpleFunction(functionName, argTypes)) {
    return returnType;
  }

  // Check if VectorFunctions has this function name + signature.
  return resolveVectorFunction(functionName, argTypes);
}

std::shared_ptr<const Type> resolveFunctionOrCallableSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  if (auto returnType = resolveCallableSpecialForm(functionName, argTypes)) {
    return returnType;
  }

  return resolveFunction(functionName, argTypes);
}

std::shared_ptr<const Type> resolveCallableSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  return exec::resolveTypeForSpecialForm(functionName, argTypes);
}

std::shared_ptr<const Type> resolveSimpleFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  if (auto resolvedFunction =
          exec::simpleFunctions().resolveFunction(functionName, argTypes)) {
    return resolvedFunction->type();
  }

  return nullptr;
}

std::shared_ptr<const Type> resolveVectorFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  return exec::resolveVectorFunction(functionName, argTypes);
}

} // namespace bytedance::bolt
