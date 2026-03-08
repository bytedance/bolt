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

#include "bolt/core/SimpleFunctionMetadata.h"
#include "bolt/expression/SignatureBinder.h"
#include "bolt/expression/SimpleFunctionAdapter.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::exec {

template <typename T>
const std::shared_ptr<const T>& singletonUdfMetadata() {
  static auto instance = std::make_shared<const T>();
  return instance;
}

using Function = SimpleFunctionAdapterFactory;
using Metadata = core::ISimpleFunctionMetadata;
using FunctionFactory = std::function<std::unique_ptr<Function>()>;

struct FunctionEntry {
  FunctionEntry(
      const std::shared_ptr<const Metadata>& metadata,
      const FunctionFactory& factory)
      : metadata_{metadata}, factory_{factory} {}

  const Metadata& getMetadata() const {
    return *metadata_;
  }

  std::unique_ptr<Function> createFunction() const {
    return factory_();
  }

 private:
  const std::shared_ptr<const Metadata> metadata_;
  const FunctionFactory factory_;
};

using SignatureMap =
    std::unordered_map<FunctionSignature, std::unique_ptr<const FunctionEntry>>;
using FunctionMap = std::unordered_map<std::string, SignatureMap>;

class SimpleFunctionRegistry {
 public:
  template <typename UDF>
  void registerFunction(const std::vector<std::string>& aliases = {}) {
    const auto& metadata = singletonUdfMetadata<typename UDF::Metadata>();
    const auto factory = [metadata]() { return CreateUdf<UDF>(); };

    if (aliases.empty()) {
      registerFunctionInternal(metadata->getName(), metadata, factory);
    } else {
      for (const auto& name : aliases) {
        registerFunctionInternal(name, metadata, factory);
      }
    }
  }

  std::vector<std::string> getFunctionNames() const {
    std::vector<std::string> result;
    registeredFunctions_.withRLock([&](const auto& map) {
      result.reserve(map.size());

      for (const auto& entry : map) {
        result.push_back(entry.first);
      }
    });
    return result;
  }

  void clearRegistry() {
    registeredFunctions_.withWLock([&](auto& map) { map.clear(); });
  }

  std::vector<std::shared_ptr<const FunctionSignature>> getFunctionSignatures(
      const std::string& name) const;

  std::unordered_map<
      std::string,
      std::vector<std::shared_ptr<const exec::FunctionSignature>>>
  getFunctionSignatureMap() const;

  class ResolvedSimpleFunction {
   public:
    ResolvedSimpleFunction(
        const FunctionEntry& functionEntry,
        const TypePtr& type)
        : functionEntry_(functionEntry), type_(type) {}

    auto createFunction() {
      return functionEntry_.createFunction();
    }

    TypePtr& type() {
      return type_;
    }

    const Metadata& getMetadata() const {
      return functionEntry_.getMetadata();
    }

   private:
    const FunctionEntry& functionEntry_;
    TypePtr type_;
  };

  std::optional<ResolvedSimpleFunction> resolveFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes) const;

 private:
  template <typename T>
  static std::unique_ptr<T> CreateUdf() {
    return std::make_unique<T>();
  }

  void registerFunctionInternal(
      const std::string& name,
      const std::shared_ptr<const Metadata>& metadata,
      const FunctionFactory& factory);

  folly::Synchronized<FunctionMap> registeredFunctions_;
};

const SimpleFunctionRegistry& simpleFunctions();

SimpleFunctionRegistry& mutableSimpleFunctions();

// This function should be called once and alone.
template <typename UDFHolder>
void registerSimpleFunction(const std::vector<std::string>& names) {
  mutableSimpleFunctions()
      .registerFunction<SimpleFunctionAdapterFactoryImpl<UDFHolder>>(names);
}

} // namespace bytedance::bolt::exec
