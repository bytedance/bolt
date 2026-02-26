/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#ifdef ENABLE_BOLT_JIT

#include <memory>
#include <string>
#include <vector>

#include "bolt/type/Type.h"

namespace bytedance::bolt::jit {

// Use abstract class to separate LLVM dependencies to avoid fmt::fmt v8.0.1
// compiling error. Since Impl class has to hold llvm::orc::ResourceTrackerSP
struct CompiledModule {
  virtual const char* getKey() const noexcept {
    return nullptr;
  }
  virtual const intptr_t getFuncPtr(const std::string& fn) const {
    return (intptr_t)0;
  }

  virtual void setKey(const std::string& key) {}
  virtual void setFuncPtr(const std::string& fn, intptr_t funcPtr) {}

  virtual void setCachedTypes(std::vector<bytedance::bolt::TypePtr>&) {}

  virtual ~CompiledModule(){};
};

using CompiledModuleSP = std::shared_ptr<CompiledModule>;

} // namespace bytedance::bolt::jit

#endif // ~ENABLE_BOLT_JIT
