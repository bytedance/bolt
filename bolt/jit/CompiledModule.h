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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace bytedance::bolt::jit {

struct CompiledModule {
  const char* getKey() const noexcept;
  intptr_t getFuncPtr(const std::string& fn) const;
  size_t getCodeSize() const noexcept;

  void setKey(const std::string& key);
  void setFuncPtr(const std::string& fn, intptr_t funcPtr);
  void setCodeSize(size_t codeSize);
  void appendCleanCallback(std::function<void()> cleanCallback);

  // type-erasured user data, CompiledModule does not own the data and won't
  // manage its lifetime. The caller should make sure the data is valid during
  // the module's lifetime.
  void setUserData(void* data) noexcept;
  void* getUserData() const noexcept;

  ~CompiledModule();

 private:
  std::string key_;
  std::unordered_map<std::string, intptr_t> functions_;
  size_t codeSize_{0};
  std::vector<std::function<void()>> cleanCallbacks_;

  void* userData_{nullptr};
};

using CompiledModuleSP = std::shared_ptr<CompiledModule>;

} // namespace bytedance::bolt::jit

#endif // ~ENABLE_BOLT_JIT
