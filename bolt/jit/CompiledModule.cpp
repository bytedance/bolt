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

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/CompiledModule.h"

namespace bytedance::bolt::jit {

const char* CompiledModule::getKey() const noexcept {
  return key_.c_str();
}

intptr_t CompiledModule::getFuncPtr(const std::string& fn) const {
  auto it = functions_.find(fn);
  return it == functions_.end() ? 0 : it->second;
}

size_t CompiledModule::getCodeSize() const noexcept {
  return codeSize_;
}

void CompiledModule::setKey(const std::string& key) {
  key_ = key;
}

void CompiledModule::setFuncPtr(const std::string& fn, intptr_t funcPtr) {
  functions_[fn] = funcPtr;
}

void CompiledModule::setCodeSize(size_t codeSize) {
  codeSize_ = codeSize;
}

void CompiledModule::setUserData(void* data) noexcept {
  userData_ = data;
}
void* CompiledModule::getUserData() const noexcept {
  return userData_;
}

void CompiledModule::appendCleanCallback(std::function<void()> cleanCallback) {
  cleanCallbacks_.push_back(std::move(cleanCallback));
}

CompiledModule::~CompiledModule() {
  for (auto& cleanCallback : cleanCallbacks_) {
    cleanCallback();
  }
}

} // namespace bytedance::bolt::jit

#endif
