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

#include <condition_variable>
#include <functional>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>

#include "bolt/jit/CompiledModule.h"

namespace bytedance::bolt::jit {

class ThrustJITv2 {
 public:
  class CodeSizeTracker {
   public:
    void addAllocatedBytes(llvm::orc::ResourceKey resourceKey, size_t size) {
      std::lock_guard lock(mutex_);
      resourceSizes_[resourceKey] += size;
    }

    size_t take(llvm::orc::ResourceKey resourceKey) {
      std::lock_guard lock(mutex_);
      auto it = resourceSizes_.find(resourceKey);
      auto codeSize = it == resourceSizes_.end() ? 0 : it->second;
      if (it != resourceSizes_.end()) {
        resourceSizes_.erase(it);
      }
      return codeSize;
    }

   private:
    std::mutex mutex_;
    std::unordered_map<llvm::orc::ResourceKey, size_t> resourceSizes_;
  };

  static ThrustJITv2* getInstance();

  CompiledModuleSP CompileModule(
      std::function<bool(llvm::Module&)> irGenerator,
      const std::string& funcName);

  CompiledModuleSP LookupSymbolsInCache(const std::string& funcName);

  const llvm::DataLayout& getDataLayout() const;

  size_t GetMemoryUsage() const noexcept;

  void SetMemoryLimitForTest(size_t limit) noexcept;
  void ClearCacheForTest();

 private:
  class Cache {
   public:
    using List = std::list<std::string>;
    using Map = std::
        unordered_map<std::string, std::pair<CompiledModuleSP, List::iterator>>;

    CompiledModuleSP get(const std::string& key) {
      auto it = entries_.find(key);
      if (it == entries_.end()) {
        return nullptr;
      }

      touch(it);
      return it->second.first;
    }

    void put(const std::string& key, CompiledModuleSP value) {
      auto it = entries_.find(key);
      if (it != entries_.end()) {
        it->second.first = std::move(value);
        touch(it);
        return;
      }

      lru_.push_front(key);
      entries_.emplace(key, std::make_pair(std::move(value), lru_.begin()));
    }

    void clear() {
      entries_.clear();
      lru_.clear();
    }

    size_t size() const noexcept {
      return entries_.size();
    }

    std::optional<std::string> oldestKey() const {
      if (lru_.empty()) {
        return std::nullopt;
      }
      return lru_.back();
    }

    CompiledModuleSP erase(const std::string& key) {
      auto it = entries_.find(key);
      if (it == entries_.end()) {
        return nullptr;
      }

      auto value = std::move(it->second.first);
      lru_.erase(it->second.second);
      entries_.erase(it);
      return value;
    }

   private:
    void touch(Map::iterator it) {
      if (it->second.second == lru_.begin()) {
        return;
      }
      lru_.splice(lru_.begin(), lru_, it->second.second);
      it->second.second = lru_.begin();
    }

    List lru_;
    Map entries_;
  };

  ThrustJITv2() = default;

  static llvm::Expected<std::unique_ptr<ThrustJITv2>> Create();

  size_t takeTrackedObjectSize(llvm::orc::ResourceKey resourceKey);
  void increaseMemoryUsage(size_t size);
  void decreaseMemoryUsage(size_t size);

 private:
  std::unique_ptr<llvm::orc::LLJIT> jit_;
  CodeSizeTracker codeSizeTracker_;
  mutable std::mutex mutex_;

  // in case compiling the same function
  std::condition_variable compilingCv_;
  std::unordered_set<std::string> compilingFunctions_;

  Cache compiledModuleCache_;

  std::atomic<size_t> memoryUsage_{0};
  std::atomic<size_t> memoryLimit_{128 * 1024 * 1024};
};

} // namespace bytedance::bolt::jit

#endif
