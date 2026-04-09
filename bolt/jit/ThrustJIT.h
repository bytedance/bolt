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

#include "llvm/ADT/StringRef.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileOnDemandLayer.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/EPCIndirectionUtils.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/ExecutorProcessControl.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/IRTransformLayer.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/ThreadPool.h"

#include "bolt/jit/LRUCache.h"
#include "bolt/jit/common.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace bytedance::bolt::jit {

/// TODO: add ThrustJit doc here

// https://llvm.org/docs/ORCv2.html

struct ThrustJitOptions {
  size_t compiling_concurrency{8};

  size_t jit_memory_usage_limit{1L << 27}; // 128 M by default
};

class ThrustJIT;

struct CacheEvictPred {
  bool operator()();
};

class CompiledModuleImpl final : public CompiledModule {
 public:
  virtual const intptr_t getFuncPtr(const std::string& fn) const override;
  virtual const char* getKey() const noexcept override;

  void setKey(const std::string& k) override;
  void setFuncPtr(const std::string& fn, intptr_t funcPtr) override;

  void setResourceTracker(llvm::orc::ResourceTrackerSP&& srcTrk);
  void setObjectSize(size_t sz) {
    objectSize_ = sz;
  }
  void setMemoryUsageCounter(std::shared_ptr<std::atomic<size_t>> counter) {
    memoryUsageCounter_ = std::move(counter);
  }

  void setCachedTypes(std::vector<bytedance::bolt::TypePtr>& types) override {
    for (auto& t : types) {
      switch (t->kind()) {
        case bytedance::bolt::TypeKind::ARRAY:
        case bytedance::bolt::TypeKind::ROW:
        case bytedance::bolt::TypeKind::MAP:
          cachedTypes.push_back(t);
          break;
        default:
          break;
      }
    }
  }

  virtual ~CompiledModuleImpl();

 private:
  std::map<std::string, llvm::JITTargetAddress> functions{};
  std::string key;
  llvm::orc::ResourceTrackerSP rt{nullptr};
  size_t objectSize_{0};
  std::shared_ptr<std::atomic<size_t>> memoryUsageCounter_{};
  std::vector<bytedance::bolt::TypePtr> cachedTypes{};
};

class ThrustJIT {
 public:
  ThrustJIT(
      std::unique_ptr<llvm::orc::ExecutionSession> execution_session,
      std::unique_ptr<llvm::orc::EPCIndirectionUtils> EPCIU,
      llvm::orc::JITTargetMachineBuilder target_machine_builder,
      llvm::DataLayout data_layout,
      ThrustJitOptions options = {});

  ~ThrustJIT();

  static ThrustJIT* getInstance();

  /// Create a thread-safe module for IR code
  llvm::orc::ThreadSafeModule CreateTSModule(std::string_view modKey);

  /// Add manually written IR into module
  bool AddIRIntoModule(const char* ir, llvm::Module* module);

  /// Call IR generator and add the generated IR into module
  bool AddIRIntoModule(
      std::function<bool(llvm::Module&)> irGenerator,
      llvm::Module* module);

  /// Compile module
  ///\param   module to be compiled
  ///\param   isGobal 'true' means that code won't be removed.
  ///\return  Return a CompliedModule with Resource Tracker
  /// Note that, jit code is still not materialized.
  CompiledModuleSP CompileModule(
      llvm::orc::ThreadSafeModule tsm,
      bool isGobal = false);

  ///
  CompiledModuleSP LookupSymbolsInCache(const std::string& modKey);

  // Create a resource tracker, binding it when adding a module.
  llvm::orc::ResourceTrackerSP createResourceTracker();

  /// Symbol lookup
  llvm::Expected<llvm::JITEvaluatedSymbol> lookup(const std::string& name);

  llvm::Expected<llvm::JITEvaluatedSymbol> findSymbol(
      const llvm::StringRef& name) {
    return execution_session_->lookup({&main_jit_dylib_}, mangle_(name));
  }

  llvm::Expected<llvm::JITTargetAddress> getSymbolAddress(
      const llvm::StringRef& name) {
    auto Sym = findSymbol(name);
    if (!Sym) {
      return Sym.takeError();
    }
    return Sym->getAddress();
  }

  /// Returns a reference to the JITDylib representing the JIT'd main program.
  llvm::orc::JITDylib& getMainJITDylib() {
    return main_jit_dylib_;
  }

  const llvm::DataLayout& getDataLayout() const {
    return data_layout_;
  }

  llvm::orc::ExecutionSession& getExecutionSession() {
    return *execution_session_;
  }

  // Returns the ProcessSymbols JITDylib, which by default reflects non-JIT'd
  /// symbols in the host process.
  ///
  /// Note: JIT'd code should not be added to the ProcessSymbols JITDylib. Use
  /// the main JITDylib or a custom JITDylib instead.
  llvm::orc::JITDylibSP getProcessSymbolsJITDylib() {
    return process_symbols_;
  }

  size_t GetMemoryUsage() {
    return jit_memory_usage_->load(std::memory_order_acquire);
  }

  void IncreaseMemoryUsage(int64_t sz) {
    if (sz >= 0) {
      jit_memory_usage_->fetch_add(
          static_cast<size_t>(sz), std::memory_order_acq_rel);
      return;
    }
    jit_memory_usage_->fetch_sub(
        static_cast<size_t>(0 - sz), std::memory_order_acq_rel);
  }

  void DecreaseMemoryUsage(int64_t sz) {
    IncreaseMemoryUsage(0 - sz);
  }

  void SetMemoryLimit(size_t limit) {
    jit_memory_usage_limit_ = limit;
  }

  size_t GetMemoryLimit() const noexcept {
    return jit_memory_usage_limit_;
  }

  // for ut
  LRUCache<std::string, CompiledModuleSP, CacheEvictPred>& GetCache() {
    return lruCache_;
  }

 private:
  llvm::Expected<llvm::orc::ThreadSafeModule> optimizeModule(
      llvm::orc::ThreadSafeModule TSM,
      const llvm::orc::MaterializationResponsibility& R);

  static llvm::Expected<std::unique_ptr<ThrustJIT>> Create(
      ThrustJitOptions options = {});

  static void handleLazyCallThroughError();
  bool tryAcquireCompileSlot();
  void releaseCompileSlot();

 private:
  std::unique_ptr<llvm::orc::ExecutionSession> execution_session_;
  std::unique_ptr<llvm::orc::EPCIndirectionUtils> EPCIU;

  llvm::DataLayout data_layout_;
  llvm::orc::MangleAndInterner mangle_; // functor

  // layer. llvm::orc::CompileOnDemandLayer COD_layer_;
  llvm::orc::RTDyldObjectLinkingLayer object_layer_;

  // Compiler Layers
  llvm::orc::IRCompileLayer compile_layer_;
  llvm::orc::IRTransformLayer optimize_layer_;

  // do not use it, create new context for better concurrency
  // TODO: remove it.
  llvm::LLVMContext context_;

  llvm::orc::JITDylib& main_jit_dylib_;

  // TODO: refactor code:
  // separate DynamicLibrarySearchGenerator::GetForCurrentProcess to this:
  llvm::orc::JITDylib* process_symbols_{nullptr};

  // module id
  std::atomic<size_t> id_{0};

  std::shared_ptr<std::atomic<size_t>> jit_memory_usage_{
      std::make_shared<std::atomic<size_t>>(0)};

  std::atomic<size_t> jit_memory_usage_limit_{1L << 27};

  bool shutting_down_{false};
  size_t active_compiles_{0};
  std::mutex shutdown_mu_;
  std::condition_variable shutdown_cv_;

  // Per-module emit fence: tracks onObjEmit tasks that have called
  // NotifyEmitted (unblocking lookup) but haven't finished withResourceKeyDo
  // (storing the MemMgr in the layer).  waitForEmit() blocks only until the
  // specific module's onObjEmit task returns — not the entire pool.
  std::mutex emit_mu_;
  std::condition_variable emit_cv_;
  std::unordered_set<uintptr_t> pending_emits_;
  std::unordered_map<uintptr_t, size_t> emit_sizes_;

  // compile_threads_ must be declared AFTER all state it references
  // (emit_mu_, jit_memory_usage_, etc.) so that ~ThreadPool joins pool
  // threads before those members are destroyed.
  llvm::ThreadPool compile_threads_;

  std::mutex cache_mutex_;
  std::condition_variable cv_;
  std::set<std::string> compilingFns_;

  LRUCache<std::string, CompiledModuleSP, CacheEvictPred> lruCache_;
};

} // namespace bytedance::bolt::jit

#endif // ~ ENABLE_BOLT_JIT
