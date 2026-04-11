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

#include "bolt/jit/ThrustJITv2.h"

#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/IPO/AlwaysInliner.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils.h"

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <sstream>
#include <vector>

namespace bytedance::bolt::jit {

llvm::Expected<std::unique_ptr<ThrustJITv2>> ThrustJITv2::Create() {
  static std::once_flag llvmTargetInitialized;
  std::call_once(llvmTargetInitialized, []() {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
  });

  auto result = std::unique_ptr<ThrustJITv2>(new ThrustJITv2());
  auto jit =
      llvm::orc::LLJITBuilder()
          .setNumCompileThreads(8)
          .setObjectLinkingLayerCreator(
              [tracker = &result->codeSizeTracker_](
                  llvm::orc::ExecutionSession& executionSession,
                  const llvm::Triple&)
                  -> llvm::Expected<std::unique_ptr<llvm::orc::ObjectLayer>> {
                auto layer = std::make_unique<
                    llvm::orc::RTDyldObjectLinkingLayer>(
                    executionSession,
                    []() -> std::unique_ptr<llvm::RuntimeDyld::MemoryManager> {
                      return std::make_unique<llvm::SectionMemoryManager>();
                    });
                layer->setNotifyLoaded(
                    [tracker](
                        llvm::orc::MaterializationResponsibility& mr,
                        const llvm::object::ObjectFile& obj,
                        const llvm::RuntimeDyld::LoadedObjectInfo&) {
                      llvm::orc::ResourceKey resourceKey = 0;
                      if (auto err = mr.withResourceKeyDo(
                              [&](llvm::orc::ResourceKey key) {
                                resourceKey = key;
                              })) {
                        llvm::consumeError(std::move(err));
                        return;
                      }
                      tracker->addAllocatedBytes(
                          resourceKey,
                          obj.getMemoryBufferRef().getBufferSize());
                    });
                return std::unique_ptr<llvm::orc::ObjectLayer>(
                    std::move(layer));
              })
          .create();
  if (!jit) {
    return jit.takeError();
  }

  result->jit_ = std::move(*jit);

  // Add IR optimization pass: inline alwaysinline functions (pre-built kernels)
  // and run basic optimizations on the inlined code.
  result->jit_->getIRTransformLayer().setTransform(
      [](llvm::orc::ThreadSafeModule TSM,
         const llvm::orc::MaterializationResponsibility&)
          -> llvm::Expected<llvm::orc::ThreadSafeModule> {
        TSM.withModuleDo([](llvm::Module& M) {
          // Module-level: inline alwaysinline functions
          {
            llvm::legacy::PassManager mpm;
            mpm.add(llvm::createAlwaysInlinerLegacyPass());
            mpm.run(M);
          }

          // Per-function: clean up inlined code
          auto fpm = std::make_unique<llvm::legacy::FunctionPassManager>(&M);
          fpm->add(llvm::createPromoteMemoryToRegisterPass());
          fpm->add(llvm::createInstructionCombiningPass());
          fpm->add(llvm::createGVNPass());
          fpm->add(llvm::createCFGSimplificationPass());
          fpm->doInitialization();
          for (auto& func : M) {
            if (!func.isDeclaration()) {
              fpm->run(func);
            }
          }
          // Dump final IR for debugging (enable with --v=1)
          if (VLOG_IS_ON(1)) {
            std::string irStr;
            llvm::raw_string_ostream os(irStr);
            M.print(os, nullptr);
            VLOG(1) << "[JIT] Final IR for module '" << M.getModuleIdentifier()
                    << "':\n"
                    << irStr;
          }
        });
        return std::move(TSM);
      });

  return result;
}

ThrustJITv2* ThrustJITv2::getInstance() {
  static auto res = Create();
  if (!res) {
    llvm::errs() << llvm::toString(res.takeError()) << "\n";
    return nullptr;
  }
  return (*res).get();
}

CompiledModuleSP ThrustJITv2::CompileModule(
    std::function<bool(llvm::Module&)> irGenerator,
    const std::string& funcName) {
  {
    std::unique_lock lock(mutex_);
    if (auto cached = compiledModuleCache_.get(funcName); cached != nullptr) {
      return cached;
    }

    if (compilingFunctions_.count(funcName) > 0) {
      compilingCv_.wait(
          lock, [&]() { return compilingFunctions_.count(funcName) == 0; });
      return compiledModuleCache_.get(funcName);
    }

    compilingFunctions_.insert(funcName);
  }

  auto clearCompilingFlag = [this, &funcName]() {
    std::lock_guard lock(mutex_);
    compilingFunctions_.erase(funcName);
    compilingCv_.notify_all();
  };

  auto compileStart = std::chrono::steady_clock::now();
  auto llvmContext = std::make_unique<llvm::LLVMContext>();
  auto llvmModule = std::make_unique<llvm::Module>(funcName, *llvmContext);
  llvmModule->setDataLayout(jit_->getDataLayout());
  if (irGenerator(*llvmModule)) {
    clearCompilingFlag();
    return nullptr;
  }

  std::vector<std::string> funcNames;
  for (auto& function : *llvmModule) {
    if (!function.isDeclaration() && !function.hasInternalLinkage()) {
      funcNames.emplace_back(function.getName().str());
    }
  }

  auto resourceTracker = jit_->getMainJITDylib().createResourceTracker();
  llvm::orc::ResourceKey resourceKey = 0;
  if (auto keyErr = resourceTracker->withResourceKeyDo(
          [&](llvm::orc::ResourceKey key) { resourceKey = key; })) {
    llvm::handleAllErrors(std::move(keyErr), [&](llvm::ErrorInfoBase& eib) {
      llvm::errs() << "[JITv2] ResourceTracker key Error: " << eib.message()
                   << '\n';
    });
    clearCompilingFlag();
    return nullptr;
  }

  auto err = jit_->addIRModule(
      resourceTracker,
      llvm::orc::ThreadSafeModule(
          std::move(llvmModule), std::move(llvmContext)));
  if (err) {
    takeTrackedObjectSize(resourceKey);
    llvm::handleAllErrors(std::move(err), [&](llvm::ErrorInfoBase& eib) {
      llvm::errs() << "[JITv2] addIRModule Error: " << eib.message() << '\n';
    });
    clearCompilingFlag();
    return nullptr;
  }

  auto compiledModule = std::make_shared<CompiledModule>();
  compiledModule->setKey(funcName);
  for (const auto& fn : funcNames) {
    auto symbol = jit_->lookup(fn);
    if (!symbol) {
      takeTrackedObjectSize(resourceKey);
      llvm::handleAllErrors(symbol.takeError(), [&](llvm::ErrorInfoBase& eib) {
        llvm::errs() << "[JITv2] lookup Error: " << eib.message() << '\n';
      });
      auto removeErr = resourceTracker->remove();
      llvm::consumeError(std::move(removeErr));
      clearCompilingFlag();
      return nullptr;
    }
    compiledModule->setFuncPtr(
        fn, reinterpret_cast<intptr_t>(symbol->toPtr<void*>()));
  }

  auto codeSize = takeTrackedObjectSize(resourceKey);
  compiledModule->setCodeSize(codeSize);
  compiledModule->appendCleanCallback(
      [this, codeSize, resourceTracker]() mutable {
        if (resourceTracker && !resourceTracker->isDefunct()) {
          auto err = resourceTracker->remove();
          llvm::handleAllErrors(std::move(err), [&](llvm::ErrorInfoBase& eib) {
            llvm::errs() << "[JITv2] Remove ResourceTracker Error: "
                         << eib.message() << '\n';
          });
        }
        if (codeSize > 0) {
          decreaseMemoryUsage(codeSize);
        }
      });
  increaseMemoryUsage(codeSize);

  {
    std::lock_guard lock(mutex_);
    compiledModuleCache_.put(funcName, compiledModule);
    if (memoryUsage_.load(std::memory_order_acquire) >
            memoryLimit_.load(std::memory_order_acquire) &&
        compiledModuleCache_.size() > 1) {
      auto oldestKey = compiledModuleCache_.oldestKey();
      if (oldestKey.has_value() && *oldestKey != funcName) {
        compiledModuleCache_.erase(*oldestKey);
      }
    }
    compilingFunctions_.erase(funcName);
  }
  compilingCv_.notify_all();

  auto compileMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - compileStart)
                       .count();
  LOG(INFO) << "[JIT] Compiled '" << funcName << "' in " << compileMs
            << " ms, code size: " << codeSize << " bytes";

  return compiledModule;
}

CompiledModuleSP ThrustJITv2::LookupSymbolsInCache(
    const std::string& funcName) {
  std::lock_guard lock(mutex_);
  return compiledModuleCache_.get(funcName);
}

const llvm::DataLayout& ThrustJITv2::getDataLayout() const {
  return jit_->getDataLayout();
}

size_t ThrustJITv2::GetMemoryUsage() const noexcept {
  return memoryUsage_.load(std::memory_order_acquire);
}

void ThrustJITv2::SetMemoryLimitForTest(size_t limit) noexcept {
  memoryLimit_.store(limit, std::memory_order_release);
}

void ThrustJITv2::ClearCacheForTest() {
  std::lock_guard lock(mutex_);
  compiledModuleCache_.clear();
}

size_t ThrustJITv2::takeTrackedObjectSize(llvm::orc::ResourceKey resourceKey) {
  return codeSizeTracker_.take(resourceKey);
}

void ThrustJITv2::increaseMemoryUsage(size_t size) {
  memoryUsage_.fetch_add(size, std::memory_order_release);
}

void ThrustJITv2::decreaseMemoryUsage(size_t size) {
  memoryUsage_.fetch_sub(size, std::memory_order_release);
}

} // namespace bytedance::bolt::jit

#endif
