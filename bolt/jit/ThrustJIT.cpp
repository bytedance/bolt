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
#include "bolt/jit/ThrustJIT.h"

#include <llvm/AsmParser/Parser.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/raw_os_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/Vectorize.h>

#include <memory>

namespace bytedance::bolt::jit {
ThrustJIT::ThrustJIT(
    std::unique_ptr<llvm::orc::ExecutionSession> execution_session,
    std::unique_ptr<llvm::orc::EPCIndirectionUtils> EPCIU,
    llvm::orc::JITTargetMachineBuilder target_machine_builder,
    llvm::DataLayout data_layout,
    ThrustJitOptions options)
    : execution_session_(std::move(execution_session)),
      EPCIU(std::move(EPCIU)),
      data_layout_(std::move(data_layout)),
      mangle_(*this->execution_session_, this->data_layout_),
      object_layer_(
          *this->execution_session_,
          []() { return std::make_unique<llvm::SectionMemoryManager>(); }),
      compile_layer_(
          *this->execution_session_,
          object_layer_,
          std::make_unique<llvm::orc::ConcurrentIRCompiler>(
              std::move(target_machine_builder))),
      optimize_layer_(
          *this->execution_session_,
          compile_layer_,
          [this](
              llvm::orc::ThreadSafeModule tsm,
              const llvm::orc::MaterializationResponsibility& R) {
            return this->optimizeModule(std::move(tsm), R);
          })
      // , COD_layer_(*this->execution_session_,
      //             optimize_layer_,
      //             this->EPCIU->getLazyCallThroughManager(),
      //             [this] { return this->EPCIU->createIndirectStubsManager();
      //             })
      ,
      main_jit_dylib_(
          this->execution_session_->createBareJITDylib("thrust-jit")),
      compile_threads_(
          llvm::hardware_concurrency(options.compiling_concurrency)),
      jit_memory_usage_limit_(options.jit_memory_usage_limit) {
  // To read the symbols from the host process
  // llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
  main_jit_dylib_.addGenerator(
      cantFail(llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(
          data_layout_.getGlobalPrefix())));

  // Register the event listener.
  mem_usage_listener_ = std::make_unique<ThrustJitMemoryUsageListener>(this);
  object_layer_.registerJITEventListener(
      *static_cast<llvm::JITEventListener*>(mem_usage_listener_.get()));

  // Set compile concurrency
  this->execution_session_->setDispatchTask(
      [this](std::unique_ptr<llvm::orc::Task> T) {
        compile_threads_.async([UnownedT = T.release()]() {
          std::unique_ptr<llvm::orc::Task> T(UnownedT);
          T->run();
        });
      });
}

/// Create a thread-safe module for IR code
llvm::orc::ThreadSafeModule ThrustJIT::CreateTSModule(std::string_view modKey) {
  // Maximize concurrency opportunities by loading every module on a
  // separate context.
  auto llvm_context = std::make_unique<llvm::LLVMContext>();

  id_++;
  auto llvm_module = std::make_unique<llvm::Module>(
      modKey.empty() ? "thrust_jit_" + std::to_string(id_) : modKey,
      *llvm_context);

  llvm_module->setDataLayout(getDataLayout());

  return llvm::orc::ThreadSafeModule(
      std::move(llvm_module), std::move(llvm_context));
}

/// Return err
bool ThrustJIT::AddIRIntoModule(const char* ir, llvm::Module* module) {
  llvm::SMDiagnostic smErr;

  llvm::MemoryBufferRef buff(ir, "<builtin IR>");
  auto err = llvm::parseAssemblyInto(
      buff, module, nullptr, smErr, nullptr, [this](llvm::StringRef s) {
        // return this->getDataLayout().getStringRepresentation();
        // Note: Make sure Layout has been set.
        return llvm::None;
      });
  if (err) {
    llvm::errs() << smErr.getMessage() << "\n";
    return true;
  }

#ifndef NDEBUG
  if (!err) {
    std::ostringstream oss;
    llvm::raw_os_ostream raw_oss(oss);
    module->print(raw_oss, nullptr, true, true);
    raw_oss.flush();
    VLOG(10) << "IR of module " << module->getModuleIdentifier() << ":\n"
             << oss.str();
  }
#endif

  return err;
}

bool ThrustJIT::AddIRIntoModule(
    std::function<bool(llvm::Module&)> irGenerator,
    llvm::Module* llvm_module) {
  auto err = irGenerator(*llvm_module);

#ifndef NDEBUG
  if (!err) {
    std::ostringstream oss;
    llvm::raw_os_ostream raw_oss(oss);
    llvm_module->print(raw_oss, nullptr, true, true);
    raw_oss.flush();
    VLOG(10) << "IR of module " << llvm_module->getModuleIdentifier() << ":\n"
             << oss.str();
  }
#endif

  return err;
}

CompiledModuleSP ThrustJIT::CompileModule(
    llvm::orc::ThreadSafeModule tsm,
    bool isGobal) { // isBobal = false

  llvm::orc::ResourceTrackerSP resource_tracker = createResourceTracker();

  std::string modId;
  std::vector<std::string> funcNames;
  tsm.withModuleDo([&modId, &funcNames](llvm::Module& m) {
    modId = m.getModuleIdentifier();
    for (auto& function : m) {
      if (function.isDeclaration()) {
        continue;
      }
      funcNames.emplace_back(std::string(function.getName()));
    }
  });

  auto err = optimize_layer_.add(resource_tracker, std::move(tsm));
  llvm::handleAllErrors(std::move(err), [&](llvm::ErrorInfoBase& eib) {
    llvm::errs() << "[JIT] CompileModule Error: " << eib.message() << '\n';
    {
      std::unique_lock lock(cache_mutex_);
      compilingFns_.erase(modId);
    }
    cv_.notify_all();
  });

  auto compiledModuleSP = std::make_shared<CompiledModuleImpl>();
  compiledModuleSP->setKey(modId);

  // Materialized functions synchronizedly.
  // !!!Note that!!!: this step is CPU-consuming.
  // in future, asynchronize this.
  for (auto&& fn : funcNames) {
    auto func = execution_session_->lookup({&main_jit_dylib_}, mangle_(fn));
    if (func) {
      llvm::JITEvaluatedSymbol& func_sym = *func;
      compiledModuleSP->setFuncPtr(fn, func_sym.getAddress());
    } else {
      llvm::errs() << llvm::toString(func.takeError()) << "\n";
      {
        std::unique_lock lock(cache_mutex_);
        compilingFns_.erase(modId);
      }
      cv_.notify_all();
      return nullptr;
    }
  }

  compiledModuleSP->setResourceTracker(std::move(resource_tracker));

  // Add into cache
  {
    std::unique_lock lock(cache_mutex_);
    lruCache_.insert(modId, compiledModuleSP);
    compilingFns_.erase(modId);
  }
  // In case that others are waiting for the same module to be finished.
  // notify them
  cv_.notify_all();

  return compiledModuleSP;
}

CompiledModuleSP ThrustJIT::LookupSymbolsInCache(const std::string& modKey) {
  std::unique_lock lock(cache_mutex_);
  auto result = lruCache_.get(modKey).value_or(nullptr);

  if (result == nullptr) {
    // If same module is compiling,
    // Just wait for the finishment
    if (compilingFns_.count(modKey) > 0) {
      cv_.wait(lock, [&modKey, this]() -> bool {
        return this->compilingFns_.count(modKey) == 0;
      });
      return lruCache_.get(modKey).value_or(nullptr);
    } else {
      // TODO: refine API.
      // Assume that it is going to compile a module
      // If others also try to compile the same module, let others to wait on
      // the cv.
      compilingFns_.insert(std::string(modKey));
    }
  }

  return result;
}

llvm::Expected<llvm::orc::ThreadSafeModule> ThrustJIT::optimizeModule(
    llvm::orc::ThreadSafeModule TSM,
    const llvm::orc::MaterializationResponsibility& R) {
  TSM.withModuleDo([this](llvm::Module& M) {
    // Create a function pass manager.
    auto FPM = std::make_unique<llvm::legacy::FunctionPassManager>(&M);

    FPM->add(llvm::createDeadCodeEliminationPass());
    FPM->add(llvm::createInstructionCombiningPass());
    FPM->add(llvm::createReassociatePass());
    FPM->add(llvm::createLoopRotatePass());
    FPM->add(llvm::createPromoteMemoryToRegisterPass());
    FPM->add(llvm::createGVNPass());
    FPM->add(llvm::createCFGSimplificationPass());
    FPM->add(llvm::createLoopVectorizePass());
    FPM->add(llvm::createSLPVectorizerPass());

    // We've manually unrolled the loop when generate IR.
    FPM->add(llvm::createLoopUnrollPass());
    // FPM->add(llvm::createGlobalOptimizerPass());
    FPM->doInitialization();

    // Run the optimizations over all functions in the module being added to
    // the JIT.
    for (auto& function : M) {
      if (function.isDeclaration()) {
        continue;
      }
      FPM->run(function);
      auto fn = std::string(function.getName());
      // auto mangled_name =
      // this->getMangledName(std::string(function.getName()));
      fn.size();
    }
  });

  return std::move(TSM);
}

ThrustJIT* ThrustJIT::getInstance() {
  static std::once_flag llvm_target_initialized;
  std::call_once(llvm_target_initialized, []() {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
  });

  static auto res = Create();
  if (!res) {
    llvm::errs() << llvm::toString(res.takeError()) << "\n";
    return nullptr;
  }
  return (*res).get();
}

llvm::Expected<std::unique_ptr<ThrustJIT>> ThrustJIT::Create(
    ThrustJitOptions options) {
  // In our case, JIT target process is current process.
  auto epc = llvm::orc::SelfExecutorProcessControl::Create();
  if (!epc) {
    return epc.takeError();
  }

  // An ExecutionSession represents a running JIT program
  auto es = std::make_unique<llvm::orc::ExecutionSession>(std::move(*epc));

  // Provides ExecutorProcessControl based indirect stubs, trampoline pool and
  // lazy call through manager.
  auto epciu =
      llvm::orc::EPCIndirectionUtils::Create(es->getExecutorProcessControl());
  if (!epciu) {
    return epciu.takeError();
  }

  (*epciu)->createLazyCallThroughManager(
      *es, llvm::pointerToJITTargetAddress(&handleLazyCallThroughError));
  if (auto err = llvm::orc::setUpInProcessLCTMReentryViaEPCIU(**epciu)) {
    return std::move(err);
  }

  // Data layout
  // llvm::orc::JITTargetMachineBuilder
  // jtmb(es->getExecutorProcessControl().getTargetTriple()); Use runtime's
  // Machine as the TargetMachine
  auto jtmb = llvm::orc::JITTargetMachineBuilder::detectHost();
  if (!jtmb) {
    return jtmb.takeError();
  }

  // Dump target machine CPU's features
  // llvm::outs() << "feature string:" << jtmb->getFeatures().getString() <<
  // "\n";
  auto dl = jtmb->getDefaultDataLayoutForTarget();
  if (!dl) {
    return dl.takeError();
  }

  return std::make_unique<ThrustJIT>(
      std::move(es),
      std::move(*epciu),
      std::move(*jtmb),
      std::move(*dl),
      options);
}

llvm::orc::ResourceTrackerSP ThrustJIT::createResourceTracker() {
  return main_jit_dylib_.createResourceTracker();
}

llvm::Expected<llvm::JITEvaluatedSymbol> ThrustJIT::lookup(
    const std::string& name) {
  return execution_session_->lookup({&main_jit_dylib_}, mangle_(name));
}

void ThrustJIT::handleLazyCallThroughError() {
  auto errMsg = "LazyCallThrough error: Could not find function body";
  llvm::errs() << errMsg;
  throw std::logic_error(errMsg);
}

ThrustJIT::~ThrustJIT() {
  // Clear cached modules before ending session
  lruCache_.clear();

  if (auto err = execution_session_->endSession()) {
    execution_session_->reportError(std::move(err));
  }
  if (auto err = EPCIU->cleanup()) {
    execution_session_->reportError(std::move(err));
  }

  compile_threads_.wait();
}

ThrustJitMemoryUsageListener::ThrustJitMemoryUsageListener(ThrustJIT* jit)
    : jit_(jit) {}

void ThrustJitMemoryUsageListener::notifyObjectLoaded(
    llvm::JITEventListener::ObjectKey key,
    const llvm::object::ObjectFile& obj,
    const llvm::RuntimeDyld::LoadedObjectInfo& loi) {
  // auto res = llvm::object::computeSymbolSizes(Obj);
  size_t sz = obj.getMemoryBufferRef().getBufferSize();
  {
    std::unique_lock lock(mutex_);
    objectSizeMap_[key] = sz;

    jit_->IncreaseMemoryUsage(sz);
  }
}

void ThrustJitMemoryUsageListener::notifyFreeingObject(
    llvm::JITEventListener::ObjectKey key) {
  std::unique_lock lock(mutex_);
  size_t sz = objectSizeMap_[key];
  objectSizeMap_.erase(key);

  jit_->DecreaseMemoryUsage(sz);
}

bool CacheEvictPred::operator()() {
  auto jit = ThrustJIT::getInstance();
  return jit->GetMemoryUsage() > jit->GetMemoryLimit();
}

void CompiledModuleImpl::setKey(const std::string& k) {
  key = k;
}
void CompiledModuleImpl::setResourceTracker(
    llvm::orc::ResourceTrackerSP&& srcTrk) {
  rt = std::move(srcTrk);
}

void CompiledModuleImpl::setFuncPtr(const std::string& fn, intptr_t funcPtr) {
  functions[fn] = funcPtr;
}

const char* CompiledModuleImpl::getKey() const noexcept {
  return key.c_str();
}

const intptr_t CompiledModuleImpl::getFuncPtr(const std::string& fn) const {
  if (functions.count(fn) > 0) {
    return (const intptr_t)functions.at(fn);
  }
  return (intptr_t)0;
}

CompiledModuleImpl::~CompiledModuleImpl() {
  // We have to manually call ResourceTracker's remove()
  // Since, by default ResourceTracker will handle resource over to the default
  // resource tracker. Even ResourceTracker is destroy, the resource
  // hold by it will not be released.
  if (rt && !rt->isDefunct()) {
    auto err = rt->remove();
    llvm::handleAllErrors(std::move(err), [&](llvm::ErrorInfoBase& eib) {
      llvm::errs() << "[JIT] Remove ResourceTracker Error: " << eib.message()
                   << '\n';
    });
  }
}

} // namespace bytedance::bolt::jit

#endif // ~ ENABLE_BOLT_JIT
