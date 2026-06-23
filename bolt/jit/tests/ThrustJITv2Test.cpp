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

#include <gtest/gtest.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Verifier.h>

#include "bolt/jit/ThrustJITv2.h"

#include <array>
#include <limits>
#include <thread>

extern "C" int64_t extern_test_sum(int64_t a, int64_t b) {
  return a + b;
}
extern "C" int jit_StringViewCompareWrapper(char* l, char* r);

namespace bytedance::bolt::jit::test {

namespace {

std::function<bool(llvm::Module&)> makeExternSumIRGenerator(
    const std::string& funcName) {
  return [funcName](llvm::Module& llvmModule) -> bool {
    auto& llvmContext = llvmModule.getContext();
    llvm::IRBuilder<> builder(llvmContext);
    auto* int64Ty = builder.getInt64Ty();

    auto* calleeType =
        llvm::FunctionType::get(int64Ty, {int64Ty, int64Ty}, false);
    llvmModule.getOrInsertFunction("extern_test_sum", calleeType);

    auto* func = llvm::Function::Create(
        calleeType, llvm::Function::ExternalLinkage, funcName, llvmModule);
    auto args = func->args().begin();
    args->setName("lhs");
    (++args)->setName("rhs");

    auto* entry = llvm::BasicBlock::Create(llvmContext, "entry", func);
    builder.SetInsertPoint(entry);

    llvm::SmallVector<llvm::Value*, 2> callArgs;
    for (auto& arg : func->args()) {
      callArgs.push_back(&arg);
    }
    auto* callee = llvmModule.getFunction("extern_test_sum");
    auto* callResult = builder.CreateCall(callee, callArgs);
    auto* doubled = builder.CreateMul(callResult, builder.getInt64(2));
    builder.CreateRet(doubled);

    return llvm::verifyFunction(*func, &llvm::errs());
  };
}

std::function<bool(llvm::Module&)> makeBasicIRGenerator(
    const std::string& funcName) {
  return [funcName](llvm::Module& llvmModule) -> bool {
    auto& llvmContext = llvmModule.getContext();
    llvm::IRBuilder<> builder(llvmContext);
    auto* int64Ty = builder.getInt64Ty();

    auto* funcType =
        llvm::FunctionType::get(int64Ty, {int64Ty, int64Ty}, false);
    auto* func = llvm::Function::Create(
        funcType, llvm::Function::ExternalLinkage, funcName, llvmModule);

    auto args = func->args().begin();
    args->setName("a");
    (++args)->setName("b");

    auto* entry = llvm::BasicBlock::Create(llvmContext, "entry", func);
    builder.SetInsertPoint(entry);

    llvm::SmallVector<llvm::Value*, 2> values;
    for (auto& arg : func->args()) {
      values.push_back(&arg);
    }
    builder.CreateRet(builder.CreateAdd(values[0], values[1], "addtmp"));

    return llvm::verifyFunction(*func, &llvm::errs());
  };
}

CompiledModuleSP compileBasicModule(
    ThrustJITv2* jit,
    const std::string& funcName) {
  return jit->CompileModule(makeBasicIRGenerator(funcName), funcName);
}

} // namespace

TEST(ThrustJITv2Test, basic) {
  int32_t sz1{0};
  int32_t sz2{0};
  auto res = ::jit_StringViewCompareWrapper(
      reinterpret_cast<char*>(&sz1), reinterpret_cast<char*>(&sz2));
  ASSERT_EQ(res, 0);

  auto* jit = ThrustJITv2::getInstance();
  ASSERT_NE(jit, nullptr);

  const std::string funcName = "thrust_jit_v2_basic_sum";
  auto mod = jit->CompileModule(makeBasicIRGenerator(funcName), funcName);
  ASSERT_NE(mod, nullptr);

  using FuncProto = int64_t (*)(int64_t, int64_t);
  auto jitFunc = reinterpret_cast<FuncProto>(mod->getFuncPtr(funcName));
  ASSERT_NE(jitFunc, nullptr);
  EXPECT_EQ(jitFunc(100, 200), 300);
}

TEST(ThrustJITv2Test, compileAndCacheWithExternSymbol) {
  auto* jit = ThrustJITv2::getInstance();
  ASSERT_NE(jit, nullptr);

  const std::string funcName = "thrust_jit_v2_test_sum";
  constexpr size_t kNumThreads = 16;
  std::array<CompiledModuleSP, kNumThreads> modules;

  std::vector<std::jthread> threads;
  threads.reserve(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      modules[i] =
          jit->CompileModule(makeExternSumIRGenerator(funcName), funcName);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  auto mod = modules[0];
  ASSERT_NE(mod, nullptr);
  ASSERT_GT(mod->getCodeSize(), 0);

  for (const auto& module : modules) {
    ASSERT_NE(module, nullptr);
    EXPECT_EQ(module.get(), mod.get());
    EXPECT_EQ(module->getCodeSize(), mod->getCodeSize());
  }

  using FuncProto = int64_t (*)(int64_t, int64_t);
  auto jitFunc = reinterpret_cast<FuncProto>(mod->getFuncPtr(funcName));
  ASSERT_NE(jitFunc, nullptr);
  EXPECT_EQ(jitFunc(100, 200), 600);
}

TEST(ThrustJITv2Test, compileConcurrently) {
  auto* jit = ThrustJITv2::getInstance();
  ASSERT_NE(jit, nullptr);

  constexpr size_t kNumThreadsPerFunction = 16;
  const std::string funcNameA = "thrust_jit_v2_concurrent_a";
  const std::string funcNameB = "thrust_jit_v2_concurrent_b";

  std::array<CompiledModuleSP, kNumThreadsPerFunction> modulesA;
  std::array<CompiledModuleSP, kNumThreadsPerFunction> modulesB;

  std::vector<std::jthread> threads;
  threads.reserve(kNumThreadsPerFunction * 2);
  for (size_t i = 0; i < kNumThreadsPerFunction; ++i) {
    threads.emplace_back([&, i]() {
      modulesA[i] =
          jit->CompileModule(makeBasicIRGenerator(funcNameA), funcNameA);
    });
    threads.emplace_back([&, i]() {
      modulesB[i] =
          jit->CompileModule(makeBasicIRGenerator(funcNameB), funcNameB);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  auto modA = modulesA[0];
  auto modB = modulesB[0];
  ASSERT_NE(modA, nullptr);
  ASSERT_NE(modB, nullptr);
  ASSERT_NE(modA.get(), modB.get());

  for (const auto& module : modulesA) {
    ASSERT_NE(module, nullptr);
    EXPECT_EQ(module.get(), modA.get());
  }
  for (const auto& module : modulesB) {
    ASSERT_NE(module, nullptr);
    EXPECT_EQ(module.get(), modB.get());
  }

  using FuncProto = int64_t (*)(int64_t, int64_t);
  auto jitFuncA = reinterpret_cast<FuncProto>(modA->getFuncPtr(funcNameA));
  auto jitFuncB = reinterpret_cast<FuncProto>(modB->getFuncPtr(funcNameB));
  ASSERT_NE(jitFuncA, nullptr);
  ASSERT_NE(jitFuncB, nullptr);
  EXPECT_EQ(jitFuncA(100, 200), 300);
  EXPECT_EQ(jitFuncB(10, 20), 30);
}

TEST(ThrustJITv2Test, cacheLimit) {
  auto* jit = ThrustJITv2::getInstance();
  ASSERT_NE(jit, nullptr);

  constexpr size_t kMemoryLimit = 1 << 20;
  constexpr size_t kNumFunctions = 1 << 11; // 2048

  jit->SetMemoryLimitForTest(std::numeric_limits<size_t>::max());
  jit->ClearCacheForTest();

  auto probeModule = compileBasicModule(jit, "thrust_jit_v2_lru_probe");
  ASSERT_NE(probeModule, nullptr);
  const auto perModuleCodeSize = probeModule->getCodeSize(); // ~ 800-1000 bytes
  ASSERT_GT(perModuleCodeSize, 0);
  ASSERT_GT(perModuleCodeSize * kNumFunctions, kMemoryLimit);
  probeModule.reset();

  jit->ClearCacheForTest();
  jit->SetMemoryLimitForTest(kMemoryLimit);

  std::string evictedFuncName;
  std::string inCacheFuncName;

  for (size_t i = 0; i < kNumFunctions; ++i) {
    auto funcName = "thrust_jit_v2_lru_" + std::to_string(i);
    auto module = compileBasicModule(jit, funcName);
    ASSERT_NE(module, nullptr);

    if (i == kNumFunctions >> 8) {
      evictedFuncName = funcName;
    }
    if (i == kNumFunctions - (kNumFunctions >> 8)) {
      inCacheFuncName = funcName;
    }
  }

  EXPECT_LE(jit->GetMemoryUsage(), kMemoryLimit);
  EXPECT_EQ(jit->LookupSymbolsInCache(evictedFuncName), nullptr);

  auto inCacheModule = jit->LookupSymbolsInCache(inCacheFuncName);
  ASSERT_NE(inCacheModule, nullptr);

  using FuncProto = int64_t (*)(int64_t, int64_t);
  auto jitFunc =
      reinterpret_cast<FuncProto>(inCacheModule->getFuncPtr(inCacheFuncName));
  ASSERT_NE(jitFunc, nullptr);
  EXPECT_EQ(jitFunc(100, 200), 300);

  jit->ClearCacheForTest();
}

} // namespace bytedance::bolt::jit::test

#endif
