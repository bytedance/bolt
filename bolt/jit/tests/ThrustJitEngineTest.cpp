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
#include <llvm/IR/MDBuilder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>

#include "bolt/jit/ThrustJIT.h"
#include "bolt/jit/tests/JitTestBase.h"

#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <regex>
#include <stdexcept>
#include <string>
#include <thread>

extern "C" {

int64_t extern_test_sum(int64_t a, int64_t b) {
  return a + b;
}

extern int jit_StringViewCompareWrapper(char* l, char* r);

} // ~ extern

namespace bytedance::bolt::jit::test {

class JitEngineTest : public ::testing::Test {
 public:
  JitEngineTest() {
    jit = bytedance::bolt::jit::ThrustJIT::getInstance();
  };

  // TODO: Replace this destructor with any teardown functionality you need
  ~JitEngineTest() override = default;

  // void SetUp() override { pool_ = arrow::default_memory_pool(); }
 protected:
  bytedance::bolt::jit::ThrustJIT* jit{nullptr};
};

using namespace bolt::jit;

TEST_F(JitEngineTest, basic) {
  // Force the linker import symbol 'jit_StringViewCompareWrapper'
  int32_t sz1{0};
  int32_t sz2{0};
  auto res = ::jit_StringViewCompareWrapper(
      reinterpret_cast<char*>(&sz1), reinterpret_cast<char*>(&sz2));
  ASSERT_TRUE(res == 0);

  std::string fn = "sum_test2";
  auto tsm = jit->CreateTSModule(fn);

  const char* ir = R"IR(
       ;declare i32 @llvm.bswap.i32(i32)

        ;eclare i64 @llvm.bswap.i64(i64)

        declare i64 @extern_test_sum(i64 noundef, i64 noundef) local_unnamed_addr

        define i64 @sum_test(i64 noundef %0, i64 noundef %1)  {
        %3 = add nsw i64 %1, %0
        ret i64 %3
        }
    )IR";

  auto IRGen = [&fn](llvm::Module& llvm_module) -> bool {
    auto& llvm_context = llvm_module.getContext();
    llvm::IRBuilder<> builder(llvm_context);

    auto int8ty = builder.getInt8Ty();

    // Declaration:
    // int64_t sum_int64_int64(int64_t a, int64_t b)  { return a + b; }
    llvm::FunctionType* func_type = llvm::FunctionType::get(
        int8ty,
        {int8ty, int8ty},
        /*isVarArg=*/false);
    llvm::Function* func = llvm::Function::Create(
        func_type, llvm::Function::ExternalLinkage, fn, llvm_module);

    unsigned int idx = 0;
    std::vector<std::string> args_name{"a", "b"};
    for (auto& arg : func->args()) {
      arg.setName(args_name[idx++]);
    }

    llvm::BasicBlock* bb =
        llvm::BasicBlock::Create(llvm_context, "entry", func);
    builder.SetInsertPoint(bb);

    auto createAlloca = [&func, &llvm_context](const std::string& name) {
      llvm::IRBuilder<> TmpB(
          &func->getEntryBlock(), func->getEntryBlock().begin());
      llvm::AllocaInst* alloc =
          TmpB.CreateAlloca(llvm::Type::getInt8Ty(llvm_context), nullptr, name);
      return alloc;
    };

    llvm::SmallVector<llvm::Value*, 2> values;
    for (auto& arg : func->args()) {
      auto name = arg.getName().str();
      llvm::Value* alloc = createAlloca(name);
      builder.CreateStore(&arg, alloc);

      auto* load = builder.CreateLoad(int8ty, alloc, name);
      values.emplace_back(load);
    }

    auto ret = builder.CreateAdd(values[0], values[1], "addtmp");
    builder.CreateRet(ret);

    bool err = llvm::verifyFunction(*func);

// func->print(llvm::errs());
#ifndef NDEBUG
    llvm_module.dump();
#endif
    return err;
  };

  tsm.withModuleDo([&, this](llvm::Module& m) {
    bool err = jit->AddIRIntoModule(ir, &m);
    ASSERT_TRUE(!err);
    err = jit->AddIRIntoModule(IRGen, &m);
    ASSERT_TRUE(!err);
  });

  CompiledModuleSP mod = jit->CompileModule(std::move(tsm));

  typedef int64_t (*FuncProto)(int64_t, int64_t);

  auto jitFunc = (FuncProto)mod->getFuncPtr(fn);
  if (jitFunc) {
    auto result = jitFunc(100, 200);
    ASSERT_TRUE(result == 300);
  }
}

TEST_F(JitEngineTest, cacheLimit) {
  const std::string irTmpl = R"IR(
        define i64 @function_name(i64 noundef %0, i64 noundef %1)  {
        %3 = add nsw i64 %1, %0
        ret i64 %3
        }
    )IR";

  constexpr size_t LIMIT = 1024;
  jit->GetCache().clear();
  jit->SetMemoryLimit(LIMIT);

  for (auto i = 0; i < 128; ++i) {
    std::string fn = "test_func_" + std::to_string(i);
    std::regex p("function_name");
    std::string ir = std::regex_replace(irTmpl, p, fn);

    auto tsm = jit->CreateTSModule(fn);
    tsm.withModuleDo([&, this](llvm::Module& m) {
      bool err = jit->AddIRIntoModule(ir.c_str(), &m);
      ASSERT_TRUE(!err);
    });

    // 768 bytes
    CompiledModuleSP mod = jit->CompileModule(std::move(tsm));

    typedef int64_t (*FuncProto)(int64_t, int64_t);

    auto jitFunc = (FuncProto)mod->getFuncPtr(fn);
    if (jitFunc) {
      auto result = jitFunc(100, 200);
      ASSERT_TRUE(result == 300);
    }

    // Check Resource Usage
    ASSERT_LT(jit->GetMemoryUsage(), 2 * LIMIT);

    {
      auto mod = jit->LookupSymbolsInCache(fn);
      ASSERT_TRUE(mod != nullptr);
      auto jitFunc = (FuncProto)mod->getFuncPtr(fn);
      if (jitFunc) {
        auto result = jitFunc(100, 200);
        ASSERT_TRUE(result == 300);
      }
    }
  }

  {
    // test_func_0 CompliedModuleSP should be remove from cache.
    std::string fn = "test_func_0";
    auto mod = jit->LookupSymbolsInCache(fn);
    ASSERT_TRUE(mod == nullptr);

    // And make sure that the binary resource has been really removed.
    auto func = jit->lookup(fn);
    if (!func) {
      llvm::handleAllErrors(
          func.takeError(), [](const llvm::ErrorInfoBase& ei) {
            ASSERT_TRUE(
                ei.message().find("Symbols not found") != std::string::npos);
          });
    }
  }
}

TEST_F(JitEngineTest, concurreny) {
  auto fn = "test_func_name";

  const std::string ir = R"IR(
        define i64 @test_func_name(i64 noundef %0, i64 noundef %1)  {
        %3 = add nsw i64 %1, %0
        ret i64 %3
        }
    )IR";

  auto codegenWorker = [&, this]() {
    auto mod = jit->LookupSymbolsInCache(fn);
    if (mod == nullptr) {
      auto tsm = jit->CreateTSModule(fn);
      tsm.withModuleDo([&, this](llvm::Module& m) {
        bool err = jit->AddIRIntoModule(ir.c_str(), &m);
        ASSERT_TRUE(!err);
      });

      mod = jit->CompileModule(std::move(tsm));
    }
    typedef int64_t (*FuncProto)(int64_t, int64_t);
    auto jitFunc = (FuncProto)mod->getFuncPtr(fn);
    if (jitFunc) {
      auto result = jitFunc(100, 200);
      ASSERT_TRUE(result == 300);
    }
  };

  std::vector<std::jthread> threads;
  for (auto i = 0; i < 256; ++i) {
    threads.emplace_back(codegenWorker);
  }
  for (auto&& t : threads) {
    t.join();
  }
}

} // namespace bytedance::bolt::jit::test

#endif
