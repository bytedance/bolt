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

#include "bolt/exec/RowContainer.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/jit/PrebuiltIR.h"
#include "bolt/jit/ThrustJITv2.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

extern "C" int jit_StringViewCompareWrapper(char* l, char* r);

// Fallback store for VARCHAR/complex types — resolved at JIT link time.
extern "C" void jit_store_row_column(
    void* rowContainer,
    const bytedance::bolt::DecodedVector* decoded,
    int32_t index,
    char* row,
    int32_t column) {
  static_cast<bytedance::bolt::exec::RowContainer*>(rowContainer)
      ->store(*decoded, index, row, column);
}

namespace bytedance::bolt::jit::test {

// Build an outer function via IRBuilder that calls jit_prebuilt_add.
// PrebuiltIR::linkInto handles linking and internalization.
// AlwaysInliner (in ThrustJITv2's IR transform) inlines the kernel.
TEST(PrebuiltIRTest, inlinedCall) {
  // Force linker to import jit_StringViewCompareWrapper
  int32_t sz1{0}, sz2{0};
  ::jit_StringViewCompareWrapper(
      reinterpret_cast<char*>(&sz1), reinterpret_cast<char*>(&sz2));

  auto* jit = ThrustJITv2::getInstance();
  ASSERT_NE(jit, nullptr);

  const std::string fnName = "prebuilt_inlined_add_test";

  auto irGenerator = [jit, &fnName](llvm::Module& m) -> bool {
    // Link pre-built functions and inline them
    PrebuiltIR::linkInto(m);

    // Build outer function: int8_t fnName(int8_t a, int8_t b)
    auto& ctx = m.getContext();
    llvm::IRBuilder<> builder(ctx);

    auto* i8ty = builder.getInt8Ty();
    auto* funcTy = llvm::FunctionType::get(i8ty, {i8ty, i8ty}, false);
    auto* func = llvm::Function::Create(
        funcTy, llvm::Function::ExternalLinkage, fnName, m);

    func->getArg(0)->setName("a");
    func->getArg(1)->setName("b");

    auto* bb = llvm::BasicBlock::Create(ctx, "entry", func);
    builder.SetInsertPoint(bb);

    // Call pre-built function — already inlined by PrebuiltIR::linkInto,
    // but the declaration remains for us to call. The inliner will
    // inline this call when LLJIT optimizes the module.
    auto* callee = m.getFunction("jit_prebuilt_add");
    if (!callee) {
      llvm::errs() << "jit_prebuilt_add not found after linking\n";
      return true;
    }

    auto* result =
        builder.CreateCall(callee, {func->getArg(0), func->getArg(1)});
    builder.CreateRet(result);

    return llvm::verifyFunction(*func, &llvm::errs());
  };

  auto mod = jit->CompileModule(irGenerator, fnName);
  ASSERT_NE(mod, nullptr);

  using AddFunc = int8_t (*)(int8_t, int8_t);
  auto fn = reinterpret_cast<AddFunc>(mod->getFuncPtr(fnName));
  ASSERT_NE(fn, nullptr);

  EXPECT_EQ(fn(10, 20), 30);
  EXPECT_EQ(fn(-50, 50), 0);
  EXPECT_EQ(fn(1, -1), 0);
  EXPECT_EQ(fn(63, 64), 127);
}

// Store PoC: JIT-compiled store for fixed-width types (i8–f64, i128, ts).
//
// This is a PoC — only fixed-width types are fully inlined in bitcode.
// StringView and complex types (ARRAY/MAP/ROW) require RowContainer's
// HashStringAllocator and ContainerRowSerde, which pull in RowContainer.h.
// That header can't be compiled to bitcode with clang because of
// clang/g++ incompatibilities in the deep header chain (DecimalUtil's
// ambiguous to_chars, folly/hash's __int128 make_unsigned, etc.).
// These types fall back to an extern call (jit_store_row_column).
//
// When the project moves to clang as the host compiler, RowContainer.h
// will be clang-compatible and all types can be fully inlined.
class PrebuiltStoreTest : public exec::test::OperatorTestBase {};

TEST_F(PrebuiltStoreTest, storeKeys) {
  int32_t sz1{0}, sz2{0};
  ::jit_StringViewCompareWrapper(
      reinterpret_cast<char*>(&sz1), reinterpret_cast<char*>(&sz2));

  using namespace bytedance::bolt;
  using namespace bytedance::bolt::exec;

  auto pool = memory::memoryManager()->addLeafPool();
  std::vector<TypePtr> keyTypes = {BIGINT(), DOUBLE(), INTEGER()};
  auto numKeys = keyTypes.size();

  // Create two RowContainers with identical schema
  auto rcExisting = std::make_shared<RowContainer>(keyTypes, pool.get());
  auto rcJit = std::make_shared<RowContainer>(keyTypes, pool.get());

  // Generate test data
  VectorFuzzer::Options opts;
  opts.vectorSize = 100;
  opts.nullRatio = 0.1;
  VectorFuzzer fuzzer(opts, pool.get(), 42);

  std::vector<std::shared_ptr<DecodedVector>> decoded;
  std::vector<const DecodedVector*> decodedPtrs;
  for (size_t i = 0; i < numKeys; ++i) {
    auto vec = fuzzer.fuzzFlat(keyTypes[i]);
    decoded.emplace_back(std::make_shared<DecodedVector>(*vec));
    decodedPtrs.push_back(decoded.back().get());
  }

  // Compose JIT store function
  auto* jit = ThrustJITv2::getInstance();
  const std::string fnName = "prebuilt_store_test";

  std::vector<int32_t> offsets, nullByteOffsets;
  std::vector<uint8_t> nullMasks;
  for (size_t i = 0; i < numKeys; ++i) {
    auto col = rcJit->columnAt(i);
    offsets.push_back(col.offset());
    nullByteOffsets.push_back(col.nullByte());
    nullMasks.push_back(col.nullMask());
  }

  auto irGenerator = [&](llvm::Module& m) -> bool {
    PrebuiltIR::linkInto(m);

    auto& ctx = m.getContext();
    llvm::IRBuilder<> builder(ctx);
    auto* ptrTy = builder.getPtrTy();
    auto* i32Ty = builder.getInt32Ty();
    auto* voidTy = builder.getVoidTy();

    // void store_keys(void* rc, char* row, DecodedVector** cols, int32_t idx)
    auto* funcTy =
        llvm::FunctionType::get(voidTy, {ptrTy, ptrTy, ptrTy, i32Ty}, false);
    auto* func = llvm::Function::Create(
        funcTy, llvm::Function::ExternalLinkage, fnName, m);

    auto* rc = func->getArg(0);
    auto* row = func->getArg(1);
    auto* cols = func->getArg(2);
    auto* index = func->getArg(3);

    auto* entry = llvm::BasicBlock::Create(ctx, "entry", func);
    builder.SetInsertPoint(entry);

    // Map type to kernel name
    auto kernelName = [](TypeKind kind) -> std::string {
      switch (kind) {
        case TypeKind::INTEGER:
          return "jit_store_i32";
        case TypeKind::BIGINT:
          return "jit_store_i64";
        case TypeKind::DOUBLE:
          return "jit_store_f64";
        default:
          return "";
      }
    };

    for (size_t i = 0; i < numKeys; ++i) {
      auto* colPtr = builder.CreateGEP(ptrTy, cols, builder.getInt32(i));
      auto* dec = builder.CreateLoad(ptrTy, colPtr);

      auto name = kernelName(keyTypes[i]->kind());
      auto* kernel = m.getFunction(name);
      EXPECT_NE(kernel, nullptr) << "kernel not found: " << name;
      if (!kernel)
        return true;

      builder.CreateCall(
          kernel,
          {row,
           builder.getInt32(offsets[i]),
           dec,
           index,
           builder.getInt32(nullByteOffsets[i]),
           builder.getInt8(nullMasks[i])});
    }

    builder.CreateRetVoid();
    return llvm::verifyFunction(*func, &llvm::errs());
  };

  auto mod = jit->CompileModule(irGenerator, fnName);
  ASSERT_NE(mod, nullptr);

  using StoreKeysFunc = void (*)(void*, char*, const DecodedVector**, int32_t);
  auto storeFn = reinterpret_cast<StoreKeysFunc>(mod->getFuncPtr(fnName));
  ASSERT_NE(storeFn, nullptr);

  // Store rows using both methods
  auto numRows = opts.vectorSize;
  std::vector<char*> existingRows(numRows), jitRows(numRows);
  for (int i = 0; i < numRows; ++i) {
    existingRows[i] = rcExisting->newRow();
    jitRows[i] = rcJit->newRow();

    // Existing: per-column store
    for (size_t col = 0; col < numKeys; ++col) {
      rcExisting->store(*decoded[col], i, existingRows[i], col);
    }

    // JIT: all columns in one call
    storeFn(rcJit.get(), jitRows[i], decodedPtrs.data(), i);
  }

  // Correctness: both should produce identical row bytes
  for (int i = 0; i < numRows; ++i) {
    auto size = rcExisting->fixedRowSize();
    ASSERT_EQ(memcmp(existingRows[i], jitRows[i], size), 0)
        << "Row " << i << " differs";
  }
}

} // namespace bytedance::bolt::jit::test

#endif // ENABLE_BOLT_JIT
