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

#include <llvm/IR/Module.h>
#include <memory>

namespace bytedance::bolt::jit {

/// Pre-built JIT IR: C++ kernel functions compiled to LLVM bitcode at build
/// time (kernels.cpp → clang → .bc → xxd → embedded byte array).
///
/// Usage with ThrustJITv2:
///
///   auto irGenerator = [](llvm::Module& m) -> bool {
///     // 1. Link pre-built kernels into this module
///     PrebuiltIR::linkInto(m);
///
///     // 2. Build outer function with IRBuilder, calling pre-built kernels
///     auto* kernel = m.getFunction("jit_store_i64");
///     builder.CreateCall(kernel, {row, offset, decoded, index, ...});
///
///     // 3. Verify the generated function
///     return llvm::verifyFunction(*func, &llvm::errs());
///   };
///
///   // ThrustJITv2 compiles the module. Its AlwaysInliner pass inlines
///   // the pre-built kernels into the outer function automatically.
///   auto mod = ThrustJITv2::getInstance()->CompileModule(irGenerator, name);
///   auto fn = mod->getFuncPtr(name);
///
/// See kernels.cpp for the list of available pre-built kernels.
class PrebuiltIR {
 public:
  /// Load pre-built bitcode and link into target module.
  /// All pre-built functions are internalized (InternalLinkage) so they
  /// don't conflict across modules in the JITDylib. The AlwaysInliner
  /// pass in ThrustJITv2's IR transform layer inlines them into callers.
  static void linkInto(llvm::Module& target);
};

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
