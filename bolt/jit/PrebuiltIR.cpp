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

#include "bolt/jit/PrebuiltIR.h"

#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/raw_ostream.h>

// Generated at build time by xxd -i
// Generated at build time by xxd -i (excluded from clang-tidy)
#include "kernels_bc.h"

namespace bytedance::bolt::jit {

void PrebuiltIR::linkInto(llvm::Module& target) {
  auto buffer = llvm::MemoryBuffer::getMemBuffer(
      llvm::StringRef(
          reinterpret_cast<const char*>(kernels_bc), kernels_bc_len),
      "prebuilt_kernels",
      /*RequiresNullTerminator=*/false);

  auto moduleOrErr =
      llvm::parseBitcodeFile(buffer->getMemBufferRef(), target.getContext());
  if (!moduleOrErr) {
    llvm::errs() << "[JIT] Failed to parse prebuilt bitcode: "
                 << llvm::toString(moduleOrErr.takeError()) << "\n";
    return;
  }

  auto prebuilt = std::move(*moduleOrErr);

  llvm::SmallVector<std::string, 8> prebuiltNames;
  for (auto& func : *prebuilt) {
    if (!func.isDeclaration()) {
      prebuiltNames.push_back(func.getName().str());
    }
  }

  prebuilt->setDataLayout(target.getDataLayout());
  llvm::Linker::linkModules(target, std::move(prebuilt));

  // Internalize pre-built functions so they don't get exported as
  // global symbols into the JITDylib (avoiding "duplicate definition"
  // errors). Internal functions are still callable within the same
  // module — the JIT compiler resolves them during compilation.
  for (auto& name : prebuiltNames) {
    if (auto* fn = target.getFunction(name)) {
      fn->setLinkage(llvm::GlobalValue::InternalLinkage);
    }
  }
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
