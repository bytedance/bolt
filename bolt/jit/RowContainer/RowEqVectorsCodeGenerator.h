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

#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"

namespace bytedance::bolt::jit {

/// Generate IR code for RowContainer::equals(row, decodedVectors)
class RowEqVectorsCodeGenerator : public RowContainerCodeGenerator {
 public:
  RowEqVectorsCodeGenerator() = default;

  bool GenCmpIR() override;
  std::string GetCmpFuncName() override;

 protected:
  llvm::BasicBlock* genNullBitCmpIR(
      const llvm::SmallVector<llvm::Value*>& values,
      const size_t idx,
      llvm::Function* func,
      llvm::BasicBlock* currBlk,
      llvm::BasicBlock* nextBlk,
      llvm::BasicBlock* phiBlk,
      PhiNodeInputs& phiInputs) override;

  llvm::BasicBlock* genIntegerCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values,
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phiInputs,
      llvm::BasicBlock* currBlk,
      llvm::BasicBlock* phiBlk) override;

  llvm::BasicBlock* genTimestampCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values,
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phiInputs,
      llvm::BasicBlock* currBlk,
      llvm::BasicBlock* phiBlk) override;

  llvm::BasicBlock* genFloatPointCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values,
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phiInputs,
      llvm::BasicBlock* currBlk,
      llvm::BasicBlock* phiBlk) override;

  llvm::BasicBlock* genStringViewCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values,
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phiInputs,
      llvm::BasicBlock* currBlk,
      llvm::BasicBlock* phiBlk) override;

  llvm::BasicBlock* genComplexCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values,
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phiInputs,
      llvm::BasicBlock* currBlk,
      llvm::BasicBlock* phiBlk) override;
};

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
