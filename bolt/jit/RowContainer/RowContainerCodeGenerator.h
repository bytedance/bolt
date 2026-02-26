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
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Value.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/MDBuilder.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>

#include "bolt/jit/common.h"
#include "bolt/type/Type.h"

#include <cstdint>
#include <string>
#include <vector>

namespace bytedance::bolt::jit {

struct CompareFlags {
  bool nullsFirst = true;
  bool ascending = true;
};

enum class CmpType { SORT_LESS, EQUAL, CMP, CMP_SPILL };

using PhiNodeInputs = std::vector<std::pair<llvm::Value*, llvm::BasicBlock*>>;

// casti1toi8
inline llvm::Value*
castToI8(llvm::IRBuilder<>& builder, llvm::Value* val, bool isSigned = false) {
  return builder.CreateIntCast(val, builder.getInt8Ty(), isSigned);
}

/// Generate IR code for RowContainer::compare()
class RowContainerCodeGenerator {
 public:
  RowContainerCodeGenerator() = default;

  CompiledModuleSP codegen();

  virtual bool GenCmpIR();
  virtual std::string GetCmpFuncName();

  // Pass in arguments for codege
  RowContainerCodeGenerator& setCompareFlags(std::vector<CompareFlags>&& flags);
  RowContainerCodeGenerator& setKeyTypes(
      std::vector<bytedance::bolt::TypePtr>&& keyTypes);
  RowContainerCodeGenerator& setKeyOffsets(std::vector<int32_t>&& offsets);
  RowContainerCodeGenerator& setNullByteOffsets(
      std::vector<int32_t>&& nullByteOffsets);
  RowContainerCodeGenerator& setNullMasks(std::vector<int8_t>&& nullByteMasks);
  RowContainerCodeGenerator& setModule(llvm::Module* m);
  RowContainerCodeGenerator& setHasNullKeys(bool hasNullKeys);
  RowContainerCodeGenerator& setOpType(CmpType cmpType);
  bool isEqualOp() const {
    return cmpType == CmpType::EQUAL;
  }

  bool isSortLess() const {
    return cmpType == CmpType::SORT_LESS;
  }

  bool isCmp() const {
    return cmpType == CmpType::CMP || cmpType == CmpType::CMP_SPILL;
  }

  bool isCmpSpill() const {
    return cmpType == CmpType::CMP_SPILL;
  }

  // For compare non-contiguous stringview, which allocated by
  // HashStringAllocator,  in the RowContainer
  const std::string rowStringViewCompareAsc = "jit_StringViewCompareWrapper";
  const std::string RowBasedStringViewCompare = "jit_RowBasedStringViewCompare";
  const std::string ComplexTypeRowCmpRow = "jit_ComplexTypeRowCmpRow";
  const std::string RowBased_ComplexTypeRowCmpRow =
      "jit_RowBased_ComplexTypeRowCmpRow";
  const std::string StringViewRowEqVectors = "jit_StringViewRowEqVectors";
  const std::string GetDecodedValueBool = "jit_GetDecodedValueBool";
  const std::string GetDecodedValueI8 = "jit_GetDecodedValueI8";
  const std::string GetDecodedValueI16 = "jit_GetDecodedValueI16";
  const std::string GetDecodedValueI32 = "jit_GetDecodedValueI32";
  const std::string GetDecodedValueI64 = "jit_GetDecodedValueI64";
  const std::string GetDecodedValueI128 = "jit_GetDecodedValueI128";
  const std::string GetDecodedValueFloat = "jit_GetDecodedValueFloat";
  const std::string GetDecodedValueDouble = "jit_GetDecodedValueDouble";
  const std::string CmpRowVecTimestamp = "jit_CmpRowVecTimestamp";
  const std::string GetDecodedValueStringView = "jit_GetDecodedValueStringView";
  const std::string GetDecodedIsNull = "jit_GetDecodedIsNull";
  const std::string ComplexTypeRowEqVectors = "jit_ComplexTypeRowEqVectors";
  const std::string DebugPrint = "jit_DebugPrint";

  static const std::string builtInDeclarationsIR;

 protected:
  /// util functions
  std::string getLabel(size_t i);

  inline llvm::CallInst* createCall(
      llvm::IRBuilder<>& builder,
      const std::string& name,
      const std::vector<llvm::Value*>& args) {
    auto callee = llvm_module->getFunction(name);
    return builder.CreateCall(callee, args);
  }

  inline llvm::LoadInst* getValueByPtr(
      llvm::IRBuilder<>& builder,
      llvm::Value* row,
      llvm::Type* dataType,
      size_t offset) {
    auto addr =
        builder.CreateConstInBoundsGEP1_64(builder.getInt8Ty(), row, offset);
    auto cast_addr = builder.CreatePointerCast(addr, dataType->getPointerTo());
    return builder.CreateLoad(dataType, cast_addr);
  };

  /// \param values:  pointers for left row and right row
  /// \param idx:  key index
  /// \param func:  compare function body
  /// \param curr_blk: block for current key
  /// \param next_blk: block for next key, passed in for branch jump
  /// \param phi_inputs: for phi node (the return result of this function)
  /// \return: block for value comparison
  virtual llvm::BasicBlock* genNullBitCmpIR(
      const llvm::SmallVector<llvm::Value*>& values, // left row, right row
      const size_t idx,
      llvm::Function* func,
      llvm::BasicBlock* curr_blk,
      llvm::BasicBlock* next_blk,
      llvm::BasicBlock* phi_blk,
      PhiNodeInputs& phi_inputs);

  virtual llvm::BasicBlock* genFloatPointCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values, // left row, right row
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phi_inputs,
      llvm::BasicBlock* curr_blk,
      llvm::BasicBlock* phi_blk);

  virtual llvm::BasicBlock* genIntegerCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values, // left row, right row
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phi_inputs,
      llvm::BasicBlock* curr_blk,
      llvm::BasicBlock* phi_blk);

  virtual llvm::BasicBlock* genStringViewCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values, // left row, right row
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phi_inputs,
      llvm::BasicBlock* curr_blk,
      llvm::BasicBlock* phi_blk);

  virtual llvm::BasicBlock* genTimestampCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values, // left row, right row
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phi_inputs,
      llvm::BasicBlock* curr_blk,
      llvm::BasicBlock* phi_blk);

  virtual llvm::BasicBlock* genComplexCmpIR(
      bytedance::bolt::TypeKind kind,
      const llvm::SmallVector<llvm::Value*>& values, // left row, right row
      const size_t idx,
      llvm::Function* func,
      PhiNodeInputs& phi_inputs,
      llvm::BasicBlock* curr_blk,
      llvm::BasicBlock* phi_blk);

 protected:
  llvm::Module* llvm_module;

  std::vector<CompareFlags> flags;
  std::vector<bytedance::bolt::TypePtr> keysTypes;
  std::vector<int32_t> nullByteOffsets;
  std::vector<int8_t> nullByteMasks;
  std::vector<int32_t> keyOffsets;
  bool hasNullKeys{true};
  CmpType cmpType{CmpType::SORT_LESS};
  const size_t nullsOffset{0};
};

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
