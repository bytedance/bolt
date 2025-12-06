/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Instructions.h>
#include <type/Type.h>
#include <iostream>
#include <limits>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"
#include "bolt/jit/ThrustJIT.h"
#include "bolt/type/StringView.h"

namespace bytedance::bolt::jit {

CompiledModuleSP RowContainerCodeGenerator::codegen() {
  auto fn = GetCmpFuncName();

  auto jit = ThrustJIT::getInstance();

  if (auto mod = jit->LookupSymbolsInCache(fn)) {
    return mod;
  }

  // Only one fuction (RowRowCompare) generated in this module,
  // pass function name as module key
  auto tsm = jit->CreateTSModule(fn);

  // Add builtin functions declaration into this Module
  tsm.withModuleDo([jit, this](llvm::Module& m) {
    jit->AddIRIntoModule(
        RowContainerCodeGenerator::builtInDeclarationsIR.c_str(), &m);
  });

  auto genIR = [this](llvm::Module& m) -> bool {
    this->setModule(&m);
    return this->GenCmpIR();
  };

  tsm.withModuleDo([jit, &genIR, this](llvm::Module& m) {
    jit->AddIRIntoModule(genIR, &m);
  });
  auto module = jit->CompileModule(std::move(tsm));
  if (isEqualOp() || !flags.empty()) { // only for row cmp/= row
    module->setCachedTypes(keysTypes);
  }
  return module;
}

/// util functions
std::string RowContainerCodeGenerator::GetCmpFuncName() {
  std::string fn = isEqualOp() ? "jit_rr_eq"
      : isCmp()                ? "jit_rr_cmp"
                               : "jit_rr_less";
  fn.append(hasNullKeys ? "N" : "");
  for (auto i = 0; i < keysTypes.size(); ++i) {
    fn.append(keysTypes[i]->jitName());
    if (!isEqualOp()) {
      fn.append(flags[i].nullsFirst ? "F" : "L"); // nulls first / nulls last
      fn.append(flags[i].ascending ? "A" : "D"); // asc / desc
    }
  }
  for (auto i = 0; i < keyOffsets.size(); ++i) {
    fn.append(std::to_string(keyOffsets[i]));
  }

  return fn;
}

std::string RowContainerCodeGenerator::getLabel(size_t i) {
  return "key_" + std::to_string(i);
};

llvm::BasicBlock* RowContainerCodeGenerator::genNullBitCmpIR(
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* next_blk,
    llvm::BasicBlock* phi_blk,
    PhiNodeInputs& phi_inputs) {
  // ```cpp
  //   auto leftNull = * (char *) (leftRow + nullByteOffsets[idx]);
  //   auto rightNull = * (char *) (rightRow + nullByteOffsets[idx])
  //   bool isLeftNull = leftNull & nullPosMask;
  //   bool isRightNull = leftNull & nullPosMask;

  //   if (isLeftNull != isRightNull) {
  //     in order to save few instructions, here the logic seems a little bit
  //     tricky, let's take NULLS FIRST for instance:
  //     |-----------------------------------------------|
  //     |   LeftIsNull   |  RightIsNull |  left < right |
  //     -------------------------------------------------
  //     |    true        |    false     |    true       |
  //     |    false       |    true      |    false      |
  //     |-----------------------------------------------|
  //
  //     return (flags[idx].nullsFirst ? leftNull : rightNull) !=0 ;
  //
  //   }
  //   else if (isLeftNull) {
  //     goto next_key_compare;
  //   }
  // ```

  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  builder.SetInsertPoint(curr_blk);

  // isNullAt(idx)
  auto row_ty = builder.getInt8Ty();
  auto byte_ty = builder.getInt8Ty();
  llvm::PointerType* byte_ptr_ty = byte_ty->getPointerTo();
  auto const_mask = llvm::ConstantInt::get(byte_ty, nullByteMasks[idx]);

  auto void_left_addr = builder.CreateConstInBoundsGEP1_64(
      row_ty, values[0], nullByteOffsets[idx]);
  auto void_right_addr = builder.CreateConstInBoundsGEP1_64(
      row_ty, values[1], nullByteOffsets[idx]);
  auto left_addr = builder.CreatePointerCast(void_left_addr, byte_ptr_ty);
  auto right_addr = builder.CreatePointerCast(void_right_addr, byte_ptr_ty);
  auto left_val_unmask = builder.CreateLoad(byte_ty, left_addr);
  auto right_val_unmask = builder.CreateLoad(byte_ty, right_addr);
  auto left_val = builder.CreateAnd(left_val_unmask, const_mask);
  auto right_val = builder.CreateAnd(right_val_unmask, const_mask);

  auto nil_ne =
      builder.CreateICmp(llvm::ICmpInst::ICMP_NE, left_val, right_val);
  auto nil_ne_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_ne_blk", func, next_blk);
  auto nil_eq_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_eq_blk", func, next_blk);
  auto no_nil_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_no_nil_blk", func, next_blk);
  auto un_likely = llvm::MDBuilder(builder.getContext())
                       .createBranchWeights(1, 1000); //(1U << 20) - 1, 1

  builder.CreateCondBr(nil_ne, nil_ne_blk, nil_eq_blk, un_likely);

  curr_blk = nil_ne_blk;
  builder.SetInsertPoint(curr_blk);

  // Note: CreateTrunc( int8 -> int1 ) does NOT work
  auto const_byte_0 = llvm::ConstantInt::get(byte_ty, 0);

  if (isEqualOp()) { // directly return false
    phi_inputs.emplace_back(builder.getInt8(0), curr_blk);
  } else {
    auto one_op_nil = builder.CreateICmpNE(
        flags[idx].nullsFirst ? left_val : right_val, const_byte_0);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(
              one_op_nil, builder.getInt8(-1), builder.getInt8(1)),
          curr_blk);
    } else {
      phi_inputs.emplace_back(castToI8(builder, one_op_nil), curr_blk);
    }
  }
  builder.CreateBr(phi_blk);

  curr_blk = nil_eq_blk;
  builder.SetInsertPoint(curr_blk);
  builder.CreateCondBr(
      builder.CreateICmpNE(left_val, const_byte_0),
      next_blk,
      no_nil_blk,
      un_likely);

  return no_nil_blk;
}

llvm::BasicBlock* RowContainerCodeGenerator::genFloatPointCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  auto row_ty = builder.getInt8Ty();
  auto data_ty = (kind == bytedance::bolt::TypeKind::DOUBLE)
      ? builder.getDoubleTy()
      : builder.getFloatTy();
  llvm::PointerType* data_ptr_ty = data_ty->getPointerTo();

  // Block for next key.
  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);

  // Generate value comparison IR for check nullity
  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);

  uint64_t row_offset = keyOffsets[idx];
  auto void_left_addr =
      builder.CreateConstInBoundsGEP1_64(row_ty, values[0], row_offset);
  auto void_right_addr =
      builder.CreateConstInBoundsGEP1_64(row_ty, values[1], row_offset);
  auto left_addr = builder.CreatePointerCast(void_left_addr, data_ptr_ty);
  auto right_addr = builder.CreatePointerCast(void_right_addr, data_ptr_ty);
  auto left_val_raw = builder.CreateLoad(data_ty, left_addr);
  auto right_val_raw = builder.CreateLoad(data_ty, right_addr);

  bool isDouble = kind == bytedance::bolt::TypeKind::DOUBLE;
  auto const_float_0 = isDouble ? llvm::ConstantFP::get(data_ty, (double)0.0)
                                : llvm::ConstantFP::get(data_ty, (float)0.0);
  auto const_float_max = isDouble
      ? llvm::ConstantFP::get(data_ty, std::numeric_limits<double>::max())
      : llvm::ConstantFP::get(data_ty, std::numeric_limits<float>::max());

  // ====== NaN check starts ==========
  // "FCMP_UNO" : Create a quiet
  // floating-point comparison (NaN) to check if it is a NaN. References:
  // 1.
  // https://stackoverflow.com/questions/8627331/what-does-ordered-unordered-comparison-mean
  // 2. RowContainer::comparePrimitiveAsc
  // ```cpp
  //  if (leftIsNan != rightIsNan) {  // only one oprand is NaN
  //      return asc ?  rightIsNan : leftIsNan;
  //  }
  //  else if (leftIsNan)  // both is Nan
  //      goto next_key_block;
  //  } else {
  //      goto normal values compare
  // }
  // ```
  auto is_left_nan =
      builder.CreateFCmp(llvm::FCmpInst::FCMP_UNO, left_val_raw, const_float_0);
  auto is_right_nan = builder.CreateFCmp(
      llvm::FCmpInst::FCMP_UNO, right_val_raw, const_float_0);

  auto ne_nan =
      builder.CreateICmp(llvm::ICmpInst::ICMP_NE, is_left_nan, is_right_nan);
  auto ne_nan_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_ne_nan_blk", func, next_blk);
  auto eq_nan_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_eq_nan_blk", func, next_blk);
  auto no_nan_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_no_nan_blk", func, next_blk);
  // unlikely weight
  auto un_likely = llvm::MDBuilder(builder.getContext())
                       .createBranchWeights(1, 1000); //(1U << 20) - 1, 1
  builder.CreateCondBr(ne_nan, ne_nan_blk, eq_nan_blk, un_likely);

  curr_blk = ne_nan_blk;
  builder.SetInsertPoint(curr_blk);
  if (isEqualOp()) { // return false
    phi_inputs.emplace_back(builder.getInt8(0), curr_blk);
  } else {
    auto res = flags[idx].ascending ? is_right_nan : is_left_nan;
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(res, builder.getInt8(-1), builder.getInt8(1)),
          curr_blk);
    } else {
      phi_inputs.emplace_back(castToI8(builder, res), curr_blk);
    }
  }
  builder.CreateBr(phi_blk);

  curr_blk = eq_nan_blk;
  builder.SetInsertPoint(curr_blk);
  builder.CreateCondBr(
      is_left_nan, next_blk, no_nan_blk, un_likely); // if Both NaN
  // ======  NaN check ends =============================

  /*
  ```cpp
    auto cmpOp = flags[idx].ascending ? FCMP_OLT : FCMP_OGT;

    auto left_val = (double*) (leftRow + keyOffsets[idx]);
    auto right_val = (double*) (rightRow + keyOffsets[idx]);

    if constexpr (is_last_key) {
       return left_val cmpOp right_val;
    }
    else {
       if (left_val == right_val) {
         goto next_key_compare;
       }
       return return left_val cmpOp right_val;
    }
  ```
  */
  curr_blk = no_nan_blk;
  builder.SetInsertPoint(curr_blk);

  auto left_val = left_val_raw;
  auto right_val = right_val_raw;

  // Ordered return true if the operands are comparable (neither number is NaN):
  // Ordered comparison of 1.0 and 1.0 gives true.
  // Ordered comparison of NaN and 1.0 gives false.
  // Ordered comparison of NaN and NaN gives false.
  auto cmpOp = isEqualOp()   ? llvm::FCmpInst::FCMP_OEQ
      : flags[idx].ascending ? llvm::FCmpInst::FCMP_OLT
                             : llvm::FCmpInst::FCMP_OGT;

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    auto cmp_res = builder.CreateFCmp(cmpOp, left_val, right_val);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(
              cmp_res,
              builder.getInt8(-1),
              castToI8(
                  builder, builder.CreateFCmp(cmpOp, right_val, left_val))),
          curr_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, cmp_res), curr_blk));
    }
    builder.CreateBr(phi_blk);
  } else {
    // If it not the last key, firstly check if left equals with right
    auto key_eq =
        builder.CreateFCmp(llvm::FCmpInst::FCMP_OEQ, left_val, right_val);

    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, phi_blk);

    builder.CreateCondBr(key_eq, next_blk, key_ne_blk);

    builder.SetInsertPoint(key_ne_blk);
    auto res_lt = builder.CreateFCmp(cmpOp, left_val, right_val);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(res_lt, builder.getInt8(-1), builder.getInt8(1)),
          key_ne_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, res_lt), key_ne_blk));
    }
    builder.CreateBr(phi_blk);
  }
  return next_blk;
};

llvm::BasicBlock* RowContainerCodeGenerator::genIntegerCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  /*
  ```cpp
    auto cmpOp = flags[idx].ascending ? OLT : OGT;

    auto left_val = *(Integer*) (leftRow + keyOffsets[idx]);
    auto right_val = *(Integer*) (rightRow + keyOffsets[idx]);

    if constexpr (is_last_key) {
       return left_val cmpOp right_val;
    }
    else {
       if (left_val == right_val) {
         goto next_key_compare;
       }
       return return left_val cmpOp right_val;
    }
  ```
  */

  auto row_ty = builder.getInt8Ty();
  llvm::Type* data_ty = nullptr;
  if (kind == bytedance::bolt::TypeKind::BOOLEAN) {
    // Just follow Clang. Clang chose i8 over i1 for a boolean field
    data_ty = builder.getInt8Ty();
  } else if (kind == bytedance::bolt::TypeKind::TINYINT) {
    data_ty = builder.getInt8Ty();
  } else if (kind == bytedance::bolt::TypeKind::SMALLINT) {
    data_ty = builder.getInt16Ty();
  } else if (kind == bytedance::bolt::TypeKind::INTEGER) {
    data_ty = builder.getInt32Ty();
  } else if (kind == bytedance::bolt::TypeKind::BIGINT) {
    data_ty = builder.getInt64Ty();
  } else if (kind == bytedance::bolt::TypeKind::HUGEINT) {
    data_ty = builder.getInt128Ty();
  }

  // Block for next key.
  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);

  llvm::PointerType* data_ptr_ty = data_ty->getPointerTo();

  // Generate value comparison IR for check nullity
  // && flags[idx].nullsFirst == flags[idx].ascending
  // if we ignore the nullity check, we have to compare Max and Null, a bit
  // complicated, just ignore this tricky optimzation we can optimize this after
  // we refactor RowConatiner
  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);

  uint64_t row_offset = keyOffsets[idx];
  auto void_left_addr =
      builder.CreateConstInBoundsGEP1_64(row_ty, values[0], row_offset);
  auto void_right_addr =
      builder.CreateConstInBoundsGEP1_64(row_ty, values[1], row_offset);
  auto left_addr = builder.CreatePointerCast(void_left_addr, data_ptr_ty);
  auto right_addr = builder.CreatePointerCast(void_right_addr, data_ptr_ty);
  auto left_val = builder.CreateLoad(data_ty, left_addr);
  auto right_val = builder.CreateLoad(data_ty, right_addr);

  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    auto cmp_res = builder.CreateICmp(cmpOp, left_val, right_val);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(
              cmp_res,
              builder.getInt8(-1),
              castToI8(
                  builder, builder.CreateICmp(cmpOp, right_val, left_val))),
          curr_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, cmp_res), curr_blk));
    }
  } else {
    // If it not the last key, firstly check if left equals with right
    auto key_eq =
        builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, left_val, right_val);

    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);

    builder.CreateCondBr(key_eq, next_blk, key_ne_blk);

    builder.SetInsertPoint(key_ne_blk);
    auto res_lt = builder.CreateICmp(cmpOp, left_val, right_val);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(res_lt, builder.getInt8(-1), builder.getInt8(1)),
          key_ne_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, res_lt), key_ne_blk));
    }
  }
  builder.CreateBr(phi_blk);
  return next_blk;
};

llvm::BasicBlock* RowContainerCodeGenerator::genStringViewCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  /*
    ```cpp
    // prefix
    auto  cmpOp =  isAsc ? "<=" : " >=";
    auto left_prefix = *(int32_t*) (leftRow + keyOffsets[idx] +
                            sizeof(uint32_t));
    auto right_prefix = *(int32_t*) (rightRow +
                              keyOffsets[idx] + sizeof(uint32_t));
      if (left_prefix != right_prefix) {
        if constexpr ( little_endian ) {
          left_prefix = bswap (left_prefix);
          right_prefix = bswap (right_prefix);
        }
        return (left_prefix)  Op  right_prefix;
    }
    // inline part
    auto left_len = *(int32_t*) (leftRow + keyOffsets[idx] );
    auto right_len = *(int32_t*) (rightRow + keyOffsets[idx]);
    if (left_len <= 12 && right_len <= 12) {
      auto left_inline = *(int64_t*) (leftRow + keyOffsets[idx] +
                    sizeof(int64_t));
      auto right_inline = *(int64_t*) (rightRow + keyOffsets[idx]
                    + sizeof(int64_t)));
      if (left_inline != right_inline) {
        if constexpr (little_endian ) {
          left_inline = bswap (left_inline);
          right_inline = bswap (right_inline);
        }
        return (left_inline)  Op  right_inline;
      }
    }
    auto res = FastStringViewCmp(left, right);
    if constexpr (lastKey) {
      return res Op 0;
    } else {
      if (res ==0) {
        goto next_key;
      } else {
        return res Op 0;
      }
    }
    ```
    */

  // Block for next key.
  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);

  // Generate value comparison IR for check nullity
  if (hasNullKeys &&
      (isEqualOp() || flags[idx].nullsFirst == flags[idx].ascending)) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);
  auto ty = builder.getInt8Ty(); // stringViewType;
  auto left_addr =
      builder.CreateConstInBoundsGEP1_64(ty, values[0], keyOffsets[idx]);
  auto right_addr =
      builder.CreateConstInBoundsGEP1_64(ty, values[1], keyOffsets[idx]);
  auto int32_ty = builder.getInt32Ty();
  auto int64_ty = builder.getInt64Ty();

  auto left_len = getValueByPtr(builder, values[0], int32_ty, keyOffsets[idx]);
  auto right_len = getValueByPtr(builder, values[1], int32_ty, keyOffsets[idx]);

  auto inline_limit = llvm::ConstantInt::get(
      int32_ty, bytedance::bolt::StringView::kInlineSize);
  auto prefix_limit =
      builder.getInt32(bytedance::bolt::StringView::kPrefixSize);
  if (isEqualOp()) { // cmp first int64_t
    auto left_val = getValueByPtr(builder, left_addr, int64_ty, 0);
    auto right_val = getValueByPtr(builder, right_addr, int64_ty, 0);
    auto pre_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_ne_", func, next_blk);
    auto pre_eq_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_eq", func, next_blk);
    auto prefix_eq =
        builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, left_val, right_val);
    builder.CreateCondBr(prefix_eq, pre_eq_blk, pre_ne_blk);
    // If prefix is NOT equal
    builder.SetInsertPoint(pre_ne_blk);
    phi_inputs.emplace_back(
        std::make_pair(castToI8(builder, prefix_eq), pre_ne_blk));
    builder.CreateBr(phi_blk);

    // If prefix is equal
    curr_blk = pre_eq_blk;
  } else { // Check Prefix (4 chars)
    auto left_pre_addr = builder.CreateConstInBoundsGEP1_64(
        ty, values[0], keyOffsets[idx] + sizeof(uint32_t));
    auto right_pre_addr = builder.CreateConstInBoundsGEP1_64(
        ty, values[1], keyOffsets[idx] + sizeof(uint32_t));

    auto left_pre_cast_addr =
        builder.CreatePointerCast(left_pre_addr, int32_ty->getPointerTo());
    auto right_pre_cast_addr =
        builder.CreatePointerCast(right_pre_addr, int32_ty->getPointerTo());
    auto left_val = builder.CreateLoad(int32_ty, left_pre_cast_addr);
    auto right_val = builder.CreateLoad(int32_ty, right_pre_cast_addr);

    auto pre_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_ne_", func, next_blk);
    auto pre_eq_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_eq", func, next_blk);
    auto prefix_eq =
        builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, left_val, right_val);
    builder.CreateCondBr(prefix_eq, pre_eq_blk, pre_ne_blk);

    // If prefix is NOT equal
    builder.SetInsertPoint(pre_ne_blk);
    llvm::Value* pre_cmp_ne{nullptr};
    auto preOp = flags[idx].ascending ? llvm::ICmpInst::ICMP_ULT
                                      : llvm::ICmpInst::ICMP_UGT;
    if (!bolt::jit::ThrustJIT::getInstance()->getDataLayout().isBigEndian()) {
      std::vector<llvm::Value*> args;
      auto callee = llvm_module->getFunction("llvm.bswap.i32");
      args.push_back(left_val);
      auto left_swap_val = builder.CreateCall(callee, args);
      args.clear();
      args.push_back(right_val);
      auto right_swap_val = builder.CreateCall(callee, args);
      // unsigned int
      pre_cmp_ne = builder.CreateICmp(preOp, left_swap_val, right_swap_val);
    } else {
      // unsigned int
      pre_cmp_ne = builder.CreateICmp(preOp, left_val, right_val);
    }
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(
              pre_cmp_ne, builder.getInt8(-1), builder.getInt8(1)),
          pre_ne_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, pre_cmp_ne), pre_ne_blk));
    }
    builder.CreateBr(phi_blk);

    curr_blk = pre_eq_blk;
    /// equal prefix with un-equal size
    builder.SetInsertPoint(curr_blk);
    auto min_len = builder.CreateSelect(
        builder.CreateICmpULT(left_len, right_len), left_len, right_len);
    // if (min_size <= prefix_limit) return left_len cmp right_len
    auto size_cmp = builder.CreateICmpULE(min_len, prefix_limit);
    auto le_pre_limit = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_le_pre_limit_", func, next_blk);
    auto gt_pre_limit = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_gt_pre_limit_", func, next_blk);
    builder.CreateCondBr(size_cmp, le_pre_limit, gt_pre_limit);
    curr_blk = le_pre_limit;
    builder.SetInsertPoint(curr_blk);
    // if (left_len == right_len) go to inline comparison
    // else return left_len cmp right_len
    auto len_ne = builder.CreateICmpNE(left_len, right_len);
    auto le_pre_ne_len = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_le_pre_ne_len_", func, next_blk);
    builder.CreateCondBr(len_ne, le_pre_ne_len, gt_pre_limit);
    curr_blk = le_pre_ne_len;
    builder.SetInsertPoint(curr_blk);
    auto len_less = builder.CreateICmp(preOp, left_len, right_len);
    if (isCmp()) {
      auto res = builder.CreateSelect(
          len_less, builder.getInt8(-1), builder.getInt8(1));
      phi_inputs.emplace_back(res, curr_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, len_less), curr_blk));
    }
    builder.CreateBr(phi_blk);
    curr_blk = gt_pre_limit;
  }

  // Inline part comparison
  {
    builder.SetInsertPoint(curr_blk);
    auto get_inline_int64 = [&](llvm::Value* inline_val) -> llvm::Value* {
      if (!bolt::jit::ThrustJIT::getInstance()->getDataLayout().isBigEndian()) {
        std::vector<llvm::Value*> args;
        auto callee = llvm_module->getFunction("llvm.bswap.i64");
        args.push_back(inline_val);
        inline_val = builder.CreateCall(callee, args);
        args.clear();
      }
      return inline_val;
    };

    auto both_inline = builder.CreateAnd(
        builder.CreateICmpULE(left_len, inline_limit),
        builder.CreateICmpULE(right_len, inline_limit));

    auto inline_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_both_inline", func, next_blk);
    auto buf_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_buf", func, next_blk);
    builder.CreateCondBr(both_inline, inline_blk, buf_blk);

    curr_blk = inline_blk;
    builder.SetInsertPoint(curr_blk);
    llvm::Value* left_inline_val_raw = getValueByPtr(
        builder, values[0], int64_ty, keyOffsets[idx] + sizeof(int64_t));
    llvm::Value* right_inline_val_raw = getValueByPtr(
        builder, values[1], int64_ty, keyOffsets[idx] + sizeof(int64_t));
    llvm::ArrayType* arrayTy = llvm::ArrayType::get(builder.getInt64Ty(), 13);
    auto left_mask_ptr =
        builder.CreateGEP(arrayTy, values[2], {builder.getInt32(0), left_len});
    auto left_mask = builder.CreateLoad(int64_ty, left_mask_ptr);
    auto right_mask_ptr =
        builder.CreateGEP(arrayTy, values[2], {builder.getInt32(0), right_len});
    auto right_mask = builder.CreateLoad(int64_ty, right_mask_ptr);
    auto left_inline_val = builder.CreateAnd(left_inline_val_raw, left_mask);
    auto right_inline_val = builder.CreateAnd(right_inline_val_raw, right_mask);
    auto left_inl =
        isEqualOp() ? left_inline_val : get_inline_int64(left_inline_val);
    auto right_inl =
        isEqualOp() ? right_inline_val : get_inline_int64(right_inline_val);
    auto ne_inline_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne_inline", func, next_blk);
    auto eq_inline_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_eq_inline", func, next_blk);
    auto ne_inline = builder.CreateICmpNE(left_inl, right_inl);
    builder.CreateCondBr(
        ne_inline, ne_inline_blk, isEqualOp() ? next_blk : eq_inline_blk);
    curr_blk = ne_inline_blk;
    builder.SetInsertPoint(curr_blk);

    // as unsigned integer
    auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
        : flags[idx].ascending ? llvm::ICmpInst::ICMP_ULT
                               : llvm::ICmpInst::ICMP_UGT;
    auto cmp_res = builder.CreateICmp(cmpOp, left_inl, right_inl);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(
              cmp_res, builder.getInt8(-1), builder.getInt8(1)),
          curr_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, cmp_res), curr_blk));
    }
    builder.CreateBr(phi_blk);
    { // only for !equalOp
      curr_blk = eq_inline_blk;
      builder.SetInsertPoint(curr_blk);
      // if (left_len == right_len) then go to next
      // else return left_len cmp right_len
      auto len_ne = builder.CreateICmpNE(left_len, right_len);
      auto eq_inline_ne_len = llvm::BasicBlock::Create(
          llvm_context, getLabel(idx) + "_eq_inline_ne_len", func, next_blk);
      builder.CreateCondBr(len_ne, eq_inline_ne_len, next_blk);
      curr_blk = eq_inline_ne_len;
      builder.SetInsertPoint(curr_blk);
      auto len_less = builder.CreateICmp(cmpOp, left_len, right_len);
      if (isCmp()) {
        auto res_lt = builder.CreateSelect(
            len_less, builder.getInt8(-1), builder.getInt8(1));
        phi_inputs.emplace_back(res_lt, curr_blk);
      } else {
        phi_inputs.emplace_back(
            std::make_pair(castToI8(builder, len_less), curr_blk));
      }
      builder.CreateBr(phi_blk);
    }
    curr_blk = buf_blk;
  }

  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;
  // Non-inline (buffer) part comparison
  builder.SetInsertPoint(curr_blk);
  std::vector<llvm::Value*> args;
  args.push_back(left_addr);
  args.push_back(right_addr);

  /// row based cmp is special
  if (isCmp() && !flags[idx].ascending) {
    std::swap(args[0], args[1]);
  }

  auto int32Type = llvm::Type::getInt32Ty(llvm_context);
  auto callee = isCmpSpill()
      ? llvm_module->getFunction(RowBasedStringViewCompare)
      : llvm_module->getFunction(rowStringViewCompareAsc);
  auto str_cmp_res = builder.CreateCall(callee, args);
  auto const_0 = llvm::ConstantInt::get(int32Type, 0);

  // if it is the last key
  if (idx == keysTypes.size() - 1) {
    auto cmp_res = builder.CreateICmp(cmpOp, str_cmp_res, const_0);
    if (isCmp()) {
      phi_inputs.emplace_back(castToI8(builder, str_cmp_res, true), curr_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, cmp_res), curr_blk));
    }
  } else {
    // If it not the last key, firstly check if left equals with right
    auto key_eq =
        builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, str_cmp_res, const_0);

    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);
    builder.CreateCondBr(key_eq, next_blk, key_ne_blk);

    builder.SetInsertPoint(key_ne_blk);
    auto res_ne = builder.CreateICmp(cmpOp, str_cmp_res, const_0);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(res_ne, builder.getInt8(-1), builder.getInt8(1)),
          key_ne_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, res_ne), key_ne_blk));
    }
  }
  builder.CreateBr(phi_blk);
  return next_blk;
};

llvm::BasicBlock* RowContainerCodeGenerator::genTimestampCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);

  auto row_ty = builder.getInt8Ty();

  llvm::Type* data_ty = builder.getInt64Ty();

  llvm::PointerType* data_ptr_ty = data_ty->getPointerTo();

  // Generate value comparison IR for check nullity
  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Offsets for second & nano
  std::vector<int32_t> timestampOffsets{
      keyOffsets[idx], keyOffsets[idx] + (int32_t)sizeof(int64_t)};

  // Refer to bytedance::bolt::Timestamp
  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;
  for (auto i = 0; i < 2; ++i) {
    builder.SetInsertPoint(curr_blk);

    auto row_offset = timestampOffsets[i];
    auto void_left_addr =
        builder.CreateConstInBoundsGEP1_64(row_ty, values[0], row_offset);
    auto void_right_addr =
        builder.CreateConstInBoundsGEP1_64(row_ty, values[1], row_offset);
    auto left_addr = builder.CreatePointerCast(void_left_addr, data_ptr_ty);
    auto right_addr = builder.CreatePointerCast(void_right_addr, data_ptr_ty);
    auto left_val = builder.CreateLoad(data_ty, left_addr);
    auto right_val = builder.CreateLoad(data_ty, right_addr);

    if (i == 1 && idx == keysTypes.size() - 1) {
      auto cmp_res = builder.CreateICmp(cmpOp, left_val, right_val);

      if (isCmp()) {
        phi_inputs.emplace_back(std::make_pair(
            builder.CreateSelect(
                cmp_res,
                builder.getInt8(-1),
                castToI8(
                    builder, builder.CreateICmp(cmpOp, right_val, left_val))),
            curr_blk));
      } else {
        phi_inputs.emplace_back(
            std::make_pair(castToI8(builder, cmp_res), curr_blk));
      }
      builder.CreateBr(phi_blk);
    } else {
      auto key_eq =
          builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, left_val, right_val);

      auto key_ne_blk = llvm::BasicBlock::Create(
          llvm_context,
          getLabel(idx) + "_ne_" + (i == 0 ? "sec" : "nano"),
          func,
          next_blk);

      auto tmp_next = i == 0
          ? llvm::BasicBlock::Create(
                llvm_context, getLabel(idx) + "_nano", func, next_blk)
          : next_blk;

      builder.CreateCondBr(key_eq, tmp_next, key_ne_blk);

      builder.SetInsertPoint(key_ne_blk);

      auto res_ne = builder.CreateICmp(cmpOp, left_val, right_val);
      if (isCmp()) {
        phi_inputs.emplace_back(std::make_pair(
            builder.CreateSelect(
                res_ne, builder.getInt8(-1), builder.getInt8(1)),
            key_ne_blk));
      } else {
        phi_inputs.emplace_back(
            std::make_pair(castToI8(builder, res_ne), key_ne_blk));
      }
      builder.CreateBr(phi_blk);
      curr_blk = tmp_next;
    }
  }

  return next_blk;
}

llvm::BasicBlock* RowContainerCodeGenerator::genComplexCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  // ```cpp
  //   auto res = ComplexTypeRowCmpRow(left_row(offset),
  //   right_row[offset], type*, flags);
  //   if constexpr (lastKey) {
  //     return res;
  //   } else {
  //     if (res) {
  //       goto next_key;
  //     } else {
  //       return false;
  //     }
  //   }
  // ```

  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);
  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);
  // return i1 (true/false) for row(offset) = decodedvec(index)?

  auto key_cmp_res = createCall(
      builder,
      isCmpSpill() ? RowBasedComplexTypeRowCmpRow : ComplexTypeRowCmpRow,
      {values[0],
       values[1],
       builder.getInt64((int64_t)keysTypes[idx].get()),
       builder.getInt32(keyOffsets[idx]),
       builder.getInt8(isEqualOp() ? 0 : (int8_t)flags[idx].nullsFirst),
       builder.getInt8(isEqualOp() ? 0 : (int8_t)flags[idx].ascending)});
  auto const_0 = llvm::ConstantInt::get(builder.getInt32Ty(), 0);
  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;
  // if it is the last key
  if (idx == keysTypes.size() - 1) {
    auto cmp_res = builder.CreateICmp(cmpOp, key_cmp_res, const_0);

    if (isCmp()) {
      phi_inputs.emplace_back(castToI8(builder, key_cmp_res, true), curr_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, cmp_res), curr_blk));
    }
  } else {
    // If it not the last key, firstly check if left equals with right
    auto key_eq =
        builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, key_cmp_res, const_0);

    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);
    builder.CreateCondBr(key_eq, next_blk, key_ne_blk);

    builder.SetInsertPoint(key_ne_blk);
    auto res_ne = builder.CreateICmp(cmpOp, key_cmp_res, const_0);
    if (isCmp()) {
      phi_inputs.emplace_back(
          builder.CreateSelect(res_ne, builder.getInt8(-1), builder.getInt8(1)),
          key_ne_blk);
    } else {
      phi_inputs.emplace_back(
          std::make_pair(castToI8(builder, res_ne), key_ne_blk));
    }
  }
  builder.CreateBr(phi_blk);
  return next_blk;
}

bool RowContainerCodeGenerator::GenCmpIR() {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  // Declaration:
  // int8_t row_cmp(char* l, char* r) ;
  auto fun_name = GetCmpFuncName();
  llvm::FunctionType* func_type = llvm::FunctionType::get(
      builder.getInt8Ty(),
      {builder.getInt8PtrTy(), builder.getInt8PtrTy()},
      /*isVarArg=*/false);
  llvm::Function* func = llvm::Function::Create(
      func_type, llvm::Function::ExternalLinkage, fun_name, llvm_module);
  // set as noexcept is performance essential for Compare()
  // llvm::AttrBuilder ab;
  // ab.addAttribute(llvm::Attribute::NoUnwind);
  // func->addAttributes(llvm::AttributeList::FunctionIndex, ab);

  unsigned int argIdx = 0;
  std::vector<std::string> args_name{"l", "r"};
  for (auto& arg : func->args()) {
    arg.setName(args_name[argIdx++]);
  }

  // Add a basic block to the function.
  llvm::BasicBlock* entry_blk =
      llvm::BasicBlock::Create(llvm_context, "entry", func);
  builder.SetInsertPoint(entry_blk);
  auto createAlloca = [&func, &llvm_context](const std::string& name) {
    llvm::IRBuilder<> TmpB(
        &func->getEntryBlock(), func->getEntryBlock().begin());
    llvm::AllocaInst* alloc = TmpB.CreateAlloca(
        llvm::Type::getInt8PtrTy(llvm_context), nullptr, name);
    return alloc;
  };
  // Function arguments:  void* left, void* right
  llvm::SmallVector<llvm::Value*> args_values;
  for (auto&& arg : func->args()) {
    auto name = arg.getName().str();
    llvm::Value* alloc = createAlloca(name);
    builder.CreateStore(&arg, alloc);

    auto* load =
        builder.CreateLoad(llvm::Type::getInt8PtrTy(llvm_context), alloc, name);
    args_values.emplace_back(load);
  }

  llvm::ArrayType* ArrayTy = llvm::ArrayType::get(builder.getInt64Ty(), 13);

  auto array = llvm::ConstantArray::get(
      ArrayTy,
      {builder.getInt64(0),
       builder.getInt64(0),
       builder.getInt64(0),
       builder.getInt64(0),
       builder.getInt64(0),
       builder.getInt64(-1),
       builder.getInt64(-1),
       builder.getInt64(-1),
       builder.getInt64(-1),
       builder.getInt64(-1),
       builder.getInt64(-1),
       builder.getInt64(-1),
       builder.getInt64(-1)});
  auto arrayVar = new llvm::GlobalVariable(
      *llvm_module,
      ArrayTy,
      true,
      llvm::GlobalValue::PrivateLinkage,
      array,
      "mask");
  args_values.emplace_back(arrayVar);

  // The phi block for keys comparison
  auto phi_blk = llvm::BasicBlock::Create(llvm_context, "phi", func);

  auto curr_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(0), func, phi_blk);
  builder.CreateBr(curr_blk);

  using PhiNodeInputs = std::vector<std::pair<llvm::Value*, llvm::BasicBlock*>>;
  PhiNodeInputs phi_inputs;

  // Step 2: Generate IR for the comparison of all the keys
  for (size_t i = 0; i < keysTypes.size(); ++i) {
    auto kind = keysTypes[i]->kind();

    if (kind == bytedance::bolt::TypeKind::DOUBLE ||
        kind == bytedance::bolt::TypeKind::REAL) {
      curr_blk = genFloatPointCmpIR(
          kind, args_values, i, func, phi_inputs, curr_blk, phi_blk);
    } else if (
        kind == bytedance::bolt::TypeKind::VARCHAR ||
        kind == bytedance::bolt::TypeKind::VARBINARY) {
      curr_blk = genStringViewCmpIR(
          kind, args_values, i, func, phi_inputs, curr_blk, phi_blk);
    } else if (
        kind == bytedance::bolt::TypeKind::BOOLEAN ||
        kind == bytedance::bolt::TypeKind::TINYINT ||
        kind == bytedance::bolt::TypeKind::SMALLINT ||
        kind == bytedance::bolt::TypeKind::INTEGER ||
        kind == bytedance::bolt::TypeKind::BIGINT ||
        kind == bytedance::bolt::TypeKind::HUGEINT) {
      curr_blk = genIntegerCmpIR(
          kind, args_values, i, func, phi_inputs, curr_blk, phi_blk);
    } else if (kind == bytedance::bolt::TypeKind::TIMESTAMP) {
      curr_blk = genTimestampCmpIR(
          kind, args_values, i, func, phi_inputs, curr_blk, phi_blk);
    } else if (
        kind == bytedance::bolt::TypeKind::ROW ||
        kind == bytedance::bolt::TypeKind::ARRAY ||
        kind == bytedance::bolt::TypeKind::MAP) {
      curr_blk = genComplexCmpIR(
          kind, args_values, i, func, phi_inputs, curr_blk, phi_blk);
    } else {
      // should not be here.
      throw std::logic_error(
          "IR generation for this type is not supported yet. TODO...");
    }
  }

  // If all key compared
  builder.SetInsertPoint(curr_blk);
  // if all keys equals, switch CmpType
  // SORT_LESS: left row < right row => return 0
  // CMP: return 0
  // EQUAL: return 1
  phi_inputs.emplace_back(builder.getInt8(isEqualOp() ? 1 : 0), curr_blk);
  builder.CreateBr(phi_blk);

  // Step 3: Phi node, return the comparison result
  {
    builder.SetInsertPoint(phi_blk);
    auto cmp_phi = builder.CreatePHI(builder.getInt8Ty(), phi_inputs.size());
    for (auto input : phi_inputs) {
      cmp_phi->addIncoming(input.first, input.second);
    }
    builder.CreateRet(cmp_phi);
  }

  auto err = llvm::verifyFunction(*func);
  BOLT_CHECK_EQ(err, false, "IR generation failed.");
  return err;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setCompareFlags(
    std::vector<CompareFlags>&& flags) {
  this->flags = std::move(flags);
  return *this;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setKeyTypes(
    std::vector<bytedance::bolt::TypePtr>&& types) {
  keysTypes = std::move(types);
  return *this;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setKeyOffsets(
    std::vector<int32_t>&& offsets) {
  keyOffsets = std::move(offsets);
  return *this;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setNullByteOffsets(
    std::vector<int32_t>&& nullByteOffsets) {
  this->nullByteOffsets = std::move(nullByteOffsets);
  return *this;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setNullMasks(
    std::vector<int8_t>&& nullByteMasks) {
  this->nullByteMasks = std::move(nullByteMasks);
  return *this;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setModule(
    llvm::Module* m) {
  llvm_module = m;
  return *this;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setHasNullKeys(
    bool hasNullKeys) {
  this->hasNullKeys = hasNullKeys;
  return *this;
}

RowContainerCodeGenerator& RowContainerCodeGenerator::setOpType(CmpType type) {
  this->cmpType = type;
  return *this;
}

const std::string RowContainerCodeGenerator::builtInDeclarationsIR = R"IR(
  declare i32 @llvm.bswap.i32(i32)
  declare i64 @llvm.bswap.i64(i64)
  declare i32 @memcmp(i8* nocapture, i8* nocapture, i64) local_unnamed_addr 

  declare i32 @FastRowStringViewCompareAsc(i8*  %0, i8*  %1) local_unnamed_addr 

  declare i32 @StringViewCompareWrapper(i8* %0, i8*  %1) local_unnamed_addr 
  declare i32 @RowBasedStringViewCompare(i8* %0, i8*  %1) local_unnamed_addr
  declare i32 @ComplexTypeRowCmpRow(i8* %0, i8* %1, i64 %2, i32 %3, i8 %4, i8 %5) local_unnamed_addr
  declare i32 @RowBasedComplexTypeRowCmpRow(i8* %0, i8* %1, i64 %2, i32 %3, i8 %4, i8 %5) local_unnamed_addr

  declare i8 @StringViewRowEqVectors(i8*  %0, i8*  %1) local_unnamed_addr 
  declare i8 @GetDecodedValueBool(i8* %0, i32 %1) local_unnamed_addr
  declare i8 @GetDecodedValueI8(i8* %0, i32 %1) local_unnamed_addr
  declare i16 @GetDecodedValueI16(i8* %0, i32 %1) local_unnamed_addr
  declare i32 @GetDecodedValueI32(i8* %0, i32 %1) local_unnamed_addr
  declare i64 @GetDecodedValueI64(i8* %0, i32 %1) local_unnamed_addr
  declare i128 @GetDecodedValueI128(i8* %0, i32 %1) local_unnamed_addr
  declare float @GetDecodedValueFloat(i8* %0, i32 %1) local_unnamed_addr
  declare double @GetDecodedValueDouble(i8* %0, i32 %1) local_unnamed_addr
  declare i8 @CmpRowVecTimestamp(i8* %0, i32 %1, i8* %2) local_unnamed_addr
  declare i8* @GetDecodedValueStringView(i8* %0, i32 %1) local_unnamed_addr
  declare i8 @GetDecodedIsNull(i8* %0, i32 %1) local_unnamed_addr
  declare i8 @ComplexTypeRowEqVectors(i8* %0, i32 %1, i8* %2, i32 %3) local_unnamed_addr
  declare void @DebugPrint(i64 %0, i64 %1, i64 %2) local_unnamed_addr

  )IR";

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT