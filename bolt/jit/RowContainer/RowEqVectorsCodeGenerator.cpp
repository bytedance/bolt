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

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/InstrTypes.h>
#include <llvm/IR/Value.h>
#include <type/Type.h>
#include <cstdint>

#include "bolt/jit/RowContainer/RowEqVectorsCodeGenerator.h"

namespace bytedance::bolt::jit {

/// util functions
std::string RowEqVectorsCodeGenerator::GetCmpFuncName() {
  std::string fn = "jit_r=v_cmp";
  fn += hasNullKeys ? "_null" : "";
  for (auto i = 0; i < keysTypes.size(); ++i) {
    fn.append(keysTypes[i]->jitName());
  }
  return fn;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genNullBitCmpIR(
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* next_blk,
    llvm::BasicBlock* phi_blk,
    PhiNodeInputs& phi_inputs) {
  /*
 ```cpp
     auto leftNull = * (char *) (leftRow + nullByteOffsets[col_idx]);
     bool isLeftNull = leftNull & nullPosMask;
     bool isRightNull = getDecodedNull(right, idx);
     if (isLeftNull!= isRightNull) {
        return false;
     }
     if (isLeftNull) {
       goto next_key_compare;
     }
     goto value_compare;
   }

 ```
  */
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  builder.SetInsertPoint(curr_blk);

  // row->isNullAt(idx)
  auto i8_ty = builder.getInt8Ty();
  auto const_mask = llvm::ConstantInt::get(i8_ty, nullByteMasks[idx]);
  auto left_val_unmask =
      getValueByPtr(builder, values[0], i8_ty, nullByteOffsets[idx]);

  auto left_val = castToI8(
      builder,
      builder.CreateICmpNE(
          builder.CreateAnd(left_val_unmask, const_mask), builder.getInt8(0)));

  // get right value from call
  auto right_val =
      createCall(builder, GetDecodedIsNull, {values[2 + idx], values[1]});

  auto nil_ne = builder.CreateICmpNE(left_val, right_val);
  auto nil_ne_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_ne_blk", func, next_blk);
  auto nil_eq_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_eq_blk", func, next_blk);
  auto no_nil_blk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_no_nil_blk", func, next_blk);
  auto un_likely = llvm::MDBuilder(builder.getContext())
                       .createBranchWeights(1, 1000); //(1U << 20) - 1, 1

  builder.CreateCondBr(nil_ne, nil_ne_blk, nil_eq_blk, un_likely);

  // return 0 if not equal
  curr_blk = nil_ne_blk;
  builder.SetInsertPoint(curr_blk);
  builder.CreateBr(phi_blk);
  phi_inputs.emplace_back(builder.getInt8(0), curr_blk);

  // goto next key compare if null, else goto value compare
  curr_blk = nil_eq_blk;
  builder.SetInsertPoint(curr_blk);
  builder.CreateCondBr(
      builder.CreateICmpNE(left_val, builder.getInt8(0)),
      next_blk,
      no_nil_blk,
      un_likely);

  return no_nil_blk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genIntegerCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  /*
```cpp
  leftRow = values[0];
  index = values[1];
  decodedVector = values[2 + idx];
  auto left_val = *(Integer*) (leftRow + keyOffsets[idx]);
  auto right_val = *(Integer*) getDecodedValue(decodedVector, index);
  { // check null
    auto left_null = (leftRow + nullByteOffsets[idx]) & nullByteMasks[idx];
    auto right_null = getDecodedNull(decodedVector, index);
    if (left_null && right_null) {
      return true;
    } else if (left_null || right_null) {
      return false;
    }
  }
  if constexpr (is_last_key) {
     return left_val == right_val;
  } else {
     if (left_val == right_val) {
       goto next_key_compare;
     }
     return false;
  }
```
*/

  auto row_ty = builder.getInt8Ty();
  llvm::Type* data_ty = nullptr;
  std::string right_val_call = GetDecodedValueI32;
  if (kind == bytedance::bolt::TypeKind::BOOLEAN) {
    // Just follow Clang. Clang chose i8 over i1 for a boolean field
    data_ty = builder.getInt8Ty();
    right_val_call = GetDecodedValueBool;
  } else if (kind == bytedance::bolt::TypeKind::TINYINT) {
    data_ty = builder.getInt8Ty();
    right_val_call = GetDecodedValueI8;
  } else if (kind == bytedance::bolt::TypeKind::SMALLINT) {
    data_ty = builder.getInt16Ty();
    right_val_call = GetDecodedValueI16;
  } else if (kind == bytedance::bolt::TypeKind::INTEGER) {
    data_ty = builder.getInt32Ty();
    right_val_call = GetDecodedValueI32;
  } else if (kind == bytedance::bolt::TypeKind::BIGINT) {
    data_ty = builder.getInt64Ty();
    right_val_call = GetDecodedValueI64;
  } else if (kind == bytedance::bolt::TypeKind::HUGEINT) {
    data_ty = builder.getInt128Ty();
    right_val_call = GetDecodedValueI128;
  } else {
    BOLT_UNREACHABLE();
  }

  // Block for next key.
  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);
  llvm::PointerType* data_ptr_ty = data_ty->getPointerTo();

  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);
  // left value
  auto left_val = getValueByPtr(builder, values[0], data_ty, keyOffsets[idx]);
  // right value from createCall
  auto right_val =
      createCall(builder, right_val_call, {values[2 + idx], values[1]});

  auto key_eq = builder.CreateICmpEQ(left_val, right_val);

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    phi_inputs.emplace_back(
        std::make_pair(castToI8(builder, key_eq), curr_blk));
    builder.CreateBr(phi_blk);
  } else {
    // return false if not equal, otherwise goto next key
    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);
    builder.CreateCondBr(key_eq, next_blk, key_ne_blk);
    builder.SetInsertPoint(key_ne_blk);
    builder.CreateBr(phi_blk);
    phi_inputs.emplace_back(std::make_pair(builder.getInt8(0), key_ne_blk));
  }

  return next_blk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genTimestampCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  auto row_ty = builder.getInt8Ty();

  // Block for next key.
  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);

  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);

  auto key_eq = createCall(
      builder,
      CmpRowVecTimestamp,
      {values[2 + idx],
       values[1],
       builder.CreateConstInBoundsGEP1_64(
           builder.getInt8Ty(), values[0], keyOffsets[idx])});

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    phi_inputs.emplace_back(std::make_pair(key_eq, curr_blk));
    builder.CreateBr(phi_blk);
  } else {
    // return false if not equal, otherwise goto next key
    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);
    builder.CreateCondBr(
        builder.CreateIntCast(key_eq, builder.getInt1Ty(), false),
        next_blk,
        key_ne_blk);
    builder.SetInsertPoint(key_ne_blk);
    builder.CreateBr(phi_blk);
    phi_inputs.emplace_back(std::make_pair(builder.getInt8(0), key_ne_blk));
  }

  return next_blk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genFloatPointCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  bool isDouble = kind == bytedance::bolt::TypeKind::DOUBLE;

  auto data_ty = isDouble ? builder.getDoubleTy() : builder.getFloatTy();
  std::string right_val_call =
      isDouble ? GetDecodedValueDouble : GetDecodedValueFloat;
  llvm::PointerType* data_ptr_ty = data_ty->getPointerTo();

  // Block for next key.
  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);
  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }
  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);

  auto left_val_raw =
      getValueByPtr(builder, values[0], data_ty, keyOffsets[idx]);
  // right value from createCall
  auto right_val_raw =
      createCall(builder, right_val_call, {values[2 + idx], values[1]});

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
  //  if (leftIsNan != rightIsNan) {  // only one operand is NaN
  //      return false;
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
  builder.CreateBr(phi_blk);
  phi_inputs.emplace_back(builder.getInt8(0), curr_blk); // return false

  curr_blk = eq_nan_blk;
  builder.SetInsertPoint(curr_blk);
  builder.CreateCondBr(
      is_left_nan, next_blk, no_nan_blk, un_likely); // if Both NaN

  curr_blk = no_nan_blk;
  builder.SetInsertPoint(curr_blk);

  auto left_val = left_val_raw;
  auto right_val = right_val_raw;
  /*
```cpp
 leftRow = values[0];
 index = values[1];
 decodedVector = values[2 + idx];
 auto left_val = *(Integer*) (leftRow + keyOffsets[idx]);
 auto right_val = *(Integer*) getDecodedValue(decodedVector, index);
 { // check null
   auto left_null = (leftRow + nullByteOffsets[idx]) & nullByteMasks[idx];
   auto right_null = getDecodedNull(decodedVector, index);
   if (left_null && right_null) {
     return true;
   } else if (left_null || right_null) {
     return false;
   }
 }
 cmp = left_val == right_val;
 if constexpr (is_last_key) {
    return cmp;
 } else {
    if (cmp) {
      goto next_key_compare;
    }
    return false;
 }
```
*/
  auto key_eq = builder.CreateFCmpOEQ(left_val, right_val);
  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    phi_inputs.emplace_back(
        std::make_pair(castToI8(builder, key_eq), curr_blk));
    builder.CreateBr(phi_blk);
  } else {
    // return false if not equal, otherwise goto next key
    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);
    builder.CreateCondBr(key_eq, next_blk, key_ne_blk);
    builder.SetInsertPoint(key_ne_blk);
    builder.CreateBr(phi_blk);
    phi_inputs.emplace_back(std::make_pair(builder.getInt8(0), key_ne_blk));
  }
  return next_blk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genStringViewCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  /*
    ```cpp
    // prefix and length
    auto left_prefix = *(int64_t*) (leftRow + keyOffsets[idx]);
    auto right_prefix = *(int64_t*) (rightAddr);
    if (left_prefix != right_prefix) {
        return false;
    }
    auto left_len = *(int32_t*) (leftRow + keyOffsets[idx] );
    auto right_len = *(int32_t*) (rightAddr);
    if (left_len <= 12 && right_len <= 12) {
    // if len <=4, only compare first 8 bytes
    // else compare the whole inline part, 16 bytes
    // optimize this comparison with mask, mask[len <=4] = 0, else mask[len] = 0
      auto left_inline = *(int64_t*) (leftRow + keyOffsets[idx] +
                    sizeof(int64_t)) & mask[len];
      auto right_inline = *(int64_t*) (rightAddr + sizeof(int64_t))) &
                    mask[len];
    return left_inline == right_inline
    }
    auto res = jit_StringViewRowEqVectors(left, right);
    if constexpr (lastKey) {
      return res;
    } else {
      if (res) {
        goto next_key;
      } else {
        return false;
      }
    }
    ```
    */

  // Block for next key.
  auto next_blk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phi_blk);
  if (hasNullKeys) {
    curr_blk = genNullBitCmpIR(
        values, idx, func, curr_blk, next_blk, phi_blk, phi_inputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(curr_blk);
  auto left_addr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), values[0], keyOffsets[idx]);

  // get right value from createCall
  auto right_addr = createCall(
      builder, GetDecodedValueStringView, {values[2 + idx], values[1]});
  // Check Prefix + length (8 chars)
  {
    auto int64_ty = builder.getInt64Ty();
    auto left_val = getValueByPtr(builder, left_addr, int64_ty, 0);
    auto right_val = getValueByPtr(builder, right_addr, int64_ty, 0);

    auto pre_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_ne", func, next_blk);
    auto pre_eq_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_eq", func, next_blk);

    auto prefix_eq = builder.CreateICmpEQ(left_val, right_val);
    builder.CreateCondBr(prefix_eq, pre_eq_blk, pre_ne_blk);

    // If prefix is NOT equal, return false
    builder.SetInsertPoint(pre_ne_blk);
    phi_inputs.emplace_back(std::make_pair(builder.getInt8(0), pre_ne_blk));
    builder.CreateBr(phi_blk);

    curr_blk = pre_eq_blk;
  }
  // return inline part comparison
  {
    builder.SetInsertPoint(curr_blk);
    auto len_ty = builder.getInt32Ty();
    auto left_len = getValueByPtr(builder, left_addr, len_ty, 0);
    constexpr int32_t SV_INLINE_LIMIT = 12;
    auto inline_limit = llvm::ConstantInt::get(len_ty, SV_INLINE_LIMIT);
    auto is_inline = builder.CreateICmpULE(left_len, inline_limit);

    auto inline_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_both_inline", func, next_blk);
    auto buf_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_buf", func, next_blk);
    builder.CreateCondBr(is_inline, inline_blk, buf_blk);

    // if both inline, return false if not equal, otherwise goto next key
    curr_blk = inline_blk;
    builder.SetInsertPoint(curr_blk);
    auto inline_ty = builder.getInt64Ty();
    llvm::ArrayType* arrayTy = llvm::ArrayType::get(builder.getInt64Ty(), 13);
    auto suffix_mask_ptr = builder.CreateGEP(
        arrayTy, values[2 + keysTypes.size()], {builder.getInt32(0), left_len});
    auto suffix_mask = builder.CreateLoad(inline_ty, suffix_mask_ptr);

    auto left_inl = builder.CreateAnd(
        suffix_mask,
        getValueByPtr(builder, left_addr, inline_ty, sizeof(int64_t)));
    auto right_inl = builder.CreateAnd(
        suffix_mask,
        getValueByPtr(builder, right_addr, inline_ty, sizeof(int64_t)));
    auto ne_inline_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne_inline", func, next_blk);
    auto ne_inline = builder.CreateICmpNE(left_inl, right_inl);
    builder.CreateCondBr(ne_inline, ne_inline_blk, next_blk);
    curr_blk = ne_inline_blk;
    builder.SetInsertPoint(curr_blk);
    phi_inputs.emplace_back(std::make_pair(builder.getInt8(0), curr_blk));
    builder.CreateBr(phi_blk);

    curr_blk = buf_blk;
  }
  // Non-inline (buffer) part comparison
  builder.SetInsertPoint(curr_blk);
  auto key_eq =
      createCall(builder, StringViewRowEqVectors, {left_addr, right_addr});

  if (idx == keysTypes.size() - 1) {
    phi_inputs.emplace_back(std::make_pair(key_eq, curr_blk));
    builder.CreateBr(phi_blk);
  } else {
    // return false if not equal, otherwise goto next key
    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);
    builder.CreateCondBr(
        builder.CreateICmpNE(key_eq, builder.getInt8(0)), next_blk, key_ne_blk);

    builder.SetInsertPoint(key_ne_blk);
    builder.CreateBr(phi_blk);
    phi_inputs.emplace_back(std::make_pair(builder.getInt8(0), key_ne_blk));
  }

  return next_blk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genComplexCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phi_inputs,
    llvm::BasicBlock* curr_blk,
    llvm::BasicBlock* phi_blk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  // ```cpp
  //   auto res = jit_ComplexTypeRowEqVectors(row(offset), decodedvec(index));
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
  auto key_eq = createCall(
      builder,
      ComplexTypeRowEqVectors,
      {values[0],
       builder.getInt32(keyOffsets[idx]),
       values[2 + idx],
       values[1]});

  if (idx == keysTypes.size() - 1) {
    phi_inputs.emplace_back(std::make_pair(key_eq, curr_blk));
    builder.CreateBr(phi_blk);
  } else {
    // If it not the last key, return false if not equal otherwise goto next_key
    auto key_ne_blk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, next_blk);
    builder.CreateCondBr(
        builder.CreateICmpNE(key_eq, builder.getInt8(0)), next_blk, key_ne_blk);

    builder.SetInsertPoint(key_ne_blk);
    builder.CreateBr(phi_blk);
    phi_inputs.emplace_back(std::make_pair(builder.getInt8(0), key_ne_blk));
  }

  return next_blk;
}

bool RowEqVectorsCodeGenerator::GenCmpIR() {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  // Declaration:
  // bool row=vec(char* row, int32_t index, char*[] decodedVectors)
  // row == vec(decodedVectors[0][index], decodedVectors[1][index],...)?
  auto fun_name = GetCmpFuncName();
  llvm::FunctionType* func_type = llvm::FunctionType::get(
      builder.getInt8Ty(),
      {builder.getInt8PtrTy(),
       builder.getInt32Ty(),
       builder.getInt8PtrTy()->getPointerTo()},
      /*isVarArg=*/false);
  llvm::Function* func = llvm::Function::Create(
      func_type, llvm::Function::ExternalLinkage, fun_name, llvm_module);

  // Add a basic block to the function.
  llvm::BasicBlock* entry_blk =
      llvm::BasicBlock::Create(llvm_context, "entry", func);
  builder.SetInsertPoint(entry_blk);

  // get all arguments.
  llvm::SmallVector<llvm::Value*> args_values(keysTypes.size() + 3, nullptr);
  auto* func_args = func->args().begin();
  llvm::Value* arg_row = func_args++;
  llvm::Value* arg_index = func_args++;
  llvm::Value* arg_vectors = func_args++;
  args_values[0] = arg_row;
  args_values[1] = arg_index; // need cast?
  for (auto i = 0; i < keysTypes.size(); ++i) {
    args_values[i + 2] = builder.CreateLoad(
        llvm::Type::getInt8PtrTy(llvm_context),
        builder.CreateConstInBoundsGEP1_64(
            llvm::Type::getInt8PtrTy(llvm_context), arg_vectors, i));
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

  args_values[keysTypes.size() + 2] = arrayVar;

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
        kind == bytedance::bolt::TypeKind::ARRAY ||
        kind == bytedance::bolt::TypeKind::MAP ||
        kind == bytedance::bolt::TypeKind::ROW) {
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
  builder.CreateBr(phi_blk);
  // if all keys equals,  return true
  phi_inputs.emplace_back(builder.getInt8(1), curr_blk);

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
  return err;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
