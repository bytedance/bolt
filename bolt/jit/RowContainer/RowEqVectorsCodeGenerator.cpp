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

llvm::Value* RowEqVectorsCodeGenerator::loadMappedIndex(
    llvm::IRBuilder<>& builder,
    llvm::Value* runtime,
    llvm::Value* row) {
  auto* func = builder.GetInsertBlock()->getParent();
  auto* i32Ty = builder.getInt32Ty();
  auto* i8Ty = builder.getInt8Ty();
  auto* i32PtrTy = i32Ty->getPointerTo();

  auto* isIdentity =
      getValueByPtr(builder, runtime, i8Ty, kRuntimeIsIdentityMappingOffset);

  auto* identityBlk = llvm::BasicBlock::Create(
      builder.getContext(), "mapped_index_identity", func);
  auto* nonIdentityBlk = llvm::BasicBlock::Create(
      builder.getContext(), "mapped_index_non_identity", func);
  auto* doneBlk =
      llvm::BasicBlock::Create(builder.getContext(), "mapped_index_done", func);
  builder.CreateCondBr(
      builder.CreateICmpNE(isIdentity, builder.getInt8(0)),
      identityBlk,
      nonIdentityBlk);

  builder.SetInsertPoint(identityBlk);
  builder.CreateBr(doneBlk);
  identityBlk = builder.GetInsertBlock();

  builder.SetInsertPoint(nonIdentityBlk);
  auto* isConstant =
      getValueByPtr(builder, runtime, i8Ty, kRuntimeIsConstantMappingOffset);
  auto* constantBlk = llvm::BasicBlock::Create(
      builder.getContext(), "mapped_index_constant", func);
  auto* dictionaryBlk = llvm::BasicBlock::Create(
      builder.getContext(), "mapped_index_dictionary", func);
  builder.CreateCondBr(
      builder.CreateICmpNE(isConstant, builder.getInt8(0)),
      constantBlk,
      dictionaryBlk);

  builder.SetInsertPoint(constantBlk);
  auto* constantIndex =
      getValueByPtr(builder, runtime, i32Ty, kRuntimeConstantIndexOffset);
  builder.CreateBr(doneBlk);
  constantBlk = builder.GetInsertBlock();

  builder.SetInsertPoint(dictionaryBlk);
  auto* indicesAddr =
      getValueByPtr(builder, runtime, i32PtrTy, kRuntimeIndicesOffset);
  auto* indexAddr = builder.CreateGEP(i32Ty, indicesAddr, row);
  auto* indexedIndex = builder.CreateLoad(i32Ty, indexAddr);
  builder.CreateBr(doneBlk);
  dictionaryBlk = builder.GetInsertBlock();

  builder.SetInsertPoint(doneBlk);
  auto* phi = builder.CreatePHI(i32Ty, 3);
  phi->addIncoming(row, identityBlk);
  phi->addIncoming(constantIndex, constantBlk);
  phi->addIncoming(indexedIndex, dictionaryBlk);
  return phi;
}

llvm::Value* RowEqVectorsCodeGenerator::loadRightValue(
    llvm::IRBuilder<>& builder,
    llvm::Value* runtime,
    llvm::Value* row,
    llvm::Type* dataTy) {
  auto* valuesAddr = getValueByPtr(
      builder, runtime, dataTy->getPointerTo(), kRuntimeValuesOffset);
  auto* index = loadMappedIndex(builder, runtime, row);
  if (dataTy->isIntegerTy(128)) {
    auto* i8Values = builder.CreatePointerCast(
        valuesAddr, builder.getInt8Ty()->getPointerTo());
    auto* valueAddr = builder.CreateGEP(
        builder.getInt8Ty(),
        i8Values,
        builder.CreateMul(index, builder.getInt32(sizeof(int128_t))));
    auto* lower64 = builder.CreateLoad(
        builder.getInt64Ty(),
        builder.CreatePointerCast(
            valueAddr, builder.getInt64Ty()->getPointerTo()));
    auto* upperAddr = builder.CreateConstInBoundsGEP1_64(
        builder.getInt8Ty(), valueAddr, sizeof(int64_t));
    auto* upper64 = builder.CreateLoad(
        builder.getInt64Ty(),
        builder.CreatePointerCast(
            upperAddr, builder.getInt64Ty()->getPointerTo()));
    auto* lower128 = builder.CreateZExt(lower64, builder.getInt128Ty());
    auto* upper128 = builder.CreateZExt(upper64, builder.getInt128Ty());
    return builder.CreateOr(builder.CreateShl(upper128, 64), lower128);
  }
  return builder.CreateLoad(
      dataTy, builder.CreateGEP(dataTy, valuesAddr, index));
}

llvm::Value* RowEqVectorsCodeGenerator::loadRightBool(
    llvm::IRBuilder<>& builder,
    llvm::Value* runtime,
    llvm::Value* row) {
  auto* i64Ty = builder.getInt64Ty();
  auto* valuesAddr = getValueByPtr(
      builder, runtime, i64Ty->getPointerTo(), kRuntimeValuesOffset);
  auto* index = loadMappedIndex(builder, runtime, row);
  auto* wordIndex = builder.CreateLShr(index, builder.getInt32(6));
  auto* bitIndex = builder.CreateAnd(index, builder.getInt32(63));
  auto* word = builder.CreateLoad(
      i64Ty, builder.CreateGEP(i64Ty, valuesAddr, wordIndex));
  auto* bit = builder.CreateAnd(
      builder.CreateLShr(word, builder.CreateZExt(bitIndex, i64Ty)),
      builder.getInt64(1));
  return castToI8(builder, builder.CreateICmpNE(bit, builder.getInt64(0)));
}

llvm::Value* RowEqVectorsCodeGenerator::loadRightStringViewAddr(
    llvm::IRBuilder<>& builder,
    llvm::Value* runtime,
    llvm::Value* row) {
  return loadRightValueAddr(builder, runtime, row, sizeof(StringView));
}

llvm::Value* RowEqVectorsCodeGenerator::loadRightValueAddr(
    llvm::IRBuilder<>& builder,
    llvm::Value* runtime,
    llvm::Value* row,
    int32_t valueSize) {
  auto* valueTy = builder.getInt8Ty();
  auto* valuesAddr = getValueByPtr(
      builder, runtime, valueTy->getPointerTo(), kRuntimeValuesOffset);
  auto* index = loadMappedIndex(builder, runtime, row);
  return builder.CreateGEP(
      valueTy,
      valuesAddr,
      builder.CreateMul(index, builder.getInt32(valueSize)));
}

llvm::Value* RowEqVectorsCodeGenerator::loadRightNull(
    llvm::IRBuilder<>& builder,
    llvm::Value* runtime,
    llvm::Value* row) {
  auto* func = builder.GetInsertBlock()->getParent();
  auto* i8Ty = builder.getInt8Ty();
  auto* i64Ty = builder.getInt64Ty();
  auto* nullsAddr = getValueByPtr(
      builder, runtime, i64Ty->getPointerTo(), kRuntimeNullsOffset);
  auto* noNullBlk =
      llvm::BasicBlock::Create(builder.getContext(), "decoded_no_nulls", func);
  auto* hasNullBlk =
      llvm::BasicBlock::Create(builder.getContext(), "decoded_has_nulls", func);
  auto* doneBlk =
      llvm::BasicBlock::Create(builder.getContext(), "decoded_null_done", func);
  builder.CreateCondBr(
      builder.CreateICmpEQ(
          nullsAddr, llvm::ConstantPointerNull::get(i64Ty->getPointerTo())),
      noNullBlk,
      hasNullBlk);

  builder.SetInsertPoint(noNullBlk);
  builder.CreateBr(doneBlk);
  noNullBlk = builder.GetInsertBlock();

  builder.SetInsertPoint(hasNullBlk);
  auto* nullsUseTopLevelIndex = getValueByPtr(
      builder, runtime, i8Ty, kRuntimeNullsUseTopLevelIndexOffset);
  auto* topLevelNullIndexBlk = llvm::BasicBlock::Create(
      builder.getContext(), "decoded_null_top_level_index", func);
  auto* mappedNullIndexBlk = llvm::BasicBlock::Create(
      builder.getContext(), "decoded_null_mapped_index", func);
  auto* nullIndexDoneBlk = llvm::BasicBlock::Create(
      builder.getContext(), "decoded_null_index_done", func);
  builder.CreateCondBr(
      builder.CreateICmpNE(nullsUseTopLevelIndex, builder.getInt8(0)),
      topLevelNullIndexBlk,
      mappedNullIndexBlk);

  builder.SetInsertPoint(topLevelNullIndexBlk);
  builder.CreateBr(nullIndexDoneBlk);
  topLevelNullIndexBlk = builder.GetInsertBlock();

  builder.SetInsertPoint(mappedNullIndexBlk);
  auto* mappedNullIndex = loadMappedIndex(builder, runtime, row);
  builder.CreateBr(nullIndexDoneBlk);
  mappedNullIndexBlk = builder.GetInsertBlock();

  builder.SetInsertPoint(nullIndexDoneBlk);
  auto* nullIndex = builder.CreatePHI(builder.getInt32Ty(), 2);
  nullIndex->addIncoming(row, topLevelNullIndexBlk);
  nullIndex->addIncoming(mappedNullIndex, mappedNullIndexBlk);
  auto* wordIndex = builder.CreateLShr(nullIndex, builder.getInt32(6));
  auto* bitIndex = builder.CreateAnd(nullIndex, builder.getInt32(63));
  auto* word =
      builder.CreateLoad(i64Ty, builder.CreateGEP(i64Ty, nullsAddr, wordIndex));
  auto* bit = builder.CreateAnd(
      builder.CreateLShr(word, builder.CreateZExt(bitIndex, i64Ty)),
      builder.getInt64(1));
  auto* isNull =
      castToI8(builder, builder.CreateICmpEQ(bit, builder.getInt64(0)));
  builder.CreateBr(doneBlk);
  hasNullBlk = builder.GetInsertBlock();

  builder.SetInsertPoint(doneBlk);
  auto* phi = builder.CreatePHI(i8Ty, 2);
  phi->addIncoming(builder.getInt8(0), noNullBlk);
  phi->addIncoming(isNull, hasNullBlk);
  return phi;
}

llvm::Value* RowEqVectorsCodeGenerator::loadDecodedVector(
    llvm::IRBuilder<>& builder,
    llvm::Value* runtime) {
  return getValueByPtr(
      builder,
      runtime,
      builder.getInt8Ty()->getPointerTo(),
      kRuntimeDecodedVectorOffset);
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genNullBitCmpIR(
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* nextBlk,
    llvm::BasicBlock* phiBlk,
    PhiNodeInputs& phiInputs) {
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
  builder.SetInsertPoint(currBlk);

  // row->isNullAt(idx)
  auto i8Ty = builder.getInt8Ty();
  auto constMask = llvm::ConstantInt::get(i8Ty, nullByteMasks[idx]);
  auto leftValUnmask =
      getValueByPtr(builder, values[0], i8Ty, nullByteOffsets[idx]);

  auto leftVal = castToI8(
      builder,
      builder.CreateICmpNE(
          builder.CreateAnd(leftValUnmask, constMask), builder.getInt8(0)));

  auto rightVal = loadRightNull(builder, values[2 + idx], values[1]);
  currBlk = builder.GetInsertBlock();

  auto nilNe = builder.CreateICmpNE(leftVal, rightVal);
  auto nilNeBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_ne_blk", func, nextBlk);
  auto nilEqBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_eq_blk", func, nextBlk);
  auto noNilBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_no_nil_blk", func, nextBlk);
  auto unLikely = llvm::MDBuilder(builder.getContext())
                      .createBranchWeights(1, 1000); //(1U << 20) - 1, 1

  builder.CreateCondBr(nilNe, nilNeBlk, nilEqBlk, unLikely);

  // return 0 if not equal
  currBlk = nilNeBlk;
  builder.SetInsertPoint(currBlk);
  builder.CreateBr(phiBlk);
  phiInputs.emplace_back(builder.getInt8(0), currBlk);

  // goto next key compare if null, else goto value compare
  currBlk = nilEqBlk;
  builder.SetInsertPoint(currBlk);
  builder.CreateCondBr(
      builder.CreateICmpNE(leftVal, builder.getInt8(0)),
      nextBlk,
      noNilBlk,
      unLikely);

  return noNilBlk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genIntegerCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  /*
```cpp
  leftRow = values[0];
  index = values[1];
  decodedVector = values[2 + idx];
  auto leftVal = *(Integer*) (leftRow + keyOffsets[idx]);
  auto rightVal = *(Integer*) getDecodedValue(decodedVector, index);
  { // check null
    auto leftNull = (leftRow + nullByteOffsets[idx]) & nullByteMasks[idx];
    auto rightNull = getDecodedNull(decodedVector, index);
    if (leftNull && rightNull) {
      return true;
    } else if (leftNull || rightNull) {
      return false;
    }
  }
  if constexpr (is_last_key) {
     return leftVal == rightVal;
  } else {
     if (leftVal == rightVal) {
       goto next_key_compare;
     }
     return false;
  }
```
*/

  auto rowTy = builder.getInt8Ty();
  llvm::Type* dataTy = nullptr;
  if (kind == bytedance::bolt::TypeKind::BOOLEAN) {
    // Just follow Clang. Clang chose i8 over i1 for a boolean field
    dataTy = builder.getInt8Ty();
  } else if (kind == bytedance::bolt::TypeKind::TINYINT) {
    dataTy = builder.getInt8Ty();
  } else if (kind == bytedance::bolt::TypeKind::SMALLINT) {
    dataTy = builder.getInt16Ty();
  } else if (kind == bytedance::bolt::TypeKind::INTEGER) {
    dataTy = builder.getInt32Ty();
  } else if (kind == bytedance::bolt::TypeKind::BIGINT) {
    dataTy = builder.getInt64Ty();
  } else if (kind == bytedance::bolt::TypeKind::HUGEINT) {
    dataTy = builder.getInt128Ty();
  } else {
    BOLT_UNREACHABLE();
  }

  // Block for next key.
  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);
  // left value
  llvm::Value* leftVal = nullptr;
  if (kind == bytedance::bolt::TypeKind::HUGEINT) {
    leftVal = getHugeIntValueByPtr(builder, values[0], keyOffsets[idx]);
  } else {
    leftVal = getValueByPtr(builder, values[0], dataTy, keyOffsets[idx]);
  }
  auto rightVal = kind == bytedance::bolt::TypeKind::BOOLEAN
      ? loadRightBool(builder, values[2 + idx], values[1])
      : loadRightValue(builder, values[2 + idx], values[1], dataTy);
  currBlk = builder.GetInsertBlock();

  auto keyEq = builder.CreateICmpEQ(leftVal, rightVal);

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    phiInputs.emplace_back(std::make_pair(castToI8(builder, keyEq), currBlk));
    builder.CreateBr(phiBlk);
  } else {
    // return false if not equal, otherwise goto next key
    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);
    builder.CreateCondBr(keyEq, nextBlk, keyNeBlk);
    builder.SetInsertPoint(keyNeBlk);
    builder.CreateBr(phiBlk);
    phiInputs.emplace_back(std::make_pair(builder.getInt8(0), keyNeBlk));
  }

  return nextBlk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genTimestampCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  auto rowTy = builder.getInt8Ty();

  // Block for next key.
  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);

  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);

  auto* int64Ty = builder.getInt64Ty();
  auto leftAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), values[0], keyOffsets[idx]);
  auto rightAddr = loadRightValueAddr(
      builder, values[2 + idx], values[1], sizeof(Timestamp));
  currBlk = builder.GetInsertBlock();
  auto leftSeconds = getValueByPtr(builder, leftAddr, int64Ty, 0);
  auto rightSeconds = getValueByPtr(builder, rightAddr, int64Ty, 0);
  auto leftNanos = getValueByPtr(builder, leftAddr, int64Ty, sizeof(int64_t));
  auto rightNanos = getValueByPtr(builder, rightAddr, int64Ty, sizeof(int64_t));
  auto keyEq = castToI8(
      builder,
      builder.CreateAnd(
          builder.CreateICmpEQ(leftSeconds, rightSeconds),
          builder.CreateICmpEQ(leftNanos, rightNanos)));

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    phiInputs.emplace_back(std::make_pair(keyEq, currBlk));
    builder.CreateBr(phiBlk);
  } else {
    // return false if not equal, otherwise goto next key
    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);
    builder.CreateCondBr(
        builder.CreateIntCast(keyEq, builder.getInt1Ty(), false),
        nextBlk,
        keyNeBlk);
    builder.SetInsertPoint(keyNeBlk);
    builder.CreateBr(phiBlk);
    phiInputs.emplace_back(std::make_pair(builder.getInt8(0), keyNeBlk));
  }

  return nextBlk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genFloatPointCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  bool isDouble = kind == bytedance::bolt::TypeKind::DOUBLE;

  auto dataTy = isDouble ? builder.getDoubleTy() : builder.getFloatTy();
  // Block for next key.
  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }
  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);

  auto leftValRaw = getValueByPtr(builder, values[0], dataTy, keyOffsets[idx]);
  auto rightValRaw =
      loadRightValue(builder, values[2 + idx], values[1], dataTy);
  currBlk = builder.GetInsertBlock();

  auto constFloat0 = isDouble ? llvm::ConstantFP::get(dataTy, (double)0.0)
                              : llvm::ConstantFP::get(dataTy, (float)0.0);
  auto constFloatMax = isDouble
      ? llvm::ConstantFP::get(dataTy, std::numeric_limits<double>::max())
      : llvm::ConstantFP::get(dataTy, std::numeric_limits<float>::max());
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
  auto isLeftNan =
      builder.CreateFCmp(llvm::FCmpInst::FCMP_UNO, leftValRaw, constFloat0);
  auto isRightNan =
      builder.CreateFCmp(llvm::FCmpInst::FCMP_UNO, rightValRaw, constFloat0);
  auto neNan =
      builder.CreateICmp(llvm::ICmpInst::ICMP_NE, isLeftNan, isRightNan);
  auto neNanBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_ne_nan_blk", func, nextBlk);
  auto eqNanBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_eq_nan_blk", func, nextBlk);
  auto noNanBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_no_nan_blk", func, nextBlk);
  // unlikely weight
  auto unLikely = llvm::MDBuilder(builder.getContext())
                      .createBranchWeights(1, 1000); //(1U << 20) - 1, 1
  builder.CreateCondBr(neNan, neNanBlk, eqNanBlk, unLikely);

  currBlk = neNanBlk;
  builder.SetInsertPoint(currBlk);
  builder.CreateBr(phiBlk);
  phiInputs.emplace_back(builder.getInt8(0), currBlk); // return false

  currBlk = eqNanBlk;
  builder.SetInsertPoint(currBlk);
  builder.CreateCondBr(isLeftNan, nextBlk, noNanBlk, unLikely); // if Both NaN

  currBlk = noNanBlk;
  builder.SetInsertPoint(currBlk);

  auto leftVal = leftValRaw;
  auto rightVal = rightValRaw;
  /*
```cpp
 leftRow = values[0];
 index = values[1];
 decodedVector = values[2 + idx];
 auto leftVal = *(Integer*) (leftRow + keyOffsets[idx]);
 auto rightVal = *(Integer*) getDecodedValue(decodedVector, index);
 { // check null
   auto leftNull = (leftRow + nullByteOffsets[idx]) & nullByteMasks[idx];
   auto rightNull = getDecodedNull(decodedVector, index);
   if (leftNull && rightNull) {
     return true;
   } else if (leftNull || rightNull) {
     return false;
   }
 }
 cmp = leftVal == rightVal;
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
  auto keyEq = builder.CreateFCmpOEQ(leftVal, rightVal);
  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    phiInputs.emplace_back(std::make_pair(castToI8(builder, keyEq), currBlk));
    builder.CreateBr(phiBlk);
  } else {
    // return false if not equal, otherwise goto next key
    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);
    builder.CreateCondBr(keyEq, nextBlk, keyNeBlk);
    builder.SetInsertPoint(keyNeBlk);
    builder.CreateBr(phiBlk);
    phiInputs.emplace_back(std::make_pair(builder.getInt8(0), keyNeBlk));
  }
  return nextBlk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genStringViewCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  /*
    ```cpp
    // prefix and length
    auto leftPrefix = *(int64_t*) (leftRow + keyOffsets[idx]);
    auto rightPrefix = *(int64_t*) (rightAddr);
    if (leftPrefix != rightPrefix) {
        return false;
    }
    auto leftLen = *(int32_t*) (leftRow + keyOffsets[idx] );
    auto rightLen = *(int32_t*) (rightAddr);
    if (leftLen <= 12 && rightLen <= 12) {
    // if len <=4, only compare first 8 bytes
    // else compare the whole inline part, 16 bytes
    // optimize this comparison with mask, mask[len <=4] = 0, else mask[len] = 0
      auto leftInline = *(int64_t*) (leftRow + keyOffsets[idx] +
                    sizeof(int64_t)) & mask[len];
      auto rightInline = *(int64_t*) (rightAddr + sizeof(int64_t))) &
                    mask[len];
    return leftInline == rightInline
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
  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);
  auto leftAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), values[0], keyOffsets[idx]);

  auto rightAddr = loadRightStringViewAddr(builder, values[2 + idx], values[1]);
  currBlk = builder.GetInsertBlock();
  // Check Prefix + length (8 chars)
  {
    auto int64Ty = builder.getInt64Ty();
    auto leftVal = getValueByPtr(builder, leftAddr, int64Ty, 0);
    auto rightVal = getValueByPtr(builder, rightAddr, int64Ty, 0);

    auto preNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_ne", func, nextBlk);
    auto preEqBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_eq", func, nextBlk);

    auto prefixEq = builder.CreateICmpEQ(leftVal, rightVal);
    builder.CreateCondBr(prefixEq, preEqBlk, preNeBlk);

    // If prefix is NOT equal, return false
    builder.SetInsertPoint(preNeBlk);
    phiInputs.emplace_back(std::make_pair(builder.getInt8(0), preNeBlk));
    builder.CreateBr(phiBlk);

    currBlk = preEqBlk;
  }
  // return inline part comparison
  {
    builder.SetInsertPoint(currBlk);
    auto lenTy = builder.getInt32Ty();
    auto leftLen = getValueByPtr(builder, leftAddr, lenTy, 0);
    constexpr int32_t SV_INLINE_LIMIT = 12;
    auto inlineLimit = llvm::ConstantInt::get(lenTy, SV_INLINE_LIMIT);
    auto isInline = builder.CreateICmpULE(leftLen, inlineLimit);

    auto inlineBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_both_inline", func, nextBlk);
    auto bufBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_buf", func, nextBlk);
    builder.CreateCondBr(isInline, inlineBlk, bufBlk);

    // if both inline, return false if not equal, otherwise goto next key
    currBlk = inlineBlk;
    builder.SetInsertPoint(currBlk);
    auto inlineTy = builder.getInt64Ty();
    llvm::ArrayType* arrayTy = llvm::ArrayType::get(builder.getInt64Ty(), 13);
    auto suffixMaskPtr = builder.CreateGEP(
        arrayTy, values[2 + keysTypes.size()], {builder.getInt32(0), leftLen});
    auto suffixMask = builder.CreateLoad(inlineTy, suffixMaskPtr);

    auto leftInl = builder.CreateAnd(
        suffixMask,
        getValueByPtr(builder, leftAddr, inlineTy, sizeof(int64_t)));
    auto rightInl = builder.CreateAnd(
        suffixMask,
        getValueByPtr(builder, rightAddr, inlineTy, sizeof(int64_t)));
    auto neInlineBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne_inline", func, nextBlk);
    auto neInline = builder.CreateICmpNE(leftInl, rightInl);
    builder.CreateCondBr(neInline, neInlineBlk, nextBlk);
    currBlk = neInlineBlk;
    builder.SetInsertPoint(currBlk);
    phiInputs.emplace_back(std::make_pair(builder.getInt8(0), currBlk));
    builder.CreateBr(phiBlk);

    currBlk = bufBlk;
  }
  // Non-inline (buffer) part comparison
  builder.SetInsertPoint(currBlk);
  auto keyEq =
      createCall(builder, StringViewRowEqVectors, {leftAddr, rightAddr});

  if (idx == keysTypes.size() - 1) {
    phiInputs.emplace_back(std::make_pair(keyEq, currBlk));
    builder.CreateBr(phiBlk);
  } else {
    // return false if not equal, otherwise goto next key
    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);
    builder.CreateCondBr(
        builder.CreateICmpNE(keyEq, builder.getInt8(0)), nextBlk, keyNeBlk);

    builder.SetInsertPoint(keyNeBlk);
    builder.CreateBr(phiBlk);
    phiInputs.emplace_back(std::make_pair(builder.getInt8(0), keyNeBlk));
  }

  return nextBlk;
}

llvm::BasicBlock* RowEqVectorsCodeGenerator::genComplexCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values,
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
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

  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);
  // return i1 (true/false) for row(offset) = decodedvec(index)?
  auto keyEq = createCall(
      builder,
      ComplexTypeRowEqVectors,
      {values[0],
       builder.getInt32(keyOffsets[idx]),
       loadDecodedVector(builder, values[2 + idx]),
       values[1]});

  if (idx == keysTypes.size() - 1) {
    phiInputs.emplace_back(std::make_pair(keyEq, currBlk));
    builder.CreateBr(phiBlk);
  } else {
    // If it not the last key, return false if not equal otherwise goto next_key
    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);
    builder.CreateCondBr(
        builder.CreateICmpNE(keyEq, builder.getInt8(0)), nextBlk, keyNeBlk);

    builder.SetInsertPoint(keyNeBlk);
    builder.CreateBr(phiBlk);
    phiInputs.emplace_back(std::make_pair(builder.getInt8(0), keyNeBlk));
  }

  return nextBlk;
}

bool RowEqVectorsCodeGenerator::GenCmpIR() {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  auto* bytePtrTy = llvm::PointerType::get(llvm_context, 0);
  auto* bytePtrPtrTy = llvm::PointerType::get(llvm_context, 0);
  // Declaration:
  // bool row=vec(char* row, int32_t index, char*[] decodedVectors)
  // row == vec(decodedVectors[0][index], decodedVectors[1][index],...)?
  auto funName = GetCmpFuncName();
  llvm::FunctionType* funcType = llvm::FunctionType::get(
      builder.getInt8Ty(),
      {bytePtrTy, builder.getInt32Ty(), bytePtrPtrTy},
      /*isVarArg=*/false);
  llvm::Function* func = llvm::Function::Create(
      funcType, llvm::Function::ExternalLinkage, funName, llvm_module);

  // Add a basic block to the function.
  llvm::BasicBlock* entryBlk =
      llvm::BasicBlock::Create(llvm_context, "entry", func);
  builder.SetInsertPoint(entryBlk);

  // get all arguments.
  llvm::SmallVector<llvm::Value*> argsValues(keysTypes.size() + 3, nullptr);
  auto* funcArgs = func->args().begin();
  llvm::Value* argRow = funcArgs++;
  llvm::Value* argIndex = funcArgs++;
  llvm::Value* argVectors = funcArgs++;
  argsValues[0] = argRow;
  argsValues[1] = argIndex; // need cast?
  for (auto i = 0; i < keysTypes.size(); ++i) {
    argsValues[i + 2] = builder.CreateLoad(
        bytePtrTy,
        builder.CreateConstInBoundsGEP1_64(bytePtrTy, argVectors, i));
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

  argsValues[keysTypes.size() + 2] = arrayVar;

  // The phi block for keys comparison
  auto phiBlk = llvm::BasicBlock::Create(llvm_context, "phi", func);

  auto currBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(0), func, phiBlk);
  builder.CreateBr(currBlk);

  using PhiNodeInputs = std::vector<std::pair<llvm::Value*, llvm::BasicBlock*>>;
  PhiNodeInputs phiInputs;

  // Step 2: Generate IR for the comparison of all the keys
  for (size_t i = 0; i < keysTypes.size(); ++i) {
    auto kind = keysTypes[i]->kind();

    if (kind == bytedance::bolt::TypeKind::DOUBLE ||
        kind == bytedance::bolt::TypeKind::REAL) {
      currBlk = genFloatPointCmpIR(
          kind, argsValues, i, func, phiInputs, currBlk, phiBlk);
    } else if (
        kind == bytedance::bolt::TypeKind::VARCHAR ||
        kind == bytedance::bolt::TypeKind::VARBINARY) {
      currBlk = genStringViewCmpIR(
          kind, argsValues, i, func, phiInputs, currBlk, phiBlk);
    } else if (
        kind == bytedance::bolt::TypeKind::BOOLEAN ||
        kind == bytedance::bolt::TypeKind::TINYINT ||
        kind == bytedance::bolt::TypeKind::SMALLINT ||
        kind == bytedance::bolt::TypeKind::INTEGER ||
        kind == bytedance::bolt::TypeKind::BIGINT ||
        kind == bytedance::bolt::TypeKind::HUGEINT) {
      currBlk = genIntegerCmpIR(
          kind, argsValues, i, func, phiInputs, currBlk, phiBlk);
    } else if (kind == bytedance::bolt::TypeKind::TIMESTAMP) {
      currBlk = genTimestampCmpIR(
          kind, argsValues, i, func, phiInputs, currBlk, phiBlk);
    } else if (
        kind == bytedance::bolt::TypeKind::ARRAY ||
        kind == bytedance::bolt::TypeKind::MAP ||
        kind == bytedance::bolt::TypeKind::ROW) {
      currBlk = genComplexCmpIR(
          kind, argsValues, i, func, phiInputs, currBlk, phiBlk);
    } else {
      // should not be here.
      throw std::logic_error(
          "IR generation for this type is not supported yet. TODO...");
    }
  }

  // If all key compared
  builder.SetInsertPoint(currBlk);
  builder.CreateBr(phiBlk);
  // if all keys equals,  return true
  phiInputs.emplace_back(builder.getInt8(1), currBlk);

  // Step 3: Phi node, return the comparison result
  {
    builder.SetInsertPoint(phiBlk);
    auto cmpPhi = builder.CreatePHI(builder.getInt8Ty(), phiInputs.size());
    for (auto input : phiInputs) {
      cmpPhi->addIncoming(input.first, input.second);
    }
    builder.CreateRet(cmpPhi);
  }
  auto err = llvm::verifyFunction(*func, &llvm::errs());
  return err;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
