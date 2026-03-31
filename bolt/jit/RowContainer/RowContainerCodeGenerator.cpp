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

#include <llvm/Config/llvm-config.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Intrinsics.h>
#include <type/Type.h>

#include <limits>
#include <memory>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"
#include "bolt/jit/ThrustJITv2.h"
#include "bolt/type/StringView.h"

namespace bytedance::bolt::jit {

namespace {

llvm::PointerType* getBytePtrTy(llvm::LLVMContext& context) {
  return llvm::PointerType::get(context, 0);
}

llvm::FunctionCallee declareFunction(
    llvm::Module& module,
    llvm::StringRef name,
    llvm::Type* returnType,
    llvm::ArrayRef<llvm::Type*> argTypes) {
  return module.getOrInsertFunction(
      name, llvm::FunctionType::get(returnType, argTypes, false));
}

void ensureBuiltinDeclarations(llvm::Module& module) {
  auto& context = module.getContext();
  auto* voidTy = llvm::Type::getVoidTy(context);
  auto* i8Ty = llvm::Type::getInt8Ty(context);
  auto* i16Ty = llvm::Type::getInt16Ty(context);
  auto* i32Ty = llvm::Type::getInt32Ty(context);
  auto* i64Ty = llvm::Type::getInt64Ty(context);
  auto* i128Ty = llvm::Type::getInt128Ty(context);
  auto* floatTy = llvm::Type::getFloatTy(context);
  auto* doubleTy = llvm::Type::getDoubleTy(context);
  auto* i8PtrTy = llvm::PointerType::get(context, 0);
  llvm::Intrinsic::getDeclaration(&module, llvm::Intrinsic::bswap, {i32Ty});
  llvm::Intrinsic::getDeclaration(&module, llvm::Intrinsic::bswap, {i64Ty});

  declareFunction(module, "memcmp", i32Ty, {i8PtrTy, i8PtrTy, i64Ty});

  declareFunction(
      module, "FastRowStringViewCompareAsc", i32Ty, {i8PtrTy, i8PtrTy});
  declareFunction(
      module, "jit_StringViewCompareWrapper", i32Ty, {i8PtrTy, i8PtrTy});
  declareFunction(
      module, "jit_RowBasedStringViewCompare", i32Ty, {i8PtrTy, i8PtrTy});
  declareFunction(
      module,
      "jit_ComplexTypeRowCmpRow",
      i32Ty,
      {i8PtrTy, i8PtrTy, i64Ty, i32Ty, i8Ty, i8Ty});
  declareFunction(
      module,
      "jit_RowBased_ComplexTypeRowCmpRow",
      i32Ty,
      {i8PtrTy, i8PtrTy, i64Ty, i32Ty, i8Ty, i8Ty});

  declareFunction(
      module, "jit_StringViewRowEqVectors", i8Ty, {i8PtrTy, i8PtrTy});
  declareFunction(module, "jit_GetDecodedValueBool", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI8", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI16", i16Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI32", i32Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI64", i64Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI128", i128Ty, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_GetDecodedValueFloat", floatTy, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_GetDecodedValueDouble", doubleTy, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_CmpRowVecTimestamp", i8Ty, {i8PtrTy, i32Ty, i8PtrTy});
  declareFunction(
      module, "jit_GetDecodedValueStringView", i8PtrTy, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedIsNull", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(
      module,
      "jit_ComplexTypeRowEqVectors",
      i8Ty,
      {i8PtrTy, i32Ty, i8PtrTy, i32Ty});
  declareFunction(module, "jit_DebugPrint", voidTy, {i64Ty, i64Ty, i64Ty});
}

} // namespace

CompiledModuleSP RowContainerCodeGenerator::codegen() {
  auto fn = GetCmpFuncName();

  auto jit = ThrustJITv2::getInstance();

  if (auto mod = jit->LookupSymbolsInCache(fn)) {
    return mod;
  }

  auto genIR = [this](llvm::Module& m) -> bool {
    ensureBuiltinDeclarations(m);
    this->setModule(&m);
    return this->GenCmpIR();
  };

  auto module = jit->CompileModule(genIR, fn);
  if (isEqualOp() || !flags.empty()) { // only for row cmp/= row
    auto cacheTypes = std::make_unique<std::vector<bytedance::bolt::TypePtr>>();
    for (auto& t : keysTypes) {
      switch (t->kind()) {
        case bytedance::bolt::TypeKind::ARRAY:
        case bytedance::bolt::TypeKind::ROW:
        case bytedance::bolt::TypeKind::MAP:
          cacheTypes->push_back(t);
          break;
        default:
          break;
      }
    }
    if (cacheTypes->size() > 0) {
      module->setUserData(static_cast<void*>(cacheTypes.release()));
      module->appendCleanCallback([mod = module.get()] {
        auto* cacheTypes = static_cast<std::vector<bytedance::bolt::TypePtr>*>(
            mod->getUserData());
        cacheTypes->clear();
        delete cacheTypes;
        mod->setUserData(nullptr);
      });
    }
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
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* nextBlk,
    llvm::BasicBlock* phiBlk,
    PhiNodeInputs& phiInputs) {
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

  builder.SetInsertPoint(currBlk);

  // isNullAt(idx)
  auto rowTy = builder.getInt8Ty();
  auto byteTy = builder.getInt8Ty();
  llvm::PointerType* bytePtrTy = byteTy->getPointerTo();
  auto constMask = llvm::ConstantInt::get(byteTy, nullByteMasks[idx]);

  auto voidLeftAddr = builder.CreateConstInBoundsGEP1_64(
      rowTy, values[0], nullByteOffsets[idx]);
  auto voidRightAddr = builder.CreateConstInBoundsGEP1_64(
      rowTy, values[1], nullByteOffsets[idx]);
  auto leftAddr = builder.CreatePointerCast(voidLeftAddr, bytePtrTy);
  auto rightAddr = builder.CreatePointerCast(voidRightAddr, bytePtrTy);
  auto leftValUnmask = builder.CreateLoad(byteTy, leftAddr);
  auto rightValUnmask = builder.CreateLoad(byteTy, rightAddr);
  auto leftVal = builder.CreateAnd(leftValUnmask, constMask);
  auto rightVal = builder.CreateAnd(rightValUnmask, constMask);

  auto nilNe = builder.CreateICmp(llvm::ICmpInst::ICMP_NE, leftVal, rightVal);
  auto nilNeBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_ne_blk", func, nextBlk);
  auto nilEqBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_nil_eq_blk", func, nextBlk);
  auto noNilBlk = llvm::BasicBlock::Create(
      llvm_context, getLabel(idx) + "_no_nil_blk", func, nextBlk);
  auto unLikely = llvm::MDBuilder(builder.getContext())
                      .createBranchWeights(1, 1000); //(1U << 20) - 1, 1

  builder.CreateCondBr(nilNe, nilNeBlk, nilEqBlk, unLikely);

  currBlk = nilNeBlk;
  builder.SetInsertPoint(currBlk);

  // Note: CreateTrunc( int8 -> int1 ) does NOT work
  auto constByte0 = llvm::ConstantInt::get(byteTy, 0);

  if (isEqualOp()) { // directly return false
    phiInputs.emplace_back(builder.getInt8(0), currBlk);
  } else {
    auto oneOpNil = builder.CreateICmpNE(
        flags[idx].nullsFirst ? leftVal : rightVal, constByte0);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(
              oneOpNil, builder.getInt8(-1), builder.getInt8(1)),
          currBlk);
    } else {
      phiInputs.emplace_back(castToI8(builder, oneOpNil), currBlk);
    }
  }
  builder.CreateBr(phiBlk);

  currBlk = nilEqBlk;
  builder.SetInsertPoint(currBlk);
  builder.CreateCondBr(
      builder.CreateICmpNE(leftVal, constByte0), nextBlk, noNilBlk, unLikely);

  return noNilBlk;
}

llvm::BasicBlock* RowContainerCodeGenerator::genFloatPointCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  auto rowTy = builder.getInt8Ty();
  auto dataTy = (kind == bytedance::bolt::TypeKind::DOUBLE)
      ? builder.getDoubleTy()
      : builder.getFloatTy();
  llvm::PointerType* dataPtrTy = dataTy->getPointerTo();

  // Block for next key.
  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);

  // Generate value comparison IR for check nullity
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);

  uint64_t rowOffset = keyOffsets[idx];
  auto voidLeftAddr =
      builder.CreateConstInBoundsGEP1_64(rowTy, values[0], rowOffset);
  auto voidRightAddr =
      builder.CreateConstInBoundsGEP1_64(rowTy, values[1], rowOffset);
  auto leftAddr = builder.CreatePointerCast(voidLeftAddr, dataPtrTy);
  auto rightAddr = builder.CreatePointerCast(voidRightAddr, dataPtrTy);
  auto leftValRaw = builder.CreateLoad(dataTy, leftAddr);
  auto rightValRaw = builder.CreateLoad(dataTy, rightAddr);

  bool isDouble = kind == bytedance::bolt::TypeKind::DOUBLE;
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
  //      return asc ?  rightIsNan : leftIsNan;
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
  if (isEqualOp()) { // return false
    phiInputs.emplace_back(builder.getInt8(0), currBlk);
  } else {
    auto res = flags[idx].ascending ? isRightNan : isLeftNan;
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(res, builder.getInt8(-1), builder.getInt8(1)),
          currBlk);
    } else {
      phiInputs.emplace_back(castToI8(builder, res), currBlk);
    }
  }
  builder.CreateBr(phiBlk);

  currBlk = eqNanBlk;
  builder.SetInsertPoint(currBlk);
  builder.CreateCondBr(isLeftNan, nextBlk, noNanBlk, unLikely); // if Both NaN
  // ======  NaN check ends =============================

  /*
  ```cpp
    auto cmpOp = flags[idx].ascending ? FCMP_OLT : FCMP_OGT;

    auto leftVal = (double*) (leftRow + keyOffsets[idx]);
    auto rightVal = (double*) (rightRow + keyOffsets[idx]);

    if constexpr (is_last_key) {
       return leftVal cmpOp rightVal;
    }
    else {
       if (leftVal == rightVal) {
         goto next_key_compare;
       }
       return return leftVal cmpOp rightVal;
    }
  ```
  */
  currBlk = noNanBlk;
  builder.SetInsertPoint(currBlk);

  auto leftVal = leftValRaw;
  auto rightVal = rightValRaw;

  // Ordered return true if the operands are comparable (neither number is NaN):
  // Ordered comparison of 1.0 and 1.0 gives true.
  // Ordered comparison of NaN and 1.0 gives false.
  // Ordered comparison of NaN and NaN gives false.
  auto cmpOp = isEqualOp()   ? llvm::FCmpInst::FCMP_OEQ
      : flags[idx].ascending ? llvm::FCmpInst::FCMP_OLT
                             : llvm::FCmpInst::FCMP_OGT;

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    auto cmpRes = builder.CreateFCmp(cmpOp, leftVal, rightVal);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(
              cmpRes,
              builder.getInt8(-1),
              castToI8(builder, builder.CreateFCmp(cmpOp, rightVal, leftVal))),
          currBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, cmpRes), currBlk));
    }
    builder.CreateBr(phiBlk);
  } else {
    // If it not the last key, firstly check if left equals with right
    auto keyEq =
        builder.CreateFCmp(llvm::FCmpInst::FCMP_OEQ, leftVal, rightVal);

    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, phiBlk);

    builder.CreateCondBr(keyEq, nextBlk, keyNeBlk);

    builder.SetInsertPoint(keyNeBlk);
    auto resLt = builder.CreateFCmp(cmpOp, leftVal, rightVal);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(resLt, builder.getInt8(-1), builder.getInt8(1)),
          keyNeBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, resLt), keyNeBlk));
    }
    builder.CreateBr(phiBlk);
  }
  return nextBlk;
};

llvm::BasicBlock* RowContainerCodeGenerator::genIntegerCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  /*
  ```cpp
    auto cmpOp = flags[idx].ascending ? OLT : OGT;

    auto leftVal = *(Integer*) (leftRow + keyOffsets[idx]);
    auto rightVal = *(Integer*) (rightRow + keyOffsets[idx]);

    if constexpr (is_last_key) {
       return leftVal cmpOp rightVal;
    }
    else {
       if (leftVal == rightVal) {
         goto next_key_compare;
       }
       return return leftVal cmpOp rightVal;
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
  }

  // Block for next key.
  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);

  // Generate value comparison IR for check nullity
  // && flags[idx].nullsFirst == flags[idx].ascending
  // if we ignore the nullity check, we have to compare Max and Null, a bit
  // complicated, just ignore this tricky optimization we can optimize this
  // after we refactor RowConatiner
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);

  uint64_t rowOffset = keyOffsets[idx];
  llvm::Value* leftVal = nullptr;
  llvm::Value* rightVal = nullptr;
  if (kind == bytedance::bolt::TypeKind::HUGEINT) {
    leftVal = getHugeIntValueByPtr(builder, values[0], rowOffset);
    rightVal = getHugeIntValueByPtr(builder, values[1], rowOffset);
  } else {
    leftVal = getValueByPtr(builder, values[0], dataTy, rowOffset);
    rightVal = getValueByPtr(builder, values[1], dataTy, rowOffset);
  }

  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;

  // If it the last key, generate the fast logic
  if (idx == keysTypes.size() - 1) {
    auto cmpRes = builder.CreateICmp(cmpOp, leftVal, rightVal);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(
              cmpRes,
              builder.getInt8(-1),
              castToI8(builder, builder.CreateICmp(cmpOp, rightVal, leftVal))),
          currBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, cmpRes), currBlk));
    }
  } else {
    // If it not the last key, firstly check if left equals with right
    auto keyEq = builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, leftVal, rightVal);

    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);

    builder.CreateCondBr(keyEq, nextBlk, keyNeBlk);

    builder.SetInsertPoint(keyNeBlk);
    auto resLt = builder.CreateICmp(cmpOp, leftVal, rightVal);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(resLt, builder.getInt8(-1), builder.getInt8(1)),
          keyNeBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, resLt), keyNeBlk));
    }
  }
  builder.CreateBr(phiBlk);
  return nextBlk;
};

llvm::BasicBlock* RowContainerCodeGenerator::genStringViewCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  /*
    ```cpp
    // prefix
    auto  cmpOp =  isAsc ? "<=" : " >=";
    auto leftPrefix = *(int32_t*) (leftRow + keyOffsets[idx] +
                            sizeof(uint32_t));
    auto rightPrefix = *(int32_t*) (rightRow +
                              keyOffsets[idx] + sizeof(uint32_t));
      if (leftPrefix != rightPrefix) {
        if constexpr ( little_endian ) {
          leftPrefix = bswap (leftPrefix);
          rightPrefix = bswap (rightPrefix);
        }
        return (leftPrefix)  Op  rightPrefix;
    }
    // inline part
    auto leftLen = *(int32_t*) (leftRow + keyOffsets[idx] );
    auto rightLen = *(int32_t*) (rightRow + keyOffsets[idx]);
    if (leftLen <= 12 && rightLen <= 12) {
      auto leftInline = *(int64_t*) (leftRow + keyOffsets[idx] +
                    sizeof(int64_t));
      auto rightInline = *(int64_t*) (rightRow + keyOffsets[idx]
                    + sizeof(int64_t)));
      if (leftInline != rightInline) {
        if constexpr (little_endian ) {
          leftInline = bswap (leftInline);
          rightInline = bswap (rightInline);
        }
        return (leftInline)  Op  rightInline;
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
  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);

  // Generate value comparison IR for check nullity
  if (hasNullKeys &&
      (isEqualOp() || flags[idx].nullsFirst == flags[idx].ascending)) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);
  auto ty = builder.getInt8Ty(); // stringViewType;
  auto leftAddr =
      builder.CreateConstInBoundsGEP1_64(ty, values[0], keyOffsets[idx]);
  auto rightAddr =
      builder.CreateConstInBoundsGEP1_64(ty, values[1], keyOffsets[idx]);
  auto int32Ty = builder.getInt32Ty();
  auto int64Ty = builder.getInt64Ty();

  auto leftLen = getValueByPtr(builder, values[0], int32Ty, keyOffsets[idx]);
  auto rightLen = getValueByPtr(builder, values[1], int32Ty, keyOffsets[idx]);

  auto inlineLimit =
      llvm::ConstantInt::get(int32Ty, bytedance::bolt::StringView::kInlineSize);
  auto prefixLimit = builder.getInt32(bytedance::bolt::StringView::kPrefixSize);
  if (isEqualOp()) { // cmp first int64_t
    auto leftVal = getValueByPtr(builder, leftAddr, int64Ty, 0);
    auto rightVal = getValueByPtr(builder, rightAddr, int64Ty, 0);
    auto preNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_ne_", func, nextBlk);
    auto preEqBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_eq", func, nextBlk);
    auto prefixEq =
        builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, leftVal, rightVal);
    builder.CreateCondBr(prefixEq, preEqBlk, preNeBlk);
    // If prefix is NOT equal
    builder.SetInsertPoint(preNeBlk);
    phiInputs.emplace_back(
        std::make_pair(castToI8(builder, prefixEq), preNeBlk));
    builder.CreateBr(phiBlk);

    // If prefix is equal
    currBlk = preEqBlk;
  } else { // Check Prefix (4 chars)
    auto leftPreAddr = builder.CreateConstInBoundsGEP1_64(
        ty, values[0], keyOffsets[idx] + sizeof(uint32_t));
    auto rightPreAddr = builder.CreateConstInBoundsGEP1_64(
        ty, values[1], keyOffsets[idx] + sizeof(uint32_t));

    auto leftPreCastAddr =
        builder.CreatePointerCast(leftPreAddr, int32Ty->getPointerTo());
    auto rightPreCastAddr =
        builder.CreatePointerCast(rightPreAddr, int32Ty->getPointerTo());
    auto leftVal = builder.CreateLoad(int32Ty, leftPreCastAddr);
    auto rightVal = builder.CreateLoad(int32Ty, rightPreCastAddr);

    auto preNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_ne_", func, nextBlk);
    auto preEqBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_pre_eq", func, nextBlk);
    auto prefixEq =
        builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, leftVal, rightVal);
    builder.CreateCondBr(prefixEq, preEqBlk, preNeBlk);

    // If prefix is NOT equal
    builder.SetInsertPoint(preNeBlk);
    llvm::Value* preCmpNe{nullptr};
    auto preOp = flags[idx].ascending ? llvm::ICmpInst::ICMP_ULT
                                      : llvm::ICmpInst::ICMP_UGT;
    if (!bolt::jit::ThrustJITv2::getInstance()->getDataLayout().isBigEndian()) {
      std::vector<llvm::Value*> args;
      auto callee = llvm_module->getFunction("llvm.bswap.i32");
      args.push_back(leftVal);
      auto leftSwapVal = builder.CreateCall(callee, args);
      args.clear();
      args.push_back(rightVal);
      auto rightSwapVal = builder.CreateCall(callee, args);
      // unsigned int
      preCmpNe = builder.CreateICmp(preOp, leftSwapVal, rightSwapVal);
    } else {
      // unsigned int
      preCmpNe = builder.CreateICmp(preOp, leftVal, rightVal);
    }
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(
              preCmpNe, builder.getInt8(-1), builder.getInt8(1)),
          preNeBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, preCmpNe), preNeBlk));
    }
    builder.CreateBr(phiBlk);

    currBlk = preEqBlk;
    /// equal prefix with un-equal size
    builder.SetInsertPoint(currBlk);
    auto minLen = builder.CreateSelect(
        builder.CreateICmpULT(leftLen, rightLen), leftLen, rightLen);
    // if (min_size <= prefixLimit) return leftLen cmp rightLen
    auto sizeCmp = builder.CreateICmpULE(minLen, prefixLimit);
    auto lePreLimit = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_le_pre_limit_", func, nextBlk);
    auto gtPreLimit = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_gt_pre_limit_", func, nextBlk);
    builder.CreateCondBr(sizeCmp, lePreLimit, gtPreLimit);
    currBlk = lePreLimit;
    builder.SetInsertPoint(currBlk);
    // if (leftLen == rightLen) go to inline comparison
    // else return leftLen cmp rightLen
    auto lenNe = builder.CreateICmpNE(leftLen, rightLen);
    auto lePreNeLen = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_le_pre_ne_len_", func, nextBlk);
    builder.CreateCondBr(lenNe, lePreNeLen, gtPreLimit);
    currBlk = lePreNeLen;
    builder.SetInsertPoint(currBlk);
    auto lenLess = builder.CreateICmp(preOp, leftLen, rightLen);
    if (isCmp()) {
      auto res = builder.CreateSelect(
          lenLess, builder.getInt8(-1), builder.getInt8(1));
      phiInputs.emplace_back(res, currBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, lenLess), currBlk));
    }
    builder.CreateBr(phiBlk);
    currBlk = gtPreLimit;
  }

  // Inline part comparison
  {
    builder.SetInsertPoint(currBlk);
    auto getInlineInt64 = [&](llvm::Value* inlineVal) -> llvm::Value* {
      if (!bolt::jit::ThrustJITv2::getInstance()
               ->getDataLayout()
               .isBigEndian()) {
        std::vector<llvm::Value*> args;
        auto callee = llvm_module->getFunction("llvm.bswap.i64");
        args.push_back(inlineVal);
        inlineVal = builder.CreateCall(callee, args);
        args.clear();
      }
      return inlineVal;
    };

    auto bothInline = builder.CreateAnd(
        builder.CreateICmpULE(leftLen, inlineLimit),
        builder.CreateICmpULE(rightLen, inlineLimit));

    auto inlineBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_both_inline", func, nextBlk);
    auto bufBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_buf", func, nextBlk);
    builder.CreateCondBr(bothInline, inlineBlk, bufBlk);

    currBlk = inlineBlk;
    builder.SetInsertPoint(currBlk);
    llvm::Value* leftInlineValRaw = getValueByPtr(
        builder, values[0], int64Ty, keyOffsets[idx] + sizeof(int64_t));
    llvm::Value* rightInlineValRaw = getValueByPtr(
        builder, values[1], int64Ty, keyOffsets[idx] + sizeof(int64_t));
    llvm::ArrayType* arrayTy = llvm::ArrayType::get(builder.getInt64Ty(), 13);
    auto leftMaskPtr =
        builder.CreateGEP(arrayTy, values[2], {builder.getInt32(0), leftLen});
    auto leftMask = builder.CreateLoad(int64Ty, leftMaskPtr);
    auto rightMaskPtr =
        builder.CreateGEP(arrayTy, values[2], {builder.getInt32(0), rightLen});
    auto rightMask = builder.CreateLoad(int64Ty, rightMaskPtr);
    auto leftInlineVal = builder.CreateAnd(leftInlineValRaw, leftMask);
    auto rightInlineVal = builder.CreateAnd(rightInlineValRaw, rightMask);
    auto leftInl = isEqualOp() ? leftInlineVal : getInlineInt64(leftInlineVal);
    auto rightInl =
        isEqualOp() ? rightInlineVal : getInlineInt64(rightInlineVal);
    auto neInlineBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne_inline", func, nextBlk);
    auto eqInlineBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_eq_inline", func, nextBlk);
    auto neInline = builder.CreateICmpNE(leftInl, rightInl);
    builder.CreateCondBr(
        neInline, neInlineBlk, isEqualOp() ? nextBlk : eqInlineBlk);
    currBlk = neInlineBlk;
    builder.SetInsertPoint(currBlk);

    // as unsigned integer
    auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
        : flags[idx].ascending ? llvm::ICmpInst::ICMP_ULT
                               : llvm::ICmpInst::ICMP_UGT;
    auto cmpRes = builder.CreateICmp(cmpOp, leftInl, rightInl);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(cmpRes, builder.getInt8(-1), builder.getInt8(1)),
          currBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, cmpRes), currBlk));
    }
    builder.CreateBr(phiBlk);
    { // only for !equalOp
      currBlk = eqInlineBlk;
      builder.SetInsertPoint(currBlk);
      // if (leftLen == rightLen) then go to next
      // else return leftLen cmp rightLen
      auto lenNe = builder.CreateICmpNE(leftLen, rightLen);
      auto eqInlineNeLen = llvm::BasicBlock::Create(
          llvm_context, getLabel(idx) + "_eq_inline_ne_len", func, nextBlk);
      builder.CreateCondBr(lenNe, eqInlineNeLen, nextBlk);
      currBlk = eqInlineNeLen;
      builder.SetInsertPoint(currBlk);
      auto lenLess = builder.CreateICmp(cmpOp, leftLen, rightLen);
      if (isCmp()) {
        auto resLt = builder.CreateSelect(
            lenLess, builder.getInt8(-1), builder.getInt8(1));
        phiInputs.emplace_back(resLt, currBlk);
      } else {
        phiInputs.emplace_back(
            std::make_pair(castToI8(builder, lenLess), currBlk));
      }
      builder.CreateBr(phiBlk);
    }
    currBlk = bufBlk;
  }

  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;
  // Non-inline (buffer) part comparison
  builder.SetInsertPoint(currBlk);
  std::vector<llvm::Value*> args;
  args.push_back(leftAddr);
  args.push_back(rightAddr);

  /// row based cmp is special
  if (isCmp() && !flags[idx].ascending) {
    std::swap(args[0], args[1]);
  }

  auto int32Type = llvm::Type::getInt32Ty(llvm_context);
  auto callee = isCmpSpill()
      ? llvm_module->getFunction(RowBasedStringViewCompare)
      : llvm_module->getFunction(rowStringViewCompareAsc);
  auto strCmpRes = builder.CreateCall(callee, args);
  auto const0 = llvm::ConstantInt::get(int32Type, 0);

  // if it is the last key
  if (idx == keysTypes.size() - 1) {
    auto cmpRes = builder.CreateICmp(cmpOp, strCmpRes, const0);
    if (isCmp()) {
      phiInputs.emplace_back(castToI8(builder, strCmpRes, true), currBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, cmpRes), currBlk));
    }
  } else {
    // If it not the last key, firstly check if left equals with right
    auto keyEq = builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, strCmpRes, const0);

    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);
    builder.CreateCondBr(keyEq, nextBlk, keyNeBlk);

    builder.SetInsertPoint(keyNeBlk);
    auto resNe = builder.CreateICmp(cmpOp, strCmpRes, const0);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(resNe, builder.getInt8(-1), builder.getInt8(1)),
          keyNeBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, resNe), keyNeBlk));
    }
  }
  builder.CreateBr(phiBlk);
  return nextBlk;
};

llvm::BasicBlock* RowContainerCodeGenerator::genTimestampCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);

  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);

  auto rowTy = builder.getInt8Ty();

  llvm::Type* dataTy = builder.getInt64Ty();

  llvm::PointerType* dataPtrTy = dataTy->getPointerTo();

  // Generate value comparison IR for check nullity
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Offsets for second & nano
  std::vector<int32_t> timestampOffsets{
      keyOffsets[idx], keyOffsets[idx] + (int32_t)sizeof(int64_t)};

  // Refer to bytedance::bolt::Timestamp
  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;
  for (auto i = 0; i < 2; ++i) {
    builder.SetInsertPoint(currBlk);

    auto rowOffset = timestampOffsets[i];
    auto voidLeftAddr =
        builder.CreateConstInBoundsGEP1_64(rowTy, values[0], rowOffset);
    auto voidRightAddr =
        builder.CreateConstInBoundsGEP1_64(rowTy, values[1], rowOffset);
    auto leftAddr = builder.CreatePointerCast(voidLeftAddr, dataPtrTy);
    auto rightAddr = builder.CreatePointerCast(voidRightAddr, dataPtrTy);
    auto leftVal = builder.CreateLoad(dataTy, leftAddr);
    auto rightVal = builder.CreateLoad(dataTy, rightAddr);

    if (i == 1 && idx == keysTypes.size() - 1) {
      auto cmpRes = builder.CreateICmp(cmpOp, leftVal, rightVal);

      if (isCmp()) {
        phiInputs.emplace_back(std::make_pair(
            builder.CreateSelect(
                cmpRes,
                builder.getInt8(-1),
                castToI8(
                    builder, builder.CreateICmp(cmpOp, rightVal, leftVal))),
            currBlk));
      } else {
        phiInputs.emplace_back(
            std::make_pair(castToI8(builder, cmpRes), currBlk));
      }
      builder.CreateBr(phiBlk);
    } else {
      auto keyEq =
          builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, leftVal, rightVal);

      auto keyNeBlk = llvm::BasicBlock::Create(
          llvm_context,
          getLabel(idx) + "_ne_" + (i == 0 ? "sec" : "nano"),
          func,
          nextBlk);

      auto tmpNext = i == 0
          ? llvm::BasicBlock::Create(
                llvm_context, getLabel(idx) + "_nano", func, nextBlk)
          : nextBlk;

      builder.CreateCondBr(keyEq, tmpNext, keyNeBlk);

      builder.SetInsertPoint(keyNeBlk);

      auto resNe = builder.CreateICmp(cmpOp, leftVal, rightVal);
      if (isCmp()) {
        phiInputs.emplace_back(std::make_pair(
            builder.CreateSelect(
                resNe, builder.getInt8(-1), builder.getInt8(1)),
            keyNeBlk));
      } else {
        phiInputs.emplace_back(
            std::make_pair(castToI8(builder, resNe), keyNeBlk));
      }
      builder.CreateBr(phiBlk);
      currBlk = tmpNext;
    }
  }

  return nextBlk;
}

llvm::BasicBlock* RowContainerCodeGenerator::genComplexCmpIR(
    bytedance::bolt::TypeKind kind,
    const llvm::SmallVector<llvm::Value*>& values, // left row, right row
    const size_t idx,
    llvm::Function* func,
    PhiNodeInputs& phiInputs,
    llvm::BasicBlock* currBlk,
    llvm::BasicBlock* phiBlk) {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  // ```cpp
  //   auto res = jit_ComplexTypeRowCmpRow(left_row(offset),
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

  auto nextBlk =
      llvm::BasicBlock::Create(llvm_context, getLabel(idx + 1), func, phiBlk);
  if (hasNullKeys) {
    currBlk =
        genNullBitCmpIR(values, idx, func, currBlk, nextBlk, phiBlk, phiInputs);
  }

  // Generate value comparison IR
  builder.SetInsertPoint(currBlk);
  // return i1 (true/false) for row(offset) = decodedvec(index)?

  auto keyCmpRes = createCall(
      builder,
      isCmpSpill() ? RowBased_ComplexTypeRowCmpRow : ComplexTypeRowCmpRow,
      {values[0],
       values[1],
       builder.getInt64((int64_t)keysTypes[idx].get()),
       builder.getInt32(keyOffsets[idx]),
       builder.getInt8(isEqualOp() ? 0 : (int8_t)flags[idx].nullsFirst),
       builder.getInt8(isEqualOp() ? 0 : (int8_t)flags[idx].ascending)});
  auto const0 = llvm::ConstantInt::get(builder.getInt32Ty(), 0);
  auto cmpOp = isEqualOp()   ? llvm::ICmpInst::ICMP_EQ
      : flags[idx].ascending ? llvm::ICmpInst::ICMP_SLT
                             : llvm::ICmpInst::ICMP_SGT;
  // if it is the last key
  if (idx == keysTypes.size() - 1) {
    auto cmpRes = builder.CreateICmp(cmpOp, keyCmpRes, const0);

    if (isCmp()) {
      phiInputs.emplace_back(castToI8(builder, keyCmpRes, true), currBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, cmpRes), currBlk));
    }
  } else {
    // If it not the last key, firstly check if left equals with right
    auto keyEq = builder.CreateICmp(llvm::ICmpInst::ICMP_EQ, keyCmpRes, const0);

    auto keyNeBlk = llvm::BasicBlock::Create(
        llvm_context, getLabel(idx) + "_ne", func, nextBlk);
    builder.CreateCondBr(keyEq, nextBlk, keyNeBlk);

    builder.SetInsertPoint(keyNeBlk);
    auto resNe = builder.CreateICmp(cmpOp, keyCmpRes, const0);
    if (isCmp()) {
      phiInputs.emplace_back(
          builder.CreateSelect(resNe, builder.getInt8(-1), builder.getInt8(1)),
          keyNeBlk);
    } else {
      phiInputs.emplace_back(
          std::make_pair(castToI8(builder, resNe), keyNeBlk));
    }
  }
  builder.CreateBr(phiBlk);
  return nextBlk;
}

bool RowContainerCodeGenerator::GenCmpIR() {
  auto& llvm_context = llvm_module->getContext();
  llvm::IRBuilder<> builder(llvm_context);
  auto* bytePtrTy = getBytePtrTy(llvm_context);

  // Declaration:
  // int8_t row_cmp(char* l, char* r) ;
  auto funName = GetCmpFuncName();
  llvm::FunctionType* funcType = llvm::FunctionType::get(
      builder.getInt8Ty(), {bytePtrTy, bytePtrTy}, /*isVarArg=*/false);
  llvm::Function* func = llvm::Function::Create(
      funcType, llvm::Function::ExternalLinkage, funName, llvm_module);
  // set as noexcept is performance essential for Compare()
  // llvm::AttrBuilder ab;
  // ab.addAttribute(llvm::Attribute::NoUnwind);
  // func->addAttributes(llvm::AttributeList::FunctionIndex, ab);

  unsigned int argIdx = 0;
  std::vector<std::string> argsName{"l", "r"};
  for (auto& arg : func->args()) {
    arg.setName(argsName[argIdx++]);
  }

  // Add a basic block to the function.
  llvm::BasicBlock* entryBlk =
      llvm::BasicBlock::Create(llvm_context, "entry", func);
  builder.SetInsertPoint(entryBlk);
  // Function arguments:  void* left, void* right
  llvm::SmallVector<llvm::Value*> argsValues;
  for (auto&& arg : func->args()) {
    argsValues.emplace_back(&arg);
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
  argsValues.emplace_back(arrayVar);

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
        kind == bytedance::bolt::TypeKind::ROW ||
        kind == bytedance::bolt::TypeKind::ARRAY ||
        kind == bytedance::bolt::TypeKind::MAP) {
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
  // if all keys equals, switch CmpType
  // SORT_LESS: left row < right row => return 0
  // CMP: return 0
  // EQUAL: return 1
  phiInputs.emplace_back(builder.getInt8(isEqualOp() ? 1 : 0), currBlk);
  builder.CreateBr(phiBlk);

  // Step 3: Phi node, return the comparison result
  {
    builder.SetInsertPoint(phiBlk);
    auto cmpPhi = builder.CreatePHI(builder.getInt8Ty(), phiInputs.size());
    for (auto input : phiInputs) {
      cmpPhi->addIncoming(input.first, input.second);
    }
    builder.CreateRet(cmpPhi);
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

} // namespace bytedance::bolt::jit

extern "C" {
extern int jit_StringViewCompareWrapper(char* l, char* r);

// This dummy function will never be called in fact.
// It is just a trick to make sure that the linker will not skip the functions
// in RowContainer.cpp, which will be called by JIT.
__attribute__((used)) void dummyImportFuctionsJitCalled(void) {
  jit_StringViewCompareWrapper(nullptr, nullptr);
}
}

#endif // ENABLE_BOLT_JIT
