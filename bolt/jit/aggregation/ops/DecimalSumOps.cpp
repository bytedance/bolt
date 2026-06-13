/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include <cstddef>

#include "bolt/jit/aggregation/HashAggrJit.h"
#include "bolt/jit/aggregation/HashAggrJitDecimalState.h"
#include "bolt/jit/aggregation/ops/DecimalOps.h"

namespace bytedance::bolt::jit {

namespace {

// Field offsets within JitDecimalSumState, relative to slot.offset.
constexpr int32_t kSumOffset =
    static_cast<int32_t>(offsetof(JitDecimalSumState, sum));
constexpr int32_t kOverflowOffset =
    static_cast<int32_t>(offsetof(JitDecimalSumState, overflow));
constexpr int32_t kIsEmptyOffset =
    static_cast<int32_t>(offsetof(JitDecimalSumState, isEmpty));

void compileDecimalSumInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto& b = codegen.builder();
  codegen.setAccumulatorNull(group, slot);
  // sum = 0 (i128), overflow = 0 (i64), isEmpty = true (i8).
  codegen.storeValue(
      group,
      b.getInt128Ty(),
      slot.offset + kSumOffset,
      llvm::ConstantInt::get(b.getInt128Ty(), 0));
  codegen.storeValue(
      group, b.getInt64Ty(), slot.offset + kOverflowOffset, b.getInt64(0));
  codegen.storeValue(
      group, b.getInt8Ty(), slot.offset + kIsEmptyOffset, b.getInt8(1));
}

void compileDecimalSumAddRawInput(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock*) {
  auto& b = codegen.builder();
  auto* inputRow = input.read(row, slot.desc.inputKind);
  auto* rawValue = IRRow::getValue(codegen.builder(), inputRow);
  auto* value =
      codegen.castValue(rawValue, slot.desc.inputKind, HashAggrJitValueKind::Int128);
  codegen.clearAccumulatorNull(group, slot);
  emitDecimalAddWithOverflow(
      codegen,
      group,
      slot.offset + kSumOffset,
      slot.offset + kOverflowOffset,
      value);
  codegen.storeValue(
      group, b.getInt8Ty(), slot.offset + kIsEmptyOffset, b.getInt8(0));
}

void compileDecimalSumAddIntermediateResults(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock* nextBlock) {
  auto& b = codegen.builder();
  auto* function = b.GetInsertBlock()->getParent();
  auto* continueBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "sum_decimal_merge_cont",
      function,
      nextBlock);
  auto* overflowBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "sum_decimal_merge_overflow",
      function,
      continueBlock);
  auto* mergeBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(), "sum_decimal_merge", function, continueBlock);
  auto* sumRow = input.readRowField(row, 0, slot.desc.inputKind);
  auto* incomingIsEmpty =
      input.readRowFieldValue(row, 1, HashAggrJitValueKind::Bool);
  auto* sumIsNull = IRRow::getIsNull(b, sumRow);
  auto* isNotEmpty = b.CreateICmpEQ(incomingIsEmpty, b.getInt8(0));
  auto* isOverflow = b.CreateAnd(sumIsNull, isNotEmpty);
  b.CreateCondBr(isOverflow, overflowBlock, mergeBlock);

  b.SetInsertPoint(overflowBlock);
  codegen.setAccumulatorNull(group, slot);
  b.CreateBr(continueBlock);

  b.SetInsertPoint(mergeBlock);
  auto* sum = IRRow::getValue(b, sumRow);
  auto* value =
      codegen.castValue(sum, slot.desc.inputKind, HashAggrJitValueKind::Int128);
  codegen.clearAccumulatorNull(group, slot);
  emitDecimalAddWithOverflow(
      codegen,
      group,
      slot.offset + kSumOffset,
      slot.offset + kOverflowOffset,
      value);
  // isEmpty = isEmpty && incomingIsEmpty.
  auto* oldIsEmpty =
      codegen.loadValue(group, b.getInt8Ty(), slot.offset + kIsEmptyOffset);
  auto* bothEmpty = b.CreateAnd(
      b.CreateICmpNE(oldIsEmpty, b.getInt8(0)),
      b.CreateICmpNE(incomingIsEmpty, b.getInt8(0)));
  codegen.storeValue(
      group,
      b.getInt8Ty(),
      slot.offset + kIsEmptyOffset,
      b.CreateZExt(bothEmpty, b.getInt8Ty()));
  b.CreateBr(continueBlock);

  b.SetInsertPoint(continueBlock);
}

bool canCompileDecimalSumExtract(const HashAggrJitSlot&, bool) {
  return true;
}

void emitDecimalSumExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* vector,
    llvm::Value* row,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    bool partialOutput) {
  auto& b = codegen.builder();
  const char* fn = partialOutput ? "jit_HashAggrExtractPartialDecimalSum"
                                 : "jit_HashAggrExtractFinalDecimalSum";
  auto* longDecimal =
      b.getInt8(slot.desc.inputKind == HashAggrJitValueKind::Int128 ? 1 : 0);
  b.CreateCall(
      codegen.module().getFunction(fn),
      {vector,
       row,
       group,
       b.getInt32(slot.offset),
       b.getInt32(slot.desc.precision),
       b.getInt32(slot.desc.scale),
       longDecimal});
}

void compileDecimalSumExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  emitDecimalSumExtract(
      codegen,
      target.output.vector(),
      target.row,
      group,
      slot,
      target.partialOutput);
}

} // namespace

const HashAggrJitOps* getDecimalSumOps() {
  static const HashAggrJitOps kOps{
      "sum_decimal",
      &compileDecimalSumInitGroup,
      &compileDecimalSumAddRawInput,
      &compileDecimalSumAddIntermediateResults,
      &canCompileDecimalSumExtract,
      &compileDecimalSumExtract};
  return &kOps;
}

void emitDecimalAddWithOverflow(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    int32_t sumOffset,
    int32_t overflowOffset,
    llvm::Value* addend) {
  auto& b = codegen.builder();
  auto* i128Ty = b.getInt128Ty();
  auto* i64Ty = b.getInt64Ty();
  auto* zero128 = llvm::ConstantInt::get(i128Ty, 0);

  auto* oldSum = codegen.loadValue(group, i128Ty, sumOffset);
  auto* newSum = b.CreateAdd(oldSum, addend);
  codegen.storeValue(group, i128Ty, sumOffset, newSum);

  // Mirror jitHashAggrAddWithOverflow:
  //   +1 if a>0 && b>0 && result<0   (positive overflow)
  //   -1 if a<0 && b<0 && result>=0  (negative overflow)
  auto* aPos = b.CreateICmpSGT(oldSum, zero128);
  auto* bPos = b.CreateICmpSGT(addend, zero128);
  auto* rNeg = b.CreateICmpSLT(newSum, zero128);
  auto* posOverflow = b.CreateAnd(b.CreateAnd(aPos, bPos), rNeg);

  auto* aNeg = b.CreateICmpSLT(oldSum, zero128);
  auto* bNeg = b.CreateICmpSLT(addend, zero128);
  auto* rNonNeg = b.CreateICmpSGE(newSum, zero128);
  auto* negOverflow = b.CreateAnd(b.CreateAnd(aNeg, bNeg), rNonNeg);

  auto* carry = b.CreateSub(
      b.CreateZExt(posOverflow, i64Ty), b.CreateZExt(negOverflow, i64Ty));
  auto* oldOverflow = codegen.loadValue(group, i64Ty, overflowOffset);
  codegen.storeValue(
      group, i64Ty, overflowOffset, b.CreateAdd(oldOverflow, carry));
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
