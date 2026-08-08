/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include <cstddef>

#include "bolt/jit/aggregation/HashAggrJit.h"
#include "bolt/jit/aggregation/HashAggrJitDecimalState.h"
#include "bolt/jit/aggregation/ops/DecimalOps.h"
#include "bolt/type/Type.h"

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
  auto* inputRow = input.read(row, slot.desc.rawInputKind);
  auto* rawValue = IRRow::getValue(codegen.builder(), inputRow);
  auto* value = codegen.castValue(
      rawValue, slot.desc.rawInputKind, HashAggrJitValueKind::Int128);
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
      "decimal_sum_merge_cont",
      function,
      nextBlock);
  auto* overflowBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "decimal_sum_merge_overflow",
      function,
      continueBlock);
  auto* mergeBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(), "decimal_sum_merge", function, continueBlock);
  const auto [sumPrecision, _] =
      hashAggrJitDecimalPrecisionScale(slot.desc.context.inputTypes()[0]);
  const auto sumKind = hashAggrJitDecimalKindForPrecision(sumPrecision);
  auto* sumRow = input.readRowField(row, 0, sumKind);
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
  auto* value = codegen.castValue(sum, sumKind, HashAggrJitValueKind::Int128);
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

void emitDecimalSumExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* vector,
    llvm::Value* row,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto& b = codegen.builder();
  const bool partialOutput = slot.desc.context.isPartialOutput;
  // long/short decimal and overflow precision are decided by the actual
  // output decimal type of this aggregation stage.
  const auto [outPrecision, outScale] =
      hashAggrJitDecimalPrecisionScale(slot.desc.context.outputType());
  const bool longDecimal = hashAggrJitDecimalKindForPrecision(outPrecision) ==
      HashAggrJitValueKind::Int128;
  const char* fn = partialOutput
      ? (longDecimal ? "jit_HashAggrExtractPartialLongDecimalSum"
                     : "jit_HashAggrExtractPartialShortDecimalSum")
      : (longDecimal ? "jit_HashAggrExtractFinalLongDecimalSum"
                     : "jit_HashAggrExtractFinalShortDecimalSum");
  // Mirror the non-JIT extract's leading `if (isNull(group))` check: a group
  // whose accumulator null flag is set (e.g. an overflowed intermediate result
  // merged in) must produce null, regardless of the sum/isEmpty fields.
  auto* accumulatorIsNull =
      b.CreateZExt(codegen.isAccumulatorNull(group, slot), b.getInt32Ty());
  b.CreateCall(
      codegen.module().getFunction(fn),
      {vector,
       row,
       group,
       b.getInt32(slot.offset),
       b.getInt32(outPrecision),
       b.getInt32(outScale),
       accumulatorIsNull});
}

void compileDecimalSumExtractAccumulators(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  emitDecimalSumExtract(
      codegen, target.output.vector(), target.row, group, slot);
}

void compileDecimalSumExtractValues(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  emitDecimalSumExtract(
      codegen, target.output.vector(), target.row, group, slot);
}

} // namespace

const HashAggrJitOps* getDecimalSumOps() {
  static const HashAggrJitOps kOps{
      &compileDecimalSumInitGroup,
      &compileDecimalSumAddRawInput,
      &compileDecimalSumAddIntermediateResults,
      &compileDecimalSumExtractAccumulators,
      &compileDecimalSumExtractValues};
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

  auto* lhs = codegen.loadValue(group, i128Ty, sumOffset);
  auto* rhs = addend;

  // Mirror DecimalUtil::addWithOverflow + addUnsignedValues exactly (i128 sum
  // kept as low 127 bits plus a separate overflow counter), instead of a
  // sign-flip heuristic which diverges once the true i128 magnitude overflows.
  //
  //   same sign:
  //     mag = (|lhs| + |rhs|) & ~(1<<127)        // low 127 bits
  //     carry = (|lhs| + |rhs|) >> 127           // bit 127
  //     both negative -> sum = -mag, overflow = -carry
  //     both positive -> sum =  mag, overflow =  carry
  //   different sign:
  //     sum = lhs + rhs, overflow = 0
  auto* lhsNeg = b.CreateICmpSLT(lhs, zero128);
  auto* rhsNeg = b.CreateICmpSLT(rhs, zero128);
  auto* sameSign = b.CreateICmpEQ(lhsNeg, rhsNeg);
  auto* bothNeg = b.CreateAnd(lhsNeg, rhsNeg);

  // Magnitudes for the same-sign path: negate operands when both negative so
  // the unsigned addition operates on |lhs|, |rhs| (matches addUnsignedValues).
  auto* absLhs = b.CreateSelect(bothNeg, b.CreateNeg(lhs), lhs);
  auto* absRhs = b.CreateSelect(bothNeg, b.CreateNeg(rhs), rhs);
  auto* unsignedSum = b.CreateAdd(absLhs, absRhs);
  auto* mask127 =
      llvm::ConstantInt::get(i128Ty, llvm::APInt::getSignedMinValue(128));
  // mag = unsignedSum & ~(1<<127)
  auto* magnitude = b.CreateAnd(unsignedSum, b.CreateNot(mask127));
  // carry = (unsignedSum >> 127) & 1, as i64
  auto* carryBit = b.CreateAnd(
      b.CreateLShr(unsignedSum, llvm::ConstantInt::get(i128Ty, 127)),
      llvm::ConstantInt::get(i128Ty, 1));
  auto* carry64 = b.CreateTrunc(carryBit, i64Ty);

  auto* sameSignSum = b.CreateSelect(bothNeg, b.CreateNeg(magnitude), magnitude);
  auto* sameSignOverflow =
      b.CreateSelect(bothNeg, b.CreateNeg(carry64), carry64);

  auto* diffSignSum = b.CreateAdd(lhs, rhs);

  auto* newSum = b.CreateSelect(sameSign, sameSignSum, diffSignSum);
  auto* overflowDelta =
      b.CreateSelect(sameSign, sameSignOverflow, b.getInt64(0));

  codegen.storeValue(group, i128Ty, sumOffset, newSum);
  auto* oldOverflow = codegen.loadValue(group, i64Ty, overflowOffset);
  codegen.storeValue(
      group, i64Ty, overflowOffset, b.CreateAdd(oldOverflow, overflowDelta));
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
