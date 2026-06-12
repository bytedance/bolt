/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include <cstddef>

#include "bolt/jit/aggregation/HashAggrJit.h"
#include "bolt/jit/aggregation/HashAggrJitDecimalState.h"

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
    bool,
    llvm::BasicBlock*) {
  auto& b = codegen.builder();
  auto* inputRow = input.read(row, slot.desc.inputKind);
  auto* rawValue = IRRow::getValue(codegen.builder(), inputRow);
  auto* value =
      codegen.castValue(rawValue, slot.desc.inputKind, HashAggrJitValueKind::Int128);
  codegen.clearAccumulatorNull(group, slot);
  codegen.emitDecimalAddWithOverflow(
      group, slot.offset + kSumOffset, slot.offset + kOverflowOffset, value);
  codegen.storeValue(
      group, b.getInt8Ty(), slot.offset + kIsEmptyOffset, b.getInt8(0));
}

void compileDecimalSumAddIntermediateResults(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
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
  auto* isEmptyRow = input.readRowField(row, 1, HashAggrJitValueKind::Bool);
  auto* sumIsNull = IRRow::getIsNull(b, sumRow);
  auto* incomingIsEmpty = IRRow::getValue(b, isEmptyRow);
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
  codegen.emitDecimalAddWithOverflow(
      group, slot.offset + kSumOffset, slot.offset + kOverflowOffset, value);
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

void compileDecimalSumExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  codegen.emitDecimalSumExtract(
      target.output.vector(), target.row, group, slot, target.partialOutput);
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

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
