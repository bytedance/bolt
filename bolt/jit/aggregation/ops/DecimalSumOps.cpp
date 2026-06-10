/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

namespace bytedance::bolt::jit {

namespace {

void compileDecimalSumInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  codegen.setAccumulatorNull(group, slot);
  codegen.builder().CreateCall(
      codegen.module().getFunction("jit_HashAggrInitDecimalSum"),
      {group, codegen.builder().getInt32(slot.offset)});
}

void compileDecimalSumAddRawInput(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock*) {
  auto* rawValue = codegen.loadDecodedValue(decoded, row, slot);
  codegen.clearAccumulatorNull(group, slot);
  const auto helper = slot.desc.inputKind == HashAggrJitValueKind::Int128
      ? "jit_HashAggrUpdateDecimalSumI128"
      : "jit_HashAggrUpdateDecimalSumI64";
  codegen.builder().CreateCall(
      codegen.module().getFunction(helper),
      {group,
       codegen.builder().getInt32(slot.offset),
       slot.desc.inputKind == HashAggrJitValueKind::Int128
           ? codegen.castValue(
                 rawValue, slot.desc.inputKind, HashAggrJitValueKind::Int128)
           : rawValue});
}

void compileDecimalSumAddIntermediateResults(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock* nextBlock) {
  auto* function = codegen.builder().GetInsertBlock()->getParent();
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
      codegen.module().getContext(),
      "sum_decimal_merge",
      function,
      continueBlock);
  auto* sumIsNull = codegen.isDecodedRowFieldNull(decoded, row, 0);
  auto* isEmpty =
      codegen.loadDecodedRowField(decoded, row, 1, HashAggrJitValueKind::Int8);
  auto* isNotEmpty =
      codegen.builder().CreateICmpEQ(isEmpty, codegen.builder().getInt8(0));
  auto* isOverflow = codegen.builder().CreateAnd(sumIsNull, isNotEmpty);
  codegen.builder().CreateCondBr(isOverflow, overflowBlock, mergeBlock);

  codegen.builder().SetInsertPoint(overflowBlock);
  codegen.setAccumulatorNull(group, slot);
  codegen.builder().CreateBr(continueBlock);

  codegen.builder().SetInsertPoint(mergeBlock);
  auto* sum = codegen.loadDecodedRowField(decoded, row, 0, slot.desc.inputKind);
  codegen.clearAccumulatorNull(group, slot);
  const auto helper = slot.desc.inputKind == HashAggrJitValueKind::Int128
      ? "jit_HashAggrMergeDecimalSumI128"
      : "jit_HashAggrMergeDecimalSumI64";
  codegen.builder().CreateCall(
      codegen.module().getFunction(helper),
      {group,
       codegen.builder().getInt32(slot.offset),
       slot.desc.inputKind == HashAggrJitValueKind::Int128
           ? codegen.castValue(
                 sum, slot.desc.inputKind, HashAggrJitValueKind::Int128)
           : sum,
       isEmpty});
  codegen.builder().CreateBr(continueBlock);

  codegen.builder().SetInsertPoint(continueBlock);
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
      target.resultVector, target.row, group, slot, target.partialOutput);
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
