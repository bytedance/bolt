/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

namespace bytedance::bolt::jit {

namespace {

void compileDecimalAvgInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  codegen.setAccumulatorNull(group, slot);
  codegen.builder().CreateCall(
      codegen.module().getFunction("jit_HashAggrInitDecimalAvg"),
      {group, codegen.builder().getInt32(slot.offset)});
}

void compileDecimalAvgAddRawInput(
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
      ? "jit_HashAggrUpdateDecimalAvgI128"
      : "jit_HashAggrUpdateDecimalAvgI64";
  codegen.builder().CreateCall(
      codegen.module().getFunction(helper),
      {group,
       codegen.builder().getInt32(slot.offset),
       slot.desc.inputKind == HashAggrJitValueKind::Int128
           ? codegen.castValue(
                 rawValue, slot.desc.inputKind, HashAggrJitValueKind::Int128)
           : rawValue});
}

void compileDecimalAvgAddIntermediateResults(
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
      "avg_decimal_merge_cont",
      function,
      nextBlock);
  auto* overflowBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "avg_decimal_merge_overflow",
      function,
      continueBlock);
  auto* mergeBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "avg_decimal_merge",
      function,
      continueBlock);
  auto* sumIsNull = codegen.isDecodedRowFieldNull(decoded, row, 0);
  auto* countIsNull = codegen.isDecodedRowFieldNull(decoded, row, 1);
  auto* count =
      codegen.loadDecodedRowField(decoded, row, 1, HashAggrJitValueKind::Int64);
  auto* countPositive =
      codegen.builder().CreateICmpSGT(count, codegen.builder().getInt64(0));
  auto* isOverflow = codegen.builder().CreateAnd(
      sumIsNull,
      codegen.builder().CreateAnd(
          codegen.builder().CreateNot(countIsNull), countPositive));
  codegen.builder().CreateCondBr(isOverflow, overflowBlock, mergeBlock);

  codegen.builder().SetInsertPoint(overflowBlock);
  codegen.setAccumulatorNull(group, slot);
  codegen.builder().CreateBr(continueBlock);

  codegen.builder().SetInsertPoint(mergeBlock);
  auto* sum = codegen.loadDecodedRowField(decoded, row, 0, slot.desc.inputKind);
  codegen.clearAccumulatorNull(group, slot);
  const auto helper = slot.desc.inputKind == HashAggrJitValueKind::Int128
      ? "jit_HashAggrMergeDecimalAvgI128"
      : "jit_HashAggrMergeDecimalAvgI64";
  codegen.builder().CreateCall(
      codegen.module().getFunction(helper),
      {group,
       codegen.builder().getInt32(slot.offset),
       slot.desc.inputKind == HashAggrJitValueKind::Int128
           ? codegen.castValue(
                 sum, slot.desc.inputKind, HashAggrJitValueKind::Int128)
           : sum,
       count});
  codegen.builder().CreateBr(continueBlock);

  codegen.builder().SetInsertPoint(continueBlock);
}

bool canCompileDecimalAvgExtract(const HashAggrJitSlot&, bool partialOutput) {
  // Only the partial (extractAccumulators) path is JIT-supported for decimal
  // avg. Final avg needs the full per-aggregate rescale logic and stays on
  // the non-JIT path.
  return partialOutput;
}

void compileDecimalAvgExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  codegen.emitDecimalAvgExtract(
      target.resultVector, target.row, group, slot, target.partialOutput);
}

} // namespace

const HashAggrJitOps* getDecimalAvgOps() {
  static const HashAggrJitOps kOps{
      "avg_decimal",
      &compileDecimalAvgInitGroup,
      &compileDecimalAvgAddRawInput,
      &compileDecimalAvgAddIntermediateResults,
      &canCompileDecimalAvgExtract,
      &compileDecimalAvgExtract};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
