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

// Field offsets within JitDecimalAvgState, relative to slot.offset.
constexpr int32_t kSumOffset =
    static_cast<int32_t>(offsetof(JitDecimalAvgState, sum));
constexpr int32_t kCountOffset =
    static_cast<int32_t>(offsetof(JitDecimalAvgState, count));
constexpr int32_t kOverflowOffset =
    static_cast<int32_t>(offsetof(JitDecimalAvgState, overflow));

void compileDecimalAvgInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto& b = codegen.builder();
  codegen.setAccumulatorNull(group, slot);
  // sum = 0 (i128), count = 0 (i64), overflow = 0 (i64).
  codegen.storeValue(
      group,
      b.getInt128Ty(),
      slot.offset + kSumOffset,
      llvm::ConstantInt::get(b.getInt128Ty(), 0));
  codegen.storeValue(
      group, b.getInt64Ty(), slot.offset + kCountOffset, b.getInt64(0));
  codegen.storeValue(
      group, b.getInt64Ty(), slot.offset + kOverflowOffset, b.getInt64(0));
}

void compileDecimalAvgAddRawInput(
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
  // ++count.
  auto* oldCount =
      codegen.loadValue(group, b.getInt64Ty(), slot.offset + kCountOffset);
  codegen.storeValue(
      group,
      b.getInt64Ty(),
      slot.offset + kCountOffset,
      b.CreateAdd(oldCount, b.getInt64(1)));
}

void compileDecimalAvgAddIntermediateResults(
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
      "avg_decimal_merge_cont",
      function,
      nextBlock);
  auto* overflowBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "avg_decimal_merge_overflow",
      function,
      continueBlock);
  auto* mergeBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(), "avg_decimal_merge", function, continueBlock);
  auto* sumRow = input.readRowField(row, 0, slot.desc.inputKind);
  auto* countRow = input.readRowField(row, 1, HashAggrJitValueKind::Int64);
  auto* sumIsNull = IRRow::getIsNull(b, sumRow);
  auto* countIsNull = IRRow::getIsNull(b, countRow);
  auto* count = IRRow::getValue(b, countRow);
  auto* countPositive = b.CreateICmpSGT(count, b.getInt64(0));
  auto* isOverflow = b.CreateAnd(
      sumIsNull, b.CreateAnd(b.CreateNot(countIsNull), countPositive));
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
  // count += incoming count.
  auto* oldCount =
      codegen.loadValue(group, b.getInt64Ty(), slot.offset + kCountOffset);
  codegen.storeValue(
      group,
      b.getInt64Ty(),
      slot.offset + kCountOffset,
      b.CreateAdd(oldCount, count));
  b.CreateBr(continueBlock);

  b.SetInsertPoint(continueBlock);
}

bool canCompileDecimalAvgExtract(const HashAggrJitSlot&, bool partialOutput) {
  // Both partial (extractAccumulators) and final extract go through runtime
  // helpers. Final decimal avg keeps the divide/rescale logic in the helper to
  // avoid duplicating Spark decimal semantics in LLVM IR.
  return true;
}

void compileDecimalAvgExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  codegen.emitDecimalAvgExtract(
      target.output.vector(), target.row, group, slot, target.partialOutput);
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
