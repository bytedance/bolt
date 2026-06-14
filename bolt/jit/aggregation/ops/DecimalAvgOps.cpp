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
    llvm::BasicBlock*) {
  auto& b = codegen.builder();
  auto* inputRow = input.read(row, slot.desc.rawInputKind);
  auto* rawValue = IRRow::getValue(codegen.builder(), inputRow);
  auto* value = codegen.castValue(
      rawValue, slot.desc.rawInputKind, HashAggrJitValueKind::Int128);
  codegen.clearAccumulatorNull(group, slot);
  emitDecimalAddWithOverflow(
      codegen, group, slot.offset + kSumOffset, slot.offset + kOverflowOffset, value);
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
    llvm::BasicBlock* nextBlock) {
  auto& b = codegen.builder();
  auto* function = b.GetInsertBlock()->getParent();
  auto* continueBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "decimal_avg_merge_cont",
      function,
      nextBlock);
  auto* overflowBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(),
      "decimal_avg_merge_overflow",
      function,
      continueBlock);
  auto* mergeBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(), "decimal_avg_merge", function, continueBlock);
  const auto [sumPrecision, _] =
      hashAggrJitDecimalPrecisionScale(slot.desc.context.inputTypes[0]);
  const auto sumKind = hashAggrJitDecimalKindForPrecision(sumPrecision);
  auto* sumRow = input.readRowField(row, 0, sumKind);
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
  auto* value = codegen.castValue(sum, sumKind, HashAggrJitValueKind::Int128);
  codegen.clearAccumulatorNull(group, slot);
  emitDecimalAddWithOverflow(
      codegen, group, slot.offset + kSumOffset, slot.offset + kOverflowOffset, value);
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

void emitDecimalAvgExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* vector,
    llvm::Value* row,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    bool partialOutput) {
  auto& b = codegen.builder();
  const auto [inputPrecision, inputScale] =
      hashAggrJitDecimalPrecisionScale(slot.desc.context.inputTypes[0]);
  const auto [outputPrecision, outputScale] =
      hashAggrJitDecimalPrecisionScale(slot.desc.context.outputType);
  const bool longDecimal = hashAggrJitDecimalKindForPrecision(
                               outputPrecision) == HashAggrJitValueKind::Int128;
  const char* fn = partialOutput
      ? (longDecimal ? "jit_HashAggrExtractPartialLongDecimalAvg"
                     : "jit_HashAggrExtractPartialShortDecimalAvg")
      : (longDecimal ? "jit_HashAggrExtractFinalLongDecimalAvg"
                     : "jit_HashAggrExtractFinalShortDecimalAvg");
  b.CreateCall(
      codegen.module().getFunction(fn),
      {vector,
       row,
       group,
       b.getInt32(slot.offset),
       b.getInt32(inputPrecision),
       b.getInt32(inputScale),
       b.getInt32(outputPrecision),
       b.getInt32(outputScale)});
}

void compileDecimalAvgExtractAccumulators(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  emitDecimalAvgExtract(
      codegen,
      target.output.vector(),
      target.row,
      group,
      slot,
      /*partialOutput=*/true);
}

void compileDecimalAvgExtractValues(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  emitDecimalAvgExtract(
      codegen,
      target.output.vector(),
      target.row,
      group,
      slot,
      /*partialOutput=*/false);
}

} // namespace

const HashAggrJitOps* getDecimalAvgOps() {
  static const HashAggrJitOps kOps{
      &compileDecimalAvgInitGroup,
      &compileDecimalAvgAddRawInput,
      &compileDecimalAvgAddIntermediateResults,
      &compileDecimalAvgExtractAccumulators,
      &compileDecimalAvgExtractValues};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
