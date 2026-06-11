/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

namespace bytedance::bolt::jit {

namespace {

constexpr int32_t kAvgCountOffset = offsetof(JitAvgState, count);

void compileAvgInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  codegen.setAccumulatorNull(group, slot);
  codegen.storeValue(
      group,
      codegen.llvmType(slot.desc.accumulatorKind),
      slot.offset,
      llvm::ConstantFP::get(codegen.llvmType(slot.desc.accumulatorKind), 0.0));
  codegen.storeValue(
      group,
      codegen.builder().getInt64Ty(),
      slot.offset + kAvgCountOffset,
      codegen.builder().getInt64(0));
}

void compileAvgAddRawInput(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock*) {
  auto* inputRow = input.read(row, slot.desc.inputKind);
  auto* rawValue = IRRow::getValue(codegen.builder(), inputRow);
  auto* value =
      codegen.castValue(rawValue, slot.desc.inputKind, slot.desc.accumulatorKind);
  codegen.clearAccumulatorNull(group, slot);
  auto* oldSum = codegen.loadValue(
      group, codegen.llvmType(slot.desc.accumulatorKind), slot.offset);
  codegen.storeValue(
      group,
      codegen.llvmType(slot.desc.accumulatorKind),
      slot.offset,
      codegen.builder().CreateFAdd(oldSum, value));
  auto* oldCount = codegen.loadValue(
      group,
      codegen.builder().getInt64Ty(),
      slot.offset + kAvgCountOffset);
  codegen.storeValue(
      group,
      codegen.builder().getInt64Ty(),
      slot.offset + kAvgCountOffset,
      codegen.builder().CreateAdd(oldCount, codegen.builder().getInt64(1)));
}

void compileAvgAddIntermediateResults(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock*) {
  codegen.clearAccumulatorNull(group, slot);
  auto* sumRow = input.readRowField(row, 0, HashAggrJitValueKind::Double);
  auto* countRow = input.readRowField(row, 1, HashAggrJitValueKind::Int64);
  auto* sum = IRRow::getValue(codegen.builder(), sumRow);
  auto* count = IRRow::getValue(codegen.builder(), countRow);
  auto* oldSum =
      codegen.loadValue(group, codegen.builder().getDoubleTy(), slot.offset);
  codegen.storeValue(
      group,
      codegen.builder().getDoubleTy(),
      slot.offset,
      codegen.builder().CreateFAdd(oldSum, sum));
  auto* oldCount = codegen.loadValue(
      group,
      codegen.builder().getInt64Ty(),
      slot.offset + kAvgCountOffset);
  codegen.storeValue(
      group,
      codegen.builder().getInt64Ty(),
      slot.offset + kAvgCountOffset,
      codegen.builder().CreateAdd(oldCount, count));
}

bool canCompileAvgExtract(const HashAggrJitSlot& slot, bool) {
  // Only double avg (JitAvgState) is supported.
  return slot.desc.accumulatorKind == HashAggrJitValueKind::Double;
}

void compileAvgExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto& builder = codegen.builder();
  auto* sum = codegen.loadValue(group, builder.getDoubleTy(), slot.offset);
  auto* count = codegen.loadValue(
      group, builder.getInt64Ty(), slot.offset + kAvgCountOffset);
  if (target.partialOutput) {
    // Intermediate output is row(sum:double, count:bigint). All-null group
    // yields (0, 0) with a non-null top-level row (isNull = 0), matching the
    // non-JIT extractAccumulators path.
    target.output.writeField(
        target.row,
        0,
        HashAggrJitValueKind::Double,
        IRRow::pack(builder, sum, builder.getFalse()));
    target.output.writeField(
        target.row,
        1,
        HashAggrJitValueKind::Int64,
        IRRow::pack(builder, count, builder.getFalse()));
    target.output.writeNull(target.row, builder.getFalse());
    return;
  }
  // Final output is double avg. count == 0 means all inputs were null -> null.
  auto* isNull = builder.CreateICmpEQ(count, builder.getInt64(0));
  auto* countAsDouble = builder.CreateSIToFP(count, builder.getDoubleTy());
  auto* avg = builder.CreateFDiv(sum, countAsDouble);
  target.output.write(
      target.row,
      HashAggrJitValueKind::Double,
      IRRow::pack(builder, avg, isNull));
}

} // namespace

const HashAggrJitOps* getAvgOps() {
  static const HashAggrJitOps kOps{
      "avg",
      &compileAvgInitGroup,
      &compileAvgAddRawInput,
      &compileAvgAddIntermediateResults,
      &canCompileAvgExtract,
      &compileAvgExtract};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
