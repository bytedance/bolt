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
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock*) {
  auto* rawValue = codegen.loadDecodedValue(decoded, row, slot);
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
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock*) {
  codegen.clearAccumulatorNull(group, slot);
  auto* sum =
      codegen.loadDecodedRowField(decoded, row, 0, HashAggrJitValueKind::Double);
  auto* count =
      codegen.loadDecodedRowField(decoded, row, 1, HashAggrJitValueKind::Int64);
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
    codegen.emitPartialAvgResult(
        target.resultVector, target.row, sum, count, builder.getInt8(0));
    return;
  }
  // Final output is double avg. count == 0 means all inputs were null -> null.
  auto* isNull = builder.CreateZExt(
      builder.CreateICmpEQ(count, builder.getInt64(0)), builder.getInt8Ty());
  auto* countAsDouble = builder.CreateSIToFP(count, builder.getDoubleTy());
  auto* avg = builder.CreateFDiv(sum, countAsDouble);
  codegen.emitFlatValue(
      target.resultVector,
      target.row,
      HashAggrJitValueKind::Double,
      avg,
      isNull);
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
