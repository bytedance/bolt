/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

#include <type_traits>

#include "bolt/functions/lib/aggregates/SumCount.h"

namespace bytedance::bolt::jit {

namespace {

// Single source of truth for the AVG intermediate layout: derive the JIT field
// offsets from the non-JIT SumCount struct so a change to SumCount is picked up
// here automatically instead of silently desyncing a mirrored copy.
using AvgAccumulatorLayout = functions::aggregate::SumCount<double>;

static_assert(std::is_standard_layout_v<AvgAccumulatorLayout>);

constexpr int32_t kAvgCountOffset = offsetof(AvgAccumulatorLayout, count);

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
    llvm::BasicBlock*) {
  codegen.clearAccumulatorNull(group, slot);
  auto* sum = input.readRowFieldValue(row, 0, HashAggrJitValueKind::Double);
  auto* count = input.readRowFieldValue(row, 1, HashAggrJitValueKind::Int64);
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

// Intermediate output is row(sum:double, count:bigint). All-null group yields
// (0, 0) with a non-null top-level row (isNull = 0), matching the non-JIT
// extractAccumulators path.
void compileAvgExtractAccumulators(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto& builder = codegen.builder();
  auto* sum = codegen.loadValue(group, builder.getDoubleTy(), slot.offset);
  auto* count = codegen.loadValue(
      group, builder.getInt64Ty(), slot.offset + kAvgCountOffset);
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
}

// Final output is double avg. count == 0 means all inputs were null -> null.
void compileAvgExtractValues(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto& builder = codegen.builder();
  auto* sum = codegen.loadValue(group, builder.getDoubleTy(), slot.offset);
  auto* count = codegen.loadValue(
      group, builder.getInt64Ty(), slot.offset + kAvgCountOffset);
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
      &compileAvgExtractAccumulators,
      &compileAvgExtractValues};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
