/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

namespace bytedance::bolt::jit {

namespace {

void compileMinMaxInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  codegen.setAccumulatorNull(group, slot);
  auto* type = codegen.llvmType(slot.desc.accumulatorKind);
  if (codegen.isFloatKind(slot.desc.accumulatorKind)) {
    codegen.storeValue(group, type, slot.offset, llvm::ConstantFP::get(type, 0.0));
  } else {
    codegen.storeValue(group, type, slot.offset, llvm::ConstantInt::get(type, 0));
  }
}

// min/max use the same logic for raw input and intermediate merge: both pick
// the better (min/max) of the decoded value and the current accumulator.
void compileMinMaxUpdate(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock*) {
  auto* inputRow = input.read(row, slot.desc.inputKind);
  auto* value = codegen.castValue(
      IRRow::getValue(codegen.builder(), inputRow),
      slot.desc.inputKind,
      slot.desc.accumulatorKind);
  auto* type = codegen.llvmType(slot.desc.accumulatorKind);
  auto* oldValue = codegen.loadValue(group, type, slot.offset);
  auto* nullState = codegen.isAccumulatorNull(group, slot);
  llvm::Value* better = nullptr;
  if (codegen.isFloatKind(slot.desc.accumulatorKind)) {
    auto* oldIsNan = codegen.builder().CreateFCmpUNO(oldValue, oldValue);
    auto* valueIsNan = codegen.builder().CreateFCmpUNO(value, value);
    if (slot.desc.kind == HashAggrJitKind::Min) {
      better = codegen.builder().CreateOr(
          codegen.builder().CreateAnd(oldIsNan, codegen.builder().CreateNot(valueIsNan)),
          codegen.builder().CreateAnd(
              codegen.builder().CreateNot(valueIsNan),
              codegen.builder().CreateFCmpOGT(oldValue, value)));
    } else {
      better = codegen.builder().CreateAnd(
          codegen.builder().CreateNot(oldIsNan),
          codegen.builder().CreateOr(
              valueIsNan, codegen.builder().CreateFCmpOLT(oldValue, value)));
    }
  } else {
    better = slot.desc.kind == HashAggrJitKind::Min
        ? codegen.builder().CreateICmpSLT(value, oldValue)
        : codegen.builder().CreateICmpSGT(value, oldValue);
  }
  auto* shouldStore = codegen.builder().CreateOr(nullState, better);
  codegen.storeValue(
      group,
      type,
      slot.offset,
      codegen.builder().CreateSelect(shouldStore, value, oldValue));
  codegen.clearAccumulatorNull(group, slot);
}

// Min/max's intermediate accumulator and final result share the same scalar
// representation, so partial/final extract emit identical IR. The two named
// entry points below both forward to this helper.
void compileMinMaxExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto* value = codegen.loadValue(
      group, codegen.llvmType(slot.desc.accumulatorKind), slot.offset);
  auto* isNull = codegen.isAccumulatorNull(group, slot);
  target.output.write(
      target.row,
      slot.desc.accumulatorKind,
      IRRow::pack(codegen.builder(), value, isNull));
}

void compileMinMaxExtractAccumulators(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  compileMinMaxExtract(codegen, group, slot, target);
}

void compileMinMaxExtractValues(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  compileMinMaxExtract(codegen, group, slot, target);
}

} // namespace

const HashAggrJitOps* getMinMaxOps() {
  static const HashAggrJitOps kOps{
      &compileMinMaxInitGroup,
      &compileMinMaxUpdate,
      &compileMinMaxUpdate,
      &compileMinMaxExtractAccumulators,
      &compileMinMaxExtractValues};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
