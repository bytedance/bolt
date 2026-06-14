/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

namespace bytedance::bolt::jit {

namespace {

void compileSumInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  codegen.setAccumulatorNull(group, slot);
  auto* accType = codegen.llvmType(slot.desc.accumulatorKind);
  if (codegen.isFloatKind(slot.desc.accumulatorKind)) {
    codegen.storeValue(
        group, accType, slot.offset, llvm::ConstantFP::get(accType, 0.0));
  } else {
    codegen.storeValue(
        group, accType, slot.offset, llvm::ConstantInt::get(accType, 0));
  }
}

// sum uses the same logic for raw input and intermediate merge: add the
// decoded value into the running accumulator.
void compileSumAccumulate(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock*) {
  auto* inputRow = input.read(row, slot.desc.rawInputKind);
  auto* rawValue = IRRow::getValue(codegen.builder(), inputRow);
  auto* value = codegen.castValue(
      rawValue, slot.desc.rawInputKind, slot.desc.accumulatorKind);
  auto* accType = codegen.llvmType(slot.desc.accumulatorKind);
  codegen.clearAccumulatorNull(group, slot);
  auto* oldValue = codegen.loadValue(group, accType, slot.offset);
  auto* newValue = codegen.isFloatKind(slot.desc.accumulatorKind)
      ? codegen.builder().CreateFAdd(oldValue, value)
      : codegen.builder().CreateAdd(oldValue, value);
  codegen.storeValue(group, accType, slot.offset, newValue);
}

// Sum's intermediate accumulator and final result share the same scalar
// representation, so partial/final extract emit identical IR. The two named
// entry points below both forward to this helper.
void compileSumExtract(
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

void compileSumExtractAccumulators(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  compileSumExtract(codegen, group, slot, target);
}

void compileSumExtractValues(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  compileSumExtract(codegen, group, slot, target);
}

} // namespace

const HashAggrJitOps* getSumOps() {
  static const HashAggrJitOps kOps{
      &compileSumInitGroup,
      &compileSumAccumulate,
      &compileSumAccumulate,
      &compileSumExtractAccumulators,
      &compileSumExtractValues};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
