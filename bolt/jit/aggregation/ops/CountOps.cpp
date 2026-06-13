/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

namespace bytedance::bolt::jit {

namespace {

void compileCountInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  codegen.storeValue(
      group,
      codegen.builder().getInt64Ty(),
      slot.offset,
      codegen.builder().getInt64(0));
}

void addInc(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    llvm::Value* inc) {
  auto* state =
      codegen.loadValue(group, codegen.builder().getInt64Ty(), slot.offset);
  codegen.storeValue(
      group,
      codegen.builder().getInt64Ty(),
      slot.offset,
      codegen.builder().CreateAdd(state, inc));
}

void compileCountAddRawInput(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& /*input*/,
    llvm::Value* /*row*/,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock*) {
  addInc(codegen, group, slot, codegen.builder().getInt64(1));
}

void compileCountAddIntermediateResults(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock*) {
  llvm::Value* inc = nullptr;
  if (slot.desc.countStar) {
    inc = codegen.builder().getInt64(1);
  } else {
    auto* inputRow = input.read(row, slot.desc.inputKind);
    inc = codegen.castValue(
        IRRow::getValue(codegen.builder(), inputRow),
        slot.desc.inputKind,
        HashAggrJitValueKind::Int64);
  }
  addInc(codegen, group, slot, inc);
}

bool canCompileCountExtract(const HashAggrJitSlot&, bool) {
  // count result is always BIGINT and never null.
  return true;
}

// Count's intermediate accumulator and final result share the same scalar
// representation, so partial/final extract emit identical IR. The two named
// entry points below both forward to this helper.
void compileCountExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto* value =
      codegen.loadValue(group, codegen.builder().getInt64Ty(), slot.offset);
  target.output.write(
      target.row,
      HashAggrJitValueKind::Int64,
      IRRow::pack(codegen.builder(), value, codegen.builder().getFalse()));
}

void compileCountExtractAccumulators(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  compileCountExtract(codegen, group, slot, target);
}

void compileCountExtractValues(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  compileCountExtract(codegen, group, slot, target);
}

} // namespace

const HashAggrJitOps* getCountOps() {
  static const HashAggrJitOps kOps{
      "count",
      &compileCountInitGroup,
      &compileCountAddRawInput,
      &compileCountAddIntermediateResults,
      &canCompileCountExtract,
      &compileCountExtractAccumulators,
      &compileCountExtractValues};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
