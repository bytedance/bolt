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
    llvm::Value* /*decoded*/,
    llvm::Value* /*row*/,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock*) {
  addInc(codegen, group, slot, codegen.builder().getInt64(1));
}

void compileCountAddIntermediateResults(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    bool,
    llvm::BasicBlock*) {
  llvm::Value* inc = slot.desc.countStar
      ? codegen.builder().getInt64(1)
      : codegen.castValue(
            codegen.loadDecodedValue(decoded, row, slot),
            slot.desc.inputKind,
            HashAggrJitValueKind::Int64);
  addInc(codegen, group, slot, inc);
}

bool canCompileCountExtract(const HashAggrJitSlot&, bool) {
  // count result is always BIGINT and never null.
  return true;
}

void compileCountExtract(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto* value =
      codegen.loadValue(group, codegen.builder().getInt64Ty(), slot.offset);
  codegen.emitFlatValue(
      target.resultVector,
      target.row,
      HashAggrJitValueKind::Int64,
      value,
      codegen.builder().getInt8(0));
}

} // namespace

const HashAggrJitOps* getCountOps() {
  static const HashAggrJitOps kOps{
      "count",
      &compileCountInitGroup,
      &compileCountAddRawInput,
      &compileCountAddIntermediateResults,
      &canCompileCountExtract,
      &compileCountExtract};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
