/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  if (slot.desc.isCountStar()) {
    inc = codegen.builder().getInt64(1);
  } else {
    auto* inputRow = input.read(row, slot.desc.rawInputKind);
    inc = codegen.castValue(
        IRRow::getValue(codegen.builder(), inputRow),
        slot.desc.rawInputKind,
        HashAggrJitValueKind::Int64);
  }
  addInc(codegen, group, slot, inc);
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
      &compileCountInitGroup,
      &compileCountAddRawInput,
      &compileCountAddIntermediateResults,
      &compileCountExtractAccumulators,
      &compileCountExtractValues};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
