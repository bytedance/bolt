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

#include <cmath>
#include <type_traits>

#include <llvm/IR/Intrinsics.h>

#include "bolt/functions/prestosql/aggregates/VarianceAccumulator.h"

namespace bytedance::bolt::jit {

namespace {

using StddevPopAccumulatorLayout = aggregate::prestosql::VarianceAccumulator;

static_assert(std::is_standard_layout_v<StddevPopAccumulatorLayout>);

constexpr int32_t kCountOffset = StddevPopAccumulatorLayout::countOffset();
constexpr int32_t kMeanOffset = StddevPopAccumulatorLayout::meanOffset();
constexpr int32_t kM2Offset = StddevPopAccumulatorLayout::m2Offset();

void compileStddevPopInitGroup(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto& builder = codegen.builder();
  codegen.setAccumulatorNull(group, slot);
  codegen.storeValue(
      group,
      builder.getInt64Ty(),
      slot.offset + kCountOffset,
      builder.getInt64(0));
  codegen.storeValue(
      group,
      builder.getDoubleTy(),
      slot.offset + kMeanOffset,
      llvm::ConstantFP::get(builder.getDoubleTy(), 0.0));
  codegen.storeValue(
      group,
      builder.getDoubleTy(),
      slot.offset + kM2Offset,
      llvm::ConstantFP::get(builder.getDoubleTy(), 0.0));
}

void compileStddevPopAddRawInput(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock*) {
  auto& builder = codegen.builder();
  auto* inputRow = input.read(row, slot.desc.rawInputKind);
  auto* rawValue = IRRow::getValue(builder, inputRow);
  auto* value = codegen.castValue(
      rawValue, slot.desc.rawInputKind, HashAggrJitValueKind::Double);

  codegen.clearAccumulatorNullIfNeeded(group, slot);
  auto* oldCount = codegen.loadValue(
      group, builder.getInt64Ty(), slot.offset + kCountOffset);
  auto* oldMean = codegen.loadValue(
      group, builder.getDoubleTy(), slot.offset + kMeanOffset);
  auto* oldM2 =
      codegen.loadValue(group, builder.getDoubleTy(), slot.offset + kM2Offset);

  auto* newCount = builder.CreateAdd(oldCount, builder.getInt64(1));
  auto* delta = builder.CreateFSub(value, oldMean);
  auto* newMean = builder.CreateFAdd(
      oldMean,
      builder.CreateFDiv(
          delta, builder.CreateSIToFP(newCount, builder.getDoubleTy())));
  auto* newM2 = builder.CreateFAdd(
      oldM2, builder.CreateFMul(delta, builder.CreateFSub(value, newMean)));

  codegen.storeValue(
      group, builder.getInt64Ty(), slot.offset + kCountOffset, newCount);
  codegen.storeValue(
      group, builder.getDoubleTy(), slot.offset + kMeanOffset, newMean);
  codegen.storeValue(
      group, builder.getDoubleTy(), slot.offset + kM2Offset, newM2);
}

void compileStddevPopAddIntermediateResults(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const InputAdapterCodegen& input,
    llvm::Value* row,
    const HashAggrJitSlot& slot,
    llvm::BasicBlock*) {
  auto& builder = codegen.builder();
  auto* function = builder.GetInsertBlock()->getParent();

  auto* otherCount =
      input.readRowFieldValue(row, 0, HashAggrJitValueKind::Int64);
  auto* otherMean =
      input.readRowFieldValue(row, 1, HashAggrJitValueKind::Double);
  auto* otherM2 = input.readRowFieldValue(row, 2, HashAggrJitValueKind::Double);

  auto* nonEmptyBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(), "stddev_pop_merge_non_empty", function);
  auto* doneBlock = llvm::BasicBlock::Create(
      codegen.module().getContext(), "stddev_pop_merge_done", function);
  builder.CreateCondBr(
      builder.CreateICmpEQ(otherCount, builder.getInt64(0)),
      doneBlock,
      nonEmptyBlock);

  builder.SetInsertPoint(nonEmptyBlock);
  codegen.clearAccumulatorNull(group, slot);
  auto* oldCount = codegen.loadValue(
      group, builder.getInt64Ty(), slot.offset + kCountOffset);
  auto* oldMean = codegen.loadValue(
      group, builder.getDoubleTy(), slot.offset + kMeanOffset);
  auto* oldM2 =
      codegen.loadValue(group, builder.getDoubleTy(), slot.offset + kM2Offset);

  auto* totalCount = builder.CreateAdd(oldCount, otherCount);
  auto* oldEmpty = builder.CreateICmpEQ(oldCount, builder.getInt64(0));
  auto* delta = builder.CreateFSub(otherMean, oldMean);
  auto* oldCountDouble = builder.CreateSIToFP(oldCount, builder.getDoubleTy());
  auto* otherCountDouble =
      builder.CreateSIToFP(otherCount, builder.getDoubleTy());
  auto* totalCountDouble =
      builder.CreateSIToFP(totalCount, builder.getDoubleTy());

  auto* mergedMean = builder.CreateFAdd(
      oldMean,
      builder.CreateFMul(
          builder.CreateFDiv(delta, totalCountDouble), otherCountDouble));
  auto* mergedM2 = builder.CreateFAdd(
      oldM2,
      builder.CreateFAdd(
          otherM2,
          builder.CreateFDiv(
              builder.CreateFMul(
                  builder.CreateFMul(delta, delta),
                  builder.CreateFMul(otherCountDouble, oldCountDouble)),
              totalCountDouble)));

  codegen.storeValue(
      group, builder.getInt64Ty(), slot.offset + kCountOffset, totalCount);
  codegen.storeValue(
      group,
      builder.getDoubleTy(),
      slot.offset + kMeanOffset,
      builder.CreateSelect(oldEmpty, otherMean, mergedMean));
  codegen.storeValue(
      group,
      builder.getDoubleTy(),
      slot.offset + kM2Offset,
      builder.CreateSelect(oldEmpty, otherM2, mergedM2));
  builder.CreateBr(doneBlock);

  builder.SetInsertPoint(doneBlock);
}

void compileStddevPopExtractAccumulators(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto& builder = codegen.builder();
  auto* isNull = codegen.isAccumulatorNull(group, slot);
  auto* count = codegen.loadValue(
      group, builder.getInt64Ty(), slot.offset + kCountOffset);
  auto* mean = codegen.loadValue(
      group, builder.getDoubleTy(), slot.offset + kMeanOffset);
  auto* m2 =
      codegen.loadValue(group, builder.getDoubleTy(), slot.offset + kM2Offset);

  target.output.writeField(
      target.row,
      0,
      HashAggrJitValueKind::Int64,
      IRRow::pack(builder, count, isNull));
  target.output.writeField(
      target.row,
      1,
      HashAggrJitValueKind::Double,
      IRRow::pack(builder, mean, isNull));
  target.output.writeField(
      target.row,
      2,
      HashAggrJitValueKind::Double,
      IRRow::pack(builder, m2, isNull));
  target.output.writeNull(target.row, isNull);
}

void compileStddevPopExtractValues(
    HashAggrJitCodegen& codegen,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    const HashAggrJitExtractTarget& target) {
  auto& builder = codegen.builder();
  auto* count = codegen.loadValue(
      group, builder.getInt64Ty(), slot.offset + kCountOffset);
  auto* m2 =
      codegen.loadValue(group, builder.getDoubleTy(), slot.offset + kM2Offset);
  auto* isNull = builder.CreateICmpEQ(count, builder.getInt64(0));
  auto* variance = builder.CreateFDiv(
      m2, builder.CreateSIToFP(count, builder.getDoubleTy()));
  auto* stddev = builder.CreateCall(
      llvm::Intrinsic::getDeclaration(
          &codegen.module(), llvm::Intrinsic::sqrt, {builder.getDoubleTy()}),
      {variance});
  target.output.write(
      target.row,
      HashAggrJitValueKind::Double,
      IRRow::pack(builder, stddev, isNull));
}

} // namespace

const HashAggrJitOps* getStddevPopOps() {
  static const HashAggrJitOps kOps{
      &compileStddevPopInitGroup,
      &compileStddevPopAddRawInput,
      &compileStddevPopAddIntermediateResults,
      &compileStddevPopExtractAccumulators,
      &compileStddevPopExtractValues};
  return &kOps;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
