/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/lib/CheckedArithmeticImpl.h"
#include "bolt/functions/lib/aggregates/AggregateToIntermediate.h"
#include "bolt/functions/lib/aggregates/DecimalAggregate.h"
#include "bolt/functions/lib/aggregates/SimpleNumericAggregate.h"
namespace bytedance::bolt::functions::aggregate {
inline bool Overflow = false;

static void setSumAggOverflowCheckFlag(bool flag) {
  Overflow = flag ? false : true;
}

template <typename TInput, typename TAccumulator, typename ResultType>
class SumAggregateBase
    : public SimpleNumericAggregate<TInput, TAccumulator, ResultType> {
  using BaseAggregate =
      SimpleNumericAggregate<TInput, TAccumulator, ResultType>;

 public:
  explicit SumAggregateBase(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return 1;
  }

  bool supportsToIntermediate() const override {
    return true;
  }

#ifdef ENABLE_BOLT_JIT
  bool supportsHashAggrJit(
      const jit::HashAggrJitPlanContext& context) const override {
    if (context.inputCount != 1 || !context.inputType) {
      return false;
    }
    if (context.inputType->isRow()) {
      return false;
    }
    return context.inputType->isDecimal() ||
        jit::isHashAggrJitSupportedType(context.inputType->kind()) ||
        context.inputType->kind() == TypeKind::HUGEINT;
  }

  std::optional<jit::HashAggrJitDescriptor> createHashAggrJitDescriptor(
      const jit::HashAggrJitPlanContext& context) const override {
    if (!supportsHashAggrJit(context)) {
      return std::nullopt;
    }

    const bool decimal = context.isRawInput && context.inputType->isDecimal();
    auto inputKind = jit::hashAggrJitValueKind(context.inputType->kind());
    if (!inputKind.has_value()) {
      return std::nullopt;
    }

    auto accumulatorKind = decimal
        ? jit::HashAggrJitValueKind::Int128
        : ((*inputKind == jit::HashAggrJitValueKind::Float ||
            *inputKind == jit::HashAggrJitValueKind::Double)
              ? jit::HashAggrJitValueKind::Double
              : jit::HashAggrJitValueKind::Int64);

    return jit::HashAggrJitDescriptor{
        jit::HashAggrJitKind::Sum,
        *inputKind,
        accumulatorKind,
        false,
        !context.isRawInput,
        decimal,
        hashAggrJitOps()};
  }

 private:
  static void compileHashAggrJitCreate(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      const jit::HashAggrJitSlot& slot) {
    if (slot.decimal) {
      codegen.setAccumulatorNull(group, slot);
      codegen.builder().CreateCall(
          codegen.module().getFunction(
              slot.kind == jit::HashAggrJitKind::Sum
                  ? "jit_HashAggrInitDecimalSum"
                  : "jit_HashAggrInitDecimalAvg"),
          {group, codegen.builder().getInt32(slot.offset)});
      return;
    }
    codegen.setAccumulatorNull(group, slot);
    auto* accType = codegen.llvmType(slot.accumulatorKind);
    if (codegen.isFloatKind(slot.accumulatorKind)) {
      codegen.storeValue(
          group, accType, slot.offset, llvm::ConstantFP::get(accType, 0.0));
    } else {
      codegen.storeValue(
          group, accType, slot.offset, llvm::ConstantInt::get(accType, 0));
    }
  }

  static void compileHashAggrJitAdd(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      llvm::Value* decoded,
      llvm::Value* row,
      const jit::HashAggrJitSlot& slot,
      bool,
      llvm::BasicBlock*) {
    auto* rawValue = codegen.loadDecodedValue(decoded, row, slot);
    if (slot.decimal) {
      codegen.clearAccumulatorNull(group, slot);
      const auto helper = slot.inputKind == jit::HashAggrJitValueKind::Int128
          ? "jit_HashAggrUpdateDecimalSumI128"
          : "jit_HashAggrUpdateDecimalSumI64";
      codegen.builder().CreateCall(
          codegen.module().getFunction(helper),
          {group,
           codegen.builder().getInt32(slot.offset),
           slot.inputKind == jit::HashAggrJitValueKind::Int128
               ? codegen.castValue(
                     rawValue,
                     slot.inputKind,
                     jit::HashAggrJitValueKind::Int128)
               : rawValue});
      return;
    }
    auto* value =
        codegen.castValue(rawValue, slot.inputKind, slot.accumulatorKind);
    auto* accType = codegen.llvmType(slot.accumulatorKind);
    codegen.clearAccumulatorNull(group, slot);
    auto* oldValue = codegen.loadValue(group, accType, slot.offset);
    auto* newValue = codegen.isFloatKind(slot.accumulatorKind)
        ? codegen.builder().CreateFAdd(oldValue, value)
        : codegen.builder().CreateAdd(oldValue, value);
    codegen.storeValue(group, accType, slot.offset, newValue);
  }

  static bool canCompileHashAggrJitExtract(
      const jit::HashAggrJitSlot& slot,
      bool) {
    // return !slot.decimal &&
    //     slot.accumulatorKind != jit::HashAggrJitValueKind::Int128;
    return false;
  }

  static void compileHashAggrJitExtract(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      const jit::HashAggrJitSlot& slot,
      const jit::HashAggrJitExtractTarget& target) {
    auto* value = codegen.loadValue(
        group, codegen.llvmType(slot.accumulatorKind), slot.offset);
    auto* isNull = codegen.builder().CreateZExt(
        codegen.isAccumulatorNull(group, slot), codegen.builder().getInt8Ty());
    codegen.emitFlatValue(
        target.resultVector, target.row, slot.accumulatorKind, value, isNull);
  }

  static const jit::HashAggrJitOps* hashAggrJitOps() {
    static const jit::HashAggrJitOps kOps{
        "sum",
        &compileHashAggrJitCreate,
        &compileHashAggrJitAdd,
        &canCompileHashAggrJitExtract,
        &compileHashAggrJitExtract};
    return &kOps;
  }

 public:
#endif

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    assignToIntermediate<TInput, ResultType>(rows, args, result);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<TAccumulator>(groups[i]) = 0;
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    BaseAggregate::template doExtractValues<ResultType>(
        groups, numGroups, result, [&](char* group) {
          // 'ResultType' and 'TAccumulator' might not be same such as sum(real)
          // and we do an explicit type conversion here.
          return (ResultType)(*BaseAggregate::Aggregate::template value<
                              TAccumulator>(group));
        });
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    BaseAggregate::template doExtractValues<TAccumulator>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<TAccumulator>(group);
        });
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateInternal<TAccumulator>(groups, rows, args, mayPushdown);
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateInternal<TAccumulator, TAccumulator>(groups, rows, args, mayPushdown);
  }

  FLATTEN void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateOneGroup<TAccumulator>(
        group,
        rows,
        args[0],
        &updateSingleValue<TAccumulator>,
        &updateDuplicateValues<TAccumulator>,
        mayPushdown,
        TAccumulator(0));
  }

  FLATTEN void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateOneGroup<TAccumulator, TAccumulator>(
        group,
        rows,
        args[0],
        &updateSingleValue<TAccumulator>,
        &updateDuplicateValues<TAccumulator>,
        mayPushdown,
        TAccumulator(0));
  }

 protected:
  // TData is used to store the updated sum state. It can be either
  // TAccumulator or TResult, which in most cases are the same, but for
  // sum(real) can differ. TValue is used to decode the sum input 'args'.
  // It can be either TAccumulator or TInput, which is most cases are the same
  // but for sum(real) can differ.
  template <typename TData, typename TValue = TInput>
  inline void updateInternal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) {
    const auto& arg = args[0];

    if (mayPushdown && arg->isLazy()) {
      if (Overflow) {
        BaseAggregate::template pushdown<
            bytedance::bolt::aggregate::SumHook<TValue, TData, true>>(
            groups, rows, arg);
      } else {
        BaseAggregate::template pushdown<
            bytedance::bolt::aggregate::SumHook<TValue, TData, false>>(
            groups, rows, arg);
      }
      return;
    }

    if (exec::Aggregate::numNulls_) {
      BaseAggregate::template updateGroups<true, TData, TValue>(
          groups, rows, arg, &updateSingleValue<TData>, false);
    } else {
      BaseAggregate::template updateGroups<false, TData, TValue>(
          groups, rows, arg, &updateSingleValue<TData>, false);
    }
  }

 private:
  /// Update functions that check for overflows for integer types.
  /// For floating points, an overflow results in +/- infinity which is a
  /// valid output.
  template <typename TData>
  static FLATTEN inline void updateSingleValue(TData& result, TData value) {
    if (Overflow || std::is_same_v<TData, double> ||
        std::is_same_v<TData, float>) {
      result += value;
    } else {
      CHECK_ADD(result, value);
    }
  }

  template <typename TData>
  static void updateDuplicateValues(TData& result, TData value, int n) {
    if (Overflow || std::is_same_v<TData, double> ||
        std::is_same_v<TData, float>) {
      result += n * value;
    } else {
      CHECK_MUL(value, n);
      CHECK_ADD(result, value);
    }
  }
};

template <typename TInputType>
class DecimalSumAggregate
    : public functions::aggregate::DecimalAggregate<int128_t, TInputType> {
 public:
  explicit DecimalSumAggregate(TypePtr resultType)
      : functions::aggregate::DecimalAggregate<int128_t, TInputType>(
            resultType) {}

  virtual int128_t computeFinalValue(
      functions::aggregate::LongDecimalWithOverflowState* accumulator) final {
    auto sum = DecimalUtil::adjustSumForOverflow(
        accumulator->sum, accumulator->overflow);
    BOLT_USER_CHECK(sum.has_value(), "Decimal overflow");
    DecimalUtil::valueInRange(sum.value());
    return sum.value();
  }
};

} // namespace bytedance::bolt::functions::aggregate
