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

#include "bolt/functions/sparksql/aggregates/SumAggregate.h"

#include "bolt/functions/lib/aggregates/SumAggregateBase.h"
#include "bolt/functions/sparksql/aggregates/DecimalSumAggregate.h"

#ifdef ENABLE_BOLT_JIT
#include "bolt/type/DecimalUtil.h"

namespace {
// Mirrors DecimalSumAggregate::computeFinalValue: applies overflow adjustment
// and reports whether the value overflows the result precision range.
bytedance::bolt::int128_t jitDecimalSumComputeFinal(
    const bytedance::bolt::jit::JitDecimalSumState* state,
    int32_t precision,
    bool& overflow) {
  using bytedance::bolt::DecimalUtil;
  bytedance::bolt::int128_t sum = state->sum;
  if ((state->overflow == 1 && state->sum < 0) ||
      (state->overflow == -1 && state->sum > 0)) {
    sum = static_cast<bytedance::bolt::int128_t>(
        DecimalUtil::kOverflowMultiplier * state->overflow + state->sum);
  } else if (state->overflow != 0) {
    overflow = true;
    return 0;
  }
  overflow = !DecimalUtil::valueInPrecisionRange(sum, precision);
  return sum;
}
} // namespace

extern "C" {

// Final decimal sum extract: write FlatVector<int128_t>. Null when the group is
// empty (all inputs null) or the sum overflows the result precision.
__attribute__((__visibility__("default"))) void
jit_HashAggrExtractFinalDecimalSum(
    char* vector,
    int32_t row,
    char* group,
    int32_t offset,
    int32_t precision,
    int32_t /*scale*/,
    int8_t /*longDecimal*/) {
  auto* state =
      reinterpret_cast<bytedance::bolt::jit::JitDecimalSumState*>(group + offset);
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<bytedance::bolt::int128_t>>();
  if (state->isEmpty) {
    flat->setNull(row, true);
    return;
  }
  bool overflow = false;
  auto result = jitDecimalSumComputeFinal(state, precision, overflow);
  if (overflow) {
    flat->setNull(row, true);
  } else {
    flat->set(row, result);
  }
}

// Partial decimal sum extract: write row(sum:decimal, isEmpty:bool).
__attribute__((__visibility__("default"))) void
jit_HashAggrExtractPartialDecimalSum(
    char* vector,
    int32_t row,
    char* group,
    int32_t offset,
    int32_t precision,
    int32_t /*scale*/,
    int8_t /*longDecimal*/) {
  auto* state =
      reinterpret_cast<bytedance::bolt::jit::JitDecimalSumState*>(group + offset);
  auto* rowVector =
      reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
          ->as<bytedance::bolt::RowVector>();
  auto* sumVector = rowVector->childAt(0)
                        ->asFlatVector<bytedance::bolt::int128_t>();
  auto* isEmptyVector = rowVector->childAt(1)->asFlatVector<bool>();
  rowVector->setNull(row, false);
  if (state->isEmpty) {
    sumVector->set(row, 0);
    isEmptyVector->set(row, true);
    return;
  }
  bool overflow = false;
  auto result = jitDecimalSumComputeFinal(state, precision, overflow);
  if (overflow) {
    sumVector->setNull(row, true);
    isEmptyVector->set(row, false);
  } else {
    sumVector->set(row, result);
    isEmptyVector->set(row, state->isEmpty);
  }
}

} // extern "C"
#endif

using namespace bytedance::bolt::functions::aggregate;
namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {
template <typename TInput, typename TAccumulator, typename ResultType>
class SumAggregate : public SumAggregateBase<TInput, TAccumulator, ResultType> {
 public:
  explicit SumAggregate(TypePtr resultType)
      : SumAggregateBase<TInput, TAccumulator, ResultType>(resultType) {}

#ifdef ENABLE_BOLT_JIT
  bool supportsHashAggrJit(
      const jit::HashAggrJitPlanContext& context) const override {
    if (context.inputCount != 1 || !context.inputType) {
      return false;
    }
    if (context.inputType->isRow() || context.inputType->isDecimal()) {
      return false;
    }
    return jit::isHashAggrJitSupportedType(context.inputType->kind()) ||
        context.inputType->kind() == TypeKind::HUGEINT;
  }

  std::optional<jit::HashAggrJitDescriptor> createHashAggrJitDescriptor(
      const jit::HashAggrJitPlanContext& context) const override {
    if (!supportsHashAggrJit(context)) {
      return std::nullopt;
    }

    auto inputKind = jit::hashAggrJitValueKind(context.inputType->kind());
    if (!inputKind.has_value()) {
      return std::nullopt;
    }

    const auto accumulatorKind =
        (*inputKind == jit::HashAggrJitValueKind::Float ||
         *inputKind == jit::HashAggrJitValueKind::Double)
        ? jit::HashAggrJitValueKind::Double
        : jit::HashAggrJitValueKind::Int64;

    return jit::HashAggrJitDescriptor{
        jit::HashAggrJitKind::Sum,
        *inputKind,
        accumulatorKind,
        false,
        !context.isRawInput,
        false,
        /*initSetsNull=*/true,
        /*precision=*/0,
        /*scale=*/0,
        /*auxPrecision=*/0,
        /*auxScale=*/0,
        hashAggrJitOps()};
  }

 private:
  static void compileHashAggrJitInitGroup(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      const jit::HashAggrJitSlot& slot) {
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

  // sum uses the same logic for raw input and intermediate merge: add the
  // decoded value into the running accumulator.
  static void compileHashAggrJitAccumulate(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      llvm::Value* decoded,
      llvm::Value* row,
      const jit::HashAggrJitSlot& slot,
      bool,
      llvm::BasicBlock*) {
    auto* rawValue = codegen.loadDecodedValue(decoded, row, slot);
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

  static void compileHashAggrJitAddRawInput(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      llvm::Value* decoded,
      llvm::Value* row,
      const jit::HashAggrJitSlot& slot,
      bool checkInputNulls,
      llvm::BasicBlock* nextBlock) {
    compileHashAggrJitAccumulate(
        codegen, group, decoded, row, slot, checkInputNulls, nextBlock);
  }

  static void compileHashAggrJitAddIntermediateResults(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      llvm::Value* decoded,
      llvm::Value* row,
      const jit::HashAggrJitSlot& slot,
      bool checkInputNulls,
      llvm::BasicBlock* nextBlock) {
    compileHashAggrJitAccumulate(
        codegen, group, decoded, row, slot, checkInputNulls, nextBlock);
  }

  static bool canCompileHashAggrJitExtract(
      const jit::HashAggrJitSlot& slot,
      bool) {
    // spark sum intermediate type == result type (bigint=bigint / double=double).
    return slot.accumulatorKind == jit::HashAggrJitValueKind::Int64 ||
        slot.accumulatorKind == jit::HashAggrJitValueKind::Double;
  }

  static void compileHashAggrJitExtract(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      const jit::HashAggrJitSlot& slot,
      const jit::HashAggrJitExtractTarget& target) {
    auto* value =
        codegen.loadValue(group, codegen.llvmType(slot.accumulatorKind), slot.offset);
    auto* isNull = codegen.builder().CreateZExt(
        codegen.isAccumulatorNull(group, slot), codegen.builder().getInt8Ty());
    codegen.emitFlatValue(
        target.resultVector, target.row, slot.accumulatorKind, value, isNull);
  }

  static const jit::HashAggrJitOps* hashAggrJitOps() {
    static const jit::HashAggrJitOps kOps{
        "sum",
        &compileHashAggrJitInitGroup,
        &compileHashAggrJitAddRawInput,
        &compileHashAggrJitAddIntermediateResults,
        &canCompileHashAggrJitExtract,
        &compileHashAggrJitExtract};
    return &kOps;
  }

 public:
#endif
};

TypePtr getDecimalSumType(
    const TypePtr& resultType,
    core::AggregationNode::Step step) {
  return exec::isPartialOutput(step) ? resultType->childAt(0) : resultType;
}
} // namespace

exec::AggregateRegistrationResult registerSum(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("double")
          .argumentType("real")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("double")
          .argumentType("double")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("r_precision", "min(38, a_precision + 10)")
          .integerVariable("r_scale", "min(38, a_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("ROW(DECIMAL(r_precision, r_scale), boolean)")
          .returnType("DECIMAL(r_precision, r_scale)")
          .build(),
  };

  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("bigint")
                             .intermediateType("bigint")
                             .argumentType(inputType)
                             .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<SumAggregate<int8_t, int64_t, int64_t>>(
                BIGINT());
          case TypeKind::SMALLINT:
            return std::make_unique<SumAggregate<int16_t, int64_t, int64_t>>(
                BIGINT());
          case TypeKind::INTEGER:
            return std::make_unique<SumAggregate<int32_t, int64_t, int64_t>>(
                BIGINT());
          case TypeKind::BIGINT: {
            if (inputType->isShortDecimal()) {
              auto sumType = getDecimalSumType(resultType, step);
              if (sumType->isShortDecimal()) {
                return std::make_unique<DecimalSumAggregate<int64_t, int64_t>>(
                    resultType, sumType);
              } else if (sumType->isLongDecimal()) {
                return std::make_unique<DecimalSumAggregate<int64_t, int128_t>>(
                    resultType, sumType);
              }
            }
            return std::make_unique<SumAggregate<int64_t, int64_t, int64_t>>(
                BIGINT());
          }
          case TypeKind::HUGEINT: {
            if (inputType->isLongDecimal()) {
              auto sumType = getDecimalSumType(resultType, step);
              // If inputType is long decimal,
              // its output type always be long decimal.
              return std::make_unique<DecimalSumAggregate<int128_t, int128_t>>(
                  resultType, sumType);
            }
          }
          case TypeKind::REAL:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<SumAggregate<float, double, float>>(
                  resultType);
            }
            return std::make_unique<SumAggregate<float, double, double>>(
                DOUBLE());
          case TypeKind::DOUBLE:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<SumAggregate<double, double, float>>(
                  resultType);
            }
            return std::make_unique<SumAggregate<double, double, double>>(
                DOUBLE());
          case TypeKind::ROW: {
            BOLT_DCHECK(!exec::isRawInput(step));
            auto sumType = getDecimalSumType(resultType, step);
            // For intermediate input agg, input intermediate sum type
            // is equal to final result sum type.
            if (inputType->childAt(0)->isShortDecimal()) {
              return std::make_unique<DecimalSumAggregate<int64_t, int64_t>>(
                  resultType, sumType);
            } else if (inputType->childAt(0)->isLongDecimal()) {
              return std::make_unique<DecimalSumAggregate<int128_t, int128_t>>(
                  resultType, sumType);
            }
          }
          default:
            BOLT_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
            return nullptr;
        }
      },
      withCompanionFunctions,
      overwrite);
}

void registerSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerSum(prefix, withCompanionFunctions, overwrite);
  bytedance::bolt::functions::aggregate::setSumAggOverflowCheckFlag(false);
}
} // namespace bytedance::bolt::functions::aggregate::sparksql
