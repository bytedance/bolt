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
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::functions::aggregate::sparksql {

struct DecimalSum {
  int128_t sum{0};
  int64_t overflow{0};
  bool isEmpty{true};

  void mergeWith(const DecimalSum& other) {
    this->overflow += other.overflow;
    this->overflow +=
        DecimalUtil::addWithOverflow(this->sum, other.sum, this->sum);
    this->isEmpty &= other.isEmpty;
  }
};

template <typename TInputType, typename TResultType>
class DecimalSumAggregate : public exec::Aggregate {
 public:
  explicit DecimalSumAggregate(TypePtr resultType, TypePtr sumType)
      : exec::Aggregate(resultType), sumType_(sumType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(DecimalSum);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(DecimalSum);
  }

#ifdef ENABLE_BOLT_JIT
  bool supportsHashAggrJit(
      const jit::HashAggrJitPlanContext& context) const override {
    if (context.inputCount != 1 || !context.inputType) {
      return false;
    }
    if (context.isRawInput) {
      return context.inputType->isDecimal() &&
          (context.inputType->isShortDecimal() ||
           context.inputType->isLongDecimal());
    }
    return context.inputType->isRow() && context.inputType->size() == 2 &&
        context.inputType->childAt(0)->isDecimal() &&
        context.inputType->childAt(1)->kind() == TypeKind::BOOLEAN;
  }

  std::optional<jit::HashAggrJitDescriptor> createHashAggrJitDescriptor(
      const jit::HashAggrJitPlanContext& context) const override {
    if (!supportsHashAggrJit(context)) {
      return std::nullopt;
    }
    const auto& valueType =
        context.isRawInput ? context.inputType : context.inputType->childAt(0);
    const auto [resultPrecision, resultScale] =
        getDecimalPrecisionScale(*sumType_.get());
    return jit::HashAggrJitDescriptor{
        jit::HashAggrJitKind::Sum,
        valueType->isShortDecimal() ? jit::HashAggrJitValueKind::Int64
                                    : jit::HashAggrJitValueKind::Int128,
        jit::HashAggrJitValueKind::Int128,
        false,
        !context.isRawInput,
        true,
        /*precision=*/resultPrecision,
        /*scale=*/resultScale,
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
    codegen.builder().CreateCall(
        codegen.module().getFunction("jit_HashAggrInitDecimalSum"),
        {group, codegen.builder().getInt32(slot.offset)});
  }

  static void compileHashAggrJitAddRawInput(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      llvm::Value* decoded,
      llvm::Value* row,
      const jit::HashAggrJitSlot& slot,
      bool,
      llvm::BasicBlock*) {
    auto* rawValue = codegen.loadDecodedValue(decoded, row, slot);
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
                   rawValue, slot.inputKind, jit::HashAggrJitValueKind::Int128)
             : rawValue});
  }

  static void compileHashAggrJitAddIntermediateResults(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      llvm::Value* decoded,
      llvm::Value* row,
      const jit::HashAggrJitSlot& slot,
      bool,
      llvm::BasicBlock* nextBlock) {
    auto* function = codegen.builder().GetInsertBlock()->getParent();
    auto* continueBlock = llvm::BasicBlock::Create(
        codegen.module().getContext(),
        "sum_decimal_merge_cont",
        function,
        nextBlock);
    auto* overflowBlock = llvm::BasicBlock::Create(
        codegen.module().getContext(),
        "sum_decimal_merge_overflow",
        function,
        continueBlock);
    auto* mergeBlock = llvm::BasicBlock::Create(
        codegen.module().getContext(),
        "sum_decimal_merge",
        function,
        continueBlock);
    auto* sumIsNull = codegen.isDecodedRowFieldNull(decoded, row, 0);
    auto* isEmpty = codegen.loadDecodedRowField(
        decoded, row, 1, jit::HashAggrJitValueKind::Int8);
    auto* isNotEmpty =
        codegen.builder().CreateICmpEQ(isEmpty, codegen.builder().getInt8(0));
    auto* isOverflow = codegen.builder().CreateAnd(sumIsNull, isNotEmpty);
    codegen.builder().CreateCondBr(isOverflow, overflowBlock, mergeBlock);

    codegen.builder().SetInsertPoint(overflowBlock);
    codegen.setAccumulatorNull(group, slot);
    codegen.builder().CreateBr(continueBlock);

    codegen.builder().SetInsertPoint(mergeBlock);
    auto* sum = codegen.loadDecodedRowField(decoded, row, 0, slot.inputKind);
    codegen.clearAccumulatorNull(group, slot);
    const auto helper = slot.inputKind == jit::HashAggrJitValueKind::Int128
        ? "jit_HashAggrMergeDecimalSumI128"
        : "jit_HashAggrMergeDecimalSumI64";
    codegen.builder().CreateCall(
        codegen.module().getFunction(helper),
        {group,
         codegen.builder().getInt32(slot.offset),
         slot.inputKind == jit::HashAggrJitValueKind::Int128
             ? codegen.castValue(
                   sum, slot.inputKind, jit::HashAggrJitValueKind::Int128)
             : sum,
         isEmpty});
    codegen.builder().CreateBr(continueBlock);

    codegen.builder().SetInsertPoint(continueBlock);
  }

  static bool canCompileHashAggrJitExtract(
      const jit::HashAggrJitSlot&,
      bool) {
    return true;
  }

  static void compileHashAggrJitExtract(
      jit::HashAggrJitCodegen& codegen,
      llvm::Value* group,
      const jit::HashAggrJitSlot& slot,
      const jit::HashAggrJitExtractTarget& target) {
    codegen.emitDecimalSumExtract(
        target.resultVector, target.row, group, slot, target.partialOutput);
  }

  static const jit::HashAggrJitOps* hashAggrJitOps() {
    static const jit::HashAggrJitOps kOps{
        "sum_decimal",
        &compileHashAggrJitInitGroup,
        &compileHashAggrJitAddRawInput,
        &compileHashAggrJitAddIntermediateResults,
        &canCompileHashAggrJitExtract,
        &compileHashAggrJitExtract};
    return &kOps;
  }

 public:
#endif

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) DecimalSum();
    }
  }

  int128_t computeFinalValue(DecimalSum* decimalSum, bool& overflow) {
    int128_t sum = decimalSum->sum;
    if ((decimalSum->overflow == 1 && decimalSum->sum < 0) ||
        (decimalSum->overflow == -1 && decimalSum->sum > 0)) {
      sum = static_cast<int128_t>(
          DecimalUtil::kOverflowMultiplier * decimalSum->overflow +
          decimalSum->sum);
    } else {
      if (decimalSum->overflow != 0) {
        overflow = true;
        return 0;
      }
    }

    auto [resultPrecision, resultScale] =
        getDecimalPrecisionScale(*sumType_.get());
    overflow = !DecimalUtil::valueInPrecisionRange(sum, resultPrecision);
    return sum;
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    BOLT_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::FLAT);
    auto vector = (*result)->as<FlatVector<TResultType>>();
    BOLT_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    TResultType* rawValues = vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto* decimalSum = accumulator(group);
        if (decimalSum->isEmpty) {
          // If isEmpty is true, we should set null.
          vector->setNull(i, true);
        } else {
          bool overflow = false;
          auto result = (TResultType)computeFinalValue(decimalSum, overflow);
          if (overflow) {
            // Sum should be set to null on overflow.
            vector->setNull(i, true);
          } else {
            rawValues[i] = result;
          }
        }
      }
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      bytedance::bolt::VectorPtr* result) override {
    BOLT_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::ROW);
    auto rowVector = (*result)->as<RowVector>();
    auto sumVector = rowVector->childAt(0)->asFlatVector<TResultType>();
    auto isEmptyVector = rowVector->childAt(1)->asFlatVector<bool>();

    rowVector->resize(numGroups);
    sumVector->resize(numGroups);
    isEmptyVector->resize(numGroups);

    TResultType* rawSums = sumVector->mutableRawValues();
    // Bool uses compact representation, use mutableRawValues<uint64_t>
    // and bits::setBit instead.
    auto* rawIsEmpty = isEmptyVector->mutableRawValues<uint64_t>();
    uint64_t* rawNulls = getRawNulls(rowVector);

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      clearNull(rawNulls, i);
      if (isNull(group)) {
        rawSums[i] = 0;
        bits::setBit(rawIsEmpty, i, true);
      } else {
        auto* decimalSum = accumulator(group);
        bool overflow = false;
        auto result = (TResultType)computeFinalValue(decimalSum, overflow);
        if (overflow) {
          // Sum should be set to null on overflow, and
          // isEmpty should be set to false.
          sumVector->setNull(i, true);
          bits::setBit(rawIsEmpty, i, false);
        } else {
          rawSums[i] = result;
          bits::setBit(rawIsEmpty, i, decimalSum->isEmpty);
        }
      }
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const bytedance::bolt::SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<TInputType>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], value, false);
        });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedRaw_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedRaw_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedRaw_.size(),
                rows.end()));
        nulls = decodedRaw_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            updateNonNullValue(
                groups[i], decodedRaw_.valueAt<TInputType>(i), false);
          },
          nulls);
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<TInputType>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<false>(groups[i], data[i], false);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue(
            groups[i], decodedRaw_.valueAt<TInputType>(i), false);
      });
    }
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<TInputType>(0);
        rows.applyToSelected(
            [&](vector_size_t i) { updateNonNullValue(group, value, false); });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedRaw_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedRaw_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedRaw_.size(),
                rows.end()));
        nulls = decodedRaw_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            updateNonNullValue(
                group, decodedRaw_.valueAt<TInputType>(i), false);
          },
          nulls);
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<TInputType>();
      DecimalSum decimalSum;
      rows.applyToSelected([&](vector_size_t i) {
        decimalSum.overflow += DecimalUtil::addWithOverflow(
            decimalSum.sum, data[i], decimalSum.sum);
        decimalSum.isEmpty = false;
      });
      mergeAccumulators(group, decimalSum);
    } else {
      DecimalSum decimalSum;
      rows.applyToSelected([&](vector_size_t i) {
        decimalSum.overflow += DecimalUtil::addWithOverflow(
            decimalSum.sum, decodedRaw_.valueAt<TInputType>(i), decimalSum.sum);
        decimalSum.isEmpty = false;
      });
      mergeAccumulators(group, decimalSum);
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    BOLT_CHECK_EQ(
        decodedPartial_.base()->encoding(), VectorEncoding::Simple::ROW);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto sumVector = baseRowVector->childAt(0)->as<SimpleVector<TResultType>>();
    auto isEmptyVector = baseRowVector->childAt(1)->as<SimpleVector<bool>>();

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        if (isIntermediateResultOverflow(
                isEmptyVector, sumVector, decodedIndex)) {
          rows.applyToSelected([&](vector_size_t i) { setNull(groups[i]); });
        } else {
          auto sum = sumVector->valueAt(decodedIndex);
          auto isEmpty = isEmptyVector->valueAt(decodedIndex);
          rows.applyToSelected([&](vector_size_t i) {
            clearNull(groups[i]);
            updateNonNullValue(groups[i], sum, isEmpty);
          });
        }
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedPartial_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedPartial_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedPartial_.size(),
                rows.end()));
        nulls = decodedPartial_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            auto decodedIndex = decodedPartial_.index(i);
            if (isIntermediateResultOverflow(
                    isEmptyVector, sumVector, decodedIndex)) {
              setNull(groups[i]);
            } else {
              auto sum = sumVector->valueAt(decodedIndex);
              auto isEmpty = isEmptyVector->valueAt(decodedIndex);
              updateNonNullValue(groups[i], sum, isEmpty);
            }
          },
          nulls);
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        clearNull(groups[i]);
        auto decodedIndex = decodedPartial_.index(i);
        if (isIntermediateResultOverflow(
                isEmptyVector, sumVector, decodedIndex)) {
          setNull(groups[i]);
        } else {
          auto sum = sumVector->valueAt(decodedIndex);
          auto isEmpty = isEmptyVector->valueAt(decodedIndex);
          updateNonNullValue(groups[i], sum, isEmpty);
        }
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    BOLT_CHECK_EQ(
        decodedPartial_.base()->encoding(), VectorEncoding::Simple::ROW);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto sumVector = baseRowVector->childAt(0)->as<SimpleVector<TResultType>>();
    auto isEmptyVector = baseRowVector->childAt(1)->as<SimpleVector<bool>>();
    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        if (isIntermediateResultOverflow(
                isEmptyVector, sumVector, decodedIndex)) {
          setNull(group);
        } else {
          auto sum = sumVector->valueAt(decodedIndex);
          auto isEmpty = isEmptyVector->valueAt(decodedIndex);
          if (rows.hasSelections()) {
            clearNull(group);
          }
          rows.applyToSelected([&](vector_size_t i) {
            updateNonNullValue(group, sum, isEmpty);
          });
        }
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedPartial_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedPartial_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedPartial_.size(),
                rows.end()));
        nulls = decodedPartial_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            auto decodedIndex = decodedPartial_.index(i);
            if (isIntermediateResultOverflow(
                    isEmptyVector, sumVector, decodedIndex)) {
              setNull(group);
              return;
            } else {
              clearNull(group);
              auto sum = sumVector->valueAt(decodedIndex);
              auto isEmpty = isEmptyVector->valueAt(decodedIndex);
              updateNonNullValue(group, sum, isEmpty);
            }
          },
          nulls);
    } else {
      if (rows.hasSelections()) {
        clearNull(group);
      }
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        if (isIntermediateResultOverflow(
                isEmptyVector, sumVector, decodedIndex)) {
          setNull(group);
          return;
        } else {
          auto sum = sumVector->valueAt(decodedIndex);
          auto isEmpty = isEmptyVector->valueAt(decodedIndex);
          updateNonNullValue(group, sum, isEmpty);
        }
      });
    }
  }

 private:
  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, TResultType value, bool isEmpty) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    auto decimalSum = accumulator(group);
    decimalSum->overflow +=
        DecimalUtil::addWithOverflow(decimalSum->sum, value, decimalSum->sum);
    decimalSum->isEmpty &= isEmpty;
  }

  template <bool tableHasNulls = true>
  inline void mergeAccumulators(char* group, DecimalSum other) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    auto decimalSum = accumulator(group);
    decimalSum->mergeWith(other);
  }

  inline DecimalSum* accumulator(char* group) {
    return exec::Aggregate::value<DecimalSum>(group);
  }

  inline bool isIntermediateResultOverflow(
      const SimpleVector<bool>* isEmptyVector,
      const SimpleVector<TResultType>* sumVector,
      vector_size_t index) {
    // If isEmpty is false and sum is null, it means this intermediate
    // result has an overflow. The final accumulator of this group will
    // be null.
    return !isEmptyVector->valueAt(index) && sumVector->isNullAt(index);
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
  TypePtr sumType_;
};

} // namespace bytedance::bolt::functions::aggregate::sparksql
