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
 */
/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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
