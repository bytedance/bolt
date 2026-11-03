/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/functions/flinksql/specialforms/DecimalRound.h"

#include <algorithm>
#include <limits>
#include <type_traits>

#include "bolt/expression/ConstantExpr.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/type/BigDecimal.h"
#include "bolt/type/DecimalUtil.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

template <bool truncate>
class DecimalRoundKernel {
 public:
  DecimalRoundKernel(
      int32_t scale,
      uint8_t inputPrecision,
      uint8_t inputScale,
      uint8_t resultPrecision,
      uint8_t resultScale)
      : resultPrecision_(resultPrecision) {
    const int64_t effectiveScale = std::min<int64_t>(scale, inputScale);
    const auto discard = inputScale - effectiveScale;
    invalidScale_ = discard > BigDecimal::kMaxScaleChange;
    zero_ = discard > inputPrecision;
    if (!zero_) {
      divisor_ = DecimalUtil::getPowersOfTen(discard);
      multiplier_ = DecimalUtil::getPowersOfTen(resultScale - effectiveScale);
    }
  }

  bool invalidScale() const {
    return invalidScale_;
  }

  template <bool checkScale = true, typename TInput, typename TResult>
  FOLLY_ALWAYS_INLINE bool apply(TInput input, TResult& result) const {
    if constexpr (checkScale) {
      BOLT_USER_CHECK(!invalidScale_ || input == 0, "Decimal scale overflow");
    }
    if (zero_) {
      result = 0;
      return true;
    }
    if (divisor_ == 1) {
      BOLT_DCHECK_EQ(multiplier_, 1);
      result = static_cast<TResult>(input);
      return true;
    }
    int128_t rounded = input;
    if constexpr (!truncate) {
      const auto half = divisor_ / 2;
      if (rounded < 0) {
        rounded -= half;
      } else if (rounded > 0) {
        rounded += half;
      }
    }
    const auto product = (rounded / divisor_) * multiplier_;
    if (!DecimalUtil::valueInPrecisionRange(product, resultPrecision_)) {
      return false;
    }
    result = static_cast<TResult>(product);
    return true;
  }

 private:
  const uint8_t resultPrecision_;
  bool zero_;
  bool invalidScale_;
  int128_t divisor_ = 1;
  int128_t multiplier_ = 1;
};

template <typename TResult, typename TInput, bool truncate, bool dynamicScale>
class FlinkDecimalRoundFunction : public exec::VectorFunction {
 public:
  FlinkDecimalRoundFunction(
      int32_t scale,
      uint8_t inputPrecision,
      uint8_t inputScale,
      uint8_t resultPrecision,
      uint8_t resultScale)
      : kernel_(
            scale,
            inputPrecision,
            inputScale,
            resultPrecision,
            resultScale),
        inputPrecision_(inputPrecision),
        inputScale_(inputScale),
        resultPrecision_(resultPrecision),
        resultScale_(resultScale) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    auto* flatResult = result->asUnchecked<FlatVector<TResult>>();
    auto* rawResults = flatResult->mutableRawValues();
    if constexpr (dynamicScale) {
      exec::LocalDecodedVector inputs(context, *args[0], rows);
      exec::LocalDecodedVector scales(context, *args[1], rows);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        const DecimalRoundKernel<truncate> kernel(
            scales->valueAt<int32_t>(row),
            inputPrecision_,
            inputScale_,
            resultPrecision_,
            resultScale_);
        if (!kernel.apply(inputs->valueAt<TInput>(row), rawResults[row])) {
          flatResult->setNull(row, true);
        } else {
          flatResult->setNull(row, false);
        }
      });
    } else {
      BOLT_CHECK(args[0]->isConstantEncoding() || args[0]->isFlatEncoding());
      result->clearNulls(rows);
      if (UNLIKELY(kernel_.invalidScale())) {
        exec::LocalDecodedVector inputs(context, *args[0], rows);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          kernel_.apply(inputs->valueAt<TInput>(row), rawResults[row]);
        });
        return;
      }
      if (args[0]->isConstantEncoding()) {
        TResult rounded;
        if (!kernel_.template apply<false>(
                args[0]->asUnchecked<ConstantVector<TInput>>()->valueAt(0),
                rounded)) {
          rows.applyToSelected(
              [&](auto row) { flatResult->setNull(row, true); });
          return;
        }
        rows.applyToSelected([&](auto row) { rawResults[row] = rounded; });
      } else {
        const auto* values =
            args[0]->asUnchecked<FlatVector<TInput>>()->rawValues();
        rows.applyToSelected([&](auto row) {
          if (!kernel_.template apply<false>(values[row], rawResults[row])) {
            flatResult->setNull(row, true);
          }
        });
      }
    }
  }

 private:
  const DecimalRoundKernel<truncate> kernel_;
  const uint8_t inputPrecision_;
  const uint8_t inputScale_;
  const uint8_t resultPrecision_;
  const uint8_t resultScale_;
};

template <bool truncate, bool dynamicScale>
std::shared_ptr<exec::VectorFunction> createDecimalRoundFunction(
    const TypePtr& inputType,
    int32_t scale,
    const TypePtr& resultType) {
  const auto [inputPrecision, inputScale] =
      getDecimalPrecisionScale(*inputType);
  const auto [resultPrecision, resultScale] =
      getDecimalPrecisionScale(*resultType);
  if (inputType->isShortDecimal()) {
    if (resultType->isShortDecimal()) {
      return std::make_shared<
          FlinkDecimalRoundFunction<int64_t, int64_t, truncate, dynamicScale>>(
          scale, inputPrecision, inputScale, resultPrecision, resultScale);
    }
    return std::make_shared<
        FlinkDecimalRoundFunction<int128_t, int64_t, truncate, dynamicScale>>(
        scale, inputPrecision, inputScale, resultPrecision, resultScale);
  }
  if (resultType->isShortDecimal()) {
    return std::make_shared<
        FlinkDecimalRoundFunction<int64_t, int128_t, truncate, dynamicScale>>(
        scale, inputPrecision, inputScale, resultPrecision, resultScale);
  }
  return std::make_shared<
      FlinkDecimalRoundFunction<int128_t, int128_t, truncate, dynamicScale>>(
      scale, inputPrecision, inputScale, resultPrecision, resultScale);
}

} // namespace

std::pair<uint8_t, uint8_t>
DecimalRoundCallToSpecialForm::getResultPrecisionScale(
    uint8_t precision,
    uint8_t scale,
    int32_t roundScale) {
  if (roundScale >= scale) {
    return {precision, scale};
  }
  if (roundScale < 0) {
    return {
        std::min(
            static_cast<int32_t>(LongDecimalType::kMaxPrecision),
            1 + precision - scale),
        0};
  }
  return {1 + precision - scale + roundScale, roundScale};
}

TypePtr DecimalRoundCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  BOLT_FAIL("Flink decimal round does not support type resolution.");
}

namespace {
template <bool truncate>
exec::ExprPtr constructDecimalRoundSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage) {
  const auto* name = truncate
      ? DecimalTruncateCallToSpecialForm::kTruncateDecimal
      : DecimalRoundCallToSpecialForm::kRoundDecimal;
  BOLT_USER_CHECK(
      type->isDecimal(), "The result type of {} must be decimal.", name);
  BOLT_USER_CHECK_GE(args.size(), 1, "{} expects one or two arguments.", name);
  BOLT_USER_CHECK_LE(args.size(), 2, "{} expects one or two arguments.", name);
  BOLT_USER_CHECK(
      args[0]->type()->isDecimal(),
      "The first argument of {} must be decimal.",
      name);

  const auto [inputPrecision, inputScale] =
      getDecimalPrecisionScale(*args[0]->type());
  int32_t roundScale = 0;
  bool nullScale = false;
  bool dynamicScale = false;
  if (args.size() == 2) {
    BOLT_USER_CHECK_EQ(
        args[1]->type()->kind(),
        TypeKind::INTEGER,
        "The second argument of {} must be integer.",
        name);
    const auto constantExpr =
        std::dynamic_pointer_cast<exec::ConstantExpr>(args[1]);
    if (constantExpr == nullptr) {
      dynamicScale = true;
    } else {
      BOLT_USER_CHECK(
          constantExpr->value()->isConstantEncoding(),
          "Scale must be wrapped in a constant vector.");
      const auto constantVector =
          constantExpr->value()->asUnchecked<ConstantVector<int32_t>>();
      nullScale = constantVector->isNullAt(0);
      if (!nullScale) {
        roundScale = constantVector->valueAt(0);
      }
    }
  }

  const auto [expectedPrecision, expectedScale] =
      (nullScale || dynamicScale || truncate)
      ? std::pair<uint8_t, uint8_t>{inputPrecision, inputScale}
      : DecimalRoundCallToSpecialForm::getResultPrecisionScale(
            inputPrecision, inputScale, roundScale);
  const auto [resultPrecision, resultScale] = getDecimalPrecisionScale(*type);
  const bool preservesInputType =
      resultPrecision == inputPrecision && resultScale == inputScale;
  // Constant folding can bind a scale after Flink has inferred the result
  // type of a nonliteral scale expression.
  BOLT_USER_CHECK(
      (resultPrecision == expectedPrecision && resultScale == expectedScale) ||
          (args.size() == 2 && preservesInputType),
      "Unexpected precision or scale for {} result.",
      name);

  auto function = dynamicScale ? createDecimalRoundFunction<truncate, true>(
                                     args[0]->type(), roundScale, type)
                               : createDecimalRoundFunction<truncate, false>(
                                     args[0]->type(), roundScale, type);
  // The bound scale is part of the kernel, keeping constant-scale evaluation
  // on the single-input flat/constant fast path.
  if (!dynamicScale && !nullScale && args.size() == 2) {
    args.pop_back();
  }
  return std::make_shared<exec::Expr>(
      type, std::move(args), std::move(function), name, trackCpuUsage);
}

} // namespace

exec::ExprPtr DecimalRoundCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  return constructDecimalRoundSpecialForm<false>(
      type, std::move(args), trackCpuUsage);
}

TypePtr DecimalTruncateCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  BOLT_USER_CHECK(
      argTypes.size() == 1 || argTypes.size() == 2,
      "decimal_truncate expects one or two arguments.");
  BOLT_USER_CHECK(
      argTypes[0]->isDecimal(), "decimal_truncate requires a decimal input.");
  return argTypes[0];
}

exec::ExprPtr DecimalTruncateCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  return constructDecimalRoundSpecialForm<true>(
      type, std::move(args), trackCpuUsage);
}

} // namespace bytedance::bolt::functions::flinksql
