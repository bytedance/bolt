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
#include "bolt/type/DecimalUtil.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

template <typename TResult, typename TInput>
class FlinkDecimalRoundFunction : public exec::VectorFunction {
 public:
  FlinkDecimalRoundFunction(
      int32_t scale,
      uint8_t inputPrecision,
      uint8_t inputScale,
      uint8_t resultPrecision,
      uint8_t resultScale)
      : scale_(
            scale >= 0
                ? std::min(scale, (int32_t)LongDecimalType::kMaxPrecision)
                : std::max(scale, -(int32_t)LongDecimalType::kMaxPrecision)),
        roundsToZero_(
            scale < 0 &&
            static_cast<int64_t>(inputScale) - static_cast<int64_t>(scale) >
                inputPrecision),
        inputPrecision_(inputPrecision),
        inputScale_(inputScale),
        resultPrecision_(resultPrecision),
        resultScale_(resultScale) {
    auto rescaleFactor = [&](int32_t rescale) {
      BOLT_USER_CHECK_GT(
          rescale, 0, "A non-negative rescale value is expected.");
      BOLT_USER_CHECK_LE(
          rescale,
          LongDecimalType::kMaxPrecision,
          "Decimal rescale value exceeds maximum precision.");
      return DecimalUtil::getPowersOfTen(rescale);
    };
    if (scale_ < 0 && !roundsToZero_) {
      divideFactor_ = rescaleFactor(inputScale_ - scale_);
      multiplyFactor_ = rescaleFactor(-scale_);
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_USER_CHECK(
        args[0]->isConstantEncoding() || args[0]->isFlatEncoding(),
        "Single-arg deterministic functions receive their only argument as flat or constant vector.");
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    auto* flatResult = result->asUnchecked<FlatVector<TResult>>();
    if (args[0]->isConstantEncoding()) {
      applyConstant(rows, args[0], flatResult);
    } else {
      applyFlat(rows, args[0], flatResult);
    }
  }

 private:
  inline bool applyRound(const TInput& input, TResult& result) const {
    if (roundsToZero_) {
      result = 0;
      return true;
    }
    if (scale_ >= 0) {
      return DecimalUtil::rescaleWithRoundUp<TInput, TResult>(
                 input,
                 inputPrecision_,
                 inputScale_,
                 resultPrecision_,
                 resultScale_,
                 result)
          .ok();
    }

    int128_t rounded = input;
    const auto addition = divideFactor_.value() / 2;
    if (rounded < 0) {
      rounded -= addition;
    } else if (rounded > 0) {
      rounded += addition;
    }
    rounded /= divideFactor_.value();

    const auto product = rounded * multiplyFactor_.value();
    if constexpr (std::is_same_v<TResult, int128_t>) {
      if (!DecimalUtil::valueInPrecisionRange(product, resultPrecision_)) {
        return false;
      }
    }
    result = static_cast<TResult>(product);
    return true;
  }

  void applyConstant(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      FlatVector<TResult>* result) const {
    TResult rounded;
    if (!applyRound(
            arg->asUnchecked<ConstantVector<TInput>>()->valueAt(0), rounded)) {
      rows.applyToSelected([&](auto row) { result->setNull(row, true); });
      return;
    }
    auto* rawResults = result->mutableRawValues();
    rows.applyToSelected([&](auto row) { rawResults[row] = rounded; });
  }

  void applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      FlatVector<TResult>* result) const {
    auto* rawResults = result->mutableRawValues();
    const auto* rawValues = arg->asUnchecked<FlatVector<TInput>>()->rawValues();
    rows.applyToSelected([&](auto row) {
      if (!applyRound(rawValues[row], rawResults[row])) {
        result->setNull(row, true);
      }
    });
  }

  const int32_t scale_;
  const bool roundsToZero_;
  const uint8_t inputPrecision_;
  const uint8_t inputScale_;
  const uint8_t resultPrecision_;
  const uint8_t resultScale_;
  std::optional<int128_t> divideFactor_ = std::nullopt;
  std::optional<int128_t> multiplyFactor_ = std::nullopt;
};

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
      return std::make_shared<FlinkDecimalRoundFunction<int64_t, int64_t>>(
          scale, inputPrecision, inputScale, resultPrecision, resultScale);
    }
    return std::make_shared<FlinkDecimalRoundFunction<int128_t, int64_t>>(
        scale, inputPrecision, inputScale, resultPrecision, resultScale);
  }
  if (resultType->isShortDecimal()) {
    return std::make_shared<FlinkDecimalRoundFunction<int64_t, int128_t>>(
        scale, inputPrecision, inputScale, resultPrecision, resultScale);
  }
  return std::make_shared<FlinkDecimalRoundFunction<int128_t, int128_t>>(
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

exec::ExprPtr DecimalRoundCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  BOLT_USER_CHECK(
      type->isDecimal(), "The result type of decimal_round must be decimal.");
  BOLT_USER_CHECK_GE(
      args.size(), 1, "Decimal_round expects one or two arguments.");
  BOLT_USER_CHECK_LE(
      args.size(), 2, "Decimal_round expects one or two arguments.");
  BOLT_USER_CHECK(
      args[0]->type()->isDecimal(),
      "The first argument of decimal_round must be decimal.");

  const auto [inputPrecision, inputScale] =
      getDecimalPrecisionScale(*args[0]->type());
  int32_t roundScale = 0;
  bool nullScale = false;
  if (args.size() == 2) {
    BOLT_USER_CHECK_EQ(
        args[1]->type()->kind(),
        TypeKind::INTEGER,
        "The second argument of decimal_round must be integer.");
    const auto constantExpr =
        std::dynamic_pointer_cast<exec::ConstantExpr>(args[1]);
    BOLT_USER_CHECK_NOT_NULL(
        constantExpr, "The second argument of decimal_round must be constant.");
    BOLT_USER_CHECK(
        constantExpr->value()->isConstantEncoding(),
        "The second argument of decimal_round must be wrapped in a constant vector.");
    const auto constantVector =
        constantExpr->value()->asUnchecked<ConstantVector<int32_t>>();
    nullScale = constantVector->isNullAt(0);
    if (!nullScale) {
      roundScale = constantVector->valueAt(0);
    }
  }

  const auto [expectedPrecision, expectedScale] = nullScale
      ? std::pair<uint8_t, uint8_t>{inputPrecision, inputScale}
      : getResultPrecisionScale(inputPrecision, inputScale, roundScale);
  const auto [resultPrecision, resultScale] = getDecimalPrecisionScale(*type);
  BOLT_USER_CHECK_EQ(
      resultPrecision,
      expectedPrecision,
      "Unexpected precision for decimal_round result.");
  BOLT_USER_CHECK_EQ(
      resultScale, expectedScale, "Unexpected scale for decimal_round result.");

  auto function = createDecimalRoundFunction(args[0]->type(), roundScale, type);
  return std::make_shared<exec::Expr>(
      type, std::move(args), std::move(function), kRoundDecimal, trackCpuUsage);
}

} // namespace bytedance::bolt::functions::flinksql
