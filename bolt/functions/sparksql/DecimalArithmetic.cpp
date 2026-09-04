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

#include "bolt/functions/sparksql/DecimalArithmetic.h"

#include <type/HugeInt.h>
#include <iostream>
#include <string>
#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/sparksql/DecimalUtil.h"
#include "bolt/type/DecimalUtil.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

template <bool AllowPrecisionLoss>
std::string getResultScale(std::string precision, std::string scale) {
  if constexpr (AllowPrecisionLoss) {
    return fmt::format(
        "({}) <= 38 ? ({}) : max(({}) - ({}) + 38, min(({}), 6))",
        precision,
        scale,
        scale,
        precision,
        scale);
  } else {
    // For AllowPrecisionLoss = false, simply cap the scale at 38
    return fmt::format("min({}, 38)", scale);
  }
}

template <
    typename R /* Result Type */,
    typename A /* Argument1 */,
    typename B /* Argument2 */,
    typename Operation /* Arithmetic operation */,
    bool AllowPrecisionLoss> // Add template parameter for precision loss
                             // control
class DecimalBaseFunction : public exec::VectorFunction {
 public:
  DecimalBaseFunction(
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale)
      : aRescale_(aRescale),
        bRescale_(bRescale),
        aPrecision_(aPrecision),
        aScale_(aScale),
        bPrecision_(bPrecision),
        bScale_(bScale),
        rPrecision_(rPrecision),
        rScale_(rScale) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto rawResults = prepareResults(rows, resultType, context, result);
    if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[0]->asUnchecked<SimpleVector<A>>()->valueAt(0);
      auto flatValues = args[1]->asUnchecked<FlatVector<B>>();
      auto rawValues = flatValues->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            constant,
            rawValues[row],
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !bolt::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      auto flatValues = args[0]->asUnchecked<FlatVector<A>>();
      auto constant = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
      auto rawValues = flatValues->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            rawValues[row],
            constant,
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !bolt::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      auto flatB = args[1]->asUnchecked<FlatVector<B>>();
      auto rawB = flatB->mutableRawValues();

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            rawA[row],
            rawB[row],
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !bolt::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
      });
    } else {
      // Fast path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto a = decodedArgs.at(0);
      auto b = decodedArgs.at(1);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            a->valueAt<A>(row),
            b->valueAt<B>(row),
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !bolt::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
      });
    }
  }

 private:
  R* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  const uint8_t aRescale_;
  const uint8_t bRescale_;
  const uint8_t aPrecision_;
  const uint8_t aScale_;
  const uint8_t bPrecision_;
  const uint8_t bScale_;
  const uint8_t rPrecision_;
  const uint8_t rScale_;
};

// Decimal integral divide function implementation.
struct DecimalIntegralDivideBase {
  void initializeBase(const TypePtr& aType, const TypePtr& bType) {
    auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
    auto bScale = getDecimalPrecisionScale(*bType).second;
    rPrecision_ = computeResultPrecision(aPrecision, aScale, bScale);
    aRescale_ = std::max<int8_t>(0, bScale - aScale);
    bRescale_ = std::max<int8_t>(0, aScale - bScale);
  }

  // Computes the quotient of 'a' and 'b' and stores it in 'out'. Returns false
  // if the result exceeds rPrecision_ or if overflow occurs during scaling.
  // Following Spark's behavior, the result is truncated to int64_t if it
  // exceeds int64_t range.
  template <typename A, typename B>
  bool computeQuotient(int64_t& out, const A& a, const B& b) const {
    // Determine sign and convert to absolute values.
    bool isNegative = (a < 0) != (b < 0);
    int128_t absA = static_cast<int128_t>(a < 0 ? -a : a);
    int128_t absB = static_cast<int128_t>(b < 0 ? -b : b);

    // Scale values, checking for overflow.
    int128_t scaledA;
    int128_t scaledB;
    if (__builtin_mul_overflow(
            absA, bolt::DecimalUtil::getPowersOfTen(aRescale_), &scaledA) ||
        __builtin_mul_overflow(
            absB, bolt::DecimalUtil::getPowersOfTen(bRescale_), &scaledB)) {
      return false;
    }

    if (scaledA < scaledB) {
      out = 0;
      return true;
    }

    int128_t quotient = scaledA / scaledB;
    quotient = isNegative ? -quotient : quotient;

    if (!bolt::DecimalUtil::valueInPrecisionRange(quotient, rPrecision_)) {
      return false;
    }
    out = static_cast<int64_t>(quotient);
    return true;
  }

 private:
  static uint8_t
  computeResultPrecision(uint8_t aPrecision, uint8_t aScale, uint8_t bScale) {
    int32_t intPrecision = static_cast<int32_t>(aPrecision) - aScale + bScale;
    if (intPrecision == 0) {
      intPrecision = 1;
    }
    return DecimalUtil::bounded(static_cast<uint8_t>(intPrecision), 0).first;
  }

 protected:
  uint8_t aRescale_;
  uint8_t bRescale_;
  uint8_t rPrecision_;
};

// Decimal integral divide function that returns null on division by zero or
// overflow.
template <bool Checked>
class DecimalIntegralDivideFunction final : public exec::VectorFunction,
                                            private DecimalIntegralDivideBase {
 public:
  explicit DecimalIntegralDivideFunction(
      const TypePtr& aType,
      const TypePtr& bType) {
    initializeBase(aType, bType);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto rawResults =
        prepareResults<int64_t>(rows, resultType, context, result);

    auto aType = args[0]->type();
    auto bType = args[1]->type();
    if (aType->isShortDecimal()) {
      if (bType->isShortDecimal()) {
        applyTyped<int64_t, int64_t>(rows, args, context, result, rawResults);
      } else {
        applyTyped<int64_t, int128_t>(rows, args, context, result, rawResults);
      }
    } else {
      if (bType->isShortDecimal()) {
        applyTyped<int128_t, int64_t>(rows, args, context, result, rawResults);
      } else {
        applyTyped<int128_t, int128_t>(rows, args, context, result, rawResults);
      }
    }
  }

 private:
  template <typename R>
  R* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  template <typename A, typename B>
  void applyTyped(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx& context,
      VectorPtr& result,
      int64_t* rawResults) const {
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto a = decodedArgs.at(0);
    auto b = decodedArgs.at(1);

    if constexpr (Checked) {
      rows.applyToSelected([&](int row) {
        const auto av = a->valueAt<A>(row);
        const auto bv = b->valueAt<B>(row);
        if (bv == 0) {
          BOLT_USER_FAIL("Division by zero");
        }
        int64_t out;
        if (!computeQuotient(out, av, bv)) {
          BOLT_USER_FAIL("Overflow in integral divide");
        }
        rawResults[row] = out;
      });
    } else {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        const auto av = a->valueAt<A>(row);
        const auto bv = b->valueAt<B>(row);
        if (bv == 0) {
          result->setNull(row, true);
          return;
        }
        int64_t out;
        if (!computeQuotient(out, av, bv)) {
          result->setNull(row, true);
          return;
        }
        rawResults[row] = out;
      });
    }
  }
};

template <bool AllowPrecisionLoss>
std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalAddSubtractSignature() {
  std::string rPrecisionExpr;

  if constexpr (AllowPrecisionLoss) {
    rPrecisionExpr =
        "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)";
  } else {
    rPrecisionExpr =
        "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)";
  }

  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("b_precision")
          .integerVariable("b_scale")
          .integerVariable("r_precision", rPrecisionExpr)
          .integerVariable(
              "r_scale",
              getResultScale<AllowPrecisionLoss>(
                  "max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1",
                  "max(a_scale, b_scale)"))
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .build()};
}

template <bool AllowPrecisionLoss>
std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalMultiplySignature() {
  std::string rPrecisionExpr;

  if constexpr (AllowPrecisionLoss) {
    rPrecisionExpr = "min(38, a_precision + b_precision + 1)";
  } else {
    rPrecisionExpr = "min(38, a_precision + b_precision + 1)";
  }

  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .integerVariable("r_precision", rPrecisionExpr)
              .integerVariable(
                  "r_scale",
                  getResultScale<AllowPrecisionLoss>(
                      "a_precision + b_precision + 1", "a_scale + b_scale"))
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

template <bool AllowPrecisionLoss>
std::vector<std::shared_ptr<exec::FunctionSignature>> decimalDivideSignature() {
  std::string rPrecisionExpr;
  std::string rScaleExpr;

  if constexpr (AllowPrecisionLoss) {
    // Original behavior for AllowPrecisionLoss = true
    rPrecisionExpr =
        "min(38, a_precision - a_scale + b_scale + max(6, a_scale + b_precision + 1))";
    rScaleExpr = getResultScale<AllowPrecisionLoss>(
        "a_precision - a_scale + b_scale + max(6, a_scale + b_precision + 1)",
        "max(6, a_scale + b_precision + 1)");
  } else {
    std::string wholeDigits = "min(38, (a_precision - a_scale) + b_scale)";
    std::string fractionDigits = "min(38, max(6, a_scale + b_precision + 1))";
    std::string diff =
        fmt::format("({}) + ({}) - 38", wholeDigits, fractionDigits);

    // For diff > 0: adjust fraction digits and set whole digits to:
    // 38 - fractionDigits
    // For diff <= 0:
    // use original wholeDigits and fractionDigits
    std::string adjustedFractionDigits = fmt::format(
        "({}) > 0 ? ({}) - ((({}) / 2) + 1) : ({})",
        diff,
        fractionDigits,
        diff,
        fractionDigits);
    std::string adjustedWholeDigits = fmt::format(
        "({}) > 0 ? (38 - ({})) : ({})",
        diff,
        adjustedFractionDigits,
        wholeDigits);

    // Calculate final precision based on adjusted values
    rPrecisionExpr = fmt::format(
        "min(38, ({}) + ({}))", adjustedWholeDigits, adjustedFractionDigits);
    rScaleExpr = fmt::format("min(38, ({}))", adjustedFractionDigits);
  }

  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .integerVariable("r_precision", rPrecisionExpr)
              .integerVariable("r_scale", rScaleExpr)
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalIntegralDivideSignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .returnType("BIGINT")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

template <typename Operation, bool AllowPrecisionLoss>
std::shared_ptr<exec::VectorFunction> createDecimalFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& aType = inputArgs[0].type;
  const auto& bType = inputArgs[1].type;
  const auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  const auto [bPrecision, bScale] = getDecimalPrecisionScale(*bType);
  const auto [rPrecision, rScale] =
      Operation::template computeResultPrecisionScale<AllowPrecisionLoss>(
          aPrecision, aScale, bPrecision, bScale);
  const uint8_t aRescale =
      Operation::computeRescaleFactor(aScale, bScale, rScale);
  const uint8_t bRescale =
      Operation::computeRescaleFactor(bScale, aScale, rScale);
  if (aType->isShortDecimal()) {
    if (bType->isShortDecimal()) {
      if (rPrecision > ShortDecimalType::kMaxPrecision) {
        return std::make_shared<DecimalBaseFunction<
            int128_t /*result*/,
            int64_t,
            int64_t,
            Operation,
            AllowPrecisionLoss>>(
            aRescale,
            bRescale,
            aPrecision,
            aScale,
            bPrecision,
            bScale,
            rPrecision,
            rScale);
      } else {
        return std::make_shared<DecimalBaseFunction<
            int64_t /*result*/,
            int64_t,
            int64_t,
            Operation,
            AllowPrecisionLoss>>(
            aRescale,
            bRescale,
            aPrecision,
            aScale,
            bPrecision,
            bScale,
            rPrecision,
            rScale);
      }
    } else {
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int64_t,
          int128_t,
          Operation,
          AllowPrecisionLoss>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale);
    }
  } else {
    if (bType->isShortDecimal()) {
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int128_t,
          int64_t,
          Operation,
          AllowPrecisionLoss>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale);
    } else {
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int128_t,
          int128_t,
          Operation,
          AllowPrecisionLoss>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale);
    }
  }
}

template <bool Checked>
std::shared_ptr<exec::VectorFunction> createDecimalIntegralDivideFunction(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return std::make_shared<DecimalIntegralDivideFunction<Checked>>(
      inputArgs[0].type, inputArgs[1].type);
}
}; // namespace

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_add,
    decimalAddSubtractSignature<true>(),
    (createDecimalFunction<Addition, true>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_sub,
    decimalAddSubtractSignature<true>(),
    (createDecimalFunction<Subtraction, true>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_mul,
    decimalMultiplySignature<true>(),
    (createDecimalFunction<Multiply, true>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_div,
    decimalDivideSignature<true>(),
    (createDecimalFunction<Divide, true>));

// Register functions with AllowPrecisionLoss = false
BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_add_deny_precision_loss,
    decimalAddSubtractSignature<false>(),
    (createDecimalFunction<Addition, false>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_sub_deny_precision_loss,
    decimalAddSubtractSignature<false>(),
    (createDecimalFunction<Subtraction, false>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_mul_deny_precision_loss,
    decimalMultiplySignature<false>(),
    (createDecimalFunction<Multiply, false>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_div_deny_precision_loss,
    decimalDivideSignature<false>(),
    (createDecimalFunction<Divide, false>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_integral_div,
    decimalIntegralDivideSignature(),
    (createDecimalIntegralDivideFunction<false>));

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_checked_integral_div,
    decimalIntegralDivideSignature(),
    (createDecimalIntegralDivideFunction<true>));
}; // namespace bytedance::bolt::functions::sparksql
