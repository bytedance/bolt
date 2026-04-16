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

#include "bolt/functions/sparksql/DecimalVectorFunctions.h"
#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/prestosql/ArithmeticImpl.h"
#include "bolt/type/DecimalUtil.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

template <class T, bool nullOnOverflow>
class MakeDecimalFunction final : public exec::VectorFunction {
 public:
  explicit MakeDecimalFunction(uint8_t precision) : precision_(precision) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    BOLT_CHECK_EQ(args.size(), 3);
    context.ensureWritable(rows, outputType, resultRef);
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto unscaledVec = decodedArgs.at(0);
    auto result = resultRef->asUnchecked<FlatVector<T>>()->mutableRawValues();
    if constexpr (std::is_same_v<T, int64_t>) {
      int128_t bound = DecimalUtil::getPowersOfTen(precision_);
      rows.applyToSelected([&](int row) {
        auto unscaled = unscaledVec->valueAt<int64_t>(row);
        if (unscaled <= -bound || unscaled >= bound) {
          // Requested precision is too low to represent this value.
          if constexpr (nullOnOverflow) {
            resultRef->setNull(row, true);
          } else {
            BOLT_USER_FAIL(
                "Unscaled value {} too large for precision {}",
                unscaled,
                static_cast<int32_t>(precision_));
          }
        } else {
          result[row] = unscaled;
        }
      });
    } else {
      rows.applyToSelected([&](int row) {
        int128_t unscaled = unscaledVec->valueAt<int64_t>(row);
        result[row] = unscaled;
      });
    }
  }

 private:
  uint8_t precision_;
};

class DecimalRoundFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto aType = args[0]->type();
    auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
    auto [rPrecision, rScale] = getDecimalPrecisionScale(*resultType);
    int32_t scale = 0;
    if (args.size() > 1) {
      BOLT_USER_CHECK(args[1]->isConstantEncoding());
      scale = args[1]->asUnchecked<ConstantVector<int32_t>>()->valueAt(0);
    }
    if (resultType->isShortDecimal()) {
      if (aType->isShortDecimal()) {
        applyRoundRows<int64_t, int64_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      } else {
        applyRoundRows<int64_t, int128_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      }
    } else {
      if (aType->isShortDecimal()) {
        applyRoundRows<int128_t, int64_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      } else {
        applyRoundRows<int128_t, int128_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      }
    }
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

 private:
  template <typename R /* Result type */>
  R* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  template <typename R /* Result */, typename A /* Argument */>
  void applyRoundRows(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t rPrecision,
      uint8_t rScale,
      int32_t scale,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Single-arg deterministic functions receive their only
    // argument as flat or constant only.
    auto rawResults = prepareResults<R>(rows, resultType, context, result);
    if (args[0]->isConstantEncoding()) {
      // Fast path for constant vectors.
      auto constant = args[0]->asUnchecked<ConstantVector<A>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        applyRound<R, A>(
            rawResults[row],
            constant,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale);
      });
    } else {
      // Fast path for flat.
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        applyRound<R, A>(
            rawResults[row],
            rawA[row],
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale);
      });
    }
  }

  template <typename R, typename A>
  inline void applyRound(
      R& r,
      const A& a,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t rPrecision,
      uint8_t rScale,
      int32_t scale) const {
    if (scale >= 0) {
      const auto status = DecimalUtil::rescaleWithRoundUp<A, R>(
          a, aPrecision, aScale, rPrecision, rScale, r);
      BOLT_DCHECK(status.ok());
    } else {
      auto reScaleFactor = DecimalUtil::getPowersOfTen(aScale - scale);
      DecimalUtil::divideWithRoundUp<R, A, int128_t>(
          r, a, reScaleFactor, false, 0, 0);
      r = r * DecimalUtil::getPowersOfTen(-scale);
    }
  }
};

template <typename R, typename A, typename B>
inline static R
roundUpAndDown(R& r, const A& a, const B& b, bool noRoundUp, uint8_t aRescale) {
  int resultSign = 1;
  A unsignedDividendRescaled(a);
  if (a < 0) {
    resultSign = -1;
    unsignedDividendRescaled *= -1;
  }
  B unsignedDivisor(b);
  bool roundUpSign = ((noRoundUp && a > 0) || (!noRoundUp && a < 0));
  R quotient = unsignedDividendRescaled / unsignedDivisor;
  R remainder = unsignedDividendRescaled % unsignedDivisor;
  if (roundUpSign && static_cast<const B>(remainder) > 0) {
    ++quotient;
  }
  r = quotient * resultSign;
  return r;
}

// Ceil decimal function
class Ceil {
 public:
  template <typename R, typename A>
  inline static void apply(
      R& r,
      const A& a,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t aRescale) {
    roundUpAndDown<R, A, int128_t>(
        r, a, DecimalUtil::getPowersOfTen(aRescale), true, aRescale);
  }

  inline static uint8_t computeRescaleFactor(uint8_t fromScale) {
    return fromScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale) {
    return {
        std::min(38, aPrecision - aScale + std::min((uint8_t)1, aScale)), 0};
  }
};

// Floor decimal function
class Floor {
 public:
  template <typename R, typename A>
  inline static void apply(
      R& r,
      const A& a,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t aRescale) {
    roundUpAndDown<R, A, int128_t>(
        r, a, DecimalUtil::getPowersOfTen(aRescale), false, aRescale);
  }

  inline static uint8_t computeRescaleFactor(uint8_t fromScale) {
    return fromScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale) {
    return {
        std::min(38, aPrecision - aScale + std::min((uint8_t)1, aScale)), 0};
  }
};

template <
    typename R /* Result Type */,
    typename A /* Argument */,
    typename Operation /* Arithmetic operation */>
class DecimalUnaryFunction : public exec::VectorFunction {
 public:
  explicit DecimalUnaryFunction(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t aRescale)
      : aPrecision_(aPrecision), aScale_(aScale), aRescale_(aRescale) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType, // cannot used in spark
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto rawResults = prepareResults(rows, resultType, context, result);
    if (args[0]->isConstantEncoding()) {
      auto constant = args[0]->asUnchecked<SimpleVector<A>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A>(
            rawResults[row], constant, aPrecision_, aScale_, aRescale_);
      });
    } else {
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A>(
            rawResults[row], rawA[row], aPrecision_, aScale_, aRescale_);
      });
    }
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
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

  const uint8_t aPrecision_;
  const uint8_t aScale_;
  const uint8_t aRescale_;
};

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalUnaryFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& aType = inputArgs[0].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  auto [rPrecision, rScale] =
      Operation::computeResultPrecisionScale(aPrecision, aScale);
  uint8_t aRescale = Operation::computeRescaleFactor(aScale);
  if (aType->isShortDecimal()) {
    return std::make_shared<
        DecimalUnaryFunction<int64_t /*result*/, int64_t, Operation>>(
        aPrecision, aScale, aRescale);
  } else if (aType->isLongDecimal()) {
    if (rPrecision <= ShortDecimalType::kMaxPrecision) {
      return std::make_shared<
          DecimalUnaryFunction<int64_t /*result*/, int128_t, Operation>>(
          aPrecision, aScale, aRescale);
    }
    return std::make_shared<
        DecimalUnaryFunction<int128_t /*result*/, int128_t, Operation>>(
        aPrecision, aScale, aRescale);
  }
  BOLT_UNSUPPORTED();
};

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalUnarySignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable(
                  "r_precision", "min(38,a_precision-a_scale+min(1,a_scale))")
              .integerVariable("r_scale", "0")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .build()};
}
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>>
makeDecimalByUnscaledValueSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("r_precision", "a_precision")
              .integerVariable("r_scale", "a_scale")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("bigint")
              .constantArgumentType("DECIMAL(a_precision, a_scale)")
              .constantArgumentType("boolean")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeMakeDecimalByUnscaledValue(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  BOLT_CHECK_EQ(inputArgs.size(), 3);
  auto type = inputArgs[1].type;
  auto nullOnOverflow =
      inputArgs[2].constantValue->as<ConstantVector<bool>>()->valueAt(0);
  if (type->isShortDecimal()) {
    if (nullOnOverflow) {
      return std::make_shared<MakeDecimalFunction<int64_t, true>>(
          type->asShortDecimal().precision());
    } else {
      return std::make_shared<MakeDecimalFunction<int64_t, false>>(
          type->asShortDecimal().precision());
    }
  } else {
    if (nullOnOverflow) {
      return std::make_shared<MakeDecimalFunction<int128_t, true>>(
          type->asLongDecimal().precision());
    } else {
      return std::make_shared<MakeDecimalFunction<int128_t, false>>(
          type->asLongDecimal().precision());
    }
  }
}

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_decimal_round,
    std::vector<std::shared_ptr<exec::FunctionSignature>>{},
    std::make_unique<DecimalRoundFunction>());

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_ceil,
    decimalUnarySignature(),
    createDecimalUnaryFunction<Ceil>);

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_floor,
    decimalUnarySignature(),
    createDecimalUnaryFunction<Floor>);
} // namespace bytedance::bolt::functions::sparksql
