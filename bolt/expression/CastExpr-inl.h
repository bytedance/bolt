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

#include <date/tz.h>
#include "bolt/common/base/CountBits.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/core/CoreTypeSystem.h"
#include "bolt/expression/PrestoCastHooks.h"
#include "bolt/expression/StringWriter.h"
#include "bolt/functions/lib/StringUtil.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/FloatingDecimal.h"
#include "bolt/type/Type.h"
#include "bolt/type/tz/TimeZoneMap.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/SelectivityVector.h"
namespace bytedance::bolt::exec {
namespace {

inline std::string makeErrorMessage(
    const BaseVector& input,
    vector_size_t row,
    const TypePtr& toType,
    const std::string& details = "") {
  return fmt::format(
      "Cannot cast {} '{}' to {}. {}",
      input.type()->toString(),
      input.toString(row),
      toType->toString(),
      details);
}

inline std::exception_ptr makeBadCastException(
    const TypePtr& resultType,
    const BaseVector& input,
    vector_size_t row,
    const std::string& errorDetails) {
  return std::make_exception_ptr(BoltUserError(
      std::current_exception(),
      makeErrorMessage(input, row, resultType, errorDetails),
      false));
}

/// @brief Convert the unscaled value of a decimal to varchar and write to raw
/// string buffer from start position.
/// @tparam T The type of input value.
/// @param unscaledValue The input unscaled value.
/// @param scale The scale of decimal.
/// @param maxVarcharSize The estimated max size of a varchar.
/// @param startPosition The start position to write from.
/// @return A string view.
template <typename T>
StringView convertToStringView(
    T unscaledValue,
    int32_t scale,
    int32_t maxVarcharSize,
    char* const startPosition) {
  auto strSize = DecimalUtil::convertToString(
      unscaledValue, scale, maxVarcharSize, startPosition);
  return StringView(startPosition, strSize);
}

} // namespace

template <bool adjustForTimeZone>
void CastExpr::castTimestampToDate(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    VectorPtr& result,
    const tz::TimeZone* timeZone) {
  auto* resultFlatVector = result->as<FlatVector<int32_t>>();
  static const int32_t kSecsPerDay{86'400};
  auto inputVector = input.as<SimpleVector<Timestamp>>();
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    auto input = inputVector->valueAt(row);
    if constexpr (adjustForTimeZone) {
      input.toTimezone(*timeZone);
    }
    auto seconds = input.getSeconds();
    if (seconds >= 0 || seconds % kSecsPerDay == 0) {
      resultFlatVector->set(row, seconds / kSecsPerDay);
    } else {
      // For division with negatives, minus 1 to compensate the discarded
      // fractional part. e.g. -1/86'400 yields 0, yet it should be
      // considered as -1 day.
      resultFlatVector->set(row, seconds / kSecsPerDay - 1);
    }
  });
}

template <typename Func>
void CastExpr::applyToSelectedNoThrowLocal(
    EvalCtx& context,
    const SelectivityVector& rows,
    VectorPtr& result,
    Func&& func) {
  if (setNullInResultAtError()) {
    rows.template applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (...) {
        result->setNull(row, true);
      }
    });
  } else {
    rows.template applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (const BoltException& e) {
        if (!e.isUserError()) {
          throw;
        }
        // Avoid double throwing.
        context.setBoltExceptionError(row, std::current_exception());
      } catch (const std::exception&) {
        context.setError(row, std::current_exception());
      }
    });
  }
}

/// The per-row level Kernel
/// @tparam ToKind The cast target type
/// @tparam FromKind The expression type
/// @tparam TPolicy The policy used by the cast
/// @param row The index of the current row
/// @param input The input vector (of type FromKind)
/// @param result The output vector (of type ToKind)
template <TypeKind ToKind, TypeKind FromKind, typename TPolicy>
void CastExpr::applyCastKernel(
    vector_size_t row,
    EvalCtx& context,
    const SimpleVector<typename TypeTraits<FromKind>::NativeType>* input,
    FlatVector<typename TypeTraits<ToKind>::NativeType>* result) {
  auto setError = [&](const std::string& details) {
    if (setNullInResultAtError()) {
      result->setNull(row, true);
    } else {
      context.setBoltExceptionError(
          row, makeBadCastException(result->type(), *input, row, details));
    }
  };

  try {
    auto inputRowValue = input->valueAt(row);

    if constexpr (
        FromKind == TypeKind::TIMESTAMP &&
        (ToKind == TypeKind::VARCHAR || ToKind == TypeKind::VARBINARY)) {
      auto writer = exec::StringWriter<>(result, row);
      hooks_->castTimestampToString(inputRowValue, writer);
      return;
    }

    // Optimize empty input strings casting by avoiding throwing exceptions.
    if constexpr (
        FromKind == TypeKind::VARCHAR || FromKind == TypeKind::VARBINARY) {
      if constexpr (
          TypeTraits<ToKind>::isPrimitiveType &&
          TypeTraits<ToKind>::isFixedWidth) {
        if (inputRowValue.size() == 0) {
          if (setNullInResultAtError()) {
            result->setNull(row, true);
          } else {
            context.setBoltExceptionError(
                row,
                makeBadCastException(
                    result->type(), *input, row, "Empty string"));
          }
          return;
        }
      }
      if constexpr (ToKind == TypeKind::TIMESTAMP) {
        result->set(row, hooks_->castStringToTimestamp(inputRowValue));
        return;
      }
    }

    bool nullOutput = false;
    auto output = util::Converter<ToKind, void, TPolicy>::cast(
        inputRowValue, &nullOutput);

    if (nullOutput) {
      result->setNull(row, true);
    } else {
      if constexpr (
          ToKind == TypeKind::VARCHAR || ToKind == TypeKind::VARBINARY) {
        // Write the result output to the output vector
        auto writer = exec::StringWriter<>(result, row);
        writer.copy_from(output);
        writer.finalize();
      } else {
        result->set(row, output);
      }
    }
  } catch (const BoltException& ue) {
    if (!ue.isUserError()) {
      throw;
    }
    setError(ue.message());
  } catch (const std::exception& e) {
    setError(e.what());
  }
}

template <typename TInput, typename TOutput>
void CastExpr::applyDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& castResult) {
  auto sourceVector = input.as<SimpleVector<TInput>>();
  auto castResultRawBuffer =
      castResult->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto& fromPrecisionScale = getDecimalPrecisionScale(*fromType);
  const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);

  applyToSelectedNoThrowLocal(
      context, rows, castResult, [&](vector_size_t row) {
        TOutput rescaledValue;
        const auto status = DecimalUtil::rescaleWithRoundUp<TInput, TOutput>(
            sourceVector->valueAt(row),
            fromPrecisionScale.first,
            fromPrecisionScale.second,
            toPrecisionScale.first,
            toPrecisionScale.second,
            rescaledValue);
        if (status.ok()) {
          castResultRawBuffer[row] = rescaledValue;
        } else {
          if (setNullInResultAtError()) {
            castResult->setNull(row, true);
          } else {
            context.setBoltExceptionError(
                row,
                std::make_exception_ptr(BoltUserError(
                    std::current_exception(), status.message(), false)));
          }
        }
      });
}

template <typename TInput, typename TOutput>
void CastExpr::applyIntToDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& castResult) {
  auto sourceVector = input.as<SimpleVector<TInput>>();
  auto castResultRawBuffer =
      castResult->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);
  applyToSelectedNoThrowLocal(
      context, rows, castResult, [&](vector_size_t row) {
        auto inputValue = sourceVector->valueAt(row);
        auto rescaledValue = DecimalUtil::rescaleInt<TInput, TOutput>(
            inputValue, toPrecisionScale.first, toPrecisionScale.second);
        if (rescaledValue.has_value()) {
          castResultRawBuffer[row] = rescaledValue.value();
        } else {
          BOLT_USER_FAIL(
              "Cannot cast {} '{}' to DECIMAL({}, {})",
              SimpleTypeTrait<TInput>::name,
              inputValue,
              toPrecisionScale.first,
              toPrecisionScale.second);
        }
      });
}

template <typename TInput, typename TOutput>
void CastExpr::applyFloatingPointToDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& result) {
  const auto floatingInput = input.as<SimpleVector<TInput>>();
  auto rawResults =
      result->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto toPrecisionScale = getDecimalPrecisionScale(*toType);

  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    TOutput output;
    const auto status = DecimalUtil::rescaleFloatingPoint<TInput, TOutput>(
        floatingInput->valueAt(row),
        toPrecisionScale.first,
        toPrecisionScale.second,
        output);
    if (status.ok()) {
      rawResults[row] = output;
    } else {
      if (setNullInResultAtError()) {
        result->setNull(row, true);
      } else {
        context.setBoltExceptionError(
            row, makeBadCastException(toType, input, row, status.message()));
      }
    }
  });
}

template <typename T>
void CastExpr::applyVarcharToDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& result) {
  auto sourceVector = input.as<SimpleVector<StringView>>();
  auto rawBuffer = result->asUnchecked<FlatVector<T>>()->mutableRawValues();
  const auto toPrecisionScale = getDecimalPrecisionScale(*toType);

  rows.applyToSelected([&](auto row) {
    T decimalValue;
    const auto status = DecimalUtil::toDecimalValue<T>(
        hooks_->removeWhiteSpaces(sourceVector->valueAt(row)),
        toPrecisionScale.first,
        toPrecisionScale.second,
        decimalValue);
    if (status.ok()) {
      rawBuffer[row] = decimalValue;
    } else {
      if (setNullInResultAtError()) {
        result->setNull(row, true);
      } else {
        context.setBoltExceptionError(
            row, makeBadCastException(toType, input, row, status.message()));
      }
    }
  });
}

template <typename FromNativeType, TypeKind ToKind>
VectorPtr CastExpr::applyDecimalToFloatCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  using To = typename TypeTraits<ToKind>::NativeType;

  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto resultBuffer = result->asUnchecked<FlatVector<To>>()->mutableRawValues();
  const auto precisionScale = getDecimalPrecisionScale(*fromType);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
#ifndef SPARK_COMPATIBLE
  const auto scaleFactor = DecimalUtil::getPowersOfTen(precisionScale.second);
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    bool nullOutput = false;
    auto output = util::Converter<ToKind, void, util::DefaultCastPolicy>::cast(
        simpleInput->valueAt(row), &nullOutput);
    if (nullOutput) {
      result->setNull(row, true);
    } else {
      resultBuffer[row] = output / scaleFactor;
    }
  });
#else
  const int32_t scale = precisionScale.second;
  const int32_t precision = precisionScale.first;
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    auto inputValue = simpleInput->valueAt(row);
    auto fValue = FloatingDecimal::toFloatFromValue(inputValue, scale);
    if (fValue.has_value()) {
      resultBuffer[row] = *fValue;
      return;
    }
    result->setNull(row, true);
  });
#endif
  return result;
}

template <typename FromNativeType, TypeKind ToKind>
VectorPtr CastExpr::applyDecimalToDoubleCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  using To = typename TypeTraits<ToKind>::NativeType;

  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto resultBuffer = result->asUnchecked<FlatVector<To>>()->mutableRawValues();
  const auto precisionScale = getDecimalPrecisionScale(*fromType);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  int32_t scale = precisionScale.second;
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    bool nullOutput = false;
    auto inputValue = simpleInput->valueAt(row);
    auto dValue = FloatingDecimal::toDoubleFromValue(inputValue, scale);
    if (dValue.has_value()) {
      resultBuffer[row] = *dValue;
      return;
    }
    result->setNull(row, true);
  });
  return result;
}

template <typename FromNativeType, TypeKind ToKind>
VectorPtr CastExpr::applyDecimalToIntegralCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  using To = typename TypeTraits<ToKind>::NativeType;

  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto resultBuffer = result->asUnchecked<FlatVector<To>>()->mutableRawValues();
  const auto precisionScale = getDecimalPrecisionScale(*fromType);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  const auto scaleFactor = DecimalUtil::getPowersOfTen(precisionScale.second);
  if (hooks_->truncate()) {
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      resultBuffer[row] =
          static_cast<To>(simpleInput->valueAt(row) / scaleFactor);
    });
  } else {
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      auto value = simpleInput->valueAt(row);
      auto integralPart = value / scaleFactor;
      auto fractionPart = value % scaleFactor;
      auto sign = value >= 0 ? 1 : -1;
      bool needsRoundUp =
          (scaleFactor != 1) && (sign * fractionPart >= (scaleFactor >> 1));
      integralPart += needsRoundUp ? sign : 0;
      if (integralPart > std::numeric_limits<To>::max() ||
          integralPart < std::numeric_limits<To>::min()) {
        if (setNullInResultAtError()) {
          result->setNull(row, true);
        } else {
          context.setBoltExceptionError(
              row,
              makeBadCastException(
                  result->type(),
                  input,
                  row,
                  makeErrorMessage(input, row, toType) + "Out of bounds."));
        }
        return;
      }

      resultBuffer[row] = static_cast<To>(integralPart);
    });
  }
  return result;
}

template <typename FromNativeType>
VectorPtr CastExpr::applyDecimalToBooleanCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context) {
  VectorPtr result;
  context.ensureWritable(rows, BOOLEAN(), result);
  (*result).clearNulls(rows);
  auto resultBuffer =
      result->asUnchecked<FlatVector<bool>>()->mutableRawValues<uint64_t>();
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    auto value = simpleInput->valueAt(row);
    bits::setBit(resultBuffer, row, value != 0);
  });
  return result;
}

template <typename FromNativeType>
VectorPtr CastExpr::applyDecimalToVarcharCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType) {
  VectorPtr result;
  context.ensureWritable(rows, VARCHAR(), result);
  (*result).clearNulls(rows);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  int precision = getDecimalPrecisionScale(*fromType).first;
  int scale = getDecimalPrecisionScale(*fromType).second;
  // A varchar's size is estimated with unscaled value digits, dot, leading
  // zero, and possible minus sign.
  int32_t rowSize = DecimalUtil::stringSize(precision, scale);

  auto flatResult = result->asFlatVector<StringView>();
  if (StringView::isInline(rowSize)) {
    char inlined[StringView::kInlineSize];
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      flatResult->setNoCopy(
          row,
          convertToStringView<FromNativeType>(
              simpleInput->valueAt(row), scale, rowSize, inlined));
    });
    return result;
  }

  Buffer* buffer =
      flatResult->getBufferWithSpace(rows.countSelected() * rowSize);
  char* rawBuffer = buffer->asMutable<char>() + buffer->size();

  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    auto stringView = convertToStringView<FromNativeType>(
        simpleInput->valueAt(row), scale, rowSize, rawBuffer);
    flatResult->setNoCopy(row, stringView);
    if (!stringView.isInline()) {
      // If string view is inline, corresponding bytes on the raw string buffer
      // are not needed.
      rawBuffer += stringView.size();
    }
  });
  // Update the exact buffer size.
  buffer->setSize(rawBuffer - buffer->asMutable<char>());
  return result;
}

template <typename FromNativeType>
VectorPtr CastExpr::applyDecimalToPrimitiveCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  switch (toType->kind()) {
    case TypeKind::BOOLEAN:
      return applyDecimalToBooleanCast<FromNativeType>(rows, input, context);
    case TypeKind::TINYINT:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::TINYINT>(
          rows, input, context, fromType, toType);
    case TypeKind::SMALLINT:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::SMALLINT>(
          rows, input, context, fromType, toType);
    case TypeKind::INTEGER:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::INTEGER>(
          rows, input, context, fromType, toType);
    case TypeKind::BIGINT:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::BIGINT>(
          rows, input, context, fromType, toType);
    case TypeKind::REAL:
      return applyDecimalToFloatCast<FromNativeType, TypeKind::REAL>(
          rows, input, context, fromType, toType);
    case TypeKind::DOUBLE:
#ifndef SPARK_COMPATIBLE
      return applyDecimalToFloatCast<FromNativeType, TypeKind::DOUBLE>(
          rows, input, context, fromType, toType);
#else
      return applyDecimalToDoubleCast<FromNativeType, TypeKind::DOUBLE>(
          rows, input, context, fromType, toType);
#endif

    default:
      BOLT_UNSUPPORTED(
          "Cast from {} to {} is not supported",
          fromType->toString(),
          toType->toString());
  }
}

template <TypeKind KIND>
struct CastWithTz {
  template <typename T>
  static typename TypeTraits<KIND>::NativeType
  cast(T val, bool& nullOutput, const tz::TimeZone* tz) {
    BOLT_UNSUPPORTED(
        "Conversion of {} to {} is not supported",
        CppToType<T>::name,
        TypeTraits<KIND>::name);
  }
};

template <>
struct CastWithTz<TypeKind::VARCHAR> {
  template <typename T>
  static std::string cast(const T&, bool&, const tz::TimeZone*) {
    BOLT_NYI();
  }

  static std::string
  cast(const Timestamp& val, bool& nullOutput, const tz::TimeZone* tz) {
    return val.toString(TimestampToStringOptions::Precision::kMilliseconds, tz);
  }
};

template <typename To, typename From>
void applyCastWithTzKernel(
    vector_size_t row,
    const SimpleVector<From>* input,
    FlatVector<To>* result,
    bool& nullOutput,
    const tz::TimeZone* tz) {
  if constexpr (CppToType<To>::typeKind == TypeKind::VARCHAR) {
    std::string out = CastWithTz<CppToType<To>::typeKind>::cast(
        input->valueAt(row), nullOutput, tz);
    if (!nullOutput) {
      // Write the result output to the output vector
      auto writer = exec::StringWriter<>(result, row);
      writer.resize(out.size());
      if (!out.empty()) {
        std::memcpy(writer.data(), out.data(), out.size());
      }
      writer.finalize();
    } else {
      result->setNull(row, true);
    }
  } else {
    BOLT_NYI();
  }
}

template <TypeKind ToKind, TypeKind FromKind>
void CastExpr::applyCastPrimitives(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  using To = typename TypeTraits<ToKind>::NativeType;
  using From = typename TypeTraits<FromKind>::NativeType;
  auto* resultFlatVector = result->as<FlatVector<To>>();
  auto* inputSimpleVector = input.as<SimpleVector<From>>();

  const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
  auto& resultType = resultFlatVector->type();

  auto setError = [&](vector_size_t row, const std::string& details) {
    if (setNullInResultAtError()) {
      result->setNull(row, true);
    } else {
      context.setBoltExceptionError(
          row, makeBadCastException(resultType, input, row, details));
    }
  };

#ifndef SPARK_COMPATIABLE
  if constexpr (
      (FromKind == TypeKind::INTEGER || FromKind == TypeKind::BIGINT) &&
      ToKind == TypeKind::TIMESTAMP) {
    if (queryConfig.throwExceptionWhenCastIntToTimestamp()) {
      BOLT_FAIL("Cannot cast integer to timestamp");
    }
  }
#endif

  if constexpr (
      CppToType<From>::typeKind == TypeKind::TIMESTAMP &&
      CppToType<To>::typeKind == TypeKind::VARCHAR) {
    if (queryConfig.adjustTimestampToTimezone()) {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        bool nullOutput = false;
        auto sessionTzName = queryConfig.sessionTimezone();
        if (!sessionTzName.empty()) {
          auto* timeZone = tz::locateZone(sessionTzName);
          applyCastWithTzKernel<To, From>(
              row, inputSimpleVector, resultFlatVector, nullOutput, timeZone);
        } else {
          applyCastKernel<ToKind, FromKind, util::DefaultCastPolicy>(
              row, context, inputSimpleVector, resultFlatVector);
        }

        if (nullOutput) {
          resultFlatVector->setNull(row, true);
        }
      });
      return;
    }
  }

  if (!hooks_->truncate()) {
    if (!hooks_->legacy()) {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastKernel<ToKind, FromKind, util::DefaultCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
    } else {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastKernel<ToKind, FromKind, util::LegacyCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
    }
  } else {
    if (!hooks_->legacy()) {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastKernel<ToKind, FromKind, util::TruncateCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
    } else {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastKernel<ToKind, FromKind, util::TruncateLegacyCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
    }
  }

#ifndef SPARK_COMPATIBLE
  // If we're converting to a TIMESTAMP, check if we need to adjust the
  // current GMT timezone to the user provided session timezone.
  if constexpr (ToKind == TypeKind::TIMESTAMP) {
    auto prestoHook = dynamic_cast<PrestoCastHooks*>(hooks_.get());
    if (FOLLY_LIKELY(prestoHook)) {
      const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
      // If user explicitly asked us to adjust the timezone.
      if (queryConfig.adjustTimestampToTimezone()) {
        auto sessionTzName = queryConfig.sessionTimezone();
        if (!sessionTzName.empty()) {
          // When context.throwOnError is false, some rows will be marked as
          // 'failed'. These rows should not be processed further.
          // 'remainingRows' will contain a subset of 'rows' that have passed
          // all the checks (e.g. keys are not nulls and number of keys and
          // values is the same).
          exec::LocalSelectivityVector remainingRows(context, rows);
          context.deselectErrors(*remainingRows);
          // locate_zone throws runtime_error if the timezone couldn't be found
          // (so we're safe to dereference the pointer).
          auto* timeZone = tz::locateZone(sessionTzName);
          auto rawTimestamps = resultFlatVector->mutableRawValues();
          applyToSelectedNoThrowLocal(
              context, *remainingRows, result, [&](int row) {
                rawTimestamps[row].toGMT(*timeZone);
              });
        }
      }
    }
  }
#endif
}

template <TypeKind ToKind>
void CastExpr::applyCastPrimitivesDispatch(
    const TypePtr& fromType,
    const TypePtr& toType,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  context.ensureWritable(rows, toType, result);

  // This already excludes complex types, hugeint and unknown from type kinds.
  BOLT_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
      applyCastPrimitives,
      ToKind,
      fromType->kind() /*dispatched*/,
      rows,
      context,
      input,
      result);
}

template <
    TypeKind FromKind,
    typename TPolicy,
    CastExpr::ComplexTypeToStringMode Mode>
void CastExpr::applyCastNonPromitiveToVarcharEval(
    vector_size_t row,
    const VectorPtr& input,
    FlatVector<StringView>* flatResult,
    functions::InPlaceString& result) {
  if constexpr (FromKind == TypeKind::MAP) {
    auto* mapVector = input->asUnchecked<MapVector>();
    result.append(leftBracket_, flatResult);
    auto& keys = mapVector->mapKeys();
    auto& values = mapVector->mapValues();
    auto keyKind = keys->typeKind();
    auto valKind = values->typeKind();
    auto size = mapVector->rawSizes()[row];
    auto offset = mapVector->rawOffsets()[row];
    for (auto i = 0; i < size; ++i) {
      auto mapIdx = offset + i;
      if (i > 0) {
        result.append(space_str_, space_size_, flatResult);
      }
      BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
          applyCastNonPromitiveToVarcharEval,
          keyKind,
          TPolicy,
          Mode,
          mapIdx,
          keys,
          flatResult,
          result);

      if constexpr (Mode != CastExpr::ComplexTypeToStringMode::FLINK) {
        result.append(arrow_str_, arrow_size_, flatResult);
      } else {
        result.append(equal_str_, equal_size_, flatResult);
      }
      if (values->isNullAt(mapIdx)) {
        if constexpr (Mode != CastExpr::ComplexTypeToStringMode::SPARK_LEGACY) {
          if constexpr (Mode != CastExpr::ComplexTypeToStringMode::FLINK) {
            result.append(space_str_, space_size_, flatResult);
          }
          result.append(null_str_, null_size_, flatResult);
        }
      } else {
        if constexpr (Mode != CastExpr::ComplexTypeToStringMode::FLINK) {
          result.append(space_str_, space_size_, flatResult);
        }
        BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
            applyCastNonPromitiveToVarcharEval,
            valKind,
            TPolicy,
            Mode,
            mapIdx,
            values,
            flatResult,
            result);
      }
      if (i + 1 < size) {
        result.append(dot_str_, dot_size_, flatResult);
      }
    }
    result.append(rightBracket_, flatResult);
  } else if constexpr (FromKind == TypeKind::ROW) {
    result.append(leftBracket_, flatResult);
    auto* rowVector = input->asUnchecked<RowVector>();
    auto childSize = rowVector->childrenSize();
    for (auto i = 0; i < childSize; i++) {
      auto& child = rowVector->childAt(i);
      auto childKind = child->typeKind();
      if (child->isNullAt(row)) {
        if constexpr (Mode != CastExpr::ComplexTypeToStringMode::SPARK_LEGACY) {
          if (i > 0) {
            result.append(space_str_, space_size_, flatResult);
          }
          result.append(null_str_, null_size_, flatResult);
        }
      } else {
        if (i > 0) {
          result.append(space_str_, space_size_, flatResult);
        }
        BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
            applyCastNonPromitiveToVarcharEval,
            childKind,
            TPolicy,
            Mode,
            row,
            child,
            flatResult,
            result);
      }
      if (i + 1 < childSize) {
        result.append(dot_str_, dot_size_, flatResult);
      }
    }
    result.append(rightBracket_, flatResult);
  } else if constexpr (FromKind == TypeKind::ARRAY) {
    result.append(left_square_bracket_str_, bracket_size_, flatResult);
    auto* arrayVector = input->asUnchecked<ArrayVector>();
    auto size = arrayVector->rawSizes()[row];
    auto offset = arrayVector->rawOffsets()[row];
    auto elements = arrayVector->elements();
    auto kind = elements->typeKind();
    for (auto i = 0; i < size; i++) {
      auto idx = i + offset;
      if (elements->isNullAt(idx)) {
        if constexpr (Mode != CastExpr::ComplexTypeToStringMode::SPARK_LEGACY) {
          if (i > 0) {
            result.append(space_str_, space_size_, flatResult);
          }
          result.append(null_str_, null_size_, flatResult);
        }
      } else {
        if (i > 0) {
          result.append(space_str_, space_size_, flatResult);
        }
        BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
            applyCastNonPromitiveToVarcharEval,
            kind,
            TPolicy,
            Mode,
            idx,
            elements,
            flatResult,
            result);
      }
      if (i + 1 < size) {
        result.append(dot_str_, dot_size_, flatResult);
      }
    }
    result.append(right_square_bracket_str_, bracket_size_, flatResult);
  } else if constexpr (FromKind == TypeKind::UNKNOWN) {
    result.append(null_str_, null_size_, flatResult);
  } else {
    bool nullOutput = false;
    auto* newInput =
        input->as<SimpleVector<typename TypeTraits<FromKind>::NativeType>>();
    auto inputRowValue = newInput->valueAt(row);
#ifdef SPARK_COMPATIBLE
    if constexpr (FromKind == TypeKind::TIMESTAMP) {
      static constexpr TimestampToStringOptions options = {
          .precision = TimestampToStringOptions::Precision::kMicroseconds,
          .leadingPositiveSign = true,
          .skipTrailingZeros = true,
          .zeroPaddingYear = true,
          .dateTimeSeparator = ' ',
      };
      result.append(inputRowValue.toString(options), flatResult);
      return;
    }
#endif
    if constexpr (
        FromKind == TypeKind::HUGEINT || FromKind == TypeKind::BIGINT) {
      if (input->type()->isDecimal()) {
        auto& fromType = input->type();
        int precision = getDecimalPrecisionScale(*fromType).first;
        int scale = getDecimalPrecisionScale(*fromType).second;
        // A varchar's size is estimated with unscaled value digits, dot,
        // leading zero, and possible minus sign.
        int32_t rowSize = DecimalUtil::stringSize(precision, scale);
        if (result.availableSize() < rowSize) {
          result.setNewBuffer(result.size() + rowSize, flatResult);
        }
        auto rawBuffer = result.data() + result.size();
        auto stringView =
            convertToStringView<typename TypeTraits<FromKind>::NativeType>(
                inputRowValue, scale, rowSize, rawBuffer);
        result.setSize(result.size() + stringView.size());
        return;
      }
    }
    auto output =
        util::Converter<TypeKind::VARCHAR, void, util::DefaultCastPolicy>::cast(
            inputRowValue, &nullOutput);
    BOLT_CHECK(!nullOutput);
    result.append(output, flatResult);
  }
}

template <
    TypeKind FromKind,
    typename TPolicy,
    CastExpr::ComplexTypeToStringMode Mode>
void CastExpr::applyCastNonPromitiveToVarcharKernel(
    vector_size_t row,
    const BaseVector& input,
    exec::EvalCtx& context,
    FlatVector<StringView>* flatResult) {
  auto setError = [&](const std::string& details) {
    if (setNullInResultAtError()) {
      flatResult->setNull(row, true);
    } else {
      context.setBoltExceptionError(
          row, makeBadCastException(flatResult->type(), input, row, details));
    }
  };

  if (input.isNullAt(row)) {
    flatResult->setNull(row, true);
  }

  try {
    functions::InPlaceString result{flatResult};
    if constexpr (FromKind == TypeKind::MAP) {
      auto* mapVector = input.asUnchecked<MapVector>();
      result.append(leftBracket_, flatResult);
      auto& keys = mapVector->mapKeys();
      auto& values = mapVector->mapValues();
      auto keyKind = keys->typeKind();
      auto valKind = values->typeKind();
      auto size = mapVector->rawSizes()[row];
      auto offset = mapVector->rawOffsets()[row];
      for (auto i = 0; i < size; ++i) {
        auto mapIdx = offset + i;
        if (i > 0) {
          result.append(space_str_, space_size_, flatResult);
        }
        BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
            applyCastNonPromitiveToVarcharEval,
            keyKind,
            TPolicy,
            Mode,
            mapIdx,
            keys,
            flatResult,
            result);
        if constexpr (Mode != CastExpr::ComplexTypeToStringMode::FLINK) {
          result.append(arrow_str_, arrow_size_, flatResult);
        } else {
          result.append(equal_str_, equal_size_, flatResult);
        }
        if (values->isNullAt(mapIdx)) {
          if constexpr (
              Mode != CastExpr::ComplexTypeToStringMode::SPARK_LEGACY) {
            if constexpr (Mode != CastExpr::ComplexTypeToStringMode::FLINK) {
              result.append(space_str_, space_size_, flatResult);
            }
            result.append(null_str_, null_size_, flatResult);
          }
        } else {
          if constexpr (Mode != CastExpr::ComplexTypeToStringMode::FLINK) {
            result.append(space_str_, space_size_, flatResult);
          }
          BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
              applyCastNonPromitiveToVarcharEval,
              valKind,
              TPolicy,
              Mode,
              mapIdx,
              values,
              flatResult,
              result);
        }
        if (i + 1 < size) {
          result.append(dot_str_, dot_size_, flatResult);
        }
      }
      result.append(rightBracket_, flatResult);
    } else if constexpr (FromKind == TypeKind::ROW) {
      result.append(leftBracket_, flatResult);
      auto* rowVector = input.asUnchecked<RowVector>();
      auto childSize = rowVector->childrenSize();
      for (auto i = 0; i < childSize; i++) {
        auto& child = rowVector->childAt(i);
        auto childKind = child->typeKind();
        if (child->isNullAt(row)) {
          if constexpr (
              Mode != CastExpr::ComplexTypeToStringMode::SPARK_LEGACY) {
            if (i > 0) {
              result.append(space_str_, space_size_, flatResult);
            }
            result.append(null_str_, null_size_, flatResult);
          }
        } else {
          if (i > 0) {
            result.append(space_str_, space_size_, flatResult);
          }
          BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
              applyCastNonPromitiveToVarcharEval,
              childKind,
              TPolicy,
              Mode,
              row,
              child,
              flatResult,
              result);
        }
        if (i + 1 < childSize) {
          result.append(dot_str_, dot_size_, flatResult);
        }
      }
      result.append(rightBracket_, flatResult);
    } else if constexpr (FromKind == TypeKind::ARRAY) {
      result.append(left_square_bracket_str_, bracket_size_, flatResult);
      auto* arrayVector = input.asUnchecked<ArrayVector>();
      auto size = arrayVector->rawSizes()[row];
      auto offset = arrayVector->rawOffsets()[row];
      auto elements = arrayVector->elements();
      auto kind = elements->typeKind();
      for (auto i = 0; i < size; i++) {
        auto idx = i + offset;
        if (elements->isNullAt(idx)) {
          if constexpr (
              Mode != CastExpr::ComplexTypeToStringMode::SPARK_LEGACY) {
            if (i > 0) {
              result.append(space_str_, space_size_, flatResult);
            }
            result.append(null_str_, null_size_, flatResult);
          }
        } else {
          if (i > 0) {
            result.append(space_str_, space_size_, flatResult);
          }
          BOLT_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
              applyCastNonPromitiveToVarcharEval,
              kind,
              TPolicy,
              Mode,
              idx,
              elements,
              flatResult,
              result);
        }
        if (i + 1 < size) {
          result.append(dot_str_, dot_size_, flatResult);
        }
      }
      result.append(right_square_bracket_str_, bracket_size_, flatResult);
    } else {
      BOLT_NYI("invalid from type: ", mapTypeKindToName(FromKind));
    }
    result.set(row, flatResult);
  } catch (const BoltException& ue) {
    if (!ue.isUserError()) {
      throw;
    }
    setError(ue.message());
  } catch (const std::exception& e) {
    setError(e.what());
  }
}

template <TypeKind FromKind, CastExpr::ComplexTypeToStringMode Mode>
void CastExpr::applyNonPrimitiveToVarcharCastPolicySelector(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    VectorPtr& result) {
  auto resultFlatVector = result->as<FlatVector<StringView>>();
  if (!hooks_->truncate()) {
    if (!hooks_->legacy()) {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastNonPromitiveToVarcharKernel<
            FromKind,
            util::DefaultCastPolicy,
            Mode>(row, input, context, resultFlatVector);
      });
    } else {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastNonPromitiveToVarcharKernel<
            FromKind,
            util::LegacyCastPolicy,
            Mode>(row, input, context, resultFlatVector);
      });
    }
  } else {
    if (!hooks_->legacy()) {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastNonPromitiveToVarcharKernel<
            FromKind,
            util::TruncateCastPolicy,
            Mode>(row, input, context, resultFlatVector);
      });
    } else {
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastNonPromitiveToVarcharKernel<
            FromKind,
            util::TruncateLegacyCastPolicy,
            Mode>(row, input, context, resultFlatVector);
      });
    }
  }
}

template <TypeKind FromKind>
void CastExpr::applyNonPrimitiveToVarchar(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result) {
  BOLT_CHECK(toType->kind() == TypeKind::VARCHAR);
  context.ensureWritable(rows, toType, result);
  const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();

  if (queryConfig.enableFlinkCompatible()) {
    applyNonPrimitiveToVarcharCastPolicySelector<
        FromKind,
        CastExpr::ComplexTypeToStringMode::FLINK>(rows, input, context, result);
  } else if (hooks_->legacyComplexTypeToString()) {
    applyNonPrimitiveToVarcharCastPolicySelector<
        FromKind,
        CastExpr::ComplexTypeToStringMode::SPARK_LEGACY>(
        rows, input, context, result);
  } else {
    applyNonPrimitiveToVarcharCastPolicySelector<
        FromKind,
        CastExpr::ComplexTypeToStringMode::SPARK>(rows, input, context, result);
  }
}

} // namespace bytedance::bolt::exec
