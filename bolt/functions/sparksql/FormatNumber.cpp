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

#include "bolt/functions/sparksql/FormatNumber.h"

#include <fmt/format.h>
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <locale>
#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/type/DecimalUtil.h"

namespace bytedance::bolt::functions::sparksql::detail {

namespace {

// Java DecimalFormat internally caps fraction digits at 340.
constexpr int32_t kMaxFractionDigits = 340;

// Custom numpunct that provides US-style thousands grouping without
// depending on system locale availability.
struct UsNumpunct : std::numpunct<char> {
  char do_thousands_sep() const override {
    return ',';
  }
  std::string do_grouping() const override {
    return "\3";
  }
  char do_decimal_point() const override {
    return '.';
  }
};

// Locale with US-style thousands grouping for fmt.
const std::locale& usLocale() {
  static const std::locale loc(std::locale::classic(), new UsNumpunct());
  return loc;
}

std::string_view zeroPadding(int32_t zeroCount) {
  static const std::string kZeros(kMaxFractionDigits, '0');
  return std::string_view(kZeros.data(), zeroCount);
}

void appendGroupedIntegerDigits(
    std::string_view digits,
    exec::StringWriter<false>& out) {
  if (digits.empty()) {
    out.append(std::string_view("0"));
    return;
  }

  const auto firstGroupSize =
      digits.size() <= 3 ? digits.size() : ((digits.size() - 1) % 3) + 1;
  out.append(digits.substr(0, firstGroupSize));

  for (size_t offset = firstGroupSize; offset < digits.size(); offset += 3) {
    out.append(std::string_view(","));
    out.append(digits.substr(offset, 3));
  }
}

template <typename T>
T roundDecimalHalfEven(T value, int32_t fromScale, int32_t toScale) {
  if (toScale >= fromScale) {
    return value;
  }

  const auto scaleFactor =
      DecimalUtil::getPowersOfTen(static_cast<uint8_t>(fromScale - toScale));
  int128_t roundedValue = value;
  int128_t remainder = roundedValue % scaleFactor;
  roundedValue /= scaleFactor;

  const int128_t absRemainder = remainder < 0 ? -remainder : remainder;
  const int128_t halfScaleFactor = scaleFactor / 2;
  if (absRemainder > halfScaleFactor) {
    roundedValue += value >= 0 ? 1 : -1;
  } else if (
      absRemainder == halfScaleFactor &&
      ((roundedValue < 0 ? -roundedValue : roundedValue) % 2 == 1)) {
    roundedValue += value >= 0 ? 1 : -1;
  }

  return static_cast<T>(roundedValue);
}

template <typename T>
size_t estimateFormattedDecimalSize(
    T value,
    int32_t inputPrecision,
    int32_t inputScale,
    int32_t decimalPlaces) {
  if (decimalPlaces < 0) {
    return 0;
  }

  const int32_t cappedPlaces = std::min(decimalPlaces, kMaxFractionDigits);
  const int32_t outputScale = std::min(cappedPlaces, inputScale);
  const int32_t integerDigits = std::max(inputPrecision - outputScale, 1);
  const int32_t groupingSeparators = (integerDigits - 1) / 3;
  return (value < 0 ? 1 : 0) + integerDigits + groupingSeparators +
      (cappedPlaces > 0 ? 1 + cappedPlaces : 0);
}

template <typename T>
void formatDecimal(
    T value,
    int32_t inputScale,
    int32_t decimalPlaces,
    exec::StringWriter<false>& out) {
  const int32_t cappedPlaces = std::min(decimalPlaces, kMaxFractionDigits);
  const int32_t outputScale = std::min(cappedPlaces, inputScale);
  const T roundedValue = roundDecimalHalfEven(value, inputScale, outputScale);
  const bool negative = value < 0;

  std::array<char, 64> plainBuffer;
  const auto plainSize = DecimalUtil::convertToPlainString<T>(
      roundedValue, outputScale, plainBuffer.size(), plainBuffer.data());
  const std::string_view plainNumber(plainBuffer.data(), plainSize);
  size_t start = 0;
  if (negative) {
    out.append(std::string_view("-"));
    if (!plainNumber.empty() && plainNumber.front() == '-') {
      start = 1;
    }
  }

  const auto decimalPoint = plainNumber.find('.', start);
  appendGroupedIntegerDigits(
      decimalPoint == std::string_view::npos
          ? plainNumber.substr(start)
          : plainNumber.substr(start, decimalPoint - start),
      out);

  if (decimalPoint != std::string_view::npos) {
    out.append(plainNumber.substr(decimalPoint));
  }

  const auto trailingZeros = cappedPlaces - outputScale;
  if (trailingZeros > 0) {
    if (decimalPoint == std::string_view::npos) {
      out.append(std::string_view("."));
    }
    out.append(zeroPadding(trailingZeros));
  }
}

} // namespace

void formatInteger(
    int64_t value,
    int32_t decimalPlaces,
    exec::StringWriter<false>& out) {
  int32_t cappedPlaces = std::min(decimalPlaces, kMaxFractionDigits);

  // Use fmt::memory_buffer (stack-allocated for typical sizes) to avoid
  // per-row heap allocation.
  fmt::memory_buffer buf;
  fmt::format_to(std::back_inserter(buf), usLocale(), "{:Ld}", value);

  if (cappedPlaces > 0) {
    buf.push_back('.');
    for (int32_t i = 0; i < cappedPlaces; ++i) {
      buf.push_back('0');
    }
  }

  out.append(std::string_view(buf.data(), buf.size()));
}

void formatFloatingPoint(
    double value,
    int32_t decimalPlaces,
    exec::StringWriter<false>& out) {
  if (std::isnan(value)) {
    out.append(std::string_view("NaN"));
    return;
  }
  if (std::isinf(value)) {
    // Java DecimalFormat with US locale uses the infinity symbol (U+221E).
    if (value < 0) {
      out.append(std::string_view("-\xE2\x88\x9E"));
    } else {
      out.append(std::string_view("\xE2\x88\x9E"));
    }
    return;
  }

  int32_t cappedPlaces = std::min(decimalPlaces, kMaxFractionDigits);

  // Use fmt::memory_buffer (stack-allocated for typical sizes) to avoid
  // per-row heap allocation.
  fmt::memory_buffer buf;
  fmt::format_to(
      std::back_inserter(buf), usLocale(), "{:.{}Lf}", value, cappedPlaces);
  out.append(std::string_view(buf.data(), buf.size()));
}

namespace {

template <typename T>
class DecimalFormatNumberFunction final : public exec::VectorFunction {
 public:
  DecimalFormatNumberFunction(int32_t inputPrecision, int32_t inputScale)
      : inputPrecision_(inputPrecision), inputScale_(inputScale) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto* decimalValues = decodedArgs.at(0);
    auto* decimalPlaces = decodedArgs.at(1);

    context.ensureWritable(rows, outputType, result);
    result->clearNulls(rows);
    auto* flatResult = result->asFlatVector<StringView>();

    size_t estimatedBufferSize = 0;
    rows.applyToSelected([&](vector_size_t row) {
      estimatedBufferSize += estimateFormattedDecimalSize(
          decimalValues->valueAt<T>(row),
          inputPrecision_,
          inputScale_,
          decimalPlaces->valueAt<int32_t>(row));
    });
    if (estimatedBufferSize > 0) {
      flatResult->getBufferWithSpace(estimatedBufferSize, true);
    }

    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      const auto places = decimalPlaces->valueAt<int32_t>(row);
      if (places < 0) {
        result->setNull(row, true);
        return;
      }
      exec::StringWriter<false> writer(flatResult, row);
      formatDecimal(
          decimalValues->valueAt<T>(row), inputScale_, places, writer);
      writer.finalize();
    });
  }

 private:
  const int32_t inputPrecision_;
  const int32_t inputScale_;
};

std::vector<std::shared_ptr<exec::FunctionSignature>>
formatNumberDecimalSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .returnType("varchar")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("integer")
              .build()};
}

std::shared_ptr<exec::VectorFunction> createDecimalFormatNumberFunction(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  BOLT_CHECK_EQ(inputArgs.size(), 2);
  const auto& decimalType = inputArgs[0].type;
  if (decimalType->isShortDecimal()) {
    return std::make_shared<DecimalFormatNumberFunction<int64_t>>(
        decimalType->asShortDecimal().precision(),
        decimalType->asShortDecimal().scale());
  }
  BOLT_USER_CHECK(decimalType->isLongDecimal(), "Expect decimal input type.");
  return std::make_shared<DecimalFormatNumberFunction<int128_t>>(
      decimalType->asLongDecimal().precision(),
      decimalType->asLongDecimal().scale());
}

} // namespace

} // namespace bytedance::bolt::functions::sparksql::detail

namespace bytedance::bolt::functions::sparksql {

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_format_number,
    detail::formatNumberDecimalSignatures(),
    detail::createDecimalFormatNumberFunction);

} // namespace bytedance::bolt::functions::sparksql
