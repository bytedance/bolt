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

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/type/HugeInt.h"
#include <fmt/format.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <locale>
#include <sstream>

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
std::string toUnsignedDigits(T value) {
  std::ostringstream out;
  if constexpr (sizeof(T) < sizeof(int128_t)) {
    const int128_t widened = value;
    out << (widened < 0 ? -widened : widened);
  } else {
    out << (value < 0 ? -value : value);
  }
  return out.str();
}

bool shouldRoundHalfEven(
    std::string_view keptDigits,
    std::string_view discardedDigits) {
  if (discardedDigits.empty()) {
    return false;
  }

  if (discardedDigits[0] > '5') {
    return true;
  }
  if (discardedDigits[0] < '5') {
    return false;
  }
  if (discardedDigits.find_first_not_of('0', 1) != std::string_view::npos) {
    return true;
  }

  const char lastKeptDigit = keptDigits.empty() ? '0' : keptDigits.back();
  return ((lastKeptDigit - '0') % 2) == 1;
}

void incrementDigits(std::string& digits) {
  for (auto it = digits.rbegin(); it != digits.rend(); ++it) {
    if (*it != '9') {
      ++(*it);
      return;
    }
    *it = '0';
  }
  digits.insert(digits.begin(), '1');
}

std::pair<std::string, std::string> splitDecimalDigits(
    std::string_view digits,
    int32_t scale) {
  if (scale == 0) {
    return {std::string(digits), ""};
  }

  if (digits.size() <= scale) {
    return {
        "0",
        std::string(scale - digits.size(), '0').append(digits.begin(), digits.end())};
  }

  return {
      std::string(digits.substr(0, digits.size() - scale)),
      std::string(digits.substr(digits.size() - scale))};
}

template <typename T>
void formatDecimal(
    T value,
    int32_t inputScale,
    int32_t decimalPlaces,
    exec::StringWriter<false>& out) {
  const int32_t cappedPlaces = std::min(decimalPlaces, kMaxFractionDigits);
  const bool negative = value < 0;
  const std::string digits = toUnsignedDigits(value);

  std::string roundedDigits;
  if (cappedPlaces >= inputScale) {
    roundedDigits = digits;
  } else {
    const int32_t discardedCount = inputScale - cappedPlaces;
    const size_t split = digits.size() > discardedCount
        ? digits.size() - discardedCount
        : 0;

    roundedDigits = split == 0 ? "0" : std::string(digits.substr(0, split));
    const std::string_view discardedDigits(digits.data() + split, digits.size() - split);
    if (shouldRoundHalfEven(roundedDigits, discardedDigits)) {
      incrementDigits(roundedDigits);
    }
  }

  auto [integerDigits, fractionDigits] = splitDecimalDigits(
      roundedDigits, cappedPlaces < inputScale ? cappedPlaces : inputScale);

  if (cappedPlaces > inputScale) {
    fractionDigits.append(cappedPlaces - inputScale, '0');
  }

  if (negative) {
    out.append(std::string_view("-"));
  }
  appendGroupedIntegerDigits(integerDigits, out);
  if (cappedPlaces > 0) {
    out.append(std::string_view("."));
    out.append(fractionDigits);
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
  explicit DecimalFormatNumberFunction(int32_t inputScale)
      : inputScale_(inputScale) {}

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
    flatResult->getBufferWithSpace(rows.countSelected() * 392);

    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      const auto places = decimalPlaces->valueAt<int32_t>(row);
      if (places < 0) {
        result->setNull(row, true);
        return;
      }
      exec::StringWriter<false> writer(flatResult, row);
      formatDecimal(
          decimalValues->valueAt<T>(row),
          inputScale_,
          places,
          writer);
      writer.finalize();
    });
  }

 private:
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
        decimalType->asShortDecimal().scale());
  }
  BOLT_USER_CHECK(decimalType->isLongDecimal(), "Expect decimal input type.");
  return std::make_shared<DecimalFormatNumberFunction<int128_t>>(
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
