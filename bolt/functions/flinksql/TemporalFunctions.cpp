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

#include "bolt/functions/flinksql/TemporalFunctions.h"

#include <date/julian.h>
#include <unicode/uchar.h>
#include <unicode/utf8.h>
#include <algorithm>
#include <charconv>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneMap.h"

namespace bytedance::bolt::functions::flinksql {
namespace {

bool isDigit(char c) {
  return c >= '0' && c <= '9';
}

// The default Java formatter uses fixed-width fields. Other accepted inputs
// pass through java.sql.Timestamp.valueOf / Date.valueOf instead.
bool isCanonicalTimestamp(std::string_view text) {
  const auto size = text.size();
  if (size != 10 && size != 19 && (size < 21 || size > 29)) {
    return false;
  }
  for (size_t i = 0; i < size; ++i) {
    char separator = 0;
    switch (i) {
      case 4:
      case 7:
        separator = '-';
        break;
      case 10:
        separator = ' ';
        break;
      case 13:
      case 16:
        separator = ':';
        break;
      case 19:
        separator = '.';
        break;
      default:
        break;
    }
    if (separator ? text[i] != separator : !isDigit(text[i])) {
      return false;
    }
  }
  return true;
}

bool parseInteger(std::string_view text, int32_t& value) {
  if (text.empty()) {
    return false;
  }
  if (text.front() == '+') {
    text.remove_prefix(1);
    if (text.empty() || !isDigit(text.front())) {
      return false;
    }
  }
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), value);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

bool parseTimestamp(
    std::string_view text,
    Timestamp& result,
    const tz::TimeZone* defaultTimeZone,
    int32_t defaultRawOffset,
    bool allowCanonical = true) {
  const bool canonical = allowCanonical && isCanonicalTimestamp(text);
  // :60 uses Java's legacy calendar, even when the other fields are canonical.
  if (canonical && text.substr(0, 4) != "0000" &&
      (text.size() == 10 || text[17] < '6')) {
    bool invalid = false;
    result = util::fromTimestampStringNanos(text.data(), text.size(), &invalid);
    if (!invalid) {
      return true;
    }
  }

  // String.trim() in the Java fallback removes code points <= U+0020.
  while (!text.empty() && static_cast<unsigned char>(text.front()) <= 0x20) {
    text.remove_prefix(1);
  }
  while (!text.empty() && static_cast<unsigned char>(text.back()) <= 0x20) {
    text.remove_suffix(1);
  }
  const auto space = text.find(' ');
  const auto date = text.substr(0, space);
  const auto firstDash = date.find('-');
  const auto secondDash =
      date.find('-', firstDash == std::string_view::npos ? 0 : firstDash + 1);
  int32_t year = 0;
  int32_t month = 0;
  int32_t day = 0;
  if (firstDash != 4 || secondDash == std::string_view::npos ||
      secondDash - firstDash < 2 || secondDash - firstDash > 3 ||
      date.size() - secondDash < 2 || date.size() - secondDash > 3 ||
      !parseInteger(date.substr(0, firstDash), year) ||
      !parseInteger(
          date.substr(firstDash + 1, secondDash - firstDash - 1), month) ||
      !parseInteger(date.substr(secondDash + 1), day) || month < 1 ||
      month > 12 || day < 1 || day > 31) {
    return false;
  }
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  uint64_t nanos = 0;
  if (space != std::string_view::npos) {
    const auto time = text.substr(space + 1);
    const auto firstColon = time.find(':');
    if (firstColon == std::string_view::npos) {
      return false;
    }
    const auto secondColon = time.find(':', firstColon + 1);
    if (secondColon == std::string_view::npos) {
      return false;
    }
    const auto dot = time.find('.', secondColon + 1);
    if (!parseInteger(time.substr(0, firstColon), hour) ||
        !parseInteger(
            time.substr(firstColon + 1, secondColon - firstColon - 1),
            minute) ||
        !parseInteger(
            time.substr(secondColon + 1, dot - secondColon - 1), second)) {
      return false;
    }
    if (dot != std::string_view::npos) {
      const auto fraction = time.substr(dot + 1);
      if (fraction.empty() || fraction.size() > 9 ||
          !std::all_of(fraction.begin(), fraction.end(), isDigit)) {
        return false;
      }
      // Reuse the nanosecond scanner for the Java fallback as well.
      std::string fractionalTime = "1970-01-01 00:00:00.";
      fractionalTime.append(fraction);
      nanos = util::fromTimestampStringNanos(
                  fractionalTime.data(), fractionalTime.size(), nullptr)
                  .getNanos();
    }
  }

  // DateTimeFormatter's SMART resolver clamps the day of month. The legacy
  // fallback normalizes overflowing days and times using the hybrid calendar.
  const bool smart = canonical && year > 0 && hour >= 0 &&
      (hour < 24 || (hour == 24 && minute == 0 && second == 0 && nanos == 0)) &&
      minute >= 0 && minute < 60 && second >= 0 && second < 60;
  if (smart) {
    day = std::min(day, util::getMaxDayOfMonth(year, month));
  }
  const int64_t time = static_cast<int64_t>(hour) * util::kSecsPerHour +
      static_cast<int64_t>(minute) * util::kSecsPerMinute + second;
  int64_t seconds = (util::daysSinceEpochFromDate(year, month, 1) + day - 1) *
          util::kSecsPerDay +
      time;
  bool julianCalendar = false;
  if (!smart) {
    const auto cutover = ::date::sys_days(::date::year(1582) / 10 / 15);
    if (seconds < std::chrono::duration_cast<std::chrono::seconds>(
                      cutover.time_since_epoch())
                      .count()) {
      julianCalendar = true;
      const auto julianDate = ::julian::year_month_day(
          ::julian::year(year), ::julian::month(month), ::julian::day(1));
      const auto instant = ::date::sys_days(julianDate) +
          ::date::days(day - 1) + std::chrono::seconds(time);
      const auto days = ::date::floor<::date::days>(instant);
      if (days >= cutover) {
        seconds = instant.time_since_epoch().count();
      } else {
        // Julian leap years repeat every four years. Reduce to one cycle
        // before constructing the 16-bit Julian year representation.
        constexpr int64_t kYearsPerCycle = 4;
        constexpr int64_t kDaysPerCycle = kYearsPerCycle * 365 + 1;
        const auto cycleStart = ::date::sys_days(
            ::julian::year(0) / ::julian::month(1) / ::julian::day(1));
        const int64_t elapsed = (days - cycleStart).count();
        const int64_t cycles =
            (elapsed >= 0 ? elapsed : elapsed - kDaysPerCycle + 1) /
            kDaysPerCycle;
        const ::julian::year_month_day normalized(
            days - ::date::days(cycles * kDaysPerCycle));
        int32_t normalizedYear =
            static_cast<int>(normalized.year()) + cycles * kYearsPerCycle;
        // java.sql.Timestamp.toLocalDateTime does not carry the BC era.
        normalizedYear =
            normalizedYear <= 0 ? 1 - normalizedYear : normalizedYear;
        bool isValid = false;
        const int64_t normalizedDays = util::daysSinceEpochFromDate(
            normalizedYear,
            static_cast<unsigned>(normalized.month()),
            static_cast<unsigned>(normalized.day()),
            &isValid);
        if (!isValid) {
          return false;
        }
        seconds = normalizedDays * util::kSecsPerDay + (instant - days).count();
      }
    }
  }
  // Legacy TimeZone uses its current raw offset before 1900 and historical
  // offsets afterwards. This creates a transition absent from the IANA data.
  // BC era removal above must not subject ancient inputs to modern DST rules.
  constexpr int64_t kLegacyZoneStart = -2'208'988'800; // 1900-01-01 UTC.
  if (!smart && !julianCalendar && defaultTimeZone != nullptr) {
    if (seconds >= kLegacyZoneStart + util::kSecsPerDay) {
      seconds = defaultTimeZone
                    ->correct_nonexistent_time(std::chrono::seconds(seconds))
                    .count();
    } else if (seconds >= kLegacyZoneStart - util::kSecsPerDay) {
      const int64_t oldLocal = kLegacyZoneStart + defaultRawOffset;
      const int64_t newLocal =
          defaultTimeZone->to_local(std::chrono::seconds(kLegacyZoneStart))
              .count();
      if (newLocal > oldLocal && seconds >= oldLocal && seconds < newLocal) {
        seconds += newLocal - oldLocal;
      } else if (seconds >= std::max(oldLocal, newLocal)) {
        seconds = defaultTimeZone
                      ->correct_nonexistent_time(std::chrono::seconds(seconds))
                      .count();
      }
    }
  }
  result = Timestamp(seconds, nanos);
  return true;
}

bool parseUnicodeTimestamp(
    std::string_view text,
    Timestamp& result,
    const tz::TimeZone* defaultTimeZone,
    int32_t defaultRawOffset) {
  if (std::all_of(
          text.begin(), text.end(), [](unsigned char c) { return c < 0x80; })) {
    return false;
  }
  if (text.size() > std::numeric_limits<int32_t>::max()) {
    return false;
  }
  std::string normalized;
  normalized.reserve(text.size());
  for (int32_t i = 0; i < text.size();) {
    UChar32 codePoint;
    U8_NEXT(text, i, text.size(), codePoint);
    if (codePoint < 0 || codePoint > 0xffff) {
      return false;
    }
    if (codePoint < 0x80) {
      normalized.push_back(static_cast<char>(codePoint));
    } else {
      // Integer.parseInt reads UTF-16 chars, so supplementary digits are
      // invalid.
      const int32_t digit = u_digit(codePoint, 10);
      UVersionInfo age;
      u_charAge(codePoint, age);
      // Flink's Java 8 runtime implements Unicode 6.2.
      if (digit < 0 || age[0] > 6 || (age[0] == 6 && age[1] > 2)) {
        return false;
      }
      normalized.push_back('0' + digit);
    }
  }
  return parseTimestamp(
      normalized, result, defaultTimeZone, defaultRawOffset, false);
}

template <int32_t Precision>
class TimestampPrecisionFunction final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    if constexpr (Precision == 9) {
      context.moveOrCopyResult(args[0], rows, result);
      return;
    }
    context.ensureWritable(rows, outputType, result);
    result->clearNulls(rows);
    auto* output = result->asUnchecked<FlatVector<Timestamp>>();
    exec::LocalDecodedVector timestamps(context, *args[0], rows);
    if constexpr (Precision >= 0) {
      rows.applyToSelected([&](vector_size_t row) {
        output->set(
            row, timestamps->valueAt<Timestamp>(row).toPrecision<Precision>());
      });
    } else {
      exec::LocalDecodedVector precisions(context, *args[1], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        output->set(
            row,
            timestamps->valueAt<Timestamp>(row).toPrecision(
                precisions->valueAt<int32_t>(row)));
      });
    }
  }
};

std::shared_ptr<exec::VectorFunction> makeTimestampPrecision(
    const std::string&,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig&) {
  const auto& precision = args[1].constantValue;
  if (precision != nullptr && !precision->isNullAt(0)) {
    switch (precision->as<SimpleVector<int32_t>>()->valueAt(0)) {
      case 0:
        return std::make_shared<TimestampPrecisionFunction<0>>();
      case 1:
        return std::make_shared<TimestampPrecisionFunction<1>>();
      case 2:
        return std::make_shared<TimestampPrecisionFunction<2>>();
      case 3:
        return std::make_shared<TimestampPrecisionFunction<3>>();
      case 4:
        return std::make_shared<TimestampPrecisionFunction<4>>();
      case 5:
        return std::make_shared<TimestampPrecisionFunction<5>>();
      case 6:
        return std::make_shared<TimestampPrecisionFunction<6>>();
      case 7:
        return std::make_shared<TimestampPrecisionFunction<7>>();
      case 8:
        return std::make_shared<TimestampPrecisionFunction<8>>();
      case 9:
        return std::make_shared<TimestampPrecisionFunction<9>>();
      default:
        break;
    }
  }
  // Defer invalid precision errors to evaluated rows so TRY and nulls work.
  return std::make_shared<TimestampPrecisionFunction<-1>>();
}

int32_t parseOffset(std::string_view name, bool legacy) {
  BOLT_USER_CHECK(name.size() >= 2, "Invalid time zone offset: {}", name);
  const bool negative = name.front() == '-';
  name.remove_prefix(1);
  int32_t hours = 0;
  int32_t minutes = 0;
  int32_t seconds = 0;
  const auto number = [&](std::string_view part, int32_t& value) {
    return !part.empty() && std::all_of(part.begin(), part.end(), isDigit) &&
        parseInteger(part, value);
  };
  bool valid = false;
  if (name.size() == 1 || name.size() == 2) {
    valid = number(name, hours);
  } else if (name.size() == 4 || name.size() == 6) {
    valid = number(name.substr(0, 2), hours) &&
        number(name.substr(2, 2), minutes) &&
        (name.size() == 4 || number(name.substr(4, 2), seconds));
  } else if (name.size() == 5 || name.size() == 8) {
    valid = name[2] == ':' && number(name.substr(0, 2), hours) &&
        number(name.substr(3, 2), minutes) &&
        (name.size() == 5 ||
         (name[5] == ':' && number(name.substr(6, 2), seconds)));
  }
  valid = valid && minutes < 60 && seconds < 60 &&
      (legacy ? hours < 24
              : hours <= 18 && (hours < 18 || (minutes == 0 && seconds == 0)));
  BOLT_USER_CHECK(valid, "Invalid time zone offset: {}", name);
  const auto offset =
      hours * util::kSecsPerHour + minutes * util::kSecsPerMinute + seconds;
  return negative ? -offset : offset;
}

void resolveTimeZone(
    std::string_view name,
    const tz::TimeZone*& zone,
    int32_t& offset,
    bool legacy = false) {
  if (name == "Z" || name == "UTC" || name == "GMT" || name == "UT") {
    return;
  }
  for (const std::string_view prefix : {"UTC", "GMT", "UT"}) {
    if (name.substr(0, prefix.size()) == prefix &&
        name.size() > prefix.size() &&
        (name[prefix.size()] == '+' || name[prefix.size()] == '-')) {
      name.remove_prefix(prefix.size());
      break;
    }
  }
  if (!name.empty() && (name.front() == '+' || name.front() == '-')) {
    offset = parseOffset(name, legacy);
  } else {
    zone = tz::locateZone(name);
  }
}

struct LegacyTimestampContext {
  const tz::TimeZone* defaultTimeZone_ = nullptr;
  int32_t defaultRawOffset_ = 0;

  template <typename StringType>
  void initializeDefaultTimeZone(
      const StringType* zone,
      const int32_t* rawOffset) {
    BOLT_USER_CHECK_NOT_NULL(zone, "Default JVM time zone must be constant");
    BOLT_USER_CHECK_NOT_NULL(
        rawOffset, "Default JVM raw offset must be constant");
    defaultRawOffset_ = *rawOffset;
    int32_t offset = 0;
    resolveTimeZone(
        std::string_view(zone->data(), zone->size()),
        defaultTimeZone_,
        offset,
        true);
  }

  bool parse(std::string_view text, Timestamp& result, bool allowCanonical)
      const {
    return parseTimestamp(
               text,
               result,
               defaultTimeZone_,
               defaultRawOffset_,
               allowCanonical) ||
        parseUnicodeTimestamp(
               text, result, defaultTimeZone_, defaultRawOffset_);
  }
};

template <typename T>
struct ToTimestampWithFormatFunction : LegacyTimestampContext {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  using DateTimeFormatterPtr = std::shared_ptr<DateTimeFormatter>;

  // Set only when the format argument is a constant (known at initialize time).
  DateTimeFormatterPtr constFormatter_;
  // One-entry memo so a non-constant format column does not rebuild the
  // formatter on every row when consecutive rows share the same format.
  std::string lastFormat_;
  DateTimeFormatterPtr lastFormatter_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*dateStr*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      constFormatter_ = buildJodaDateTimeFormatter(
          std::string_view(format->data(), format->size()));
    }
  }

  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* dateStr,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* zone,
      const arg_type<int32_t>* rawOffset) {
    initialize(inputTypes, config, dateStr, format);
    initializeDefaultTimeZone(zone, rawOffset);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& dateStr,
      const arg_type<Varchar>& format) {
    const DateTimeFormatter* fmt;
    if (constFormatter_ != nullptr) {
      fmt = constFormatter_.get();
    } else {
      const auto formatView = std::string_view(format.data(), format.size());
      if (lastFormatter_ == nullptr ||
          std::string_view(lastFormat_) != formatView) {
        lastFormatter_ = buildJodaDateTimeFormatter(formatView);
        lastFormat_.assign(formatView.data(), formatView.size());
      }
      fmt = lastFormatter_.get();
    }
    const auto input = std::string_view(dateStr.data(), dateStr.size());
    auto parsed = fmt->parse(input, TimePolicy::CORRECTED);
    if (parsed.hasError()) {
      return parse(input, result, /*allowCanonical=*/false);
    }
    result = parsed.value().timestamp;
    return true;
  }

  bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& text,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>&,
      const arg_type<int32_t>&) {
    return call(result, text, format);
  }
};

template <typename T>
struct StringToTimestampFunction : LegacyTimestampContext {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void initialize(
      const std::vector<TypePtr>&,
      const core::QueryConfig&,
      const arg_type<Varchar>*,
      const arg_type<bool>*,
      const arg_type<Varchar>* zone,
      const arg_type<int32_t>* rawOffset) {
    initializeDefaultTimeZone(zone, rawOffset);
  }

  bool call(out_type<Timestamp>& result, const arg_type<Varchar>& text) {
    return call(result, text, true);
  }

  bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& text,
      const arg_type<bool>& nullOnFailure) {
    const std::string_view input(text.data(), text.size());
    const bool valid = parse(input, result, /*allowCanonical=*/true);
    BOLT_USER_CHECK(valid || nullOnFailure, "Invalid timestamp literal");
    return valid;
  }

  bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& text,
      const arg_type<bool>& nullOnFailure,
      const arg_type<Varchar>&,
      const arg_type<int32_t>&) {
    return call(result, text, nullOnFailure);
  }
};

template <typename T, bool ToUtc>
struct TimestampZoneFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  const tz::TimeZone* zone_ = nullptr;
  int32_t offset_ = 0;

  void initialize(
      const std::vector<TypePtr>&,
      const core::QueryConfig&,
      const arg_type<Timestamp>*,
      const arg_type<Varchar>* zone) {
    BOLT_USER_CHECK_NOT_NULL(zone, "Session time zone must be constant");
    std::string_view name(zone->data(), zone->size());
    resolveTimeZone(name, zone_, offset_);
  }

  void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& input,
      const arg_type<Varchar>&) {
    if (zone_ == nullptr) {
      result = Timestamp(
          input.getSeconds() + (ToUtc ? -offset_ : offset_), input.getNanos());
      return;
    }
    result = input;
    if constexpr (ToUtc) {
      result.toGMT(*zone_, TimestampGapPolicy::kShiftForward);
    } else {
      result.toTimezone(*zone_);
    }
  }
};
template <typename T>
struct TimestampToUtcFunction : TimestampZoneFunction<T, true> {};
template <typename T>
struct TimestampFromUtcFunction : TimestampZoneFunction<T, false> {};

template <typename T>
struct TimeToTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  void call(out_type<Timestamp>& result, const arg_type<int32_t>& millis) {
    result = Timestamp::fromMillis(millis);
  }
};

template <typename T>
struct TimestampToTimeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  void call(out_type<int32_t>& result, const arg_type<Timestamp>& input) {
    // Flink's NTZ CAST uses Java remainder on epoch milliseconds.
    constexpr int64_t kMillisPerDay = util::kSecsPerDay * util::kMsecsPerSec;
    int64_t millis =
        (input.getSeconds() % util::kSecsPerDay) * util::kMsecsPerSec +
        static_cast<int64_t>(input.getNanos() / 1'000'000);
    if (input.getSeconds() < 0 && millis > 0) {
      millis -= kMillisPerDay;
    }
    result = millis;
  }
};

// LTZ casts extract local clock time rather than the NTZ epoch remainder.
template <typename T>
struct TimestampWithLocalZoneToTimeFunction : TimestampZoneFunction<T, false> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<int32_t>& result,
      const arg_type<Timestamp>& input,
      const arg_type<Varchar>& zone) {
    Timestamp local;
    TimestampZoneFunction<T, false>::call(local, input, zone);
    int64_t seconds = local.getSeconds() % util::kSecsPerDay;
    if (seconds < 0) {
      seconds += util::kSecsPerDay;
    }
    result = seconds * util::kMsecsPerSec + local.getNanos() / 1'000'000;
  }
};

} // namespace

void registerTemporalFunctions(const std::string& prefix) {
  registerFunction<StringToTimestampFunction, Timestamp, Varchar>(
      {prefix + "to_timestamp"});
  registerFunction<ToTimestampWithFormatFunction, Timestamp, Varchar, Varchar>(
      {prefix + "to_timestamp"});
  registerFunction<
      ToTimestampWithFormatFunction,
      Timestamp,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "to_timestamp"});
  registerFunction<StringToTimestampFunction, Timestamp, Varchar, bool>(
      {prefix + "flink_string_to_timestamp"});
  registerFunction<
      StringToTimestampFunction,
      Timestamp,
      Varchar,
      bool,
      Varchar,
      int32_t>({prefix + "flink_string_to_timestamp"});
  exec::registerStatefulVectorFunction(
      prefix + "flink_timestamp_precision",
      {exec::FunctionSignatureBuilder()
           .returnType("timestamp")
           .argumentType("timestamp")
           .argumentType("integer")
           .build()},
      makeTimestampPrecision);
  registerFunction<TimestampToUtcFunction, Timestamp, Timestamp, Varchar>(
      {prefix + "flink_timestamp_to_utc"});
  registerFunction<TimestampFromUtcFunction, Timestamp, Timestamp, Varchar>(
      {prefix + "flink_timestamp_from_utc"});
  registerFunction<TimeToTimestampFunction, Timestamp, int32_t>(
      {prefix + "flink_time_to_timestamp"});
  registerFunction<TimestampToTimeFunction, int32_t, Timestamp>(
      {prefix + "flink_timestamp_to_time"});
  registerFunction<
      TimestampWithLocalZoneToTimeFunction,
      int32_t,
      Timestamp,
      Varchar>({prefix + "flink_timestamp_to_time"});
}

} // namespace bytedance::bolt::functions::flinksql
