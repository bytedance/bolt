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
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/functions/lib/TimeUtils.h"
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

template <typename T, bool ToUtc, bool LaterOffset = false>
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
    if constexpr (ToUtc && LaterOffset) {
      const std::chrono::seconds seconds(input.getSeconds());
      tz::validateRange(tz::time_point<std::chrono::seconds>(seconds));
      // Calendar shifts gaps forward and chooses the later overlap offset.
      const auto local = zone_->correct_nonexistent_time(seconds);
      result = Timestamp(
          zone_->to_sys(local, tz::TimeZone::TChoose::kLatest).count(),
          input.getNanos());
    } else if constexpr (ToUtc) {
      result.toGMT(*zone_, TimestampGapPolicy::kShiftForward);
    } else {
      result.toTimezone(*zone_);
    }
  }
};
template <typename T>
struct TimestampToUtcFunction : TimestampZoneFunction<T, true> {};
template <typename T>
struct TimestampToUtcLaterOffsetFunction
    : TimestampZoneFunction<T, true, true> {};
template <typename T>
struct TimestampFromUtcFunction : TimestampZoneFunction<T, false> {};

template <typename T, typename In>
struct FloatingSecondsToTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(out_type<Timestamp>& result, const arg_type<In>& seconds) {
    const In millis = seconds * 1000;
    int64_t epochMillis;
    // Flink uses (long) (seconds * 1000), including Java's NaN and overflow
    // rules.
    if (std::isnan(millis)) {
      epochMillis = 0;
    } else if (millis >= std::numeric_limits<int64_t>::max()) {
      epochMillis = std::numeric_limits<int64_t>::max();
    } else if (millis <= std::numeric_limits<int64_t>::min()) {
      epochMillis = std::numeric_limits<int64_t>::min();
    } else {
      epochMillis = static_cast<int64_t>(millis);
    }
    result = Timestamp::fromMillis(epochMillis);
  }
};
template <typename T>
struct FloatSecondsToTimestampFunction
    : FloatingSecondsToTimestampFunction<T, float> {};
template <typename T>
struct DoubleSecondsToTimestampFunction
    : FloatingSecondsToTimestampFunction<T, double> {};

// Overflow builtins retain Java's low bits without C++ signed overflow.
template <typename To, typename From>
To narrowInteger(From value) {
  To result;
  __builtin_add_overflow(value, 0, &result);
  return result;
}

template <typename I>
I addWrapped(I left, I right) {
  I result;
  __builtin_add_overflow(left, right, &result);
  return result;
}

template <typename I>
I subtractWrapped(I left, I right) {
  I result;
  __builtin_sub_overflow(left, right, &result);
  return result;
}

template <typename T>
struct TimestampToSecondsFunction {
  template <typename Out>
  void call(Out& result, const Timestamp& timestamp) {
    const auto millis = timestamp.toMillis();
    if constexpr (std::is_floating_point_v<Out>) {
      // Java divides as double before narrowing to FLOAT.
      result = static_cast<Out>(millis / 1000.0);
    } else {
      result = narrowInteger<Out>(millis / 1000);
    }
  }
};

template <typename T>
struct IntegerSecondsToTimestampFunction {
  template <typename In>
  void call(Timestamp& result, const In& seconds) {
    int64_t millis;
    __builtin_mul_overflow(static_cast<int64_t>(seconds), 1000, &millis);
    result = Timestamp::fromMillis(millis);
  }
};

template <typename T>
struct TimestampToDateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(out_type<Date>& result, const arg_type<Timestamp>& timestamp) {
    result = narrowInteger<int32_t>(timestamp.toMillis() / kMillisInDay);
  }
};

// This millisecond UDF uses strict local-time resolution. SQL casts use the
// separate timestamp-to-UTC functions to select an overlap/gap policy.
template <typename T>
struct TimestampToLongFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* zone_ = nullptr;
  int32_t offset_ = 0;
  bool constantZone_ = false;

  void initialize(
      const std::vector<TypePtr>&,
      const core::QueryConfig& config,
      const arg_type<Timestamp>*) {
    if (config.adjustTimestampToTimezone()) {
      const auto name = config.sessionTimezone();
      if (!name.empty()) {
        resolveTimeZone(name, zone_, offset_);
      }
    }
  }

  void initialize(
      const std::vector<TypePtr>&,
      const core::QueryConfig&,
      const arg_type<Timestamp>*,
      const arg_type<Varchar>* zone) {
    if (zone != nullptr && !zone->empty()) {
      resolveTimeZone(
          std::string_view(zone->data(), zone->size()), zone_, offset_);
      constantZone_ = true;
    }
  }

  void call(out_type<int64_t>& result, const arg_type<Timestamp>& input) {
    result = toMillis(input, zone_, offset_);
  }

  void call(
      out_type<int64_t>& result,
      const arg_type<Timestamp>& input,
      const arg_type<Varchar>& zone) {
    const tz::TimeZone* timeZone = zone_;
    int32_t offset = offset_;
    if (!constantZone_) {
      timeZone = nullptr;
      offset = 0;
      resolveTimeZone(
          std::string_view(zone.data(), zone.size()), timeZone, offset);
    }
    result = toMillis(input, timeZone, offset);
  }

  static int64_t
  toMillis(const Timestamp& input, const tz::TimeZone* zone, int32_t offset) {
    if (zone != nullptr && zone->id() != 0) {
      if (const auto zoneOffset = zone->offset()) {
        offset = zoneOffset->count() * util::kSecsPerMinute;
      } else {
        const std::chrono::seconds seconds(input.getSeconds());
        return Timestamp(zone->to_sys(seconds).count(), input.getNanos())
            .toMillis();
      }
    }
    // UTC and fixed offsets skip the calendar lookup. The offset-adjusted
    // result must fit in int64 milliseconds.
    if (offset == 0) {
      return input.toMillis();
    }
    const __int128_t millis =
        static_cast<__int128_t>(input.getSeconds()) * 1'000 +
        input.getNanos() / 1'000'000 - static_cast<int64_t>(offset) * 1'000;
    BOLT_USER_CHECK(
        millis >= INT64_MIN && millis <= INT64_MAX,
        "Could not convert Timestamp({}, {}) to milliseconds",
        input.getSeconds(),
        input.getNanos());
    return static_cast<int64_t>(millis);
  }
};

template <typename T>
struct IntervalAddFunction {
  template <typename I>
  void call(I& result, const I& left, const I& right) {
    result = addWrapped(left, right);
  }
};

template <typename T>
struct IntervalSubtractFunction {
  template <typename I>
  void call(I& result, const I& left, const I& right) {
    result = subtractWrapped(left, right);
  }
};

template <typename T>
struct IntervalNegateFunction {
  template <typename I>
  void call(I& result, const I& value) {
    result = subtractWrapped(I{0}, value);
  }
};

// Calcite counts the digits without the sign when padding interval fields.
void appendIntervalNumber(std::string& result, int64_t value, int width) {
  int digits = 1;
  for (auto remaining = value / 10; remaining != 0; remaining /= 10) {
    ++digits;
  }
  result.append(std::max(0, width - digits), '0');
  result.append(std::to_string(value));
}

template <typename T>
struct IntervalYearMonthToStringFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<Varchar>& result,
      const arg_type<IntervalYearMonth>& input) {
    const auto value = input < 0 ? subtractWrapped(int32_t{0}, input) : input;
    std::string text = input < 0 ? "-" : "+";
    text += std::to_string(value / 12) + "-";
    appendIntervalNumber(text, value % 12, 2);
    result.copy_from(text);
  }
};

template <typename T>
struct IntervalDayTimeToStringFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(out_type<Varchar>& result, const arg_type<IntervalDayTime>& input) {
    const auto value = input < 0 ? subtractWrapped(int64_t{0}, input) : input;
    std::string text = input < 0 ? "-" : "+";
    // Flink's Calcite formatter narrows the day count to int32.
    text += std::to_string(narrowInteger<int32_t>(value / kMillisInDay)) + " ";
    appendIntervalNumber(text, value / 3'600'000 % 24, 2);
    text += ":";
    appendIntervalNumber(text, value / 60'000 % 60, 2);
    text += ":";
    appendIntervalNumber(text, value / 1000 % 60, 2);
    text += ".";
    appendIntervalNumber(text, value % 1000, 3);
    result.copy_from(text);
  }
};

// Flink and Calcite use truncating division and int32 narrowing, including
// civil dates before -4800-03-01.
int32_t localDateToUnixDate(const util::CivilDate& date) {
  const int32_t a = (14 - date.month) / 12;
  const int64_t y = static_cast<int64_t>(date.year) + 4800 - a;
  const int32_t m = date.month + 12 * a - 3;
  return narrowInteger<int32_t>(
      date.day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 -
      32'045 - 2'440'588);
}

// Calcite's Julian-day decomposition deliberately uses Java signed division
// and int32 addition. Keep these rules for dates outside the usual SQL range.
util::CivilDate intervalDate(int32_t days) {
  const int32_t shifted = addWrapped(days, int32_t{2'440'588 + 32'044});
  const int32_t eras = shifted / 146'097;
  const int32_t eraDays = shifted % 146'097;
  const int32_t centuries = ((eraDays / 36'524 + 1) * 3) / 4;
  const int32_t centuryDays = eraDays - centuries * 36'524;
  const int32_t cycles = centuryDays / 1461;
  const int32_t cycleDays = centuryDays % 1461;
  const int32_t yearsInCycle = ((cycleDays / 365 + 1) * 3) / 4;
  const int32_t yearDays = cycleDays - yearsInCycle * 365;
  const int32_t year = eras * 400 + centuries * 100 + cycles * 4 + yearsInCycle;
  const int32_t month = (yearDays * 5 + 308) / 153 - 2;
  const int32_t day = yearDays - (month + 4) * 153 / 5 + 123;
  return {year - 4800 + (month + 2) / 12, (month + 2) % 12 + 1, day};
}

int32_t addMonths(int32_t days, int32_t months) {
  const auto date = intervalDate(days);
  const int32_t month = addWrapped(date.month, months);
  const int32_t year = date.year + month / 12 - (month % 12 <= 0);
  const int32_t newMonth = (month % 12 + 11) % 12 + 1;
  const int32_t day =
      std::min(date.day, util::getMaxDayOfMonth(year, newMonth));
  return localDateToUnixDate({year, newMonth, day});
}

template <typename T, bool Subtract>
struct DateIntervalFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<Timestamp>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    const auto millis = static_cast<int64_t>(date) * kMillisInDay;
    result = Timestamp::fromMillis(
        Subtract ? subtractWrapped(millis, interval)
                 : addWrapped(millis, interval));
  }

  void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    const auto days = interval / kMillisInDay;
    BOLT_USER_CHECK(
        days >= std::numeric_limits<int32_t>::min() &&
            days <= std::numeric_limits<int32_t>::max(),
        "Day interval exceeds integer range");
    if constexpr (Subtract) {
      result = subtractWrapped(date, static_cast<int32_t>(days));
    } else {
      result = addWrapped(date, static_cast<int32_t>(days));
    }
  }

  void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    const auto months =
        Subtract ? subtractWrapped(int32_t{0}, interval) : interval;
    result = addMonths(date, months);
  }
};
template <typename T>
using DateAddIntervalFunction = DateIntervalFunction<T, false>;
template <typename T>
using DateSubtractIntervalFunction = DateIntervalFunction<T, true>;

template <typename T>
struct IntervalAddDateFunction {
  template <typename Out, typename I>
  void call(Out& result, const I& interval, const int32_t& date) {
    DateAddIntervalFunction<T>().call(result, date, interval);
  }
};

template <typename T, bool Subtract>
struct TimestampIntervalFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& input,
      const arg_type<IntervalDayTime>& interval) {
    const auto millis = Subtract ? subtractWrapped(input.toMillis(), interval)
                                 : addWrapped(input.toMillis(), interval);
    result = withMillis(input, millis);
  }

  void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& input,
      const arg_type<IntervalYearMonth>& interval) {
    const auto millis = input.toMillis();
    const auto remainder =
        (millis % kMillisInDay + kMillisInDay) % kMillisInDay;
    const auto days = narrowInteger<int32_t>(
        subtractWrapped(millis, remainder) / kMillisInDay);
    const auto months =
        Subtract ? subtractWrapped(int32_t{0}, interval) : interval;
    result = withMillis(
        input,
        static_cast<int64_t>(addMonths(days, months)) * kMillisInDay +
            remainder);
  }

  static Timestamp withMillis(const Timestamp& input, int64_t millis) {
    const auto result = Timestamp::fromMillis(millis);
    return Timestamp(
        result.getSeconds(), result.getNanos() + input.getNanos() % 1'000'000);
  }
};
template <typename T>
using TimestampAddIntervalFunction = TimestampIntervalFunction<T, false>;
template <typename T>
using TimestampSubtractIntervalFunction = TimestampIntervalFunction<T, true>;

template <typename T>
struct IntervalAddTimestampFunction {
  template <typename I>
  void call(Timestamp& result, const I& interval, const Timestamp& timestamp) {
    TimestampAddIntervalFunction<T>().call(result, timestamp, interval);
  }
};

template <typename Interval>
void registerIntervalFunctions(const std::string& prefix) {
  registerFunction<IntervalAddFunction, Interval, Interval, Interval>(
      {prefix + "add"});
  registerFunction<IntervalSubtractFunction, Interval, Interval, Interval>(
      {prefix + "subtract"});
  registerFunction<IntervalNegateFunction, Interval, Interval>(
      {prefix + "negative"});
  registerFunction<DateAddIntervalFunction, Date, Date, Interval>(
      {prefix + "add"});
  registerFunction<DateSubtractIntervalFunction, Date, Date, Interval>(
      {prefix + "subtract"});
  registerFunction<IntervalAddDateFunction, Date, Interval, Date>(
      {prefix + "add"});
  registerFunction<
      TimestampAddIntervalFunction,
      Timestamp,
      Timestamp,
      Interval>({prefix + "add"});
  registerFunction<
      TimestampSubtractIntervalFunction,
      Timestamp,
      Timestamp,
      Interval>({prefix + "subtract"});
  registerFunction<
      IntervalAddTimestampFunction,
      Timestamp,
      Interval,
      Timestamp>({prefix + "add"});
}

// The input has already been shifted into the session's local time zone.
template <typename T>
struct TimestampToLocalDateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(out_type<Date>& result, const arg_type<Timestamp>& timestamp) {
    // From -4800-03-01, Flink's shifted calendar year is non-negative and its
    // formula matches epoch-day flooring.
    constexpr int64_t kFirstMatchingEpochDay = -2'472'632;
    const auto days = timestamp.getDays();
    if (days >= kFirstMatchingEpochDay) {
      result = narrowInteger<int32_t>(days);
      return;
    }
    result = localDateToUnixDate(
        util::toCivilDateTime(timestamp, /*allowOverflow=*/true).date);
  }
};

template <typename T>
struct TimeExtractFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<int32_t>& result,
      const arg_type<int32_t>& millis,
      const arg_type<int32_t>& modulus,
      const arg_type<int32_t>& divisor) {
    BOLT_USER_CHECK_GT(modulus, 0);
    BOLT_USER_CHECK_GT(divisor, 0);
    // Flink TIME extraction retains the sign of its millisecond remainder.
    result = (millis % modulus) / divisor;
  }
};

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
  registerFunction<
      TimestampToUtcLaterOffsetFunction,
      Timestamp,
      Timestamp,
      Varchar>({prefix + "flink_timestamp_to_utc_later_offset"});
  registerFunction<TimestampFromUtcFunction, Timestamp, Timestamp, Varchar>(
      {prefix + "flink_timestamp_from_utc"});
  registerFunction<TimestampToLongFunction, int64_t, Timestamp>(
      {prefix + "timestamp_to_long"});
  registerFunction<TimestampToLongFunction, int64_t, Timestamp, Varchar>(
      {prefix + "timestamp_to_long"});
  registerFunction<TimestampToSecondsFunction, int64_t, Timestamp>(
      {prefix + "flink_timestamp_to_long_seconds"});
  registerFunction<TimestampToSecondsFunction, int32_t, Timestamp>(
      {prefix + "flink_timestamp_to_int_seconds"});
  registerFunction<TimestampToSecondsFunction, int16_t, Timestamp>(
      {prefix + "flink_timestamp_to_smallint_seconds"});
  registerFunction<TimestampToSecondsFunction, int8_t, Timestamp>(
      {prefix + "flink_timestamp_to_tinyint_seconds"});
  registerFunction<TimestampToSecondsFunction, float, Timestamp>(
      {prefix + "flink_timestamp_to_float_seconds"});
  registerFunction<TimestampToSecondsFunction, double, Timestamp>(
      {prefix + "flink_timestamp_to_double_seconds"});
  registerFunction<IntegerSecondsToTimestampFunction, Timestamp, int64_t>(
      {prefix + "flink_long_seconds_to_timestamp"});
  registerFunction<IntegerSecondsToTimestampFunction, Timestamp, int32_t>(
      {prefix + "flink_int_seconds_to_timestamp"});
  registerFunction<IntegerSecondsToTimestampFunction, Timestamp, int16_t>(
      {prefix + "flink_short_seconds_to_timestamp"});
  registerFunction<IntegerSecondsToTimestampFunction, Timestamp, int8_t>(
      {prefix + "flink_byte_seconds_to_timestamp"});
  registerFunction<TimestampToDateFunction, Date, Timestamp>(
      {prefix + "flink_timestamp_to_date"});
  registerFunction<
      IntervalYearMonthToStringFunction,
      Varchar,
      IntervalYearMonth>({prefix + "flink_interval_year_month_to_string"});
  registerFunction<IntervalDayTimeToStringFunction, Varchar, IntervalDayTime>(
      {prefix + "flink_interval_day_time_to_string"});
  registerIntervalFunctions<IntervalYearMonth>(prefix);
  registerIntervalFunctions<IntervalDayTime>(prefix);
  registerFunction<DateAddIntervalFunction, Timestamp, Date, IntervalDayTime>(
      {prefix + "flink_date_add_interval"});
  registerFunction<IntervalAddDateFunction, Timestamp, IntervalDayTime, Date>(
      {prefix + "flink_date_add_interval"});
  registerFunction<
      DateSubtractIntervalFunction,
      Timestamp,
      Date,
      IntervalDayTime>({prefix + "flink_date_subtract_interval"});
  registerFunction<FloatSecondsToTimestampFunction, Timestamp, float>(
      {prefix + "flink_float_seconds_to_timestamp"});
  registerFunction<DoubleSecondsToTimestampFunction, Timestamp, double>(
      {prefix + "flink_double_seconds_to_timestamp"});
  registerFunction<TimestampToLocalDateFunction, Date, Timestamp>(
      {prefix + "flink_timestamp_to_local_date"});
  registerFunction<TimeExtractFunction, int32_t, int32_t, int32_t, int32_t>(
      {prefix + "flink_time_extract"});
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
