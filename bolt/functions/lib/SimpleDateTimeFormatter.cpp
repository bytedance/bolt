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

#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/functions/lib/DateTimeFormatterInternal.h"
#include "bolt/functions/lib/DateTimePattern.h"

#include <date/tz.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <unicode/uchar.h>
#include <unicode/utf8.h>
#include <array>
#include <cmath>
#include <stdexcept>

#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneNames.h"

namespace bytedance::bolt::functions {
namespace detail {

icu::GregorianCalendar& simpleDateCalendar() {
  // Calendar fields are scratch state; one bounded instance per thread keeps
  // shared formatters immutable and avoids allocations while resolving dates.
  static thread_local auto calendar = [] {
    UErrorCode status = U_ZERO_ERROR;
    auto result = std::make_unique<icu::GregorianCalendar>(
        *icu::TimeZone::getGMT(), icu::Locale::getUS(), status);
    BOLT_CHECK(U_SUCCESS(status), "Cannot initialize Gregorian calendar");
    result->setFirstDayOfWeek(UCAL_SUNDAY);
    result->setMinimalDaysInFirstWeek(1);
    return result;
  }();
  return *calendar;
}

std::pair<int32_t, int32_t> simpleTimeZoneOffsets(
    const tz::TimeZone* zone,
    const DateTimeFormatterContext* context) {
  if (!zone) {
    return {0, 0};
  }
  if (const auto fixed = zone->offset()) {
    return {
        std::chrono::duration_cast<std::chrono::seconds>(*fixed).count(), 0};
  }
  struct ZoneOffsets {
    const tz::TimeZone* zone{nullptr};
    std::pair<int32_t, int32_t> offsets;
  };
  static thread_local std::array<ZoneOffsets, 16> cache;
  static thread_local size_t next = 0;
  auto cached =
      std::find_if(cache.begin(), cache.end(), [&](const auto& entry) {
        return entry.zone == zone;
      });
  if (cached == cache.end()) {
    std::unique_ptr<icu::TimeZone> javaZone(icu::TimeZone::createTimeZone(
        icu::UnicodeString::fromUTF8(zone->name())));
    cached = cache.begin() + next;
    cached->offsets = {
        javaZone->getRawOffset() / 1000, javaZone->getDSTSavings() / 1000};
    cached->zone = zone;
    next = (next + 1) % cache.size();
  }
  if (context && !context->timezoneRawOffsets.empty()) {
    const auto it = context->timezoneRawOffsets.find(zone->id());
    BOLT_USER_CHECK(
        it != context->timezoneRawOffsets.end(),
        "No Java raw offset for time zone {}",
        zone->name());
    return {it->second, cached->offsets.second};
  }
  return cached->offsets;
}

std::pair<int64_t, bool>
resolveLocalTime(int64_t seconds, const tz::TimeZone* zone, bool latest) {
  if (!zone || zone->id() == 0) {
    return {seconds, false};
  }
  if (const auto offset = zone->offset()) {
    return {seconds - offset->count() * 60, false};
  }
  struct ZoneInterval {
    const tz::TimeZone* zone{nullptr};
    int64_t begin{0};
    int64_t end{0};
    int64_t offset{0};
  };
  static thread_local std::array<ZoneInterval, 16> intervals;
  auto& interval = intervals[zone->id() % intervals.size()];
  if (interval.zone == zone && seconds >= interval.begin &&
      seconds < interval.end) {
    return {seconds - interval.offset, false};
  }
  const auto info = zone->tz()->get_info(
      ::date::local_seconds{std::chrono::seconds(seconds)});
  if (info.result == ::date::local_info::unique) {
    const auto& period = info.first;
    auto begin =
        period.begin.time_since_epoch().count() + period.offset.count();
    auto end = period.end.time_since_epoch().count() + period.offset.count();
    const ::date::sys_seconds min{
        ::date::sys_days{::date::year::min() / 1 / 1}};
    const ::date::sys_seconds max =
        ::date::sys_days{::date::year::max() / 12 / 31} + ::date::days{1};
    // Exclude overlaps at both boundaries; only unique local times are cached.
    if (period.begin > min) {
      const auto previous =
          zone->tz()->get_info(period.begin - std::chrono::seconds{1});
      begin = std::max(
          begin,
          period.begin.time_since_epoch().count() + previous.offset.count());
    }
    if (period.end < max) {
      const auto next = zone->tz()->get_info(period.end);
      end = std::min(
          end, period.end.time_since_epoch().count() + next.offset.count());
    }
    interval = {zone, begin, end, period.offset.count()};
    return {seconds - period.offset.count(), false};
  }
  const bool gap = info.result == ::date::local_info::nonexistent;
  const auto offset = !gap && latest ? info.second.offset : info.first.offset;
  return {seconds - offset.count(), gap};
}

Timestamp simpleDateTimeToUtc(
    Timestamp timestamp,
    const tz::TimeZone* zone,
    const DateTimeFormatterContext* context,
    bool* shifted) {
  if (shifted) {
    *shifted = false;
  }
  if (!zone || zone->id() == 0) {
    return timestamp;
  }
  const auto local = std::chrono::seconds(timestamp.getSeconds());
  if (zone->offset()) {
    return Timestamp(
        local.count() - zone->offset()->count() * 60, timestamp.getNanos());
  }
  constexpr int64_t start = -2'208'988'800;
  if (local.count() < start + 86'400 &&
      local < zone->to_local(std::chrono::seconds(start))) {
    return Timestamp(
        local.count() - simpleTimeZoneOffsets(zone, context).first,
        timestamp.getNanos());
  }
  const auto [utc, gap] = resolveLocalTime(local.count(), zone, true);
  if (shifted) {
    *shifted = gap;
  }
  return Timestamp(utc, timestamp.getNanos());
}

Timestamp simpleDateTimeFromUtc(
    Timestamp timestamp,
    const tz::TimeZone* zone,
    const DateTimeFormatterContext* context) {
  if (!zone || zone->id() == 0) {
    return timestamp;
  }
  if (zone->offset()) {
    return Timestamp(
        timestamp.getSeconds() + zone->offset()->count() * 60,
        timestamp.getNanos());
  }
  if (timestamp.getSeconds() < -2'208'988'800) {
    return Timestamp(
        timestamp.getSeconds() + simpleTimeZoneOffsets(zone, context).first,
        timestamp.getNanos());
  }
  timestamp.toTimezone(*zone);
  return timestamp;
}

Timestamp rebaseDateTime(
    Timestamp timestamp,
    const tz::TimeZone* zone,
    bool toGregorian,
    const DateTimeFormatterContext* context) {
  // Java's hybrid calendar and the proleptic Gregorian calendar agree for
  // modern instants, including their time zone rules.
  if (timestamp.getSeconds() >= -2'208'902'400) {
    return timestamp;
  }
  auto& calendar = simpleDateCalendar();
  calendar.setLenient(true);
  UErrorCode status = U_ZERO_ERROR;
  if (toGregorian) {
    auto local = simpleDateTimeFromUtc(timestamp, zone, context);
    calendar.setTime(local.toMillis(), status);
    int32_t year = calendar.get(UCAL_YEAR, status);
    if (calendar.get(UCAL_ERA, status) == 0) {
      year = 1 - year;
    }
    const auto days = util::daysSinceEpochFromDate(
                          year, calendar.get(UCAL_MONTH, status) + 1, 1) +
        calendar.get(UCAL_DATE, status) - 1;
    const auto seconds = days * util::kSecsPerDay +
        calendar.get(UCAL_HOUR_OF_DAY, status) * 3600 +
        calendar.get(UCAL_MINUTE, status) * 60 +
        calendar.get(UCAL_SECOND, status);
    BOLT_USER_CHECK(U_SUCCESS(status), "Date outside supported calendar range");
    auto result = Timestamp(seconds, timestamp.getNanos());
    if (zone) {
      const auto adjusted = zone->correct_nonexistent_time(
          std::chrono::seconds(result.getSeconds()));
      result = Timestamp(adjusted.count(), result.getNanos());
      result.toGMT(*zone);
    }
    return result;
  }
  auto local = timestamp;
  if (zone) {
    local.toTimezone(*zone);
  }
  auto civil = util::toCivilDateTime(local);
  if (civil.date.year == 1582 && civil.date.month == 10 &&
      civil.date.day >= 5 && civil.date.day <= 14) {
    civil.date.day = 15;
  }
  calendar.clear();
  calendar.set(UCAL_ERA, civil.date.year > 0 ? 1 : 0);
  calendar.set(
      UCAL_YEAR, civil.date.year > 0 ? civil.date.year : 1 - civil.date.year);
  calendar.set(UCAL_MONTH, civil.date.month - 1);
  calendar.set(UCAL_DATE, civil.date.day);
  calendar.set(UCAL_HOUR_OF_DAY, civil.time.hour);
  calendar.set(UCAL_MINUTE, civil.time.minute);
  calendar.set(UCAL_SECOND, civil.time.second);
  const auto millis = calendar.getTime(status);
  BOLT_USER_CHECK(U_SUCCESS(status), "Date outside supported calendar range");
  return simpleDateTimeToUtc(
      Timestamp(
          Timestamp::fromMillis(millis).getSeconds(), timestamp.getNanos()),
      zone,
      context);
}

} // namespace detail

namespace {

void appendPattern(
    DateTimeFormatterBuilder& builder,
    char letter,
    size_t count) {
  BOLT_USER_CHECK(
      std::string_view("GyYMLwWDdFEuaHkKhmsSzZX").find(letter) !=
          std::string_view::npos,
      "Illegal pattern character: {}",
      letter);
  switch (letter) {
    case 'Y':
      builder.appendWeekYear(count);
      break;
    case 'W':
      builder.appendWeekOfMonth(count);
      break;
    case 'F':
      builder.appendDayOfWeekInMonth(count);
      break;
    case 'L':
      detail::appendCommonDateTimePattern(builder, 'M', count);
      break;
    case 'a':
      builder.appendHalfDayOfDay();
      break;
    case 'X':
      BOLT_USER_CHECK_LE(count, 3, "Invalid ISO 8601 pattern length");
      builder.appendISOTimeZoneOffsetId(count, true);
      break;
    default:
      detail::appendCommonDateTimePattern(builder, letter, count);
  }
}

using ErrorCode = DateTimeResult::ErrorCode;

const std::shared_ptr<const DateTimeFormatterContext>&
defaultFormatterContext() {
  static const std::shared_ptr<const DateTimeFormatterContext> context = [] {
    std::unique_ptr<icu::TimeZone> zone(icu::TimeZone::createDefault());
    icu::UnicodeString id;
    if (zone) {
      zone->getID(id);
    }
    std::string name;
    id.toUTF8String(name);
    if (name.starts_with("GMT+") || name.starts_with("GMT-")) {
      name.erase(0, 3);
    }
    auto result = std::make_shared<DateTimeFormatterContext>();
    result->defaultTimezone = tz::locateZone(name, false);
    if (!result->defaultTimezone) {
      result->defaultTimezone = tz::locateZone(0);
    }
    return result;
  }();
  return context;
}

bool startsWithIgnoreCase(std::string_view text, std::string_view prefix) {
  return text.size() >= prefix.size() &&
      folly::StringPiece(text.data(), prefix.size())
          .equals(prefix, folly::AsciiCaseInsensitive());
}

bool isSimpleNumeric(DateTimeFormatSpecifier specifier) {
  return detail::simpleCalendarField(specifier) >= 0 &&
      specifier != DateTimeFormatSpecifier::ERA &&
      specifier != DateTimeFormatSpecifier::HALFDAY_OF_DAY &&
      specifier != DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT &&
      specifier != DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT;
}

bool parseSimpleTimezone(
    std::string_view& text,
    FormatPattern pattern,
    const tz::TimeZone* timezone,
    int32_t& offset,
    const DateTimeFormatterContext* context) {
  const bool iso =
      pattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601;
  if (text.empty()) {
    return false;
  }
  if (iso && text.front() == 'Z') {
    offset = 0;
    text.remove_prefix(1);
    return true;
  }
  bool gmt = !iso && startsWithIgnoreCase(text, "GMT");
  if (gmt) {
    text.remove_prefix(3);
    if (text.empty() || (text.front() != '+' && text.front() != '-')) {
      offset = 0;
      return true;
    }
  }
  if (text.front() == '+' || text.front() == '-') {
    const int sign = text.front() == '-' ? -1 : 1;
    size_t pos = 1;
    auto digit = [&](int32_t& value) {
      if (pos == text.size() || text[pos] < '0' || text[pos] > '9') {
        return false;
      }
      value = value * 10 + text[pos++] - '0';
      return true;
    };
    int32_t hours = 0, minutes = 0;
    if (!digit(hours)) {
      return false;
    }
    if (!gmt || (pos < text.size() && text[pos] != ':')) {
      if (!digit(hours)) {
        return false;
      }
    }
    if (gmt || !iso || pattern.minRepresentDigits != 1) {
      if (gmt || (iso && pattern.minRepresentDigits == 3)) {
        if (pos == text.size() || text[pos++] != ':') {
          return false;
        }
      }
      if (!digit(minutes) || !digit(minutes)) {
        return false;
      }
    }
    if (hours > 23 || minutes > 59) {
      return false;
    }
    offset = sign * (hours * 3600 + minutes * 60);
    text.remove_prefix(pos);
    return true;
  }
  if (gmt || iso || static_cast<unsigned char>(text.front()) > 127) {
    return false;
  }
  auto matchZone = [&](const tz::TimeZone* zone) {
    if (!zone) {
      return false;
    }
    const auto& names = tz::getTimeZoneNames();
    const auto it = names.find(zone->name());
    if (it == names.end()) {
      return false;
    }
    const auto& namesForZone = it->second;
    const std::pair<std::string_view, bool> candidates[] = {
        {namesForZone.standardTimeLongName, false},
        {namesForZone.daylightTimeLongName, true},
        {namesForZone.standardTimeAbbreviation, false},
        {namesForZone.daylightTimeAbbreviation, true}};
    for (const auto& [name, daylight] : candidates) {
      if (startsWithIgnoreCase(text, name)) {
        const auto [raw, saving] = detail::simpleTimeZoneOffsets(zone, context);
        offset = raw + (daylight ? saving : 0);
        text.remove_prefix(name.size());
        return true;
      }
    }
    return false;
  };
  if (matchZone(timezone) ||
      (context && context->defaultTimezone != timezone &&
       matchZone(context->defaultTimezone))) {
    return true;
  }
  static const auto names = [] {
    folly::F14FastMap<std::string, std::pair<const tz::TimeZone*, bool>> result;
    const auto& zones = tz::getTimeZoneNames();
    auto add = [&](const std::string& id) {
      const auto it = zones.find(id);
      if (it == zones.end()) {
        return;
      }
      const auto* zone = tz::locateZone(id, false);
      if (!zone) {
        return;
      }
      const auto& names = it->second;
      auto put = [&](std::string name, bool daylight) {
        folly::toLowerAscii(name);
        result.emplace(std::move(name), std::pair{zone, daylight});
      };
      put(names.standardTimeAbbreviation, false);
      put(names.standardTimeLongName, false);
      put(names.daylightTimeAbbreviation, true);
      put(names.daylightTimeLongName, true);
    };
    // Java's English symbols prefer these zones for ambiguous abbreviations.
    for (const auto* id :
         {"America/Los_Angeles",
          "America/Denver",
          "America/Chicago",
          "America/New_York",
          "Pacific/Honolulu",
          "America/Anchorage",
          "America/Halifax",
          "America/St_Johns",
          "Europe/Paris",
          "GMT",
          "Asia/Jerusalem",
          "Asia/Tokyo",
          "Europe/Bucharest",
          "Asia/Shanghai",
          "UTC"}) {
      add(id);
    }
    std::vector<std::string> ids;
    for (const auto& [id, names] : zones) {
      ids.push_back(id);
    }
    std::sort(ids.begin(), ids.end());
    for (const auto& id : ids) {
      add(id);
    }
    return result;
  }();
  std::array<char, 64> lower;
  const auto length = std::min(text.size(), lower.size());
  for (size_t i = 0; i < length; ++i) {
    lower[i] =
        text[i] >= 'A' && text[i] <= 'Z' ? text[i] + ('a' - 'A') : text[i];
  }
  for (size_t size = length; size > 0; --size) {
    auto it = names.find(std::string_view(lower.data(), size));
    if (it != names.end()) {
      const auto [raw, saving] =
          detail::simpleTimeZoneOffsets(it->second.first, context);
      offset = raw + (it->second.second ? saving : 0);
      text.remove_prefix(size);
      return true;
    }
  }
  return false;
}

} // namespace

DateTimeResult DateTimeFormatter::parseSimpleDateFormat(
    const std::string_view& input,
    const tz::TimeZone* timezone) const {
  std::array<int32_t, UCAL_FIELD_COUNT> fields{};
  std::array<size_t, UCAL_FIELD_COUNT> stamps{};
  fields[UCAL_YEAR] = 1970;
  fields[UCAL_DATE] = 1;
  fields[UCAL_ERA] = 1;
  std::string_view text = input;
  std::optional<int32_t> offset;
  int32_t ambiguousYear = -1;
  for (size_t index = 0; index < tokens_.size(); ++index) {
    const auto& token = tokens_[index];
    if (token.type == DateTimeToken::Type::kLiteral) {
      if (!text.starts_with(token.literal)) {
        return ErrorCode::PARSE_LITERAL_ERROR;
      }
      text.remove_prefix(token.literal.size());
      continue;
    }
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
      text.remove_prefix(1);
    }
    const auto specifier = token.pattern.specifier;
    const auto field = detail::simpleCalendarField(specifier);
    if (field < 0) {
      int32_t seconds;
      if (!parseSimpleTimezone(
              text, token.pattern, timezone, seconds, context_.get())) {
        return ErrorCode::PARSE_TIMEZONE_ERROR;
      }
      offset = seconds;
      continue;
    }
    int64_t value = 0;
    if (isSimpleNumeric(specifier)) {
      const bool adjacent = index + 1 < tokens_.size() &&
          tokens_[index + 1].type == DateTimeToken::Type::kPattern &&
          isSimpleNumeric(tokens_[index + 1].pattern.specifier);
      size_t width = adjacent ? token.pattern.minRepresentDigits : text.size();
      const bool negative = !text.empty() && text.front() == '-';
      if (negative) {
        text.remove_prefix(1);
        if (width == 0) {
          return ErrorCode::PARSE_DIGIT_ERROR;
        }
        --width;
      }
      size_t digits = 0;
      while (!text.empty() && digits < width) {
        int32_t length = 1;
        int32_t digit = static_cast<unsigned char>(text.front()) - '0';
        if (static_cast<unsigned char>(text.front()) >= 128) {
          int32_t pos = 0;
          UChar32 codePoint;
          U8_NEXT(
              text.data(), pos, std::min<size_t>(text.size(), 4), codePoint);
          if (codePoint < 0 || codePoint > 0xffff) {
            break;
          }
          UVersionInfo age;
          u_charAge(codePoint, age);
          digit = age[0] < 6 || (age[0] == 6 && age[1] <= 2)
              ? u_digit(codePoint, 10)
              : -1;
          length = pos;
        }
        if (digit < 0 || digit > 9) {
          break;
        }
        value = value * 10 + digit;
        if (value > int64_t{std::numeric_limits<int32_t>::max()} + negative) {
          return ErrorCode::PARSE_DIGIT_ERROR;
        }
        text.remove_prefix(length);
        ++digits;
      }
      if (digits == 0) {
        return ErrorCode::PARSE_DIGIT_ERROR;
      }
      if (negative) {
        value = -value;
      }
      if (field == UCAL_YEAR || field == UCAL_YEAR_WOY) {
        ambiguousYear = -1;
        if (!negative && token.pattern.minRepresentDigits <= 2 && digits == 2) {
          const auto startYear = twoDigitYearStartYear_;
          value += (startYear / 100) * 100;
          if (value < startYear) {
            value += 100;
          } else if (value == startYear) {
            ambiguousYear = field;
          }
        }
      } else if (field == UCAL_MONTH) {
        --value;
      } else if (field == UCAL_DAY_OF_WEEK && value >= 1 && value <= 7) {
        value = value % 7 + 1;
      } else if (specifier == DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY) {
        if (parseOptions_.strict && (value < 1 || value > 24)) {
          return ErrorCode::HOUR_OUT_OF_RANGE;
        }
        if (value == 24) {
          value = 0;
        }
      } else if (specifier == DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY) {
        if (parseOptions_.strict && (value < 1 || value > 12)) {
          return ErrorCode::HOUR_OUT_OF_RANGE;
        }
        if (value == 12) {
          value = 0;
        }
      }
    } else {
      auto match = [&](const auto& names) {
        for (int32_t i = 0; i < std::size(names); ++i) {
          if (startsWithIgnoreCase(text, names[i])) {
            text.remove_prefix(names[i].size());
            value = i;
            return true;
          }
        }
        return false;
      };
      bool matched = false;
      if (field == UCAL_MONTH) {
        matched = match(detail::monthsFull) || match(detail::monthsShort);
      } else if (field == UCAL_DAY_OF_WEEK) {
        matched = match(detail::weekdaysFull) || match(detail::weekdaysShort);
        ++value;
      } else if (field == UCAL_AM_PM) {
        constexpr std::string_view names[] = {"AM", "PM"};
        matched = match(names);
      } else if (field == UCAL_ERA) {
        constexpr std::string_view names[] = {"BC", "AD"};
        matched = match(names);
      }
      if (!matched) {
        return ErrorCode::PARSE_DIGIT_ERROR;
      }
    }
    if (value < std::numeric_limits<int32_t>::min()) {
      return ErrorCode::PARSE_DIGIT_ERROR;
    }
    fields[field] = value;
    stamps[field] = index + 1;
  }
  if (text.size() == input.size()) {
    return ErrorCode::PARSE_DIGIT_ERROR;
  }
  int64_t millis;
  for (int attempt = 0; attempt < 2; ++attempt) {
    if (fields[UCAL_ERA] == 1 && fields[UCAL_YEAR] >= 1583 &&
        fields[UCAL_YEAR] <= util::kMaxYear && fields[UCAL_MONTH] >= 0 &&
        fields[UCAL_MONTH] < 12 &&
        util::isValidDate(
            fields[UCAL_YEAR], fields[UCAL_MONTH] + 1, fields[UCAL_DATE]) &&
        !stamps[UCAL_YEAR_WOY] && !stamps[UCAL_WEEK_OF_YEAR] &&
        !stamps[UCAL_WEEK_OF_MONTH] && !stamps[UCAL_DAY_OF_WEEK_IN_MONTH] &&
        !stamps[UCAL_DAY_OF_WEEK] && !stamps[UCAL_DAY_OF_YEAR] &&
        !stamps[UCAL_HOUR] && !stamps[UCAL_AM_PM] &&
        (!parseOptions_.strict ||
         (fields[UCAL_HOUR_OF_DAY] >= 0 && fields[UCAL_HOUR_OF_DAY] <= 23 &&
          fields[UCAL_MINUTE] >= 0 && fields[UCAL_MINUTE] <= 59 &&
          fields[UCAL_SECOND] >= 0 && fields[UCAL_SECOND] <= 59 &&
          fields[UCAL_MILLISECOND] >= 0 && fields[UCAL_MILLISECOND] <= 999))) {
      const __int128_t value =
          __int128_t{util::daysSinceEpochFromDate(
              fields[UCAL_YEAR], fields[UCAL_MONTH] + 1, fields[UCAL_DATE])} *
              86'400'000 +
          int64_t{fields[UCAL_HOUR_OF_DAY]} * 3'600'000 +
          int64_t{fields[UCAL_MINUTE]} * 60'000 +
          int64_t{fields[UCAL_SECOND]} * 1000 + fields[UCAL_MILLISECOND];
      if (value < std::numeric_limits<int64_t>::min() ||
          value > std::numeric_limits<int64_t>::max()) {
        return ErrorCode::YEAR_OUT_OF_RANGE;
      }
      millis = value;
    } else {
      auto calendarStamps = stamps;
      auto& calendar = detail::simpleDateCalendar();
      calendar.setLenient(!parseOptions_.strict);
      calendar.clear();
      const bool weekYear =
          calendarStamps[UCAL_YEAR_WOY] > calendarStamps[UCAL_YEAR];
      auto rank = [&](int32_t field, bool complete) {
        const auto stamp = calendarStamps[field];
        const bool needsWeekday = field == UCAL_WEEK_OF_YEAR ||
            field == UCAL_WEEK_OF_MONTH || field == UCAL_DAY_OF_WEEK_IN_MONTH;
        const auto score = needsWeekday
            ? (complete
                   ? (stamp && calendarStamps[UCAL_DAY_OF_WEEK]
                          ? std::max(stamp, calendarStamps[UCAL_DAY_OF_WEEK])
                          : 0)
                   : (field == UCAL_DAY_OF_WEEK_IN_MONTH
                          ? std::max(stamp, calendarStamps[UCAL_DAY_OF_WEEK])
                          : stamp))
            : stamp;
        return std::pair(score, stamp);
      };
      int32_t selected = UCAL_DATE;
      for (bool complete : {true, false}) {
        for (auto field :
             {UCAL_DATE,
              UCAL_WEEK_OF_MONTH,
              UCAL_DAY_OF_WEEK_IN_MONTH,
              UCAL_DAY_OF_YEAR,
              UCAL_WEEK_OF_YEAR}) {
          if (rank(field, complete) > rank(selected, complete)) {
            selected = field;
          }
        }
        if (rank(selected, complete).first) {
          break;
        }
      }
      const bool weekDate = weekYear || selected == UCAL_WEEK_OF_YEAR;
      if (calendarStamps[UCAL_HOUR_OF_DAY] &&
          (!calendarStamps[UCAL_HOUR] || !calendarStamps[UCAL_AM_PM] ||
           std::max(calendarStamps[UCAL_HOUR], calendarStamps[UCAL_AM_PM]) <
               calendarStamps[UCAL_HOUR_OF_DAY])) {
        calendarStamps[UCAL_HOUR] = calendarStamps[UCAL_AM_PM] = 0;
      } else if (calendarStamps[UCAL_HOUR] || calendarStamps[UCAL_AM_PM]) {
        calendarStamps[UCAL_HOUR_OF_DAY] = 0;
      }
      auto setFields = [&] {
        for (int32_t field = 0; field < UCAL_FIELD_COUNT; ++field) {
          if (!calendarStamps[field] || field == UCAL_YEAR_WOY) {
            continue;
          }
          if ((field == UCAL_DATE || field == UCAL_DAY_OF_YEAR ||
               field == UCAL_WEEK_OF_YEAR || field == UCAL_WEEK_OF_MONTH ||
               field == UCAL_DAY_OF_WEEK_IN_MONTH) &&
              field != selected) {
            continue;
          }
          if ((field == UCAL_DAY_OF_WEEK &&
               (selected == UCAL_DATE || selected == UCAL_DAY_OF_YEAR)) ||
              (field == UCAL_MONTH &&
               (selected == UCAL_DAY_OF_YEAR ||
                selected == UCAL_WEEK_OF_YEAR))) {
            continue;
          }
          calendar.set(static_cast<UCalendarDateFields>(field), fields[field]);
        }
      };
      if (weekDate) {
        const int32_t year = fields[weekYear ? UCAL_YEAR_WOY : UCAL_YEAR];
        int64_t week =
            calendarStamps[UCAL_WEEK_OF_YEAR] ? fields[UCAL_WEEK_OF_YEAR] : 1;
        int64_t day =
            calendarStamps[UCAL_DAY_OF_WEEK] ? fields[UCAL_DAY_OF_WEEK] : 1;
        if (weekYear && (day < 1 || day > 7)) {
          int64_t normalized = (day - 1) % 7;
          if (normalized < 0) {
            normalized += 7;
          }
          week += (day - 1 - normalized) / 7;
          day = (normalized + 1) % 7 + 1;
        }
        calendar.set(UCAL_YEAR, year);
        int32_t era = fields[UCAL_ERA];
        if (weekYear) {
          calendar.clear();
          calendar.set(UCAL_ERA, fields[UCAL_ERA]);
          calendar.set(UCAL_YEAR, fields[UCAL_YEAR]);
          setFields();
          UErrorCode status = U_ZERO_ERROR;
          era = calendar.get(UCAL_ERA, status);
          if (U_FAILURE(status)) {
            return ErrorCode::YEAR_OUT_OF_RANGE;
          }
          calendar.clear();
          calendar.set(UCAL_YEAR, year);
        }
        calendar.set(UCAL_ERA, era);
        calendar.set(UCAL_MONTH, 0);
        calendar.set(UCAL_DATE, 1);
        UErrorCode status = U_ZERO_ERROR;
        const auto first = calendar.getTime(status);
        const int32_t weekday = calendar.get(UCAL_DAY_OF_WEEK, status);
        if (U_FAILURE(status)) {
          return ErrorCode::YEAR_OUT_OF_RANGE;
        }
        const int64_t days =
            (week - 1) * 7 + ((day - 1) % 7 + 7) % 7 - (weekday - 1);
        calendar.setTime(first + days * 86'400'000, status);
        for (auto field :
             {UCAL_YEAR,
              UCAL_YEAR_WOY,
              UCAL_MONTH,
              UCAL_DATE,
              UCAL_DAY_OF_YEAR,
              UCAL_WEEK_OF_YEAR,
              UCAL_WEEK_OF_MONTH,
              UCAL_DAY_OF_WEEK,
              UCAL_DAY_OF_WEEK_IN_MONTH,
              UCAL_ERA}) {
          calendarStamps[field] = 0;
        }
      }
      setFields();
      UErrorCode status = U_ZERO_ERROR;
      const auto resolved = calendar.getTime(status);
      if (U_FAILURE(status)) {
        return ErrorCode::DAY_OUT_OF_RANGE;
      }
      if (parseOptions_.strict) {
        for (int32_t field = 0; field < UCAL_FIELD_COUNT; ++field) {
          if (weekYear && field < UCAL_AM_PM && field != UCAL_WEEK_OF_YEAR &&
              field != UCAL_DAY_OF_WEEK) {
            continue;
          }
          if (stamps[field] &&
              calendar.get(static_cast<UCalendarDateFields>(field), status) !=
                  fields[field]) {
            return ErrorCode::DAY_CONFLICT;
          }
        }
      }
      millis = resolved;
    }
    if (ambiguousYear < 0 || attempt == 1) {
      break;
    }
    const auto resolvedMillis = offset
        ? millis - int64_t{*offset} * 1000
        : detail::simpleDateTimeToUtc(
              Timestamp::fromMillis(millis), timezone, context_.get())
              .toMillis();
    if (resolvedMillis >= twoDigitYearStartMillis_) {
      break;
    }
    fields[UCAL_YEAR] += 100;
    fields[UCAL_YEAR_WOY] += 100;
  }
  return DateTimeResult{
      Timestamp::fromMillis(millis),
      offset && std::abs(*offset) <= 14 * 3600 ? tz::getTimeZoneID(*offset / 60)
                                               : -1,
      offset};
}

std::shared_ptr<DateTimeFormatter> buildSimpleDateTimeFormatter(
    const std::string_view& format,
    std::optional<Timestamp> twoDigitYearStart,
    bool lenient,
    std::shared_ptr<const DateTimeFormatterContext> context) {
  auto formatter = detail::buildPatternDateTimeFormatter<appendPattern>(
      format, DateTimeFormatterType::SIMPLE_DATE_FORMAT);
  formatter->parseOptions_.strict = !lenient;
  bool needsCenturyWindow = false;
  bool needsDefaultTimezone = false;
  for (const auto& token : formatter->tokens_) {
    if (token.type != DateTimeToken::Type::kPattern) {
      continue;
    }
    const auto specifier = token.pattern.specifier;
    needsDefaultTimezone |= specifier == DateTimeFormatSpecifier::TIMEZONE ||
        specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID;
    if (token.pattern.minRepresentDigits <= 2 &&
        (specifier == DateTimeFormatSpecifier::YEAR ||
         specifier == DateTimeFormatSpecifier::WEEK_YEAR)) {
      needsCenturyWindow = true;
      break;
    }
  }
  if (needsCenturyWindow || needsDefaultTimezone) {
    if (!context) {
      context = defaultFormatterContext();
    } else if (!context->defaultTimezone) {
      auto resolved = std::make_shared<DateTimeFormatterContext>(*context);
      resolved->defaultTimezone = defaultFormatterContext()->defaultTimezone;
      context = std::move(resolved);
    }
  }
  formatter->context_ = std::move(context);
  if (!needsCenturyWindow) {
    return formatter;
  }
  const auto* contextPtr = formatter->context_.get();
  if (!twoDigitYearStart && contextPtr) {
    twoDigitYearStart = contextPtr->twoDigitYearStart;
  }
  if (twoDigitYearStart) {
    formatter->twoDigitYearStartMillis_ = twoDigitYearStart->toMillis();
  } else {
    auto& calendar = detail::simpleDateCalendar();
    calendar.setLenient(true);
    UErrorCode status = U_ZERO_ERROR;
    auto now = Timestamp::now();
    if (contextPtr && contextPtr->defaultTimezone) {
      now = detail::simpleDateTimeFromUtc(
          now, contextPtr->defaultTimezone, contextPtr);
    }
    calendar.setTime(now.toMillis(), status);
    calendar.add(UCAL_YEAR, -80, status);
    const auto start = calendar.getTime(status);
    BOLT_CHECK(U_SUCCESS(status), "Cannot initialize two-digit year window");
    auto startTimestamp = Timestamp::fromMillis(start);
    if (contextPtr && contextPtr->defaultTimezone) {
      startTimestamp = detail::simpleDateTimeToUtc(
          startTimestamp, contextPtr->defaultTimezone, contextPtr);
    }
    formatter->twoDigitYearStartMillis_ = startTimestamp.toMillis();
  }
  if (contextPtr && contextPtr->twoDigitYearStartYear) {
    formatter->twoDigitYearStartYear_ = *contextPtr->twoDigitYearStartYear;
  } else {
    auto start = Timestamp::fromMillis(formatter->twoDigitYearStartMillis_);
    if (contextPtr && contextPtr->defaultTimezone) {
      start = detail::simpleDateTimeFromUtc(
          start, contextPtr->defaultTimezone, contextPtr);
    }
    auto& calendar = detail::simpleDateCalendar();
    UErrorCode status = U_ZERO_ERROR;
    calendar.setTime(start.toMillis(), status);
    formatter->twoDigitYearStartYear_ = calendar.get(UCAL_YEAR, status);
    BOLT_USER_CHECK(U_SUCCESS(status), "Invalid two-digit year window");
  }
  return formatter;
}

} // namespace bytedance::bolt::functions
