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
#include "bolt/type/tz/TimeZoneMap.h"
#include <common/process/StackTrace.h>
#include <date/tz.h>

#include <boost/algorithm/string.hpp>
#include <date/date.h>
#include <fmt/core.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <algorithm>
#include <array>
#include <exception>
#include <iterator>
#include <limits>
#include <sstream>
#include <unordered_map>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/type/tz/SystemVTimeZone.h"
#include "bolt/type/tz/TimeZoneNames.h"

using bytedance::bolt::common::testutil::TestValue;

namespace bytedance::bolt::tz {

namespace detail {

// Java retains the obsolete tzdb SystemV zones even on systems whose zoneinfo
// no longer ships them. These rules are independent of modern America/* zones.
struct SystemVTimeZone {
  const char* name;
  int32_t offsetHours;
  bool hasDaylightSaving;

  ::date::sys_seconds transition(int year, bool daylight) const {
    using namespace ::date;
    sys_days day;
    if (daylight && year == 1974) {
      day = sys_days{::date::year{year} / January / 6};
    } else if (daylight && year == 1975) {
      day = sys_days{::date::year{year} / February / 23};
    } else if (!daylight && year == 1974) {
      day = sys_days{::date::year{year} / November / 24};
    } else {
      day = sys_days{
          ::date::year{year} / (daylight ? April : October) / Sunday[last]};
    }
    // Both transitions occur at 02:00 wall time, using the preceding offset.
    return day + std::chrono::hours{2 - offsetHours - (daylight ? 0 : 1)};
  }

  ::date::sys_info getInfo(::date::sys_seconds timestamp) const {
    using namespace ::date;
    const auto standardOffset = std::chrono::hours{offsetHours};
    // Offset arithmetic on these endpoints stays inside int64 seconds while
    // covering the signed millisecond range accepted by TimeZone.
    sys_seconds begin{std::chrono::seconds{
        std::numeric_limits<int64_t>::min() / 1000 - 86400}};
    sys_seconds end{std::chrono::seconds{
        std::numeric_limits<int64_t>::max() / 1000 + 86400}};
    bool daylight = false;
    if (hasDaylightSaving) {
      const auto firstTransition = transition(1900, true);
      if (timestamp < firstTransition) {
        end = firstTransition;
      } else {
        // date::days uses a 32-bit count. Gregorian calendars and these DST
        // rules repeat every 400 years after the historical exceptions.
        static constexpr sys_seconds kCycleStart{
            sys_days{::date::year{2000} / January / 1}};
        static constexpr std::chrono::seconds kCycle{146097LL * 86400};
        const auto elapsed = timestamp + standardOffset - kCycleStart;
        if (elapsed >= kCycle) {
          const auto shift = kCycle * (elapsed / kCycle);
          auto info = getInfo(timestamp - shift);
          auto shiftBoundary = [shift](sys_seconds boundary) {
            int64_t result;
            BOLT_USER_CHECK(
                !__builtin_add_overflow(
                    boundary.time_since_epoch().count(),
                    shift.count(),
                    &result),
                "SystemV time zone interval is outside the supported seconds range");
            return sys_seconds{std::chrono::seconds{result}};
          };
          info.begin = shiftBoundary(info.begin);
          info.end = shiftBoundary(info.end);
          return info;
        }
        const int year =
            int(year_month_day{floor<days>(timestamp + standardOffset)}.year());
        const auto start = transition(year, true);
        const auto stop = transition(year, false);
        if (timestamp < start) {
          if (year > 1900) {
            begin = transition(year - 1, false);
          }
          end = start;
        } else if (timestamp < stop) {
          begin = start;
          end = stop;
          daylight = true;
        } else {
          begin = stop;
          end = transition(year + 1, true);
        }
      }
    }
    const auto& names = getTimeZoneNames().at(name);
    return {
        begin,
        end,
        standardOffset + std::chrono::hours{daylight ? 1 : 0},
        std::chrono::minutes{daylight ? 60 : 0},
        daylight ? names.daylightTimeAbbreviation
                 : names.standardTimeAbbreviation};
  }

  ::date::local_info getInfo(::date::local_seconds timestamp) const {
    using namespace ::date;
    const auto standardOffset = std::chrono::hours{offsetHours};
    auto candidate = [timestamp](std::chrono::seconds offset) {
      int64_t result;
      BOLT_USER_CHECK(
          !__builtin_sub_overflow(
              timestamp.time_since_epoch().count(), offset.count(), &result),
          "SystemV local time is outside the supported seconds range");
      return sys_seconds{std::chrono::seconds{result}};
    };
    const auto standard = getInfo(candidate(standardOffset));
    if (!hasDaylightSaving) {
      return {local_info::unique, standard, {}};
    }
    const auto daylight =
        getInfo(candidate(standardOffset + std::chrono::hours{1}));
    const bool validStandard = standard.offset == standardOffset;
    const bool validDaylight = daylight.offset != standardOffset;
    if (validStandard && validDaylight) {
      return {local_info::ambiguous, daylight, standard};
    }
    if (!validStandard && !validDaylight) {
      return {local_info::nonexistent, daylight, standard};
    }
    return {local_info::unique, validStandard ? standard : daylight, {}};
  }
};

} // namespace detail

using TTimeZoneDatabase = std::vector<std::unique_ptr<TimeZone>>;
using TTimeZoneIndex = folly::F14FastMap<std::string, const TimeZone*>;

// Defined in TimeZoneDatabase.cpp
extern const std::vector<std::pair<int16_t, std::string>>& getTimeZoneEntries();

// Defined in TimeZoneLinks.cpp
extern const std::unordered_map<std::string, std::string>& getTimeZoneLinks();

namespace {
// Keep Bolt's additional names separate from the generated Presto IDs. These
// stable IDs also fit the 12-bit time zone field in packed timestamps.
constexpr int16_t kSystemVTimeZoneIdStart = 4000;
constexpr detail::SystemVTimeZone kSystemVTimeZones[] = {
    {"SystemV/AST4", -4, false},
    {"SystemV/AST4ADT", -4, true},
    {"SystemV/CST6", -6, false},
    {"SystemV/CST6CDT", -6, true},
    {"SystemV/EST5", -5, false},
    {"SystemV/EST5EDT", -5, true},
    {"SystemV/HST10", -10, false},
    {"SystemV/MST7", -7, false},
    {"SystemV/MST7MDT", -7, true},
    {"SystemV/PST8", -8, false},
    {"SystemV/PST8PDT", -8, true},
    {"SystemV/YST9", -9, false},
    {"SystemV/YST9YDT", -9, true},
};
static_assert(kSystemVTimeZoneIdStart + std::size(kSystemVTimeZones) <= 4096);

// Returns the offset in minutes for a specific time zone offset in the
// database. Do not call for tzID 0 (UTC / "+00:00").
inline std::chrono::minutes getTimeZoneOffset(int16_t tzID) {
  return std::chrono::minutes{(tzID <= 840) ? (tzID - 841) : (tzID - 840)};
}

const ::date::time_zone* locateZoneImpl(
    std::string_view tz_name,
    bool failOnMissing = true) {
  BOLT_TEST_ADJUST("bytedance::bolt::tz::locateZoneImpl", &tz_name);
  if (!failOnMissing) {
    const auto& zones = ::date::get_tzdb().zones;
    const auto zone = std::lower_bound(
        zones.begin(),
        zones.end(),
        tz_name,
        [](const ::date::time_zone& candidate, std::string_view name) {
          return candidate.name() < name;
        });
    if (zone == zones.end() || zone->name() != tz_name) {
      return nullptr;
    }
  }
  const ::date::time_zone* zone = ::date::locate_zone(tz_name);
  return zone;
}

// Flattens the input vector of pairs into a vector, assuming that the
// timezoneIDs are (mostly) sequential. Note that since they are "mostly"
// senquential, the vector can have holes. But it is still more efficient than
// looking up on a map.
TTimeZoneDatabase buildTimeZoneDatabase(
    const std::vector<std::pair<int16_t, std::string>>& dbInput) {
  TTimeZoneDatabase tzDatabase;
  tzDatabase.resize(dbInput.back().first + 1);

  for (const auto& entry : dbInput) {
    std::unique_ptr<TimeZone> timeZonePtr;

    if (entry.first == 0) {
      timeZonePtr =
          std::make_unique<TimeZone>("UTC", entry.first, locateZoneImpl("UTC"));
    } else if (entry.first <= kMaxFixedOffsetTimeZoneId) {
      std::chrono::minutes offset = getTimeZoneOffset(entry.first);
      timeZonePtr =
          std::make_unique<TimeZone>(entry.second, entry.first, offset);
    }
    // Every single other time zone entry (outside of offsets) needs to be
    // available in external/date or this will throw.
    else {
      const ::date::time_zone* zone;
      try {
        zone = locateZoneImpl(entry.second);
      } catch (std::exception& err) {
        const auto& links = getTimeZoneLinks();
        const auto link = links.find(entry.second);
        if (link != links.end()) {
          zone = locateZoneImpl(link->second);
          timeZonePtr =
              std::make_unique<TimeZone>(entry.second, entry.first, zone);
          tzDatabase[entry.first] = std::move(timeZonePtr);
          continue;
        }

        // When this exception is thrown, it typically means the time zone name
        // we are trying to locate cannot be found from OS's time zone database.
        // Thus, we just command a "continue;" to skip the creation of the
        // corresponding TimeZone object.
        //
        // Then, once this time zone is requested at runtime, a runtime error
        // will be thrown and caller is expected to handle that error in code.
        LOG(WARNING) << "Invalid time zone [" << entry.second
                     << "]: " << err.what();
        continue;
      }
      timeZonePtr = std::make_unique<TimeZone>(entry.second, entry.first, zone);
    }
    tzDatabase[entry.first] = std::move(timeZonePtr);
  }
  BOLT_CHECK_LT(
      dbInput.back().first,
      kSystemVTimeZoneIdStart,
      "Presto time zone IDs overlap Bolt's SystemV IDs");
  tzDatabase.resize(kSystemVTimeZoneIdStart + std::size(kSystemVTimeZones));
  for (size_t i = 0; i < std::size(kSystemVTimeZones); ++i) {
    const auto id = kSystemVTimeZoneIdStart + i;
    const auto& rules = kSystemVTimeZones[i];
    if (const auto* zone = locateZoneImpl(rules.name, false)) {
      tzDatabase[id] = std::make_unique<TimeZone>(rules.name, id, zone);
    } else {
      tzDatabase[id] = std::make_unique<TimeZone>(rules, id);
    }
  }
  return tzDatabase;
}

const TTimeZoneDatabase& getTimeZoneDatabase() {
  static TTimeZoneDatabase timeZoneDatabase =
      buildTimeZoneDatabase(getTimeZoneEntries());
  return timeZoneDatabase;
}

// Reverses the vector of pairs into a map key'ed by the timezone name for
// reverse look ups.
TTimeZoneIndex buildTimeZoneIndex(const TTimeZoneDatabase& tzDatabase) {
  TTimeZoneIndex reversed;
  const auto zoneCount =
      std::count_if(tzDatabase.begin(), tzDatabase.end(), [](const auto& zone) {
        return zone != nullptr;
      });
  reversed.reserve(zoneCount + 2);

  for (int16_t i = 0; i < tzDatabase.size(); ++i) {
    if (tzDatabase[i] != nullptr) {
      reversed.emplace(
          boost::algorithm::to_lower_copy(tzDatabase[i]->name()),
          tzDatabase[i].get());
    }
  }

  // Add aliases to UTC.
  reversed.emplace("+00:00", tzDatabase.front().get());
  reversed.emplace("-00:00", tzDatabase.front().get());
  return reversed;
}

const TTimeZoneIndex& getTimeZoneIndex() {
  static TTimeZoneIndex timeZoneIndex =
      buildTimeZoneIndex(getTimeZoneDatabase());
  return timeZoneIndex;
}

inline bool isDigit(char c) {
  return c >= '0' && c <= '9';
}

inline bool startsWith(std::string_view str, const char* prefix) {
  return str.rfind(prefix, 0) == 0;
}

inline bool isTimeZoneOffset(std::string_view str) {
  return str.size() >= 2 && (str[0] == '+' || str[0] == '-');
}

// The timezone parsing logic follows what is defined here:
//   https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
inline bool isUtcEquivalentName(std::string_view zone) {
  static folly::F14FastSet<std::string> utcSet = {
      "utc", "uct", "gmt", "gmt0", "greenwich", "universal", "zulu", "ut", "z"};
  return utcSet.find(zone) != utcSet.end();
}

// This function tries to apply two normalization rules to time zone offsets:
//
// 1. If the offset only defines the single hours portion, assume minutes are
// zeroed out (e.g. "+0" -> "+00:00")
//
// 2. If the offset only defines the hours portion, assume minutes are zeroed
// out (e.g. "+00" -> "+00:00")
//
// 3. Check if the ':' in between in missing; if so, correct the offset string
// (e.g. "+0000" -> "+00:00").
//
// This function assumes the first character is either '+' or '-'.
std::string normalizeTimeZoneOffset(const std::string& zoneOffset) {
  if (zoneOffset.size() == 2 && isDigit(zoneOffset[1])) {
    return std::string() + zoneOffset[0] + '0' + zoneOffset[1] + ":00";
  }
  if (zoneOffset.size() == 3 && isDigit(zoneOffset[1]) &&
      isDigit(zoneOffset[2])) {
    return zoneOffset + ":00";
  }
  if (zoneOffset.size() == 5 && isDigit(zoneOffset[1]) &&
      isDigit(zoneOffset[2]) && isDigit(zoneOffset[3]) &&
      isDigit(zoneOffset[4])) {
    return zoneOffset.substr(0, 3) + ':' + zoneOffset.substr(3, 2);
  }
  return zoneOffset;
}

std::string normalizeTimeZone(const std::string& originalZoneId) {
  // If this is an offset that hasn't matched, check if this is an incomplete
  // offset.
  if (isTimeZoneOffset(originalZoneId)) {
    return normalizeTimeZoneOffset(originalZoneId);
  }

  // Otherwise, try other time zone name normalizations.
  std::string_view zoneId = originalZoneId;
  const bool startsWithEtc = startsWith(zoneId, "etc/");

  if (startsWithEtc) {
    zoneId = zoneId.substr(4);
  }

  // ETC/GMT, ETC/GREENWICH, and others are all valid and link to GMT.
  if (isUtcEquivalentName(zoneId)) {
    return "utc";
  }

  bool startsWithUtc = startsWith(zoneId, "utc");
  bool startsWithGmt = startsWith(zoneId, "gmt");
  bool startsWithUt = !startsWithUtc && startsWith(zoneId, "ut");

  // Check for Etc/GMT(+/-)H[H] UTC(+/-)H[H] GMT(+/-)H[H] UT(+/-)H[H] patterns.
  if ((zoneId.size() > 4 && (startsWithUtc || startsWithGmt)) ||
      (zoneId.size() > 3 && startsWithUt)) {
    if (startsWithUtc || startsWithGmt) {
      zoneId = zoneId.substr(3);
    } else {
      BOLT_DCHECK(startsWithUt);
      zoneId = zoneId.substr(2);
    }

    char signChar = zoneId[0];

    if (signChar == '+' || signChar == '-') {
      // ETC flips the sign for GMT.
      if (startsWithEtc && startsWithGmt) {
        signChar = (signChar == '-') ? '+' : '-';
      }

      // Extract the tens and ones characters for the hour.
      char hourTens;
      char hourOnes;

      if (zoneId.size() == 2) {
        hourTens = '0';
        hourOnes = zoneId[1];
      } else {
        hourTens = zoneId[1];
        hourOnes = zoneId[2];
      }

      // Prevent it from returning -00:00, which is just utc.
      if (hourTens == '0' && hourOnes == '0') {
        return "utc";
      }

      if (isDigit(hourTens) && isDigit(hourOnes)) {
        return std::string() + signChar + hourTens + hourOnes + ":00";
      }
    }
  }

  return originalZoneId;
}

template <typename TDuration>
void validateRangeImpl(time_point<TDuration> timePoint) {
  using namespace ::date;
  // update year range as using int64_t to store us
  // and toCivilDateTime can handle the year range [-290308, 294247]
  static constexpr auto kMinYear = year(-290308);
  static constexpr auto kMaxYear = year(294247);

  if (timePoint.time_since_epoch() <
          std::chrono::seconds(std::numeric_limits<int64_t>::min() / 1000) ||
      timePoint.time_since_epoch() >
          std::chrono::seconds(std::numeric_limits<int64_t>::max() / 1000)) {
    BOLT_USER_FAIL(
        "Timepoint is outside of supported timestamp seconds since epoch range: [{}, {}], got {}",
        std::chrono::seconds(std::numeric_limits<int64_t>::min() / 1000)
            .count(),
        std::chrono::seconds(std::numeric_limits<int64_t>::max() / 1000)
            .count(),
        static_cast<int64_t>(timePoint.time_since_epoch().count()));
  }
  auto year = year_month_day(floor<days>(timePoint)).year();

  if (year < kMinYear || year > kMaxYear) {
    // This is a special case where we intentionally throw
    // VeloxRuntimeError to avoid it being suppressed by TRY().
    std::stringstream ss;
    ss << "[" << kMinYear << ", " << kMaxYear << "], got " << year;
    BOLT_USER_FAIL(fmt::format(
        "Timepoint is outside of supported year range: {}", ss.str()));
  }
}

template <typename TDuration>
::date::zoned_time<TDuration> getZonedTime(
    const ::date::time_zone* tz,
    ::date::local_time<TDuration> timestamp,
    TimeZone::TChoose choose) {
  if (choose == TimeZone::TChoose::kFail) {
    // By default, throws.
    return ::date::zoned_time{tz, timestamp};
  }

  if (choose == TimeZone::TChoose::kEarliestOrShiftForward) {
    // The first offset selects the earlier instant in an overlap. In a gap,
    // it shifts the local time forward without a second time zone lookup.
    const auto info = tz->get_info(timestamp);
    return ::date::zoned_time{
        tz,
        ::date::sys_time<TDuration>{
            timestamp.time_since_epoch() - info.first.offset}};
  }

  auto dateChoose = (choose == TimeZone::TChoose::kEarliest)
      ? ::date::choose::earliest
      : ::date::choose::latest;
  return ::date::zoned_time{tz, timestamp, dateChoose};
}

template <typename TDuration>
TDuration toSysImpl(
    const TDuration& timestamp,
    const TimeZone::TChoose choose,
    const ::date::time_zone* tz,
    const std::chrono::minutes offset) {
  ::date::local_time<TDuration> timePoint{timestamp};
  validateRange(::date::sys_time<TDuration>{timestamp});

  if (tz == nullptr) {
    // We can ignore `choose` as time offset conversions are always linear.
    return (timePoint - offset).time_since_epoch();
  }

  return getZonedTime(tz, timePoint, choose).get_sys_time().time_since_epoch();
}

template <typename TDuration>
TDuration toLocalImpl(
    const TDuration& timestamp,
    const ::date::time_zone* tz,
    const std::chrono::minutes offset) {
  ::date::sys_time<TDuration> timePoint{timestamp};
  validateRange(timePoint);

  // If this is an offset time zone.
  if (tz == nullptr) {
    return (timePoint + offset).time_since_epoch();
  }
  return ::date::zoned_time{tz, timePoint}.get_local_time().time_since_epoch();
}

template <typename TDuration>
TDuration systemVToSys(
    TDuration timestamp,
    TimeZone::TChoose choose,
    const TimeZone& zone) {
  using namespace ::date;
  const local_time<TDuration> local{timestamp};
  validateRange(sys_time<TDuration>{timestamp});
  const auto info = zone.getInfo(floor<std::chrono::seconds>(local));
  if (info.result == local_info::nonexistent) {
    if (choose == TimeZone::TChoose::kFail) {
      throw nonexistent_local_time(local, info);
    }
    if (choose != TimeZone::TChoose::kEarliestOrShiftForward) {
      return std::chrono::duration_cast<TDuration>(
          info.first.end.time_since_epoch());
    }
  } else if (info.result == local_info::ambiguous) {
    if (choose == TimeZone::TChoose::kFail) {
      throw ambiguous_local_time(local, info);
    }
    if (choose == TimeZone::TChoose::kLatest) {
      return timestamp - info.second.offset;
    }
  }
  return timestamp - info.first.offset;
}

template <bool isLongName>
std::string getName(
    TimeZone::milliseconds timestamp,
    TimeZone::TChoose choose,
    const TimeZone& zone) {
  validateRange(::date::sys_time<TimeZone::milliseconds>{timestamp});
  if (zone.offset()) {
    return zone.name();
  }

  static const auto& timeZoneNames = getTimeZoneNames();
  auto it = timeZoneNames.find(zone.name());
  BOLT_CHECK(
      it != timeZoneNames.end(),
      "Unable to find short name for time zone: {}",
      zone.name());

  const auto info = zone.tz()
      ? getZonedTime(
            zone.tz(),
            ::date::local_time<TimeZone::milliseconds>{timestamp},
            choose)
            .get_info()
      : zone.getInfo(::date::floor<std::chrono::seconds>(
            ::date::sys_time<TimeZone::milliseconds>{
                zone.to_sys(timestamp, choose)}));
  const bool isDst = info.save != std::chrono::minutes{0};
  if constexpr (isLongName) {
    return isDst ? it->second.daylightTimeLongName
                 : it->second.standardTimeLongName;
  } else {
    return isDst ? it->second.daylightTimeAbbreviation
                 : it->second.standardTimeAbbreviation;
  }
}
} // namespace

void validateRange(time_point<std::chrono::seconds> timePoint) {
  validateRangeImpl(timePoint);
}

void validateRange(time_point<std::chrono::milliseconds> timePoint) {
  validateRangeImpl(timePoint);
}

std::string getTimeZoneName(int64_t timeZoneID) {
  return locateZone(timeZoneID, true)->name();
}

const TimeZone* locateZone(int16_t timeZoneID, bool failOnError) {
  const auto& timeZoneDatabase = getTimeZoneDatabase();

  // Check if timeZoneID does not exceed the vector size or is one of the
  // "holes".
  if (timeZoneID < 0 ||
      static_cast<size_t>(timeZoneID) >= timeZoneDatabase.size() ||
      timeZoneDatabase[timeZoneID] == nullptr) {
    if (failOnError) {
      BOLT_FAIL("Unable to resolve timeZoneID '{}'", timeZoneID);
    }
    return nullptr;
  }
  return timeZoneDatabase[timeZoneID].get();
}

const TimeZone* locateZone(std::string_view timeZone, bool failOnError) {
  const auto& timeZoneIndex = getTimeZoneIndex();

  std::string timeZoneLowered;
  boost::algorithm::to_lower_copy(
      std::back_inserter(timeZoneLowered), timeZone);

  auto it = timeZoneIndex.find(timeZoneLowered);
  if (it != timeZoneIndex.end()) {
    BOLT_CHECK_NOT_NULL(it->second);
    return it->second;
  }

  // If an exact match wasn't found, try to normalize the timezone name.
  it = timeZoneIndex.find(normalizeTimeZone(timeZoneLowered));
  if (it != timeZoneIndex.end()) {
    BOLT_CHECK_NOT_NULL(it->second);
    return it->second;
  }
  if (failOnError) {
    BOLT_USER_FAIL("Unknown time zone: '{}'", timeZone);
  }
  return nullptr;
}

int16_t getTimeZoneID(std::string_view timeZone, bool failOnError) {
  const TimeZone* tz = locateZone(timeZone, failOnError);
  return tz == nullptr ? -1 : tz->id();
}

int16_t getTimeZoneID(int32_t offsetMinutes) {
  static constexpr int32_t kMinOffset = -14 * 60;
  static constexpr int32_t kMaxOffset = 14 * 60;

  if (offsetMinutes == 0) {
    return 0;
  }

  BOLT_USER_CHECK_LE(
      kMinOffset,
      offsetMinutes,
      "Invalid timezone offset minutes: {}",
      offsetMinutes);
  BOLT_USER_CHECK_LE(
      offsetMinutes,
      kMaxOffset,
      "Invalid timezone offset minutes: {}",
      offsetMinutes);

  if (offsetMinutes < 0) {
    return 1 + (offsetMinutes - kMinOffset);
  } else {
    return offsetMinutes - kMinOffset;
  }
}

std::vector<int16_t> getTimeZoneIDs() {
  const auto& timeZoneDatabase = getTimeZoneDatabase();

  std::vector<int16_t> ids;
  ids.reserve(timeZoneDatabase.size());

  for (int16_t i = 0; i < timeZoneDatabase.size(); ++i) {
    if (timeZoneDatabase[i] != nullptr) {
      ids.push_back(i);
    }
  }

  return ids;
}

std::optional<std::pair<int32_t, int32_t>> detail::getSystemVTimeZoneOffsets(
    std::string_view name) {
  for (const auto& rules : kSystemVTimeZones) {
    if (name == rules.name) {
      return std::pair{
          rules.offsetHours * 3600, rules.hasDaylightSaving ? 3600 : 0};
    }
  }
  return std::nullopt;
}

const TimeZone* detail::locateBuiltinSystemVZone(std::string_view name) {
  static const auto zones = [] {
    std::array<std::unique_ptr<TimeZone>, std::size(kSystemVTimeZones)> result;
    for (size_t i = 0; i < result.size(); ++i) {
      result[i] = std::make_unique<TimeZone>(
          kSystemVTimeZones[i], kSystemVTimeZoneIdStart + i);
    }
    return result;
  }();
  for (const auto& zone : zones) {
    if (zone->name() == name) {
      return zone.get();
    }
  }
  return nullptr;
}

TimeZone::TimeZone(const detail::SystemVTimeZone& systemV, int16_t timeZoneID)
    : offset_(std::chrono::minutes::min()),
      timeZoneName_(systemV.name),
      timeZoneID_(timeZoneID) {
  BOLT_CHECK_GE(timeZoneID, kSystemVTimeZoneIdStart);
  const auto index = timeZoneID - kSystemVTimeZoneIdStart;
  BOLT_CHECK_LT(index, std::size(kSystemVTimeZones));
  BOLT_CHECK(&systemV == &kSystemVTimeZones[index]);
}

TimeZone::seconds TimeZone::to_sys(
    TimeZone::seconds timestamp,
    TimeZone::TChoose choose) const {
  if (hasSystemVRules()) {
    return systemVToSys(timestamp, choose, *this);
  }
  return toSysImpl(timestamp, choose, tz_, offset_);
}

TimeZone::milliseconds TimeZone::to_sys(
    TimeZone::milliseconds timestamp,
    TimeZone::TChoose choose) const {
  if (hasSystemVRules()) {
    return systemVToSys(timestamp, choose, *this);
  }
  return toSysImpl(timestamp, choose, tz_, offset_);
}

TimeZone::seconds TimeZone::to_local(TimeZone::seconds timestamp) const {
  if (hasSystemVRules()) {
    const ::date::sys_time<decltype(timestamp)> utc{timestamp};
    validateRange(utc);
    return timestamp + getInfo(::date::floor<seconds>(utc)).offset;
  }
  return toLocalImpl(timestamp, tz_, offset_);
}

TimeZone::milliseconds TimeZone::to_local(
    TimeZone::milliseconds timestamp) const {
  if (hasSystemVRules()) {
    const ::date::sys_time<decltype(timestamp)> utc{timestamp};
    validateRange(utc);
    return timestamp + getInfo(::date::floor<seconds>(utc)).offset;
  }
  return toLocalImpl(timestamp, tz_, offset_);
}

TimeZone::seconds TimeZone::correct_nonexistent_time(
    TimeZone::seconds timestamp) const {
  // If this is an offset time zone.
  if (offset()) {
    return timestamp;
  }

  const auto localInfo = getInfo(::date::local_time<seconds>{timestamp});

  if (localInfo.result != ::date::local_info::nonexistent) {
    return timestamp;
  }

  const auto adjustment = localInfo.second.offset - localInfo.first.offset;

  return timestamp + adjustment;
}

::date::sys_info TimeZone::getInfo(::date::sys_seconds timestamp) const {
  if (hasSystemVRules()) {
    return kSystemVTimeZones[timeZoneID_ - kSystemVTimeZoneIdStart].getInfo(
        timestamp);
  }
  if (tz_) {
    return tz_->get_info(timestamp);
  }
  return {
      ::date::sys_seconds::min(),
      ::date::sys_seconds::max(),
      offset_,
      std::chrono::minutes{0},
      timeZoneName_};
}

::date::local_info TimeZone::getInfo(::date::local_seconds timestamp) const {
  if (hasSystemVRules()) {
    return kSystemVTimeZones[timeZoneID_ - kSystemVTimeZoneIdStart].getInfo(
        timestamp);
  }
  if (tz_) {
    return tz_->get_info(timestamp);
  }
  return {::date::local_info::unique, getInfo(::date::sys_seconds{}), {}};
}

std::string TimeZone::getShortName(
    TimeZone::milliseconds timestamp,
    TimeZone::TChoose choose) const {
  return getName<false>(timestamp, choose, *this);
}

std::string TimeZone::getLongName(
    TimeZone::milliseconds timestamp,
    TimeZone::TChoose choose) const {
  return getName<true>(timestamp, choose, *this);
}

bool isDstTimezone(int16_t tzID) {
  // for daylight saving time
  static const folly::F14FastSet<int16_t> dstTzID{
      1698, // Africa/Dakar
      1825, // America/Los_Angeles
      2056 // Europe/Brussels
  };
  if (dstTzID.find(tzID) != dstTzID.end()) {
    return true;
  }
  return false;
}

} // namespace bytedance::bolt::tz
