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

#include <date/tz.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <type_traits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/type/tz/SystemVTimeZone.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::tz {
namespace {

using common::testutil::TestValue;

using Seconds = std::chrono::seconds;
using Milliseconds = std::chrono::milliseconds;
using Choose = TimeZone::TChoose;

static_assert(std::is_constructible_v<
              TimeZone,
              std::string_view,
              int16_t,
              std::nullptr_t>);

struct SystemVZoneCase {
  std::string_view name;
  int16_t id;
  int32_t standardOffset;
  bool hasDst;
};

constexpr SystemVZoneCase kSystemVZones[] = {
    {"SystemV/AST4", 4000, -14'400, false},
    {"SystemV/AST4ADT", 4001, -14'400, true},
    {"SystemV/CST6", 4002, -21'600, false},
    {"SystemV/CST6CDT", 4003, -21'600, true},
    {"SystemV/EST5", 4004, -18'000, false},
    {"SystemV/EST5EDT", 4005, -18'000, true},
    {"SystemV/HST10", 4006, -36'000, false},
    {"SystemV/MST7", 4007, -25'200, false},
    {"SystemV/MST7MDT", 4008, -25'200, true},
    {"SystemV/PST8", 4009, -28'800, false},
    {"SystemV/PST8PDT", 4010, -28'800, true},
    {"SystemV/YST9", 4011, -32'400, false},
    {"SystemV/YST9YDT", 4012, -32'400, true},
};

struct SystemVSeason {
  int32_t year;
  int64_t springLocal;
  int64_t autumnLocal;
};

// Reference transition calendar fields from Java ZoneRules (tzdb 2022g),
// encoded as seconds since the epoch without applying a time zone offset.
// These include the first DST year, the 1974/1975 exceptions, the last year
// with explicit history, and wide timestamps.
constexpr SystemVSeason kSystemVSeasons[] = {
    {1900, -2'198'786'400, -2'183'061'600},
    {1973, 104'896'800, 120'621'600},
    {1974, 126'669'600, 154'490'400},
    {1975, 162'352'800, 183'520'800},
    {1976, 199'245'600, 215'575'200},
    {1977, 230'695'200, 247'024'800},
    {2021, 1'619'316'000, 1'635'645'600},
    {2038, 2'155'773'600, 2'172'103'200},
    {2100, 4'112'301'600, 4'128'631'200},
    {2400, 13'579'840'800, 13'595'565'600},
    {32767, 971'869'716'000, 971'885'440'800},
    {294000, 9'215'587'044'000, 9'215'602'768'800},
};

void testSystemVFixedOffsetsAndEarlyHistory(bool useBuiltIn = true);
void testSystemVTransitions(bool useBuiltIn = true);
void testSystemVLocalResolution(bool useBuiltIn = true);

#ifndef NDEBUG
TEST(TimeZoneMapDeathTest, missingNamedTimeZone) {
  // Re-execute the test binary so the injection precedes the first lookup,
  // even if another test has already initialized the static time zone cache.
  const auto previousStyle = testing::FLAGS_gtest_death_test_style;
  testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(
      {
        BOLT_TEST_VALUE_ENABLE();
        SCOPED_TESTVALUE_SET(
            "bytedance::bolt::tz::locateZoneImpl",
            std::function<void(std::string_view*)>([](std::string_view* name) {
              if (*name == "Pacific/Kanton") {
                *name = "Invalid/Pacific-Kanton";
              }
            }));

        const auto* shanghai = locateZone("Asia/Shanghai");
        ASSERT_NE(nullptr, shanghai);
        EXPECT_EQ(1980, shanghai->id());
        EXPECT_EQ(shanghai, locateZone(1980));
        EXPECT_EQ(
            std::chrono::seconds(28800),
            shanghai->to_local(std::chrono::seconds(0)));

        BOLT_ASSERT_THROW(
            locateZone("Pacific/Kanton", true), "Unknown time zone");
        EXPECT_EQ(nullptr, locateZone("Pacific/Kanton", false));
        EXPECT_EQ(-1, getTimeZoneID("Pacific/Kanton", false));
        BOLT_ASSERT_THROW(
            locateZone(2231, true), "Unable to resolve timeZoneID");
        EXPECT_EQ(nullptr, locateZone(2231, false));
        const auto ids = getTimeZoneIDs();
        EXPECT_EQ(ids.end(), std::find(ids.begin(), ids.end(), 2231));

        const auto* utc = locateZone("UTC");
        ASSERT_NE(nullptr, utc);
        EXPECT_EQ(0, utc->id());
        EXPECT_EQ(utc, locateZone("GMT"));
        EXPECT_EQ(utc, locateZone("+00:00"));
        EXPECT_EQ(utc, locateZone("-00:00"));
        EXPECT_EQ(1320, getTimeZoneID("+08:00"));
        EXPECT_EQ(
            std::chrono::seconds(28800),
            locateZone("+08:00")->to_local(std::chrono::seconds(0)));
        EXPECT_EQ(541, getTimeZoneID("-05:00"));
        std::exit(testing::Test::HasFailure() ? EXIT_FAILURE : EXIT_SUCCESS);
      },
      testing::ExitedWithCode(EXIT_SUCCESS),
      "");
  testing::FLAGS_gtest_death_test_style = previousStyle;
}

TEST(TimeZoneMapDeathTest, systemVUsesSystemRules) {
  const auto previousStyle = testing::FLAGS_gtest_death_test_style;
  testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(
      {
        BOLT_TEST_VALUE_ENABLE();
        size_t systemVLookups = 0;
        SCOPED_TESTVALUE_SET(
            "bytedance::bolt::tz::locateZoneImpl",
            std::function<void(std::string_view*)>([&](std::string_view* name) {
              if (name->substr(0, 8) == "SystemV/") {
                ++systemVLookups;
                *name = "Etc/UTC";
              }
            }));

        for (const auto& reference : kSystemVZones) {
          SCOPED_TRACE(reference.name);
          const auto* zone = locateZone(reference.name);
          ASSERT_NE(nullptr, zone);
          ASSERT_NE(nullptr, zone->tz());
          EXPECT_EQ(date::locate_zone("Etc/UTC"), zone->tz());
          EXPECT_EQ(reference.id, zone->id());
          EXPECT_EQ(reference.name, zone->name());
          EXPECT_EQ(zone, locateZone(reference.id));
          EXPECT_FALSE(zone->offset().has_value());
          // This local time is a gap in the built-in DST rules, but is unique
          // with the supplied UTC rules. Every built-in SystemV offset is
          // nonzero, so identity conversions also distinguish the fixed zones.
          const auto local = Seconds(1'619'317'800);
          EXPECT_EQ(local, zone->to_local(local));
          EXPECT_EQ(local, zone->to_sys(local));
          const auto info = zone->getInfo(date::local_seconds{local});
          EXPECT_EQ(date::local_info::unique, info.result);
          EXPECT_EQ(Seconds(0), info.first.offset);
          EXPECT_EQ(Seconds(0), zone->getInfo(date::sys_seconds{local}).offset);
        }
        EXPECT_EQ(std::size(kSystemVZones), systemVLookups);
        std::exit(testing::Test::HasFailure() ? EXIT_FAILURE : EXIT_SUCCESS);
      },
      testing::ExitedWithCode(EXIT_SUCCESS),
      "");
  testing::FLAGS_gtest_death_test_style = previousStyle;
}

TEST(TimeZoneMapDeathTest, systemVUsesBuiltInRulesWhenMissing) {
  const auto previousStyle = testing::FLAGS_gtest_death_test_style;
  testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(
      {
        BOLT_TEST_VALUE_ENABLE();
        size_t systemVLookups = 0;
        SCOPED_TESTVALUE_SET(
            "bytedance::bolt::tz::locateZoneImpl",
            std::function<void(std::string_view*)>([&](std::string_view* name) {
              if (name->substr(0, 8) == "SystemV/") {
                ++systemVLookups;
                *name = "Invalid/SystemV";
              }
            }));

        for (const auto& reference : kSystemVZones) {
          SCOPED_TRACE(reference.name);
          const auto* zone = locateZone(reference.name);
          ASSERT_NE(nullptr, zone);
          EXPECT_EQ(nullptr, zone->tz());
          EXPECT_EQ(reference.id, zone->id());
          EXPECT_EQ(reference.name, zone->name());
          EXPECT_EQ(zone, locateZone(reference.id));
          EXPECT_FALSE(zone->offset().has_value());
        }
        EXPECT_EQ(std::size(kSystemVZones), systemVLookups);
        // Run the complete fallback oracle in this fresh process so it is
        // independent of both host tzdata and previous cache initialization.
        testSystemVFixedOffsetsAndEarlyHistory(false);
        testSystemVTransitions(false);
        testSystemVLocalResolution(false);
        std::exit(testing::Test::HasFailure() ? EXIT_FAILURE : EXIT_SUCCESS);
      },
      testing::ExitedWithCode(EXIT_SUCCESS),
      "");
  testing::FLAGS_gtest_death_test_style = previousStyle;
}
#endif

class TimeZoneMapTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
#ifndef NDEBUG
    BOLT_TEST_VALUE_ENABLE();
    TestValue::set<std::string_view>(
        "bytedance::bolt::tz::locateZoneImpl", [](std::string_view* name) {
          if (*name == "Africa/Asmera") {
            *name = "Invalid/Africa-Asmera";
          }
        });
#endif
  }

  static void TearDownTestSuite() {
#ifndef NDEBUG
    TestValue::clear("bytedance::bolt::tz::locateZoneImpl");
#endif
  }
};

TEST_F(TimeZoneMapTest, getTimeZoneName) {
  EXPECT_EQ("America/Los_Angeles", getTimeZoneName(1825));
  EXPECT_EQ("Europe/Moscow", getTimeZoneName(2079));
  EXPECT_EQ("Pacific/Kanton", getTimeZoneName(2231));
  EXPECT_EQ("Europe/Kyiv", getTimeZoneName(2232));
  EXPECT_EQ("America/Ciudad_Juarez", getTimeZoneName(2233));
  EXPECT_EQ("-00:01", getTimeZoneName(840));
}

TEST_F(TimeZoneMapTest, getTimeZoneID) {
  EXPECT_EQ(1825, getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(2079, getTimeZoneID("Europe/Moscow"));
  EXPECT_EQ(2231, getTimeZoneID("Pacific/Kanton"));
  EXPECT_EQ(2232, getTimeZoneID("Europe/Kyiv"));
  EXPECT_EQ(2233, getTimeZoneID("America/Ciudad_Juarez"));
  EXPECT_EQ(1686, getTimeZoneID("Africa/Asmera"));
  EXPECT_EQ("Africa/Asmera", getTimeZoneName(1686));
#ifndef NDEBUG
  EXPECT_EQ("Africa/Asmara", locateZone(1686)->tz()->name());
#endif
  EXPECT_EQ(0, getTimeZoneID("UTC"));
  EXPECT_EQ(0, getTimeZoneID("GMT"));
  EXPECT_EQ(0, getTimeZoneID("Z"));
  EXPECT_EQ(0, getTimeZoneID("greenwich"));
  EXPECT_EQ(0, getTimeZoneID("ETC/GMT"));
  EXPECT_EQ(0, getTimeZoneID("ETC/GMT0"));
  EXPECT_EQ(0, getTimeZoneID("ETC/UCT"));
  EXPECT_EQ(0, getTimeZoneID("ETC/universal"));
  EXPECT_EQ(0, getTimeZoneID("etc/zulu"));

  // (+/-)XX:MM format.
  EXPECT_EQ(840, getTimeZoneID("-00:01"));
  EXPECT_EQ(0, getTimeZoneID("+00:00"));
  EXPECT_EQ(454, getTimeZoneID("-06:27"));
  EXPECT_EQ(541, getTimeZoneID("-05:00"));
  EXPECT_EQ(1140, getTimeZoneID("+05:00"));

  EXPECT_EQ(0, getTimeZoneID("etc/GMT+0"));
  EXPECT_EQ(0, getTimeZoneID("etc/GMT-0"));
  EXPECT_EQ(1020, getTimeZoneID("etc/GMT-3"));
  EXPECT_EQ(301, getTimeZoneID("etc/GMT+9"));
  EXPECT_EQ(1680, getTimeZoneID("etc/GMT-14"));

  // Case insensitive.
  EXPECT_EQ(0, getTimeZoneID("utc"));
  EXPECT_EQ(1825, getTimeZoneID("america/los_angeles"));
  EXPECT_EQ(1825, getTimeZoneID("aMERICa/los_angeles"));
}

TEST_F(TimeZoneMapTest, invalid) {
  BOLT_ASSERT_THROW(getTimeZoneName(99999999), "Unable to resolve timeZoneID");
  BOLT_ASSERT_THROW(getTimeZoneID("This is a test"), "Unknown time zone");

  BOLT_ASSERT_THROW(getTimeZoneID("ETC/05:00"), "Unknown time zone");
  BOLT_ASSERT_THROW(getTimeZoneID("ETC+05:00"), "Unknown time zone");

  BOLT_ASSERT_THROW(getTimeZoneID("etc/GMT-15"), "Unknown time zone");
  BOLT_ASSERT_THROW(getTimeZoneID("etc/GMT+ab"), "Unknown time zone");
  BOLT_ASSERT_THROW(getTimeZoneID("etc/GMT+300"), "Unknown time zone");
}

TEST_F(TimeZoneMapTest, systemVLookup) {
  const auto ids = getTimeZoneIDs();
  for (const auto& reference : kSystemVZones) {
    SCOPED_TRACE(reference.name);
    const auto* zone = locateZone(reference.name);
    ASSERT_NE(nullptr, zone);
    EXPECT_EQ(reference.id, zone->id());
    EXPECT_EQ(reference.id, getTimeZoneID(reference.name));
    EXPECT_EQ(reference.name, zone->name());
    EXPECT_EQ(reference.name, getTimeZoneName(reference.id));
    EXPECT_EQ(zone, locateZone(reference.id));
    EXPECT_EQ(1, std::count(ids.begin(), ids.end(), reference.id));
    EXPECT_FALSE(zone->offset().has_value());
  }
  EXPECT_EQ(locateZone("SystemV/EST5EDT"), locateZone("systemv/est5edt"));

  for (const auto name : {"SystemV/EST4EDT", "SystemV/GMT0", "SystemV/UTC"}) {
    EXPECT_EQ(nullptr, locateZone(name, false));
    EXPECT_EQ(-1, getTimeZoneID(name, false));
    BOLT_ASSERT_THROW(locateZone(name), "Unknown time zone");
  }
  // 4013 is exactly one past the last assigned ID; 3999 is an unassigned gap.
  for (const int16_t id :
       {std::numeric_limits<int16_t>::min(),
        int16_t{-1},
        int16_t{3999},
        int16_t{4013},
        std::numeric_limits<int16_t>::max()}) {
    EXPECT_EQ(nullptr, locateZone(id, false));
    BOLT_ASSERT_THROW(locateZone(id), "Unable to resolve timeZoneID");
  }
}

void testSystemVFixedOffsetsAndEarlyHistory(bool useBuiltIn) {
  for (const auto& reference : kSystemVZones) {
    SCOPED_TRACE(reference.name);
    const auto* zone = useBuiltIn
        ? detail::locateBuiltinSystemVZone(reference.name)
        : locateZone(reference.name);
    ASSERT_NE(nullptr, zone);
    ASSERT_EQ(nullptr, zone->tz());
    const auto offset = Seconds(reference.standardOffset);
    // Year 1, summer 1899, and January 1900 all precede the first DST season.
    for (const auto count :
         {-62'135'596'800LL,
          -2'224'756'800LL,
          -2'208'988'800LL,
          -1LL,
          0LL,
          1'625'097'600LL,
          9'215'592'364'800LL}) {
      if (reference.hasDst && count > 0) {
        continue;
      }
      const auto instant = Seconds(count);
      const auto info = zone->getInfo(date::sys_seconds{instant});
      EXPECT_EQ(offset, info.offset);
      EXPECT_EQ(std::chrono::minutes(0), info.save);
      EXPECT_LE(info.begin, date::sys_seconds{instant});
      EXPECT_GT(info.end, date::sys_seconds{instant});
      EXPECT_EQ(instant + offset, zone->to_local(instant));
      EXPECT_EQ(instant, zone->to_sys(instant + offset));
      const auto localInfo =
          zone->getInfo(date::local_seconds{instant + offset});
      EXPECT_EQ(date::local_info::unique, localInfo.result);
      EXPECT_EQ(offset, localInfo.first.offset);
      EXPECT_EQ(
          instant + offset, zone->correct_nonexistent_time(instant + offset));
      const auto millis = Milliseconds(count * 1000 + 123);
      EXPECT_EQ(millis + offset, zone->to_local(millis));
      EXPECT_EQ(millis, zone->to_sys(millis + offset));
    }
  }
}

void testSystemVTransitions(bool useBuiltIn) {
  for (const auto& reference : kSystemVZones) {
    if (!reference.hasDst) {
      continue;
    }
    SCOPED_TRACE(reference.name);
    const auto* zone = useBuiltIn
        ? detail::locateBuiltinSystemVZone(reference.name)
        : locateZone(reference.name);
    ASSERT_NE(nullptr, zone);
    ASSERT_EQ(nullptr, zone->tz());
    const auto standardOffset = Seconds(reference.standardOffset);
    const auto daylightOffset = standardOffset + Seconds(3600);
    for (const auto& season : kSystemVSeasons) {
      SCOPED_TRACE(season.year);
      const auto spring = Seconds(season.springLocal) - standardOffset;
      const auto autumn = Seconds(season.autumnLocal) - daylightOffset;
      const auto winter = zone->getInfo(date::sys_seconds{spring - Seconds(1)});
      const auto summer = zone->getInfo(date::sys_seconds{spring});
      EXPECT_EQ(standardOffset, winter.offset);
      EXPECT_EQ(std::chrono::minutes(0), winter.save);
      EXPECT_EQ(date::sys_seconds{spring}, winter.end);
      EXPECT_EQ(daylightOffset, summer.offset);
      EXPECT_EQ(std::chrono::minutes(60), summer.save);
      EXPECT_EQ(date::sys_seconds{spring}, summer.begin);
      EXPECT_EQ(date::sys_seconds{autumn}, summer.end);
      const auto autumnBefore =
          zone->getInfo(date::sys_seconds{autumn - Seconds(1)});
      const auto autumnAfter = zone->getInfo(date::sys_seconds{autumn});
      EXPECT_EQ(summer.begin, autumnBefore.begin);
      EXPECT_EQ(summer.end, autumnBefore.end);
      EXPECT_EQ(daylightOffset, autumnBefore.offset);
      EXPECT_EQ(standardOffset, autumnAfter.offset);
      EXPECT_EQ(std::chrono::minutes(0), autumnAfter.save);
      EXPECT_EQ(date::sys_seconds{autumn}, autumnAfter.begin);

      for (const auto transition : {spring, autumn}) {
        const auto offsetBefore =
            transition == spring ? standardOffset : daylightOffset;
        const auto offsetAfter =
            transition == spring ? daylightOffset : standardOffset;
        EXPECT_EQ(
            transition - Seconds(1) + offsetBefore,
            zone->to_local(transition - Seconds(1)));
        EXPECT_EQ(transition + offsetAfter, zone->to_local(transition));
        const auto millis =
            std::chrono::duration_cast<Milliseconds>(transition);
        EXPECT_EQ(
            millis - Milliseconds(1) + offsetBefore,
            zone->to_local(millis - Milliseconds(1)));
        EXPECT_EQ(
            millis + Milliseconds(1) + offsetAfter,
            zone->to_local(millis + Milliseconds(1)));
      }
    }
    for (size_t i = 1; i < std::size(kSystemVSeasons); ++i) {
      const auto& previous = kSystemVSeasons[i - 1];
      const auto& current = kSystemVSeasons[i];
      if (current.year != previous.year + 1) {
        continue;
      }
      const auto autumn = Seconds(previous.autumnLocal) - daylightOffset;
      const auto spring = Seconds(current.springLocal) - standardOffset;
      const auto winter = zone->getInfo(date::sys_seconds{autumn});
      EXPECT_EQ(date::sys_seconds{autumn}, winter.begin);
      EXPECT_EQ(date::sys_seconds{spring}, winter.end);
    }
  }
}

void testSystemVLocalResolution(bool useBuiltIn) {
  for (const auto& reference : kSystemVZones) {
    if (!reference.hasDst) {
      continue;
    }
    SCOPED_TRACE(reference.name);
    const auto* zone = useBuiltIn
        ? detail::locateBuiltinSystemVZone(reference.name)
        : locateZone(reference.name);
    ASSERT_NE(nullptr, zone);
    ASSERT_EQ(nullptr, zone->tz());
    const auto standardOffset = Seconds(reference.standardOffset);
    const auto daylightOffset = standardOffset + Seconds(3600);
    for (const auto& season : kSystemVSeasons) {
      SCOPED_TRACE(season.year);
      const auto springLocal = Seconds(season.springLocal);
      const auto autumnLocal = Seconds(season.autumnLocal);
      const auto spring = springLocal - standardOffset;
      const auto autumn = autumnLocal - daylightOffset;
      const auto gap = springLocal + Seconds(1800);
      const auto overlap = autumnLocal - Seconds(1800);
      for (const auto local : {springLocal, springLocal + Seconds(3599)}) {
        EXPECT_EQ(
            date::local_info::nonexistent,
            zone->getInfo(date::local_seconds{local}).result);
      }
      for (const auto local :
           {autumnLocal - Seconds(3600), autumnLocal - Seconds(1)}) {
        EXPECT_EQ(
            date::local_info::ambiguous,
            zone->getInfo(date::local_seconds{local}).result);
      }
      const auto gapInfo = zone->getInfo(date::local_seconds{gap});
      EXPECT_EQ(date::local_info::nonexistent, gapInfo.result);
      EXPECT_EQ(standardOffset, gapInfo.first.offset);
      EXPECT_EQ(daylightOffset, gapInfo.second.offset);
      EXPECT_EQ(date::sys_seconds{spring}, gapInfo.first.end);
      EXPECT_EQ(date::sys_seconds{spring}, gapInfo.second.begin);
      const auto overlapInfo = zone->getInfo(date::local_seconds{overlap});
      EXPECT_EQ(date::local_info::ambiguous, overlapInfo.result);
      EXPECT_EQ(daylightOffset, overlapInfo.first.offset);
      EXPECT_EQ(standardOffset, overlapInfo.second.offset);
      EXPECT_EQ(date::sys_seconds{autumn}, overlapInfo.first.end);
      EXPECT_EQ(date::sys_seconds{autumn}, overlapInfo.second.begin);

      EXPECT_THROW(zone->to_sys(gap), date::nonexistent_local_time);
      EXPECT_EQ(spring, zone->to_sys(gap, Choose::kEarliest));
      EXPECT_EQ(spring, zone->to_sys(gap, Choose::kLatest));
      EXPECT_EQ(
          gap - standardOffset,
          zone->to_sys(gap, Choose::kEarliestOrShiftForward));
      EXPECT_EQ(gap + Seconds(3600), zone->correct_nonexistent_time(gap));
      EXPECT_THROW(zone->to_sys(overlap), date::ambiguous_local_time);
      EXPECT_EQ(
          overlap - daylightOffset, zone->to_sys(overlap, Choose::kEarliest));
      EXPECT_EQ(
          overlap - standardOffset, zone->to_sys(overlap, Choose::kLatest));
      EXPECT_EQ(
          overlap - daylightOffset,
          zone->to_sys(overlap, Choose::kEarliestOrShiftForward));
      EXPECT_EQ(overlap, zone->correct_nonexistent_time(overlap));

      const auto gapMillis =
          std::chrono::duration_cast<Milliseconds>(gap) + Milliseconds(123);
      const auto overlapMillis =
          std::chrono::duration_cast<Milliseconds>(overlap) + Milliseconds(123);
      const auto springMillis =
          std::chrono::duration_cast<Milliseconds>(spring);
      EXPECT_THROW(zone->to_sys(gapMillis), date::nonexistent_local_time);
      EXPECT_EQ(springMillis, zone->to_sys(gapMillis, Choose::kEarliest));
      EXPECT_EQ(springMillis, zone->to_sys(gapMillis, Choose::kLatest));
      EXPECT_EQ(
          gapMillis - standardOffset,
          zone->to_sys(gapMillis, Choose::kEarliestOrShiftForward));
      EXPECT_THROW(zone->to_sys(overlapMillis), date::ambiguous_local_time);
      EXPECT_EQ(
          overlapMillis - daylightOffset,
          zone->to_sys(overlapMillis, Choose::kEarliest));
      EXPECT_EQ(
          overlapMillis - standardOffset,
          zone->to_sys(overlapMillis, Choose::kLatest));
      EXPECT_EQ(
          overlapMillis - daylightOffset,
          zone->to_sys(overlapMillis, Choose::kEarliestOrShiftForward));

      for (const auto local :
           {springLocal - Seconds(1),
            springLocal + Seconds(3600),
            autumnLocal - Seconds(3601),
            autumnLocal}) {
        EXPECT_EQ(
            date::local_info::unique,
            zone->getInfo(date::local_seconds{local}).result);
        for (const auto choose :
             {Choose::kFail,
              Choose::kEarliest,
              Choose::kLatest,
              Choose::kEarliestOrShiftForward}) {
          EXPECT_EQ(local, zone->to_local(zone->to_sys(local, choose)));
          const auto millis = std::chrono::duration_cast<Milliseconds>(local) +
              Milliseconds(123);
          EXPECT_EQ(millis, zone->to_local(zone->to_sys(millis, choose)));
        }
      }
    }
  }
}

TEST_F(TimeZoneMapTest, systemVFixedOffsetsAndEarlyHistory) {
  testSystemVFixedOffsetsAndEarlyHistory();
}

TEST_F(TimeZoneMapTest, systemVTransitions) {
  testSystemVTransitions();
}

TEST_F(TimeZoneMapTest, systemVLocalResolution) {
  testSystemVLocalResolution();
}

TEST_F(TimeZoneMapTest, fixedOffsetInfoAtSecondLimits) {
  for (const auto name : {"-14:00", "-05:00", "+05:00", "+14:00"}) {
    SCOPED_TRACE(name);
    const auto* zone = locateZone(name);
    const auto expected = zone->getInfo(date::sys_seconds{});
    for (const auto local :
         {date::local_seconds::min(), date::local_seconds::max()}) {
      const auto actual = zone->getInfo(local);
      EXPECT_EQ(date::local_info::unique, actual.result);
      EXPECT_EQ(expected.offset, actual.first.offset);
      EXPECT_EQ(expected.begin, actual.first.begin);
      EXPECT_EQ(expected.end, actual.first.end);
    }
  }
}

TEST_F(TimeZoneMapTest, systemVInfoAtSecondLimits) {
  for (const auto& reference : kSystemVZones) {
    SCOPED_TRACE(reference.name);
    const auto* zone = detail::locateBuiltinSystemVZone(reference.name);
    ASSERT_NE(nullptr, zone);
    ASSERT_EQ(nullptr, zone->tz());
    EXPECT_EQ(reference.id, zone->id());
    BOLT_ASSERT_THROW(
        zone->getInfo(date::local_seconds::max()), "supported seconds range");
    if (reference.hasDst) {
      BOLT_ASSERT_THROW(
          zone->getInfo(date::sys_seconds::max()), "supported seconds range");
    }
    EXPECT_EQ(
        Seconds(reference.standardOffset),
        zone->getInfo(date::local_seconds::min()).first.offset);
    EXPECT_EQ(
        Seconds(reference.standardOffset),
        zone->getInfo(date::sys_seconds::min()).offset);
    for (const auto seconds :
         {std::numeric_limits<int64_t>::min() / 1000 - 1,
          std::numeric_limits<int64_t>::max() / 1000}) {
      const date::sys_seconds utc{Seconds(seconds)};
      const auto info = zone->getInfo(utc);
      EXPECT_LE(info.begin, utc);
      EXPECT_GT(info.end, utc);
      EXPECT_NO_THROW(zone->getInfo(info.end));
      const auto localInfo =
          zone->getInfo(date::local_seconds{Seconds(seconds)});
      EXPECT_EQ(date::local_info::unique, localInfo.result);
      EXPECT_LE(
          localInfo.first.begin,
          date::sys_seconds{Seconds(seconds) - localInfo.first.offset});
      EXPECT_GT(
          localInfo.first.end,
          date::sys_seconds{Seconds(seconds) - localInfo.first.offset});
    }
  }
  EXPECT_EQ(nullptr, detail::locateBuiltinSystemVZone("SystemV/Unknown"));
}

TEST_F(TimeZoneMapTest, systemVSystemRules) {
  bool hasSystemRules = false;
  for (const auto& reference : kSystemVZones) {
    const auto* zone = locateZone(reference.name);
    if (zone->tz() == nullptr) {
      continue;
    }
    hasSystemRules = true;
    SCOPED_TRACE(reference.name);
    for (const auto count : {126'230'400LL, 1'609'459'200LL, 1'625'097'600LL}) {
      const auto timestamp = Seconds(count);
      const auto expected = zone->tz()->get_info(date::sys_seconds{timestamp});
      const auto actual = zone->getInfo(date::sys_seconds{timestamp});
      EXPECT_EQ(expected.begin, actual.begin);
      EXPECT_EQ(expected.end, actual.end);
      EXPECT_EQ(expected.offset, actual.offset);
      EXPECT_EQ(expected.save, actual.save);
      EXPECT_EQ(expected.abbrev, actual.abbrev);
      EXPECT_EQ(timestamp + expected.offset, zone->to_local(timestamp));
      const auto local = zone->tz()->get_info(date::local_seconds{timestamp});
      EXPECT_EQ(
          local.result, zone->getInfo(date::local_seconds{timestamp}).result);
      EXPECT_EQ(
          zone->tz()->to_sys(date::local_seconds{timestamp}).time_since_epoch(),
          zone->to_sys(timestamp));
    }
  }
  if (!hasSystemRules) {
    GTEST_SKIP() << "SystemV rules are not supplied by the system tzdb";
  }
}

} // namespace
} // namespace bytedance::bolt::tz
