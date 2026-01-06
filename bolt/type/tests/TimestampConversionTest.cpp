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

#include "bolt/type/TimestampConversion.h"

#include <date/tz.h>
#include <gmock/gmock.h>
#include <cstdint>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::util {
namespace {

TEST(DateTimeUtilTest, fromDate) {
  EXPECT_EQ(0, daysSinceEpochFromDate(1970, 1, 1));
  EXPECT_EQ(1, daysSinceEpochFromDate(1970, 1, 2));
  EXPECT_EQ(365, daysSinceEpochFromDate(1971, 1, 1));
  EXPECT_EQ(730, daysSinceEpochFromDate(1972, 1, 1)); // leap year.
  EXPECT_EQ(1096, daysSinceEpochFromDate(1973, 1, 1));

  EXPECT_EQ(10957, daysSinceEpochFromDate(2000, 1, 1));
  EXPECT_EQ(18474, daysSinceEpochFromDate(2020, 7, 31));

  // Before unix epoch.
  EXPECT_EQ(-1, daysSinceEpochFromDate(1969, 12, 31));
  EXPECT_EQ(-365, daysSinceEpochFromDate(1969, 1, 1));
  EXPECT_EQ(-731, daysSinceEpochFromDate(1968, 1, 1)); // leap year.
  EXPECT_EQ(-719528, daysSinceEpochFromDate(0, 1, 1));

  // Negative year - BC.
  EXPECT_EQ(-719529, daysSinceEpochFromDate(-1, 12, 31));
  EXPECT_EQ(-719893, daysSinceEpochFromDate(-1, 1, 1));
}

TEST(DateTimeUtilTest, fromDateInvalid) {
  EXPECT_THROW(daysSinceEpochFromDate(1970, 1, -1), BoltUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, -1, 1), BoltUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, 0, 1), BoltUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, 13, 1), BoltUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, 1, 32), BoltUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, 2, 29), BoltUserError); // non-leap.
  EXPECT_THROW(daysSinceEpochFromDate(1970, 6, 31), BoltUserError);
}

TEST(DateTimeUtilTest, fromDateString) {
  EXPECT_EQ(10957, fromDateString("2000-01-01", nullptr));
  EXPECT_EQ(0, fromDateString("1970-01-01", nullptr));
  EXPECT_EQ(1, fromDateString("1970-01-02", nullptr));

  // Single character
  EXPECT_EQ(1, fromDateString("1970-1-2", nullptr));

  // Old and negative years.
  EXPECT_EQ(-719528, fromDateString("0-1-1", nullptr));
  EXPECT_EQ(-719162, fromDateString("1-1-1", nullptr));
  EXPECT_EQ(-719893, fromDateString("-1-1-1", nullptr));
  EXPECT_EQ(-720258, fromDateString("-2-1-1", nullptr));

// 1BC is equal 0-1-1.
#ifndef SPARK_COMPATIBLE
  EXPECT_EQ(-719528, fromDateString("1-1-1 (BC)", nullptr));
  EXPECT_EQ(-719893, fromDateString("2-1-1 (BC)", nullptr));
#endif

  // Leading zeros and spaces.
  EXPECT_EQ(-719162, fromDateString("00001-1-1", nullptr));
  EXPECT_EQ(-719162, fromDateString(" 1-1-1", nullptr));
  EXPECT_EQ(-719162, fromDateString("     1-1-1", nullptr));
  EXPECT_EQ(-719162, fromDateString("\t1-1-1", nullptr));
  EXPECT_EQ(-719162, fromDateString("  \t    \n 00001-1-1  \n", nullptr));

  // Different separators.
  EXPECT_EQ(-719162, fromDateString("1/1/1", nullptr));
  EXPECT_EQ(-719162, fromDateString("1 1 1", nullptr));
  EXPECT_EQ(-719162, fromDateString("1\\1\\1", nullptr));

  // Other string types.
  EXPECT_EQ(0, fromDateString(StringView("1970-01-01"), nullptr));
}

TEST(DateTimeUtilTest, fromDateStrInvalid) {
  EXPECT_THROW(fromDateString("", nullptr), BoltUserError);
  EXPECT_THROW(fromDateString("     ", nullptr), BoltUserError);
  EXPECT_THROW(fromDateString("2000", nullptr), BoltUserError);

  // Different separators.
  EXPECT_THROW(fromDateString("2000/01-01", nullptr), BoltUserError);
  EXPECT_THROW(fromDateString("2000 01-01", nullptr), BoltUserError);

#ifndef SPARK_COMPATIBLE
  // Trailing characters.
  EXPECT_THROW(fromDateString("2000-01-01   asdf", nullptr), BoltUserError);
  EXPECT_THROW(fromDateString("2000-01-01 0", nullptr), BoltUserError);

  // Too large of a year.
  EXPECT_THROW(fromDateString("1000000", nullptr), BoltUserError);
  EXPECT_THROW(fromDateString("-1000000", nullptr), BoltUserError);
#endif
}

TEST(DateTimeUtilTest, castFromDateString) {
  for (bool isIso8601 : {true, false}) {
    EXPECT_EQ(0, castFromDateString("1970-01-01", isIso8601));
    EXPECT_EQ(3789742, castFromDateString("12345-12-18", isIso8601));

    EXPECT_EQ(1, castFromDateString("1970-1-2", isIso8601));
    EXPECT_EQ(1, castFromDateString("1970-01-2", isIso8601));
    EXPECT_EQ(1, castFromDateString("1970-1-02", isIso8601));

    EXPECT_EQ(1, castFromDateString("+1970-01-02", isIso8601));

    EXPECT_EQ(0, castFromDateString(" 1970-01-01", isIso8601));
  }

  EXPECT_EQ(-719893, castFromDateString("-1-1-1", true));
  EXPECT_EQ(std::nullopt, castFromDateString("-1-1-1", false));

  EXPECT_EQ(3789391, castFromDateString("12345", false));
  EXPECT_EQ(16436, castFromDateString("2015", false));
  EXPECT_EQ(16495, castFromDateString("2015-03", false));
  EXPECT_EQ(16512, castFromDateString("2015-03-18T", false));
  EXPECT_EQ(16512, castFromDateString("2015-03-18T123123", false));
  EXPECT_EQ(16512, castFromDateString("2015-03-18 123142", false));
  EXPECT_EQ(16512, castFromDateString("2015-03-18 (BC)", false));

  EXPECT_EQ(0, castFromDateString("1970-01-01 ", false));
  EXPECT_EQ(0, castFromDateString(" 1970-01-01 ", false));
}

TEST(DateTimeUtilTest, castFromDateStringInvalid) {
  auto testCastFromDateStringInvalid = [&](const StringView& str,
                                           bool isIso8601) {
    EXPECT_EQ(castFromDateString(str, isIso8601), std::nullopt);
  };

  for (bool isIso8601 : {true, false}) {
    testCastFromDateStringInvalid("2012-Oct-23", isIso8601);
    testCastFromDateStringInvalid("2012-Oct-23", isIso8601);
    testCastFromDateStringInvalid("2015-03-18X", isIso8601);
    testCastFromDateStringInvalid("2015/03/18", isIso8601);
    testCastFromDateStringInvalid("2015.03.18", isIso8601);
    testCastFromDateStringInvalid("20150318", isIso8601);
    testCastFromDateStringInvalid("2015-031-8", isIso8601);
  }

  testCastFromDateStringInvalid("12345", true);
  testCastFromDateStringInvalid("2015", true);
  testCastFromDateStringInvalid("2015-03", true);
  testCastFromDateStringInvalid("2015-03-18 123412", true);
  testCastFromDateStringInvalid("2015-03-18T", true);
  testCastFromDateStringInvalid("2015-03-18T123412", true);
  testCastFromDateStringInvalid("2015-03-18 (BC)", true);

  testCastFromDateStringInvalid("1970-01-01 ", true);
  testCastFromDateStringInvalid(" 1970-01-01 ", true);
}

TEST(DateTimeUtilTest, fromTimeString) {
  EXPECT_EQ(0, fromTimeString("00:00:00", nullptr));
  EXPECT_EQ(0, fromTimeString("00:00:00.00", nullptr));
  EXPECT_EQ(1, fromTimeString("00:00:00.000001", nullptr));
  EXPECT_EQ(10, fromTimeString("00:00:00.00001", nullptr));
  EXPECT_EQ(100, fromTimeString("00:00:00.0001", nullptr));
  EXPECT_EQ(1000, fromTimeString("00:00:00.001", nullptr));
  EXPECT_EQ(10000, fromTimeString("00:00:00.01", nullptr));
  EXPECT_EQ(100000, fromTimeString("00:00:00.1", nullptr));
  EXPECT_EQ(1'000'000, fromTimeString("00:00:01", nullptr));
  EXPECT_EQ(60'000'000, fromTimeString("00:01:00", nullptr));
  EXPECT_EQ(3'600'000'000, fromTimeString("01:00:00", nullptr));

  // 1 day minus 1 second.
  EXPECT_EQ(86'399'000'000, fromTimeString("23:59:59", nullptr));

  // Single digit.
  EXPECT_EQ(0, fromTimeString("0:0:0.0", nullptr));
  EXPECT_EQ(3'661'000'000, fromTimeString("1:1:1", nullptr));

  // Leading and trailing spaces.
  EXPECT_EQ(0, fromTimeString("   \t \n 00:00:00.00  \t", nullptr));
}

#ifndef SPARK_COMPATIBLE
TEST(DateTimeUtilTest, fromTimeStrInvalid) {
  EXPECT_THROW(fromTimeString("", nullptr), BoltUserError);
  EXPECT_THROW(fromTimeString("00", nullptr), BoltUserError);
  EXPECT_THROW(fromTimeString("00:00", nullptr), BoltUserError);

  // Invalid hour, minutes and seconds.
  EXPECT_THROW(fromTimeString("24:00:00", nullptr), BoltUserError);
  EXPECT_THROW(fromTimeString("00:61:00", nullptr), BoltUserError);
  EXPECT_THROW(fromTimeString("00:00:61", nullptr), BoltUserError);

  // Trailing characters.
  EXPECT_THROW(fromTimeString("00:00:00   12", nullptr), BoltUserError);
}
#endif

// bash command to verify:
// $ date -d "2000-01-01 12:21:56Z" +%s
// ('Z' at the end means UTC).
TEST(DateTimeUtilTest, fromTimestampString) {
  EXPECT_EQ(Timestamp(0, 0), fromTimestampString("1970-01-01", nullptr));
  EXPECT_EQ(
      Timestamp(946684800, 0), fromTimestampString("2000-01-01", nullptr));

  EXPECT_EQ(
      Timestamp(0, 0), fromTimestampString("1970-01-01 00:00:00", nullptr));
  EXPECT_EQ(
      Timestamp(946729316, 0),
      fromTimestampString("2000-01-01 12:21:56", nullptr));
  EXPECT_EQ(
      Timestamp(946729316, 0),
      fromTimestampString("2000-01-01T12:21:56", nullptr));
  EXPECT_EQ(
      Timestamp(946729316, 0),
      fromTimestampString("2000-01-01T 12:21:56", nullptr));

  // Test UTC offsets.
  EXPECT_EQ(
      Timestamp(7200, 0),
      fromTimestampString("1970-01-01 00:00:00-02:00", nullptr));
  EXPECT_EQ(
      Timestamp(946697400, 0),
      fromTimestampString("2000-01-01 00:00:00Z-03:30", nullptr));
  EXPECT_EQ(
      Timestamp(1587583417, 0),
      fromTimestampString("2020-04-23 04:23:37+09:00", nullptr));
}

TEST(DateTimeUtilTest, fromTimestampStrInvalid) {
  // Needs at least a date.
  EXPECT_THROW(fromTimestampString("", nullptr), BoltUserError);
  EXPECT_THROW(fromTimestampString("00:00:00", nullptr), BoltUserError);

  // Broken UTC offsets.
  EXPECT_THROW(
      fromTimestampString("1970-01-01 00:00:00-asd", nullptr), BoltUserError);
  EXPECT_THROW(
      fromTimestampString("1970-01-01 00:00:00+00:00:00", nullptr),
      BoltUserError);

  // Integer overflow during timestamp parsing.
  EXPECT_THROW(
      fromTimestampString("2773581570-01-01 00:00:00-asd", nullptr),
      BoltUserError);
  EXPECT_THROW(
      fromTimestampString("-2147483648-01-01 00:00:00-asd", nullptr),
      BoltUserError);
}

TEST(DateTimeUtilTest, fromTimestampWithTimezoneString) {
  // -1 means no timezone information.
  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:00:00"),
      std::make_pair(Timestamp(0, 0), (int16_t)-1));

  // Test timezone offsets.
  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:00:00 -02:00"),
      std::make_pair(Timestamp(0, 0), tz::getTimeZoneID("-02:00")));
  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:00:00+13:36"),
      std::make_pair(Timestamp(0, 0), tz::getTimeZoneID("+13:36")));

  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:00:00Z"),
      std::make_pair(Timestamp(0, 0), tz::getTimeZoneID("UTC")));

  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01T00:00:00Z"),
      std::make_pair(Timestamp(0, 0), tz::getTimeZoneID("UTC")));

  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:01:00 UTC"),
      std::make_pair(Timestamp(60, 0), tz::getTimeZoneID("UTC")));

  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:01:00 UTC-02:00"),
      std::make_pair(Timestamp(60, 0), tz::getTimeZoneID("-02:00")));

  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:01:00 UT-02:00"),
      std::make_pair(Timestamp(60, 0), tz::getTimeZoneID("-02:00")));

  EXPECT_EQ(
      fromTimestampWithTimezoneString("1970-01-01 00:01:00 GMT+13:36"),
      std::make_pair(Timestamp(60, 0), tz::getTimeZoneID("+13:36")));

  EXPECT_EQ(
      fromTimestampWithTimezoneString(
          "1970-01-01 00:00:01 America/Los_Angeles"),
      std::make_pair(
          Timestamp(1, 0), tz::getTimeZoneID("America/Los_Angeles")));
}

TEST(DateTimeUtilTest, toGMT) {
  auto* laZone = tz::locateZone("America/Los_Angeles");

  // The GMT time when LA gets to "1970-01-01 00:00:00" (8h ahead).
  auto ts = fromTimestampString("1970-01-01 00:00:00", nullptr);
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, fromTimestampString("1970-01-01 08:00:00", nullptr));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37", nullptr);

  // To LA:
  auto tsCopy = ts;
  tsCopy.toGMT(*laZone);
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 11:23:37", nullptr));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37", nullptr));

  // Moscow:
  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37", nullptr));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00", nullptr);
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 08:00:00", nullptr));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-14 08:00:00", nullptr);
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 15:00:00", nullptr));

  // Ambiguous time 2019-11-03 01:00:00.
  // It could be 2019-11-03 01:00:00 PDT == 2019-11-03 08:00:00 UTC
  // or 2019-11-03 01:00:00 PST == 2019-11-03 09:00:00 UTC.
  ts = fromTimestampString("2019-11-03 01:00:00", nullptr);
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2019-11-03 08:00:00", nullptr));

  // Nonexistent time 2019-03-10 02:00:00.
  // It is in a gap between 2019-03-10 02:00:00 PST and 2019-03-10 03:00:00 PDT
  // which are both equivalent to 2019-03-10 10:00:00 UTC.
  ts = fromTimestampString("2019-03-10 02:00:00", nullptr);
  EXPECT_THROW(ts.toGMT(*laZone), BoltUserError);
}

TEST(DateTimeUtilTest, toTimezone) {
  auto* laZone = tz::locateZone("America/Los_Angeles");

  // The LA time when GMT gets to "1970-01-01 00:00:00" (8h behind).
  auto ts = fromTimestampString("1970-01-01 00:00:00", nullptr);
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, fromTimestampString("1969-12-31 16:00:00", nullptr));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37", nullptr);

  // To LA:
  auto tsCopy = ts;
  tsCopy.toTimezone(*laZone);
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 21:23:37", nullptr));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37", nullptr));

  // Moscow:
  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37", nullptr));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00", nullptr);
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-13 16:00:00", nullptr));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-15 00:00:00", nullptr);
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 17:00:00", nullptr));
}

TEST(DateTimeUtilTest, toGMTFromID) {
  // The GMT time when LA gets to "1970-01-01 00:00:00" (8h ahead).
  auto ts = fromTimestampString("1970-01-01 00:00:00", nullptr);
  ts.toGMT(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("1970-01-01 08:00:00", nullptr));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37", nullptr);

  // To LA:
  auto tsCopy = ts;
  tsCopy.toGMT(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 11:23:37", nullptr));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toGMT(tz::getTimeZoneID("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37", nullptr));

  // Moscow:
  tsCopy = ts;
  tsCopy.toGMT(tz::getTimeZoneID("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37", nullptr));

  // Numerical time zones: +HH:MM and -HH:MM
  tsCopy = ts;
  tsCopy.toGMT(tz::getTimeZoneID("+14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 14:23:37", nullptr));

  tsCopy = ts;
  tsCopy.toGMT(tz::getTimeZoneID("-14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 18:23:37", nullptr));

  tsCopy = ts;
  tsCopy.toGMT(0); // "+00:00" is not in the time zone id map
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:23:37", nullptr));

  tsCopy = ts;
  tsCopy.toGMT(tz::getTimeZoneID("-00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:24:37", nullptr));

  tsCopy = ts;
  tsCopy.toGMT(tz::getTimeZoneID("+00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:22:37", nullptr));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00", nullptr);
  ts.toGMT(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 08:00:00", nullptr));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-15 00:00:00", nullptr);
  ts.toGMT(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-15 07:00:00", nullptr));
}

TEST(DateTimeUtilTest, toTimezoneFromID) {
  // The LA time when GMT gets to "1970-01-01 00:00:00" (8h behind).
  auto ts = fromTimestampString("1970-01-01 00:00:00", nullptr);
  ts.toTimezone(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("1969-12-31 16:00:00", nullptr));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37", nullptr);

  // To LA:
  auto tsCopy = ts;
  tsCopy.toTimezone(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 21:23:37", nullptr));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toTimezone(tz::getTimeZoneID("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37", nullptr));

  // Moscow:
  tsCopy = ts;
  tsCopy.toTimezone(tz::getTimeZoneID("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37", nullptr));

  // Numerical time zones: +HH:MM and -HH:MM
  tsCopy = ts;
  tsCopy.toTimezone(tz::getTimeZoneID("+14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 18:23:37", nullptr));

  tsCopy = ts;
  tsCopy.toTimezone(tz::getTimeZoneID("-14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 14:23:37", nullptr));

  tsCopy = ts;
  tsCopy.toTimezone(0); // "+00:00" is not in the time zone id map
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:23:37", nullptr));

  tsCopy = ts;
  tsCopy.toTimezone(tz::getTimeZoneID("-00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:22:37", nullptr));

  tsCopy = ts;
  tsCopy.toTimezone(tz::getTimeZoneID("+00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:24:37", nullptr));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00", nullptr);
  ts.toTimezone(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-13 16:00:00", nullptr));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-15 00:00:00", nullptr);
  ts.toTimezone(tz::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 17:00:00", nullptr));
}

} // namespace
} // namespace bytedance::bolt::util
