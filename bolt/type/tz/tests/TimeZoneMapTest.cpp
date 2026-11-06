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

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::tz {
namespace {

using common::testutil::TestValue;

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

} // namespace
} // namespace bytedance::bolt::tz
