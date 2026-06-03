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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"

#include <cstdint>
#include <limits>
namespace bytedance::bolt::functions::flinksql::test {

namespace {
const int64_t LongMinValue = std::numeric_limits<int64_t>::min();

class FlinkSqlDateTimeFunctionsTest : public FlinkFunctionBaseTest {};

TEST_F(FlinkSqlDateTimeFunctionsTest, currentTimestamp) {
  const auto currentTimestampWithTimeZone =
      [&](const std::optional<StringView> timeZone) {
        return evaluateOnce<Timestamp>("current_timestamp(c0)", timeZone);
      };

  EXPECT_TRUE(currentTimestampWithTimeZone("Asia/Shanghai").has_value());
}

TEST_F(FlinkSqlDateTimeFunctionsTest, unixTimestamp) {
  const auto unixTimestamp = [&](const std::optional<std::string>& dateString,
                                 const std::optional<std::string>& format,
                                 const std::string& timeZone) {
    return evaluateOnce<int64_t>(
        fmt::format("flink_unix_timestamp(c0, c1, '{}')", timeZone),
        makeRowVector(
            {makeNullableFlatVector(
                 std::vector<std::optional<std::string>>{dateString}),
             makeNullableFlatVector(
                 std::vector<std::optional<std::string>>{format})}));
  };

  EXPECT_EQ(
      1747843200, unixTimestamp("2025-05-22", "yyyy-MM-dd", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481944,
      unixTimestamp(
          "1900-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481600,
      unixTimestamp(
          "1901-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  // daylight saving time
  EXPECT_EQ(
      675702000,
      unixTimestamp(
          "1991-06-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));

  EXPECT_EQ(
      0,
      unixTimestamp(
          "1970-01-01 08:00:00.000",
          "yyyy-MM-dd HH:mm:ss.SSS",
          "Asia/Shanghai"));
  EXPECT_EQ(
      -28800,
      unixTimestamp(
          "1970-01-01 00:00:00.000",
          "yyyy-MM-dd HH:mm:ss.SSS",
          "Asia/Shanghai"));
  EXPECT_EQ(
      -62135625943,
      unixTimestamp(
          "0001-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481600,
      unixTimestamp(
          "1901-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481944,
      unixTimestamp(
          "1900-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));

  const std::string ISO_8601_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSZZ";
  std::string isoTime = "1970-01-01 05:30:01.00+05:30";
  EXPECT_EQ(1, unixTimestamp(isoTime, ISO_8601_DATE_FORMAT, "Asia/Shanghai"));

  // Empty or malformed input returns null.
  EXPECT_EQ(
      std::nullopt,
      unixTimestamp(
          std::nullopt, "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      std::nullopt,
      unixTimestamp("2025-05-22", std::nullopt, "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp(
          "1970-01-01", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp("00:00:00", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp("", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp(
          "malformed input", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));

  EXPECT_EQ(
      0,
      unixTimestamp(
          "1970-01-01 05:30:00", "yyyy-MM-dd HH:mm:ss", "Asia/Kolkata"));
  EXPECT_EQ(
      5 * 3600,
      unixTimestamp(
          "1970-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "America/Toronto"));
  EXPECT_EQ(
      0, unixTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "UTC"));
}

TEST_F(FlinkSqlDateTimeFunctionsTest, fromUnixtimeWithTimezone) {
  const auto fromUnixtimeOnce = [&](std::optional<int64_t> unixtime,
                                    const std::string& format,
                                    const std::string& timezone) {
    return evaluateOnce<std::string>(
        fmt::format("flink_from_unixtime(c0, '{}', '{}')", format, timezone),
        unixtime);
  };
  EXPECT_EQ(
      "20201228", fromUnixtimeOnce(1609167953, "yyyyMMdd", "Asia/Shanghai"));
  EXPECT_EQ(
      "1970-01-02 11:46:40.000",
      fromUnixtimeOnce(100000, "yyyy-MM-dd HH:mm:ss.SSS", "Asia/Shanghai"));

  EXPECT_EQ(
      "2025-09-11 18:00:00",
      fromUnixtimeOnce(1757615001, "yyyy-MM-dd HH:00:00", "UTC"));

  EXPECT_EQ(
      "2025-09-11 18:00:00",
      fromUnixtimeOnce(1757615001, "yyyy-MM-dd HH:00:00", "Africa/Abidjan"));

  EXPECT_EQ(
      "1970-01-01 08:00:00",
      fromUnixtimeOnce(LongMinValue, "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
}

TEST_F(FlinkSqlDateTimeFunctionsTest, timestampToStringV2) {
  const auto formatOnce = [&](Timestamp timestamp, int32_t precision) {
    return evaluateOnce<std::string>(
        "flink_timestamp_to_string_v2(c0, c1)",
        makeRowVector(
            {makeFlatVector<Timestamp>({timestamp}),
             makeFlatVector<int32_t>({precision})}));
  };

  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56"),
      formatOnce(Timestamp(946729316, 0), 0));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.123"),
      formatOnce(Timestamp(946729316, 123000000), 3));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.123456789"),
      formatOnce(Timestamp(946729316, 123456789), 3));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.000000123"),
      formatOnce(Timestamp(946729316, 123), 9));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.000000123"),
      formatOnce(Timestamp(946729316, 123), 12));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.1"),
      formatOnce(Timestamp(946729316, 100000000), -1));
}
} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
