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

#include <cmath>
#include <unordered_map>
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::functions::test;

namespace {

class DownsampleTest : public FunctionBaseTest {
 protected:
  void assertEqualMaps(
      const std::unordered_map<int64_t, double>& expected,
      const std::unordered_map<int64_t, double>& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    for (const auto& [timestamp, value] : expected) {
      auto actualIt = actual.find(timestamp);
      ASSERT_NE(actualIt, actual.end());
      double actualValue = actualIt->second;
      if (value != value) {
        // NaN
        ASSERT_TRUE(actualValue != actualValue);
      } else {
        ASSERT_TRUE(std::abs(value - actualValue) < 0.001);
      }
    }
  }

  void extractResultMap(
      VectorPtr result,
      std::unordered_map<int64_t, double>& resultMap) {
    resultMap.clear();
    auto mapVector = result->as<MapVector>();
    auto mapKeys = mapVector->mapKeys()->template as<SimpleVector<int64_t>>();
    auto mapValues =
        mapVector->mapValues()->template as<SimpleVector<double>>();
    auto size = mapVector->sizeAt(0);
    resultMap.reserve(size);
    for (int i = 0; i < size; i++) {
      auto key = mapKeys->valueAt(i);
      resultMap[key] = mapValues->valueAt(i);
    }
  }

  static constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
};

TEST_F(DownsampleTest, one_minute_downsample) {
  auto data = makeRowVector({makeMapVector<int64_t, double>({{
      {1685322000000, 1}, // 2023-05-29 09:00:00
      {1685322030000, 2}, // 2023-05-29 09:00:30
      {1685322060000, 3}, // 2023-05-29 09:01:00

      {1685322180000, 4}, // 2023-05-29 09:03:00
      {1685322210000, 5}, // 2023-05-29 09:03:30

      {1685322360000, 6}, // 2023-05-29 09:06:00

      {1685322450000, 7}, // 2023-05-29 09:07:30

      {1685322510000, 8}, // 2023-05-29 09:08:30
      {1685322540000, 9}, // 2023-05-29 09:09:00

      {1685322720000, 10}, // 2023-05-29 09:12:00
  }})});
  // 1m-sum-none
  auto result = evaluate("downsample(c0, 60000, 'sum', 'none')", data);
  std::unordered_map<int64_t, double> resultMap;
  extractResultMap(result, resultMap);
  std::unordered_map<int64_t, double> expected = {{
      {1685322000000, 3}, // 2023-05-29 09:00:00
      {1685322060000, 3}, // 2023-05-29 09:01:00
      {1685322180000, 9}, // 2023-05-29 09:03:00
      {1685322360000, 6}, // 2023-05-29 09:06:00
      {1685322420000, 7}, // 2023-05-29 09:07:00
      {1685322480000, 8}, // 2023-05-29 09:08:00
      {1685322540000, 9}, // 2023-05-29 09:09:00
      {1685322720000, 10}, // 2023-05-29 09:12:00
  }};
  assertEqualMaps(expected, resultMap);

  // 1m-avg-zero
  result = evaluate("downsample(c0, 60000, 'avg', 'zero')", data);
  extractResultMap(result, resultMap);
  expected = {
      {
          {1685322000000, 1.5}, // 2023-05-29 09:00:00
          {1685322060000, 3}, // 2023-05-29 09:01:00
          {1685322120000, 0}, // 2023-05-29 09:02:00
          {1685322180000, 4.5}, // 2023-05-29 09:03:00
          {1685322240000, 0}, // 2023-05-29 09:04:00
          {1685322300000, 0}, // 2023-05-29 09:05:00
          {1685322360000, 6}, // 2023-05-29 09:06:00
          {1685322420000, 7}, // 2023-05-29 09:07:00
          {1685322480000, 8}, // 2023-05-29 09:08:00
          {1685322540000, 9}, // 2023-05-29 09:09:00
          {1685322600000, 0}, // 2023-05-29 09:10:00
          {1685322660000, 0}, // 2023-05-29 09:11:00
          {1685322720000, 10}, // 2023-05-29 09:12:00
      },
  };
  assertEqualMaps(expected, resultMap);

  // 1m-max-int
  result = evaluate("downsample(c0, 60000, 'max', 'int')", data);
  extractResultMap(result, resultMap);
  expected = {
      {
          {1685322000000, 2}, // 2023-05-29 09:00:00
          {1685322060000, 3}, // 2023-05-29 09:01:00
          {1685322120000, 4}, // 2023-05-29 09:02:00
          {1685322180000, 5}, // 2023-05-29 09:03:00
          {1685322240000, 5.3333}, // 2023-05-29 09:04:00
          {1685322300000, 5.6666}, // 2023-05-29 09:05:00
          {1685322360000, 6}, // 2023-05-29 09:06:00
          {1685322420000, 7}, // 2023-05-29 09:07:00
          {1685322480000, 8}, // 2023-05-29 09:08:00
          {1685322540000, 9}, // 2023-05-29 09:09:00
          {1685322600000, 9.3333}, // 2023-05-29 09:10:00
          {1685322660000, 9.6666}, // 2023-05-29 09:11:00
          {1685322720000, 10}, // 2023-05-29 09:12:00
      },
  };
  assertEqualMaps(expected, resultMap);

  // 1m-min-nan
  result = evaluate("downsample(c0, 60000, 'min', 'nan')", data);
  extractResultMap(result, resultMap);
  expected = {
      {
          {1685322000000, 1}, // 2023-05-29 09:00:00
          {1685322060000, 3}, // 2023-05-29 09:01:00
          {1685322120000, kNan}, // 2023-05-29 09:02:00
          {1685322180000, 4}, // 2023-05-29 09:03:00
          {1685322240000, kNan}, // 2023-05-29 09:04:00
          {1685322300000, kNan}, // 2023-05-29 09:05:00
          {1685322360000, 6}, // 2023-05-29 09:06:00
          {1685322420000, 7}, // 2023-05-29 09:07:00
          {1685322480000, 8}, // 2023-05-29 09:08:00
          {1685322540000, 9}, // 2023-05-29 09:09:00
          {1685322600000, kNan}, // 2023-05-29 09:10:00
          {1685322660000, kNan}, // 2023-05-29 09:11:00
          {1685322720000, 10}, // 2023-05-29 09:12:00
      },
  };
  assertEqualMaps(expected, resultMap);
}

TEST_F(DownsampleTest, three_minute_downsample) {
  auto data = makeRowVector({makeMapVector<int64_t, double>({{
      {1685322000000, 1}, // 2023-05-29 09:00:00
      {1685322030000, 2}, // 2023-05-29 09:00:30
      {1685322060000, 3}, // 2023-05-29 09:01:00

      {1685322180000, 4}, // 2023-05-29 09:03:00
      {1685322210000, 5}, // 2023-05-29 09:03:30

      {1685322360000, 6}, // 2023-05-29 09:06:00

      {1685322450000, 7}, // 2023-05-29 09:07:30

      {1685322510000, 8}, // 2023-05-29 09:08:30
      {1685322540000, 9}, // 2023-05-29 09:09:00

      {1685322720000, 10}, // 2023-05-29 09:12:00
  }})});
  // 3m-sum-none
  auto result = evaluate("downsample(c0, 180000, 'sum', 'none')", data);
  std::unordered_map<int64_t, double> resultMap;
  extractResultMap(result, resultMap);
  std::unordered_map<int64_t, double> expected = {{
      {1685322000000, 6}, // 2023-05-29 09:00:00
      {1685322180000, 9}, // 2023-05-29 09:03:00
      {1685322360000, 21}, // 2023-05-29 09:06:00
      {1685322540000, 9}, // 2023-05-29 09:09:00
      {1685322720000, 10}, // 2023-05-29 09:12:00
  }};
  assertEqualMaps(expected, resultMap);

  // 3m-avg-zero
  result = evaluate("downsample(c0, 180000, 'avg', 'zero')", data);
  extractResultMap(result, resultMap);
  expected = {
      {
          {1685322000000, 2}, // 2023-05-29 09:00:00
          {1685322180000, 4.5}, // 2023-05-29 09:03:00
          {1685322360000, 7}, // 2023-05-29 09:06:00
          {1685322540000, 9}, // 2023-05-29 09:09:00
          {1685322720000, 10}, // 2023-05-29 09:12:00
      },
  };
  assertEqualMaps(expected, resultMap);

  // 3m-max-int
  result = evaluate("downsample(c0, 180000, 'max', 'int')", data);
  extractResultMap(result, resultMap);
  expected = {
      {
          {1685322000000, 3}, // 2023-05-29 09:00:00
          {1685322180000, 5}, // 2023-05-29 09:03:00
          {1685322360000, 8}, // 2023-05-29 09:06:00
          {1685322540000, 9}, // 2023-05-29 09:09:00
          {1685322720000, 10}, // 2023-05-29 09:12:00
      },
  };
  assertEqualMaps(expected, resultMap);

  // 3m-min-nan
  result = evaluate("downsample(c0, 180000, 'min', 'nan')", data);
  extractResultMap(result, resultMap);
  expected = {
      {
          {1685322000000, 1}, // 2023-05-29 09:00:00
          {1685322180000, 4}, // 2023-05-29 09:03:00
          {1685322360000, 6}, // 2023-05-29 09:06:00
          {1685322540000, 9}, // 2023-05-29 09:09:00
          {1685322720000, 10}, // 2023-05-29 09:12:00
      },
  };
  assertEqualMaps(expected, resultMap);
}

} // namespace
