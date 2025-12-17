/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

//
// Created by Jenson on 2023/5/23.
//

#pragma once

#include <limits>
#include <unordered_map>
#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/Macros.h"
#include "bolt/type/StringView.h"
namespace bytedance::bolt::functions {

template <typename T>
struct DownsampleFunction {
  inline bolt::StringView aggStrSum() {
    static bolt::StringView sum = "sum";
    return sum;
  }
  inline bolt::StringView aggStrAvg() {
    static bolt::StringView avg = "avg";
    return avg;
  }
  inline bolt::StringView aggStrMax() {
    static bolt::StringView max = "max";
    return max;
  }
  inline bolt::StringView aggStrMin() {
    static bolt::StringView min = "min";
    return min;
  }
  inline bolt::StringView aggStrCount() {
    static bolt::StringView count = "count";
    return count;
  }

  inline bolt::StringView fillPolicyStrNone() {
    static bolt::StringView str = "none";
    return str;
  }
  inline bolt::StringView fillPolicyStrNaN() {
    static bolt::StringView str = "nan";
    return str;
  }
  inline bolt::StringView fillPolicyStrZero() {
    static bolt::StringView str = "zero";
    return str;
  }
  // linear interpolation
  inline bolt::StringView fillPolicyStrInt() {
    static bolt::StringView str = "int";
    return str;
  }

  BOLT_DEFINE_FUNCTION_TYPES(T);

  // This method takes and returns a Map. Map proxy objects are implemented
  // using std::unordered_map; values are wrapped by std::optional.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Map<int64_t, double>>& result,
      const arg_type<Map<int64_t, double>>& inputMap,
      const int64_t& interval,
      const arg_type<Varchar>& aggregate,
      const arg_type<Varchar>& fillPolicy) {
    // out_type<Map<int64_t, double>> only supports limited function,
    // so we have to use an intermediate Map to hold the result first.
    std::unordered_map<int64_t, double> intermediateResult;
    int64_t intermediateResultStartTime = std::numeric_limits<int64_t>::max();
    int64_t intermediateResultEndTime = -1;
    // aggregate first
    if (aggStrSum() == aggregate) {
      aggSum(
          inputMap,
          interval,
          intermediateResult,
          intermediateResultStartTime,
          intermediateResultEndTime);
    } else if (aggStrAvg() == aggregate) {
      aggAvg(
          inputMap,
          interval,
          intermediateResult,
          intermediateResultStartTime,
          intermediateResultEndTime);
    } else if (aggStrMax() == aggregate) {
      aggMax(
          inputMap,
          interval,
          intermediateResult,
          intermediateResultStartTime,
          intermediateResultEndTime);
    } else if (aggStrMin() == aggregate) {
      aggMin(
          inputMap,
          interval,
          intermediateResult,
          intermediateResultStartTime,
          intermediateResultEndTime);
    } else if (aggStrCount() == aggregate) {
      aggCount(
          inputMap,
          interval,
          intermediateResult,
          intermediateResultStartTime,
          intermediateResultEndTime);
    }
    // then do the interpolation with given fill policy.
    if (fillPolicyStrNone() != fillPolicy) {
      auto dataPointNum = intermediateResult.size();
      int64_t expectDataPointNum =
          (intermediateResultEndTime - intermediateResultStartTime) / interval +
          1;
      if (dataPointNum < expectDataPointNum) {
        if (fillPolicyStrZero() == fillPolicy) {
          fillWithZero(
              intermediateResult,
              interval,
              intermediateResultStartTime,
              intermediateResultEndTime);
        } else if (fillPolicyStrInt() == fillPolicy) {
          fillWithInt(
              intermediateResult,
              interval,
              intermediateResultStartTime,
              intermediateResultEndTime);
        } else if (fillPolicyStrNaN() == fillPolicy) {
          fillWithNaN(
              intermediateResult,
              interval,
              intermediateResultStartTime,
              intermediateResultEndTime);
        }
      }
    }
    result.reserve(intermediateResult.size());
    for (const auto& [timestamp, value] : intermediateResult) {
      result.emplace(timestamp, value);
    }
  }

  inline void aggSum(
      const arg_type<Map<int64_t, double>>& inputMap,
      const int64_t& interval,
      std::unordered_map<int64_t, double>& intermediateResult,
      int64_t& startTime,
      int64_t& endTime) {
    for (const auto& [timestamp, valueAccessor] : inputMap) {
      int64_t alignedTimestamp = timestamp - timestamp % interval;
      intermediateResult[alignedTimestamp] =
          intermediateResult[alignedTimestamp] + valueAccessor.value();
      startTime = startTime < alignedTimestamp ? startTime : alignedTimestamp;
      endTime = endTime > alignedTimestamp ? endTime : alignedTimestamp;
    }
  }

  inline void aggAvg(
      const arg_type<Map<int64_t, double>>& inputMap,
      const int64_t& interval,
      std::unordered_map<int64_t, double>& intermediateResult,
      int64_t& startTime,
      int64_t& endTime) {
    std::unordered_map<int64_t, int64_t> intermediateCountResult;
    for (const auto& [timestamp, valueAccessor] : inputMap) {
      int64_t alignedTimestamp = timestamp - timestamp % interval;
      intermediateResult[alignedTimestamp] =
          intermediateResult[alignedTimestamp] + valueAccessor.value();
      intermediateCountResult[alignedTimestamp] =
          intermediateCountResult[alignedTimestamp] + 1;
      startTime = startTime < alignedTimestamp ? startTime : alignedTimestamp;
      endTime = endTime > alignedTimestamp ? endTime : alignedTimestamp;
    }
    for (const auto& [timestamp, value] : intermediateResult) {
      intermediateResult[timestamp] =
          value / intermediateCountResult[timestamp];
    }
  }

  inline void aggMax(
      const arg_type<Map<int64_t, double>>& inputMap,
      const int64_t& interval,
      std::unordered_map<int64_t, double>& intermediateResult,
      int64_t& startTime,
      int64_t& endTime) {
    for (const auto& [timestamp, valueAccessor] : inputMap) {
      int64_t alignedTimestamp = timestamp - timestamp % interval;
      double intermediateValue = intermediateResult[alignedTimestamp];
      double currentValue = valueAccessor.value();
      intermediateResult[alignedTimestamp] =
          currentValue > intermediateValue ? currentValue : intermediateValue;
      startTime = startTime < alignedTimestamp ? startTime : alignedTimestamp;
      endTime = endTime > alignedTimestamp ? endTime : alignedTimestamp;
    }
  }

  inline void aggMin(
      const arg_type<Map<int64_t, double>>& inputMap,
      const int64_t& interval,
      std::unordered_map<int64_t, double>& intermediateResult,
      int64_t& startTime,
      int64_t& endTime) {
    const double MAX_VALUE = std::numeric_limits<double>::max();
    for (const auto& [timestamp, valueAccessor] : inputMap) {
      int64_t alignedTimestamp = timestamp - timestamp % interval;
      auto it = intermediateResult.find(alignedTimestamp);
      double intermediateValue = MAX_VALUE;
      if (it != intermediateResult.end()) {
        intermediateValue = it->second;
      }
      double currentValue = valueAccessor.value();
      intermediateResult[alignedTimestamp] =
          currentValue < intermediateValue ? currentValue : intermediateValue;
      startTime = startTime < alignedTimestamp ? startTime : alignedTimestamp;
      endTime = endTime > alignedTimestamp ? endTime : alignedTimestamp;
    }
  }

  inline void aggCount(
      const arg_type<Map<int64_t, double>>& inputMap,
      const int64_t& interval,
      std::unordered_map<int64_t, double>& intermediateResult,
      int64_t& startTime,
      int64_t& endTime) {
    for (const auto& [timestamp, valueAccessor] : inputMap) {
      int64_t alignedTimestamp = timestamp - timestamp % interval;
      intermediateResult[alignedTimestamp] =
          intermediateResult[alignedTimestamp] + 1;
      startTime = startTime < alignedTimestamp ? startTime : alignedTimestamp;
      endTime = endTime > alignedTimestamp ? endTime : alignedTimestamp;
    }
  }

  inline void fillWithZero(
      std::unordered_map<int64_t, double>& dataMap,
      const int64_t& interval,
      int64_t startTime,
      int64_t endTime) {
    for (int64_t time = startTime; time <= endTime; time += interval) {
      if (dataMap.find(time) == dataMap.end()) {
        // missing data point at time
        dataMap[time] = 0;
      }
    }
  }

  inline void fillWithInt(
      std::unordered_map<int64_t, double>& dataMap,
      const int64_t& interval,
      int64_t startTime,
      int64_t endTime) {
    // TODO: this is just a quick implementation. Optimize it if it is a
    // bottleneck.
    int64_t currentTime = startTime;
    int64_t lastTimestamp = -1, nextTimestamp = -1;
    double lastValue = 0, nextValue = 0;
    while (currentTime <= endTime) {
      auto it = dataMap.find(currentTime);
      if (it == dataMap.end()) {
        /** missing data point at time **/
        // should never happen
        BOLT_CHECK(lastTimestamp != -1);
        // find next timestamp/value
        bool found = true;
        if (currentTime > nextTimestamp) {
          found = findFirstTimestampAndValue(
              dataMap,
              interval,
              currentTime + interval,
              endTime,
              nextTimestamp,
              nextValue);
        }
        if (!found) {
          // Current missing timestamp is the end timestamp. Do nothing
        } else {
          double coef =
              (nextValue - lastValue) / (nextTimestamp - lastTimestamp);
          double currentValue =
              lastValue + (currentTime - lastTimestamp) * coef;
          dataMap[currentTime] = currentValue;
          lastTimestamp = currentTime;
          lastValue = currentValue;
        }
      } else {
        lastTimestamp = currentTime;
        lastValue = it->second;
      }
      currentTime += interval;
    }
  }

  // return false if not found
  bool findFirstTimestampAndValue(
      std::unordered_map<int64_t, double>& dataMap,
      const int64_t& interval,
      int64_t startTime,
      int64_t endTime,
      int64_t& firstTimestamp,
      double& firstValue) {
    if (dataMap.empty()) {
      return false;
    }
    for (int64_t time = startTime; time <= endTime; time += interval) {
      auto it = dataMap.find(time);
      if (it != dataMap.end()) {
        firstTimestamp = time;
        firstValue = it->second;
        return true;
      }
    }
    return false;
  }

  inline void fillWithNaN(
      std::unordered_map<int64_t, double>& dataMap,
      const int64_t& interval,
      int64_t startTime,
      int64_t endTime) {
    const double naN = std::numeric_limits<double>::quiet_NaN();
    for (int64_t time = startTime; time <= endTime; time += interval) {
      if (dataMap.find(time) == dataMap.end()) {
        // missing data point at time
        dataMap[time] = naN;
      }
    }
  }
};

} // namespace bytedance::bolt::functions
