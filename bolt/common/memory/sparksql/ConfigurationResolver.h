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

#pragma once

#include <folly/Conv.h>
#include <cstdint>
#include <string>
#include <unordered_map>
namespace bytedance::bolt::memory::sparksql {

class ConfigurationResolver final {
 public:
  static std::string getStringParamFromConf(
      const std::unordered_map<std::string, std::string>& conf,
      const std::string& key,
      const std::string& defaultValue) {
    auto it = conf.find(key);
    if (it != conf.end()) {
      return it->second;
    }
    return defaultValue;
  }

  static int64_t getIntParamFromConf(
      const std::unordered_map<std::string, std::string>& conf,
      const std::string& key,
      const std::string& defaultValue) {
    auto val = getStringParamFromConf(conf, key, defaultValue);
    return folly::to<int64_t>(val);
  }

  static bool getBoolParamFromConf(
      const std::unordered_map<std::string, std::string>& conf,
      const std::string& key,
      const std::string& defaultValue) {
    auto val = getStringParamFromConf(conf, key, defaultValue);
    return folly::to<bool>(val);
  }

  static double getDoubleParamFromConf(
      const std::unordered_map<std::string, std::string>& conf,
      const std::string& key,
      const std::string& defaultValue) {
    auto val = getStringParamFromConf(conf, key, defaultValue);
    return folly::to<double>(val);
  }

  static constexpr const char* kBacktraceAllocationPoolRegex =
      "spark.gluten.backtrace.allocation.poolRegex";
  static constexpr const char* kBacktraceAllocationPoolRegexDefaultValue = "";

  static constexpr const char* kBacktraceAllocationSingleThreshold =
      "spark.gluten.backtrace.allocation.singleThreshold";
  static constexpr const char* kBacktraceAllocationSingleThresholdDefaultValue =
      "0";

  static constexpr const char* kBacktraceAllocationAccumulativeThreshold =
      "spark.gluten.backtrace.allocation.accumulativeThreshold";
  static constexpr const char*
      kBacktraceAllocationAccumulativeThresholdDefaultValue = "0";

  static constexpr const char* kMemoryOffHeapBytes =
      "spark.gluten.memory.offHeap.size.in.bytes";
  static constexpr const char* kMemoryOffHeapBytesDefaultValue =
      "9223372036854775807";

  static constexpr const char* kMemoryOffHeap = "spark.memory.offHeap.size";
  static constexpr const char* kMemoryOffHeapDefaultValue =
      "9223372036854775807";

  static constexpr const char* kUseBoltMemoryManager =
      "spark.gluten.useBoltMemoryManager";
  static constexpr const char* kUseBoltMemoryManagerDefaultValue = "true";

  static constexpr const char* kBoltMemoryManagerMaxWaitTimeWhenFree =
      "spark.gluten.boltMemoryManager.maxWaitTimeWhenFree";
  static constexpr const char*
      kBoltMemoryManagerMaxWaitTimeWhenFreeDefaultValue = "180000";

  static constexpr const char* kExecutionPoolMinMemoryMaxWaitMs =
      "spark.gluten.boltExecutionPool.minMemoryMaxWaitTime";
  static constexpr const char* kExecutionPoolMinMemoryMaxWaitMsDefaultValue =
      "300000";

  static constexpr const char* kSparkExecutorCores = "spark.executor.cores";
  static constexpr const char* kSparkExecutorCoresDefaultValue = "1";

  static constexpr const char* kSparkVCoreBoostRatio =
      "spark.vcore.boost.ratio";
  static constexpr const char* kSparkVCoreBoostRatioDefaultValue = "1";

  static constexpr const char* kDynamicMemoryQuotaManager =
      "spark.gluten.boltMemoryManager.enableDynamicMemoryQuotaManager";
  static constexpr const char* kDynamicMemoryQuotaManagerDefaultValue = "true";

  static constexpr const char* kDynamicMemoryQuotaManagerRatios =
      "spark.gluten.boltMemoryManager.dynamicMemoryQuotaManager.ratios";
  static constexpr const char* kDynamicMemoryQuotaManagerRatiosDefaultValue =
      "0.5|0.9|1.0|1.0|6.0|1.0|0.05|0.0|0.05|";

  // arbitrator extra config keys
  static constexpr const char* kMemoryReclaimMaxWaitMs =
      "memory-reclaim-max-wait-time";
  static constexpr const char* kDefaultMemoryReclaimMaxWaitMs = "3600000ms";
};
} // namespace bytedance::bolt::memory::sparksql
