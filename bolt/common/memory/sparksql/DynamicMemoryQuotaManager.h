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

#include <fmt/core.h>
#include <cstdint>
#include <string>

#include "bolt/common/base/SuccinctPrinter.h"
namespace bytedance::bolt::memory::sparksql {

struct DynamicMemoryQuotaManagerOption {
  bool enable;
  double quotaTriggerRatio;
  double rssMinRatio;
  double rssMaxRatio;
  double extendMinRatio;
  double extendMaxRatio;
  double extendScaleRatio;
  double sampleRatio;
  int64_t sampleSize;
  double changeThresholdRatio;
  double logPrintFreq;

  enum RatioKey {
    kQuotaTriggerRatio = 0,
    kRssMinRatio,
    kRssMaxRatio,
    kExtendMinRatio,
    kExtendMaxRatio,
    kExtendScaleRatio,
    kSampleRatio,
    kChangeThresholdRatio,
    kLogPrintFreq,
    KEY_NUM
  };

  std::string toString() const {
    return fmt::format(
        "enable={}, quotaTriggerRatio={}, rssMinRatio={}, rssMaxRatio={}, "
        "extendMaxRatio={}, extendMinRatio={}, extendScaleRatio={}, sampleRatio={}, sampleSize={}"
        ", changeThresholdRatio={}, logPrintFreq={}",
        enable,
        quotaTriggerRatio,
        rssMinRatio,
        rssMaxRatio,
        extendMaxRatio,
        extendMinRatio,
        extendScaleRatio,
        sampleRatio,
        succinctBytes(sampleSize),
        changeThresholdRatio,
        logPrintFreq);
  }
};

inline std::string succinctBytesPrinter(int64_t size) {
  return size >= 0 ? (succinctBytes(size))
                   : fmt::format("-{}", succinctBytes(-size));
};

struct DynamicMemoryQuotaManagerStatistics {
  int64_t extendCount{0};
  uint64_t extendTotalTimeCost{0}, extendLogTimeCost{0}, extendApiTimeCost{0};
  int64_t minExtendSize{INT64_MAX}, maxExtendSize{INT64_MIN};
  int64_t totalExtendSize{0};

  void updateExtendSize(int64_t newValue) {
    minExtendSize = std::min(minExtendSize, newValue);
    maxExtendSize = std::max(maxExtendSize, newValue);
    totalExtendSize += newValue;
    extendCount++;
  }

  std::string toString() const {
    return fmt::format(
        "Total extend {} times, {}. Total time cost {}, log time cost {}, "
        "api time cost {}, minExtendSize is {}, maxExtendSize is {}, totalExtendSize is {}, "
        "avgExtendSize is {}",
        extendCount,
        succinctBytesPrinter(totalExtendSize),
        succinctNanos(extendTotalTimeCost),
        succinctNanos(extendLogTimeCost),
        succinctNanos(extendApiTimeCost),
        succinctBytesPrinter(minExtendSize),
        succinctBytesPrinter(maxExtendSize),
        succinctBytesPrinter(totalExtendSize),
        succinctBytesPrinter(totalExtendSize / extendCount));
  }
};

} // namespace bytedance::bolt::memory::sparksql
