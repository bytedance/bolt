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

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <folly/dynamic.h>

#define K4 4096 // 4k = 4*1024
#define K32 32768 // 32k = 32*1024
#define K128 131072 // 128k = 128*1024
namespace bytedance::bolt::io {

struct OperationCounters {
  uint64_t resourceThrottleCount{0};
  uint64_t localThrottleCount{0};
  uint64_t globalThrottleCount{0};
  uint64_t retryCount{0};
  uint64_t latencyInMs{0};
  uint64_t requestCount{0};
  uint64_t delayInjectedInSecs{0};

  void merge(const OperationCounters& other);
};

class IoCounter {
 public:
  uint64_t count() const {
    return count_;
  }

  uint64_t sum() const {
    return sum_;
  }

  uint64_t min() const {
    return min_;
  }

  uint64_t max() const {
    return max_;
  }

  void increment(uint64_t amount) {
    ++count_;
    sum_ += amount;
    casLoop(min_, amount, std::greater());
    casLoop(max_, amount, std::less());
  }

  void merge(const IoCounter& other) {
    sum_ += other.sum_;
    count_ += other.count_;
    casLoop(min_, other.min_, std::greater());
    casLoop(max_, other.max_, std::less());
  }

 private:
  template <typename Compare>
  static void
  casLoop(std::atomic<uint64_t>& value, uint64_t newValue, Compare compare) {
    uint64_t old = value;
    while (compare(old, newValue) &&
           !value.compare_exchange_weak(old, newValue)) {
    }
  }

  std::atomic<uint64_t> count_{0};
  std::atomic<uint64_t> sum_{0};
  std::atomic<uint64_t> min_{std::numeric_limits<uint64_t>::max()};
  std::atomic<uint64_t> max_{0};
};

class IoStatistics {
 public:
  struct ReadStats {
    std::array<std::atomic<uint64_t>, 4> rawBytesReads_;
    std::array<std::atomic<uint64_t>, 4> cntReads_;
    std::array<std::atomic<uint64_t>, 4> totalTimeReads_;

    ReadStats()
        : rawBytesReads_{{0, 0, 0, 0}},
          cntReads_{{0, 0, 0, 0}},
          totalTimeReads_{{0, 0, 0, 0}} {}
  };

  uint64_t rawBytesRead() const;
  uint64_t rawOverreadBytes() const;
  uint64_t rawBytesWritten() const;
  uint64_t inputBatchSize() const;
  uint64_t outputBatchSize() const;
  uint64_t totalScanTime() const;
  uint64_t totalMergeTime() const;
  uint64_t loadFileMetaDataTimeNs() const;

  uint64_t writeIOTimeUs() const;

  ReadStats& readStats();

  std::vector<uint64_t> rawBytesReads() const;
  std::vector<uint64_t> cntReads() const;
  std::vector<uint64_t> scanTimeReads() const;

  uint64_t incRawBytesRead(int64_t);
  uint64_t incRawOverreadBytes(int64_t);
  uint64_t incRawBytesWritten(int64_t);
  uint64_t incInputBatchSize(int64_t);
  uint64_t incOutputBatchSize(int64_t);
  uint64_t incTotalScanTime(int64_t);
  uint64_t incTotalMergeTime(int64_t);
  uint64_t incLoadFileMetaDataTimeNs(int64_t);
  void incIOInfo(int64_t size, int64_t time);

  int is_greater_than(int x, int threshold) {
    return !((x - threshold) >> 31);
  }

  uint64_t incWriteIOTimeUs(int64_t);

  IoCounter& prefetch() {
    return prefetch_;
  }

  IoCounter& read() {
    return read_;
  }

  IoCounter& ssdRead() {
    return ssdRead_;
  }

  IoCounter& ramHit() {
    return ramHit_;
  }

  IoCounter& queryThreadIoLatency() {
    return queryThreadIoLatency_;
  }

  IoCounter& queryThreadIoLatencySync() {
    return queryThreadIoLatencySync_;
  }

  IoCounter& queryThreadIoLatencyAsync() {
    return queryThreadIoLatencyAsync_;
  }

  void incOperationCounters(
      const std::string& operation,
      const uint64_t resourceThrottleCount,
      const uint64_t localThrottleCount,
      const uint64_t globalThrottleCount,
      const uint64_t retryCount,
      const uint64_t latencyInMs,
      const uint64_t delayInjectedInSecs);

  std::unordered_map<std::string, OperationCounters> operationStats() const;

  void merge(const IoStatistics& other);

  folly::dynamic getOperationStatsSnapshot() const;

 private:
  std::atomic<uint64_t> rawBytesRead_{0};
  std::atomic<uint64_t> rawBytesWritten_{0};
  std::atomic<uint64_t> inputBatchSize_{0};
  std::atomic<uint64_t> outputBatchSize_{0};
  std::atomic<uint64_t> rawOverreadBytes_{0};
  std::atomic<uint64_t> totalScanTime_{0};
  std::atomic<uint64_t> totalMergeTime_{0};
  std::atomic<uint64_t> loadFileMetaDataTimeNs_{0};

  std::atomic<uint64_t> writeIOTimeUs_{0};

  ReadStats readStats_{};

  // Planned read from storage or SSD.
  IoCounter prefetch_;

  // Read from storage, for sparsely accessed columns.
  IoCounter read_;

  // Hits from RAM cache. Does not include first use of prefetched data.
  IoCounter ramHit_;

  // Read from SSD cache instead of storage. Includes both random and planned
  // reads.
  IoCounter ssdRead_;

  // Time spent by a query processing thread waiting for synchronously
  // issued IO or for an in-progress read-ahead to finish.
  IoCounter queryThreadIoLatency_;
  IoCounter queryThreadIoLatencySync_;
  IoCounter queryThreadIoLatencyAsync_;

  std::unordered_map<std::string, OperationCounters> operationStats_;
  mutable std::mutex operationStatsMutex_;
};

} // namespace bytedance::bolt::io
