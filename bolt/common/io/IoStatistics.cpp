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

#include <glog/logging.h>
#include <atomic>
#include <utility>

#include "bolt/common/io/IoStatistics.h"
namespace bytedance::bolt::io {

uint64_t IoStatistics::rawBytesRead() const {
  return rawBytesRead_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::rawOverreadBytes() const {
  return rawOverreadBytes_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::rawBytesWritten() const {
  return rawBytesWritten_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::inputBatchSize() const {
  return inputBatchSize_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::outputBatchSize() const {
  return outputBatchSize_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::totalScanTime() const {
  return totalScanTime_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::totalMergeTime() const {
  return totalMergeTime_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::loadFileMetaDataTimeNs() const {
  return loadFileMetaDataTimeNs_.load(std::memory_order_relaxed);
}

uint64_t IoStatistics::writeIOTimeUs() const {
  return writeIOTimeUs_.load(std::memory_order_relaxed);
}

IoStatistics::ReadStats& IoStatistics::readStats() {
  return readStats_;
}

std::vector<uint64_t> IoStatistics::rawBytesReads() const {
  std::vector<uint64_t> rawBytesReads;
  for (int i = 0; i < 4; i++) {
    rawBytesReads.push_back(
        readStats_.rawBytesReads_[i].load(std::memory_order_relaxed));
  }
  return rawBytesReads;
}

std::vector<uint64_t> IoStatistics::cntReads() const {
  std::vector<uint64_t> cntReads;
  for (int i = 0; i < 4; i++) {
    cntReads.push_back(readStats_.cntReads_[i].load(std::memory_order_relaxed));
  }
  return cntReads;
}

std::vector<uint64_t> IoStatistics::scanTimeReads() const {
  std::vector<uint64_t> scanTimeReads;
  for (int i = 0; i < 4; i++) {
    scanTimeReads.push_back(
        readStats_.totalTimeReads_[i].load(std::memory_order_relaxed));
  }
  return scanTimeReads;
}

void IoStatistics::incIOInfo(int64_t size, int64_t time) {
  int gt4k = is_greater_than(size, K4);
  int gt32k = is_greater_than(size, K32);
  int gt128k = is_greater_than(size, K128);

  int index = gt4k + gt32k + gt128k;
  readStats_.rawBytesReads_[index].fetch_add(size, std::memory_order_relaxed);
  readStats_.cntReads_[index].fetch_add(1, std::memory_order_relaxed);
  readStats_.totalTimeReads_[index].fetch_add(time, std::memory_order_relaxed);
}

uint64_t IoStatistics::incRawBytesRead(int64_t v) {
  return rawBytesRead_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incRawBytesWritten(int64_t v) {
  return rawBytesWritten_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incInputBatchSize(int64_t v) {
  return inputBatchSize_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incOutputBatchSize(int64_t v) {
  return outputBatchSize_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incRawOverreadBytes(int64_t v) {
  return rawOverreadBytes_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incTotalScanTime(int64_t v) {
  return totalScanTime_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incTotalMergeTime(int64_t v) {
  return totalMergeTime_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incLoadFileMetaDataTimeNs(int64_t v) {
  return loadFileMetaDataTimeNs_.fetch_add(v, std::memory_order_relaxed);
}

uint64_t IoStatistics::incWriteIOTimeUs(int64_t v) {
  return writeIOTimeUs_.fetch_add(v, std::memory_order_relaxed);
}

void IoStatistics::incOperationCounters(
    const std::string& operation,
    const uint64_t resourceThrottleCount,
    const uint64_t localThrottleCount,
    const uint64_t globalThrottleCount,
    const uint64_t retryCount,
    const uint64_t latencyInMs,
    const uint64_t delayInjectedInSecs) {
  std::lock_guard<std::mutex> lock{operationStatsMutex_};
  operationStats_[operation].localThrottleCount += localThrottleCount;
  operationStats_[operation].resourceThrottleCount += resourceThrottleCount;
  operationStats_[operation].globalThrottleCount += globalThrottleCount;
  operationStats_[operation].retryCount += retryCount;
  operationStats_[operation].latencyInMs += latencyInMs;
  operationStats_[operation].requestCount++;
  operationStats_[operation].delayInjectedInSecs += delayInjectedInSecs;
}

std::unordered_map<std::string, OperationCounters>
IoStatistics::operationStats() const {
  std::lock_guard<std::mutex> lock{operationStatsMutex_};
  return operationStats_;
}

void IoStatistics::merge(const IoStatistics& other) {
  rawBytesRead_ += other.rawBytesRead_;
  rawBytesWritten_ += other.rawBytesWritten_;
  totalScanTime_ += other.totalScanTime_;
  totalMergeTime_ += other.totalMergeTime_;
  writeIOTimeUs_ += other.writeIOTimeUs_;
  for (int i = 0; i < 4; ++i) {
    readStats_.rawBytesReads_[i] += other.readStats_.rawBytesReads_[i];
    readStats_.cntReads_[i] += other.readStats_.cntReads_[i];
    readStats_.totalTimeReads_[i] += other.readStats_.totalTimeReads_[i];
  }
  loadFileMetaDataTimeNs_ += other.loadFileMetaDataTimeNs_;

  rawOverreadBytes_ += other.rawOverreadBytes_;
  prefetch_.merge(other.prefetch_);
  read_.merge(other.read_);
  ramHit_.merge(other.ramHit_);
  ssdRead_.merge(other.ssdRead_);
  queryThreadIoLatency_.merge(other.queryThreadIoLatency_);
  queryThreadIoLatencySync_.merge(other.queryThreadIoLatencySync_);
  queryThreadIoLatencyAsync_.merge(other.queryThreadIoLatencyAsync_);

  std::lock_guard<std::mutex> l(operationStatsMutex_);
  for (auto& item : other.operationStats_) {
    operationStats_[item.first].merge(item.second);
  }
}

void OperationCounters::merge(const OperationCounters& other) {
  resourceThrottleCount += other.resourceThrottleCount;
  localThrottleCount += other.localThrottleCount;
  globalThrottleCount += other.globalThrottleCount;
  retryCount += other.retryCount;
  latencyInMs += other.latencyInMs;
  requestCount += other.requestCount;
  delayInjectedInSecs += other.delayInjectedInSecs;
}

folly::dynamic serialize(const OperationCounters& counters) {
  folly::dynamic json = folly::dynamic::object;
  json["latencyInMs"] = counters.latencyInMs;
  json["localThrottleCount"] = counters.localThrottleCount;
  json["resourceThrottleCount"] = counters.resourceThrottleCount;
  json["globalThrottleCount"] = counters.globalThrottleCount;
  json["retryCount"] = counters.retryCount;
  json["requestCount"] = counters.requestCount;
  json["delayInjectedInSecs"] = counters.delayInjectedInSecs;
  return json;
}

folly::dynamic IoStatistics::getOperationStatsSnapshot() const {
  auto snapshot = operationStats();
  folly::dynamic json = folly::dynamic::object;
  for (auto stat : snapshot) {
    json[stat.first] = serialize(stat.second);
  }
  return json;
}

} // namespace bytedance::bolt::io
