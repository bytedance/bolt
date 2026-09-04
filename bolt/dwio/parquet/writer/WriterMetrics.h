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

#include <atomic>
#include <chrono>

#include "bolt/dwio/common/WriterMetrics.h"

namespace bytedance::bolt::parquet {

struct WriterMetricsCollector {
  std::atomic<uint64_t> writeRecodeWallNanos{0};
  std::atomic<uint64_t> writeEncodeWallNanos{0};
  std::atomic<uint64_t> writeCompressionWallNanos{0};
  std::atomic<uint64_t> writeIOWallNanos{0};
  std::atomic<uint64_t> writeFinalizeWallNanos{0};

  dwio::common::WriterMetrics snapshot() const {
    return {
        .writeRecodeWallNanos =
            writeRecodeWallNanos.load(std::memory_order_relaxed),
        .writeEncodeWallNanos =
            writeEncodeWallNanos.load(std::memory_order_relaxed),
        .writeCompressionWallNanos =
            writeCompressionWallNanos.load(std::memory_order_relaxed),
        .writeIOWallNanos = writeIOWallNanos.load(std::memory_order_relaxed),
        .writeFinalizeWallNanos =
            writeFinalizeWallNanos.load(std::memory_order_relaxed)};
  }
};

class WriterEncodeMetricTimer {
 public:
  explicit WriterEncodeMetricTimer(std::atomic<uint64_t>* metric)
      : metric_(metric), ownsScope_(metric_ != nullptr && active_ == nullptr) {
    if (ownsScope_) {
      active_ = this;
      start_ = std::chrono::steady_clock::now();
    }
  }

  ~WriterEncodeMetricTimer() {
    if (!ownsScope_) {
      return;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - start_)
                             .count();
    metric_->fetch_add(
        elapsed > excludedNanos_ ? elapsed - excludedNanos_ : 0,
        std::memory_order_relaxed);
    active_ = nullptr;
  }

  static void exclude(uint64_t nanos) {
    if (active_ != nullptr) {
      active_->excludedNanos_ += nanos;
    }
  }

 private:
  inline static thread_local WriterEncodeMetricTimer* active_{nullptr};

  std::atomic<uint64_t>* const metric_;
  const bool ownsScope_;
  std::chrono::steady_clock::time_point start_;
  uint64_t excludedNanos_{0};
};

class WriterMetricTimer {
 public:
  explicit WriterMetricTimer(
      std::atomic<uint64_t>* metric,
      bool excludeFromEncode = false)
      : metric_(metric),
        excludeFromEncode_(excludeFromEncode),
        start_(std::chrono::steady_clock::now()) {}

  ~WriterMetricTimer() {
    if (metric_ != nullptr) {
      const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now() - start_)
                               .count();
      metric_->fetch_add(elapsed, std::memory_order_relaxed);
      if (excludeFromEncode_) {
        WriterEncodeMetricTimer::exclude(elapsed);
      }
    }
  }

 private:
  std::atomic<uint64_t>* const metric_;
  const bool excludeFromEncode_;
  const std::chrono::steady_clock::time_point start_;
};

} // namespace bytedance::bolt::parquet
