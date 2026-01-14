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

// Description: This file contains code that references and adapts parts of the
// ClickHouse
//              project's AggregateFunctionQuantileGK.cpp
//              (https://github.com/ClickHouse/ClickHouse). The original code is
//              subject to the terms of the Apache License, Version 2.0.
//
// License: Apache License, Version 2.0
// (http://www.apache.org/licenses/LICENSE-2.0)

#pragma once

#include <gfx/timsort.hpp>
#include <vector>

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::functions::gk {

static constexpr size_t kDefaultCompressThreshold = 10000;
static constexpr size_t kDefaultHeadSize = 50000;
static constexpr double kDefaultRelativeError = 0.0001;

template <typename T>
constexpr size_t AlignedSize() {
  size_t n = sizeof(T);
  if (n == 0)
    return 1;
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  return n + 1;
}

template <typename T>
using Allocator = memory::AlignedStlAllocator<T, AlignedSize<T>()>;

template <typename T>
class GKQuantileSummaries {
 public:
  struct Stats {
    T value; // The sampled_ value
    int64_t g; // The minimum rank jump from the previous value's minimum rank
    int64_t delta; // The maximum span of the rank

    Stats() = default;
    Stats(T value, int64_t g, int64_t delta)
        : value(value), g(g), delta(delta) {}
    bool operator==(const Stats& other) const {
      return g == other.g && delta == other.delta && value == other.value;
    }
    bool operator!=(const Stats& other) const {
      return g != other.g || delta != other.delta || value != other.value;
    }
    [[nodiscard]] std::string toString() const {
      if constexpr (std::is_floating_point_v<T>) {
        return fmt::format(
            "{{g: {}, delta: {}, value: {:.8f}}}", g, delta, value);
      } else {
        return fmt::format("{{g: {}, delta: {}, value: {}}}", g, delta, value);
      }
    }
  };
  static_assert(
      std::is_trivially_copyable<Stats>::value,
      "Stats must be trivially copyable");

  struct QueryResult {
    size_t index;
    int64_t rank;
    T value;

    QueryResult(size_t index, int64_t rank, T value)
        : index(index), rank(rank), value(value) {}
  };

  explicit GKQuantileSummaries(
      memory::MemoryPool* pool,
      double relativeError = kDefaultRelativeError,
      size_t compressThreshold = kDefaultCompressThreshold,
      size_t maxHeadSize = kDefaultHeadSize)
      : relativeError_(relativeError),
        compressThreshold_(compressThreshold),
        maxHeadSize_(maxHeadSize),
        count_(0),
        compressed_(false),
        pool_(pool),
        sampled_(Allocator<Stats>(pool)),
        backupSampled_(Allocator<Stats>(pool)),
        headSampled_(Allocator<T>(pool)) {}

  bool isCompressed() const {
    return compressed_;
  }
  void setCompressed(bool compressed) {
    compressed_ = compressed;
  }

  bool equals(const GKQuantileSummaries<T>& other) const {
    return count_ == other.count_ && compressed_ == other.compressed_ &&
        sampled_ == other.sampled_ && backupSampled_ == other.backupSampled_ &&
        headSampled_ == other.headSampled_;
  }

  void insert(T value) {
    headSampled_.emplace_back(value);
    compressed_ = false;
    if (headSampled_.size() >= maxHeadSize_) {
      withHeadBufferInserted();
      if (sampled_.size() >= compressThreshold_)
        compress();
    }
  }

  static GKQuantileSummaries<T> fromRepeatedValue(
      T value,
      size_t count,
      memory::MemoryPool* pool,
      double relativeError = kDefaultRelativeError,
      size_t compressThreshold = kDefaultCompressThreshold,
      size_t maxHeadSize = kDefaultHeadSize) {
    GKQuantileSummaries<T> ans(
        pool, relativeError, compressThreshold, maxHeadSize);
    ans.headSampled_.assign(count, value);
    ans.compressed_ = false;
    if (ans.headSampled_.size() >= ans.maxHeadSize_) {
      ans.withHeadBufferInserted();
      if (ans.sampled_.size() >= ans.compressThreshold_) {
        ans.compress();
      }
    }
    return ans;
  }

  void setRelativeError(double relativeError) {
    relativeError_ = relativeError;
  }

  void setAccuracy(int64_t accuracy) {
    BOLT_CHECK_GT(accuracy, 0);
    relativeError_ = 1.0F / accuracy;
  }

  void query(const double* percentiles, size_t size, T* result) const {
    BOLT_CHECK(
        headSampled_.empty(),
        "Cannot operate on an uncompressed summary, call compress() first");
    BOLT_CHECK(percentiles, "percentiles must not be null")
    BOLT_CHECK(result, "result must not be null");

    if (sampled_.empty()) {
      std::fill(result, result + size, T());
      return;
    }

    int64_t currentMax = std::numeric_limits<int64_t>::min();
    for (const auto& stats : sampled_) {
      currentMax = std::max(stats.delta + stats.g, currentMax);
    }
    int64_t targetError = currentMax / 2;

    // the follow loop stores the index and minRank so need to call
    // findApproxQuantile in order of percentiles, so we need indices, and sort
    // the indices in ascending order of percentiles
    std::vector<size_t> indices(size);
    for (size_t i = 0; i < size; ++i) {
      indices[i] = i;
    }

    gfx::timsort(
        indices.begin(), indices.end(), [&percentiles](size_t lhs, size_t rhs) {
          return percentiles[lhs] < percentiles[rhs];
        });

    size_t index = 0;
    auto minRank = sampled_[0].g;

    for (size_t i = 0; i < size; ++i) {
      size_t actualIndex = indices[i];
      double percentile = percentiles[actualIndex];
      BOLT_CHECK_GE(percentile, 0);
      BOLT_CHECK_LE(percentile, 1);
      if (percentile <= relativeError_) {
        result[actualIndex] = sampled_.front().value;
      } else if (percentile >= 1 - relativeError_) {
        result[actualIndex] = sampled_.back().value;
      } else {
        QueryResult res =
            findApproxQuantile(index, minRank, targetError, percentile);
        index = res.index;
        minRank = res.rank;
        result[actualIndex] = res.value;
      }
    }
  }

  size_t sampleSize() const {
    return sampled_.size();
  }

  void query(const std::vector<double>& percentiles, T* result) const {
    query(percentiles.data(), percentiles.size(), result);
  }

  std::vector<T> query(const std::vector<double>& percentiles) const {
    std::vector<T> result(percentiles.size());
    query(percentiles.data(), percentiles.size(), result.data());
    return result;
  }

  T query(double percentile) const {
    BOLT_CHECK(
        headSampled_.empty(),
        "Cannot operate on an uncompressed summary, call compress() first");
    BOLT_CHECK(
        percentile >= 0.0 && percentile <= 1.0,
        "All percentage values must be between 0.0 and 1.0 (current = {})",
        percentile);
    if (sampled_.empty()) {
      return T();
    }

    int64_t currentMax = std::numeric_limits<int64_t>::min();
    for (const auto& stats : sampled_)
      currentMax = std::max(stats.delta + stats.g, currentMax);
    int64_t targetError = currentMax / 2;
    if (percentile <= relativeError_) {
      return sampled_.front().value;
    } else if (percentile >= 1 - relativeError_) {
      return sampled_.back().value;
    } else {
      auto minRank = sampled_[0].g;
      QueryResult res = findApproxQuantile(0, minRank, targetError, percentile);
      return res.value;
    }
  }

  void compress() {
    if (compressed_ && headSampled_.empty()) {
      return;
    }

    withHeadBufferInserted();

    doCompress(2 * relativeError_ * count_);
  }

  void merge(const GKQuantileSummaries<T>& other) {
    if (other.count_ == 0) {
      return;
    }
    if (count_ == 0) {
      compressThreshold_ = other.compressThreshold_;
      maxHeadSize_ = other.maxHeadSize_;
      relativeError_ = other.relativeError_;
      count_ = other.count_;
      compressed_ = other.compressed_;
      sampled_.resize(other.sampled_.size());
      std::copy(other.sampled_.begin(), other.sampled_.end(), sampled_.begin());
      headSampled_.resize(other.headSampled_.size());
      std::copy(
          other.headSampled_.begin(),
          other.headSampled_.end(),
          headSampled_.begin());
      backupSampled_.resize(other.backupSampled_.size());
      std::copy(
          other.backupSampled_.begin(),
          other.backupSampled_.end(),
          backupSampled_.begin());
      return;
    }
    BOLT_CHECK(
        headSampled_.empty(),
        "Current buffer needs to be compressed before merge");
    BOLT_CHECK(
        other.headSampled_.empty(),
        "Other buffer needs to be compressed before merge");
    double mergeThreshold = 2 * relativeError_ * count_;
    // merge this sampled_ with other.sampled_ and then sort
    sampled_.reserve(sampled_.size() + other.sampled_.size());
    sampled_.insert(
        sampled_.end(), other.sampled_.begin(), other.sampled_.end());
    gfx::timsort(
        sampled_.begin(), sampled_.end(), [](const Stats& a, const Stats& b) {
          return a.value < b.value;
        });
    doCompress(mergeThreshold);
    compressThreshold_ = other.compressThreshold_;
    relativeError_ = other.relativeError_;
    count_ += other.count_;
  }
  size_t count() const {
    return count_;
  }
  // Calculate the size needed for serialization.
  size_t serializedByteSize() const {
    BOLT_CHECK(backupSampled_.empty(), "buffer needs to be compressed");
    size_t ans = sizeof(compressThreshold_) + sizeof(relativeError_) +
        sizeof(count_) + sizeof(sampled_.size()) + sizeof(headSampled_.size());
    size_t statsSize =
        (sizeof(T) + sizeof(int64_t) + sizeof(int64_t)) * sampled_.size();
    size_t headSize = sizeof(T) * headSampled_.size();
    return ans + statsSize + headSize;
  }

  size_t serialize(char* FOLLY_NONNULL out) const {
    BOLT_CHECK(out, "can not serialize data to null.");
    char* start = out;
    memcpy(out, &compressThreshold_, sizeof(compressThreshold_));
    out += sizeof(compressThreshold_);
    memcpy(out, &relativeError_, sizeof(relativeError_));
    out += sizeof(relativeError_);
    memcpy(out, &count_, sizeof(count_));
    out += sizeof(count_);
    auto sampledSize = sampled_.size();
    memcpy(out, &sampledSize, sizeof(sampledSize));
    out += sizeof(sampledSize);
    for (const auto& stats : sampled_) {
      memcpy(out, &stats.value, sizeof(stats.value));
      out += sizeof(stats.value);
      memcpy(out, &stats.g, sizeof(stats.g));
      out += sizeof(stats.g);
      memcpy(out, &stats.delta, sizeof(stats.delta));
      out += sizeof(stats.delta);
    }
    // headSampled_
    auto headSize = headSampled_.size();
    memcpy(out, &headSize, sizeof(headSize));
    out += sizeof(headSize);
    for (const auto& value : headSampled_) {
      memcpy(out, &value, sizeof(value));
      out += sizeof(value);
    }
    int64_t serializedSize = out - start;
    BOLT_USER_CHECK(serializedSize >= 0, "Invalid size of serialized data");
    BOLT_USER_CHECK_EQ(
        serializedSize,
        serializedByteSize(),
        "Invalid size of serialized data, actrual: {}, expected: {}",
        serializedSize,
        serializedByteSize());
    return serializedSize;
  }

  // Deserialize a summary from bytes.
  void deserialize(const char* FOLLY_NONNULL data, size_t expectedSize) {
    BOLT_CHECK(data, "can not deserialize data from nullptr.");
    const char* start = data;
    memcpy(&compressThreshold_, data, sizeof(compressThreshold_));
    data += sizeof(compressThreshold_);
    memcpy(&relativeError_, data, sizeof(relativeError_));
    data += sizeof(relativeError_);
    memcpy(&count_, data, sizeof(count_));
    data += sizeof(count_);
    size_t sampledSize = 0;
    memcpy(&sampledSize, data, sizeof(sampledSize));
    data += sizeof(sampledSize);
    if (sampledSize > 0) {
      sampled_.resize(sampledSize);
      for (size_t i = 0; i < sampledSize; ++i) {
        memcpy(&sampled_[i].value, data, sizeof(sampled_[i].value));
        data += sizeof(sampled_[i].value);
        memcpy(&sampled_[i].g, data, sizeof(sampled_[i].g));
        data += sizeof(sampled_[i].g);
        memcpy(&sampled_[i].delta, data, sizeof(sampled_[i].delta));
        data += sizeof(sampled_[i].delta);
      }
    }
    size_t headSize = 0;
    memcpy(&headSize, data, sizeof(headSize));
    data += sizeof(headSize);
    if (headSize > 0) {
      headSampled_.resize(headSize);
      for (size_t i = 0; i < headSize; ++i) {
        memcpy(&headSampled_[i], data, sizeof(headSampled_[i]));
        data += sizeof(headSampled_[i]);
      }
      compress();
    }
    compressed_ = true;
    size_t acrtualSize = data - start;

    BOLT_USER_CHECK_EQ(
        acrtualSize,
        data - start,
        "Invalid size of deserialized data, expectedSize:{}, data - start:{}",
        expectedSize,
        acrtualSize);
  }

  bool empty() const {
    return sampled_.empty() && headSampled_.empty() && backupSampled_.empty();
  }

 private:
  QueryResult findApproxQuantile(
      size_t index,
      int64_t minRankAtIndex,
      double targetError,
      double percentile) const {
    BOLT_CHECK(compressed_, "Cannot query on an uncompressed summary");
    BOLT_CHECK(
        headSampled_.empty(), "Cannot operate on an uncompressed summary");
    Stats currSample = sampled_[index];
    int64_t rank = static_cast<int64_t>(std::ceil(percentile * count_));
    size_t i = index;
    int64_t minRank = minRankAtIndex;
    while (i < sampled_.size() - 1) {
      int64_t maxRank = minRank + currSample.delta;
      if (maxRank - targetError <= rank && rank <= minRank + targetError) {
        return {i, minRank, currSample.value};
      } else {
        ++i;
        currSample = sampled_[i];
        minRank += currSample.g;
      }
    }
    return {sampled_.size() - 1, 0, sampled_.back().value};
  }

  void withHeadBufferInserted() {
    if (headSampled_.empty()) {
      return;
    }
    compressed_ = false;
    gfx::timsort(headSampled_.begin(), headSampled_.end(), std::less<>());

    backupSampled_.clear();
    backupSampled_.reserve(sampled_.size() + headSampled_.size());

    size_t sampleIdx = 0;
    size_t opsIdx = 0;
    size_t currentCount = count_;
    for (; opsIdx < headSampled_.size(); ++opsIdx) {
      T currentSample = headSampled_[opsIdx];

      // Add all the samples before the next observation.
      while (sampleIdx < sampled_.size() &&
             sampled_[sampleIdx].value <= currentSample) {
        backupSampled_.emplace_back(sampled_[sampleIdx]);
        ++sampleIdx;
      }

      // If it is the first one to insert, of if it is the last one
      ++currentCount;
      int64_t delta;
      if (backupSampled_.empty() ||
          (sampleIdx == sampled_.size() &&
           opsIdx == (headSampled_.size() - 1))) {
        delta = 0;
      } else {
        delta =
            static_cast<int64_t>(std::floor(2 * relativeError_ * currentCount));
      }

      backupSampled_.emplace_back(currentSample, 1, delta);
    }

    // Add all the remaining existing samples
    for (; sampleIdx < sampled_.size(); ++sampleIdx) {
      backupSampled_.emplace_back(sampled_[sampleIdx]);
    }
    std::swap(sampled_, backupSampled_);
    backupSampled_.clear();
    headSampled_.clear();
    count_ = currentCount;
  }

  void doCompress(double mergeThreshold) {
    compressed_ = true;
    if (sampled_.empty()) {
      return;
    }

    backupSampled_.clear();
    backupSampled_.reserve(sampled_.size());
    // Start for the last element, which is always part of the set.
    // The head contains the current new head, that may be merged with the
    // current element.
    Stats head = sampled_.back();
    ssize_t i = sampled_.size() - 2;

    // Do not compress the last element
    while (i >= 1) {
      // The current sample:
      const auto& sample1 = sampled_[i];
      // Do we need to compress?
      if (sample1.g + head.g + head.delta < mergeThreshold) {
        // Do not insert yet, just merge the current element into the head.
        head.g += sample1.g;
      } else {
        // Prepend the current head, and keep the current sample as target for
        // merging.

        backupSampled_.emplace_back(head.value, head.g, head.delta);
        head = sample1;
      }
      --i;
    }

    backupSampled_.emplace_back(head.value, head.g, head.delta);
    // If necessary, add the minimum element:
    auto currHead = sampled_.front();

    // don't add the minimum element if `currentSamples` has only one element
    // (both `currHead` and `head` point to the same element)
    if (currHead.value <= head.value && sampled_.size() > 1) {
      backupSampled_.emplace_back(sampled_.front());
    }
    std::reverse(backupSampled_.begin(), backupSampled_.end());
    std::swap(sampled_, backupSampled_);
    backupSampled_.clear();
  }

  double relativeError_{kDefaultRelativeError};
  size_t compressThreshold_{kDefaultCompressThreshold};
  size_t maxHeadSize_{kDefaultHeadSize};
  size_t count_{0};
  bool compressed_{false};
  memory::MemoryPool* pool_{nullptr};

  std::vector<Stats, Allocator<Stats>> sampled_;
  std::vector<Stats, Allocator<Stats>> backupSampled_;
  std::vector<T, Allocator<T>> headSampled_;
};
extern template class GKQuantileSummaries<int8_t>;
extern template class GKQuantileSummaries<int16_t>;
extern template class GKQuantileSummaries<int32_t>;
extern template class GKQuantileSummaries<int64_t>;
extern template class GKQuantileSummaries<int128_t>;
extern template class GKQuantileSummaries<float>;
extern template class GKQuantileSummaries<double>;
extern template class GKQuantileSummaries<Timestamp>;
} // namespace bytedance::bolt::functions::gk
