/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstddef>
#include <cstdint>

namespace bytedance::bolt::aggregate::prestosql {

// Shared accumulator layout for variance-based aggregates and hash aggregate
// JIT codegen. Keep offsets here so generated IR cannot drift from C++ state.
struct VarianceAccumulator {
  VarianceAccumulator() = default;

  VarianceAccumulator(int64_t count, double value)
      : count_(count), mean_(value), m2_(0.0) {}

  int64_t count() const {
    return count_;
  }

  double mean() const {
    return mean_;
  }

  double m2() const {
    return m2_;
  }

  void update(double value) {
    count_ += 1;
    double delta = value - mean();
    mean_ += delta / count();
    m2_ += delta * (value - mean());
  }

  void merge(const VarianceAccumulator& other) {
    merge(other.count(), other.mean(), other.m2());
  }

  void merge(int64_t countOther, double meanOther, double m2Other) {
    if (countOther == 0) {
      return;
    }
    if (count_ == 0) {
      count_ = countOther;
      mean_ = meanOther;
      m2_ = m2Other;
      return;
    }
    int64_t newCount = countOther + count();
    double delta = meanOther - mean();
    double newMean = mean() + delta / newCount * countOther;
    m2_ += m2Other + delta * delta * countOther * count() / (double)newCount;
    count_ = newCount;
    mean_ = newMean;
  }

  static constexpr int32_t countOffset();
  static constexpr int32_t meanOffset();
  static constexpr int32_t m2Offset();

 private:
  int64_t count_{0};
  double mean_{0};
  double m2_{0};
};

constexpr int32_t VarianceAccumulator::countOffset() {
  return offsetof(VarianceAccumulator, count_);
}

constexpr int32_t VarianceAccumulator::meanOffset() {
  return offsetof(VarianceAccumulator, mean_);
}

constexpr int32_t VarianceAccumulator::m2Offset() {
  return offsetof(VarianceAccumulator, m2_);
}

} // namespace bytedance::bolt::aggregate::prestosql
