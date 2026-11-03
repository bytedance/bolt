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

#include <cstdint>

#include "bolt/core/QueryConfig.h"
#include "bolt/functions/Macros.h"
#include "folly/Random.h"
namespace bytedance::bolt::functions::flinksql {

template <typename TExec>
struct RandIntegerFunction {
  BOLT_DEFINE_FUNCTION_TYPES(TExec);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int32_t>* /*max*/,
      const arg_type<int32_t>* seed) {
    generator_ = std::mt19937{};
    if (seed) {
      generator_.seed(*seed);
    }
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int32_t max) {
    result = folly::Random::rand32(max);
  }

  FOLLY_ALWAYS_INLINE void
  call(int32_t& result, int32_t max, int32_t /*seed*/) {
    result = folly::Random::rand32(max, generator_);
  }

 private:
  std::mt19937 generator_;
};

namespace detail {

class JavaRandom {
 public:
  // java.util.Random keeps a 48-bit linear congruential state.
  explicit JavaRandom(int64_t seed = 0) {
    setSeed(seed);
  }

  void setSeed(int64_t seed) {
    state_ = (static_cast<uint64_t>(seed) ^ kMultiplier) & kMask;
  }

  double nextDouble() {
    const auto high = static_cast<uint64_t>(next(26)) << 27;
    const auto low = next(27);
    return static_cast<double>(high | low) /
        static_cast<double>(uint64_t{1} << 53);
  }

  int32_t nextInt(int32_t bound) {
    BOLT_USER_CHECK_GT(bound, 0, "bound must be positive");
    if ((bound & (bound - 1)) == 0) {
      return static_cast<int32_t>(
          (static_cast<int64_t>(bound) * next(31)) >> 31);
    }
    while (true) {
      const auto bits = next(31);
      const auto value = bits % bound;
      if (static_cast<int64_t>(bits) - value + bound - 1 <= INT32_MAX) {
        return value;
      }
    }
  }

 private:
  int32_t next(int32_t bits) {
    state_ = (state_ * kMultiplier + kAddend) & kMask;
    return static_cast<int32_t>(state_ >> (48 - bits));
  }

  static constexpr uint64_t kMultiplier = 0x5deece66dULL;
  static constexpr uint64_t kAddend = 0xbULL;
  static constexpr uint64_t kMask = (uint64_t{1} << 48) - 1;
  uint64_t state_ = 0;
};

// Literal seeds advance shared state; dynamic seeds restart it for each row.
class FlinkRandomState {
 public:
  void initialize() {
    initialize<int64_t>(nullptr);
  }

  template <typename TSeed>
  void initialize(const TSeed* seed) {
    constantSeed_ = seed != nullptr;
    generator_.setSeed(seed ? *seed : folly::Random::rand64());
  }

  JavaRandom& generator() {
    return generator_;
  }

  template <typename TNext>
  auto generate(int64_t seed, TNext next) {
    if (constantSeed_) {
      return next(generator_);
    }
    JavaRandom generator(seed);
    return next(generator);
  }

 private:
  bool constantSeed_ = false;
  JavaRandom generator_;
};

} // namespace detail

/// flink_rand([seed]) uses the java.util.Random sequence.
template <typename T>
struct FlinkRandFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  void initialize(const std::vector<TypePtr>&, const core::QueryConfig&) {
    state_.initialize();
  }

  template <typename TSeed>
  void initialize(
      const std::vector<TypePtr>&,
      const core::QueryConfig&,
      const TSeed* seed) {
    state_.initialize(seed);
  }

  void call(double& result) {
    result = state_.generator().nextDouble();
  }

  template <typename TSeed>
  void call(double& result, TSeed seed) {
    result = state_.generate(
        seed, [](auto& generator) { return generator.nextDouble(); });
  }

 private:
  detail::FlinkRandomState state_;
};

/// flink_rand_integer(bound[, seed]) uses native plan argument order.
template <typename T>
struct FlinkRandIntegerFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  void initialize(
      const std::vector<TypePtr>&,
      const core::QueryConfig&,
      const int32_t*) {
    state_.initialize();
  }

  void initialize(
      const std::vector<TypePtr>&,
      const core::QueryConfig&,
      const int32_t*,
      const int32_t* seed) {
    state_.initialize(seed);
  }

  void call(int32_t& result, int32_t bound) {
    result = state_.generator().nextInt(bound);
  }

  void call(int32_t& result, int32_t bound, int32_t seed) {
    result = state_.generate(
        seed, [bound](auto& generator) { return generator.nextInt(bound); });
  }

 private:
  detail::FlinkRandomState state_;
};

} // namespace bytedance::bolt::functions::flinksql
