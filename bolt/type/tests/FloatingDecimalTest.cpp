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

#include <atomic>
#include <thread>

#include <gtest/gtest.h>
#include <type/DecimalUtil.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/FloatingDecimal.h"
namespace bytedance::bolt {

namespace {

inline std::vector<std::tuple<int128_t, int, double>> doubleInput() {
  using M = std::tuple<int128_t, int, double>;
  std::vector<M> inputData{
      M{0, 2, 0},
      M{12345678910L, 9, 12.34567891},
      M{12000000, 4, 1200},
      M{DecimalUtil::kPowersOfTen[33] * 12345, 0, 1.2345e+37},
      M{(int128_t)12345678912L * DecimalUtil::kPowersOfTen[27],
        37,
        1.2345678912},
      M{123456789, 37, 1.23456789e-29},
  };
  return inputData;
}

inline std::vector<std::tuple<int128_t, int, float>> floatInput() {
  using M = std::tuple<int128_t, int, float>;
  return {
      M{0, 2, 0},
      M{100, 1, 10},
      M{123456, 4, 12.3456},
      M{123456789112LL, 11, 1.23456789112},
      M{84059812, 2, 840598.1},
      M{12345678000, 2, 123456784}};
}

TEST(FloatingDecimalTest, basic) {
  {
    auto input = doubleInput();
    for (auto& data : input) {
      auto doubleRes = FloatingDecimal::toDoubleFromValue(
          std::get<0>(data), std::get<1>(data));
      EXPECT_TRUE(doubleRes.has_value());
      EXPECT_EQ(std::get<2>(data), *doubleRes);
    }
  }

  {
    auto input = floatInput();
    for (auto& data : input) {
      auto floatRes = FloatingDecimal::toFloatFromValue(
          std::get<0>(data), std::get<1>(data));
      EXPECT_TRUE(floatRes.has_value());
      EXPECT_EQ(std::get<2>(data), *floatRes);
    }
  }
}

TEST(FloatingDecimalTest, computesPowersOfFiveConcurrently) {
  constexpr int32_t kNumThreads = 32;
  constexpr int32_t kIterations = 100;
  std::atomic<int32_t> ready{0};
  std::atomic<bool> release{false};
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (int32_t i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      ready.fetch_add(1, std::memory_order_release);
      while (!release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      for (int32_t iteration = 0; iteration < kIterations; ++iteration) {
        const int32_t exponent = 64 + i * 9 + iteration % 3;
        EXPECT_EQ(
            FloatingDecimal::big5pow(exponent),
            boost::multiprecision::pow(
                boost::multiprecision::cpp_int{5}, exponent));
      }
    });
  }

  while (ready.load(std::memory_order_acquire) != kNumThreads) {
    std::this_thread::yield();
  }
  release.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(FloatingDecimalTest, computesPowersOfFiveAtCacheBoundary) {
  for (const int32_t exponent : {0, 26, 27, 339, 340, 341}) {
    EXPECT_EQ(
        FloatingDecimal::big5pow(exponent),
        boost::multiprecision::pow(
            boost::multiprecision::cpp_int{5}, exponent));
  }
}

} // namespace

} // namespace bytedance::bolt
