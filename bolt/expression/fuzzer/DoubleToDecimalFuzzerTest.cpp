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

#include <arrow/util/decimal.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <random>
#include "bolt/type/DecimalUtil.h"
#include "bolt/type/HugeInt.h"

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  // Random number generator engine
  std::random_device rd; // Non-deterministic seed (if available)
  std::mt19937 gen(rd()); // Mersenne Twister engine
  std::uniform_real_distribution<double> dist(-pow(10, 20), pow(10, 20));

  int64_t batchSize = 1024;
  int64_t value = 0;
  constexpr int precision = 38;
  constexpr int scale = 18;
  while (true) {
    double d = dist(gen);
    arrow::Decimal128 decimalValue =
        arrow::Decimal128::FromReal(d, precision, scale).ValueOrDie();

    bytedance::bolt::int128_t to;
    auto s = bytedance::bolt::DecimalUtil::
        rescaleFullFloatingPoint<double, bytedance::bolt::int128_t>(
            d, precision, scale, to);
    if ((uint64_t)to != decimalValue.low_bits() ||
        (int64_t)(to >> 64) != decimalValue.high_bits()) {
      char cached[64];
      auto strSize = bytedance::bolt::DecimalUtil::convertToString<
          bytedance::bolt::DecimalUtil::DecimalStringFormat::kPlain>(
          to, scale, 64, cached);
      std::cout << std::setprecision(50) << "Mismatch for value: " << d << "\n";
      std::cout << "Expected: " << decimalValue.ToString(scale) << "\n";
      std::cout << "Got: " << std::string_view(cached, strSize) << "\n";
      return 1;
    }
    value++;
    if (value % 1000000 == 0) {
      std::cout << "Processed " << value << " values.\n";
    }
  }
  return 0;
}
