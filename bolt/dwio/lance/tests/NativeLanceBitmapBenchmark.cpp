/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <array>
#include <cstdlib>
#include <string_view>

#include "bolt/common/base/BitUtil.h"
#include "bolt/dwio/lance/NativeLanceBitmap.h"

namespace bytedance::bolt::lance::reader::benchmark {
namespace {

constexpr uint64_t kRows = 1'024;
constexpr uint64_t kWords = (kRows + 64) / 64 + 2;

bool useScalar() {
  const auto* mode = std::getenv("BOLT_LANCE_BITMAP_MODE");
  return mode != nullptr && std::string_view(mode) == "scalar";
}

struct Input {
  Input() {
    for (uint64_t i = 0; i < source.size() * 8; ++i) {
      bits::setBit(source.data(), i, i % 17 != 0);
    }
  }

  std::array<uint8_t, kRows / 8 + 16> source{};
  std::array<uint64_t, kWords> output{};
};

uint64_t
runCopy(uint32_t iterations, uint64_t sourceOffset, uint64_t targetOffset) {
  static Input input;
  static const bool scalar = useScalar();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    input.output.fill(0xa5a5a5a5a5a5a5a5);
    if (scalar) {
      copyLanceBitmapScalar(
          input.source.data(),
          sourceOffset,
          kRows,
          input.output.data(),
          targetOffset);
    } else {
      copyLanceBitmap(
          input.source.data(),
          sourceOffset,
          kRows,
          input.output.data(),
          targetOffset);
    }
    folly::doNotOptimizeAway(input.output.data());
  }
  return iterations;
}

uint64_t runValidity(uint32_t iterations, bool allValid, uint64_t bitOffset) {
  static Input sparse;
  static Input valid;
  static const bool scalar = useScalar();
  valid.source.fill(0xff);
  auto& input = allValid ? valid : sparse;
  bool hasNulls = false;
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    input.output.fill(~uint64_t{0});
    if (scalar) {
      hasNulls = false;
      for (uint64_t i = 0; i < kRows; ++i) {
        const auto bit = bitOffset + i;
        const auto isValid = (input.source[bit / 8] & (1U << (bit % 8))) != 0;
        hasNulls |= !isValid;
        bits::setBit(input.output.data(), i, isValid);
      }
    } else {
      hasNulls = !lanceBitmapIsAllSet(input.source.data(), bitOffset, kRows);
      if (hasNulls) {
        copyLanceBitmap(
            input.source.data(), bitOffset, kRows, input.output.data(), 0);
      }
    }
    folly::doNotOptimizeAway(hasNulls);
    folly::doNotOptimizeAway(input.output.data());
  }
  return iterations;
}

BENCHMARK_MULTI(bitmap_value_aligned, n) {
  return runCopy(n, 0, 0);
}

BENCHMARK_MULTI(bitmap_value_unaligned, n) {
  return runCopy(n, 3, 5);
}

BENCHMARK_MULTI(validity_all_valid_aligned, n) {
  return runValidity(n, true, 0);
}

BENCHMARK_MULTI(validity_all_valid_unaligned, n) {
  return runValidity(n, true, 3);
}

BENCHMARK_MULTI(validity_sparse_null_aligned, n) {
  return runValidity(n, false, 0);
}

BENCHMARK_MULTI(validity_sparse_null_unaligned, n) {
  return runValidity(n, false, 3);
}

} // namespace
} // namespace bytedance::bolt::lance::reader::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
