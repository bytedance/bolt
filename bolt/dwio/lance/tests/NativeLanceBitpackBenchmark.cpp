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
#include <vector>

#include "bolt/dwio/lance/NativeLanceBitpack.h"

namespace bytedance::bolt::lance::reader::benchmark {
namespace {

constexpr uint64_t kRows = 1'024;

struct Input {
  explicit Input(uint8_t bitWidth, uint8_t bitOffset)
      : bitWidth(bitWidth),
        bitOffset(bitOffset),
        packed((bitOffset + kRows * bitWidth + 7) / 8 + 8, 0),
        output(kRows) {
    const auto mask = (uint64_t{1} << bitWidth) - 1;
    for (uint64_t row = 0; row < kRows; ++row) {
      const auto value = (row * 37 + 3) & mask;
      const auto start = bitOffset + row * bitWidth;
      for (uint8_t bit = 0; bit < bitWidth; ++bit) {
        if ((value & (uint64_t{1} << bit)) != 0) {
          const auto outputBit = start + bit;
          packed[outputBit / 8] |= 1U << (outputBit % 8);
        }
      }
    }
  }

  uint8_t bitWidth;
  uint8_t bitOffset;
  std::vector<uint8_t> packed;
  std::vector<uint32_t> output;
};

struct Input64 {
  explicit Input64(uint8_t bitOffset)
      : bitOffset(bitOffset),
        packed((bitOffset + kRows * bitWidth + 7) / 8 + 8, 0),
        output(kRows) {
    const auto mask = (uint64_t{1} << bitWidth) - 1;
    for (uint64_t row = 0; row < kRows; ++row) {
      const auto value = (row * 37 + 3) & mask;
      const auto start = bitOffset + row * bitWidth;
      for (uint8_t bit = 0; bit < bitWidth; ++bit) {
        if ((value & (uint64_t{1} << bit)) != 0) {
          const auto outputBit = start + bit;
          packed[outputBit / 8] |= 1U << (outputBit % 8);
        }
      }
    }
  }

  static constexpr uint8_t bitWidth = 37;
  uint8_t bitOffset;
  std::vector<uint8_t> packed;
  std::vector<uint64_t> output;
};

bool useScalar() {
  const auto* mode = std::getenv("BOLT_LANCE_BITPACK_MODE");
  return mode != nullptr && std::string_view(mode) == "scalar";
}

uint64_t run(uint32_t iterations, uint8_t bitWidth, uint8_t bitOffset) {
  static std::array<Input, 8> inputs{
      Input(5, 0),
      Input(5, 3),
      Input(13, 0),
      Input(13, 3),
      Input(23, 0),
      Input(23, 3),
      Input(29, 0),
      Input(29, 3)};
  const auto index = (bitWidth == 5        ? 0
                          : bitWidth == 13 ? 2
                          : bitWidth == 23 ? 4
                                           : 6) +
      (bitOffset == 0 ? 0 : 1);
  auto& input = inputs[index];
  static const bool scalar = useScalar();
  const auto inputBytes = (bitOffset + kRows * bitWidth + 7) / 8;
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    if (scalar) {
      decodeLanceBitpackedScalar(
          input.packed.data(),
          inputBytes,
          bitOffset,
          kRows,
          bitWidth,
          32,
          false,
          reinterpret_cast<uint8_t*>(input.output.data()));
    } else {
      decodeLanceBitpacked(
          input.packed.data(),
          inputBytes,
          bitOffset,
          kRows,
          bitWidth,
          32,
          false,
          reinterpret_cast<uint8_t*>(input.output.data()));
    }
    folly::doNotOptimizeAway(input.output.data());
  }
  return iterations;
}

uint64_t run64(uint32_t iterations, uint8_t bitOffset) {
  static std::array<Input64, 2> inputs{Input64(0), Input64(3)};
  auto& input = inputs[bitOffset == 0 ? 0 : 1];
  static const bool scalar = useScalar();
  const auto inputBytes = (bitOffset + kRows * Input64::bitWidth + 7) / 8;
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    const auto decode =
        scalar ? decodeLanceBitpackedScalar : decodeLanceBitpacked;
    decode(
        input.packed.data(),
        inputBytes,
        bitOffset,
        kRows,
        Input64::bitWidth,
        64,
        false,
        reinterpret_cast<uint8_t*>(input.output.data()));
    folly::doNotOptimizeAway(input.output.data());
  }
  return iterations;
}

#define LANCE_BITPACK_BENCHMARK(width, offset)            \
  BENCHMARK_MULTI(bitpack_##width##_offset_##offset, n) { \
    return run(n, width, offset);                         \
  }

LANCE_BITPACK_BENCHMARK(5, 0)
LANCE_BITPACK_BENCHMARK(5, 3)
LANCE_BITPACK_BENCHMARK(13, 0)
LANCE_BITPACK_BENCHMARK(13, 3)
LANCE_BITPACK_BENCHMARK(23, 0)
LANCE_BITPACK_BENCHMARK(23, 3)
LANCE_BITPACK_BENCHMARK(29, 0)
LANCE_BITPACK_BENCHMARK(29, 3)

BENCHMARK_MULTI(bitpack_37_offset_0, n) {
  return run64(n, 0);
}

BENCHMARK_MULTI(bitpack_37_offset_3, n) {
  return run64(n, 3);
}

} // namespace
} // namespace bytedance::bolt::lance::reader::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
