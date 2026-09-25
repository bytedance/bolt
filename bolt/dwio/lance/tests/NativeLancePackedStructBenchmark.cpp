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

#include "bolt/dwio/lance/NativeLancePackedStruct.h"

namespace bytedance::bolt::lance::reader::benchmark {
namespace {

constexpr uint64_t kRows = 1'024;

template <uint32_t kColumns>
struct Input {
  Input() : packed(kRows * kColumns) {
    for (uint64_t row = 0; row < kRows; ++row) {
      for (uint32_t column = 0; column < kColumns; ++column) {
        packed[row * kColumns + column] = row * 37 + column;
        descriptors[column] = {
            column * sizeof(uint64_t),
            sizeof(uint64_t),
            reinterpret_cast<uint8_t*>(outputs[column].data())};
      }
    }
  }

  std::vector<uint64_t> packed;
  std::array<std::array<uint64_t, kRows>, kColumns> outputs;
  std::array<NativeLancePackedStructColumn, kColumns> descriptors;
};

bool useScalar() {
  const auto* mode = std::getenv("BOLT_LANCE_PACKED_STRUCT_MODE");
  return mode != nullptr && std::string_view(mode) == "scalar";
}

template <uint32_t kColumns>
uint64_t run(uint32_t iterations) {
  static Input<kColumns> input;
  static const bool scalar = useScalar();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    if (scalar) {
      decodeLancePackedStructScalar(
          reinterpret_cast<const uint8_t*>(input.packed.data()),
          kRows,
          kColumns * sizeof(uint64_t),
          input.descriptors.data(),
          kColumns);
    } else {
      decodeLancePackedStructAvx2(
          reinterpret_cast<const uint8_t*>(input.packed.data()),
          kRows,
          kColumns * sizeof(uint64_t),
          input.descriptors.data(),
          kColumns);
    }
    folly::doNotOptimizeAway(input.outputs.data());
  }
  return iterations;
}

BENCHMARK_MULTI(packed_struct_4x64, n) {
  return run<4>(n);
}

BENCHMARK_MULTI(packed_struct_8x64, n) {
  return run<8>(n);
}

} // namespace
} // namespace bytedance::bolt::lance::reader::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
