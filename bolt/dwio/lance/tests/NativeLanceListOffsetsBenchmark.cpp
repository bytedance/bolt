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
#include <folly/lang/Bits.h>

#include <array>
#include <cstdlib>
#include <limits>
#include <string_view>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/dwio/lance/NativeLanceListOffsets.h"

namespace bytedance::bolt::lance::reader::benchmark {
namespace {

constexpr uint64_t kRows = 1'024;
constexpr uint64_t kNullOffsetAdjustment = 8'193;
volatile uint64_t benchmarkNullOffsetAdjustment = kNullOffsetAdjustment;

struct Input {
  std::array<uint64_t, kRows> encodedEnds;
  std::array<vector_size_t, kRows> offsets;
  std::array<vector_size_t, kRows> sizes;
  std::array<uint64_t, kRows / 64> nulls;
};

Input makeInput(bool withNulls) {
  Input input;
  uint64_t offset = 0;
  for (uint64_t i = 0; i < kRows; ++i) {
    offset += i % 5;
    input.encodedEnds[i] =
        offset + (withNulls && i % 17 == 0 ? kNullOffsetAdjustment : 0);
  }
  input.nulls.fill(bits::kNotNull64);
  return input;
}

FOLLY_NOINLINE void decodeModulo(
    const uint64_t* encodedEnds,
    vector_size_t* offsets,
    vector_size_t* sizes,
    uint64_t* nulls) {
  const auto nullOffsetAdjustment = benchmarkNullOffsetAdjustment;
  uint64_t previous = 0;
  for (uint64_t i = 0; i < kRows; ++i) {
    const auto encoded = folly::Endian::little(encodedEnds[i]);
    const auto end = encoded % nullOffsetAdjustment;
    BOLT_CHECK_LT(
        encoded,
        2 * nullOffsetAdjustment,
        "Lance list offset contains more than one null adjustment");
    BOLT_CHECK_LE(previous, end);
    BOLT_CHECK_LE(
        previous,
        static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
    BOLT_CHECK_LE(
        end - previous,
        static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
    offsets[i] = static_cast<vector_size_t>(previous);
    sizes[i] = static_cast<vector_size_t>(end - previous);
    if (encoded >= nullOffsetAdjustment) {
      bits::setNull(nulls, i);
    }
    previous = end;
  }
}

enum class Mode { kModulo, kScalar, kAvx2 };

Mode mode() {
  const auto* mode = std::getenv("BOLT_LANCE_LIST_OFFSETS_MODE");
  if (mode != nullptr && std::string_view(mode) == "modulo") {
    return Mode::kModulo;
  }
  if (mode != nullptr && std::string_view(mode) == "scalar") {
    return Mode::kScalar;
  }
  return Mode::kAvx2;
}

uint64_t run(uint32_t iterations, bool withNulls) {
  static auto noNullInput = makeInput(false);
  static auto nullableInput = makeInput(true);
  static const auto selectedMode = mode();
  auto& input = withNulls ? nullableInput : noNullInput;
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    input.nulls.fill(bits::kNotNull64);
    if (selectedMode == Mode::kModulo) {
      decodeModulo(
          input.encodedEnds.data(),
          input.offsets.data(),
          input.sizes.data(),
          input.nulls.data());
    } else if (selectedMode == Mode::kScalar) {
      decodeLanceListOffsetsScalar(
          reinterpret_cast<const char*>(input.encodedEnds.data()),
          kRows,
          kNullOffsetAdjustment,
          0,
          input.offsets.data(),
          input.sizes.data(),
          input.nulls.data());
    } else {
      decodeLanceListOffsetsAvx2(
          reinterpret_cast<const char*>(input.encodedEnds.data()),
          kRows,
          kNullOffsetAdjustment,
          0,
          input.offsets.data(),
          input.sizes.data(),
          input.nulls.data());
    }
    folly::doNotOptimizeAway(input.offsets.data());
    folly::doNotOptimizeAway(input.sizes.data());
    folly::doNotOptimizeAway(input.nulls.data());
  }
  return iterations;
}

BENCHMARK_MULTI(list_offsets_no_null, n) {
  return run(n, false);
}

BENCHMARK_MULTI(list_offsets_sparse_null, n) {
  return run(n, true);
}

} // namespace
} // namespace bytedance::bolt::lance::reader::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
