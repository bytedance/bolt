/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/dwio/parquet/arrow/LevelConversion.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <map>
#include <memory>
#include <random>

using namespace bytedance::bolt::parquet::arrow;

namespace {

constexpr int32_t kNumLists = 4 * 1024 * 1024;
constexpr uint32_t kSeed = 20260723;

enum class ListShape {
  kSingleElement,
  kMixed,
  kLong,
};

struct Levels {
  std::vector<int16_t> definitions;
  std::vector<int16_t> repetitions;
};

Levels makeLevels(ListShape shape) {
  std::mt19937 rng(kSeed);
  std::uniform_int_distribution<int32_t> mixedLengthDist(0, 12);
  std::uniform_int_distribution<int32_t> longLengthDist(16, 64);
  std::uniform_int_distribution<int32_t> pct(0, 99);

  Levels levels;
  levels.definitions.reserve(
      shape == ListShape::kSingleElement ? kNumLists : kNumLists * 6);
  levels.repetitions.reserve(levels.definitions.capacity());

  for (int32_t list = 0; list < kNumLists; ++list) {
    const auto roll = pct(rng);
    if (roll < 5) {
      // Null list.
      levels.definitions.push_back(0);
      levels.repetitions.push_back(0);
      continue;
    }
    if (roll < 15) {
      // Empty list.
      levels.definitions.push_back(1);
      levels.repetitions.push_back(0);
      continue;
    }

    const int32_t length = [&]() {
      switch (shape) {
        case ListShape::kSingleElement:
          return 1;
        case ListShape::kMixed:
          return mixedLengthDist(rng);
        case ListShape::kLong:
          return longLengthDist(rng);
      }
      return 1;
    }();
    if (length == 0) {
      levels.definitions.push_back(1);
      levels.repetitions.push_back(0);
      continue;
    }

    for (int32_t i = 0; i < length; ++i) {
      levels.definitions.push_back(pct(rng) < 10 ? 1 : 2);
      levels.repetitions.push_back(i == 0 ? 0 : 1);
    }
  }

  return levels;
}

LevelInfo listLevelInfo() {
  LevelInfo info;
  info.rep_level = 1;
  info.def_level = 2;
  info.repeated_ancestor_def_level = 0;
  return info;
}

class LevelConversionBenchmark {
 public:
  explicit LevelConversionBenchmark(ListShape shape)
      : levels_(makeLevels(shape)) {
    validity_.resize(kNumLists);
    offsets_.resize(kNumLists + 1);
    lengths_.resize(kNumLists);
  }

  int64_t offsetsThenLengths() {
    resetIo();
    std::fill(offsets_.begin(), offsets_.end(), 0);
    DefRepLevelsToList(
        levels_.definitions.data(),
        levels_.repetitions.data(),
        levels_.definitions.size(),
        listLevelInfo(),
        &io_,
        offsets_.data());
    for (int64_t i = 0; i < io_.values_read; ++i) {
      lengths_[i] = offsets_[i + 1] - offsets_[i];
    }
    return io_.values_read;
  }

  int64_t directLengths() {
    resetIo();
    DefRepLevelsToListLengths(
        levels_.definitions.data(),
        levels_.repetitions.data(),
        levels_.definitions.size(),
        listLevelInfo(),
        &io_,
        lengths_.data());
    return io_.values_read;
  }

 private:
  void resetIo() {
    std::fill(validity_.begin(), validity_.end(), 0);
    io_ = ValidityBitmapInputOutput();
    io_.valid_bits = validity_.data();
    io_.values_read_upper_bound = kNumLists;
  }

  Levels levels_;
  std::vector<uint8_t> validity_;
  std::vector<int32_t> offsets_;
  std::vector<int32_t> lengths_;
  ValidityBitmapInputOutput io_;
};

enum class FusedListShape {
  kSingleElement,
  kMixed,
  kLong,
};

Levels makeFusedLevels(int32_t numLists, FusedListShape shape) {
  std::mt19937 rng(kSeed);
  std::uniform_int_distribution<int32_t> mixedLengthDist(0, 12);
  std::uniform_int_distribution<int32_t> longLengthDist(32, 128);
  std::uniform_int_distribution<int32_t> pct(0, 99);

  Levels levels;
  for (int32_t list = 0; list < numLists; ++list) {
    if (list % 19 == 0) {
      // Null struct and therefore null list.
      levels.definitions.push_back(0);
      levels.repetitions.push_back(0);
      continue;
    }
    if (list % 17 == 0) {
      // Present struct, empty list.
      levels.definitions.push_back(2);
      levels.repetitions.push_back(0);
      continue;
    }

    int32_t length = 1;
    if (shape == FusedListShape::kMixed) {
      length = mixedLengthDist(rng);
    } else if (shape == FusedListShape::kLong) {
      length = longLengthDist(rng);
    }
    if (length == 0) {
      levels.definitions.push_back(2);
      levels.repetitions.push_back(0);
      continue;
    }

    for (int32_t index = 0; index < length; ++index) {
      levels.definitions.push_back(pct(rng) < 10 ? 3 : 4);
      levels.repetitions.push_back(index == 0 ? 0 : 1);
    }
  }
  return levels;
}

LevelInfo fusedListLevelInfo() {
  LevelInfo info;
  info.rep_level = 1;
  info.def_level = 3;
  return info;
}

LevelInfo fusedStructLevelInfo() {
  LevelInfo info;
  info.rep_level = 0;
  info.def_level = 1;
  return info;
}

LevelInfo unsupportedFusedStructLevelInfo() {
  LevelInfo info = fusedStructLevelInfo();
  info.rep_level = 1;
  return info;
}

class FusedLevelConversionBenchmark {
 public:
  FusedLevelConversionBenchmark(int32_t numLists, FusedListShape shape)
      : numLists_(numLists), levels_(makeFusedLevels(numLists, shape)) {
    lengths_.resize(numLists);
    listValidity_.resize(numLists);
    structValidity_.resize(numLists);
  }

  int64_t separate() {
    auto listOutput = makeOutput(listValidity_);
    DefRepLevelsToListLengths(
        levels_.definitions.data(),
        levels_.repetitions.data(),
        levels_.definitions.size(),
        fusedListLevelInfo(),
        &listOutput,
        lengths_.data());

    auto structOutput = makeOutput(structValidity_);
    DefRepLevelsToBitmap(
        levels_.definitions.data(),
        levels_.repetitions.data(),
        levels_.definitions.size(),
        fusedStructLevelInfo(),
        &structOutput);
    return listOutput.values_read + structOutput.values_read;
  }

  int64_t fused() {
    auto listOutput = makeOutput(listValidity_);
    auto structOutput = makeOutput(structValidity_);
    const auto converted = DefRepLevelsToListLengthsAndStructBitmap(
        levels_.definitions.data(),
        levels_.repetitions.data(),
        levels_.definitions.size(),
        fusedListLevelInfo(),
        fusedStructLevelInfo(),
        &listOutput,
        lengths_.data(),
        &structOutput);
    folly::doNotOptimizeAway(converted);
    return listOutput.values_read + structOutput.values_read;
  }

  int64_t fallback() {
    auto listOutput = makeOutput(listValidity_);
    auto structOutput = makeOutput(structValidity_);
    const auto converted = DefRepLevelsToListLengthsAndStructBitmap(
        levels_.definitions.data(),
        levels_.repetitions.data(),
        levels_.definitions.size(),
        fusedListLevelInfo(),
        unsupportedFusedStructLevelInfo(),
        &listOutput,
        lengths_.data(),
        &structOutput);
    folly::doNotOptimizeAway(converted);
    if (!converted) {
      DefRepLevelsToListLengths(
          levels_.definitions.data(),
          levels_.repetitions.data(),
          levels_.definitions.size(),
          fusedListLevelInfo(),
          &listOutput,
          lengths_.data());

      structOutput = makeOutput(structValidity_);
      DefRepLevelsToBitmap(
          levels_.definitions.data(),
          levels_.repetitions.data(),
          levels_.definitions.size(),
          fusedStructLevelInfo(),
          &structOutput);
    }
    return listOutput.values_read + structOutput.values_read;
  }

 private:
  ValidityBitmapInputOutput makeOutput(std::vector<uint8_t>& validity) {
    std::fill(validity.begin(), validity.end(), 0);
    ValidityBitmapInputOutput output;
    output.valid_bits = validity.data();
    output.values_read_upper_bound = numLists_;
    return output;
  }

  int32_t numLists_;
  Levels levels_;
  std::vector<int32_t> lengths_;
  std::vector<uint8_t> listValidity_;
  std::vector<uint8_t> structValidity_;
};

LevelConversionBenchmark& benchmark(ListShape shape) {
  static LevelConversionBenchmark singleElement(ListShape::kSingleElement);
  static LevelConversionBenchmark mixed(ListShape::kMixed);
  static LevelConversionBenchmark longLists(ListShape::kLong);
  switch (shape) {
    case ListShape::kSingleElement:
      return singleElement;
    case ListShape::kMixed:
      return mixed;
    case ListShape::kLong:
      return longLists;
  }
  return singleElement;
}

FusedLevelConversionBenchmark& fusedBenchmark(
    int32_t numLists,
    FusedListShape shape) {
  using Key = std::pair<int32_t, FusedListShape>;
  static std::map<Key, std::unique_ptr<FusedLevelConversionBenchmark>>
      benchmarks;
  const Key key{numLists, shape};
  auto& instance = benchmarks[key];
  if (!instance) {
    instance = std::make_unique<FusedLevelConversionBenchmark>(numLists, shape);
  }
  return *instance;
}

void runOffsetsThenLengths(uint32_t iters, ListShape shape) {
  folly::BenchmarkSuspender suspender;
  auto& instance = benchmark(shape);
  suspender.dismiss();
  while (iters--) {
    folly::doNotOptimizeAway(instance.offsetsThenLengths());
  }
}

void runDirectLengths(uint32_t iters, ListShape shape) {
  folly::BenchmarkSuspender suspender;
  auto& instance = benchmark(shape);
  suspender.dismiss();
  while (iters--) {
    folly::doNotOptimizeAway(instance.directLengths());
  }
}

void runFusedLevelConversion(
    uint32_t iters,
    int32_t numLists,
    FusedListShape shape,
    bool fused,
    bool fallback = false) {
  folly::BenchmarkSuspender suspender;
  auto& instance = fusedBenchmark(numLists, shape);
  suspender.dismiss();
  while (iters--) {
    folly::doNotOptimizeAway(
        fallback    ? instance.fallback()
            : fused ? instance.fused()
                    : instance.separate());
  }
}

} // namespace

BENCHMARK(offsetsThenLengths_single, iters) {
  runOffsetsThenLengths(iters, ListShape::kSingleElement);
}

BENCHMARK_RELATIVE(directLengths_single, iters) {
  runDirectLengths(iters, ListShape::kSingleElement);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(offsetsThenLengths_mixed, iters) {
  runOffsetsThenLengths(iters, ListShape::kMixed);
}

BENCHMARK_RELATIVE(directLengths_mixed, iters) {
  runDirectLengths(iters, ListShape::kMixed);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(offsetsThenLengths_long, iters) {
  runOffsetsThenLengths(iters, ListShape::kLong);
}

BENCHMARK_RELATIVE(directLengths_long, iters) {
  runDirectLengths(iters, ListShape::kLong);
}

BENCHMARK_DRAW_LINE();

#define FUSED_LEVEL_CONVERSION_BENCHMARK(size, shape, name)                   \
  BENCHMARK(separateStructAndList_##name##_##size, iters) {                   \
    runFusedLevelConversion(iters, size, FusedListShape::shape, false);       \
  }                                                                           \
  BENCHMARK_RELATIVE(fusedStructAndList_##name##_##size, iters) {             \
    runFusedLevelConversion(iters, size, FusedListShape::shape, true);        \
  }                                                                           \
  BENCHMARK_RELATIVE(fallbackStructAndList_##name##_##size, iters) {          \
    runFusedLevelConversion(iters, size, FusedListShape::shape, false, true); \
  }                                                                           \
  BENCHMARK_DRAW_LINE()

FUSED_LEVEL_CONVERSION_BENCHMARK(1024, kSingleElement, single);
FUSED_LEVEL_CONVERSION_BENCHMARK(65536, kSingleElement, single);
FUSED_LEVEL_CONVERSION_BENCHMARK(1048576, kSingleElement, single);
FUSED_LEVEL_CONVERSION_BENCHMARK(1024, kMixed, mixed);
FUSED_LEVEL_CONVERSION_BENCHMARK(65536, kMixed, mixed);
FUSED_LEVEL_CONVERSION_BENCHMARK(1048576, kMixed, mixed);
FUSED_LEVEL_CONVERSION_BENCHMARK(1024, kLong, long);
FUSED_LEVEL_CONVERSION_BENCHMARK(65536, kLong, long);
FUSED_LEVEL_CONVERSION_BENCHMARK(1048576, kLong, long);

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
