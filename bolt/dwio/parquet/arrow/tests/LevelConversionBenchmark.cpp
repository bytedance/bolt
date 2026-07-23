/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/dwio/parquet/arrow/LevelConversion.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

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

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
