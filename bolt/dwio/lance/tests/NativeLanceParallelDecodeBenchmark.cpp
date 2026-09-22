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
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <zstd.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/common/ParallelFor.h"

namespace bytedance::bolt::lance::reader::benchmark {
namespace {

size_t parallelism() {
  const auto* value = std::getenv("BOLT_LANCE_PARALLEL_DECODE_THREADS");
  return value == nullptr ? 1 : std::max<size_t>(1, std::stoull(value));
}

struct ZstdWorkload {
  ZstdWorkload(size_t numColumns, size_t bytesPerColumn)
      : compressed(numColumns), outputs(numColumns) {
    std::vector<char> input(bytesPerColumn);
    for (size_t i = 0; i < input.size(); ++i) {
      input[i] = static_cast<char>((i * 17 + (i / 4096) * 13) & 0xff);
    }
    for (size_t column = 0; column < numColumns; ++column) {
      compressed[column].resize(ZSTD_compressBound(input.size()));
      const auto size = ZSTD_compress(
          compressed[column].data(),
          compressed[column].size(),
          input.data(),
          input.size(),
          0);
      BOLT_CHECK(!ZSTD_isError(size));
      compressed[column].resize(size);
      outputs[column].resize(input.size());
    }
  }

  std::vector<std::vector<char>> compressed;
  std::vector<std::vector<char>> outputs;
};

void decodeOne(ZstdWorkload& workload, size_t column) {
  struct ZstdContextDeleter {
    void operator()(ZSTD_DCtx* context) const {
      ZSTD_freeDCtx(context);
    }
  };
  thread_local auto context =
      std::unique_ptr<ZSTD_DCtx, ZstdContextDeleter>(ZSTD_createDCtx());
  const auto& input = workload.compressed[column];
  auto& output = workload.outputs[column];
  const auto size = ZSTD_decompressDCtx(
      context.get(), output.data(), output.size(), input.data(), input.size());
  BOLT_CHECK(!ZSTD_isError(size));
  BOLT_CHECK_EQ(size, output.size());
}

uint64_t run(uint32_t iterations, size_t numColumns, size_t bytesPerColumn) {
  static ZstdWorkload small(8, 64 << 10);
  static ZstdWorkload medium(16, 64 << 10);
  static ZstdWorkload wide(32, 64 << 10);
  static ZstdWorkload large(8, 256 << 10);
  auto& workload = numColumns == 32   ? wide
      : numColumns == 16              ? medium
      : bytesPerColumn == (256 << 10) ? large
                                      : small;
  static const auto threads = parallelism();
  static const std::shared_ptr<folly::Executor> executor = threads > 1
      ? std::make_shared<folly::CPUThreadPoolExecutor>(threads - 1)
      : nullptr;
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    dwio::common::ParallelFor(executor, 0, numColumns, threads)
        .execute([&](size_t column) { decodeOne(workload, column); });
    folly::doNotOptimizeAway(workload.outputs.data());
  }
  return iterations;
}

BENCHMARK_MULTI(zstd_columns_8x64k, n) {
  return run(n, 8, 64 << 10);
}

BENCHMARK_MULTI(zstd_columns_32x64k, n) {
  return run(n, 32, 64 << 10);
}

BENCHMARK_MULTI(zstd_columns_16x64k, n) {
  return run(n, 16, 64 << 10);
}

BENCHMARK_MULTI(zstd_columns_8x256k, n) {
  return run(n, 8, 256 << 10);
}

} // namespace
} // namespace bytedance::bolt::lance::reader::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
