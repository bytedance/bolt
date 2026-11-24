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

#include "bolt/exec/bm/BmWindowPartition.h"

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

DEFINE_uint64(
    bolt_benchmark_bm_window_extract_logical_bytes,
    16ULL << 20,
    "Logical payload bytes stored for each column-width case.");
DEFINE_uint32(
    bolt_benchmark_bm_window_extract_output_rows,
    32'768,
    "Rows extracted by each measured Window output batch.");
DEFINE_uint32(
    bolt_benchmark_bm_window_extract_input_batch_rows,
    4'096,
    "Rows per input vector while building the benchmark partition.");
DEFINE_uint32(
    bolt_benchmark_bm_window_extract_string_bytes,
    64,
    "Bytes in each VARCHAR benchmark value.");
DEFINE_uint32(
    bolt_benchmark_bm_window_extract_repeats,
    5,
    "Measured repetitions per extraction path.");
DEFINE_bool(
    bolt_benchmark_bm_window_extract_force_reclaim,
    false,
    "Evict unpinned BM blocks before every measured extraction so the timed "
    "path includes spill reads.");

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

using bytedance::bolt::exec::window::BmWindowPartition;
using bytedance::bolt::exec::window::BmWindowPartitionDescriptor;
using bytedance::bolt::memory::bm::MemoryTag;

constexpr vector_size_t kArrayElements = 4;

enum class PayloadKind : uint8_t {
  kFixed,
  kVarchar,
  kMixed,
  kArray,
};

const char* payloadName(PayloadKind payload) {
  switch (payload) {
    case PayloadKind::kFixed:
      return "fixed";
    case PayloadKind::kVarchar:
      return "varchar";
    case PayloadKind::kMixed:
      return "mixed";
    case PayloadKind::kArray:
      return "array_i64";
  }
  BOLT_UNREACHABLE();
}

enum class ColumnKind : uint8_t {
  kFixed,
  kVarchar,
  kArray,
};

ColumnKind columnKind(PayloadKind payload, column_index_t column) {
  switch (payload) {
    case PayloadKind::kFixed:
      return ColumnKind::kFixed;
    case PayloadKind::kVarchar:
      return ColumnKind::kVarchar;
    case PayloadKind::kMixed:
      return column % 2 == 0 ? ColumnKind::kFixed : ColumnKind::kVarchar;
    case PayloadKind::kArray:
      return ColumnKind::kArray;
  }
  BOLT_UNREACHABLE();
}

uint64_t logicalValueBytes(ColumnKind kind) {
  switch (kind) {
    case ColumnKind::kFixed:
      return sizeof(int64_t);
    case ColumnKind::kVarchar:
      return FLAGS_bolt_benchmark_bm_window_extract_string_bytes;
    case ColumnKind::kArray:
      return kArrayElements * sizeof(int64_t);
  }
  BOLT_UNREACHABLE();
}

int64_t fixedValue(vector_size_t row, column_index_t column) {
  return static_cast<int64_t>(row) * 131 + column;
}

int64_t
arrayValue(vector_size_t row, column_index_t column, vector_size_t index) {
  return fixedValue(row, column) * kArrayElements + index;
}

std::string
stringValue(vector_size_t row, column_index_t column, uint32_t size) {
  auto value = fmt::format("{}:{}:", column, row);
  BOLT_CHECK_LE(value.size(), size);
  value.append(size - value.size(), static_cast<char>('a' + column % 26));
  return value;
}

struct Sample {
  uint64_t elapsedUs{0};
  uint64_t loadCalls{0};
  uint64_t loadedRows{0};
  uint64_t maxLoadedRows{0};
  uint64_t pinCount{0};
  uint64_t spillReadCount{0};
  uint64_t spillReadBytes{0};
};

struct Average {
  void add(const Sample& sample) {
    elapsedUs += sample.elapsedUs;
    loadCalls += sample.loadCalls;
    loadedRows += sample.loadedRows;
    maxLoadedRows = std::max(maxLoadedRows, sample.maxLoadedRows);
    pinCount += sample.pinCount;
    spillReadCount += sample.spillReadCount;
    spillReadBytes += sample.spillReadBytes;
    ++samples;
  }

  uint64_t average(uint64_t value) const {
    return samples == 0 ? 0 : value / samples;
  }

  uint64_t elapsedUs{0};
  uint64_t loadCalls{0};
  uint64_t loadedRows{0};
  uint64_t maxLoadedRows{0};
  uint64_t pinCount{0};
  uint64_t spillReadCount{0};
  uint64_t spillReadBytes{0};
  uint64_t samples{0};
};

class ExtractCase {
 public:
  ExtractCase(PayloadKind payload, column_index_t numColumns)
      : payload_(payload),
        numColumns_(numColumns),
        context_(
            fmt::format(
                "bm-window-extract-{}-{}",
                payloadName(payload),
                numColumns),
            FLAGS_bolt_benchmark_bm_window_extract_logical_bytes,
            0,
            SpillCompressionKind::kRaw) {
    BOLT_CHECK_GT(numColumns_, 0);
    BOLT_CHECK_GT(FLAGS_bolt_benchmark_bm_window_extract_logical_bytes, 0);
    BOLT_CHECK_GT(FLAGS_bolt_benchmark_bm_window_extract_output_rows, 0);
    BOLT_CHECK_GT(FLAGS_bolt_benchmark_bm_window_extract_input_batch_rows, 0);
    BOLT_CHECK_GT(FLAGS_bolt_benchmark_bm_window_extract_repeats, 0);
    if (payload_ == PayloadKind::kVarchar || payload_ == PayloadKind::kMixed) {
      BOLT_CHECK_GE(FLAGS_bolt_benchmark_bm_window_extract_string_bytes, 16);
    }

    uint64_t logicalRowBytes = 0;
    for (column_index_t column = 0; column < numColumns_; ++column) {
      logicalRowBytes += logicalValueBytes(columnKind(payload_, column));
    }
    numRows_ = std::max<uint64_t>(
        1,
        FLAGS_bolt_benchmark_bm_window_extract_logical_bytes / logicalRowBytes);
    BOLT_CHECK_LE(numRows_, std::numeric_limits<vector_size_t>::max());

    types_.reserve(numColumns_);
    for (column_index_t column = 0; column < numColumns_; ++column) {
      switch (columnKind(payload_, column)) {
        case ColumnKind::kFixed:
          types_.push_back(BIGINT());
          break;
        case ColumnKind::kVarchar:
          types_.push_back(VARCHAR());
          break;
        case ColumnKind::kArray:
          types_.push_back(ARRAY(BIGINT()));
          break;
      }
    }
    columns_.resize(numColumns_);
    std::iota(columns_.begin(), columns_.end(), 0);
    std::vector<bool> nullable(numColumns_, true);
    container_ = std::make_unique<BmRowContainer>(
        types_, nullable, 0, context_.bufferManager, MemoryTag::kWindow);

    test::VectorMaker maker(context_.pool.get());
    for (vector_size_t start = 0; start < numRows_;
         start += FLAGS_bolt_benchmark_bm_window_extract_input_batch_rows) {
      const auto batchRows = std::min<vector_size_t>(
          FLAGS_bolt_benchmark_bm_window_extract_input_batch_rows,
          numRows_ - start);
      std::vector<VectorPtr> children;
      children.reserve(numColumns_);
      for (column_index_t column = 0; column < numColumns_; ++column) {
        switch (columnKind(payload_, column)) {
          case ColumnKind::kFixed:
            children.push_back(
                maker.flatVector<int64_t>(batchRows, [start, column](auto row) {
                  return fixedValue(start + row, column);
                }));
            break;
          case ColumnKind::kVarchar:
            children.push_back(maker.flatVector<std::string>(
                batchRows, [start, column](auto row) {
                  return stringValue(
                      start + row,
                      column,
                      FLAGS_bolt_benchmark_bm_window_extract_string_bytes);
                }));
            break;
          case ColumnKind::kArray:
            children.push_back(maker.arrayVector<int64_t>(
                batchRows,
                [](auto /*row*/) { return kArrayElements; },
                [start, column](auto row, auto index) {
                  return arrayValue(start + row, column, index);
                },
                nullptr,
                types_[column]));
            break;
        }
      }
      container_->appendBatch(maker.rowVector(std::move(children)));
    }

    segment_ = container_->spillActiveSegment();
    context_.bufferManager->Reclaim(context_.bufferManager->reclaimableBytes());
    context_.bufferManager->ReleaseUnusedReservation();
    context_.pool->release();

    BmWindowPartitionDescriptor descriptor;
    descriptor.numRows = numRows_;
    descriptor.ranges.push_back(
        {segment_, 0, static_cast<RowNumber>(numRows_)});
    descriptor.hasSpilledRows = true;
    partition_ = std::make_unique<BmWindowPartition>(
        container_.get(),
        context_.pool.get(),
        types_,
        std::move(descriptor),
        columns_,
        std::vector<std::pair<column_index_t, core::SortOrder>>{});

    // Leave too little root-pool headroom for a whole-partition BulkRead so
    // the first extraction selects WindowRead. Release the setup pressure
    // before measuring either extraction path.
    constexpr uint64_t kWindowReadSlack = 32ULL << 20;
    const auto freeBeforePressure = context_.rootPool->freeBytes();
    BOLT_CHECK_GT(freeBeforePressure, kWindowReadSlack);
    const auto pressureBytes = freeBeforePressure - kWindowReadSlack;
    auto pressurePool = context_.rootPool->addLeafChild("window-read-pressure");
    auto* pressure = pressurePool->allocate(pressureBytes);
    auto pressureGuard = folly::makeGuard(
        [&]() { pressurePool->free(pressure, pressureBytes); });
    auto probe = BaseVector::create(types_.front(), 0, context_.pool.get());
    partition_->extractColumn(0, 0, 1, 0, probe);
    pressureGuard.dismiss();
    pressurePool->free(pressure, pressureBytes);
    BOLT_CHECK_GT(partition_->reclaimReadChunks(), 0);
    context_.bufferManager->Reclaim(context_.bufferManager->reclaimableBytes());
    outputRows_ = std::min<vector_size_t>(
        FLAGS_bolt_benchmark_bm_window_extract_output_rows, numRows_);
    partitionOffset_ = (numRows_ - outputRows_) / 2;
  }

  Sample run(bool batchColumns, bool verify) {
    if (FLAGS_bolt_benchmark_bm_window_extract_force_reclaim) {
      context_.bufferManager->Reclaim(
          context_.bufferManager->reclaimableBytes());
    }

    std::vector<VectorPtr> results;
    results.reserve(numColumns_);
    for (const auto& type : types_) {
      results.push_back(BaseVector::create(type, 0, context_.pool.get()));
    }

#ifndef NDEBUG
    window::resetBmWindowPartitionTestStats();
#endif
    const auto before = context_.bufferManager->stats();
    const auto start = std::chrono::steady_clock::now();
    if (batchColumns) {
      partition_->extractInputColumns(
          partitionOffset_, outputRows_, 0, {results.data(), results.size()});
    } else {
      for (column_index_t column = 0; column < numColumns_; ++column) {
        partition_->extractColumn(
            column, partitionOffset_, outputRows_, 0, results[column]);
      }
    }
    const auto elapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    const auto after = context_.bufferManager->stats();
#ifndef NDEBUG
    const auto stats = window::bmWindowPartitionTestStats();
#endif

    if (verify) {
      verifyResults(results);
#ifndef NDEBUG
      BOLT_CHECK_GT(stats.loadRowsCalls, 0);
#endif
      BOLT_CHECK_EQ(context_.bufferManager->stats().pinnedResidentBytes, 0);
    }

    Sample sample{
        .elapsedUs = static_cast<uint64_t>(elapsedUs),
        .pinCount = after.pinCount - before.pinCount,
        .spillReadCount = after.spillReadCount - before.spillReadCount,
        .spillReadBytes = after.spillReadBytes - before.spillReadBytes};
#ifndef NDEBUG
    sample.loadCalls = stats.loadRowsCalls;
    sample.loadedRows = stats.loadedRows;
    sample.maxLoadedRows = stats.maxLoadedRows;
#endif
    return sample;
  }

  vector_size_t numRows() const {
    return numRows_;
  }

  vector_size_t outputRows() const {
    return outputRows_;
  }

 private:
  void verifyResults(const std::vector<VectorPtr>& results) const {
    BOLT_CHECK_EQ(results.size(), numColumns_);
    const std::array<vector_size_t, 3> sampleRows{
        0, outputRows_ / 2, outputRows_ - 1};
    for (const auto outputRow : sampleRows) {
      const auto sourceRow = partitionOffset_ + outputRow;
      for (column_index_t column = 0; column < numColumns_; ++column) {
        BOLT_CHECK_EQ(results[column]->size(), outputRows_);
        BOLT_CHECK(!results[column]->isNullAt(outputRow));
        switch (columnKind(payload_, column)) {
          case ColumnKind::kFixed:
            BOLT_CHECK_EQ(
                results[column]->asFlatVector<int64_t>()->valueAt(outputRow),
                fixedValue(sourceRow, column));
            break;
          case ColumnKind::kVarchar:
            BOLT_CHECK_EQ(
                results[column]
                    ->asFlatVector<StringView>()
                    ->valueAt(outputRow)
                    .str(),
                stringValue(
                    sourceRow,
                    column,
                    FLAGS_bolt_benchmark_bm_window_extract_string_bytes));
            break;
          case ColumnKind::kArray: {
            const auto* array = results[column]->as<ArrayVector>();
            BOLT_CHECK_NOT_NULL(array);
            BOLT_CHECK_EQ(kArrayElements, array->sizeAt(outputRow));
            const auto* elements =
                array->elements()->as<SimpleVector<int64_t>>();
            BOLT_CHECK_NOT_NULL(elements);
            const auto offset = array->offsetAt(outputRow);
            for (vector_size_t index = 0; index < kArrayElements; ++index) {
              BOLT_CHECK_EQ(
                  arrayValue(sourceRow, column, index),
                  elements->valueAt(offset + index));
            }
            break;
          }
        }
      }
    }
  }

  const PayloadKind payload_;
  const column_index_t numColumns_;
  BenchmarkContext context_;
  std::vector<TypePtr> types_;
  std::vector<column_index_t> columns_;
  std::unique_ptr<BmRowContainer> container_;
  SegmentId segment_{0};
  std::unique_ptr<BmWindowPartition> partition_;
  vector_size_t numRows_{0};
  vector_size_t outputRows_{0};
  vector_size_t partitionOffset_{0};
};

void printResult(
    PayloadKind payload,
    column_index_t columns,
    const char* path,
    const Average& result,
    double speedup) {
  fmt::print(
      "{:<9} {:>7} {:<12} {:>11} {:>10.2f} {:>12} {:>14} {:>14} {:>11} {:>12} {:>14}\n",
      payloadName(payload),
      columns,
      path,
      result.average(result.elapsedUs),
      speedup,
#ifndef NDEBUG
      fmt::format("{}", result.average(result.loadCalls)),
      fmt::format("{}", result.average(result.loadedRows)),
      fmt::format("{}", result.maxLoadedRows),
#else
      "n/a",
      "n/a",
      "n/a",
#endif
      result.average(result.pinCount),
      result.average(result.spillReadCount),
      result.average(result.spillReadBytes));
}

void runCase(PayloadKind payload, column_index_t columns) {
  ExtractCase benchmark(payload, columns);
  benchmark.run(false, true);
  benchmark.run(true, true);

  Average columnResults;
  Average batchResults;
  for (uint32_t repeat = 0;
       repeat < FLAGS_bolt_benchmark_bm_window_extract_repeats;
       ++repeat) {
    if (repeat % 2 == 0) {
      columnResults.add(benchmark.run(false, repeat == 0));
      batchResults.add(benchmark.run(true, repeat == 0));
    } else {
      batchResults.add(benchmark.run(true, false));
      columnResults.add(benchmark.run(false, false));
    }
  }

  const auto columnUs = columnResults.average(columnResults.elapsedUs);
  const auto batchUs = batchResults.average(batchResults.elapsedUs);
  printResult(payload, columns, "per-column", columnResults, 1.0);
  printResult(
      payload,
      columns,
      "chunk-batch",
      batchResults,
      batchUs == 0 ? 0 : static_cast<double>(columnUs) / batchUs);
}

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});
  bytedance::bolt::filesystems::registerLocalFileSystem();

  fmt::print(
      "{:<9} {:>7} {:<12} {:>11} {:>10} {:>12} {:>14} {:>14} {:>11} {:>12} {:>14}\n",
      "payload",
      "columns",
      "path",
      "avg_us",
      "speedup",
      "load_calls",
      "loaded_rows",
      "max_load_rows",
      "pin_count",
      "spill_reads",
      "spill_bytes");
  for (const auto payload :
       {bytedance::bolt::exec::bm::benchmarks::PayloadKind::kFixed,
        bytedance::bolt::exec::bm::benchmarks::PayloadKind::kVarchar,
        bytedance::bolt::exec::bm::benchmarks::PayloadKind::kMixed,
        bytedance::bolt::exec::bm::benchmarks::PayloadKind::kArray}) {
    for (const auto columns : {2, 4, 8, 16, 32}) {
      bytedance::bolt::exec::bm::benchmarks::runCase(payload, columns);
    }
  }
  return 0;
}
