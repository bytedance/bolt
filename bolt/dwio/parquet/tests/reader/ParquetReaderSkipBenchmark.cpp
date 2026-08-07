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

#include "bolt/common/io/IoStatistics.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/tests/utils/DataSetBuilder.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::test;

namespace {

// Keep each row group deterministic and reasonably small so skip overhead is
// observable. Data size around 100MB.
constexpr uint32_t kRowsPerRowGroup = 100'000; // 100k rows per row group
constexpr uint32_t kNumRowGroups = 5; // 5 row groups
constexpr uint32_t kNumRowsPerBatch = kRowsPerRowGroup;
constexpr uint32_t kNumBatches = kNumRowGroups;
constexpr uint32_t kTotalRows = kRowsPerRowGroup * kNumRowGroups;

class SkipBenchmark {
 public:
  SkipBenchmark() {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetSkipBenchmark");
    leafPool_ = rootPool_->addLeafChild("ParquetSkipBenchmark");
    // Use multiple columns so that each row group has a reasonable size
    rowType_ =
        ROW({"id",
             "value1",
             "value2",
             "value3",
             "value4",
             "value5",
             "value6",
             "value7",
             "value8",
             "value9"},
            {BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT()});
    path_ = fileFolder_->path + "/skip_bench.parquet";
    writeFile();
  }

  // Produces a single-column BIGINT parquet file with kNumRowGroups fixed-size
  // row groups of kRowsPerRowGroup rows each.
  void writeFile() {
    auto localWriteFile = std::make_unique<LocalWriteFile>(path_, true, false);
    auto sink =
        std::make_unique<WriteFileSink>(std::move(localWriteFile), path_);

    bytedance::bolt::parquet::WriterOptions options;
    options.memoryPool = rootPool_.get();
    // Pin the row group boundary at exactly kRowsPerRowGroup rows, regardless
    // of memory pressure.
    const uint64_t rowsInRowGroup = kRowsPerRowGroup;
    options.flushPolicyFactory = [rowsInRowGroup]() {
      // Large byte limit so only row count decides the boundary.
      return std::make_unique<DefaultFlushPolicy>(rowsInRowGroup, 1LL << 40);
    };

    auto writer = std::make_unique<bytedance::bolt::parquet::Writer>(
        std::move(sink), options, rowType_);

    DataSetBuilder builder(*leafPool_, 0);
    auto batches =
        builder.makeDataset(rowType_, kNumBatches, kNumRowsPerBatch).build();
    for (auto& batch : *batches) {
      writer->write(batch);
    }
    writer->flush();
    writer->close();
  }

  std::pair<
      std::unique_ptr<dwio::common::RowReader>,
      std::shared_ptr<io::IoStatistics>>
  makeRowReaderWithStats(int32_t prefetch) {
    auto ioStats = std::make_shared<io::IoStatistics>();
    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    readerOpts.setPrefetchRowGroups(prefetch);
    // Avoid preloading the whole file; keep I/O observable at row-group level.
    readerOpts.setFilePreloadThreshold(0);
    readerOpts.setFooterEstimatedSize(16);
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>(path_),
        readerOpts.getMemoryPool(),
        dwio::common::MetricsLog::voidLog(),
        ioStats.get());
    auto reader = std::make_unique<ParquetReader>(std::move(input), readerOpts);

    dwio::common::RowReaderOptions rowReaderOpts;
    auto scanSpec = std::make_shared<ScanSpec>("root");
    scanSpec->addAllChildFields(*rowType_);
    rowReaderOpts.setScanSpec(scanSpec);
    return {reader->createRowReader(rowReaderOpts), ioStats};
  }

  uint64_t skipAll(dwio::common::RowReader& rr, uint64_t rows) {
    uint64_t total = 0;
    while (rows > 0) {
      auto n = rr.skip(rows);
      if (n == 0) {
        break;
      }
      total += n;
      rows -= n;
    }
    return total;
  }

  // Touch a small batch so that the benchmark captures the cost of landing +
  // decoding at the skip target (and so the skip optimization actually
  // influences observed wall time).
  uint64_t touchBatch(dwio::common::RowReader& rr, uint64_t size) {
    VectorPtr result = BaseVector::create(rowType_, 0, leafPool_.get());
    auto n = rr.next(size, result);
    folly::doNotOptimizeAway(result);
    return n;
  }

  // Scenario A: skip kept within the first row group. Baseline.
  void skipWithinRowGroup(
      int32_t prefetch,
      folly::UserCounters& counters,
      unsigned int iters) {
    uint64_t totalRawBytes = 0;
    for (unsigned int i = 0; i < iters; i++) {
      folly::BenchmarkSuspender suspender;
      auto [rr, ioStats] = makeRowReaderWithStats(prefetch);
      suspender.dismiss();

      auto skipped = skipAll(*rr, kRowsPerRowGroup / 2);
      folly::doNotOptimizeAway(skipped);
      touchBatch(*rr, 1024);

      totalRawBytes += ioStats->rawBytesRead();
    }
    counters["RawBytes"] = totalRawBytes / iters;
  }

  // Scenario B: skip exactly N whole row groups then read a small batch from
  // the landing group. The core win of the optimization lives here.
  void skipWholeRowGroups(
      int32_t prefetch,
      uint32_t numWholeGroups,
      folly::UserCounters& counters,
      unsigned int iters) {
    uint64_t totalRawBytes = 0;
    for (unsigned int i = 0; i < iters; i++) {
      folly::BenchmarkSuspender suspender;
      auto [rr, ioStats] = makeRowReaderWithStats(prefetch);
      suspender.dismiss();

      const uint64_t rows =
          static_cast<uint64_t>(numWholeGroups) * kRowsPerRowGroup;
      auto skipped = skipAll(*rr, rows);
      folly::doNotOptimizeAway(skipped);
      touchBatch(*rr, 1024);

      totalRawBytes += ioStats->rawBytesRead();
    }
    counters["RawBytes"] = totalRawBytes / iters;
  }

  // Scenario C: skip that lands inside some later row group (not aligned).
  void skipIntoMiddleOfFarRowGroup(
      int32_t prefetch,
      uint32_t numWholeGroups,
      folly::UserCounters& counters,
      unsigned int iters) {
    uint64_t totalRawBytes = 0;
    for (unsigned int i = 0; i < iters; i++) {
      folly::BenchmarkSuspender suspender;
      auto [rr, ioStats] = makeRowReaderWithStats(prefetch);
      suspender.dismiss();

      const uint64_t rows =
          static_cast<uint64_t>(numWholeGroups) * kRowsPerRowGroup +
          kRowsPerRowGroup / 3;
      auto skipped = skipAll(*rr, rows);
      folly::doNotOptimizeAway(skipped);
      touchBatch(*rr, 1024);

      totalRawBytes += ioStats->rawBytesRead();
    }
    counters["RawBytes"] = totalRawBytes / iters;
  }

  // Scenario D: skip past EOF; no subsequent read required.
  void skipPastEof(
      int32_t prefetch,
      folly::UserCounters& counters,
      unsigned int iters) {
    uint64_t totalRawBytes = 0;
    for (unsigned int i = 0; i < iters; i++) {
      folly::BenchmarkSuspender suspender;
      auto [rr, ioStats] = makeRowReaderWithStats(prefetch);
      suspender.dismiss();

      auto skipped = skipAll(*rr, kTotalRows + 1'000'000);
      folly::doNotOptimizeAway(skipped);

      totalRawBytes += ioStats->rawBytesRead();
    }
    counters["RawBytes"] = totalRawBytes / iters;
  }

  // Scenario E: alternating next()/skip() mimicking a LIMIT/OFFSET style
  // workload that skims through the file.
  void alternatingNextSkip(
      int32_t prefetch,
      folly::UserCounters& counters,
      unsigned int iters) {
    uint64_t totalRawBytes = 0;
    for (unsigned int i = 0; i < iters; i++) {
      folly::BenchmarkSuspender suspender;
      auto [rr, ioStats] = makeRowReaderWithStats(prefetch);
      suspender.dismiss();

      // Read 1k rows, skip 9k rows (one whole-group worth), repeat. This
      // exercises many skip()s each crossing exactly one row-group boundary.
      uint64_t totalRead = 0;
      while (true) {
        auto n = touchBatch(*rr, 1024);
        if (n == 0) {
          break;
        }
        totalRead += n;
        auto skipped = skipAll(*rr, kRowsPerRowGroup - 1024);
        if (skipped == 0) {
          break;
        }
      }
      folly::doNotOptimizeAway(totalRead);

      totalRawBytes += ioStats->rawBytesRead();
    }
    counters["RawBytes"] = totalRawBytes / iters;
  }

 private:
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  RowTypePtr rowType_;
  std::shared_ptr<bytedance::bolt::exec::test::TempDirectoryPath> fileFolder_ =
      bytedance::bolt::exec::test::TempDirectoryPath::create();
  std::string path_;
};

SkipBenchmark* FOLLY_NULLABLE gFixture = nullptr;

SkipBenchmark& fixture() {
  BOLT_CHECK_NOT_NULL(gFixture);
  return *gFixture;
}

} // namespace

// --- Registrations ---

// Scenario A: skip only within current row group (prefetch0)
BENCHMARK_COUNTERS(skipWithinRG_prefetch0, counters, iters) {
  fixture().skipWithinRowGroup(0, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario B: skip N whole row groups (prefetch0)
BENCHMARK_COUNTERS(skip1RG_prefetch0, counters, iters) {
  fixture().skipWholeRowGroups(0, 1, counters, iters);
}
BENCHMARK_COUNTERS(skip2RG_prefetch0, counters, iters) {
  fixture().skipWholeRowGroups(0, 2, counters, iters);
}
BENCHMARK_COUNTERS(skip3RG_prefetch0, counters, iters) {
  fixture().skipWholeRowGroups(0, 3, counters, iters);
}
BENCHMARK_COUNTERS(skip4RG_prefetch0, counters, iters) {
  fixture().skipWholeRowGroups(0, 4, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario C: skip into middle of a row group (prefetch0)
BENCHMARK_COUNTERS(skipMid1RG_prefetch0, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(0, 1, counters, iters);
}
BENCHMARK_COUNTERS(skipMid2RG_prefetch0, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(0, 2, counters, iters);
}
BENCHMARK_COUNTERS(skipMid3RG_prefetch0, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(0, 3, counters, iters);
}
BENCHMARK_COUNTERS(skipMid4RG_prefetch0, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(0, 4, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario D: skip past end of file (prefetch0)
BENCHMARK_COUNTERS(skipPastEof_prefetch0, counters, iters) {
  fixture().skipPastEof(0, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario E: alternating next()/skip() (prefetch0)
BENCHMARK_COUNTERS(alternatingNextSkip_prefetch0, counters, iters) {
  fixture().alternatingNextSkip(0, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario A: skip only within current row group (prefetch1)
BENCHMARK_COUNTERS(skipWithinRG_prefetch1, counters, iters) {
  fixture().skipWithinRowGroup(1, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario B: skip N whole row groups (prefetch1)
BENCHMARK_COUNTERS(skip1RG_prefetch1, counters, iters) {
  fixture().skipWholeRowGroups(1, 1, counters, iters);
}
BENCHMARK_COUNTERS(skip2RG_prefetch1, counters, iters) {
  fixture().skipWholeRowGroups(1, 2, counters, iters);
}
BENCHMARK_COUNTERS(skip3RG_prefetch1, counters, iters) {
  fixture().skipWholeRowGroups(1, 3, counters, iters);
}
BENCHMARK_COUNTERS(skip4RG_prefetch1, counters, iters) {
  fixture().skipWholeRowGroups(1, 4, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario C: skip into middle of a row group (prefetch1)
BENCHMARK_COUNTERS(skipMid1RG_prefetch1, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(1, 1, counters, iters);
}
BENCHMARK_COUNTERS(skipMid2RG_prefetch1, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(1, 2, counters, iters);
}
BENCHMARK_COUNTERS(skipMid3RG_prefetch1, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(1, 3, counters, iters);
}
BENCHMARK_COUNTERS(skipMid4RG_prefetch1, counters, iters) {
  fixture().skipIntoMiddleOfFarRowGroup(1, 4, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario D: skip past end of file (prefetch1)
BENCHMARK_COUNTERS(skipPastEof_prefetch1, counters, iters) {
  fixture().skipPastEof(1, counters, iters);
}
BENCHMARK_DRAW_LINE();

// Scenario E: alternating next()/skip() (prefetch1)
BENCHMARK_COUNTERS(alternatingNextSkip_prefetch1, counters, iters) {
  fixture().alternatingNextSkip(1, counters, iters);
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  registerParquetReaderFactory();
  SkipBenchmark fixtureInstance;
  gFixture = &fixtureInstance;

  folly::runBenchmarks();

  return 0;
}
