#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);
DECLARE_bool(bm_row_container_spill_metrics);

DEFINE_uint64(
    bm_row_container_pipeline_window_rows,
    65536,
    "Rows per loadRows() call in BM row container pipeline window benchmark.");

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

enum class PipelineBmReadMode {
  kLoadedPointers,
  kWindowRead,
};

struct PipelineOldMetrics {
  uint64_t storeNs{0};
  uint64_t spillWriteNs{0};
  uint64_t spillReadNs{0};
  uint64_t extractNs{0};
  uint64_t rows{0};
  OldSpillWriteMetrics spillWrite;
  OldSpillReadMetrics spillRead;
};

struct PipelineBmMetrics {
  uint64_t storeNs{0};
  uint64_t spillWriteNs{0};
  uint64_t spillReadNs{0};
  uint64_t extractNs{0};
  uint64_t rows{0};
  uint64_t rowIds{0};
  uint64_t windows{0};
  bool resultPointers{false};
  BulkLoadMetrics bulkLoad;
  memory::bm::BufferManagerStats statsDelta;
};

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

const char* bmModeName(PipelineBmReadMode mode) {
  switch (mode) {
    case PipelineBmReadMode::kLoadedPointers:
      return "loaded_pointers";
    case PipelineBmReadMode::kWindowRead:
      return "window_read";
  }
  BOLT_UNREACHABLE();
}

void accumulateBmStatsDelta(
    memory::bm::BufferManagerStats& total,
    const memory::bm::BufferManagerStats& before,
    const memory::bm::BufferManagerStats& after) {
  total.batchPinCount += counterDelta(before.batchPinCount, after.batchPinCount);
  total.pinReadCount += counterDelta(before.pinReadCount, after.pinReadCount);
  total.spillWriteCount +=
      counterDelta(before.spillWriteCount, after.spillWriteCount);
  total.spillWriteBytes +=
      counterDelta(before.spillWriteBytes, after.spillWriteBytes);
  total.spillPhysicalWriteBytes += counterDelta(
      before.spillPhysicalWriteBytes, after.spillPhysicalWriteBytes);
  total.spillCompressionTimeUs += counterDelta(
      before.spillCompressionTimeUs, after.spillCompressionTimeUs);
  total.spillCompressedBlocks += counterDelta(
      before.spillCompressedBlocks, after.spillCompressedBlocks);
  total.spillReadCount +=
      counterDelta(before.spillReadCount, after.spillReadCount);
  total.spillReadBytes +=
      counterDelta(before.spillReadBytes, after.spillReadBytes);
  total.spillPhysicalReadBytes += counterDelta(
      before.spillPhysicalReadBytes, after.spillPhysicalReadBytes);
  total.spillDecompressionTimeUs += counterDelta(
      before.spillDecompressionTimeUs, after.spillDecompressionTimeUs);
}

void printOldMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const PipelineOldMetrics& metrics) {
  if (!shouldPrintSpillMetrics("pipelineOld", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] pipelineOld dataset={} iterations={} "
      "logical_bytes={} rows={} store_ms={:.3f} spill_write_ms={:.3f} "
      "spill_read_ms={:.3f} extract_ms={:.3f} total_ms={:.3f} "
      "serialized_bytes={} batches={} files={} spill_bytes={}\n",
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      metrics.rows,
      nsToMs(metrics.storeNs),
      nsToMs(metrics.spillWriteNs),
      nsToMs(metrics.spillReadNs),
      nsToMs(metrics.extractNs),
      nsToMs(
          metrics.storeNs + metrics.spillWriteNs + metrics.spillReadNs +
          metrics.extractNs),
      metrics.spillRead.serializedBytes,
      metrics.spillRead.batches,
      metrics.spillWrite.files,
      metrics.spillWrite.spillBytes);
}

void printBmMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    PipelineBmReadMode mode,
    const BenchmarkOptions& opts,
    const PipelineBmMetrics& metrics) {
  const auto* benchmark = mode == PipelineBmReadMode::kLoadedPointers
      ? "pipelineBmLoaded"
      : "pipelineBmWindow";
  if (!shouldPrintSpillMetrics(benchmark, dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  const auto& stats = metrics.statsDelta;
  const auto& bulk = metrics.bulkLoad;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] {} dataset={} mode={} iterations={} "
      "logical_bytes={} rows={} row_ids={} windows={} result={} "
      "store_ms={:.3f} spill_write_ms={:.3f} spill_read_ms={:.3f} "
      "extract_ms={:.3f} total_ms={:.3f} "
      "bulk_estimate_ms={:.3f} bulk_reserve_ms={:.3f} "
      "bulk_collect_blocks_ms={:.3f} bulk_batch_pin_ms={:.3f} "
      "bulk_update_ptrs_ms={:.3f} bulk_rebase_strings_ms={:.3f} "
      "bulk_append_ptrs_ms={:.3f} bulk_append_row_ids_ms={:.3f} "
      "bulk_estimated_bytes={} bulk_pinned_blocks={} "
      "bulk_pointer_rows={} bulk_row_id_rows={} "
      "bulk_rebased_string_views={} "
      "bm_batch_pins={} bm_pin_reads={} "
      "bm_spill_write_count={} bm_spill_write_bytes={} "
      "bm_spill_physical_write_bytes={} bm_compress_ms={:.3f} "
      "bm_compressed_blocks={} bm_spill_read_count={} "
      "bm_spill_read_bytes={} bm_spill_physical_read_bytes={} "
      "bm_decompress_ms={:.3f}\n",
      benchmark,
      datasetName(dataset),
      bmModeName(mode),
      iterations,
      opts.dataBytes,
      metrics.rows,
      metrics.rowIds,
      metrics.windows,
      metrics.resultPointers ? "pointers" : "row_ids",
      nsToMs(metrics.storeNs),
      nsToMs(metrics.spillWriteNs),
      nsToMs(metrics.spillReadNs),
      nsToMs(metrics.extractNs),
      nsToMs(
          metrics.storeNs + metrics.spillWriteNs + metrics.spillReadNs +
          metrics.extractNs),
      nsToMs(bulk.estimateBytesNs),
      nsToMs(bulk.reserveNs),
      nsToMs(bulk.collectBlocksNs),
      nsToMs(bulk.batchPinNs),
      nsToMs(bulk.updateBlockPointersNs),
      nsToMs(bulk.rebaseStringViewsNs),
      nsToMs(bulk.appendRowPointersNs),
      nsToMs(bulk.appendRowIdsNs),
      bulk.estimatedBytes,
      bulk.pinnedBlocks,
      bulk.pointerRows,
      bulk.rowIdRows,
      bulk.rebasedStringViews,
      stats.batchPinCount,
      stats.pinReadCount,
      stats.spillWriteCount,
      stats.spillWriteBytes,
      stats.spillPhysicalWriteBytes,
      static_cast<double>(stats.spillCompressionTimeUs) / 1000.0,
      stats.spillCompressedBlocks,
      stats.spillReadCount,
      stats.spillReadBytes,
      stats.spillPhysicalReadBytes,
      static_cast<double>(stats.spillDecompressionTimeUs) / 1000.0);
}

void extractBmWindowRows(
    BmRowContainer& container,
    ReadOnlyWindowReadSession& session,
    const std::vector<RowId>& rowIds,
    const BenchmarkOptions& opts,
    memory::MemoryPool* pool,
    PipelineBmMetrics& metrics) {
  const auto windowRows = static_cast<size_t>(
      std::max<uint64_t>(1, FLAGS_bm_row_container_pipeline_window_rows));
  for (size_t offset = 0; offset < rowIds.size();) {
    const auto batchRows = std::min(windowRows, rowIds.size() - offset);
    const auto spillReadStart = benchmarkNowNs();
    auto rows = session.loadRows({rowIds.data() + offset, batchRows});
    metrics.spillReadNs += benchmarkNowNs() - spillReadStart;
    ++metrics.windows;

    const auto extractStart = benchmarkNowNs();
    extractBmRowsResident(container, rows, opts, pool);
    metrics.extractNs += benchmarkNowNs() - extractStart;
    offset += batchRows;
  }
}

void pipelineOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  PipelineOldMetrics metrics;
  BenchmarkOptions printedOpts;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    checkOldRowBasedSpillBenchmarkSupported(opts);
    printedOpts = opts;
    BenchmarkContext context("pipeline-old", opts.dataBytes, 8);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    auto container = makeOldRowContainer(dataset, context.pool.get());
    suspender.dismiss();

    const auto storeStart = benchmarkNowNs();
    storeReusableInputBatchesOld(*container, input, opts);
    metrics.storeNs += benchmarkNowNs() - storeStart;
    metrics.rows += rowCount(opts);

    const auto spillWriteStart = benchmarkNowNs();
    auto spill = spillOldRows(
        context,
        *container,
        dataset,
        FLAGS_bm_row_container_spill_metrics ? &metrics.spillWrite : nullptr);
    metrics.spillWriteNs += benchmarkNowNs() - spillWriteStart;
    container.reset();

    std::vector<char*> restoredRows;
    restoredRows.reserve(rowCount(opts));
    const auto spillReadStart = benchmarkNowNs();
    auto restored = readOldSpillIntoNewRowContainer(
        context,
        spill,
        dataset,
        FLAGS_bm_row_container_spill_metrics ? &metrics.spillRead : nullptr,
        &restoredRows);
    metrics.spillReadNs += benchmarkNowNs() - spillReadStart;

    const auto extractStart = benchmarkNowNs();
    extractOldRows(*restored, restoredRows, opts, context.pool.get());
    metrics.extractNs += benchmarkNowNs() - extractStart;
    folly::doNotOptimizeAway(restored->numRows());
    suspender.rehire();
  }
  printOldMetrics(dataset, iterations, printedOpts, metrics);
}

void pipelineBm(
    uint32_t iterations,
    DatasetKind dataset,
    PipelineBmReadMode mode,
    uint64_t bytes) {
  PipelineBmMetrics metrics;
  BenchmarkOptions printedOpts;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    printedOpts = opts;
    BenchmarkContext context("pipeline-bm", opts.dataBytes);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    auto container = makeBmRowContainer(dataset, context.bufferManager);
    const auto statsBefore = context.bufferManager->stats();
    suspender.dismiss();

    const auto storeStart = benchmarkNowNs();
    storeReusableInputBatchesBm(*container, input, opts);
    metrics.storeNs += benchmarkNowNs() - storeStart;
    metrics.rows += rowCount(opts);

    const auto spillWriteStart = benchmarkNowNs();
    const auto segment = container->spillActiveSegment();
    metrics.spillWriteNs += benchmarkNowNs() - spillWriteStart;

    std::vector<char*> rows;
    std::vector<RowId> rowIds;
    rows.reserve(rowCount(opts));
    rowIds.reserve(rowCount(opts));

    if (mode == PipelineBmReadMode::kLoadedPointers) {
      const auto spillReadStart = benchmarkNowNs();
      auto bulk = container->beginBulkReadSegments({&segment, 1});
      rows = bulk.loadRows(
          FLAGS_bm_row_container_spill_metrics ? &metrics.bulkLoad : nullptr);
      metrics.spillReadNs += benchmarkNowNs() - spillReadStart;
      metrics.resultPointers = true;
      BOLT_CHECK(rowIds.empty());
      const auto extractStart = benchmarkNowNs();
      extractBmRowsResident(*container, rows, opts, context.pool.get());
      metrics.extractNs += benchmarkNowNs() - extractStart;
      folly::doNotOptimizeAway(rows.data());
    } else {
      const auto spillReadStart = benchmarkNowNs();
      auto session = container->beginReadOnlyWindowReadSegments({&segment, 1});
      rowIds = session.listRowIds();
      metrics.spillReadNs += benchmarkNowNs() - spillReadStart;
      metrics.resultPointers = false;
      BOLT_CHECK(rows.empty());
      metrics.rowIds += rowIds.size();
      extractBmWindowRows(
          *container, session, rowIds, opts, context.pool.get(), metrics);
      folly::doNotOptimizeAway(rowIds.data());
    }

    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
    if (FLAGS_bm_row_container_spill_metrics) {
      const auto statsAfter = context.bufferManager->stats();
      accumulateBmStatsDelta(metrics.statsDelta, statsBefore, statsAfter);
    }
  }
  printBmMetrics(dataset, iterations, mode, printedOpts, metrics);
}

BENCHMARK_NAMED_PARAM(pipelineOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_loaded_fixed,
    DatasetKind::kFixed,
    PipelineBmReadMode::kLoadedPointers,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineBm,
    bm_window_fixed,
    DatasetKind::kFixed,
    PipelineBmReadMode::kWindowRead,
    0);
BENCHMARK_NAMED_PARAM(pipelineOld, old_variable, DatasetKind::kVariable, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm,
    bm_loaded_variable,
    DatasetKind::kVariable,
    PipelineBmReadMode::kLoadedPointers,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineBm,
    bm_window_variable,
    DatasetKind::kVariable,
    PipelineBmReadMode::kWindowRead,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
