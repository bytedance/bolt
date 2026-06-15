#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DECLARE_uint64(bm_row_container_data_bytes);
DECLARE_bool(bm_row_container_spill_metrics);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

void printOldWriteMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    uint64_t storeNs,
    const OldSpillWriteMetrics& metrics) {
  if (!shouldPrintSpillMetrics(
          "spillWriteOld", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillWriteOld compression={} dataset={} "
      "iterations={} logical_bytes={} rows={} store_setup_ms={:.3f} "
      "spill_ms={:.3f} spill_bytes={} files={}\n",
      spillCompressionName(opts.compression),
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      metrics.rows,
      nsToMs(storeNs),
      nsToMs(metrics.spillNs),
      metrics.spillBytes,
      metrics.files);
}

void printBmWriteMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    uint64_t storeNs,
    uint64_t flushNs,
    const BmAppendMetrics& appendMetrics,
    const BmSegmentSpillMetrics& segmentMetrics,
    const memory::bm::BufferManagerStats& stats) {
  if (!shouldPrintSpillMetrics("spillWriteBm", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillWriteBm compression={} dataset={} "
      "iterations={} logical_bytes={} rows={} store_setup_ms={:.3f} "
      "flush_ms={:.3f} bm_spill_write_count={} bm_spill_write_bytes={} "
      "bm_spill_physical_write_bytes={} bm_compress_ms={:.3f} "
      "bm_compressed_blocks={} append_row_alloc_ms={:.3f} "
      "append_fixed_store_ms={:.3f} append_string_store_ms={:.3f} "
      "append_heap_alloc_ms={:.3f} append_string_copy_ms={:.3f} "
      "append_heap_record_ms={:.3f} append_rows={} append_string_rows={} "
      "append_string_bytes={} append_heap_allocations={} "
      "flush_zero_heap_tail_ms={:.3f} flush_collect_blocks_ms={:.3f} "
      "flush_spill_blocks_ms={:.3f} flush_chunks={} flush_row_blocks={} "
      "flush_heap_blocks={} flush_total_blocks={} flush_row_block_bytes={} "
      "flush_heap_block_bytes={} flush_used_row_bytes={} "
      "flush_used_heap_bytes={} flush_unused_heap_tail_bytes={}\n",
      spillCompressionName(opts.compression),
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      rowCount(opts) * iterations,
      nsToMs(storeNs),
      nsToMs(flushNs),
      stats.spillWriteCount,
      stats.spillWriteBytes,
      stats.spillPhysicalWriteBytes,
      static_cast<double>(stats.spillCompressionTimeUs) / 1000.0,
      stats.spillCompressedBlocks,
      nsToMs(appendMetrics.rowAllocNs),
      nsToMs(appendMetrics.fixedStoreNs),
      nsToMs(appendMetrics.stringStoreNs),
      nsToMs(appendMetrics.heapAllocNs),
      nsToMs(appendMetrics.stringCopyNs),
      nsToMs(appendMetrics.heapRecordNs),
      appendMetrics.rows,
      appendMetrics.stringRows,
      appendMetrics.stringBytes,
      appendMetrics.heapAllocations,
      nsToMs(segmentMetrics.zeroHeapTailNs),
      nsToMs(segmentMetrics.collectBlocksNs),
      nsToMs(segmentMetrics.spillBlocksNs),
      segmentMetrics.chunks,
      segmentMetrics.rowBlocks,
      segmentMetrics.heapBlocks,
      segmentMetrics.totalBlocks,
      segmentMetrics.rowBlockBytes,
      segmentMetrics.heapBlockBytes,
      segmentMetrics.usedRowBytes,
      segmentMetrics.usedHeapBytes,
      segmentMetrics.unusedHeapTailBytes);
}

void spillWriteOld(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  OldSpillWriteMetrics metrics;
  BenchmarkOptions printedOpts;
  uint64_t storeNs = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes), compression);
    checkOldRowBasedSpillBenchmarkSupported(opts);
    printedOpts = opts;
    BenchmarkContext context(
        "spill-write-old", opts.dataBytes, 0, opts.compression);
    const auto storeStart = benchmarkNowNs();
    auto stored = storeOldRows(context, opts, false);
    storeNs += benchmarkNowNs() - storeStart;
    suspender.dismiss();
    auto spill = spillOldRows(
        context,
        *stored.container,
        dataset,
        FLAGS_bm_row_container_spill_metrics ? &metrics : nullptr);
    folly::doNotOptimizeAway(spill.partition.rowCount());
    suspender.rehire();
  }
  printOldWriteMetrics(dataset, iterations, printedOpts, storeNs, metrics);
}

void spillWriteBm(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  BenchmarkOptions printedOpts;
  uint64_t storeNs = 0;
  uint64_t flushNs = 0;
  BmAppendMetrics appendMetrics;
  BmSegmentSpillMetrics segmentMetrics;
  memory::bm::BufferManagerStats stats;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes), compression);
    printedOpts = opts;
    BenchmarkContext context(
        "spill-write-bm", opts.dataBytes, 0, opts.compression);
    const auto storeStart = benchmarkNowNs();
    auto stored = storeBmRows(
        context,
        opts,
        false,
        FLAGS_bm_row_container_spill_metrics ? &appendMetrics : nullptr);
    storeNs += benchmarkNowNs() - storeStart;
    const auto statsBefore = context.bufferManager->stats();
    suspender.dismiss();
    const auto flushStart = benchmarkNowNs();
    const auto segment = stored.container->spillActiveSegment(
        FLAGS_bm_row_container_spill_metrics ? &segmentMetrics : nullptr);
    flushNs += benchmarkNowNs() - flushStart;
    folly::doNotOptimizeAway(segment);
    suspender.rehire();
    if (FLAGS_bm_row_container_spill_metrics) {
      const auto statsAfter = context.bufferManager->stats();
      stats.spillWriteCount +=
          counterDelta(statsBefore.spillWriteCount, statsAfter.spillWriteCount);
      stats.spillWriteBytes +=
          counterDelta(statsBefore.spillWriteBytes, statsAfter.spillWriteBytes);
      stats.spillPhysicalWriteBytes += counterDelta(
          statsBefore.spillPhysicalWriteBytes,
          statsAfter.spillPhysicalWriteBytes);
      stats.spillCompressionTimeUs += counterDelta(
          statsBefore.spillCompressionTimeUs,
          statsAfter.spillCompressionTimeUs);
      stats.spillCompressedBlocks += counterDelta(
          statsBefore.spillCompressedBlocks,
          statsAfter.spillCompressedBlocks);
    }
  }
  printBmWriteMetrics(
      dataset,
      iterations,
      printedOpts,
      storeNs,
      flushNs,
      appendMetrics,
      segmentMetrics,
      stats);
}

BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    spillWriteOld,
    old_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillWriteBm,
    bm_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
