#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

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

double usToMs(uint64_t us) {
  return static_cast<double>(us) / 1000.0;
}

double avgUs(uint64_t totalUs, uint64_t samples) {
  return samples == 0 ? 0.0 : static_cast<double>(totalUs) / samples;
}

uint64_t priorityValue(
    const std::array<uint64_t, memory::bm::kIoPriorityCount>& values,
    memory::bm::IoPriority priority) {
  return values[memory::bm::priorityIndex(priority)];
}

void accumulateArrayDelta(
    std::array<uint64_t, memory::bm::kIoPriorityCount>& total,
    const std::array<uint64_t, memory::bm::kIoPriorityCount>& before,
    const std::array<uint64_t, memory::bm::kIoPriorityCount>& after) {
  for (size_t i = 0; i < memory::bm::kIoPriorityCount; ++i) {
    total[i] += counterDelta(before[i], after[i]);
  }
}

void accumulateDiskIoStatsDelta(
    memory::bm::DiskIoSchedulerStats& total,
    const memory::bm::DiskIoSchedulerStats& before,
    const memory::bm::DiskIoSchedulerStats& after) {
  accumulateArrayDelta(
      total.queuedRequests, before.queuedRequests, after.queuedRequests);
  accumulateArrayDelta(
      total.submittedRequests,
      before.submittedRequests,
      after.submittedRequests);
  accumulateArrayDelta(
      total.completedRequestsByPriority,
      before.completedRequestsByPriority,
      after.completedRequestsByPriority);
  accumulateArrayDelta(
      total.completedBytesByPriority,
      before.completedBytesByPriority,
      after.completedBytesByPriority);
  accumulateArrayDelta(
      total.successfulRequestsByPriority,
      before.successfulRequestsByPriority,
      after.successfulRequestsByPriority);
  accumulateArrayDelta(
      total.failedRequestsByPriority,
      before.failedRequestsByPriority,
      after.failedRequestsByPriority);

  total.acceptedRequests +=
      counterDelta(before.acceptedRequests, after.acceptedRequests);
  total.rejectedRequests +=
      counterDelta(before.rejectedRequests, after.rejectedRequests);
  total.shutdownRejectedRequests += counterDelta(
      before.shutdownRejectedRequests, after.shutdownRejectedRequests);
  total.completedRequests +=
      counterDelta(before.completedRequests, after.completedRequests);
  total.completedBytes +=
      counterDelta(before.completedBytes, after.completedBytes);
  total.successfulRequests +=
      counterDelta(before.successfulRequests, after.successfulRequests);
  total.failedRequests +=
      counterDelta(before.failedRequests, after.failedRequests);
  total.backendSubmitFailedRequests += counterDelta(
      before.backendSubmitFailedRequests,
      after.backendSubmitFailedRequests);
  total.backendIoErrorRequests += counterDelta(
      before.backendIoErrorRequests, after.backendIoErrorRequests);
  total.cumulativeDeviceLatencyUs += counterDelta(
      before.cumulativeDeviceLatencyUs, after.cumulativeDeviceLatencyUs);
  total.latencySamples +=
      counterDelta(before.latencySamples, after.latencySamples);
  total.cumulativeQueueWaitUs +=
      counterDelta(before.cumulativeQueueWaitUs, after.cumulativeQueueWaitUs);
  total.queueWaitSamples +=
      counterDelta(before.queueWaitSamples, after.queueWaitSamples);
  total.cumulativeEndToEndLatencyUs += counterDelta(
      before.cumulativeEndToEndLatencyUs,
      after.cumulativeEndToEndLatencyUs);
  total.submitBatches +=
      counterDelta(before.submitBatches, after.submitBatches);
  total.submittedRequestsInBatches += counterDelta(
      before.submittedRequestsInBatches,
      after.submittedRequestsInBatches);
  total.completionBatches +=
      counterDelta(before.completionBatches, after.completionBatches);
  total.completedRequestsInBatches += counterDelta(
      before.completedRequestsInBatches,
      after.completedRequestsInBatches);
  total.backendReapCalls +=
      counterDelta(before.backendReapCalls, after.backendReapCalls);
  total.cumulativeBackendReapUs += counterDelta(
      before.cumulativeBackendReapUs, after.cumulativeBackendReapUs);
  total.backendSubmitCalls +=
      counterDelta(before.backendSubmitCalls, after.backendSubmitCalls);
  total.cumulativeBackendSubmitUs += counterDelta(
      before.cumulativeBackendSubmitUs, after.cumulativeBackendSubmitUs);
  total.workerWaitCalls +=
      counterDelta(before.workerWaitCalls, after.workerWaitCalls);
  total.cumulativeWorkerWaitUs += counterDelta(
      before.cumulativeWorkerWaitUs, after.cumulativeWorkerWaitUs);
  total.futureFulfillBatches +=
      counterDelta(before.futureFulfillBatches, after.futureFulfillBatches);
  total.fulfilledResults +=
      counterDelta(before.fulfilledResults, after.fulfilledResults);
  total.cumulativeFutureFulfillUs += counterDelta(
      before.cumulativeFutureFulfillUs, after.cumulativeFutureFulfillUs);
}

void printOldReadMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const OldSpillReadMetrics& metrics) {
  if (!shouldPrintSpillMetrics("spillReadOld", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillReadOld compression={} dataset={} "
      "iterations={} logical_bytes={} rows={} serialized_bytes={} batches={} "
      "create_reader_ms={:.3f} next_batch_ms={:.3f} "
      "copy_rows_ms={:.3f} list_rows_ms={:.3f}\n",
      spillCompressionName(opts.compression),
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      metrics.rows,
      metrics.serializedBytes,
      metrics.batches,
      nsToMs(metrics.createReaderNs),
      nsToMs(metrics.nextBatchNs),
      nsToMs(metrics.copyRowsNs),
      nsToMs(metrics.listRowsNs));
}

void printBmReadMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const BmSpillReadMetrics& metrics) {
  if (!shouldPrintSpillMetrics("spillReadBm", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  const auto& stats = metrics.statsDelta;
  const auto& ioStats = metrics.ioStatsDelta;
  const auto& bulk = metrics.bulkLoad;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] spillReadBm compression={} dataset={} "
      "iterations={} logical_bytes={} rows={} row_ids={} windows={} "
      "result={} begin_ms={:.3f} list_rows_ms={:.3f} "
      "window_load_ms={:.3f} "
      "bulk_estimate_ms={:.3f} bulk_reserve_ms={:.3f} "
      "bulk_collect_blocks_ms={:.3f} bulk_batch_pin_ms={:.3f} "
      "bulk_update_ptrs_ms={:.3f} bulk_rebase_strings_ms={:.3f} "
      "bulk_append_ptrs_ms={:.3f} bulk_append_row_ids_ms={:.3f} "
      "bulk_estimated_bytes={} bulk_pinned_blocks={} "
      "bulk_pointer_rows={} bulk_row_id_rows={} "
      "bulk_rebased_string_views={} "
      "bm_batch_pins={} bm_pin_reads={} bm_spill_read_count={} "
      "bm_spill_read_bytes={} bm_spill_physical_read_bytes={} "
      "bm_decompress_ms={:.3f} "
      "io_accepted={} io_completed={} io_completed_bytes={} "
      "io_successful={} io_failed={} io_rejected={} "
      "io_submitted_high={} io_submitted_medium={} io_submitted_low={} "
      "io_completed_high={} io_completed_medium={} io_completed_low={} "
      "io_submit_batches={} io_completion_batches={} "
      "io_queue_wait_ms={:.3f} io_avg_queue_wait_us={:.3f} "
      "io_device_latency_ms={:.3f} io_avg_device_latency_us={:.3f} "
      "io_end_to_end_latency_ms={:.3f} io_avg_end_to_end_latency_us={:.3f} "
      "io_backend_submit_ms={:.3f} io_backend_reap_ms={:.3f} "
      "io_worker_wait_ms={:.3f} io_future_fulfill_ms={:.3f}\n",
      spillCompressionName(opts.compression),
      datasetName(dataset),
      iterations,
      opts.dataBytes,
      metrics.rows,
      metrics.rowIds,
      metrics.windows,
      metrics.resultPointers ? "pointers" : "row_ids",
      nsToMs(metrics.beginNs),
      nsToMs(metrics.listRowsNs),
      nsToMs(metrics.windowLoadNs),
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
      stats.spillReadCount,
      stats.spillReadBytes,
      stats.spillPhysicalReadBytes,
      usToMs(stats.spillDecompressionTimeUs),
      ioStats.acceptedRequests,
      ioStats.completedRequests,
      ioStats.completedBytes,
      ioStats.successfulRequests,
      ioStats.failedRequests,
      ioStats.rejectedRequests,
      priorityValue(ioStats.submittedRequests, memory::bm::IoPriority::High),
      priorityValue(ioStats.submittedRequests, memory::bm::IoPriority::Medium),
      priorityValue(ioStats.submittedRequests, memory::bm::IoPriority::Low),
      priorityValue(
          ioStats.completedRequestsByPriority, memory::bm::IoPriority::High),
      priorityValue(
          ioStats.completedRequestsByPriority,
          memory::bm::IoPriority::Medium),
      priorityValue(
          ioStats.completedRequestsByPriority, memory::bm::IoPriority::Low),
      ioStats.submitBatches,
      ioStats.completionBatches,
      usToMs(ioStats.cumulativeQueueWaitUs),
      avgUs(ioStats.cumulativeQueueWaitUs, ioStats.queueWaitSamples),
      usToMs(ioStats.cumulativeDeviceLatencyUs),
      avgUs(ioStats.cumulativeDeviceLatencyUs, ioStats.latencySamples),
      usToMs(ioStats.cumulativeEndToEndLatencyUs),
      avgUs(ioStats.cumulativeEndToEndLatencyUs, ioStats.completedRequests),
      usToMs(ioStats.cumulativeBackendSubmitUs),
      usToMs(ioStats.cumulativeBackendReapUs),
      usToMs(ioStats.cumulativeWorkerWaitUs),
      usToMs(ioStats.cumulativeFutureFulfillUs));
}

void spillReadOld(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupSpillReadOld(options(dataset, dataBytes(bytes), compression));
    suspender.dismiss();
  }
  OldSpillReadMetrics metrics;
  BenchmarkOptions printedOpts;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes), compression);
    checkOldRowBasedSpillBenchmarkSupported(opts);
    printedOpts = opts;
    BenchmarkContext context(
        "spill-read-old", opts.dataBytes, 8, opts.compression);
    auto stored = storeOldRows(context, opts, false);
    auto spill = spillOldRows(context, *stored.container, dataset);
    stored.container.reset();
    std::vector<char*> restoredRows;
    restoredRows.reserve(rowCount(opts));
    suspender.dismiss();
    auto restored = readOldSpillIntoNewRowContainer(
        context,
        spill,
        dataset,
        FLAGS_bm_row_container_spill_metrics ? &metrics : nullptr,
        &restoredRows);
    folly::doNotOptimizeAway(restored->numRows());
    folly::doNotOptimizeAway(restoredRows.data());
    folly::doNotOptimizeAway(restoredRows.size());
    suspender.rehire();
  }
  printOldReadMetrics(dataset, iterations, printedOpts, metrics);
}

void spillReadBm(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupSpillReadBm(options(dataset, dataBytes(bytes), compression));
    suspender.dismiss();
  }
  BmSpillReadMetrics metrics;
  BenchmarkOptions printedOpts;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes), compression);
    printedOpts = opts;
    BenchmarkContext context(
        "spill-read-bm", opts.dataBytes, 0, opts.compression);
    auto spill = spillBmRows(context, opts);
    const auto statsBefore = context.bufferManager->stats();
    // DiskIoScheduler stats are process-wide, so only the read-stage delta is
    // useful for this benchmark line.
    const auto ioStatsBefore = memory::bm::diskIoScheduler().stats();
    suspender.dismiss();
    readBmSpill(
        *spill.container,
        spill.segment,
        opts,
        FLAGS_bm_row_container_spill_metrics ? &metrics : nullptr);
    suspender.rehire();
    if (FLAGS_bm_row_container_spill_metrics) {
      const auto statsAfter = context.bufferManager->stats();
      const auto ioStatsAfter = memory::bm::diskIoScheduler().stats();
      metrics.statsDelta.batchPinCount +=
          counterDelta(statsBefore.batchPinCount, statsAfter.batchPinCount);
      metrics.statsDelta.pinReadCount +=
          counterDelta(statsBefore.pinReadCount, statsAfter.pinReadCount);
      metrics.statsDelta.spillReadCount +=
          counterDelta(statsBefore.spillReadCount, statsAfter.spillReadCount);
      metrics.statsDelta.spillReadBytes +=
          counterDelta(statsBefore.spillReadBytes, statsAfter.spillReadBytes);
      metrics.statsDelta.spillPhysicalReadBytes += counterDelta(
          statsBefore.spillPhysicalReadBytes,
          statsAfter.spillPhysicalReadBytes);
      metrics.statsDelta.spillDecompressionTimeUs += counterDelta(
          statsBefore.spillDecompressionTimeUs,
          statsAfter.spillDecompressionTimeUs);
      accumulateDiskIoStatsDelta(
          metrics.ioStatsDelta, ioStatsBefore, ioStatsAfter);
    }
  }
  printBmReadMetrics(dataset, iterations, printedOpts, metrics);
}

BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_raw_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_lz4_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    spillReadOld,
    old_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    spillReadBm,
    bm_zstd_variable,
    DatasetKind::kVariable,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
