#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>

#include <exception>
#include <thread>

DECLARE_uint64(bm_row_container_data_bytes);
DECLARE_uint64(bm_row_container_warmup_data_bytes);
DECLARE_bool(bm_row_container_spill_metrics);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

constexpr uint32_t kPipelineThreads = 4;

struct PipelineOldMetrics {
  uint64_t storeNs{0};
  uint64_t spillWriteNs{0};
  uint64_t spillReadNs{0};
  uint64_t readNs{0};
  uint64_t wallNs{0};
  uint64_t rows{0};
  OldSpillWriteMetrics spillWrite;
  OldSpillReadMetrics spillRead;
};

struct PipelineBmMetrics {
  uint64_t storeNs{0};
  uint64_t spillWriteNs{0};
  uint64_t spillReadNs{0};
  uint64_t readNs{0};
  uint64_t wallNs{0};
  uint64_t rows{0};
  memory::bm::BufferManagerStats statsDelta;
  memory::bm::DiskIoSchedulerStats processIoStatsDelta;
};

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

bool shouldWarmup() {
  return FLAGS_bm_row_container_warmup_data_bytes != 0;
}

BenchmarkOptions makeOptions(
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  return options(dataset, dataBytes(bytes), compression);
}

BenchmarkOptions makeWarmupOptions(const BenchmarkOptions& opts) {
  return options(
      opts.dataset,
      FLAGS_bm_row_container_warmup_data_bytes,
      opts.compression);
}

double usToMs(uint64_t us) {
  return static_cast<double>(us) / 1000.0;
}

double avgUs(uint64_t totalUs, uint64_t samples) {
  return samples == 0 ? 0.0 : static_cast<double>(totalUs) / samples;
}

void accumulateBmStatsDelta(
    memory::bm::BufferManagerStats& total,
    const memory::bm::BufferManagerStats& before,
    const memory::bm::BufferManagerStats& after) {
  total.spillWriteCount +=
      counterDelta(before.spillWriteCount, after.spillWriteCount);
  total.spillWriteBytes +=
      counterDelta(before.spillWriteBytes, after.spillWriteBytes);
  total.spillPhysicalWriteBytes += counterDelta(
      before.spillPhysicalWriteBytes, after.spillPhysicalWriteBytes);
  total.spillReadCount +=
      counterDelta(before.spillReadCount, after.spillReadCount);
  total.spillReadBytes +=
      counterDelta(before.spillReadBytes, after.spillReadBytes);
  total.spillPhysicalReadBytes += counterDelta(
      before.spillPhysicalReadBytes, after.spillPhysicalReadBytes);
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

void addOldMetrics(PipelineOldMetrics& total, const PipelineOldMetrics& value) {
  total.storeNs += value.storeNs;
  total.spillWriteNs += value.spillWriteNs;
  total.spillReadNs += value.spillReadNs;
  total.readNs += value.readNs;
  total.rows += value.rows;
  total.spillWrite.spillBytes += value.spillWrite.spillBytes;
  total.spillWrite.files += value.spillWrite.files;
  total.spillRead.batches += value.spillRead.batches;
}

void addBmMetrics(PipelineBmMetrics& total, const PipelineBmMetrics& value) {
  total.storeNs += value.storeNs;
  total.spillWriteNs += value.spillWriteNs;
  total.spillReadNs += value.spillReadNs;
  total.readNs += value.readNs;
  total.rows += value.rows;
  total.statsDelta.spillWriteCount += value.statsDelta.spillWriteCount;
  total.statsDelta.spillWriteBytes += value.statsDelta.spillWriteBytes;
  total.statsDelta.spillPhysicalWriteBytes +=
      value.statsDelta.spillPhysicalWriteBytes;
  total.statsDelta.spillReadCount += value.statsDelta.spillReadCount;
  total.statsDelta.spillReadBytes += value.statsDelta.spillReadBytes;
  total.statsDelta.spillPhysicalReadBytes +=
      value.statsDelta.spillPhysicalReadBytes;
}

void runOldPipelineThreadsOnce(
    const BenchmarkOptions& opts,
    PipelineOldMetrics* metrics) {
  checkOldRowBasedSpillBenchmarkSupported(opts);

  std::vector<PipelineOldMetrics> threadMetrics(kPipelineThreads);
  std::vector<std::exception_ptr> exceptions(kPipelineThreads);
  std::vector<std::thread> threads;
  threads.reserve(kPipelineThreads);
  const auto wallStart = metrics == nullptr ? 0 : benchmarkNowNs();
  for (uint32_t i = 0; i < kPipelineThreads; ++i) {
    threads.emplace_back([&, i] {
      try {
        BenchmarkContext context(
            "pipeline-old-threads", opts.dataBytes, 8, opts.compression);
        auto input = makeReusableInputBatches(context.pool.get(), opts);
        auto container = makeOldRowContainer(opts.dataset, context.pool.get());

        auto* local = metrics == nullptr ? nullptr : &threadMetrics[i];
        const auto storeStart = local == nullptr ? 0 : benchmarkNowNs();
        storeReusableInputBatchesOldBatch(*container, input, opts);
        if (local != nullptr) {
          local->storeNs += benchmarkNowNs() - storeStart;
          local->rows += rowCount(opts);
        }

        const auto spillWriteStart = local == nullptr ? 0 : benchmarkNowNs();
        auto spill = spillOldRows(
            context,
            *container,
            opts.dataset,
            local != nullptr && FLAGS_bm_row_container_spill_metrics
                ? &local->spillWrite
                : nullptr);
        if (local != nullptr) {
          local->spillWriteNs += benchmarkNowNs() - spillWriteStart;
        }
        container.reset();

        std::vector<char*> restoredRows;
        restoredRows.reserve(rowCount(opts));
        const auto spillReadStart = local == nullptr ? 0 : benchmarkNowNs();
        auto restored = readOldSpillIntoNewRowContainer(
            context,
            spill,
            opts.dataset,
            local != nullptr && FLAGS_bm_row_container_spill_metrics
                ? &local->spillRead
                : nullptr,
            &restoredRows);
        if (local != nullptr) {
          local->spillReadNs += benchmarkNowNs() - spillReadStart;
        }
        BOLT_CHECK_EQ(rowCount(opts), restoredRows.size());

        const auto readStart = local == nullptr ? 0 : benchmarkNowNs();
        extractOldRows(*restored, restoredRows, opts, context.pool.get());
        if (local != nullptr) {
          local->readNs += benchmarkNowNs() - readStart;
        }
        folly::doNotOptimizeAway(restored->numRows());
      } catch (...) {
        exceptions[i] = std::current_exception();
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& exception : exceptions) {
    if (exception != nullptr) {
      std::rethrow_exception(exception);
    }
  }

  if (metrics == nullptr) {
    return;
  }
  metrics->wallNs += benchmarkNowNs() - wallStart;
  for (const auto& local : threadMetrics) {
    addOldMetrics(*metrics, local);
  }
}

void runBmPipelineThreadsOnce(
    const BenchmarkOptions& opts,
    PipelineBmMetrics* metrics) {
  std::vector<PipelineBmMetrics> threadMetrics(kPipelineThreads);
  std::vector<std::exception_ptr> exceptions(kPipelineThreads);
  std::vector<std::thread> threads;
  threads.reserve(kPipelineThreads);
  const auto ioStatsBefore =
      metrics != nullptr && FLAGS_bm_row_container_spill_metrics
      ? memory::bm::diskIoScheduler().stats()
      : memory::bm::DiskIoSchedulerStats{};
  const auto wallStart = metrics == nullptr ? 0 : benchmarkNowNs();
  for (uint32_t i = 0; i < kPipelineThreads; ++i) {
    threads.emplace_back([&, i] {
      try {
        BenchmarkContext context(
            "pipeline-bm-threads", opts.dataBytes, 0, opts.compression);
        auto input = makeReusableInputBatches(context.pool.get(), opts);
        auto container = makeBmRowContainer(opts.dataset, context.bufferManager);
        memory::bm::BufferManagerStats statsBefore;
        if (metrics != nullptr && FLAGS_bm_row_container_spill_metrics) {
          statsBefore = context.bufferManager->stats();
        }

        auto* local = metrics == nullptr ? nullptr : &threadMetrics[i];
        const auto storeStart = local == nullptr ? 0 : benchmarkNowNs();
        storeReusableInputBatchesBmBatch(*container, input, opts);
        if (local != nullptr) {
          local->storeNs += benchmarkNowNs() - storeStart;
          local->rows += rowCount(opts);
        }

        const auto spillWriteStart = local == nullptr ? 0 : benchmarkNowNs();
        const auto segment = container->spillActiveSegment();
        if (local != nullptr) {
          local->spillWriteNs += benchmarkNowNs() - spillWriteStart;
        }

        const auto spillReadStart = local == nullptr ? 0 : benchmarkNowNs();
        auto bulk = container->beginBulkReadSegments({&segment, 1});
        auto rows = bulk.loadRows();
        if (local != nullptr) {
          local->spillReadNs += benchmarkNowNs() - spillReadStart;
        }
        BOLT_CHECK_EQ(rowCount(opts), rows.size());

        const auto readStart = local == nullptr ? 0 : benchmarkNowNs();
        extractBmRowsResident(*container, rows, opts, context.pool.get());
        if (local != nullptr) {
          local->readNs += benchmarkNowNs() - readStart;
        }

        folly::doNotOptimizeAway(container->numRows());
        folly::doNotOptimizeAway(rows.data());
        if (local != nullptr && FLAGS_bm_row_container_spill_metrics) {
          const auto statsAfter = context.bufferManager->stats();
          accumulateBmStatsDelta(local->statsDelta, statsBefore, statsAfter);
        }
      } catch (...) {
        exceptions[i] = std::current_exception();
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& exception : exceptions) {
    if (exception != nullptr) {
      std::rethrow_exception(exception);
    }
  }

  if (metrics == nullptr) {
    return;
  }
  metrics->wallNs += benchmarkNowNs() - wallStart;
  for (const auto& local : threadMetrics) {
    addBmMetrics(*metrics, local);
  }
  if (FLAGS_bm_row_container_spill_metrics) {
    const auto ioStatsAfter = memory::bm::diskIoScheduler().stats();
    accumulateDiskIoStatsDelta(
        metrics->processIoStatsDelta, ioStatsBefore, ioStatsAfter);
  }
}

void printOldMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const PipelineOldMetrics& metrics) {
  if (!shouldPrintSpillMetrics(
          "pipelineOld4Threads", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  const auto totalNs =
      metrics.storeNs + metrics.spillWriteNs + metrics.spillReadNs +
      metrics.readNs;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] pipelineOld4Threads dataset={} "
      "compression={} iterations={} threads={} per_thread_logical_bytes={} "
      "total_logical_bytes={} rows_per_thread={} total_rows={} "
      "sum_store_ms={:.3f} sum_spill_write_ms={:.3f} "
      "sum_spill_read_ms={:.3f} sum_read_ms={:.3f} sum_total_ms={:.3f} "
      "wall_ms={:.3f} parallel_efficiency={:.3f} spill_bytes={} files={} "
      "batches={}\n",
      datasetName(dataset),
      spillCompressionName(opts.compression),
      iterations,
      kPipelineThreads,
      opts.dataBytes,
      opts.dataBytes * kPipelineThreads,
      rowCount(opts),
      metrics.rows,
      nsToMs(metrics.storeNs),
      nsToMs(metrics.spillWriteNs),
      nsToMs(metrics.spillReadNs),
      nsToMs(metrics.readNs),
      nsToMs(totalNs),
      nsToMs(metrics.wallNs),
      metrics.wallNs == 0 ? 0.0
                          : static_cast<double>(totalNs) / metrics.wallNs,
      metrics.spillWrite.spillBytes,
      metrics.spillWrite.files,
      metrics.spillRead.batches);
}

void printBmMetrics(
    DatasetKind dataset,
    uint32_t iterations,
    const BenchmarkOptions& opts,
    const PipelineBmMetrics& metrics) {
  if (!shouldPrintSpillMetrics(
          "pipelineBm4Threads", dataset, opts.compression)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  const auto totalNs =
      metrics.storeNs + metrics.spillWriteNs + metrics.spillReadNs +
      metrics.readNs;
  const auto& stats = metrics.statsDelta;
  const auto& ioStats = metrics.processIoStatsDelta;
  fmt::print(
      stderr,
      "[bm-row-container-metrics] pipelineBm4Threads dataset={} "
      "compression={} iterations={} threads={} per_thread_logical_bytes={} "
      "total_logical_bytes={} rows_per_thread={} total_rows={} "
      "sum_store_ms={:.3f} sum_spill_write_ms={:.3f} "
      "sum_spill_read_ms={:.3f} sum_read_ms={:.3f} sum_total_ms={:.3f} "
      "wall_ms={:.3f} parallel_efficiency={:.3f} "
      "spill_write_count={} spill_write_bytes={} "
      "spill_physical_write_bytes={} spill_read_count={} spill_read_bytes={} "
      "spill_physical_read_bytes={} process_io_accepted={} "
      "process_io_completed={} process_io_completed_bytes={} "
      "process_io_successful={} process_io_failed={} process_io_rejected={} "
      "process_io_submit_batches={} process_io_completion_batches={} "
      "process_io_queue_wait_ms={:.3f} "
      "process_io_avg_queue_wait_us={:.3f} "
      "process_io_device_latency_ms={:.3f} "
      "process_io_avg_device_latency_us={:.3f} "
      "process_io_end_to_end_latency_ms={:.3f} "
      "process_io_avg_end_to_end_latency_us={:.3f} "
      "process_io_backend_submit_ms={:.3f} "
      "process_io_backend_reap_ms={:.3f} "
      "process_io_worker_wait_ms={:.3f} "
      "process_io_future_fulfill_ms={:.3f}\n",
      datasetName(dataset),
      spillCompressionName(opts.compression),
      iterations,
      kPipelineThreads,
      opts.dataBytes,
      opts.dataBytes * kPipelineThreads,
      rowCount(opts),
      metrics.rows,
      nsToMs(metrics.storeNs),
      nsToMs(metrics.spillWriteNs),
      nsToMs(metrics.spillReadNs),
      nsToMs(metrics.readNs),
      nsToMs(totalNs),
      nsToMs(metrics.wallNs),
      metrics.wallNs == 0 ? 0.0
                          : static_cast<double>(totalNs) / metrics.wallNs,
      stats.spillWriteCount,
      stats.spillWriteBytes,
      stats.spillPhysicalWriteBytes,
      stats.spillReadCount,
      stats.spillReadBytes,
      stats.spillPhysicalReadBytes,
      ioStats.acceptedRequests,
      ioStats.completedRequests,
      ioStats.completedBytes,
      ioStats.successfulRequests,
      ioStats.failedRequests,
      ioStats.rejectedRequests,
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

void pipelineOld4Threads(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  PipelineOldMetrics metrics;
  const auto printedOpts = makeOptions(dataset, compression, bytes);
  if (shouldWarmup()) {
    folly::BenchmarkSuspender suspender;
    runOldPipelineThreadsOnce(makeWarmupOptions(printedOpts), nullptr);
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    runOldPipelineThreadsOnce(printedOpts, &metrics);
  }
  printOldMetrics(dataset, iterations, printedOpts, metrics);
}

void pipelineBm4Threads(
    uint32_t iterations,
    DatasetKind dataset,
    SpillCompressionKind compression,
    uint64_t bytes) {
  PipelineBmMetrics metrics;
  const auto printedOpts = makeOptions(dataset, compression, bytes);
  if (shouldWarmup()) {
    folly::BenchmarkSuspender suspender;
    runBmPipelineThreadsOnce(makeWarmupOptions(printedOpts), nullptr);
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    runBmPipelineThreadsOnce(printedOpts, &metrics);
  }
  printBmMetrics(dataset, iterations, printedOpts, metrics);
}

BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_raw_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_lz4_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_zstd_fixed,
    DatasetKind::kFixed,
    SpillCompressionKind::kZstd,
    0);

BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_raw_variable_small,
    DatasetKind::kVariableSmall,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_raw_variable_small,
    DatasetKind::kVariableSmall,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_lz4_variable_small,
    DatasetKind::kVariableSmall,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_lz4_variable_small,
    DatasetKind::kVariableSmall,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_zstd_variable_small,
    DatasetKind::kVariableSmall,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_zstd_variable_small,
    DatasetKind::kVariableSmall,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_raw_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_raw_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kRaw,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_lz4_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_lz4_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kLz4,
    0);
BENCHMARK_NAMED_PARAM(
    pipelineOld4Threads,
    old4_zstd_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    pipelineBm4Threads,
    bm4_zstd_variable_large,
    DatasetKind::kVariableLarge,
    SpillCompressionKind::kZstd,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
