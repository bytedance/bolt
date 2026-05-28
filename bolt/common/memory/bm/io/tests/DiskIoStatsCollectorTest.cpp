#include "bolt/common/memory/bm/io/DiskIoStatsCollector.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(DiskIoStatsCollectorTest, RecordsSubmitBatchAverage) {
  DiskIoSchedulerStats stats;

  DiskIoStatsCollector::recordSubmitBatch(stats, 3);
  DiskIoStatsCollector::recordSubmitBatch(stats, 1);

  EXPECT_EQ(stats.submitBatches, 2);
  EXPECT_EQ(stats.submittedRequestsInBatches, 4);
  EXPECT_EQ(stats.maxSubmitBatchSize, 3);
  EXPECT_DOUBLE_EQ(stats.averageSubmitBatchSize, 2.0);
}

TEST(DiskIoStatsCollectorTest, RecordsCompletionLatencyAndOutcome) {
  DiskIoSchedulerStats stats;
  IoResult result{4096, IoErrorCode::Ok};

  DiskIoStatsCollector::recordCompletion(
      stats, IoPriority::Medium, result, 7, 11, 2);

  EXPECT_EQ(stats.completedRequests, 1);
  EXPECT_EQ(stats.completedBytes, 4096);
  EXPECT_EQ(stats.successfulRequests, 1);
  EXPECT_EQ(stats.latencySamples, 1);
  EXPECT_EQ(stats.cumulativeDeviceLatencyUs, 7);
  EXPECT_EQ(stats.cumulativeEndToEndLatencyUs, 11);
  EXPECT_DOUBLE_EQ(stats.averageDeviceLatencyUs, 7);
  EXPECT_DOUBLE_EQ(stats.averageEndToEndLatencyUs, 11);
  EXPECT_EQ(stats.minLatencyUs, 7);
  EXPECT_EQ(stats.maxLatencyUs, 7);
  EXPECT_EQ(stats.inflightRequests, 2);
}

TEST(DiskIoStatsCollectorTest, RecordsBackendSubmitFailure) {
  DiskIoSchedulerStats stats;

  DiskIoStatsCollector::recordBackendSubmitFailed(stats, IoPriority::Low);

  EXPECT_EQ(stats.completedRequests, 1);
  EXPECT_EQ(stats.failedRequests, 1);
  EXPECT_EQ(stats.backendSubmitFailedRequests, 1);
  EXPECT_EQ(stats.failedRequestsByPriority[priorityIndex(IoPriority::Low)], 1);
}
