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

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

class DiskIoStatsCollector {
 public:
  static void recordQueuedSnapshot(
      DiskIoSchedulerStats& stats,
      const std::array<uint64_t, kIoPriorityCount>& queuedCounts);
  static void recordMaxQueueDepth(
      DiskIoSchedulerStats& stats,
      uint64_t queueDepth);
  static void recordRejected(DiskIoSchedulerStats& stats);
  static void recordShutdownRejected(DiskIoSchedulerStats& stats);
  static void recordAccepted(DiskIoSchedulerStats& stats);
  static void recordSubmitBatch(DiskIoSchedulerStats& stats, size_t batchSize);
  static void recordSubmitted(
      DiskIoSchedulerStats& stats,
      IoPriority priority,
      uint64_t queueWaitUs,
      size_t inflightSize);
  static void recordBackendSubmitFailed(
      DiskIoSchedulerStats& stats,
      IoPriority priority);
  static void recordCompletionBatch(
      DiskIoSchedulerStats& stats,
      size_t batchSize);
  static void recordCompletion(
      DiskIoSchedulerStats& stats,
      IoPriority priority,
      const IoResult& result,
      uint64_t deviceLatencyUs,
      uint64_t endToEndLatencyUs,
      size_t inflightSize);
  static void recordQueuedShutdown(
      DiskIoSchedulerStats& stats,
      IoPriority priority);
  static void recordBackendReap(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs);
  static void recordBackendSubmit(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs);
  static void recordWorkerWait(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs);
  static void recordFutureFulfill(
      DiskIoSchedulerStats& stats,
      uint64_t durationUs,
      size_t batchSize);
};

} // namespace bytedance::bolt::memory::bm
