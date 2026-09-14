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

#include <folly/String.h>
#include <optional>

#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltRowBasedSortShuffleWriter.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriterV2.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
using namespace bytedance::bolt::shuffle::sparksql;
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::memory::sparksql;

namespace {
// Times a shuffle write section and keeps it marked active while timed, so a
// reclaim nested in the section is not timed twice (see reclaim()).
class ShuffleWriteSection {
 public:
  ShuffleWriteSection(uint64_t* writeTime, std::atomic<bool>* inSection)
      : inSection_(inSection), timer_(writeTime) {
    inSection_->store(true);
  }

  ~ShuffleWriteSection() {
    // Stop timing before the section stops being active.
    timer_.reset();
    inSection_->store(false);
  }

 private:
  std::atomic<bool>* inSection_;
  std::optional<bytedance::bolt::NanosecondTimer> timer_;
};
} // namespace

SparkShuffleWriter::SparkShuffleWriter(
    int32_t operatorId,
    bytedance::bolt::exec::DriverCtx* driverCtx,
    std::shared_ptr<const SparkShuffleWriterNode> shuffleWriterNode)
    : bytedance::bolt::exec::Operator(
          driverCtx,
          shuffleWriterNode->outputType(),
          operatorId,
          shuffleWriterNode->id(),
          std::string(shuffleWriterNode->name())),
      shuffleWriterOptions_(shuffleWriterNode->getShuffleWriterOptions()),
      // shuffle writer memory limit should at least hold one max shuffle batch
      minMemLimit_(shuffleWriterOptions_.shuffleBatchSize),
      reportShuffleStatusCallback_(
          shuffleWriterNode->getReportShuffleStatusCallback()) {
  VLOG(1) << "Spark shuffle writer options: "
          << shuffleWriterOptions_.toString();
}

void SparkShuffleWriter::init(const bytedance::bolt::RowVectorPtr& rv) {
  arrowPool_ = std::make_unique<BoltArrowMemoryPool>(pool());
  auto freeMem = ExecutionMemoryPool::getMinimumFreeMemoryForTask(
      shuffleWriterOptions_.taskAttemptId);
  BOLT_CHECK(
      freeMem.has_value(),
      "Expect ExecutionMemoryPool::getMinimumFreeMemoryForTask return value");
  shuffleWriter_ = BoltShuffleWriter::create(
      shuffleWriterOptions_,
      rv->childrenSize() - 1,
      rv->size(),
      rv->estimateFlatSize(),
      freeMem.value() + pool()->freeBytes(),
      pool(),
      arrowPool_.get());
}

void SparkShuffleWriter::addInput(RowVectorPtr input) {
  ShuffleWriteSection section(&shuffleWriteTime_, &inShuffleSection_);
  Operator::ReclaimableSectionGuard guard(this);
  std::call_once(initOnceFlag_, [this, &input]() { this->init(input); });
  auto freeMem = ExecutionMemoryPool::getMinimumFreeMemoryForTask(
      shuffleWriterOptions_.taskAttemptId);
  BOLT_CHECK(
      freeMem.has_value(),
      "Expect ExecutionMemoryPool::getMinimumFreeMemoryForTask return value");
  auto memLimit = freeMem.value() + pool()->freeBytes();
  if (pool()->reservedBytes() < minMemLimit_) {
    // minMemLimit_ ensures that ShuffleWriter retains a minimum amount of
    // memory. If ShuffleWriter has already consumed some memory, that usage is
    // deducted from minMemLimit_ to determine the final effective threshold.
    memLimit = std::max(memLimit, minMemLimit_ - pool()->reservedBytes());
  }
  VLOG(1) << "ShuffleWriterNode::addInput: memLimit = " << memLimit
          << ", pool used: " << pool()->usedBytes()
          << ", pool free: " << pool()->freeBytes()
          << ", pool reserved: " << pool()->reservedBytes()
          << ", total free: " << freeMem.value();
  auto status = shuffleWriter_->split(input, memLimit);
  BOLT_CHECK(
      status.ok(),
      "Native split: shuffle writer split failed: {}",
      status.ToString());
}

void SparkShuffleWriter::noMoreInput() {
  Operator::noMoreInput();
  ShuffleWriterMetrics metrics;
  if (shuffleWriter_) {
    arrow::Status status;
    {
      ShuffleWriteSection section(&shuffleWriteTime_, &inShuffleSection_);
      status = shuffleWriter_->stop();
    }
    BOLT_CHECK(
        status.ok(),
        "Native shuffle write: ShuffleWriter stop failed, status: {}, shuffleWriter: {}",
        status.ToString(),
        shuffleWriter_->toString());
    metrics = shuffleWriter_->metrics();
  } else {
    metrics.partitionLengths = std::vector<int64_t>(
        shuffleWriterOptions_.partitionWriterOptions.numPartitions, 0);
    metrics.rawPartitionLengths = std::vector<int64_t>(
        shuffleWriterOptions_.partitionWriterOptions.numPartitions, 0);

    LOG(INFO) << "ShuffleWriter is null";
  }

  metrics.shuffleWriteTime = shuffleWriteTime_;
  metrics.externalReclaimTime = externalReclaimTime_;

  VLOG(1) << "Shuffle writer metrics: " << metrics.toString();
  VLOG(1) << "Shuffle writer partition lengths: "
          << folly::join(",", metrics.partitionLengths)
          << ", raw partition lengths: "
          << folly::join(",", metrics.rawPartitionLengths);

  reportShuffleStatusCallback_(metrics);
}

RowVectorPtr SparkShuffleWriter::getOutput() {
  if (noMoreInput_) {
    finished_ = true;
  }
  return nullptr;
}

void SparkShuffleWriter::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  int64_t evictedSize;
  if (shuffleWriter_) {
    const auto doReclaim = [&]() {
      return shuffleWriter_->reclaimFixedSize(targetBytes, &evictedSize);
    };
    if (inShuffleSection_.load()) {
      // Nested in a timed section: already covered by shuffleWriteTime_.
      BOLT_CHECK(doReclaim().ok(), "(shuffle) nativeEvict: evict failed");
    } else {
      // External reclaim: not covered by shuffleWriteTime_.
      bytedance::bolt::NanosecondTimer timer(&externalReclaimTime_);
      BOLT_CHECK(doReclaim().ok(), "(shuffle) nativeEvict: evict failed");
    }
  } else {
    LOG(INFO) << "ShuffleWriter is null when reclaim";
  }
}

void SparkShuffleWriter::close() {
  Operator::close();
}
