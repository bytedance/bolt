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

#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include <folly/ScopeGuard.h>
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/exec/Task.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltRowBasedSortShuffleWriter.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriterV2.h"
#include "bolt/shuffle/sparksql/partition_writer/LocalPartitionWriter.h"
using namespace bytedance::bolt::shuffle::sparksql;
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::memory::sparksql;

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
      minMemLimit_(
          shuffleWriterOptions_.partitionWriterOptions.shuffleBufferSize),
      reportShuffleStatusCallback_(
          shuffleWriterNode->getReportShuffleStatusCallback()) {
  shuffleWriterOptions_.partitionWriterOptions.part = driverCtx->driverId;
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
  const bool isLocalPartitionWriter =
      shuffleWriterOptions_.partitionWriterOptions.partitionWriterType ==
      PartitionWriterType::kLocal;

  if (shuffleWriter_ && isLocalPartitionWriter) {
    auto status = shuffleWriter_->stop();
    BOLT_CHECK(status.ok(), "Native shuffle write: ShuffleWriter stop failed");
  }

  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    BOLT_CHECK(future_.valid());
    LOG(INFO) << "SparkShuffleWriter finished but not the last one.";
    return;
  }

  LOG(INFO) << "Last SparkShuffleWriter finished.";
  if (shuffleWriter_ && !isLocalPartitionWriter) {
    auto status = shuffleWriter_->stop();
    BOLT_CHECK(status.ok(), "Native shuffle write: ShuffleWriter stop failed");
  }

  std::vector<std::vector<int64_t>> peerPartitionLengths;
  std::vector<std::string> peerDataFiles;
  ShuffleWriterMetrics metrics;
  {
    auto promisesGuard = folly::makeGuard([&]() {
      peers.clear();
      for (auto& promise : promises) {
        promise.setValue();
      }
    });

    for (auto& peer : peers) {
      auto* op = peer->findOperator(planNodeId());
      auto* writer = dynamic_cast<SparkShuffleWriter*>(op);
      BOLT_CHECK_NOT_NULL(writer);
      if (writer->shuffleWriter_) {
        if (!isLocalPartitionWriter) {
          auto status = writer->shuffleWriter_->localStop();
          BOLT_CHECK(
              status.ok(), "Native shuffle write: ShuffleWriter stop failed");
        }
        peerDataFiles.push_back(writer->shuffleWriter_->metrics().dataFile);
        peerPartitionLengths.push_back(
            writer->shuffleWriter_->metrics().partitionLengths);
        metrics += writer->shuffleWriter_->metrics();
      }
    }
  }

  if (shuffleWriter_) {
    peerDataFiles.push_back(shuffleWriter_->metrics().dataFile);
    peerPartitionLengths.push_back(shuffleWriter_->metrics().partitionLengths);
    metrics += shuffleWriter_->metrics();
  }

  const auto numPartitions =
      shuffleWriterOptions_.partitionWriterOptions.numPartitions;
  for (const auto& partitionLengths : peerPartitionLengths) {
    BOLT_CHECK_EQ(
        partitionLengths.size(),
        numPartitions,
        "partitionLengths size={} not equal to numPartitions={}",
        partitionLengths.size(),
        numPartitions);
  }

  if (!peerDataFiles.empty() && isLocalPartitionWriter) {
    if (peerDataFiles.size() == 1) {
      const auto& srcFileName = peerDataFiles[0];
      LOG(INFO) << "SparkShuffleWriter: rename shuffle file " << srcFileName
                << " -> "
                << shuffleWriterOptions_.partitionWriterOptions.dataFile;
      auto localFs = std::make_shared<arrow::fs::LocalFileSystem>();
      auto status = localFs->Move(
          srcFileName, shuffleWriterOptions_.partitionWriterOptions.dataFile);
      BOLT_CHECK(
          status.ok(),
          "SparkShuffleWriter: rename shuffle file {} -> {} failed",
          srcFileName,
          shuffleWriterOptions_.partitionWriterOptions.dataFile);
    } else {
      LOG(INFO) << "SparkShuffleWriter merge [" << peerDataFiles.size()
                << "] shuffle files -> "
                << shuffleWriterOptions_.partitionWriterOptions.dataFile;
      auto status = LocalPartitionWriter::merge(
          peerDataFiles,
          peerPartitionLengths,
          shuffleWriterOptions_.partitionWriterOptions.dataFile);
      BOLT_CHECK(status.ok(), "Merge shuffle files failed");
    }
  }

  if (peerPartitionLengths.empty()) {
    metrics.partitionLengths = std::vector<int64_t>(numPartitions, 0);
    metrics.rawPartitionLengths = std::vector<int64_t>(numPartitions, 0);
    LOG(INFO) << "No SparkShuffleWriter generates shuffle data";
  }

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
    auto status = shuffleWriter_->reclaimFixedSize(targetBytes, &evictedSize);
    BOLT_CHECK(status.ok(), "(shuffle) nativeEvict: evict failed");
  } else {
    LOG(INFO) << "ShuffleWriter is null when reclaim";
  }
}

void SparkShuffleWriter::close() {
  Operator::close();
}
