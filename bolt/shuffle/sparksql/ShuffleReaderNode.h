/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <cstdint>
#include "bolt/exec/Driver.h"
#include "bolt/exec/Operator.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
namespace bytedance::bolt::shuffle::sparksql {

class SparkShuffleReaderNode : public bytedance::bolt::core::PlanNode {
 public:
  SparkShuffleReaderNode(
      const bytedance::bolt::core::PlanNodeId& id,
      const RowTypePtr outputType,
      const ShuffleReaderOptions& options,
      std::shared_ptr<ReaderStreamIterator> readerStreamIterator)
      : bytedance::bolt::core::PlanNode(id),
        outputType_(outputType),
        shuffleReaderOptions_(options),
        readerStreamIterator_(std::move(readerStreamIterator)) {}

  const bytedance::bolt::RowTypePtr& outputType() const override {
    return outputType_;
  }

  std::string_view name() const override {
    return "SparkShuffleReader";
  }

  folly::dynamic serialize() const override {
    folly::dynamic result = PlanNode::serialize();
    return result;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  ShuffleReaderOptions getShuffleReaderOptions() const {
    return shuffleReaderOptions_;
  }

  std::shared_ptr<ReaderStreamIterator> getReaderStreams() const {
    return readerStreamIterator_;
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "ShuffleReader";
  }

  const RowTypePtr outputType_;
  const ShuffleReaderOptions shuffleReaderOptions_;
  std::shared_ptr<ReaderStreamIterator> readerStreamIterator_;
  const std::vector<bytedance::bolt::core::PlanNodePtr> sources_;
};

class SparkShuffleReader : public bytedance::bolt::exec::SourceOperator {
 public:
  SparkShuffleReader(
      int32_t operatorId,
      bytedance::bolt::exec::DriverCtx* driverCtx,
      std::shared_ptr<const SparkShuffleReaderNode> shuffleReaderNode);

  bytedance::bolt::RowVectorPtr getOutput() override;

  bytedance::bolt::exec::BlockingReason isBlocked(
      bytedance::bolt::ContinueFuture* /* unused */) override {
    return bytedance::bolt::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  void close() override;

  void init();

 private:
  std::once_flag initFlag_;
  ShuffleReaderOptions shuffleReaderOptions_;
  std::shared_ptr<ReaderStreamIterator> readerStreamIterator_;
  std::shared_ptr<BoltArrowMemoryPool> arrowPool_;

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Codec> codec_;

  int32_t batchSize_;
  int32_t shuffleBatchByteSize_;
  int32_t numPartitions_{0};
  ShuffleWriterType shuffleWriterType_{ShuffleWriterType::V1};
  std::string partitioningShortName_;

  std::vector<bool> isValidityBuffer_;
  bool hasComplexType_{false};

  uint64_t deserializeTime_{0};
  uint64_t decompressTime_{0};

  // for rowbased shuffle
  std::shared_ptr<AdaptiveParallelZstdCodec> zstdCodec_{nullptr};
  std::shared_ptr<RowBufferPool> rowBufferPool_{nullptr};
  std::shared_ptr<ShuffleRowToColumnarConverter> row2ColConverter_{nullptr};

  std::unique_ptr<BoltColumnarBatchDeserializer> columnarBatchDeserializer_;

  bool isRowBased_ = false;

  bool finished_ = false;
};

class SparkShuffleReaderTranslator
    : public bytedance::bolt::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<bytedance::bolt::exec::Operator> toOperator(
      bytedance::bolt::exec::DriverCtx* ctx,
      int32_t id,
      const bytedance::bolt::core::PlanNodePtr& node) {
    if (auto shuffleReaderNode =
            std::dynamic_pointer_cast<const SparkShuffleReaderNode>(node)) {
      return std::make_unique<SparkShuffleReader>(id, ctx, shuffleReaderNode);
    }
    return nullptr;
  }
};

} // namespace bytedance::bolt::shuffle::sparksql
