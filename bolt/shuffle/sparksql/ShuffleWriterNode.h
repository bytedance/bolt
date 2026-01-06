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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
namespace bytedance::bolt::shuffle::sparksql {

typedef std::function<void(const ShuffleWriterMetrics&)>
    ReportShuffleStatusCallback;

class SparkShuffleWriterNode : public bytedance::bolt::core::PlanNode {
 public:
  SparkShuffleWriterNode(
      const bytedance::bolt::core::PlanNodeId& id,
      const ShuffleWriterOptions& options,
      ReportShuffleStatusCallback reportShuffleStatusCallback,
      bytedance::bolt::core::PlanNodePtr source)
      : bytedance::bolt::core::PlanNode(id),
        shuffleWriterOptions_(options),
        reportShuffleStatusCallback_(reportShuffleStatusCallback),
        sources_{source} {}

  const bytedance::bolt::RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  std::string_view name() const override {
    return "SparkShuffleWriter";
  }

  folly::dynamic serialize() const override {
    folly::dynamic result = PlanNode::serialize();
    return result;
  }

  const ShuffleWriterOptions& getShuffleWriterOptions() const {
    return shuffleWriterOptions_;
  }

  ReportShuffleStatusCallback getReportShuffleStatusCallback() const {
    return reportShuffleStatusCallback_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "ShuffleWriter";
  }

  const ShuffleWriterOptions shuffleWriterOptions_;
  ReportShuffleStatusCallback reportShuffleStatusCallback_;
  const std::vector<bytedance::bolt::core::PlanNodePtr> sources_;
};

class SparkShuffleWriter : public bytedance::bolt::exec::Operator {
 public:
  SparkShuffleWriter(
      int32_t operatorId,
      bytedance::bolt::exec::DriverCtx* driverCtx,
      std::shared_ptr<const SparkShuffleWriterNode> shuffleWriterNode);

  // init when first row vector received
  void init(const bytedance::bolt::RowVectorPtr& rv);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(bytedance::bolt::RowVectorPtr input) override;

  bytedance::bolt::RowVectorPtr getOutput() override;

  bytedance::bolt::exec::BlockingReason isBlocked(
      bytedance::bolt::ContinueFuture* /* unused */) override {
    return bytedance::bolt::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  void noMoreInput() override;

  bool canReclaim() const override {
    return true;
  }

  void reclaim(
      uint64_t targetBytes,
      bytedance::bolt::memory::MemoryReclaimer::Stats& stats) override;

  void close() override;

 private:
  std::once_flag initOnceFlag_;
  ShuffleWriterOptions shuffleWriterOptions_;
  uint64_t minMemLimit_;
  std::unique_ptr<BoltArrowMemoryPool> arrowPool_;
  std::shared_ptr<BoltShuffleWriter> shuffleWriter_;
  bool finished_ = false;
  ReportShuffleStatusCallback reportShuffleStatusCallback_;
};

class SparkShuffleWriterTranslator
    : public bytedance::bolt::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<bytedance::bolt::exec::Operator> toOperator(
      bytedance::bolt::exec::DriverCtx* ctx,
      int32_t id,
      const bytedance::bolt::core::PlanNodePtr& node) {
    if (auto shuffleWriterNode =
            std::dynamic_pointer_cast<const SparkShuffleWriterNode>(node)) {
      return std::make_unique<SparkShuffleWriter>(id, ctx, shuffleWriterNode);
    }
    return nullptr;
  }
};
} // namespace bytedance::bolt::shuffle::sparksql
