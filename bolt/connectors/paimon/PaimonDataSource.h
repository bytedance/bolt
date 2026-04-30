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

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonConnectorSplit.h"
#include "bolt/connectors/paimon/PaimonTableHandle.h"

// Forward declare paimon types
namespace paimon {
class BatchReader;
class TableRead;
class Split;
class MemoryPool;
} // namespace paimon

namespace bytedance::bolt::connector::paimon {

class PaimonDataSource : public DataSource {
 public:
  PaimonDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
          columnHandles,
      const std::shared_ptr<ConnectorQueryCtx>& queryCtx,
      const core::QueryConfig& queryConfig,
      const std::shared_ptr<PaimonConfig>& paimonConfig);

  // Declare destructor in header but define in cpp to avoid incomplete type
  // issue
  ~PaimonDataSource();

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future)
      override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }
  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }
  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }
  void addDynamicFilter(column_index_t, const std::shared_ptr<common::Filter>&)
      override {}

 private:
  std::shared_ptr<const RowType> outputType_;
  std::shared_ptr<PaimonTableHandle> tableHandle_;
  memory::MemoryPool* pool_;

  std::unique_ptr<::paimon::BatchReader> reader_;
  std::unique_ptr<::paimon::TableRead> tableRead_;
  std::vector<std::shared_ptr<::paimon::Split>> inputSplits_;
  std::shared_ptr<::paimon::MemoryPool> paimonPool_;
  std::shared_ptr<PaimonConnectorSplit> currentSplit_;

  uint64_t completedRows_{0};
  uint64_t completedBytes_{0};
};

} // namespace bytedance::bolt::connector::paimon
