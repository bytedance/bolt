/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/connectors/paimon/PaimonDataSource.h"
#include "paimon/reader/batch_reader.h"

namespace bytedance::bolt::connector::paimon {

PaimonDataSource::PaimonDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<ColumnHandle>>& columnHandles,
    std::shared_ptr<ConnectorQueryCtx> queryCtx,
    const core::QueryConfig& queryConfig)
    : outputType_(outputType),
      tableHandle_(std::dynamic_pointer_cast<PaimonTableHandle>(tableHandle)) {}

PaimonDataSource::~PaimonDataSource() = default;

void PaimonDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
    currentSplit_ = std::dynamic_pointer_cast<PaimonConnectorSplit>(split);
    // TODO: Deserialize split and create reader using paimon-cpp
    // auto paimonSplit = deserialize(currentSplit_->serializedSplit);
    // reader_ = table_->createReadBuilder(projection)->newRead()->createReader(paimonSplit);
}

std::optional<RowVectorPtr> PaimonDataSource::next(uint64_t size, ContinueFuture& future) {
    if (!reader_) {
        return std::nullopt;
    }
    
    // Placeholder implementation
    // auto batchResult = reader_->NextBatch();
    // Convert to RowVectorPtr
    return std::nullopt; 
}

} // namespace bytedance::bolt::connector::paimon
