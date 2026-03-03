/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/connectors/paimon/PaimonConnector.h"
#include "bolt/connectors/paimon/PaimonDataSource.h"

namespace paimon {
  class AvroFileFormatFactory;
}

namespace bytedance::bolt::connector::paimon {

std::unique_ptr<DataSource> PaimonConnector::createDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<ColumnHandle>>& columnHandles,
    std::shared_ptr<ConnectorQueryCtx> queryCtx,
    const core::QueryConfig& queryConfig) {
  return std::make_unique<PaimonDataSource>(outputType, tableHandle, columnHandles, queryCtx, queryConfig);
}

} // namespace bytedance::bolt::connector::paimon
