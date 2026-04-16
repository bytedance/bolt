/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/dwio/parquet/reader/StructColumnReader.h"

namespace bytedance::bolt::parquet {

class VariantColumnReader : public StructColumnReader {
 public:
  VariantColumnReader(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec,
      memory::MemoryPool& pool);

  void getValues(const RowSet& rows, VectorPtr* result) override;
};

} // namespace bytedance::bolt::parquet
