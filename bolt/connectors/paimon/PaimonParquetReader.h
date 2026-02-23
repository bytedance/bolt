/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <paimon/format/file_format.h>
#include "bolt/dwio/parquet/reader/ParquetReader.h"

namespace bytedance::bolt::connector::paimon {

class PaimonParquetReader : public ::paimon::FileFormat {
 public:
  const std::string& Identifier() const override;

  ::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>> CreateReaderBuilder(
      int32_t batch_size) const override;

  ::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>> CreateWriterBuilder(
      ::ArrowSchema* schema, int32_t batch_size) const override;

  ::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>> CreateStatsExtractor(
      ::ArrowSchema* schema) const override;
};

} // namespace bytedance::bolt::connector::paimon
