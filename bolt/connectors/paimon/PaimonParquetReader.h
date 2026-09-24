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

#include <memory>
#include <string>

#include <paimon/format/file_format.h>
#include <paimon/format/file_format_factory.h>
#include "bolt/connectors/paimon/PaimonReadFile.h"

namespace bytedance::bolt::connector::paimon {

// Native predicates and selection bitmaps are evaluated before materialization.
// Both batch interfaces return compact results. The v0.3.0 row mapping API
// reports each survivor's original physical file position.
class PaimonParquetReader : public ::paimon::FileFormat {
 public:
  explicit PaimonParquetReader(
      const std::map<std::string, std::string>& options);

  const std::string& Identifier() const override;

  ::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>>
  CreateReaderBuilder(int32_t batch_size) const override;

  ::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>>
  CreateWriterBuilder(::ArrowSchema* schema, int32_t batch_size) const override;

  ::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>>
  CreateStatsExtractor(::ArrowSchema* schema) const override;

 private:
  PaimonIoOptions ioOptions_;
  uint8_t timestampPrecision_ = 3; // default: milliseconds (matches hive)
};

// Ensures that paimon's FileFormatFactory for "parquet" (backed by
// bolt's native parquet reader) is registered
void EnsurePaimonParquetFormatRegistered();

} // namespace bytedance::bolt::connector::paimon

namespace paimon {

class ParquetFileFormatFactory : public ::paimon::FileFormatFactory {
 public:
  static constexpr char kIDENTIFIER[] = "parquet";

  const char* Identifier() const override {
    return kIDENTIFIER;
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileFormat>> Create(
      const std::map<std::string, std::string>& options) const override;
};

void ensureParquetFormatFactoryRegistered();

} // namespace paimon
