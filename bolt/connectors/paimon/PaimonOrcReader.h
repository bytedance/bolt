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

#include <paimon/format/file_format.h>
#include "bolt/connectors/paimon/PaimonReadFile.h"

namespace bytedance::bolt::connector::paimon {

/// Native ORC adapter exporting contiguous physical batches. Paimon applies
/// selection bitmaps and exact predicates; this reader does not compact rows.
/// Timestamp fields in projections or predicates are rejected until the native
/// decoder supports ORC writer timezones. Non-timestamp projections remain
/// valid.
class PaimonOrcReader : public ::paimon::FileFormat {
 public:
  explicit PaimonOrcReader(const std::map<std::string, std::string>& options);

  const std::string& Identifier() const override;
  ::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>>
  CreateReaderBuilder(int32_t batchSize) const override;
  ::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>>
  CreateWriterBuilder(::ArrowSchema* schema, int32_t batchSize) const override;
  ::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>>
  CreateStatsExtractor(::ArrowSchema* schema) const override;

 private:
  std::map<std::string, std::string> options_;
  PaimonIoOptions ioOptions_;
  uint8_t timestampPrecision_{6};
};

// Explicit reference keeps the factory linked when Bolt is a static archive.
void EnsurePaimonOrcFormatRegistered();

} // namespace bytedance::bolt::connector::paimon
