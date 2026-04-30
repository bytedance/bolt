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

#include <cstdint>
#include <string>

#include "bolt/common/config/Config.h"

namespace bytedance::bolt::config {
class ConfigBase;
}

namespace bytedance::bolt::connector::paimon {

/// Paimon connector performance-tuning configuration.
///
/// All values are read from `config::ConfigBase` (session properties) with
/// sensible defaults so that no explicit configuration is required for basic
/// usage.
class PaimonConfig {
 public:
  // ---- Batch / Row Reading --------------------------------------------------

  static constexpr const char* kReadBatchSize = "paimon.read.batch-size";
  static constexpr const char* kMultiThreadRowToBatch =
      "paimon.read.multi-thread-row-to-batch";
  static constexpr const char* kRowToBatchThreadNum =
      "paimon.read.row-to-batch-thread-num";

  // ---- Prefetch & I/O -------------------------------------------------------

  static constexpr const char* kPrefetchEnabled =
      "paimon.read.prefetch-enabled";
  static constexpr const char* kPrefetchBatchCount =
      "paimon.read.prefetch-batch-count";
  static constexpr const char* kPrefetchMaxParallel =
      "paimon.read.prefetch-max-parallel";

  // ---- Filter Pushdown ------------------------------------------------------

  static constexpr const char* kPredicateFilterEnabled =
      "paimon.read.predicate-filter-enabled";

  // ---- I/O Tuning (consumed by PaimonReadFile / bolt::ReadFile)
  // -----------------

  static constexpr const char* kNaturalReadSize = "paimon.io.natural-read-size";
  static constexpr const char* kCoalesceReads = "paimon.io.coalesce-reads";

  // ---- Read Semantics (consumed by ParquetReader)
  // ------------------------------

  static constexpr const char* kReadTimestampUnit =
      "spark.gluten.paimon.read.timestamp-unit";

  explicit PaimonConfig(std::shared_ptr<const config::ConfigBase> config);

  // Batch / Row Reading
  int32_t readBatchSize() const;
  bool multiThreadRowToBatch() const;
  uint32_t rowToBatchThreadNumber() const;

  // Prefetch & I/O
  bool prefetchEnabled() const;
  uint32_t prefetchBatchCount() const;
  uint32_t prefetchMaxParallel() const;

  // Filter Pushdown
  bool predicateFilterEnabled() const;

  // I/O Tuning
  uint64_t naturalReadSize() const;
  bool coalesceReads() const;

  // Read Semantics
  uint8_t readTimestampUnit() const;

  const std::shared_ptr<const config::ConfigBase>& config() const {
    return config_;
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};

/// Per-query I/O options threaded from PaimonDataSource through paimon's
/// options map → PaimonParquetReader → PaimonParquetReaderBuilder → Build()
/// → PaimonReadFile (the bolt::ReadFile adapter used inside paimon's internal
/// reader pipeline).
///
/// These map onto bolt::ReadFile interface methods that bolt's generic I/O
/// layer calls:
///   - naturalReadSize   → BufferedInput buffer sizing
///   - coalesceReads     → I/O coalescing in dwio/common/file/Utils.h
struct PaimonIoOptions {
  uint64_t naturalReadSize = 32ULL << 20; // 32 MB
  bool coalesceReads = true;
};

} // namespace bytedance::bolt::connector::paimon
