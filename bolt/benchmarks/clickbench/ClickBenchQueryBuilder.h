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

#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/exec/tests/utils/TpchQueryBuilder.h"

namespace bytedance::bolt::exec::test {

/// Builds the 43 ClickBench queries for a Hive-style Parquet or DWRF data
/// source. ClickBench is a SQL workload, while Bolt is an execution library;
/// this class is the deliberately explicit SQL-to-PlanNode translation.
class ClickBenchQueryBuilder {
 public:
  explicit ClickBenchQueryBuilder(
      dwio::common::FileFormat format = dwio::common::FileFormat::PARQUET);

  /// Discovers a single hits file, or files below dataPath[/hits], and reads
  /// the physical schema from the first file.
  void initialize(const std::string& dataPath);

  /// Initializes from an already known schema and set of files. This is used
  /// by deterministic tests and by callers that discover data themselves.
  void initialize(RowTypePtr fileType, std::vector<std::string> dataFiles);

  TpchPlan getQueryPlan(int32_t queryId) const;

  static const std::vector<std::string>& queryNames();
  static const std::vector<std::string>& columnNames();

 private:
  RowTypePtr selectedType(const std::vector<std::string>& columns) const;
  TpchPlan makePlan(
      int32_t queryId,
      const std::vector<std::string>& scanColumns,
      const std::string& filter,
      const std::vector<std::string>& projections,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postProjections,
      const std::string& postFilter,
      const std::vector<std::string>& orderBy,
      int64_t offset = 0,
      int64_t limit = 0,
      bool hasDistinctAggregation = false,
      const std::vector<std::string>& outputProjections = {}) const;

  const dwio::common::FileFormat format_;
  RowTypePtr fileType_;
  RowTypePtr canonicalType_;
  std::vector<std::string> dataFiles_;
  std::unordered_map<std::string, std::string> fileColumnNames_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

} // namespace bytedance::bolt::exec::test
