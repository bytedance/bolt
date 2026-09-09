/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2026-08-28.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/exec/tests/utils/TpchQueryBuilder.h"

namespace bytedance::bolt::exec::test {

/// Loads serialized Bolt plans for the TPC-DS queries. Plan file names match
/// the SQL file stems, for example q1.json, q14a.json and q14b.json.
class TpcdsPlanLoader {
 public:
  TpcdsPlanLoader(
      std::string planDirectory,
      memory::MemoryPool* pool = nullptr,
      bool stripPartitionedOutput = true);

  /// Loads and deserializes the plan for 'queryName'.
  TpchPlan loadPlan(const std::string& queryName) const;

  /// Returns all query names represented by the TPC-DS SQL files.
  static const std::vector<std::string>& queryNames();

  /// Verifies that sqlDirectory contains exactly the supported .sql files.
  static void validateQueryDirectory(const std::string& sqlDirectory);

  /// Returns all table scans in depth-first plan order.
  static std::vector<core::TableScanNodePtr> collectTableScanNodes(
      const core::PlanNodePtr& plan);

 private:
  std::string pathForQuery(const std::string& queryName) const;
  void maybeStripPartitionedOutput(core::PlanNodePtr& plan) const;

  const std::string planDirectory_;
  memory::MemoryPool* pool_;
  std::shared_ptr<memory::MemoryPool> ownedPool_;
  const bool stripPartitionedOutput_;
};

} // namespace bytedance::bolt::exec::test
