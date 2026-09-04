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

#include "bolt/exec/tests/utils/TpcdsPlanLoader.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>

#include <fmt/format.h>
#include <folly/json.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/serialization/Serializable.h"

namespace bytedance::bolt::exec::test {
namespace {

void collectTableScanNodesRecursive(
    const core::PlanNodePtr& node,
    std::vector<core::TableScanNodePtr>& scans) {
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
    scans.push_back(std::move(scan));
  }
  for (const auto& source : node->sources()) {
    collectTableScanNodesRecursive(source, scans);
  }
}

std::string readFile(const std::string& path) {
  std::ifstream input(path);
  BOLT_USER_CHECK(input, "Failed to open TPC-DS plan file: {}", path);

  std::stringstream contents;
  contents << input.rdbuf();
  return contents.str();
}

std::string lowercase(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return std::tolower(c);
      });
  return value;
}

} // namespace

TpcdsPlanLoader::TpcdsPlanLoader(
    std::string planDirectory,
    memory::MemoryPool* pool,
    bool stripPartitionedOutput)
    : planDirectory_(std::move(planDirectory)),
      pool_(pool),
      stripPartitionedOutput_(stripPartitionedOutput) {
  if (pool_ == nullptr) {
    ownedPool_ = memory::memoryManager()->addLeafPool("TpcdsPlanLoader");
    pool_ = ownedPool_.get();
  }
}

TpchPlan TpcdsPlanLoader::loadPlan(const std::string& queryName) const {
  const auto path = pathForQuery(queryName);
  folly::dynamic serializedPlan;
  try {
    serializedPlan = folly::parseJson(readFile(path));
  } catch (const std::exception& error) {
    BOLT_USER_FAIL(
        "Failed to parse TPC-DS plan JSON from {}: {}", path, error.what());
  }

  core::PlanNodePtr plan;
  try {
    plan = ISerializable::deserialize<core::PlanNode>(serializedPlan, pool_);
  } catch (const std::exception& error) {
    BOLT_USER_FAIL(
        "Failed to deserialize TPC-DS plan from {}: {}", path, error.what());
  }

  maybeStripPartitionedOutput(plan);
  return {
      .plan = std::move(plan),
      .dataFiles = {},
      .dataFileFormat = dwio::common::FileFormat::PARQUET,
      .planName = queryName};
}

const std::vector<std::string>& TpcdsPlanLoader::queryNames() {
  static const std::vector<std::string> kQueryNames = {
      "q1",  "q2",  "q3",  "q4",  "q5",   "q6",   "q7",   "q8",   "q9",
      "q10", "q11", "q12", "q13", "q14a", "q14b", "q15",  "q16",  "q17",
      "q18", "q19", "q20", "q21", "q22",  "q23a", "q23b", "q24a", "q24b",
      "q25", "q26", "q27", "q28", "q29",  "q30",  "q31",  "q32",  "q33",
      "q34", "q35", "q36", "q37", "q38",  "q39a", "q39b", "q40",  "q41",
      "q42", "q43", "q44", "q45", "q46",  "q47",  "q48",  "q49",  "q50",
      "q51", "q52", "q53", "q54", "q55",  "q56",  "q57",  "q58",  "q59",
      "q60", "q61", "q62", "q63", "q64",  "q65",  "q66",  "q67",  "q68",
      "q69", "q70", "q71", "q72", "q73",  "q74",  "q75",  "q76",  "q77",
      "q78", "q79", "q80", "q81", "q82",  "q83",  "q84",  "q85",  "q86",
      "q87", "q88", "q89", "q90", "q91",  "q92",  "q93",  "q94",  "q95",
      "q96", "q97", "q98", "q99"};
  return kQueryNames;
}

void TpcdsPlanLoader::validateQueryDirectory(const std::string& sqlDirectory) {
  std::set<std::string> actualQueryNames;
  std::error_code error;
  for (const auto& entry : std::filesystem::directory_iterator(
           sqlDirectory,
           std::filesystem::directory_options::skip_permission_denied,
           error)) {
    if (entry.is_regular_file() &&
        lowercase(entry.path().extension().string()) == ".sql") {
      actualQueryNames.insert(lowercase(entry.path().stem().string()));
    }
  }
  BOLT_USER_CHECK(
      !error,
      "Failed to scan TPC-DS SQL directory {}: {}",
      sqlDirectory,
      error.message());

  const std::set<std::string> expectedQueryNames(
      queryNames().begin(), queryNames().end());
  BOLT_USER_CHECK(
      actualQueryNames == expectedQueryNames,
      "TPC-DS SQL directory must contain the 103 standard query files "
      "q1.sql through q99.sql, including the a/b variants; found {} files",
      actualQueryNames.size());
}

std::vector<core::TableScanNodePtr> TpcdsPlanLoader::collectTableScanNodes(
    const core::PlanNodePtr& plan) {
  std::vector<core::TableScanNodePtr> scans;
  if (plan != nullptr) {
    collectTableScanNodesRecursive(plan, scans);
  }
  return scans;
}

std::string TpcdsPlanLoader::pathForQuery(const std::string& queryName) const {
  const auto normalizedQueryName = lowercase(queryName);
  BOLT_USER_CHECK(
      std::find(
          queryNames().begin(), queryNames().end(), normalizedQueryName) !=
          queryNames().end(),
      "Unknown TPC-DS query: {}",
      queryName);

  const auto planDirectory = std::filesystem::path(planDirectory_);
  const std::array candidates = {
      planDirectory / fmt::format("{}.json", normalizedQueryName),
      planDirectory / fmt::format("Q{}.json", normalizedQueryName.substr(1)),
  };
  for (const auto& candidate : candidates) {
    if (std::filesystem::is_regular_file(candidate)) {
      return candidate.string();
    }
  }
  BOLT_USER_FAIL(
      "TPC-DS plan file does not exist for {} in {} (tried {} and {})",
      normalizedQueryName,
      planDirectory_,
      candidates[0].filename().string(),
      candidates[1].filename().string());
}

void TpcdsPlanLoader::maybeStripPartitionedOutput(
    core::PlanNodePtr& plan) const {
  if (!stripPartitionedOutput_ || plan == nullptr) {
    return;
  }
  if (auto output =
          std::dynamic_pointer_cast<const core::PartitionedOutputNode>(plan)) {
    BOLT_CHECK_EQ(
        output->sources().size(),
        1,
        "PartitionedOutput must have exactly one source");
    plan = output->sources().front();
  }
}

} // namespace bytedance::bolt::exec::test
