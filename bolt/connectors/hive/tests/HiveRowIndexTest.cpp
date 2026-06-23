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

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/hive/HiveConnectorUtil.h"
#include "bolt/type/Subfield.h"
#include "bolt/type/Type.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;

TEST(HiveRowIndexTest, marksScanSpecForGenericRowIndex) {
  auto rowType = ROW({{"a", INTEGER()}, {"_row_index", BIGINT()}});

  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      subfields;
  SubfieldFilters filters;
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>
      partitionKeys;
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>
      infoColumns;

  // Ensure a MemoryManager exists and create a small leaf pool for API needs.
  if (!memory::MemoryManager::testInstance()) {
    memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  }
  auto poolHolder =
      memory::MemoryManager::getInstance()->addLeafPool("rowindex-test");
  memory::MemoryPool* pool = poolHolder.get();

  // Mark the second column as a row index column with no backing file column.
  std::vector<std::tuple<size_t, std::optional<std::string>>> rowIndexColumns =
      {{1, std::nullopt}};

  auto spec = makeScanSpec(
      rowType,
      subfields,
      filters,
      rowType,
      partitionKeys,
      infoColumns,
      pool,
      /*expressionEvaluator*/ nullptr,
      /*statis*/ nullptr,
      rowIndexColumns);

  auto* child = spec->childByName("_row_index");
  ASSERT_NE(child, nullptr);
  EXPECT_TRUE(child->isRowIndex());
}
