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
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <folly/dynamic.h>
#include <gtest/gtest.h>

#include "bolt/connectors/hive/tests/HiveConnectorTestBase.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/type/tests/SubfieldFiltersBuilder.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::connector::hive;

namespace {

class HiveConnectorSerDeTest : public HiveConnectorTestBase {
 protected:
  HiveConnectorSerDeTest() {
    Type::registerSerDe();
    common::Filter::registerSerDe();
    core::ITypedExpr::registerSerDe();
    HiveTableHandle::registerSerDe();
    HiveColumnHandle::registerSerDe();
    LocationHandle::registerSerDe();
    HiveInsertTableHandle::registerSerDe();
  }

  template <typename T>
  static void testSerde(const T& handle) {
    auto str = handle.toString();
    auto obj = handle.serialize();
    auto clone = ISerializable::deserialize<T>(obj);
    ASSERT_EQ(clone->toString(), str);
  }

  static void testSerde(const HiveTableHandle& handle) {
    auto str = handle.toString();
    auto obj = handle.serialize();
    auto clone = ISerializable::deserialize<HiveTableHandle>(obj);
    ASSERT_EQ(clone->toString(), str);
    ASSERT_EQ(
        handle.remainingFilter()->type(), clone->remainingFilter()->type());

    auto& filters = handle.subfieldFilters();
    auto& cloneFilters = clone->subfieldFilters();
    ASSERT_EQ(filters.size(), cloneFilters.size());
    for (const auto& [subfield, filter] : handle.subfieldFilters()) {
      ASSERT_NE(cloneFilters.find(subfield), cloneFilters.end());
      ASSERT_TRUE(filter->testingEquals(*cloneFilters.at(subfield)));
    }
  }
};

TEST_F(HiveConnectorSerDeTest, hiveTableHandle) {
  auto rowType =
      ROW({"c0c0", "c1", "c2", "c3", "c4", "c5"},
          {INTEGER(), BIGINT(), DOUBLE(), BOOLEAN(), BIGINT(), VARCHAR()});
  auto tableHandle = makeTableHandle(
      common::test::SubfieldFiltersBuilder()
          .add("c0.c0", isNotNull())
          .add("c1", lessThanOrEqual(std::numeric_limits<int64_t>::max()))
          .add("c2", greaterThanOrEqualDouble(3.1415))
          .add("c3", boolEqual(true))
          .add("c4", in(std::vector<int64_t>{0xdeadbeaf, 0xcafecafe}))
          .add("c2", notIn(std::vector<int64_t>{0xdeadbeaf, 0xcafecafe}))
          .add(
              "c5",
              orFilter(between("abc", "efg"), greaterThanOrEqual("dragon")))
          .build(),
      parseExpr("c1 > c4 and c3 = true", rowType),
      "hive_table",
      ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}));
  testSerde(*tableHandle);
}

TEST_F(HiveConnectorSerDeTest, hiveColumnHandle) {
  auto columnType = ROW(
      {{"c0c0", BIGINT()},
       {"c0c1",
        ARRAY(MAP(
            VARCHAR(), ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})))}});
  auto columnHandle = makeColumnHandle(
      "columnHandle", columnType, {"c0.c0c1[3][\"foo\"].c0c1c0"});
  testSerde(*columnHandle);
}

TEST_F(HiveConnectorSerDeTest, locationHandle) {
  auto locationHandle = makeLocationHandle(
      "targetDirectory", "writeDirectory", LocationHandle::TableType::kNew);
  testSerde(*locationHandle);
}

TEST_F(HiveConnectorSerDeTest, hiveInsertTableHandle) {
  auto tableColumnNames = std::vector<std::string>{"id", "row", "arr", "loc"};
  auto bigintType = TypeFactory<TypeKind::BIGINT>::create();
  auto rowType{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
  auto arrType =
      ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto varcharType = TypeFactory<TypeKind::VARCHAR>::create();
  std::vector<TypePtr> tableColumnTypes;
  tableColumnTypes.reserve(4);
  tableColumnTypes.emplace_back(bigintType);
  tableColumnTypes.emplace_back(rowType);
  tableColumnTypes.emplace_back(arrType);
  tableColumnTypes.emplace_back(varcharType);

  auto locationHandle = makeLocationHandle(
      "targetDirectory",
      std::optional("writeDirectory"),
      LocationHandle::TableType::kNew);
  auto hiveInsertTableHandle = makeHiveInsertTableHandle(
      tableColumnNames, tableColumnTypes, {"loc"}, locationHandle);

  testSerde(*hiveInsertTableHandle);
}

} // namespace
