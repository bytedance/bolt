/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/connectors/hive/bytelake/BytelakeScanSpecUtil.h"

#include "gtest/gtest.h"

using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt;

namespace {

RowTypePtr makeRowType(std::vector<std::pair<std::string, TypePtr>> fields) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(fields.size());
  types.reserve(fields.size());
  for (auto& f : fields) {
    names.push_back(std::move(f.first));
    types.push_back(std::move(f.second));
  }
  return ROW(std::move(names), std::move(types));
}

} // namespace

TEST(BytelakeScanSpecUtilTest, singlePkSinglePrecombine) {
  // Minimal viable shape: 1 PK + 1 precombine + 1 isDeleted.
  auto full = makeRowType({
      {"a", BIGINT()},
      {"pk", VARCHAR()},
      {"c", DOUBLE()},
      {"ts", BIGINT()},
      {"deleted", BOOLEAN()},
  });
  auto schema = makeBytelakeKeyOnlySchema(full, {1}, {3}, 4);

  // Layout: [pk, ts, deleted] at reduced channels 0, 1, 2.
  ASSERT_EQ(schema.rowType->size(), 3u);
  EXPECT_EQ(schema.rowType->nameOf(0), "pk");
  EXPECT_EQ(schema.rowType->nameOf(1), "ts");
  EXPECT_EQ(schema.rowType->nameOf(2), "deleted");

  EXPECT_EQ(schema.numPkColumns, 1);
  EXPECT_EQ(schema.numPrecombineColumns, 1);
  // isDeleted is at channel = numPk + numPrecombine = 2.

  auto& children = schema.scanSpec->children();
  ASSERT_EQ(children.size(), 3u);
  EXPECT_EQ(children[0]->channel(), 0);
  EXPECT_EQ(children[0]->fieldName(), "pk");
  EXPECT_EQ(children[1]->channel(), 1);
  EXPECT_EQ(children[1]->fieldName(), "ts");
  EXPECT_EQ(children[2]->channel(), 2);
  EXPECT_EQ(children[2]->fieldName(), "deleted");
}

TEST(BytelakeScanSpecUtilTest, productionLikeShape) {
  // Realistic: 1 PK (unique_id) + 2 precombine (ts, _hoodie_commit_time)
  // + isDeleted (_hoodie_is_deleted), all sparse in a wide schema.
  auto full = makeRowType({
      {"col0", BIGINT()},
      {"unique_id", VARCHAR()},
      {"col2", BIGINT()},
      {"ts", BIGINT()},
      {"_hoodie_commit_time", VARCHAR()},
      {"_hoodie_is_deleted", BOOLEAN()},
      {"col6", VARCHAR()},
  });
  auto schema = makeBytelakeKeyOnlySchema(
      full,
      /*pk=*/{1},
      /*precombine=*/{3, 4},
      /*isDeleted=*/5);

  // Reduced rowType: [unique_id, ts, _hoodie_commit_time, _hoodie_is_deleted]
  ASSERT_EQ(schema.rowType->size(), 4u);
  EXPECT_EQ(schema.rowType->nameOf(0), "unique_id");
  EXPECT_EQ(schema.rowType->nameOf(1), "ts");
  EXPECT_EQ(schema.rowType->nameOf(2), "_hoodie_commit_time");
  EXPECT_EQ(schema.rowType->nameOf(3), "_hoodie_is_deleted");
  EXPECT_TRUE(schema.rowType->childAt(0)->isVarchar());
  EXPECT_TRUE(schema.rowType->childAt(1)->isBigint());
  EXPECT_TRUE(schema.rowType->childAt(2)->isVarchar());
  EXPECT_TRUE(schema.rowType->childAt(3)->isBoolean());

  EXPECT_EQ(schema.numPkColumns, 1);
  EXPECT_EQ(schema.numPrecombineColumns, 2);
  // isDeleted at reduced channel = 1 + 2 = 3 (last column).

  auto& children = schema.scanSpec->children();
  ASSERT_EQ(children.size(), 4u);
  EXPECT_EQ(children[0]->channel(), 0);
  EXPECT_EQ(children[1]->channel(), 1);
  EXPECT_EQ(children[2]->channel(), 2);
  EXPECT_EQ(children[3]->channel(), 3);
  EXPECT_EQ(children[0]->fieldName(), "unique_id");
  EXPECT_EQ(children[1]->fieldName(), "ts");
  EXPECT_EQ(children[2]->fieldName(), "_hoodie_commit_time");
  EXPECT_EQ(children[3]->fieldName(), "_hoodie_is_deleted");
  for (auto& c : children) {
    EXPECT_TRUE(c->projectOut());
  }
}

TEST(BytelakeScanSpecUtilTest, multiColumnPk) {
  // Composite PK case: 2 PK cols + 1 precombine + isDeleted.
  auto full = makeRowType({
      {"date", VARCHAR()},
      {"hour", VARCHAR()},
      {"user_id", BIGINT()},
      {"event_seq", BIGINT()},
      {"deleted", BOOLEAN()},
  });
  auto schema = makeBytelakeKeyOnlySchema(
      full,
      /*pk=*/{0, 2},
      /*precombine=*/{3},
      /*isDeleted=*/4);

  // Layout: [date, user_id, event_seq, deleted]
  ASSERT_EQ(schema.rowType->size(), 4u);
  EXPECT_EQ(schema.rowType->nameOf(0), "date");
  EXPECT_EQ(schema.rowType->nameOf(1), "user_id");
  EXPECT_EQ(schema.rowType->nameOf(2), "event_seq");
  EXPECT_EQ(schema.rowType->nameOf(3), "deleted");

  EXPECT_EQ(schema.numPkColumns, 2);
  EXPECT_EQ(schema.numPrecombineColumns, 1);
}

TEST(BytelakeScanSpecUtilTest, emptyPrecombineAllowed) {
  // Rare case: no precombine keys (just PK + isDeleted).
  auto full = makeRowType({
      {"pk", VARCHAR()},
      {"deleted", BOOLEAN()},
  });
  auto schema = makeBytelakeKeyOnlySchema(
      full,
      /*pk=*/{0},
      /*precombine=*/{},
      /*isDeleted=*/1);

  ASSERT_EQ(schema.rowType->size(), 2u);
  EXPECT_EQ(schema.rowType->nameOf(0), "pk");
  EXPECT_EQ(schema.rowType->nameOf(1), "deleted");

  EXPECT_EQ(schema.numPkColumns, 1);
  EXPECT_EQ(schema.numPrecombineColumns, 0);
}

TEST(BytelakeScanSpecUtilTest, channelsCanBeFarApartInWideSchema) {
  // Simulate the bytelake post-addColumnsIfNotExists shape where
  // _hoodie_* columns get appended at high indices.
  std::vector<std::pair<std::string, TypePtr>> fields;
  fields.reserve(500);
  for (int i = 0; i < 500; ++i) {
    fields.emplace_back("col" + std::to_string(i), BIGINT());
  }
  // PK at small index (col 42), precombine + isDeleted appended at end.
  fields[42] = {"unique_id", VARCHAR()};
  fields.emplace_back("_hoodie_commit_time", VARCHAR()); // index 500
  fields.emplace_back("_hoodie_is_deleted", BOOLEAN()); // index 501
  auto full = makeRowType(std::move(fields));

  auto schema = makeBytelakeKeyOnlySchema(
      full,
      /*pk=*/{42},
      /*precombine=*/{31, 500}, // ts at 31, commit_time at 500
      /*isDeleted=*/501);

  // Reduced rowType has 4 cols at channels 0..3.
  ASSERT_EQ(schema.rowType->size(), 4u);
  EXPECT_EQ(schema.rowType->nameOf(0), "unique_id");
  EXPECT_EQ(schema.rowType->nameOf(1), "col31");
  EXPECT_EQ(schema.rowType->nameOf(2), "_hoodie_commit_time");
  EXPECT_EQ(schema.rowType->nameOf(3), "_hoodie_is_deleted");

  EXPECT_EQ(schema.numPkColumns, 1);
  EXPECT_EQ(schema.numPrecombineColumns, 2);

  auto& children = schema.scanSpec->children();
  ASSERT_EQ(children.size(), 4u);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(children[i]->channel(), static_cast<int>(i));
  }
}

TEST(BytelakeScanSpecUtilTest, emptyPkRejected) {
  // PK is required; helper should fail when missing.
  auto full =
      makeRowType({{"pk", VARCHAR()}, {"ts", BIGINT()}, {"deleted", BOOLEAN()}});
  EXPECT_ANY_THROW(
      makeBytelakeKeyOnlySchema(full, /*pk=*/{}, /*precombine=*/{1}, /*isDeleted=*/2));
}
