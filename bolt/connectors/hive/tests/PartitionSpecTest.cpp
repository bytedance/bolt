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
 * 2026-03-30.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/connectors/hive/iceberg/PartitionSpec.h"

#include <gtest/gtest.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace bytedance::bolt;

namespace bytedance::bolt::connector::hive::iceberg {
namespace {

TEST(PartitionSpecTest, invalidColumnType) {
  auto makeSpec = [](const TypePtr& type) {
    return std::make_shared<const IcebergPartitionSpec>(
        1,
        std::vector<IcebergPartitionSpec::Field>{
            {"c0", type, TransformType::kIdentity, std::nullopt}});
  };

  BOLT_ASSERT_USER_THROW(
      makeSpec(ROW({{"a", INTEGER()}})),
      "Type is not supported as a partition column: ROW");
  BOLT_ASSERT_USER_THROW(
      makeSpec(ARRAY(INTEGER())),
      "Type is not supported as a partition column: ARRAY");
  BOLT_ASSERT_USER_THROW(
      makeSpec(MAP(VARCHAR(), INTEGER())),
      "Type is not supported as a partition column: MAP");
  BOLT_ASSERT_USER_THROW(
      makeSpec(TIMESTAMP_WITH_TIME_ZONE()),
      "Type is not supported as a partition column: TIMESTAMP WITH TIME ZONE");
}

TEST(PartitionSpecTest, invalidTransformTypeCompatibility) {
  BOLT_ASSERT_USER_THROW(
      std::make_shared<const IcebergPartitionSpec>(
          1,
          std::vector<IcebergPartitionSpec::Field>{
              {"c0", VARCHAR(), TransformType::kYear, std::nullopt}}),
      "Transform is not supported for partition column. Column: 'c0', Type: 'VARCHAR', Transform: 'year'.");

  BOLT_ASSERT_USER_THROW(
      std::make_shared<const IcebergPartitionSpec>(
          1,
          std::vector<IcebergPartitionSpec::Field>{
              {"c0", DATE(), TransformType::kHour, std::nullopt}}),
      "Transform is not supported for partition column. Column: 'c0', Type: 'DATE', Transform: 'hour'.");
}

TEST(PartitionSpecTest, invalidMultipleTransforms) {
  BOLT_ASSERT_USER_THROW(
      std::make_shared<const IcebergPartitionSpec>(
          1,
          std::vector<IcebergPartitionSpec::Field>{
              {"c0", VARCHAR(), TransformType::kIdentity, std::nullopt},
              {"c0", VARCHAR(), TransformType::kIdentity, std::nullopt}}),
      "Column: 'c0', Category: Identity, Transforms: [identity, identity]");

  BOLT_ASSERT_USER_THROW(
      std::make_shared<const IcebergPartitionSpec>(
          1,
          std::vector<IcebergPartitionSpec::Field>{
              {"c0", VARCHAR(), TransformType::kBucket, 16},
              {"c0", VARCHAR(), TransformType::kBucket, 32}}),
      "Column: 'c0', Category: Bucket, Transforms: [bucket, bucket]");

  BOLT_ASSERT_USER_THROW(
      std::make_shared<const IcebergPartitionSpec>(
          1,
          std::vector<IcebergPartitionSpec::Field>{
              {"c0", TIMESTAMP(), TransformType::kYear, std::nullopt},
              {"c0", TIMESTAMP(), TransformType::kMonth, std::nullopt},
              {"c0", TIMESTAMP(), TransformType::kDay, std::nullopt},
              {"c0", TIMESTAMP(), TransformType::kHour, std::nullopt}}),
      "Column: 'c0', Category: Temporal, Transforms: [year, month, day, hour]");
}

TEST(PartitionSpecTest, validMultipleTransforms) {
  auto spec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"c0", VARCHAR(), TransformType::kIdentity, std::nullopt},
          {"c0", VARCHAR(), TransformType::kBucket, 16},
          {"c0", VARCHAR(), TransformType::kTruncate, 10},
          {"c1", DATE(), TransformType::kYear, std::nullopt}});

  ASSERT_EQ(spec->fields.size(), 4);
  EXPECT_TRUE(spec->fields[0].resultType()->equivalent(*VARCHAR()));
  EXPECT_TRUE(spec->fields[1].resultType()->equivalent(*INTEGER()));
  EXPECT_TRUE(spec->fields[2].resultType()->equivalent(*VARCHAR()));
  EXPECT_TRUE(spec->fields[3].resultType()->equivalent(*INTEGER()));
}

TEST(PartitionSpecTest, resultType) {
  EXPECT_TRUE((IcebergPartitionSpec::Field{
      "c0", DATE(), TransformType::kDay, std::nullopt}
                   .resultType()
                   ->equivalent(*DATE())));
  EXPECT_TRUE((IcebergPartitionSpec::Field{
      "c0", TIMESTAMP(), TransformType::kHour, std::nullopt}
                   .resultType()
                   ->equivalent(*INTEGER())));
  EXPECT_TRUE(
      (IcebergPartitionSpec::Field{"c0", BIGINT(), TransformType::kTruncate, 4}
           .resultType()
           ->equivalent(*BIGINT())));
}

} // namespace
} // namespace bytedance::bolt::connector::hive::iceberg
