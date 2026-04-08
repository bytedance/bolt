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

#include "bolt/connectors/hive/iceberg/IcebergPartitionName.h"

#include <fmt/core.h>
#include <gtest/gtest.h>

#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;

namespace bytedance::bolt::connector::hive::iceberg {
namespace {

IcebergPartitionSpecPtr makeSpec(
    std::vector<IcebergPartitionSpec::Field> fields) {
  return std::make_shared<const IcebergPartitionSpec>(1, std::move(fields));
}

std::vector<std::string> partitionKeyNames(
    const IcebergPartitionSpecPtr& partitionSpec) {
  std::vector<std::string> names;
  names.reserve(partitionSpec->fields.size());
  for (const auto& field : partitionSpec->fields) {
    std::string key = field.transformType == TransformType::kIdentity
        ? field.name
        : fmt::format(
              "{}_{}",
              field.name,
              TransformTypeName::toName(field.transformType));
    names.emplace_back(std::move(key));
  }
  return names;
}

class IcebergPartitionNameTest : public testing::Test,
                                 public bytedance::bolt::test::VectorTestBase {
 protected:
  RowVectorPtr makePartitionValues(
      const IcebergPartitionSpecPtr& partitionSpec,
      const std::vector<VectorPtr>& children) {
    return makeRowVector(partitionKeyNames(partitionSpec), children);
  }
};

TEST_F(IcebergPartitionNameTest, formatsPartitionValues) {
  EXPECT_EQ(
      IcebergPartitionName::toName(18'262, DATE(), TransformType::kIdentity),
      "2020-01-01");
  EXPECT_EQ(
      IcebergPartitionName::toName(true, BOOLEAN(), TransformType::kIdentity),
      "true");
  EXPECT_EQ(
      IcebergPartitionName::toName(
          StringView("a/b=c"), VARCHAR(), TransformType::kIdentity),
      "a/b=c");
  EXPECT_EQ(
      IcebergPartitionName::toName(
          StringView("\x01\x02\x03", 3), VARBINARY(), TransformType::kIdentity),
      "AQID");
  EXPECT_EQ(
      IcebergPartitionName::toName(
          Timestamp(1'609'459'200, 999'000'000),
          TIMESTAMP(),
          TransformType::kIdentity),
      "2021-01-01T00:00:00.999");
  EXPECT_EQ(
      IcebergPartitionName::toName(0, INTEGER(), TransformType::kYear), "1970");
  EXPECT_EQ(
      IcebergPartitionName::toName(612, INTEGER(), TransformType::kMonth),
      "2021-01");
  EXPECT_EQ(
      IcebergPartitionName::toName(0, DATE(), TransformType::kDay),
      "1970-01-01");
  EXPECT_EQ(
      IcebergPartitionName::toName(24, INTEGER(), TransformType::kHour),
      "1970-01-02-00");
  EXPECT_EQ(
      IcebergPartitionName::toName(
          int64_t{12'345}, DECIMAL(6, 2), TransformType::kTruncate),
      "123.45");
}

TEST_F(IcebergPartitionNameTest, generatesPartitionPath) {
  auto spec = makeSpec({
      {"name", VARCHAR(), TransformType::kIdentity, std::nullopt},
      {"ts", TIMESTAMP(), TransformType::kMonth, std::nullopt},
      {"id", BIGINT(), TransformType::kBucket, 16},
      {"note", VARCHAR(), TransformType::kTruncate, 3},
  });
  auto generator = IcebergPartitionName(spec);

  auto partitionValues = makePartitionValues(
      spec,
      {
          makeConstant<std::string>("a/b=c", 1, VARCHAR()),
          makeConstant<int32_t>(612, 1, INTEGER()),
          makeConstant<int32_t>(7, 1, INTEGER()),
          makeConstant<std::string>("xy z", 1, VARCHAR()),
      });

  EXPECT_EQ(
      generator.partitionName(0, partitionValues, false),
      "name=a%2Fb%3Dc/ts_month=2021-01/id_bucket=7/note_trunc=xy+z");
}

TEST_F(IcebergPartitionNameTest, generatesPartitionPathForNullAndTemporal) {
  auto spec = makeSpec({
      {"ds", DATE(), TransformType::kDay, std::nullopt},
      {"ts", TIMESTAMP(), TransformType::kHour, std::nullopt},
      {"payload", VARCHAR(), TransformType::kIdentity, std::nullopt},
  });
  auto generator = IcebergPartitionName(spec);

  auto partitionValues = makePartitionValues(
      spec,
      {
          makeConstant<int32_t>(std::optional<int32_t>{}, 1, DATE()),
          makeConstant<int32_t>(1, 1, INTEGER()),
          makeConstant<std::string>(std::optional<std::string>{}, 1, VARCHAR()),
      });

  EXPECT_EQ(
      generator.partitionName(0, partitionValues, false),
      "ds_day=null/ts_hour=1970-01-01-01/payload=null");
}

} // namespace
} // namespace bytedance::bolt::connector::hive::iceberg
