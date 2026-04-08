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

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace bytedance::bolt::connector::hive::iceberg {

namespace {

TransformCategory getTransformCategory(TransformType transformType) {
  switch (transformType) {
    case TransformType::kIdentity:
      return TransformCategory::kIdentity;
    case TransformType::kYear:
    case TransformType::kMonth:
    case TransformType::kDay:
    case TransformType::kHour:
      return TransformCategory::kTemporal;
    case TransformType::kBucket:
      return TransformCategory::kBucket;
    case TransformType::kTruncate:
      return TransformCategory::kTruncate;
  }
  BOLT_UNREACHABLE("Unknown transform type");
}

bool isValidPartitionType(const TypePtr& type) {
  return !(
      type->isRow() || type->isArray() || type->isMap() || type->isDouble() ||
      type->isReal() || isTimestampWithTimeZoneType(type));
}

bool canTransform(TransformType transformType, const TypePtr& type) {
  switch (transformType) {
    case TransformType::kIdentity:
      return type->isTinyint() || type->isSmallint() || type->isInteger() ||
          type->isBigint() || type->isBoolean() || type->isDecimal() ||
          type->isDate() || type->isTimestamp() || type->isVarchar() ||
          type->isVarbinary();
    case TransformType::kYear:
    case TransformType::kMonth:
    case TransformType::kDay:
      return type->isDate() || type->isTimestamp();
    case TransformType::kHour:
      return type->isTimestamp();
    case TransformType::kBucket:
      return type->isInteger() || type->isBigint() || type->isDecimal() ||
          type->isVarchar() || type->isVarbinary() || type->isDate() ||
          type->isTimestamp();
    case TransformType::kTruncate:
      return type->isInteger() || type->isBigint() || type->isDecimal() ||
          type->isVarchar() || type->isVarbinary();
  }
  BOLT_UNREACHABLE("Unsupported partition transform type");
}

const auto& transformTypeNames() {
  static const folly::F14FastMap<TransformType, std::string_view>
      kTransformNames = {
          {TransformType::kIdentity, "identity"},
          {TransformType::kHour, "hour"},
          {TransformType::kDay, "day"},
          {TransformType::kMonth, "month"},
          {TransformType::kYear, "year"},
          {TransformType::kBucket, "bucket"},
          {TransformType::kTruncate, "trunc"},
      };
  return kTransformNames;
}

const auto& transformCategoryNames() {
  static const folly::F14FastMap<TransformCategory, std::string_view>
      kTransformCategoryNames = {
          {TransformCategory::kIdentity, "Identity"},
          {TransformCategory::kBucket, "Bucket"},
          {TransformCategory::kTruncate, "Truncate"},
          {TransformCategory::kTemporal, "Temporal"},
      };
  return kTransformCategoryNames;
}

} // namespace

std::string_view TransformTypeName::toName(TransformType type) {
  return transformTypeNames().at(type);
}

std::string_view TransformCategoryName::toName(TransformCategory category) {
  return transformCategoryNames().at(category);
}

TypePtr IcebergPartitionSpec::Field::resultType() const {
  switch (transformType) {
    case TransformType::kBucket:
    case TransformType::kYear:
    case TransformType::kMonth:
    case TransformType::kHour:
      return INTEGER();
    case TransformType::kDay:
      return DATE();
    case TransformType::kIdentity:
    case TransformType::kTruncate:
      return type;
  }
  BOLT_UNREACHABLE("Unknown transform type");
}

void IcebergPartitionSpec::checkCompatibility() const {
  folly::F14FastMap<std::string_view, std::vector<TransformType>>
      columnTransforms;

  for (const auto& field : fields) {
    BOLT_USER_CHECK(
        isValidPartitionType(field.type),
        "Type is not supported as a partition column: {}",
        field.type->name());

    BOLT_USER_CHECK(
        canTransform(field.transformType, field.type),
        "Transform is not supported for partition column. Column: '{}', Type: '{}', Transform: '{}'.",
        field.name,
        field.type->name(),
        TransformTypeName::toName(field.transformType));

    columnTransforms[field.name].push_back(field.transformType);
  }

  std::vector<std::string> errors;
  for (const auto& [columnName, transforms] : columnTransforms) {
    folly::F14FastSet<TransformCategory> seenCategories;
    for (const auto transform : transforms) {
      const auto category = getTransformCategory(transform);
      if (!seenCategories.insert(category).second) {
        std::vector<std::string> names;
        names.reserve(transforms.size());
        for (const auto item : transforms) {
          names.emplace_back(TransformTypeName::toName(item));
        }
        errors.emplace_back(fmt::format(
            "Column: '{}', Category: {}, Transforms: [{}]",
            columnName,
            TransformCategoryName::toName(category),
            folly::join(", ", names)));
        break;
      }
    }
  }

  BOLT_USER_CHECK(
      errors.empty(),
      "Multiple transforms of the same category on a column are not allowed. "
      "Each transform category can appear at most once per column. {}",
      folly::join("; ", errors));
}

} // namespace bytedance::bolt::connector::hive::iceberg
