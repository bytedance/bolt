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

#include "bolt/common/encode/Base64.h"
#include "bolt/connectors/hive/HivePartitionName.h"
#include "bolt/dwio/catalog/fbhive/FileUtils.h"
#include "bolt/functions/prestosql/URLFunctions.h"
#include "bolt/type/DecimalUtil.h"

namespace bytedance::bolt::connector::hive::iceberg {

namespace {

std::string escapePathName(const std::string& name) {
  std::string encoded;
  encoded.resize(name.size() * 9);
  functions::urlEscape(encoded, name);
  return encoded;
}

std::string formatTemporalMonth(int32_t value) {
  constexpr int32_t kEpochYear = 1970;
  int32_t year = kEpochYear + value / 12;
  int32_t month = 1 + value % 12;
  if (month <= 0) {
    month += 12;
    --year;
  }
  return fmt::format("{:04d}-{:02d}", year, month);
}

std::string formatTemporalHour(int32_t value) {
  const int64_t seconds = static_cast<int64_t>(value) * 3600;
  std::tm tmValue;
  BOLT_USER_CHECK(
      Timestamp::epochToCalendarUtc(seconds, tmValue),
      "Failed to convert seconds to time: {}",
      seconds);
  return fmt::format(
      "{:04d}-{:02d}-{:02d}-{:02d}",
      tmValue.tm_year + 1900,
      tmValue.tm_mon + 1,
      tmValue.tm_mday,
      tmValue.tm_hour);
}

} // namespace

IcebergPartitionName::IcebergPartitionName(
    const IcebergPartitionSpecPtr& partitionSpec) {
  BOLT_CHECK_NOT_NULL(partitionSpec);
  transformTypes_.reserve(partitionSpec->fields.size());
  for (const auto& field : partitionSpec->fields) {
    transformTypes_.emplace_back(field.transformType);
  }
}

std::string IcebergPartitionName::partitionName(
    uint32_t partitionId,
    const RowVectorPtr& partitionValues,
    bool partitionKeyAsLowerCase) const {
  auto toPartitionName = [this](
                             auto value, const TypePtr& type, int columnIndex) {
    return IcebergPartitionName::toName(
        value, type, transformTypes_[columnIndex]);
  };

  return dwio::catalog::fbhive::FileUtils::makePartName(
      HivePartitionName::partitionKeyValues(
          partitionId,
          partitionValues,
          /*nullValueString=*/"null",
          toPartitionName),
      partitionKeyAsLowerCase,
      /*useDefaultPartitionValue=*/false,
      escapePathName);
}

std::string IcebergPartitionName::toName(
    bool value,
    const TypePtr& /*type*/,
    TransformType /*transformType*/) {
  return value ? "true" : "false";
}

std::string IcebergPartitionName::toName(
    int32_t value,
    const TypePtr& type,
    TransformType transformType) {
  constexpr int32_t kEpochYear = 1970;
  switch (transformType) {
    case TransformType::kIdentity:
      if (type->isDate()) {
        return DATE()->toString(value);
      }
      return folly::to<std::string>(value);
    case TransformType::kDay:
      return DATE()->toString(value);
    case TransformType::kYear:
      return fmt::format("{:04d}", kEpochYear + value);
    case TransformType::kMonth:
      return formatTemporalMonth(value);
    case TransformType::kHour:
      return formatTemporalHour(value);
    case TransformType::kBucket:
    case TransformType::kTruncate:
      return folly::to<std::string>(value);
  }
  BOLT_UNREACHABLE("Unknown transform type");
}

std::string IcebergPartitionName::toName(
    int64_t value,
    const TypePtr& type,
    TransformType /*transformType*/) {
  if (type->isDecimal()) {
    return DecimalUtil::toString(value, type);
  }
  return folly::to<std::string>(value);
}

std::string IcebergPartitionName::toName(
    int128_t value,
    const TypePtr& type,
    TransformType /*transformType*/) {
  if (type->isDecimal()) {
    return DecimalUtil::toString(value, type);
  }
  return folly::to<std::string>(value);
}

std::string IcebergPartitionName::toName(
    Timestamp value,
    const TypePtr& /*type*/,
    TransformType transformType) {
  BOLT_CHECK(transformType == TransformType::kIdentity);
  TimestampToStringOptions options;
  options.precision = TimestampToStringOptions::Precision::kMilliseconds;
  options.zeroPaddingYear = true;
  options.skipTrailingZeros = true;
  options.leadingPositiveSign = true;
  options.dateTimeSeparator = 'T';
  return value.toString(options);
}

std::string IcebergPartitionName::toName(
    StringView value,
    const TypePtr& type,
    TransformType transformType) {
  BOLT_CHECK(
      transformType == TransformType::kIdentity ||
      transformType == TransformType::kTruncate);
  if (type->isVarbinary()) {
    return encoding::Base64::encode(value.data(), value.size());
  }
  return value.str();
}

} // namespace bytedance::bolt::connector::hive::iceberg
