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
 */

#include "bolt/connectors/hive/HivePartitionName.h"

#include "bolt/dwio/catalog/fbhive/FileUtils.h"
#include "bolt/type/DecimalUtil.h"

namespace bytedance::bolt::connector::hive {

using namespace bytedance::bolt::dwio::catalog::fbhive;

std::string HivePartitionName::toName(int32_t value, const TypePtr& type) {
  if (type->isDate()) {
    return DATE()->toString(value);
  }
  return fmt::to_string(value);
}

std::string HivePartitionName::toName(int64_t value, const TypePtr& type) {
  if (type->isDecimal()) {
    return DecimalUtil::toString(value, type);
  }
  return fmt::to_string(value);
}

std::string HivePartitionName::toName(int128_t value, const TypePtr& type) {
  if (type->isDecimal()) {
    return DecimalUtil::toString(value, type);
  }
  return fmt::to_string(value);
}

std::string HivePartitionName::toName(Timestamp value, const TypePtr& type) {
  value.toTimezone(Timestamp::defaultTimezone());
  TimestampToStringOptions options;
  options.dateTimeSeparator = ' ';
  options.precision = TimestampToStringOptions::Precision::kMilliseconds;
  options.skipTrailingZeros = true;

  auto result = value.toString(options);
  if (result.find_last_of('.') == std::string::npos) {
    result += ".0";
  }
  return result;
}

std::string HivePartitionName::partitionName(
    uint32_t partitionId,
    const RowVectorPtr& partitionValues,
    bool partitionKeyAsLowerCase) {
  auto toPartitionName =
      [](auto value, const TypePtr& type, int /*columnIndex*/) {
        return HivePartitionName::toName(value, type);
      };
  return FileUtils::makePartName(
      partitionKeyValues(
          partitionId,
          partitionValues,
          /*nullValueString=*/"",
          toPartitionName),
      partitionKeyAsLowerCase);
}

} // namespace bytedance::bolt::connector::hive
