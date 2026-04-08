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

#pragma once

#include <folly/Conv.h>

#include "bolt/connectors/hive/iceberg/PartitionSpec.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::connector::hive::iceberg {

/// Generates Iceberg-compliant partition path names.
/// Converts partition keys to human-readable strings based on their transform
/// types (e.g., year, month, day, hour, identity, truncate) and constructs
/// URL-encoded partition paths in the format "key1=value1/key2=value2/...".
class IcebergPartitionName {
 public:
  /// @param partitionSpec Iceberg partition specification containing transform
  /// definitions for each partition field. Used to get transform type and call
  /// different format functions to convert transformed partition values to
  /// human-readable strings.
  IcebergPartitionName(const IcebergPartitionSpecPtr& partitionSpec);

  /// Generates an Iceberg compliant partition path string for the given
  /// partition ID.
  ///
  /// Constructs a partition path in the format "key1=value1/key2=value2/..."
  /// where:
  /// - Keys are partition column names for identity transforms, or
  ///   "columnName_transformName" for non-identity transforms.
  /// - Values are human-readable string representations of transformed
  ///   partition keys, formatted according to their transform types.
  /// - Both keys and values are URL-encoded per java.net.URLEncoder.encode().
  ///
  /// Example: "store_id=123/date_year=2025/address_bucket=1"
  std::string partitionName(
      uint32_t partitionId,
      const RowVectorPtr& partitionValues,
      bool partitionKeyAsLowerCase) const;

  /// Generic template for formatting simple types that just need string
  /// conversion. Specialized for types that need special handling.
  template <typename T>
  FOLLY_ALWAYS_INLINE static std::string
  toName(T value, const TypePtr& /*type*/, TransformType /*transformType*/) {
    return folly::to<std::string>(value);
  }

  /// Converts a boolean partition key to its Iceberg string representation.
  static std::string
  toName(bool value, const TypePtr& type, TransformType transformType);

  /// Converts an int32_t partition key to its string representation based on
  /// the transform type:
  /// - kIdentity: For DATE type return "YYYY-MM-DD" format.
  /// - kDay: Returns date in "YYYY-MM-DD" format.
  /// - kYear: Returns 4-digit year "YYYY".
  /// - kMonth: Returns "YYYY-MM" format.
  /// - kHour: Returns "YYYY-MM-DD-HH" format.
  static std::string
  toName(int32_t value, const TypePtr& type, TransformType transformType);

  /// Converts an int64_t partition key to its Iceberg string representation.
  static std::string
  toName(int64_t value, const TypePtr& type, TransformType transformType);

  /// Converts a decimal partition key using Iceberg's unscaled decimal text
  /// form expected in partition paths.
  static std::string
  toName(int128_t value, const TypePtr& type, TransformType transformType);

  /// Returns timestamp formatted with Iceberg-compatible millisecond precision.
  static std::string
  toName(Timestamp value, const TypePtr& type, TransformType transformType);

  /// Converts a StringView partition key to its string representation.
  /// - For VARBINARY type returns Base64-encoded string.
  /// - For VARCHAR type returns the string value as-is.
  static std::string
  toName(StringView value, const TypePtr& type, TransformType transformType);

 private:
  std::vector<TransformType> transformTypes_;
};

using IcebergPartitionNamePtr = std::shared_ptr<const IcebergPartitionName>;

} // namespace bytedance::bolt::connector::hive::iceberg
