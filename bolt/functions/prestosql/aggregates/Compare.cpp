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

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/functions/prestosql/aggregates/Compare.h"
using namespace bytedance::bolt::functions::aggregate;
namespace bytedance::bolt::aggregate::prestosql {

int32_t compare(
    const SingleValueAccumulator* accumulator,
    const DecodedVector& decoded,
    vector_size_t index) {
  static const CompareFlags kCompareFlags{
      true, // nullsFirst
      true, // ascending
      false, // equalsOnly
      ::bytedance::bolt::kSparkCompatible
          ? CompareFlags::NullHandlingMode::kNullAsValue
          : CompareFlags::NullHandlingMode::kNullAsIndeterminate,
      ::bytedance::bolt::kSparkCompatible,
  };

  auto result = accumulator->compare(decoded, index, kCompareFlags);
  BOLT_USER_CHECK(
      result.has_value(),
      fmt::format(
          "{} comparison not supported for values that contain nulls",
          mapTypeKindToName(decoded.base()->typeKind())));
  return result.value();
}
} // namespace bytedance::bolt::aggregate::prestosql
