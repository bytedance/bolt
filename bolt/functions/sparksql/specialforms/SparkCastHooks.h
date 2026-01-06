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

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#pragma once
#include <date/tz.h>

#include "bolt/expression/CastHooks.h"
#include "bolt/expression/EvalCtx.h"
#include "bolt/type/tz/TimeZoneMap.h"

namespace bytedance::bolt::functions::sparksql {

// This class provides cast hooks following Spark semantics.
class SparkCastHooks : public exec::CastHooks {
 public:
  explicit SparkCastHooks(const core::QueryConfig& config)
      : CastHooks(),
        legacyCastComplexTypeToString_(
            config.isSparkLegacyCastComplexTypesToStringEnabled() != "false") {
    const auto sessionTzName = config.sessionTimezone();
    if (config.adjustTimestampToTimezone() && !sessionTzName.empty()) {
      sessionTimezone_ = tz::locateZone(sessionTzName);
    } else {
      sessionTimezone_ = nullptr;
    }
  }

  // TODO: Spark hook allows more string patterns than Presto.
  Timestamp castStringToTimestamp(const StringView& view) const override;

  /// 1) Removes all leading and trailing UTF8 white-spaces before cast. 2) Uses
  /// non-standard cast mode to cast from string to date.
  std::optional<int32_t> castStringToDate(
      const StringView& dateString) const override;

  /// 1) Does not follow 'isLegacyCast' config. 2) The conversion precision is
  /// microsecond. 3) Does not append trailing zeros. 4) Adds a positive sign at
  /// first if the year exceeds 9999.
  void castTimestampToString(
      const Timestamp& timestamp,
      exec::StringWriter<false>& out) const override;

  // Returns false.
  bool legacy() const override;

  // Follows 'spark_legacy_cast_complex_type_to_string' config
  bool legacyComplexTypeToString() const override;

  /// When casting from string to integral, floating-point, decimal, date, and
  /// timestamp types, Spark hook trims all leading and trailing UTF8
  /// whitespaces before cast.
  StringView removeWhiteSpaces(const StringView& view) const override;

  // Returns true.
  bool truncate() const override;

 private:
  bool legacyCastComplexTypeToString_;
  const tz::TimeZone* sessionTimezone_;
};
} // namespace bytedance::bolt::functions::sparksql
