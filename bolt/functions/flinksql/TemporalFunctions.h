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

#pragma once

#include <string>

namespace bytedance::bolt::functions::flinksql {

/// Registers Flink Java 8 temporal functions (with an optional name prefix):
/// - to_timestamp(text [, format]).
/// - to_timestamp(text, format, defaultJvmTimeZone,
/// defaultJvmRawOffsetSeconds).
///   The shorter forms use UTC for the legacy parsing fallback; the
///   four-argument form uses the supplied constant JVM zone and raw offset.
/// - flink_string_to_timestamp(text, nullOnFailure [, defaultJvmTimeZone,
///   defaultJvmRawOffsetSeconds]).
///   The two-argument form uses UTC for Java's legacy parsing fallback. Pass
///   the resolved JVM default zone and raw offset explicitly when it differs.
///   The boolean controls parse failures, not Flink's MySQL zero-date option.
/// - flink_timestamp_precision(timestamp, precision), with precision in [0, 9].
/// - flink_timestamp_to_utc(timestamp, zone) and flink_timestamp_from_utc(...).
/// - flink_time_to_timestamp(milliseconds).
/// - flink_timestamp_to_time(timestamp [, zone]). The one-argument form uses
///   Flink NTZ epoch remainder; the zone form extracts LTZ local clock time.
///
/// Time zones and the default raw offset must be constant. Flink callers must
/// resolve the actual Java TimeZone with toZoneId().getId() before passing it,
/// preserving Java's short-ID mappings and fallback behavior. For the legacy
/// default zone, GMT offsets beyond ZoneId's 18-hour limit use the normalized
/// GMT offset ID instead. Custom Java TimeZone rules that cannot be represented
/// by an IANA or fixed-offset ID require Java evaluation.
/// Regional conversions use Bolt's shared time-zone implementation and inherit
/// its transition-range limitations. Deployments must align the time-zone data
/// with the JVM's rules. TIME uses milliseconds.
void registerTemporalFunctions(const std::string& prefix);

} // namespace bytedance::bolt::functions::flinksql
