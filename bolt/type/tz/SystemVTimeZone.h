/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

namespace bytedance::bolt::tz {

class TimeZone;

namespace detail {

/// Returns the standard UTC offset and DST savings in seconds for a canonical
/// Java SystemV name, or nullopt for other names.
std::optional<std::pair<int32_t, int32_t>> getSystemVTimeZoneOffsets(
    std::string_view name);

/// Returns an immutable built-in zone for testing independently of system tzdb.
/// The pointer remains valid for the process lifetime. Unknown names return
/// null.
const TimeZone* locateBuiltinSystemVZone(std::string_view name);

} // namespace detail
} // namespace bytedance::bolt::tz
