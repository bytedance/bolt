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

namespace bytedance::bolt::row {

// On-wire row format selectable by the row-based shuffle. Values are stable
// (persisted via out-of-band config; the writer and reader must agree, like the
// existing row-based-vs-columnar decision). DenseRow is the bolt-native
// null-fused/varint format; Compact is the Velox-derived CompactRow, used
// exactly as elsewhere in the codebase (no behavior change in Compact mode).
enum class RowFormat : uint8_t {
  Dense = 0,
  Compact = 1,
};

} // namespace bytedance::bolt::row
