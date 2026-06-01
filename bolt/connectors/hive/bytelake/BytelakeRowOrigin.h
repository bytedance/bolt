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

#include <cstdint>

namespace bytedance::bolt::connector::hive {

// Compact 8-byte (fileIdx, rowInFile) identifier for a row within a bucket.
// Both fields are uint32_t — gives ~4B headroom on each dimension with no
// padding waste in the struct layout.
struct BytelakeRowOrigin {
  uint32_t fileIdx;
  uint32_t rowInFile;
};

static_assert(
    sizeof(BytelakeRowOrigin) == 8,
    "BytelakeRowOrigin must be 8 bytes (uint32_t + uint32_t)");

} // namespace bytedance::bolt::connector::hive
