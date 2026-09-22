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

namespace bytedance::bolt::lance::reader {

struct NativeLancePackedStructColumn {
  uint64_t offset;
  uint64_t width;
  uint8_t* output;
};

void decodeLancePackedStructScalar(
    const uint8_t* packedRows,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns,
    uint32_t columnCount);

void decodeLancePackedStructAvx2(
    const uint8_t* packedRows,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns,
    uint32_t columnCount);

void decodeLancePackedStruct(
    const uint8_t* packedRows,
    uint64_t rowCount,
    uint64_t rowWidth,
    const NativeLancePackedStructColumn* columns,
    uint32_t columnCount);

} // namespace bytedance::bolt::lance::reader
