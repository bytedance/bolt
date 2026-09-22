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

// Copies a Lance bitmap into a Bolt bitmap. Both formats use the least
// significant bit first within each byte. Bits outside the destination range
// are preserved.
void copyLanceBitmapScalar(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t* target,
    uint64_t targetBitOffset);

void copyLanceBitmap(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t* target,
    uint64_t targetBitOffset);

bool lanceBitmapIsAllSet(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count);

} // namespace bytedance::bolt::lance::reader
