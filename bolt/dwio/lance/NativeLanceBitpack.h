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

void decodeLanceBitpackedScalar(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t bitOffset,
    uint64_t count,
    uint8_t compressedBits,
    uint8_t uncompressedBits,
    bool isSigned,
    uint8_t* output);

void decodeLanceBitpacked(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t bitOffset,
    uint64_t count,
    uint8_t compressedBits,
    uint8_t uncompressedBits,
    bool isSigned,
    uint8_t* output);

/// Decodes Lance's FastLanes-style BitpackedForNonNeg encoding. Input must
/// begin at a 1,024-value chunk boundary. 'rowOffset' identifies the first
/// requested value relative to that input buffer.
void decodeLanceBitpackedForNonNeg(
    const uint8_t* input,
    uint64_t inputBytes,
    uint64_t rowOffset,
    uint64_t count,
    uint8_t compressedBits,
    uint8_t uncompressedBits,
    uint8_t* output);

} // namespace bytedance::bolt::lance::reader
