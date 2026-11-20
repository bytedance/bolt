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

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace bytedance::bolt::memory::bm::compress {

size_t SpillRecordHeaderSize();

IoBuffer AllocateSpillRecord(size_t bodyCapacity);

char* SpillRecordBody(IoBuffer& record);

void FinalizeSpillRecord(
    IoBuffer& record,
    CompressionKind storedKind,
    uint64_t rawSize,
    uint64_t storedSize);

IoBuffer AllocateDecodedPayload(MemoryPool* outputPool, uint64_t rawSize);

std::span<const char> StoredPayloadSpan(
    std::span<const char> record,
    size_t headerSize,
    uint64_t storedSize);

} // namespace bytedance::bolt::memory::bm::compress
