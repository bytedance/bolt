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

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

/// Payload assembly shared by the output backends: byte-buffer helpers, the
/// null region and payload prefix of a window's partition, the stream-major
/// gather of resident cells and the run compression policy.
namespace bytedance::bolt::shuffle::sparksql::cell {

/// Runs below this size are stored: compression would not pay for itself.
constexpr uint64_t kMinCompressRunBytes = 1 << 10;

/// Bytes of a run header: layout byte, stored size, per-stream sizes.
inline uint64_t runHeaderBytes(uint32_t numStreams) {
  return 1 + 8 + 8ull * numStreams;
}

uint64_t currentTimeNs();

/// Sets the size of `buffer`, allocating or growing it from `pool`. Keeps
/// the capacity on clear or shrink, so one workspace serves every partition.
void resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool);

void appendBytes(
    BufferPtr& out,
    const void* data,
    size_t bytes,
    memory::MemoryPool* pool);

void appendLe32(BufferPtr& out, uint32_t value, memory::MemoryPool* pool);

void appendLe64(BufferPtr& out, uint64_t value, memory::MemoryPool* pool);

/// Appends the null body (spec section 4.2) of one partition.
void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    BufferPtr& out,
    memory::MemoryPool* pool);

/// Appends everything of one partition's payload that precedes its runs,
/// for the window in memory: fixed header, uncompressed null region and
/// encoding tags.
void appendPayloadPrefix(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t runCount,
    BufferPtr& out,
    memory::MemoryPool* pool);

/// Bytes of one partition's resident cells, all streams together.
uint64_t residentBytes(const CellWindowInput& in, uint32_t pid);

/// Copies one partition's resident cells stream-major into `out`, which
/// holds residentBytes(); fills the per-stream sizes.
void gatherResident(
    const CellWindowInput& in,
    uint32_t pid,
    char* out,
    uint64_t* streamSizes);

/// Writes a run header at `out`, which holds runHeaderBytes().
void writeRunHeader(
    char* out,
    RunLayout layout,
    uint64_t stored,
    const uint64_t* decodedSizes,
    uint32_t numStreams);

/// The run compression policy: compresses into `scratch` and points
/// `body`/`stored` at the COMBINED form when the codec shrinks the bytes,
/// else at the input (spec section 5: fall back to the stored form when
/// compression does not pay). Adds the codec time to `compressTimeNs`.
/// Returns the layout to declare.
RunLayout maybeCompressRun(
    Codec* codec,
    const char* data,
    uint64_t dataBytes,
    BufferPtr& scratch,
    memory::MemoryPool* pool,
    const char*& body,
    uint64_t& stored,
    uint64_t& compressTimeNs);

} // namespace bytedance::bolt::shuffle::sparksql::cell
