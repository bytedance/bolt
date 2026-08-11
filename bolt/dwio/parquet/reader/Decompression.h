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

#include "bolt/buffer/Buffer.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"

#include <functional>

namespace bytedance::bolt::parquet {

// Decompress function for all bolt supported codec type,
// Including: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4, LZO, LZ4_RAW
// For BROTLI, the function will throw a BOLT_UNSUPPORTED.
const char* bdCodecDecompression(
    const char* pageData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec);

// bd use both old and new ztsd slice read
const char* bdZstdDecompression(
    const char* pageData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec);

const char* decompressLz4AndLzo(
    const char* compressedData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec);

bool tryDecompressZstdPrefix(
    const char* compressedData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    uint32_t outputQuantum,
    memory::MemoryPool& pool,
    const std::function<bool(const char* data, uint32_t availableSize)>&
        isPrefixReady,
    const char*& prefixData);
} // namespace bytedance::bolt::parquet
