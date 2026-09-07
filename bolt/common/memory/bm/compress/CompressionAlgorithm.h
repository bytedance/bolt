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

#include "bolt/common/memory/bm/compress/CompressionAlgorithmContext.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"

#include <cstddef>
#include <cstdint>

namespace bytedance::bolt::memory::bm::compress {

bool SupportedCompressionKind(CompressionKind kind);
size_t MaxCompressedLength(CompressionKind kind, size_t rawSize);

uint64_t CompressWithAlgorithm(
    const CompressionContextSet& contexts,
    CompressionKind kind,
    const CompressionConfig& config,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);

void DecompressWithAlgorithm(
    const DecompressionContextSet& contexts,
    CompressionKind kind,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);

size_t Lz4MaxCompressedLength(size_t rawSize);
uint64_t Lz4Compress(
    Lz4CompressionContext* context,
    const Lz4Options& options,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);
void Lz4Decompress(
    Lz4DecompressionContext* context,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);

size_t ZstdMaxCompressedLength(size_t rawSize);
uint64_t ZstdCompress(
    ZstdCompressionContext* context,
    const ZstdOptions& options,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);
void ZstdDecompress(
    ZstdDecompressionContext* context,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);
size_t SnappyMaxCompressedLength(size_t rawSize);
uint64_t SnappyCompress(
    const SnappyOptions& options,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);
void SnappyDecompress(
    SnappyDecompressionContext* context,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);

} // namespace bytedance::bolt::memory::bm::compress
