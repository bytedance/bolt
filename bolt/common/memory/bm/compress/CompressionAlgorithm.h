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
