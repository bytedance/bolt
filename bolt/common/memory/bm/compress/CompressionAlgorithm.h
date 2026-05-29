#pragma once

#include "bolt/common/memory/bm/compress/CompressionConfig.h"

#include <cstddef>
#include <cstdint>

namespace bytedance::bolt::memory::bm::compress {

struct CompressionAlgorithmContext {
  void* lz4Context{nullptr};
  void* zstdContext{nullptr};
};

void DestroyCompressionAlgorithmContext(CompressionAlgorithmContext& context);

bool SupportedCompressionKind(CompressionKind kind);
size_t MaxCompressedLength(CompressionKind kind, size_t rawSize);

uint64_t CompressWithAlgorithm(
    CompressionAlgorithmContext* context,
    CompressionKind kind,
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);

void DecompressWithAlgorithm(
    CompressionKind kind,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);

size_t Lz4MaxCompressedLength(size_t rawSize);
uint64_t Lz4Compress(
    CompressionAlgorithmContext* context,
    CompressionKind kind,
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);
void Lz4Decompress(
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);

void DestroyLz4Context(void* context);

size_t ZstdMaxCompressedLength(size_t rawSize);
uint64_t ZstdCompress(
    CompressionAlgorithmContext* context,
    CompressionKind kind,
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);
void ZstdDecompress(
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);
void DestroyZstdContext(void* context);

size_t SnappyMaxCompressedLength(size_t rawSize);
uint64_t SnappyCompress(
    CompressionKind kind,
    int compressionLevel,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);
void SnappyDecompress(
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize);

} // namespace bytedance::bolt::memory::bm::compress
