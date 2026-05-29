# BM Spill Compression Usage

`bm/compress` provides block-level compression for BufferManager spill records.
It is designed to be used through `SpillStoreConfig`, so BufferManager callers
do not need to handle compression metadata, headers, or decompression manually.

## Configure BufferManager

Use `BufferManagerConfig::spillStoreConfig.compressionConfig`.

```cpp
#include "bolt/common/memory/bm/BufferManager.h"

using namespace bytedance::bolt::memory::bm;

BufferManagerConfig config;
config.spillStoreConfig.fileAllocatorConfig = fileAllocatorConfig;
config.spillStoreConfig.compressionConfig.kind =
    compress::CompressionKind::kZstdContext;
config.spillStoreConfig.compressionConfig.minCompressBytes = 256 * 1024;
config.spillStoreConfig.compressionConfig.minCompressionRatio = 0.95;
config.spillStoreConfig.compressionConfig.compressionLevel = 3;

auto bm = BufferManager::Create(parentPool, std::move(config));
```

Compression is enabled by default with `compress::CompressionKind::kLz4`.
Blocks smaller than `minCompressBytes` are stored uncompressed. Blocks that do
not reach `minCompressionRatio` are also stored uncompressed.

For example, the default `minCompressionRatio = 0.95` means the compressed
payload must be smaller than 95% of the original payload. Otherwise the original
payload is written.

## Compression Kinds

`bm/compress` uses its own `compress::CompressionKind`. Do not use
`common::CompressionKind` for BM spill compression.

Available kinds:

- `kNone`: disable compression.
- `kLz4`: alias of `kLz4Default`.
- `kLz4Default`: `LZ4_compress_default`.
- `kLz4Fast`: `LZ4_compress_fast`; `compressionLevel` is used as LZ4 acceleration.
- `kLz4Context`: `LZ4_compress_fast_extState` with a reusable `LZ4_stream_t`.
- `kZstd`: alias of `kZstdOneShot`.
- `kZstdOneShot`: `ZSTD_compress`.
- `kZstdContext`: `ZSTD_compressCCtx` with a reusable `ZSTD_CCtx`.
- `kSnappy`: alias of `kSnappyRaw`.
- `kSnappyRaw`: `snappy::RawCompress`.
- `kSnappyLevel`: `snappy::RawCompress` with `snappy::CompressionOptions`.

`compressionLevel` is interpreted by the selected algorithm. For Snappy level
mode, the value is clamped to the range supported by Snappy.

## Context Reuse

`SpillStore` owns a `compress::CompressionCodec` instance. Context-based kinds
reuse their internal compression contexts for the lifetime of that `SpillStore`.
This avoids allocating and freeing ZSTD/LZ4 contexts for every spilled block.

The reusable codec serializes compression calls internally because algorithm
contexts are not thread-safe. Decompression remains stateless and happens when
`SpillReadFuture::get()` is consumed.

## Direct Codec Usage

Most production code should go through BufferManager or SpillStore. Direct codec
usage is mainly useful for tests and focused benchmarks.

```cpp
#include "bolt/common/memory/bm/compress/CompressionCodec.h"

using namespace bytedance::bolt::memory::bm;

compress::CompressionConfig config;
config.kind = compress::CompressionKind::kZstdContext;
config.minCompressBytes = 1;
config.minCompressionRatio = 1.0;

compress::CompressionCodec codec;
auto compressed =
    codec.TryCompress(std::move(payload), config, /*pool=*/nullptr);

auto decompressed = compress::Decompress(
    std::move(compressed.buffer),
    compressed.rawSize,
    compressed.storedSize,
    compressed.storedKind,
    /*pool=*/nullptr,
    /*decompressionTimeUs=*/nullptr);
```

When a `MemoryPool*` is supplied, output buffers are allocated from that pool.
Passing `nullptr` uses heap allocation and is intended for tests or isolated
experiments.

## Spill Record Format

SpillStore writes a small self-describing header before each stored payload.
The header records the raw size, stored size, and actual stored compression
kind. If compression is skipped because of threshold or ratio checks, the
stored kind is `kNone`.

Callers should not parse this header directly. Use `SpillStore::SubmitReadBlock`
or BufferManager pin/prefetch APIs.
