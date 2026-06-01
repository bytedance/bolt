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
    compress::CompressionKind::kZstdFrame;
config.spillStoreConfig.compressionConfig.minCompressBytes = 256 * 1024;
config.spillStoreConfig.compressionConfig.zstd.strategy =
    compress::ZstdStrategy::kPooledContext;
config.spillStoreConfig.compressionConfig.zstd.compressionLevel = 3;

auto bm = BufferManager::Create(parentPool, std::move(config));
```

Compression is enabled by default with `compress::CompressionKind::kLz4Block`.
Blocks smaller than `minCompressBytes` are stored uncompressed. First-version
BM spill compression does not perform compression-ratio probing; if compression
is attempted and succeeds, the compressed payload is written.

## Compression Kinds

`bm/compress` uses its own `compress::CompressionKind`. Do not use
`common::CompressionKind` for BM spill compression.

Available stable record formats:

- `kNone`: store the original payload.
- `kLz4Block`: LZ4 block payload.
- `kZstdFrame`: ZSTD frame payload.
- `kSnappyRaw`: Snappy raw payload.

The spill record header stores only the stable `CompressionKind`. Writer-side
strategy and level options are runtime policy and are not persisted.

## Algorithm Options

LZ4 options:

- `Lz4Strategy::kDefault`: `LZ4_compress_default`.
- `Lz4Strategy::kFast`: `LZ4_compress_fast`; `acceleration` is used as the LZ4
  acceleration value.
- `Lz4Strategy::kPooledContext`: `LZ4_compress_fast_extState` with a pooled
  `LZ4_stream_t`.

ZSTD options:

- `ZstdStrategy::kOneShot`: `ZSTD_compress`.
- `ZstdStrategy::kPooledContext`: `ZSTD_compressCCtx` with a pooled
  `ZSTD_CCtx`.

Snappy options:

- `SnappyStrategy::kRaw`: `snappy::RawCompress`.
- `SnappyStrategy::kWithOptions`: `snappy::RawCompress` with
  `snappy::CompressionOptions`; `compressionLevel` is clamped by Snappy.

## Buffer Ownership

Spill write records are built inside `CompressionManager` as malloc-backed
`IoBuffer`s. Spill raw read records are also malloc-backed. Decoded payloads in
`SpillStore` are allocated from the BufferManager `MemoryPool`, so the resident
block payload does not need an extra transfer after decompression.

## Direct Manager Usage

Most production code should go through BufferManager or SpillStore. Direct
manager usage is mainly useful for tests and focused benchmarks.

```cpp
#include "bolt/common/memory/bm/compress/CompressionCodec.h"

using namespace bytedance::bolt::memory::bm;

compress::CompressionConfig config;
config.kind = compress::CompressionKind::kZstdFrame;
config.minCompressBytes = 1;
config.zstd.strategy = compress::ZstdStrategy::kPooledContext;

compress::CompressionManager compression(config);
auto record = compression.BuildSpillRecord(
    std::span<const char>(payload.data(), payload.length()));

auto decoded = compression.DecodeSpillRecord(
    std::span<const char>(record.record.data(), record.record.length()),
    record.rawSize,
    outputPool,
    /*decompressionTimeUs=*/nullptr);
```

## Spill Record Format

SpillStore writes a small self-describing header before each stored payload.
The header records the raw size, stored size, and stable stored compression
kind. If compression is disabled or skipped because the payload is below
`minCompressBytes`, the stored kind is `kNone`.

Callers should not parse this header directly. Use `SpillStore::SubmitReadBlock`
or BufferManager pin/prefetch APIs.
