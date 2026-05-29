# BM Spill 压缩抽象调研与改造建议

日期：2026-05-29

本文档总结 Doris、StarRocks、ClickHouse 在压缩抽象上的设计方式，并把调研结论映射到 BufferManager spill 压缩场景。目标是找到一个既方便压测，又能保持落盘格式稳定、并且适合并发 spill 的设计。

## 背景与范围

BM spill 的压缩场景有几个明确特征：

- 输入是完整 block payload。
- 读路径按 spilled block 随机访问。
- 每个 spill record 必须能独立解码。
- 落盘 record 格式应该稳定，不能因为内部实现方式调整就改变语义。
- 只有 block 大小达到阈值，且压缩收益达标时，才应该真正压缩落盘。
- one-shot API、可复用 context、压缩等级等实现策略需要能配置，方便做 benchmark。

这属于 block compression，不是跨多个 spill block 的长生命周期 stream compression。

## 调研对象

### Doris

Doris 有一个面向 block 的 `BlockCompressionCodec` 抽象。接口核心包括：

- `compress(input, output)`
- `compress(vector<input>, uncompressed_size, output)`
- `decompress(input, output)`
- `max_compressed_len(len)`

Doris 代码明确把 block compression 和 stream compression 区分开，并注明 codec 不是线程安全的，不应该跨线程共享一个可变 codec 实例。

在 LZ4 实现上，Doris 维护了内部 compression context pool。每次压缩会借一个 context，使用可复用的 LZ4 stream 和 buffer，压缩完成后再归还 pool。这样既避免了每个 block 都创建/释放 context，也不会把所有压缩都串行化到一个全局 mutex 后面。

相关源码：

- Doris block compression interface: https://github.com/apache/doris/blob/477cb0c6bbaad03e7e65b4589560a3b8a1bf659a/be/src/util/block_compression.h
- Doris LZ4 context reuse: https://github.com/apache/doris/blob/477cb0c6bbaad03e7e65b4589560a3b8a1bf659a/be/src/util/block_compression.cpp

### StarRocks

StarRocks 继承了 Doris 风格的 block codec interface，并进一步把 codec 和 native context 的职责拆得更清楚。

关键点：

- `BlockCompressionCodec` 表示稳定的算法接口。
- `BlockCompressionOptions` 承载每次调用的可调参数，例如 LZ4 acceleration。
- `CompressionContext` 结构体持有 native context，例如 `ZSTD_CCtx`、`ZSTD_DCtx`、`LZ4_stream_t`，以及可复用临时 buffer。
- `CompressionContextPool` 提供 RAII 风格的借还机制。
- `get_block_compression_codec(type, codec, compression_level)` 返回稳定 codec 实例，mutable native state 则由 context pool 管理。

StarRocks 的 ZSTD 实现会从 pool 中借一个 `ZSTD_CCtx`，设置目标 compression level，执行压缩，然后归还 context。解压也使用 `ZSTD_DCtx` pool。这个分层比较适合 BM：codec 身份稳定，native context 生命周期可复用，level/options 不进入落盘类型。

相关源码：

- StarRocks block codec interface: https://github.com/StarRocks/starrocks/blob/58f6213c1303b46ffbfe36ff0c0d69afdcb80746/be/src/base/compression/block_compression.h
- StarRocks compression contexts: https://github.com/StarRocks/starrocks/blob/58f6213c1303b46ffbfe36ff0c0d69afdcb80746/be/src/base/compression/compression_context.h
- StarRocks context pool: https://github.com/StarRocks/starrocks/blob/58f6213c1303b46ffbfe36ff0c0d69afdcb80746/be/src/base/compression/compression_context_pool.h
- StarRocks ZSTD/LZ4 implementations: https://github.com/StarRocks/starrocks/blob/58f6213c1303b46ffbfe36ff0c0d69afdcb80746/be/src/base/compression/block_compression.cpp

### ClickHouse

ClickHouse 通过 codec interface 和 factory 暴露压缩能力。codec 是存储元数据的一部分，也可以组合成 pipeline，例如先做数据变换 codec，再做压缩 codec。BM 不需要完整照搬 ClickHouse 的 pipeline 模型，但它给出的核心启发是：稳定 codec identity 和实现细节应该分离。

对应到 BM，spill record header 应该记录稳定的算法格式，例如 ZSTD frame 或 LZ4 block，而不应该记录写入时使用的是 one-shot API 还是 reusable context。one-shot 和 context 只是 writer 的实现策略，通常产出的可解码格式是同一种。

相关源码和文档：

- ClickHouse compression codec interface: https://github.com/ClickHouse/ClickHouse/blob/master/src/Compression/ICompressionCodec.h
- ClickHouse compression factory: https://github.com/ClickHouse/ClickHouse/blob/master/src/Compression/CompressionFactory.h
- ClickHouse codec overview: https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema

## 调研结论

### 1. 稳定落盘格式和运行时实现策略应该分离

当前 BM 实现把 `kZstdOneShot`、`kZstdContext`、`kLz4Fast`、`kLz4Context` 等变体都放进了 `CompressionKind`。这对 benchmark 切换很方便，但混淆了两个概念：

- 落盘压缩格式：reader 解码 bytes 时真正需要知道的信息。
- 运行时策略：writer 当时是怎么生产这些 bytes 的。

例如 `ZSTD_compress` 和 `ZSTD_compressCCtx` 产出的都是 ZSTD payload。reader 只需要知道这是 ZSTD。把 `kZstdContext` 写入 spill header，会把 writer 的实现细节泄漏到 record format 里，后续调整实现方式会看起来像一次格式变更。

### 2. 单个可复用 codec 加 mutex 不是理想并发模型

当前 `CompressionCodec` 内部保存 native context，并用 mutex 串行化压缩调用。这样确实避免了每个 block 都分配 context，但也会让同一个 `SpillStore` 的并发压缩排队。

Doris 和 StarRocks 的模式更适合：

- mutable native state 放到 context object。
- 每次操作借一个 context。
- 操作结束后 reset 并归还 pool。
- 如果 pool 中有多个 context，多个 spill 压缩可以并发执行。

这样既能复用 context，又不会不必要地全局串行化。

### 3. codec 应该是 block-oriented 且尽量无状态

BM spill 的 codec 应该只关心算法本身：

- 最大压缩长度。
- 压缩一个 block。
- 解压一个 block。
- 返回稳定算法格式。

它不应该负责 `minCompressBytes`、`minCompressionRatio` 这类 spill policy。阈值、收益判断和 fallback 应该放在 codec 上层。

### 4. 压缩尝试和 fallback 应该由 policy/manager 负责

当前 `TryCompress` 同时做了：

- 算法选择。
- native compression 调用。
- buffer allocation。
- 耗时统计。
- size threshold 判断。
- ratio threshold 判断。
- fallback 到原始 payload。

第一版这样写能跑通，但算法实现和策略逻辑耦合太紧。更好的方式是 raw algorithm 只做算法，`CompressionManager` 或 `SpillCompressionPolicy` 负责是否尝试压缩、收益是否达标、以及最终落盘 `storedKind`。

### 5. context reuse 应该是 strategy/pool policy，不应该编码为格式

ZSTD 和 LZ4 都可以从 context reuse 中受益。但 context reuse 应该是运行时 strategy，而不是落盘 `CompressionKind`。

spill header 应该记录 `kZstdFrame`；运行时可以根据配置选择 one-shot 或 pooled context。

### 6. Snappy 的可选模式更少

Snappy 没有 ZSTD/LZ4 那种 reusable compression context 模型。它可以有 raw API 和带 options 的 API，但除非 bytes 格式真的变化，否则不应该新增多个 persisted format kind。

## 当前 BM 实现的问题

当前实现状态：

- `compress::CompressionKind` 同时包含格式和实现模式。
- `CompressionCodec` 持有 reusable native contexts。
- `CompressionCodec` 用 mutex 保护这些 contexts。
- `SpillStore` 持有一个 `CompressionCodec`。
- spill record header 写入 `compressed.storedKind`。

主要问题：

- header 可能记录 `kZstdContext` 这类实现模式。
- 并发 spill 压缩会被 codec mutex 串行化。
- 算法实现、policy 判断、allocation、耗时统计和 fallback 都集中在 `CompressionCodec.cpp`。
- 新增一个运行时模式会牵涉 format enum 和 reader validation，即使产出的 bytes 格式没有变化。
- benchmark 切换虽然方便，但生产语义不够干净。

## 推荐目标设计

### 对外配置

建议把稳定格式和运行策略拆成两个字段。

```cpp
namespace bytedance::bolt::memory::bm::compress {

enum class CompressionKind : uint32_t {
  kNone = 0,
  kLz4Block = 1,
  kZstdFrame = 2,
  kSnappyRaw = 3,
};

enum class CompressionStrategy : uint8_t {
  kDefault,
  kOneShot,
  kPooledContext,
  kFast,
};

struct CompressionConfig {
  CompressionKind kind{CompressionKind::kLz4Block};
  CompressionStrategy strategy{CompressionStrategy::kDefault};
  size_t minCompressBytes{256 * 1024};
  double minCompressionRatio{0.95};
  int compressionLevel{3};
};

} // namespace bytedance::bolt::memory::bm::compress
```

规则：

- `kind` 是 spill record header 里允许出现的稳定格式。
- `strategy` 不落盘，只影响当前进程写入时怎么压缩。
- `compressionLevel` 由具体算法解释。
- `minCompressBytes` 和 `minCompressionRatio` 属于 policy gate。

### 算法接口

建议引入窄接口 `BlockCompressionCodec`。

```cpp
struct CompressionOptions {
  CompressionStrategy strategy{CompressionStrategy::kDefault};
  int compressionLevel{3};
};

class BlockCompressionCodec {
 public:
  virtual ~BlockCompressionCodec() = default;

  virtual CompressionKind kind() const = 0;
  virtual size_t MaxCompressedLength(size_t rawSize) const = 0;

  virtual uint64_t Compress(
      std::span<const char> input,
      std::span<char> output,
      const CompressionOptions& options,
      CompressionContextPoolSet& pools) const = 0;

  virtual void Decompress(
      std::span<const char> input,
      std::span<char> output,
      CompressionContextPoolSet& pools) const = 0;
};
```

具体算法仍然一个文件一个实现：

- `Lz4BlockCompression.cpp`
- `ZstdBlockCompression.cpp`
- `SnappyBlockCompression.cpp`

各算法自己解释 `CompressionStrategy`：

- LZ4:
  - `kDefault`: `LZ4_compress_default`
  - `kFast`: `LZ4_compress_fast`
  - `kPooledContext`: 借用 `LZ4_stream_t`
- ZSTD:
  - `kDefault` / `kOneShot`: `ZSTD_compress`
  - `kPooledContext`: 借用 `ZSTD_CCtx`
- Snappy:
  - `kDefault` / `kOneShot`: `snappy::RawCompress`
  - `kFast` 等 unsupported strategy 可以映射到 default，也可以在 config validation 阶段报错。建议先严格报错，避免 benchmark 结果误读。

### Context Pool

建议新增一个 BM-local 的 context pool，不引入额外并发队列依赖，第一版用 mutex + vector 就够。

```cpp
template <typename T>
class CompressionContextPool {
 public:
  class Ref {
   public:
    Ref() = default;
    Ref(T* context, CompressionContextPool* pool);
    ~Ref();

    Ref(const Ref&) = delete;
    Ref& operator=(const Ref&) = delete;
    Ref(Ref&&) noexcept;
    Ref& operator=(Ref&&) noexcept;

    T* get() const;

   private:
    T* context_{nullptr};
    CompressionContextPool* pool_{nullptr};
  };

  Ref Acquire();

 private:
  void Release(T* context);
  std::mutex mutex_;
  std::vector<std::unique_ptr<T>> idle_;
};
```

Context object 示例：

```cpp
struct Lz4CompressionContext {
  LZ4_stream_t* stream{nullptr};
  IoBuffer scratch;
};

struct ZstdCompressionContext {
  ZSTD_CCtx* cctx{nullptr};
  ZSTD_DCtx* dctx{nullptr};
  IoBuffer scratch;
};
```

reset 规则：

- LZ4 context 归还 pool 前需要 reset stream state。
- ZSTD context 归还 pool 前需要 reset 或恢复默认参数。
- 如果压缩/解压失败，可以直接丢弃该 context，不归还 pool。

### CompressionManager

把 policy 从 raw codec 中拆出来。

```cpp
class CompressionManager {
 public:
  explicit CompressionManager(CompressionConfig config);

  CompressResult TryCompress(IoBuffer payload, MemoryPool* pool);

  IoBuffer Decompress(
      IoBuffer storedPayload,
      uint64_t rawSize,
      uint64_t storedSize,
      CompressionKind storedKind,
      MemoryPool* pool,
      uint64_t* decompressionTimeUs);

 private:
  const BlockCompressionCodec* GetCodec(CompressionKind kind) const;

  CompressionConfig config_;
  CompressionContextPoolSet pools_;
};
```

`CompressionManager::TryCompress` 流程：

1. 如果 `kind == kNone`，直接返回原始 payload。
2. 如果 payload 小于 `minCompressBytes`，直接返回原始 payload。
3. 根据稳定 `kind` 获取 codec。
4. 用 `codec.MaxCompressedLength(rawSize)` 分配输出 buffer。
5. 调用 `codec.Compress(...)`。
6. 用实际 compressed size 和 `minCompressionRatio` 比较收益。
7. 如果收益不达标，返回原始 payload，`storedKind = kNone`。
8. 如果收益达标，返回压缩 payload，`storedKind = config.kind`。

### Spill Header

header 只记录稳定格式。

```cpp
struct SpillRecordHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t headerSize;
  uint32_t compressionKind; // 只允许稳定 CompressionKind
  uint32_t reserved;
  uint64_t rawSize;
  uint64_t storedSize;
};
```

允许写入 header 的值：

- `kNone`
- `kLz4Block`
- `kZstdFrame`
- `kSnappyRaw`

不应该写入 header 的信息：

- one-shot vs pooled context
- LZ4 acceleration
- ZSTD level
- compression ratio threshold
- 任何 implementation-specific mode

### SpillStore 集成

`SpillStore` 应该持有 `CompressionManager`。

```cpp
class SpillStore {
 private:
  SpillStoreConfig config_;
  compress::CompressionManager compression_;
  std::shared_ptr<FileBlockAllocator> allocator_;
  MemoryPool* pool_{nullptr};
};
```

BM 仍然对压缩无感，只看到：

- `WriteBlock`
- `SubmitReadBlock`
- `SpillWriteResult`
- `SpillReadResult`

## 推荐迁移计划

### Step 1: 收敛 CompressionKind

把当前混合 enum：

- `kLz4`, `kLz4Default`, `kLz4Fast`, `kLz4Context`
- `kZstd`, `kZstdOneShot`, `kZstdContext`
- `kSnappy`, `kSnappyRaw`, `kSnappyLevel`

替换为稳定格式：

- `kNone`
- `kLz4Block`
- `kZstdFrame`
- `kSnappyRaw`

同时新增 `CompressionStrategy`。

兼容性说明：BM spill compression 是新功能，spill 文件本身也不是跨进程长期存储格式，因此可以直接迁移代码，不需要为旧 enum 做兼容 shim。

### Step 2: 拆分 policy 和 algorithm

新增：

- `CompressionManager.h/.cpp`
- `BlockCompressionCodec.h`
- `Lz4BlockCompression.cpp`
- `ZstdBlockCompression.cpp`
- `SnappyBlockCompression.cpp`

把 `minCompressBytes` 和 `minCompressionRatio` 逻辑移入 `CompressionManager`。

### Step 3: 增加 Context Pool

用 per-algorithm context pool 替换当前 `CompressionCodec` 内部的 mutex-protected context。

第一版可以很简单：

- 一个 mutex。
- 一个 idle context vector。
- RAII ref 析构时归还 context。
- 失败 context 直接丢弃。

这样能获得关键并发收益，不需要引入额外队列依赖。

### Step 4: 更新 SpillStore

让 `SpillStore` 持有 `CompressionManager`，不再直接持有 `CompressionCodec`。

写路径：

- `compression_.TryCompress(...)`
- header 写稳定 `storedKind`
- record payload 格式保持 `[header][stored payload]`

读路径：

- decode header
- `compression_.Decompress(...)`

### Step 5: 更新测试

UT 应覆盖：

- 每个稳定 `CompressionKind` 都能 round trip。
- 每个支持的 `CompressionStrategy` 都能 round trip。
- `kZstdFrame + kOneShot` 和 `kZstdFrame + kPooledContext` 都写入 `kZstdFrame`。
- `kLz4Block + kFast` 和 `kLz4Block + kPooledContext` 都写入 `kLz4Block`。
- 小于阈值的 payload 写入 `kNone`。
- 压缩收益不足的 payload 写入 `kNone`。
- header 拒绝未知稳定 kind。
- context pool 能跨 block 复用 context。
- 并发压缩不会因为单个 codec mutex 被全部串行化。

### Step 6: 更新 Benchmark

benchmark 参数建议拆成：

- compression kind: none/lz4/zstd/snappy
- compression strategy: default/one_shot/pooled_context/fast
- compression level
- min compress bytes
- min compression ratio

这样 benchmark 输出可以按稳定格式聚合，同时比较不同 strategy 的性能差异。

## 推荐最终 API 形态

配置示例：

```cpp
config.spillStoreConfig.compressionConfig.kind =
    compress::CompressionKind::kZstdFrame;
config.spillStoreConfig.compressionConfig.strategy =
    compress::CompressionStrategy::kPooledContext;
config.spillStoreConfig.compressionConfig.compressionLevel = 3;
config.spillStoreConfig.compressionConfig.minCompressBytes = 256 * 1024;
config.spillStoreConfig.compressionConfig.minCompressionRatio = 0.95;
```

header 示例：

```cpp
header.compressionKind =
    static_cast<uint32_t>(compress::CompressionKind::kZstdFrame);
```

统计建议：

- logical spill bytes
- physical spill write bytes
- physical spill read bytes
- compressed block count
- compression time
- decompression time
- skipped by threshold count
- skipped by ratio count

## 结论

推荐 BM 的压缩设计按以下方向收敛：

1. 压缩仍然放在 `SpillStore` 内部，BM 不感知压缩。
2. spill record header 只记录稳定压缩格式。
3. 使用 `kind + strategy`，不要用 mode-specific kinds。
4. raw algorithm implementation 保持窄职责，并按算法拆文件。
5. 阈值、收益判断和 fallback 放到 manager/policy 层。
6. 用 per-algorithm context pool 替代单个 mutex-protected codec context。
7. 解压在 SpillStore API 层仍保持无状态语义；底层可以选择 pooled native decompression context。

这个方案能保留当前分层目标，同时让压缩模块更容易压测、推理和长期扩展。
