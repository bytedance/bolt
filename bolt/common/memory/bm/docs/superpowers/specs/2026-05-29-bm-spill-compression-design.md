# BM Spill 压缩设计

## 背景

`bolt/common/memory/bm` 里的 `BufferManager` 当前通过 `SpillStore` 将
unpinned resident block 写入文件 extent，并在后续 `Pin` 或 `Prefetch` 时读回。
现有实现里，`BufferManager` 仍然直接参与 file extent 分配、写入失败后的
free 回滚，以及 `FileBlockAllocator` 的创建和持有。

本设计在 `bolt/common/memory/bm/compress` 下新增 BM 专用压缩模块，并同时收紧
`BufferManager` 与 `SpillStore` 的职责边界：

```text
BufferManager
  -> SpillStore
      -> FileBlockAllocator
      -> DiskIoScheduler
      -> bm/compress
```

`BufferManager` 继续负责 block 生命周期、pin/unpin/reclaim 状态机和逻辑字节
统计。`SpillStore` 负责 spilled block 的持久化格式、file extent 管理、裸 IO
和压缩读写。

## 目标

- 在 `bolt/common/memory/bm/compress` 下提供独立压缩模块。
- 支持 LZ4、ZSTD 和 Snappy。
- 直接使用各压缩库专用 API，不通过 Folly codec。
- 默认启用压缩。
- 只在 block 大于等于可配置阈值时尝试压缩。
- 如果压缩收益不明显，自动写原始 payload。
- 对 `BufferManager` 隐藏压缩算法、压缩后大小和 spill record header。
- `BufferManager` 不再创建或持有 `FileBlockAllocator`。
- `SpillStore` 对 `BufferManager` 暴露 block 级读写接口，而不是裸 extent IO。
- 保持 `BufferHandle`、`BlockHandle` 和调用方 payload 访问语义不变。

## 非目标

- 第一版不做异步 CPU 解压 executor。
- 第一版不让 `Prefetch` 提前解压。
- 第一版不做 dictionary compression。这里指使用预训练字典提高小块或结构相似
  数据的压缩率；它需要训练、版本管理和读写双方持有同一份字典，先不引入这类
  额外状态。
- 第一版不做 checksum 或加密；header 预留 version，后续可以扩展。
- 第一版不改变 BM public API 中 `Allocate`、`Pin`、`BatchPin` 和 `Prefetch`
  的语义。
- 第一版不改变 `BlockMemory::size` 的含义；它始终表示逻辑原始大小。

## 配置

新增 BM 压缩配置：

```cpp
namespace bytedance::bolt::memory::bm::compress {

struct CompressionConfig {
  common::CompressionKind kind{common::CompressionKind_LZ4};
  size_t minCompressBytes{256 * 1024};
  double minCompressionRatio{0.95};
  int compressionLevel{3};
};

} // namespace bytedance::bolt::memory::bm::compress
```

语义：

- `kind == CompressionKind_NONE` 表示关闭压缩。
- `rawSize < minCompressBytes` 时不尝试压缩，直接写原始 payload。
- `rawSize >= minCompressBytes` 时使用 `kind` 指定的算法尝试压缩。
- 如果 `compressedSize >= rawSize * minCompressionRatio`，认为收益不明显，
  写原始 payload。
- 第一版只有 ZSTD 使用 `compressionLevel`；LZ4 和 Snappy 忽略该字段。

新增 `SpillStoreConfig`：

```cpp
struct SpillStoreConfig {
  FileBlockAllocatorConfig fileAllocatorConfig;
  compress::CompressionConfig compressionConfig;
};
```

`BufferManagerConfig` 调整为：

```cpp
struct BufferManagerConfig {
  std::string poolName;
  SpillStoreConfig spillStoreConfig;
  IoPriority readPriority{IoPriority::High};
  IoPriority writePriority{IoPriority::Medium};
  IoPriority prefetchPriority{IoPriority::Low};
};
```

`BufferManagerConfig` 不再直接暴露 `fileAllocatorConfig` 字段。所有 spill
持久化相关配置都放在 `SpillStoreConfig` 下。

## 压缩模块

目录结构：

```text
bolt/common/memory/bm/compress/
  CompressionConfig.h
  CompressionCodec.h
  CompressionCodec.cpp
  Lz4Codec.cpp
  ZstdCodec.cpp
  SnappyCodec.cpp
  SpillRecordHeader.h
  CMakeLists.txt
  tests/
    CompressionCodecTest.cpp
```

`CompressionCodec` 提供统一接口，内部按算法分发到专用 API：

```cpp
struct CompressResult {
  IoBuffer buffer;
  uint64_t rawSize{0};
  uint64_t storedSize{0};
  common::CompressionKind storedKind{common::CompressionKind_NONE};
  uint64_t compressionTimeUs{0};
  bool compressed{false};
};

CompressResult TryCompress(
    IoBuffer payload,
    const CompressionConfig& config,
    MemoryPool* pool);

IoBuffer Decompress(
    IoBuffer storedPayload,
    uint64_t rawSize,
    common::CompressionKind storedKind,
    MemoryPool* pool,
    uint64_t* decompressionTimeUs);
```

`TryCompress` 拥有输入 payload 的所有权。压缩未启用、未达到阈值或收益不足时，
返回原始 payload，`storedKind` 为 `CompressionKind_NONE`。压缩成功且收益达标时，
返回新分配的压缩 buffer。

专用 API 选择：

- LZ4：`LZ4_compress_default`、`LZ4_decompress_safe`。
- ZSTD：`ZSTD_compress`、`ZSTD_decompress`。
- Snappy：`snappy::MaxCompressedLength`、`snappy::RawCompress`、
  `snappy::RawUncompress`。

所有压缩和解压失败都通过 Bolt 异常路径表达。压缩失败不自动 fallback 为原文，
因为失败表示算法 API、输入大小或内存状态异常；继续运行会隐藏数据完整性问题。
收益不足不是失败，按原文写入。

## Spill Record 格式

`SpillStore` 写入的内容从裸 payload 改为 block record：

```text
[SpillRecordHeader][stored payload]
```

header 使用固定小端整数布局：

```cpp
struct SpillRecordHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t headerSize;
  uint32_t compressionKind;
  uint32_t reserved;
  uint64_t rawSize;
  uint64_t storedSize;
};
```

字段语义：

- `magic` 标识 BM spill record。
- `version` 第一版为 1。
- `headerSize` 允许后续扩展 header。
- `compressionKind` 为实际落盘算法；原文落盘时为 `CompressionKind_NONE`。
- `rawSize` 为 block 逻辑原始大小。
- `storedSize` 为 header 后 payload 的实际字节数。

`SpillStore` 读回时校验：

- magic 正确。
- version 支持。
- headerSize 不小于当前 header 大小。
- `rawSize == expectedRawSize`。
- `headerSize + storedSize <= extent.requested_size`。
- `compressionKind` 是 BM 支持的算法。

BM 不保存 `storedSize` 或 `compressionKind`。这些元信息只存在于 spill record
header 中，由 `SpillStore` 私有解析。

## SpillStore 接口

`SpillStore` 从裸 IO helper 升级为 block store：

```cpp
struct SpillWriteResult {
  IoResult io;
  OwnedFileExtent extent;
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
  uint64_t compressionTimeUs{0};
  bool compressed{false};

  bool ok() const {
    return io.ok();
  }
};

struct SpillReadResult {
  IoResult io;
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
  uint64_t decompressionTimeUs{0};

  bool ok() const {
    return io.ok();
  }
};

class SpillReadFuture {
 public:
  SpillReadResult get();

 private:
  // Holds the raw disk read future plus decode context owned by SpillStore.
};

class SpillStore {
 public:
  SpillStore(SpillStoreConfig config, MemoryPool* pool);

  SpillWriteResult WriteBlock(
      IoBuffer& payload,
      size_t rawSize,
      IoPriority priority);

  SpillReadFuture SubmitReadBlock(
      const OwnedFileExtent& extent,
      size_t expectedRawSize,
      IoPriority priority);

 private:
  FileAllocateResult AllocateExtent(size_t size);
  FileFreeResult FreeExtent(const FileExtent& extent);
  OwnedFileExtent OwnExtent(FileExtent extent) const;

  IoResult WriteRaw(
      const FileExtent& extent,
      IoBuffer& payload,
      IoPriority priority);

  std::future<IoResult> SubmitReadRaw(
      const OwnedFileExtent& extent,
      size_t size,
      IoPriority priority);

  SpillStoreConfig config_;
  std::shared_ptr<FileBlockAllocator> allocator_;
  MemoryPool* pool_{nullptr};
  bool schedulerReadyForPayloadMove_{false};
};
```

构造函数内部调用 `CreateFileBlockAllocator(config.fileAllocatorConfig)`，并持有
`allocator_`。`BufferManager` 不再包含 `FileBlockAllocator` 成员，也不再 include
`FileBlockAllocator.h`；只需要看到 `SpillStoreConfig`。

`AllocateExtent`、`FreeExtent`、`OwnExtent`、`WriteRaw` 和 `SubmitReadRaw`
是 `SpillStore` 的低层 helper，不再由 `BufferManager` 调用。

## 写路径

`BufferManager::SpillBlock` 仍负责状态迁移：

```text
IN_MEMORY(pin_count = 0)
  -> move payload out
  -> SPILLING
  -> SpillStore::WriteBlock
  -> SPILLED
```

`SpillStore::WriteBlock` 内部流程：

1. 调用 `compress::TryCompress`，得到 stored payload。
2. 构造 `SpillRecordHeader`。
3. 分配大小为 `headerSize + storedSize` 的 file extent。
4. 将 header 和 stored payload 组合成连续 `IoBuffer`。
5. 调用 `WriteRaw` 写入。
6. 写入失败或异常时释放刚分配的 extent。
7. 成功时返回 `OwnedFileExtent` 和物理写入统计。

`WriteBlock` 必须保证：如果返回 `ok() == false` 或抛异常，已经分配的 extent
不会泄漏。若写入失败后 extent free 失败，走 FATAL，沿用现有 BM 对 file free
失败的处理语义。

## 读路径

`BufferManager::SubmitRead` 只调用：

```cpp
memory.prefetchFuture =
    spillStore_->SubmitReadBlock(*memory.extent, memory.size, priority);
```

`SubmitReadBlock` 内部提交一个按 `extent.extent().requested_size` 读取完整
record 的异步 IO，并返回 `SpillReadFuture`。`SpillReadFuture::get()` 会先等待
raw IO future 完成，然后在调用 `get()` 的线程解析 header、校验
`expectedRawSize`，并在需要时解压。`SpillReadResult::io.buffer` 必须是原始未压缩
payload，大小等于 `expectedRawSize`。

第一版解压发生在消费 `SpillReadFuture` 的线程，也就是当前 `PinPrefetching()`
所在线程：

```text
Prefetch thread:
  submit async read and return

IO scheduler:
  read spill record bytes and fulfill raw IO future

Pin thread:
  SpillReadFuture::get()
  wait raw IO future
  parse header
  decompress if needed
  transition block to IN_MEMORY
```

这样 `Prefetch()` 只提前发起磁盘读，不承担 CPU 解压成本，也避免 IO 完成线程被
解压阻塞。后续如果要让 prefetch 提前解压，需要单独引入 CPU executor 和更明确的
取消、内存占用管理策略。

## BufferManager 修改

`BufferManager` 删除：

```cpp
std::shared_ptr<FileBlockAllocator> allocator_;
```

`Initialize()` 从创建 allocator 改为创建 `SpillStore`：

```cpp
spillStore_ = std::make_unique<SpillStore>(
    config_.spillStoreConfig,
    pool_.get());
```

`SpillBlock()` 从显式分配 extent、写入、失败 free、own extent，改为：

```cpp
auto result =
    spillStore_->WriteBlock(payload, memory->size, config_.writePriority);
if (!result.ok()) {
  // 恢复 payload、状态和逻辑 stats，然后抛 IO failure。
}
memory->extent = std::move(result.extent);
```

`SubmitRead()` 从裸读改为 block 读：

```cpp
memory.prefetchFuture =
    spillStore_->SubmitReadBlock(*memory.extent, memory.size, priority);
```

`BlockMemory` 不增加压缩字段。仍然只保存：

```cpp
std::optional<IoBuffer> payload;
std::optional<OwnedFileExtent> extent;
std::optional<SpillReadFuture> prefetchFuture;
```

`BlockMemory` 需要知道 `SpillReadFuture` 这个 opaque future-like 类型，但不保存
压缩算法、压缩后大小或 header 字段。

## 统计

现有逻辑统计保持原语义，继续表示原始 block 字节数：

- `spilledBytes`
- `reclaimedBytes`
- `spillWriteBytes`
- `spillReadBytes`
- tag 维度的 `spilledBytes` 和 `reclaimedBytes`

新增物理和压缩观测字段：

```cpp
uint64_t spillPhysicalWriteBytes{0};
uint64_t spillPhysicalReadBytes{0};
uint64_t spillCompressedBlocks{0};
uint64_t spillCompressionTimeUs{0};
uint64_t spillDecompressionTimeUs{0};
```

这些字段只用于观测，不参与 block 状态机、不参与 reclaimable bytes 计算。

tag 维度第一版不新增物理压缩统计，避免扩大结构和 debug string 输出。后续如果
需要按 operator 来源分析压缩收益，可以再给 `BufferManagerTagStats` 增加对应字段。

## 错误处理

写路径错误：

- 压缩 API 失败：抛异常，BM 恢复 block 为 `IN_MEMORY`。
- extent allocate 失败：返回失败 result 或抛现有 file allocation failure，BM
  恢复 block 为 `IN_MEMORY`。
- raw write 返回失败：`WriteBlock` free extent，BM 恢复 block 为 `IN_MEMORY`。
- write 失败后的 free 失败：FATAL。

读路径错误：

- raw read 失败：`SpillReadResult::io` 表示失败，BM 维持 block 为 `SPILLED`。
- header 校验失败：`SpillReadResult::io` 表示失败，BM 维持 block 为 `SPILLED`。
- 解压失败：`SpillReadResult::io` 表示失败，BM 维持 block 为 `SPILLED`。

普通 IO 失败继续通过 `IoResult` 表达。状态不变量破坏、extent 生命周期异常和
回滚 free 失败走 FATAL。

## 测试

新增 `bm/compress/tests`：

- LZ4 round-trip。
- ZSTD round-trip。
- Snappy round-trip。
- `kind == NONE` 时不压缩。
- 小于 `minCompressBytes` 时不压缩。
- 不可压缩数据收益不足时写原文。
- 可压缩数据达到收益阈值时写压缩 payload。
- header encode/decode 校验。
- header magic/version/rawSize/storedSize 异常时失败。
- 解压错误输入时失败。

扩展 `bm/tests/BufferManagerTest.cpp`：

- 默认配置下 reclaim 后 pin 读回原数据。
- 配置很大的 `minCompressBytes` 时不压缩，但 reclaim/readback 行为正确。
- 指定 LZ4/ZSTD/Snappy 时都能 reclaim/readback。
- 可压缩 payload 下 `spillPhysicalWriteBytes < spillWriteBytes`。
- 不可压缩 payload 下 fallback 原文，逻辑行为正确。

现有测试配置从：

```cpp
config.fileAllocatorConfig = test::ValidConfigWithDirectory(directory);
```

调整为：

```cpp
config.spillStoreConfig.fileAllocatorConfig =
    test::ValidConfigWithDirectory(directory);
```

## 构建

`bolt/common/memory/bm/CMakeLists.txt` 增加：

```cmake
add_subdirectory(compress)
```

`bolt_memory_bm` 链接 `bolt_memory_bm_compress`。

`bolt_memory_bm_compress` 需要链接 LZ4、ZSTD、Snappy 对应库。具体 target 名称以
当前 Conan/CMake 生成结果为准；如果仓库已有其它模块直接链接这些库，应复用现有
target 命名和 link 方式。

## 迁移顺序

1. 新增 `bm/compress` 库和单元测试。
2. 新增 `SpillStoreConfig`，调整 `BufferManagerConfig`。
3. 将 `FileBlockAllocator` 创建和持有从 `BufferManager` 移到 `SpillStore`。
4. 给 `SpillStore` 增加 block record 写入和读回接口。
5. 将 `BufferManager::SpillBlock` 和 `SubmitRead` 切到 block 级接口。
6. 增加 BM 压缩集成测试和 stats 测试。
7. 编译并运行最窄相关测试目标。

这个顺序先验证压缩模块，再调整分层，最后接入 BM，便于定位失败来源。
