# BM File Block Allocator 使用说明

`file` 模块提供一个进程内的文件块分配器。调用方传入需要写入的数据大小，分配器返回一个可写文件位置：

```cpp
struct FileExtent {
  int fd;
  uint64_t offset;
  uint64_t requested_size;
  uint64_t allocated_size;
  FileExtentKind kind;
  uint64_t id;
};
```

调用方使用 `fd + offset` 做显式 offset IO，例如 `pwrite()`、`pread()` 或 io_uring 中带 offset 的写请求。分配器只管理文件空间和 fd 生命周期，不负责实际 IO，也不感知 IO 是否完成。

## 基本用法

```cpp
#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

using namespace bytedance::bolt::memory::bm;

std::shared_ptr<FileBlockAllocator> CreateAllocator() {
  FileBlockAllocatorConfig config;
  config.directory = "/tmp/bolt-bm-file";
  config.bucket_sizes = {32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024};
  config.file_size_limit_bytes = 1024 * 1024 * 1024; // 1 GiB
  config.max_open_files_per_bucket = 16;

  return CreateFileBlockAllocator(std::move(config));
}

bool WriteData(FileBlockAllocator& allocator, const char* data, int64_t size) {
  auto allocation = allocator.Allocate(size);
  if (!allocation.ok()) {
    return false;
  }

  const auto& extent = allocation.extent;
  const ssize_t written = ::pwrite(extent.fd, data, size, extent.offset);
  if (written != size) {
    allocator.Free(extent);
    return false;
  }

  // 调用方确认不再需要这段文件空间后释放。
  auto free_result = allocator.Free(extent);
  return free_result.ok();
}
```

## 分配策略

- `bucket_sizes` 表示小块分配的 bucket 大小，单位是字节。
- `Allocate(size)` 会选择第一个 `bucket_size >= size` 的 bucket。
- bucket 文件内部按固定块大小分配，返回的 `allocated_size` 等于 bucket 大小。
- 如果 `size > bucket_sizes.back()`，分配器会创建 dedicated 文件，返回 `offset = 0`，`allocated_size = requested_size`。
- bucket 文件按需创建，不会在初始化时提前创建所有文件。
- 一个 bucket 内可以有多个打开文件；数量受 `max_open_files_per_bucket` 限制。
- bucket 文件内所有块都释放后，该文件会被关闭并删除，释放 fd 名额。

## 配置约束

`CreateFileBlockAllocator()` 会校验配置。配置不合法时会触发 `BOLT_CHECK` 异常。

- `directory` 不能为空。
- allocator 会在 `directory` 下创建一个带 uuid 的实例子目录，实际 bucket 和 dedicated 文件都位于该子目录下。
- 创建 allocator 不会删除 `directory` 下已有内容。
- `bucket_sizes` 不能为空。
- `bucket_sizes` 必须严格单调递增，不能乱序，不能重复。
- 每个 bucket size 必须 4 KiB 对齐。
- `file_size_limit_bytes` 必须大于 0，必须 4 KiB 对齐，并且不能小于最大 bucket size。
- `max_open_files_per_bucket` 必须大于 0。

可以在初始化前用 `ValidateFileBlockAllocatorConfig(config)` 做显式校验。

## IO 约定

调用方必须使用显式 offset IO：

```cpp
::pwrite(extent.fd, data, size, extent.offset);
::pread(extent.fd, buffer, size, extent.offset);
```

不要依赖文件当前 offset，也不要把分配器返回的 fd 当成 append-only fd 使用。模块创建文件时不会使用 `O_APPEND`。

如果两个线程分别拿到 `offset = 0` 和 `offset = 4096`，后一个线程先写 `4096` 不会影响前一个线程之后写 `0`，前提是两次写都使用显式 offset。

## 释放约定

调用方必须在对应 IO 完成后再调用：

```cpp
auto result = allocator.Free(extent);
```

注意：

- `Free()` 只表示这段空间可以被复用或对应文件可以删除。
- 如果异步 IO 还没有完成就释放，后续分配可能复用同一个 offset，导致数据竞争或文件被提前删除。
- 同一个 `FileExtent` 只能释放一次；重复释放返回 `FileErrorCode::kDoubleFree`。
- `FileExtent` 必须交回创建它的同一个 allocator 释放。
- 调用方不能关闭 `extent.fd`。fd 生命周期由 allocator 管理。

## 错误处理

初始化属于低频路径，配置错误通过异常暴露。`Allocate()` 和 `Free()` 属于高频路径，通过错误码返回。

常见错误码：

- `FileErrorCode::kInvalidSize`：`Allocate()` 传入的 size 小于等于 0。
- `FileErrorCode::kTooManyOpenFiles`：某个 bucket 已达到 `max_open_files_per_bucket`，且现有文件没有可用空间。
- `FileErrorCode::kIoError`：创建文件失败，`native_error_code` 保存 errno。
- `FileErrorCode::kDoubleFree`：释放未知或已释放的 extent。
- `FileErrorCode::kInvalidExtent`：extent 元数据不一致。
- `FileErrorCode::kShutdown`：allocator 已进入 shutdown 状态。

## 生命周期和线程安全

生产路径推荐由调用方显式持有 allocator：

```cpp
auto allocator = CreateFileBlockAllocator(config);
auto allocation = allocator->Allocate(size);
allocator->Free(allocation.extent);
```

约束：

- `FileBlockAllocator` 不是线程安全对象。
- 调用方必须保证同一个 allocator 实例的 `Allocate()` / `Free()` 不并发执行。
- 如果需要并发，调用方应在外层用对象级锁、分片锁或其他上层同步机制保护同一个 allocator。
- 可以同时创建多个 allocator；即使它们使用相同的 base `directory`，也会写入不同的 uuid 子目录。
- allocator 析构时会关闭并删除自己管理的 bucket 和 dedicated 文件，并删除自己的 uuid 子目录。
- 析构前调用方应确保没有未完成的 IO，也没有继续使用旧 fd 的线程。

单测可以直接实例化 `FileBlockAllocatorImpl`，也可以通过 `CreateFileBlockAllocator()` 验证公共接口。
