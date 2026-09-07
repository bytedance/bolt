# DiskIoScheduler 使用说明

`DiskIoScheduler` 是 BM 磁盘 IO 调度层的对外 facade。它接收读写请求，按优先级排队，在受控 queue depth 下提交给 io_uring backend，并为每个请求返回一个 `std::future<IoResult>`。

## 适用场景

适合在 BM 中做异步 buffered file IO：

- 读写 BM memory block 对应的文件数据。
- 批量提交多个独立 IO 请求，避免调用方每次直接阻塞在 syscall 上。
- 通过 stats 观察排队深度、inflight 深度、延迟、吞吐和 backend 错误。

如果调用方需要明确的同步 IO 语义，直接使用 `pread` / `pwrite` 更合适。

## 入口

业务侧使用进程级全局 facade：

```cpp
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

using namespace bytedance::bolt::memory::bm;

DiskIoScheduler& scheduler = diskIoScheduler();
```

`diskIoScheduler()` 持有进程生命周期的 scheduler 实例。业务代码不应该启动、停止或析构这个全局 scheduler，public facade 也不暴露 shutdown 接口。

## 请求模型

一次 IO 请求由 `IoRequest` 描述：

```cpp
struct IoRequest {
  IoOpcode opcode;
  IoPriority priority;
  int fd;
  uint64_t fileOffset;
  IoBuffer buffer;
};
```

buffer 由 move-only 的 `IoBuffer` 描述。`IoBuffer` 持有一段连续内存和对应的释放逻辑，默认兼容 `std::unique_ptr<char[]>`，也可以通过 `fromOwned()` 传入自定义 deleter：

```cpp
auto* data = new char[size];
auto buffer = IoBuffer::fromOwned(
    data,
    size,
    0,
    size,
    [](char* p) noexcept { delete[] p; });
```

所有权规则：调用 `submit(std::move(request))` 之后，buffer 所有权转移给 scheduler。返回的 future ready 之后，buffer 会通过 `IoResult::buffer` 归还给调用方。

如果 buffer 来自 `MemoryPool`，优先使用 `allocateFromPool()`，完成后 `IoBuffer` 析构会通过同一个 pool 释放内存：

```cpp
request.buffer = IoBuffer::allocateFromPool(pool, size);
```

不使用 shared ownership 时，调用方必须保证 `MemoryPool*` 的生命周期覆盖所有相关 `IoResult` 的生命周期。

## 提交写请求

```cpp
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <cstring>
#include <memory>

using namespace bytedance::bolt::memory::bm;

std::future<IoResult> submitWrite(int fd, uint64_t offset) {
  constexpr size_t kSize = 4096;
  auto data = std::make_unique<char[]>(kSize);
  std::memset(data.get(), 'x', kSize);

  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = IoPriority::Medium;
  request.fd = fd;
  request.fileOffset = offset;
  request.buffer = IoBuffer{std::move(data), kSize, 0, kSize};

  return diskIoScheduler().submit(std::move(request));
}
```

处理返回结果：

```cpp
auto result = submitWrite(fd, 0).get();
if (!result.ok()) {
  // result.error 表示 scheduler/backend 层面的错误。
  // 如果 backend 返回了系统 IO 错误，result.nativeErrorCode 会保存原始 errno。
}
```

## 提交读请求

```cpp
std::future<IoResult> submitRead(int fd, uint64_t offset, size_t size) {
  auto data = std::make_unique<char[]>(size);

  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::High;
  request.fd = fd;
  request.fileOffset = offset;
  request.buffer = IoBuffer{std::move(data), size, 0, size};

  return diskIoScheduler().submit(std::move(request));
}

auto result = submitRead(fd, 0, 4096).get();
if (result.ok()) {
  const char* bytes = result.buffer.data();
  const uint64_t bytesRead = result.bytes;
}
```

## 优先级

`IoPriority` 支持三个级别：

- `IoPriority::High`
- `IoPriority::Medium`
- `IoPriority::Low`

scheduler 内部使用加权调度。一般 BM 读写请求建议使用 `Medium`；延迟敏感请求使用 `High`；后台维护类请求使用 `Low`。

## 参数校验和错误码

如果请求非法，`submit()` 会直接返回 ready future，结果为 `IoErrorCode::InvalidRequest`。常见非法请求包括：

- `fd < 0`
- `buffer.data() == nullptr`
- `buffer.length() == 0`
- `buffer.offset() > buffer.size()`
- `buffer.offset() + buffer.length()` 超过 `buffer.size()`
- 非法 opcode 或 priority

`IoErrorCode` 取值：

- `Ok`：请求成功完成。
- `InvalidRequest`：请求在入队前被拒绝。
- `Shutdown`：scheduler 正在停止，或停机过程中拒绝了排队请求。
- `BackendSubmitFailed`：backend 硬失败，无法提交请求。
- `BackendIoError`：backend 完成请求时返回 IO 错误。
- `ShortIo`：backend 完成的字节数少于请求字节数。

使用读数据或认为写入完整成功之前，必须先检查 `result.ok()`。

## 可观测性

通过 `stats()` 获取当前调度器状态：

```cpp
const auto stats = diskIoScheduler().stats();
```

常用字段：

- `queuedRequests`：按优先级统计的排队请求数。
- `inflightRequests`：已经提交给 backend、尚未完成的请求数。
- `completedRequests`、`successfulRequests`、`failedRequests`。
- `backendSubmitFailedRequests`、`backendIoErrorRequests`。
- `averageDeviceLatencyUs`：backend 设备侧平均延迟。
- `averageQueueWaitUs`：请求在 scheduler 队列中的平均等待时间。
- `averageEndToEndLatencyUs`：从入队到完成的端到端平均延迟。
- `averageSubmitBatchSize`、`averageCompletionBatchSize`。
- `depthControl`：当前 depth controller 模式和深度相关统计。

`average*` 字段是从累计值和样本数派生出来的快照值。scheduler 的 record 路径只维护
计数、累计值和 max/min，`stats()` / `toString()` 阶段再计算平均值，避免 IO worker
hot path 持续做浮点派生计算。

`DiskIoScheduler` 不主动把 stats 打到日志。调用方需要诊断时，应在低频边界按需读取
`stats()`，再接入上层 runtime stats 或监控系统；不要在每个请求提交、完成或 future
fulfill 上打印日志。

## 注意事项

- 当前 backend 基于 io_uring。如果运行环境不支持或不允许 io_uring，默认 backend 构造会走项目已有异常路径。
- 当前测的是 buffered IO，不要求 `O_DIRECT` 对齐 buffer。
- fd 生命周期由调用方负责。提交请求后，必须保证 `fd` 在 future ready 前仍然有效。
- `submit()` 返回的是 `std::future`。调用方应该在自己的线程中 `wait()` / `get()`，不要假设 future ready 的线程固定。
- `IoBuffer` 的 `size` 表示整块 buffer 大小，`offset` 和 `length` 表示本次 IO 使用的子区间。
