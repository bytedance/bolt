# Disk IO 调度器设计

## **背景**

实现一个小而可测试的 io\_uring 调度层：单 buffer 的 positional read/write、三个优先级、加权公平调度，以及以吞吐为目标的自适应 inflight depth。



## **目标**

- 在 `bolt/common/memory/bm` 下提供独立的 Disk IO 调度器库。

- 当 `IO_URING_SUPPORTED` 可用时，真实磁盘 IO 使用 io\_uring。

- 将 IO 调度逻辑和 io\_uring backend 分离。

- 对外暴露 `std::future<IoResult>` API。

- 支持调用方已打开 fd 上的 `Read` 和 `Write` 请求。

- 支持三个优先级：`High`、`Medium`、`Low`。

- 优先级权重可配置。

- 动态调整有效 inflight depth，以提升吞吐。

- 使用 shared buffer ownership，保证异步 IO 完成前 buffer 仍然有效。

- 提供 mock backend，用于确定性的单元测试。

## **非目标**

- 第一版不接入现有 spill 或 file 路径。

- 不支持 open、close、fsync、fdatasync、cancel、scatter/gather 或请求合并。

- 不提供 multi\-shard 调度器 API 或实现。

- 公共 API 不依赖 Folly。

- 自适应 depth 控制器不使用 target latency，也不做 latency guard。



## **架构**

模块分为三层：

```Plain Text
DiskIoScheduler API
  -> std::future<IoResult> submit(IoRequest)

Scheduler core
  -> priority queues
  -> weighted fair dispatch
  -> adaptive currentDepth control
  -> shutdown and drain

IoBackend
  -> IoUringBackend for real IO
  -> MockIoBackend for tests
```

- 调度器负责请求队列、调度策略、inflight 计数和 promise completion。

- Backend只负责底层 submit 和 reap completion 的机制。

- 调用方负责 fd 生命周期。一个请求引用一个已经打开的 `fd`、一个文件 offset，以及一个连续的 buffer range。



## **公共 API**

公共 API 基于 C\+\+ 标准库：

```C++
enum class IoOpcode : uint8_t {
  Read,
  Write,
};

enum class IoPriority : uint8_t {
  High = 0,
  Medium = 1,
  Low = 2,
};

struct IoBuffer {
  std::unique_ptr<void> data;
  size_t size;
  size_t offset;
  size_t length;
};

struct IoRequest {
  IoOpcode opcode;
  IoPriority priority;
  int fd;
  uint64_t fileOffset;
  IoBuffer buffer;
};

struct IoResult {
  uint64_t bytes;
  int errorCode;
};
```

- `errorCode == 0` 表示成功。普通 IO 失败通过 `IoResult` 表达，不通过`future.get()` 抛异常表达。



调度器 facade：

```C++
class DiskIoScheduler {
 public:
  explicit DiskIoScheduler(DiskIoSchedulerConfig config);
  ~DiskIoScheduler();

  std::future<IoResult> submit(IoRequest request);

  void stopAndDrain();
  DiskIoSchedulerStats stats() const;
};
```

`submit()` 在入队前做基础请求校验：

- `fd >= 0`

- buffer 非空

- `length > 0`

- `offset + length <= size`，且不能发生 overflow

- opcode 和 priority 合法

- scheduler 仍在接收请求



非法请求返回一个已经完成的 future，错误码例如 `EINVAL`。如果 shutdown 已经开始，后续 submit 返回一个已经完成的 future，错误码使用 `ESHUTDOWN` 或最接近的平台错误码。



## **配置**

```C++
struct AdaptiveDepthConfig {
  bool enabled = true;
  uint32_t minDepth = 1;
  uint32_t initialDepth = 64;
  uint32_t maxDepth = 256;
  std::chrono::milliseconds controlInterval{200};
  uint32_t increaseStep = 4;
  double minThroughputGain = 0.02;
};

struct DiskIoSchedulerConfig {
  uint32_t ringDepth = 256;
  std::array<uint32_t, 3> priorityWeights = {
      8, // High
      4, // Medium
      1, // Low
  };
  AdaptiveDepthConfig adaptiveDepth;
};
```



配置校验规则：



- `ringDepth > 0`

- 所有 priority weight 都必须大于 0

- `minDepth > 0`

- `minDepth <= initialDepth <= maxDepth`

- `maxDepth <= ringDepth`

- `controlInterval > 0`

- `increaseStep > 0`

- `minThroughputGain >= 0`

## **调度策略**

调度器为每个优先级维护一个 FIFO 队列：

```Plain Text
High
Medium
Low
```

调度策略使用 Deficit Weighted Round Robin。每个优先级队列有一个可配置weight 和一个 deficit counter。每轮调度时，为非空队列增加对应 weight。当队列非空、deficit 为正，并且 `inflight < currentDepth` 时，调度器提交一个请求，并将该队列的 deficit 减 1。

使用默认权重时，如果三个优先级都持续有请求，长期 submit 比例大致为：

```Plain Text
High : Medium : Low ~= 8 : 4 : 1
```

同一优先级内保持 FIFO。优先级只影响尚未提交到 io\_uring 的请求。一旦请求已经提交，完成顺序由 backend 和内核行为决定。

第一版中，每个请求的调度成本相同，不按 byte size 加权。



## **自适应 Depth**

io\_uring ring depth 在初始化后固定。动态控制变量是调度器的有效 inflight，ring depth上限：currentDepth \<= ringDepth。



第一版使用吞吐导向的 hill\-climbing：

1. 从 `initialDepth` 开始。

2. 每个 `controlInterval` 统计 completed bytes per second。

3. 如果队列有积压，并且上一次提升 depth 后吞吐至少提升了

`minThroughputGain`，则将 `currentDepth` 增加 `increaseStep`。

4. 如果提升 depth 后吞吐持平或下降，则回退到近期观测到的 best depth。

5. 始终将 `currentDepth` 限制在 `[minDepth, maxDepth]`。



Latency 会记录到 stats，但第一版不参与控制决策。如果关闭 adaptive depth，则调度器使用 `initialDepth` 作为固定 inflight 上限。



## **线程模型**

`DiskIoScheduler` 启动一个后台 scheduler thread。调用方线程调用 `submit()`，完成请求校验、创建 promise、请求入队，然后返回future。



Scheduler thread 负责：

- 当 `inflight < currentDepth` 时，按加权优先级从队列中 dispatch 请求

- 将选中的请求提交给 backend

- reap backend completions

- 完成对应 promise

- 更新 stats 和 adaptive\-depth 状态

第一版只支持 single\-shard。



## **Shutdown**

`stopAndDrain()` 分为两个阶段：

1. 停止接收新请求。

2. 继续 dispatch queued requests 并 reap inflight requests，直到所有已经接收的请求都完成。



如果调用方没有显式调用 `stopAndDrain()`，`DiskIoScheduler` 析构函数会兜底调用。请求不会被 cancel。



## **Backend 接口**

Backend 接口是模块内部接口，只暴露 scheduler 需要的能力：

- 使用 scheduler\-owned request id 提交请求

- reap 零个或多个 completions

- wait 或 poll completion events

- 所有 inflight 请求完成后关闭 backend 资源

`IoUringBackend` 使用 `io_uring_prep_read` 和 `io_uring_prep_write` 实现单 buffer positional IO。它将 scheduler request id 存入 `user_data`，用于将completion 路由回对应 promise。



`MockIoBackend` 只用于测试，并且行为确定。测试可以控制 completion 顺序、返回字节数、错误码和模拟 latency。



## **错误处理**

IO 错误通过 `IoResult` 返回：

```Plain Text
success: IoResult{bytes, 0}
failure: IoResult{bytes, errno_value}
```

部分读写返回 backend 报告的 byte count。第一版中，scheduler 不自动重试部分 IO。

配置错误在构造阶段检测。请求校验错误返回已经完成的 future，并携带错误码。



## **Stats**

调度器暴露 stats snapshot，至少包含：

- 每个优先级的 queued request 数

- inflight request 数

- current depth

- completed request count

- completed bytes

- successful 和 failed request counts

- recent throughput

- average observed latency

- 每个优先级的 submitted 和 completed counts

Stats 用于测试、运行时可观测性和后续调优。



## **测试**

单元测试使用 `MockIoBackend` 覆盖：

- 请求校验

- 同优先级 FIFO 顺序

- `High`、`Medium`、`Low` 之间的加权公平调度

- 可配置 priority weights

- `currentDepth` 对 inflight request 数的限制

- 吞吐提升时 adaptive depth 上升

- 吞吐不再提升时 adaptive depth 回退

- shutdown drain 行为

- IO 错误通过 `IoResult` 传播

- stats 更新

io\_uring 集成测试只在 `IO_URING_SUPPORTED` 下编译，并覆盖：



- 临时文件 read 和 write

- 多请求并发

- invalid fd 错误处理

- 真实 inflight IO 下的 `stopAndDrain()`



