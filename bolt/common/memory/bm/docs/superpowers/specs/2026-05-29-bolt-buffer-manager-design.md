# Bolt BufferManager 设计

## 目标和范围

在 `bolt/common/memory/bm` 下新增一个 DuckDB 风格的
`BufferManager`。第一版只管理执行过程中的临时 block，不管理数据库持久化
数据页、checkpoint、WAL 或 page cache。

`BufferManager` 负责：

- 通过专属 child `MemoryPool` 持有 resident block 内存。
- 通过 BM file allocator 为 spilled block 分配文件 extent。
- 通过 BM disk IO scheduler 读写 spilled block。
- 通过 Bolt `MemoryReclaimer` 接入现有 memory arbitration。

第一版不做：

- `PinReadOnly` 或 const buffer handle。
- `canDestroy` / destroyable block。
- block resize / `ReAllocate`。
- BufferManager 自己的独立内存上限。
- DuckDB 风格 eviction queue purge。
- 线程安全 public API。

## 公开 API

`BufferManager` 必须通过 factory 创建，以便 `BufferHandle` 可以保存
`std::weak_ptr<BufferManager>` 做生命周期诊断。

```cpp
class BufferManager : public std::enable_shared_from_this<BufferManager> {
 public:
  static std::shared_ptr<BufferManager> Create(
      memory::MemoryPool& parent,
      BufferManagerConfig config);

  BufferHandle Allocate(
      size_t size,
      MemoryTag tag,
      std::shared_ptr<BlockHandle>* block = nullptr);

  BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);

  std::vector<BufferHandle> BatchPin(
      std::span<const std::shared_ptr<BlockHandle>> blocks);

  void Prefetch(std::span<const std::shared_ptr<BlockHandle>> blocks);

  uint64_t ReclaimForTest(uint64_t targetBytes);
};
```

`Allocate` 采用 DuckDB 的语义：创建新 block，并返回已经 pinned 的 mutable
`BufferHandle`。如果 `block` 非空，创建出的 `BlockHandle` 会写入该参数，供
后续 `Pin`、`BatchPin` 或 `Prefetch` 使用。该签名刻意贴近 DuckDB
`BufferManager::Allocate`：返回 `BufferHandle`，并通过可选 out-param 返回
`BlockHandle`。

`MemoryTag` 用来标识 block 的申请来源，便于后续 debug、日志和 fatal 诊断。
第一版可以定义为轻量 enum，并提供 `toString(MemoryTag)`：

```cpp
enum class MemoryTag : uint8_t {
  kUnknown,
  kHashBuild,
  kAggregation,
  kSort,
  kWindow,
  kExchange,
  kTesting,
};
```

调用方应传入最贴近业务来源的 tag；无法分类时使用 `kUnknown`。`MemoryTag` 只做
归因和诊断，不影响 reclaim 优先级、IO 优先级或 MemoryPool 记账。

`BufferHandle` 内部强持有对应 `BlockHandle`。如果调用方不传 `block`，该 block
只通过返回的 `BufferHandle` 存活；handle 析构后如果没有其它引用，block 可以
自然销毁。这是一次性临时 buffer 语义。

`Allocate`、`Pin`、`BatchPin` 直接返回 handle。内存分配失败、内存仲裁失败、
file allocation 失败和 IO 失败沿用 Bolt 现有异常路径。异常只用于可恢复的运行时
错误；任何会破坏 BM 状态不变量的错误走 FATAL。

`BatchPin` 是 all-or-throw：如果全部成功，返回顺序与输入顺序一致的 handles；
如果任意 block pin 失败，函数抛异常。已经创建的 handles 在栈展开时自动析构并
unpin。所有 handle 析构路径必须是 `noexcept`，不会在栈展开期间再次抛异常。

`Prefetch` 是 fire-and-forget hint。它不返回 handle，不增加 pin count，也不
保证 block 在后续 `Pin` 前仍然 resident。

## 配置

```cpp
struct BufferManagerConfig {
  std::string poolName;
  FileBlockAllocatorConfig fileAllocatorConfig;
  IoPriority readPriority{IoPriority::High};
  IoPriority writePriority{IoPriority::Medium};
  IoPriority prefetchPriority{IoPriority::Low};
};
```

`poolName` 必须由调用方保证在同一个 parent pool 下唯一。第一版不自动追加后缀，
避免隐藏调用方的命名和归属关系。

file allocator factory 返回 shared ownership：

```cpp
std::shared_ptr<FileBlockAllocator> CreateFileBlockAllocator(
    FileBlockAllocatorConfig config);
```

BM 内部持有 `std::shared_ptr<FileBlockAllocator>`。`OwnedFileExtent` 持有
`std::weak_ptr<FileBlockAllocator>`，用于在释放 extent 时诊断 allocator 是否仍然
存活。

## 初始化顺序

`BufferManager::Create` 的初始化顺序必须固定：

1. 构造 `std::shared_ptr<BufferManager>`。
2. 创建并保存 `std::shared_ptr<FileBlockAllocator>`。
3. 从 parent pool 创建 leaf child pool。
4. 使用 `weak_from_this()` 创建并安装 `BufferManagerReclaimer`。

构造函数中不能调用 `weak_from_this()` 或 `shared_from_this()`，因为此时对象还没
有被 `shared_ptr` 完整接管。

建议实现形态：

```cpp
std::shared_ptr<BufferManager> BufferManager::Create(
    memory::MemoryPool& parent,
    BufferManagerConfig config) {
  auto manager = std::shared_ptr<BufferManager>(
      new BufferManager(parent, std::move(config)));
  manager->initFileAllocator();
  manager->initPoolAndReclaimer();
  return manager;
}
```

leaf child pool 第一版使用默认 `threadSafe=true`。这是可优化点；后续如果确认
BM 始终单线程使用，可以改为 `threadSafe=false` 以减少 `MemoryPool` 内部锁开销。

## 核心类型

类型模型参考 DuckDB：

- `BlockHandle`：调用方可见的逻辑 block handle，内部包装
  `std::shared_ptr<BlockMemory>`。
- `BlockMemory`：block 的内部状态和资源所有者。
- `BufferHandle`：move-only RAII pin guard，析构时调用
  `BufferManager::Unpin`。

`BufferHandle` 不可 copy。move 构造和 move 赋值转移 pin 所有权。
`Destroy()` 是幂等的。`Ptr()` 返回 resident payload 指针，并检查 handle 有效。

`BlockMemory` 保存 `MemoryTag`。所有和 block 相关的 debug 日志、FATAL 诊断、
测试可观测输出都应包含该 tag。

`BufferHandle` 持有 `std::weak_ptr<BufferManager>` 用于诊断。weak owner 不是为了
支持 late destruction；它的价值是让“BM 已经销毁但 handle 仍然存活”的生命周期
bug 可以被结构化观察到，而不是表现为 raw pointer use-after-free。

`BufferHandle::~BufferHandle()` 必须是 `noexcept`。如果 manager lock 失败、
pin count 异常、block 状态不满足 unpin 条件，必须走 FATAL，不允许抛异常。
FATAL 信息至少包含 block id、block size、当前状态、pin count 和触发操作名。

## 资源所有权和生命周期

resident payload 使用 manager 的 leaf pool 分配出的 `IoBuffer`：

```cpp
IoBuffer::allocateFromPool(pool_.get(), size)
```

`BlockMemory` 保存 resident payload 时直接保存 `IoBuffer`，不保存裸指针。当
payload 被 move 到 IO request 中时，scheduler 持有这段内存直到 `IoResult`
返回。如果 `IoBuffer` 被销毁，它的 deleter 会把内存释放回 leaf pool。

spill 文件位置用 `OwnedFileExtent` 包装。`OwnedFileExtent` 保存 `FileExtent`
和 `std::weak_ptr<FileBlockAllocator>`。析构时 lock allocator 并调用
`Free(extent)`。

`OwnedFileExtent::~OwnedFileExtent()` 必须是 `noexcept`。如果 allocator 已失效，
或 `Free` 返回非 ok，必须走 FATAL，不允许抛异常，也不允许只记录日志后继续运行。
析构期间 extent 释放失败表示 allocator 生命周期或元数据不变量被破坏，继续运行会
隐藏文件空间泄漏、double-free 或 fd 生命周期问题。

`BufferManager` 必须比它创建的所有 `BlockHandle`、`BlockMemory` 和
`BufferHandle` 存活更久。weak owner 引用只用于诊断生命周期违规，不表示支持 late
destruction。BM 推荐由比所有 block consumer 都长寿的 owner 持有，典型位置是
query/operator 上下文的根对象。

## Block 状态机

第一版有四个状态：

```text
IN_MEMORY    payload resident 在 leaf MemoryPool 中
SPILLED      payload 不在内存中；FileExtent 保存可恢复副本
PREFETCHING  已提交异步 read；旧 FileExtent 仍然保留
SPILLING     已提交 write；payload 正由 IO request 持有
```

不维护 dirty bit。任何被 reclaim 选中的 in-memory unpinned block 都写入一个新的
file extent。

状态转移：

```text
Allocate:
  new block -> IN_MEMORY(pin_count = 1)

Unpin:
  IN_MEMORY(pin_count > 1) -> IN_MEMORY(pin_count - 1)
  IN_MEMORY(pin_count == 1) -> IN_MEMORY(pin_count = 0), 加入 eviction queue

Pin:
  IN_MEMORY    -> pin_count++
  SPILLED      -> 同步读取 extent，安装 payload，释放 extent，pin_count = 1
  PREFETCHING  -> 等待 prefetch future，安装 payload，释放 extent，pin_count = 1

Prefetch:
  SPILLED -> PREFETCHING(read future)

Reclaim:
  IN_MEMORY(pin_count = 0) -> SPILLING(write future, target extent)
  SPILLING                 -> 写成功后变为 SPILLED
  SPILLING                 -> 写失败后恢复为 IN_MEMORY(pin_count = 0)
```

Reclaim 跳过 `SPILLED`、`SPILLING`、pinned `IN_MEMORY` 和未完成的
`PREFETCHING` block。选择 victim 前，reclaim 会 non-blocking harvest 已 ready
的 prefetch future。成功 harvest 的 block 变为 `IN_MEMORY(pin_count = 0)`，本
轮 reclaim 可以继续 spill 它。

## Pin 和读回

从 `SPILLED` 读回：

1. 从 BM leaf pool 分配一个 `IoBuffer`。
2. 使用 block 的 file extent 提交 read request。
3. 等待 future 完成。
4. 成功后，把 `result.buffer` move 到 `BlockMemory::payload`。
5. 释放旧 `OwnedFileExtent`。
6. 失败时让 `result.buffer` 自动释放内存，然后抛异常，block 保持 `SPILLED`。

如果读回成功但释放旧 extent 失败，`Pin` 不回滚已经安装好的 payload，而是走
FATAL。此时 block 内容已经在内存中，旧 extent 又没有成功归还给 allocator；如果
继续运行，block 可能永久保持 pinned 或泄漏文件空间。该情况表示 allocator 状态
或生命周期不变量已经被破坏。

如果 prefetch IO 失败，后续 `Pin` 观察到该结果时，block 回到 `SPILLED`，然后
`Pin` 再发起一次同步 read。同步 read 失败会抛异常，并保持 block 为 `SPILLED`。

## BatchPin

`BatchPin` 对 batch 中所有 `SPILLED` blocks 先分配 read buffers 并提交 read
futures，再统一等待完成并安装 payload。对 `PREFETCHING` blocks，`BatchPin`
复用已有 future，不重复提交 read。

任一 future 失败时，`BatchPin` 抛异常。已经成功安装并放入局部结果集中的
handles 会在栈展开时 unpin；尚未安装的 `IoBuffer` 通过 `IoResult` 析构释放回
pool；失败 block 保持或恢复为 `SPILLED`。所有析构路径必须 `noexcept`，状态不
变量错误走 FATAL。

## Prefetch

`Prefetch` 是 hint：

1. 从 BM leaf pool 分配一个 `IoBuffer`。
2. 提交 read request。
3. 把 returned future 存入 block，状态设为 `PREFETCHING`。

如果分配 read buffer、提交 IO 或其它准备步骤失败，`Prefetch` 不向调用方抛异
常，只记录日志/计数，并保持 block 为 `SPILLED`。如果 future 完成后结果失败，
下一次 `Pin` 会丢弃失败的 prefetch 结果并重新发起同步 read。

第一版暂不把 `PREFETCHING` 中已经分配的 read buffer 计入 `reclaimableBytes()`。
这些 buffer 已经占用 leaf pool，但在 IO 完成前不能安全 spill。第一版依赖调用方
控制 prefetch 规模；后续可以增加 prefetch budget，或把 ready/harvestable
prefetch 纳入 reclaimable 估算。

## Spill 和 Reclaim

从 `IN_MEMORY` spill：

1. 按 block size 分配新的 file extent。
2. 把 `BlockMemory::payload` move 到 write request，状态设为 `SPILLING`。
3. 提交 IO 并等待 future。
4. 成功后保存新的 `OwnedFileExtent`，让 `result.buffer` 释放内存回 pool，
   状态变为 `SPILLED`。
5. 失败时把 `result.buffer` move 回 `BlockMemory::payload`，释放新 extent，
   状态恢复为 `IN_MEMORY(pin_count = 0)`，然后抛异常。

reclaim write 失败必须先恢复 in-memory payload，再抛异常。异常会沿
`MemoryReclaimer::reclaim` 向上传播给 memory arbitrator。v1 不在 BM 内部重试
IO，也不吞掉 reclaim write 失败；arbitrator 对该异常是让当前 reclaimer 本轮失
败还是让整个仲裁失败，取决于现有 memory arbitration 实现。

`reclaim(targetBytes, maxWaitMs, stats)` 和 `ReclaimForTest` 使用同一套逻辑。
返回值是成功 spill 后实际释放的 resident bytes。

`maxWaitMs` 为了兼容 `MemoryReclaimer` 接口保留。第一版暂时忽略该参数：一旦
开始本轮 reclaim，就同步 spill victim，直到达到 `targetBytes` 或没有更多
victim。后续优化可以在每个 block spill 完成后检查 elapsed time，超过
`maxWaitMs` 则提前返回已经释放的 bytes。

## MemoryPool 和 Reclaimer

`BufferManager::Create` 创建 leaf child pool，并通过 `setReclaimer` 安装
`BufferManagerReclaimer`。

reclaimer 持有 `std::weak_ptr<BufferManager>`。正常生命周期下 manager 应当存
在；如果 weak pointer 已过期，reclaimer 报告没有可 reclaim 内存。

`reclaimableBytes()` 返回 `unpinnedResidentBytes_`。该值在 BM 单线程约束下基本
精确，并且不需要扫描 eviction queue。

`unpinnedResidentBytes_` 更新规则：

- `Allocate`：不变，因为新 block 是 pinned。
- `Unpin` 到 0 且状态为 `IN_MEMORY`：加 block size。
- `Pin` 一个 unpinned `IN_MEMORY` block：减 block size。
- 从 `SPILLED` 或 `PREFETCHING` `Pin`：不变，因为读回结果直接是 pinned。
- ready prefetch harvest 成 `IN_MEMORY(pin_count = 0)`：加 block size。
- reclaim 选中 unpinned resident block 并进入 `SPILLING`：减 block size。
- `SPILLING` 写失败并恢复为 `IN_MEMORY(pin_count = 0)`：加回 block size。
- `SPILLING` 写成功变为 `SPILLED`：不再调整，因为进入 `SPILLING` 时已经扣减。

## Eviction Queue

eviction queue 使用 DuckDB 的 append-only sequence-number 模型：

```cpp
struct EvictionEntry {
  std::weak_ptr<BlockMemory> block;
  uint64_t sequence;
};
```

每个 `BlockMemory` 有一个 `evictionSequence`。

- `Unpin` 到 0 时递增 sequence，并追加 entry。
- `Pin` 时递增 sequence，使旧 queue entry 失效。
- Reclaim 从队头 pop，跳过 expired weak pointer、sequence mismatch、pinned
  block 和非 `IN_MEMORY` block。

第一版不实现 DuckDB 风格的 purge。stale queue entry 在 reclaim 扫描时通过
`std::deque::pop_front` 自然清理前缀，避免已经扫描过的 entry 长期占用内存。

v1 limitation：如果长任务中 eviction queue 长度持续超过 live unpinned block
数的 N 倍，或 queue 自身内存占用超过 BM leaf pool 的 X%，需要引入
purge/compact。N 和 X 由压测确定，初始建议值为 N=4；X 可以先作为观测指标保
留。

## 线程模型

第一版 `BufferManager` 不是线程安全组件。

调用方必须串行化同一个 manager 实例上的所有 public calls，包括 `Allocate`、
`Pin`、`BatchPin`、`Prefetch`、`ReclaimForTest` 和 handle destruction。生产
reclaim 必须通过 Bolt 现有 operator/task reclaim 协议进入，保证 BM reclaim 不
和正常 BM API 调用并发。

`BufferManagerReclaimer::reclaimableBytes` 也遵循同一串行化约束。实现中应加入
debug check/guard 来发现违反该约束的并发调用；第一版不要求
`unpinnedResidentBytes_` 使用 atomic。建议所有 BM public API、handle
destruction、`reclaimableBytes` 和 `reclaim` 入口共享同一个 debug-only
single-thread/reentrancy guard。

`Prefetch` 会提交异步 IO，但后台 IO completion 不直接修改 BM 状态。状态安装发
生在 `Pin` 中，或发生在 BM API / reclaim 入口做的 non-blocking harvest 中。

manager-owned file allocator 也不是线程安全对象，只在相同的单线程 BM 约束下使
用。

## 错误处理原则

错误分成两类：

- 可恢复运行时错误：内存分配失败、内存仲裁失败、read IO 失败、write IO 失败、
  file allocation 失败。这些错误可以抛 Bolt 异常，但抛出前必须保证 block 状态
  一致。
- 状态不变量错误：析构路径失败、生命周期违规、pin count 异常、block 状态不合
  法、`Pin` / prefetch harvest 读回成功后释放旧 extent 失败。这些错误不能抛异
  常，必须走 FATAL。

析构策略统一为：

- `BufferHandle::~BufferHandle() noexcept`：正常路径调用 `Unpin`。如果 manager
  已失效、pin count 异常或 block 状态不满足 unpin 条件，走 FATAL。
- `OwnedFileExtent::~OwnedFileExtent() noexcept`：调用 `Free(extent)`。如果
  allocator 已失效或 `Free` 返回非 ok，走 FATAL。不在析构中抛异常，也不在析构
  中二次尝试删除底层文件。
- 内部状态不变量违规必须走 FATAL，不能用会抛异常的检查宏。

FATAL 诊断至少包含 block id、block size、memory tag、当前状态、pin count、
extent id、fd、offset、操作名和底层错误码中可获得的信息。

## 测试

单测需要覆盖：

- `Allocate` 记录调用方传入的 `MemoryTag`，返回 pinned handle，并写出可选
  `BlockHandle`。
- `BufferHandle` move-only，且只 unpin 一次。
- `Pin` resident block 时不触发 IO。
- `Pin` spilled block 时读回 payload，并释放旧 file extent。
- `Pin` / prefetch harvest 读回成功后释放旧 extent 失败触发 FATAL。
- `BatchPin` 对所有 `SPILLED` blocks 先提交 read futures，再统一等待和安装；
  对 `PREFETCHING` blocks 复用已有 future。
- `BatchPin` 失败时已 pin 的 handles 自动 unpin，析构路径不抛异常。
- `Prefetch` 将 `SPILLED` 转为 `PREFETCHING`；`Pin` 能 harvest 该 future。
- `Prefetch` 分配或提交失败时不抛异常，block 保持 `SPILLED`。
- 大量 `Prefetch` 占用 leaf pool 但不增加 `reclaimableBytes` 的行为。
- Reclaim 跳过 pinned blocks。
- Reclaim 能 harvest ready prefetch，并在同一轮 spill 这些 blocks。
- Reclaim 写期间 block 进入 `SPILLING`，写成功变 `SPILLED`，写失败恢复
  `IN_MEMORY`。
- Reclaim 写 IO 失败时恢复 payload，并保持 block resident。
- `ReclaimForTest` spill unpinned block，并释放 MemoryPool bytes。
- `maxWaitMs` 在 v1 被忽略，reclaim 仍然返回实际释放 bytes。
- Eviction queue stale entry 通过 sequence number 跳过。
- `BufferHandle` late destruction 触发 FATAL。
- `OwnedFileExtent` 析构释放 extent 失败触发 FATAL。
- FATAL 诊断信息包含 `MemoryTag`。
- file extent 在 pin、reclaim、destruction 路径中只释放一次。
- `BufferManagerReclaimer::reclaimableBytes` 跟随 `unpinnedResidentBytes_`。
- debug single-thread/reentrancy guard 能发现违反 BM 串行化约束的调用。

集成测试需要验证真实 `MemoryPool`、真实 `FileBlockAllocator` 和
`DiskIoScheduler` 的组合行为；如果运行环境支持对应 IO backend，再覆盖真实 IO
路径。
