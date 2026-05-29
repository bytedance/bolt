# Bolt BufferManager 设计

## 范围

在 `bolt/common/memory/bm` 下新增一个 DuckDB 风格的
`BufferManager`。第一版只管理执行过程中的临时 block，不管理数据库持久化
数据页、checkpoint、WAL 或 page cache。

`BufferManager` 通过专属 child `MemoryPool` 持有 resident block 内存，
通过现有 BM file allocator spill 未被 pin 的 block，并通过 BM disk IO
scheduler 处理读写请求。

第一版不做以下能力：

- `PinReadOnly` 或 const buffer handle。
- `canDestroy` / destroyable block。
- block resize / `ReAllocate`。
- BufferManager 自己的独立内存上限。
- eviction queue purge 或 compact。
- 线程安全 public API。

## 公开 API

`BufferManager` 通过 factory 创建。这样 handle 可以保存 weak owner 引用，
用于诊断生命周期问题：

```cpp
class BufferManager : public std::enable_shared_from_this<BufferManager> {
 public:
  static std::shared_ptr<BufferManager> Create(
      memory::MemoryPool& parent,
      BufferManagerConfig config);

  BufferHandle Allocate(
      size_t size,
      std::shared_ptr<BlockHandle>* block = nullptr);

  BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);

  std::vector<BufferHandle> BatchPin(
      std::span<const std::shared_ptr<BlockHandle>> blocks);

  void Prefetch(std::span<const std::shared_ptr<BlockHandle>> blocks);

  uint64_t ReclaimForTest(uint64_t targetBytes);
};
```

`Allocate` 采用 DuckDB 的语义：创建一个新 block，并返回已经 pinned 的
mutable `BufferHandle`。如果 `block` 非空，创建出的 `BlockHandle` 会写入
该参数，调用方后续可以用它调用 `Pin`、`BatchPin` 或 `Prefetch`。
该签名刻意贴近 DuckDB `BufferManager::Allocate`，即返回 `BufferHandle`
并通过可选 out-param 返回 `BlockHandle`，以便后续语义对齐。

`BufferHandle` 内部强持有对应 `BlockHandle`。如果调用方不传 `block`，
则该 block 只通过返回的 `BufferHandle` 存活；handle 析构后如果没有其它引用，
block 可以自然销毁。这是一次性临时 buffer 语义，不支持后续再次 `Pin`。

`Allocate`、`Pin`、`BatchPin` 直接返回 handle。内存分配失败、内存仲裁失败、
file allocation 失败和 IO 失败沿用 Bolt 现有异常路径。这和 `MemoryPool`
以及 DuckDB 的行为一致。

`BatchPin` 对调用方表现为 all-or-throw：如果所有 block 都 pin 成功，返回顺序
和输入顺序一致的 handles；如果任意 block pin 失败，函数抛异常，调用过程中
已经创建的 handles 会在栈展开时自动析构并 unpin。

`Prefetch` 是 fire-and-forget hint。它不返回 handle，不增加 pin count，也不
保证 block 在后续 `Pin` 前仍然 resident。

## 配置

```cpp
struct BufferManagerConfig {
  std::string poolName{"buffer_manager"};
  FileBlockAllocatorConfig fileAllocatorConfig;
  IoPriority readPriority{IoPriority::High};
  IoPriority writePriority{IoPriority::Medium};
  IoPriority prefetchPriority{IoPriority::Low};
};
```

`BufferManager` 从传入的 parent pool 创建一个 leaf child pool，并在该 pool
上安装 `BufferManagerReclaimer`。同时，`BufferManager` 通过
`CreateFileBlockAllocator` 创建并持有一个专属 `FileBlockAllocator`。
file allocator factory 返回 shared ownership：

```cpp
std::shared_ptr<FileBlockAllocator> CreateFileBlockAllocator(
    FileBlockAllocatorConfig config);
```

这样 `OwnedFileExtent` 可以保存 `std::weak_ptr<FileBlockAllocator>`，在析构时
检测 allocator 是否仍然存活。

`poolName` 必须由调用方保证在同一个 parent pool 下唯一。第一版不自动追加
后缀，避免隐藏调用方的命名和归属关系。

## 核心类型

类型模型参考 DuckDB：

- `BlockHandle`：调用方可见的逻辑 block handle，内部包装
  `std::shared_ptr<BlockMemory>`。
- `BlockMemory`：block 的内部状态和资源所有者。
- `BufferHandle`：move-only RAII pin guard，析构时调用
  `BufferManager::Unpin`。

`BufferHandle` 不可 copy。move 构造和 move 赋值转移 pin 所有权。
`Destroy()` 是幂等的。`Ptr()` 返回 resident payload 指针，并检查 handle
有效。

`BufferHandle` 持有 `std::weak_ptr<BufferManager>` 用于诊断。析构时 lock
manager 并调用 `Unpin`。weak owner 不是为了支持 late destruction；它的价值是
让“BM 已经销毁但 handle 仍然存活”的生命周期 bug 可以被结构化观察到，而不是
表现为 raw pointer use-after-free。

析构路径必须是 `noexcept`。如果 lock 失败，或发现其它生命周期 invariant
被破坏，不能抛异常；必须走不抛异常的 fatal/abort 路径，并输出足够定位的信息，
至少包括 block id、block size、当前状态、pin count 和触发的操作名。

## 资源所有权

resident payload 使用 manager 的 leaf pool 分配出来的 `IoBuffer`：

```cpp
IoBuffer::allocateFromPool(pool_.get(), size)
```

`BlockMemory` 保存 resident payload 时直接保存 `IoBuffer`，不保存裸指针。
当 payload 被 move 到 IO request 中时，scheduler 会持有这段内存直到
`IoResult` 返回。如果 `IoBuffer` 被销毁，它的 deleter 会把内存释放回 leaf
pool。

spill 文件位置用 `OwnedFileExtent` RAII helper 包装。BM 内部持有
`std::shared_ptr<FileBlockAllocator>`；`OwnedFileExtent` 保存 `FileExtent` 和
`std::weak_ptr<FileBlockAllocator>`。析构时 lock allocator 并调用
`Free(extent)`，但析构路径必须 `noexcept`，因此不能抛异常。析构路径如果发现
allocator 已失效或 `Free` 返回非 ok，只记录严重日志并放弃继续处理。

显式释放路径必须检查 `FileFreeResult`。例如 `Pin` 读回成功后释放旧 extent，
如果 `Free` 返回非 ok，则抛 Bolt 异常。此时不回滚已经安装好的 payload；旧
extent 释放失败表示 allocator 元数据或文件状态异常，而不是 block 内容恢复失败。

`BufferManager` 应当比它创建的所有 `BlockHandle`、`BlockMemory` 和
`BufferHandle` 存活更久。weak owner 引用只用于诊断生命周期违规，不表示支持
late destruction。BM 推荐由比所有 block consumer 都长寿的 owner 持有，典型
位置是 query/operator 上下文的根对象，避免 operator 析构顺序或测试局部变量把
BM 提前销毁。

`BufferManager::Create` 的初始化顺序必须固定：

1. 构造 `std::shared_ptr<BufferManager>`。
2. 创建并保存 `std::shared_ptr<FileBlockAllocator>`。
3. 从 parent pool 创建 leaf child pool。
4. 使用 `weak_from_this()` 创建并安装 `BufferManagerReclaimer`。

构造函数中不能调用 `weak_from_this()` 或 `shared_from_this()`，因为此时对象还
没有被 `shared_ptr` 完整接管。leaf child pool 第一版使用默认
`threadSafe=true`；这是可优化点，后续如果确认 BM 始终单线程使用，可以改为
`threadSafe=false` 以减少 MemoryPool 内部锁开销。

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

`initPoolAndReclaimer()` 中才能使用 `manager->weak_from_this()` 安装
`BufferManagerReclaimer`。

## Block 状态

第一版只有三个状态：

```text
IN_MEMORY    payload resident 在 leaf MemoryPool 中
SPILLED      payload 不在内存中；FileExtent 保存可恢复副本
PREFETCHING  已提交异步 read；旧 FileExtent 仍然保留
```

不维护 dirty bit。任何被 reclaim 选中的 in-memory unpinned block 都写入一个
新的 file extent。

### 状态转移

`Allocate(size)`：

```text
new block -> IN_MEMORY(pin_count = 1)
```

`Unpin`：

```text
IN_MEMORY(pin_count > 1) -> IN_MEMORY(pin_count - 1)
IN_MEMORY(pin_count == 1) -> IN_MEMORY(pin_count = 0), 加入 eviction queue
```

`Pin`：

```text
IN_MEMORY    -> pin_count++
SPILLED      -> 同步读取 extent，安装 payload，释放 extent，pin_count = 1
PREFETCHING  -> 等待 prefetch future，安装 payload，释放 extent，pin_count = 1
```

如果 prefetch IO 失败，后续 `Pin` 观察到该结果时，block 回到 `SPILLED`，
然后 `Pin` 再发起一次同步 read。同步 read 失败会抛异常，并保持 block 为
spilled。

`Prefetch`：

```text
SPILLED -> PREFETCHING(read future)
```

`Prefetch` 遇到已经 `IN_MEMORY` 或 `PREFETCHING` 的 block 会跳过。`Prefetch`
是 hint：如果分配 read buffer、提交 IO 或其它准备步骤失败，不向调用方抛异常，
只记录日志/计数，并保持 block 为 `SPILLED`。

`Reclaim`：

```text
IN_MEMORY(pin_count = 0) -> 同步写入新 extent
                         -> 成功后变为 SPILLED
```

Reclaim 会跳过 `SPILLED`、pinned `IN_MEMORY` 和尚未完成的 `PREFETCHING`
block。选择 victim 前，reclaim 会 non-blocking harvest 已 ready 的 prefetch
future。成功 harvest 的 block 变为 `IN_MEMORY(pin_count = 0)`，本轮 reclaim
可以继续 spill 它。

## IO 集成

所有读写请求都使用 `DiskIoScheduler`。

从 `SPILLED` 读回：

1. 从 BM leaf pool 分配一个 `IoBuffer`。
2. 使用 block 的 file extent 提交 read request。
3. 等待 future 完成。
4. 成功后，把 `result.buffer` move 到 `BlockMemory::payload`。
5. 释放旧 `OwnedFileExtent`。
6. 失败时让 `result.buffer` 自动释放内存，然后抛异常。

如果读回成功但释放旧 extent 失败，`Pin` 不回滚已经安装好的 payload，而是抛出
allocator 释放错误。此时 block 内容已经在内存中，失败只表示旧文件空间未能正确
归还给 allocator。

从 `IN_MEMORY` spill：

1. 按 block size 分配一个新的 file extent。
2. 把 `BlockMemory::payload` move 到 write request。
3. 提交 IO 并等待 future。
4. 成功后，保存新的 `OwnedFileExtent`；让 `result.buffer` 释放内存回 pool；
   状态变为 `SPILLED`。
5. 失败时，把 `result.buffer` move 回 `BlockMemory::payload`，释放新 extent，
   保持 `IN_MEMORY`，然后抛异常。

Prefetch：

1. 从 BM leaf pool 分配一个 `IoBuffer`。
2. 提交 read request。
3. 把 returned future 存入 block，状态设为 `PREFETCHING`。
4. 后续 `Pin` 等待该 future；或者 BM API / reclaim 入口用 zero-timeout
   readiness check 做 non-blocking harvest。

如果上述任一步在提交成功前失败，`Prefetch` 不抛异常，block 保持 `SPILLED`。
如果 future 完成后结果失败，下一次 `Pin` 会丢弃失败的 prefetch 结果并重新发起
同步 read。

`BatchPin` 对 batch 中所有 `SPILLED` blocks 先分配 read buffers 并提交 read
futures，再统一等待完成并安装 payload。对 `PREFETCHING` blocks，`BatchPin`
复用已有 future，不重复提交 read。任一 future 失败时，`BatchPin` 抛异常；
已经成功安装并返回到局部结果集中的 handles 会在栈展开时 unpin，尚未安装的
`IoBuffer` 通过 `IoResult` 析构释放回 pool，失败 block 保持或恢复为
`SPILLED`。

## File Allocator 集成

manager 持有一个非线程安全的 `FileBlockAllocator` 实例。这和第一版
BufferManager 的单线程约束一致。

每次 spill write 都分配 fresh extent。`Pin` 或成功 prefetch 会在 payload
安装进内存后释放旧 extent。这样状态模型保持简单：block resident 时不保留磁盘
副本。

## MemoryPool 和 Reclaim 集成

`BufferManager::Create` 创建 leaf child pool，并通过 `setReclaimer` 安装
`BufferManagerReclaimer`。

reclaimer 持有 `std::weak_ptr<BufferManager>`。正常生命周期下 manager 应当
存在；如果 weak pointer 已过期，reclaimer 报告没有可 reclaim 内存。

`reclaimableBytes()` 返回 `unpinnedResidentBytes_`。在单线程约束下，这个值
基本精确，并且不需要扫描 eviction queue。

第一版暂不把 `PREFETCHING` 中已经分配的 read buffer 计入
`reclaimableBytes()`。这些 buffer 已经占用 leaf pool，但在 IO 完成前不能安全
spill。后续优化可以增加 prefetch budget，或把 ready/harvestable prefetch 纳入
reclaimable 估算。

`unpinnedResidentBytes_` 更新规则：

- `Allocate`：不变，因为新 block 是 pinned。
- `Unpin` 到 0 且状态为 `IN_MEMORY`：加 block size。
- `Pin` 一个 unpinned `IN_MEMORY` block：减 block size。
- 从 `SPILLED` 或 `PREFETCHING` `Pin`：不变，因为读回结果直接是 pinned。
- ready prefetch harvest 成 `IN_MEMORY(pin_count = 0)`：加 block size。
- 成功 reclaim 一个 unpinned resident block：减 block size。

`reclaim(targetBytes, maxWaitMs, stats)` 和 `ReclaimForTest` 使用同一套逻辑。
IO 或 file 错误会抛异常。返回值是成功 spill 后实际释放的 resident bytes。

`maxWaitMs` 为了兼容 `MemoryReclaimer` 接口保留。第一版暂时忽略该参数：
一旦开始本轮 reclaim，就同步 spill victim，直到达到 `targetBytes` 或没有更多
victim。后续优化可以在每个 block spill 完成后检查 elapsed time，超过
`maxWaitMs` 则提前返回已经释放的 bytes。

v1 limitation：

- 大量 `Prefetch` 可能占用 leaf pool，但这些 `PREFETCHING` bytes 暂不计入
  `reclaimableBytes()`。第一版依赖调用方控制 prefetch 规模；后续可以增加
  prefetch budget，或把 ready/harvestable prefetch 纳入 reclaimable 估算。
- `maxWaitMs` 暂不生效，单次 reclaim 可能超过 arbitrator 给出的时间预算。后续
  应按 block 粒度检查 elapsed time。

## Eviction Queue

eviction queue 使用 DuckDB 的 append-only sequence-number 模型：

```cpp
struct EvictionEntry {
  std::weak_ptr<BlockMemory> block;
  uint64_t sequence;
};
```

每个 `BlockMemory` 有一个 `evictionSequence`。

- `Unpin` 到 0 时递增 sequence，并追加一个 entry。
- `Pin` 时递增 sequence，使旧 queue entry 失效。
- Reclaim 从队头 pop，跳过 expired weak pointer、sequence mismatch、pinned
  block 和非 `IN_MEMORY` block。

第一版不实现 DuckDB 风格的 purge。stale queue entry 在 reclaim 扫描时通过
`std::deque::pop_front` 自然清理前缀，避免已经扫描过的 entry 长期占用内存。

这是 v1 的明确 limitation：如果长任务中 eviction queue 长度持续超过 live
unpinned block 数的 N 倍，或 queue 自身内存占用超过 BM leaf pool 的 X%，需要
引入 purge/compact。N 和 X 由压测确定，初始建议值为 N=4；X 可以先作为观测指标
保留。

## 线程模型

第一版 `BufferManager` 不是线程安全组件。

调用方必须串行化同一个 manager 实例上的所有 public calls，包括 `Allocate`、
`Pin`、`BatchPin`、`Prefetch`、`ReclaimForTest` 和 handle destruction。
生产 reclaim 必须通过 Bolt 现有 operator/task reclaim 协议进入，保证 BM
reclaim 不和正常 BM API 调用并发。

`BufferManagerReclaimer::reclaimableBytes` 也遵循同一串行化约束。实现中应加入
debug check/guard 来发现违反该约束的并发调用；第一版不要求
`unpinnedResidentBytes_` 使用 atomic。建议所有 BM public API、handle
destruction、`reclaimableBytes` 和 `reclaim` 入口共享同一个 debug-only
single-thread/reentrancy guard。

`Prefetch` 会提交异步 IO，但后台 IO completion 不直接修改 BM 状态。状态安装
发生在 `Pin` 中，或发生在 BM API / reclaim 入口做的 non-blocking harvest 中。

manager-owned file allocator 也不是线程安全对象，只在相同的单线程 BM 约束下
使用。

## 错误处理

内存分配失败和内存仲裁失败使用现有 `MemoryPool` 异常。

file allocator 错误转换成 Bolt 异常，并携带 file error code 和 native errno
信息。

IO scheduler 错误转换成 Bolt 异常，并携带 IO error code 和 native errno 信息。

reclaim write 失败时，必须先把返回的 `IoBuffer` move 回 block，恢复
in-memory payload，再抛异常。

显式释放旧 extent 时，如果 `FileBlockAllocator::Free` 返回非 ok，转换成 Bolt
异常。析构路径中调用 `Free` 时不能抛异常；失败只记录严重日志。

析构策略统一为：

- `BufferHandle::~BufferHandle() noexcept`：正常路径调用 `Unpin`。如果 manager
  已失效、pin count 异常或 block 状态不满足 unpin 条件，走 fatal/abort 诊断，
  不抛异常。
- `OwnedFileExtent::~OwnedFileExtent() noexcept`：best-effort 调用
  `Free(extent)`。如果 allocator 已失效或 `Free` 返回非 ok，记录 `LOG(ERROR)`
  并允许该 extent 泄漏；不在析构中抛异常，也不在析构中二次尝试删除底层文件。
- 显式释放路径承担错误报告责任，失败时抛 Bolt 异常。

该异常会沿 `MemoryReclaimer::reclaim` 向上传播给 memory arbitrator。v1 不在
BM 内部重试 IO，也不吞掉 reclaim write 失败；arbitrator 对该异常是让当前
reclaimer 本轮失败还是让整个仲裁失败，取决于现有 memory arbitration 实现。这个
取舍是有意的：v1 优先保持数据状态正确和错误可见，后续可以在
`BufferManagerConfig` 中增加 retry policy。

内部状态不变量违规使用 `BOLT_CHECK`。

## 测试

单测需要覆盖：

- `Allocate` 返回 pinned handle，并写出可选 `BlockHandle`。
- `BufferHandle` move-only，且只 unpin 一次。
- `Pin` resident block 时不触发 IO。
- `ReclaimForTest` spill unpinned block，并释放 MemoryPool bytes。
- `Pin` spilled block 时读回 payload，并释放旧 file extent。
- `BatchPin` 按输入顺序 pin 所有 blocks；失败时已 pin 的 handles 自动 unpin。
- `BatchPin` 对所有 `SPILLED` blocks 先提交 read futures，再统一等待和安装；
  对 `PREFETCHING` blocks 复用已有 future。
- `Prefetch` 将 `SPILLED` 转为 `PREFETCHING`；`Pin` 能 harvest 该 future。
- `Prefetch` 分配或提交失败时不抛异常，block 保持 `SPILLED`。
- Reclaim 能 harvest ready prefetch，并在同一轮 spill 这些 blocks。
- `PREFETCHING` 不计入第一版 `reclaimableBytes`，并有对应注释/测试约束。
- 大量 `Prefetch` 占用 leaf pool 但不增加 `reclaimableBytes` 的行为。
- Reclaim 跳过 pinned blocks。
- `maxWaitMs` 在 v1 被忽略，reclaim 仍然返回实际释放 bytes。
- Eviction queue stale entry 通过 sequence number 跳过。
- Reclaim 写 IO 失败时恢复 payload，并保持 block resident。
- 显式释放 extent 失败会抛异常；析构释放 extent 失败不抛。
- `BufferHandle` late destruction 触发 fatal 诊断。
- file extent 在 pin、reclaim、destruction 路径中只释放一次。
- `BufferManagerReclaimer::reclaimableBytes` 跟随 `unpinnedResidentBytes_`。
- debug single-thread/reentrancy guard 能发现违反 BM 串行化约束的调用。

集成测试需要验证真实 `MemoryPool`、真实 `FileBlockAllocator` 和
`DiskIoScheduler` 的组合行为；如果运行环境支持对应 IO backend，再覆盖真实 IO
路径。
