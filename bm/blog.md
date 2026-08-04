# 用 BufferManager 重塑 RowContainer：把临时数据变成可调度资源

## 摘要

在执行引擎里，临时数据并不只是“申请一块内存、用完释放”这么简单。排序、聚合、Join、批量写入、分段读回都会制造大量中间状态：有些数据正在被 CPU 访问，有些暂时用不到但未来还要读，有些已经可以释放，有些只能写到磁盘后再回收内存。

传统做法通常让每个容器或算子自己处理 spill：什么时候写盘、写成什么格式、读回后指针是否仍然有效、内存压力下释放哪一部分，都散落在各自逻辑里。这种方式能工作，但随着场景变多，问题会越来越明显：状态机重复、释放粒度粗、全量 spill 容易放大 I/O，读回路径也很难统一优化。

BM RowContainer 的核心变化，是把 row-based 临时数据拆成可被 BufferManager 管理的 block。上层仍然以行容器的方式写入、比较、提取列；底层则用 `BlockHandle` 表示稳定身份，用 `BufferHandle` 表示一次短生命周期访问权。数据是否驻留内存、是否已经写入 backing store、什么时候重新 pin 回来，交给统一的 BufferManager 处理。

这篇文章讨论这套设计真正解决的问题：它不是简单换一个 spill 文件格式，而是把临时数据从“容器内部私有内存”变成“有生命周期、有状态、有 I/O 策略、可被统一回收的资源”。

## 问题：row-based 临时数据为什么难管理

RowContainer 的职责看似直接：把输入向量转成行格式，后续按行比较、排序、读取列或释放。但在真实执行链路中，它会遇到几类矛盾。

第一，row 指针天然依赖物理地址。写入后如果上层长期保存 `char*`，底层就很难自由地把数据搬走、写盘或重新加载。只要裸指针还被认为有效，这块内存就不能真正被调度。

第二，变长列会把问题放大。固定宽度列可以紧凑地放在 row block 中，`VARCHAR`、`VARBINARY` 这类 payload 往往需要额外 heap block。一次逻辑行访问可能同时依赖 row block 和多个 heap block，读回时还要保证 `StringView` 指向正确的新地址。

第三，spill 不能只看“写出多少字节”。如果容器只能全量 spill，那么内存压力来临时会写出很多短期内可能马上又要读的数据；如果粒度过细，又会导致 pin、IO、metadata 管理成本过高。

第四，I/O 不是单个容器能独立优化好的。多个执行组件同时读写临时文件时，需要统一控制 queue depth、优先级、错误传播和统计，否则局部优化容易变成全局竞争。

因此，一个更稳妥的方向是：让 RowContainer 继续负责行布局和行语义，让 BufferManager 负责 block 生命周期和内存回收。

## 核心思路：身份和访问权分离

BufferManager 的关键抽象是把“这块数据是谁”和“现在能不能访问它”拆开。

```text
BlockHandle
    稳定的逻辑身份
    可长期保存在容器 metadata 中
    不保证 payload 当前在内存

BufferHandle
    一次 pin 产生的访问权
    RAII 持有 resident payload
    析构后自动 unpin
    析构后 Ptr() 返回的地址不能继续使用
```

这带来一个重要约束：调用方可以保存 `BlockHandle`，但不能长期保存未受保护的 payload 指针。需要访问数据时必须 pin，使用结束后释放 `BufferHandle`。

一个 block 的生命周期可以简化为：

```text
Allocate
   |
   v
InMemory + pinned
   |
   | BufferHandle 析构
   v
InMemory + unpinned
   |
   | reclaim / SpillBlocks
   v
Spilling
   |
   | write complete
   v
Spilled + backing segment
   |
   | Pin / BatchPin
   v
Prefetching / reading
   |
   | read complete
   v
InMemory + pinned
```

这套模型的收益不只在 spill。它让内存回收、读回、预取、压缩、文件空间管理和统计都围绕同一个 block 状态机展开。

## 整体架构

BM RowContainer 可以分成两层：上层是行容器语义，下层是 BufferManager 基础设施。

```text
                 +----------------------+
                 |    BmRowContainer    |
                 | row layout / compare |
                 | extract / copy/store |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 |  BmSegmentCollection |
                 | segment / chunk meta |
                 +----------+-----------+
                            |
          +-----------------+-----------------+
          |                                   |
          v                                   v
+-------------------+              +-------------------+
| rowBlock          |              | heapBlocks        |
| fixed-width rows  |              | variable payloads |
+---------+---------+              +---------+---------+
          |                                  |
          v                                  v
   +-----------------------------------------------+
   |                BufferManager                  |
   | Allocate / Pin / BatchPin / Spill / Reclaim   |
   +----------------------+------------------------+
                          |
          +---------------+----------------+
          |                                |
          v                                v
 +----------------+              +------------------+
 | SpillStore     |              | DiskIoScheduler  |
 | compression    |              | priority/depth   |
 | file segment   |              | io_uring backend |
 +----------------+              +------------------+
```

这里的边界很明确：

| 层次 | 主要职责 |
|---|---|
| `BmRowContainer` | 维护 row layout，提供写入、比较、列提取等行语义接口 |
| `BmSegmentCollection` | 管理 segment、chunk、row block、heap block 的元数据和生命周期 |
| `BufferManager` | 管理 block pin/unpin、resident/spilled 状态、reclaim 和统计 |
| `SpillStore` | 把 block 编码成 spill record，负责压缩、解压和文件 segment 绑定 |
| `DiskIoScheduler` | 异步提交 read/write，控制优先级、inflight depth 和完成统计 |

这样做的核心价值是：RowContainer 不需要自己实现一整套文件级 spill 系统，BufferManager 也不需要理解行格式。

## RowContainer 的数据组织

BM RowContainer 使用 `segment -> chunk -> block` 的层次组织数据。

```text
Segment
  |
  +-- Chunk 0
  |     |
  |     +-- rowBlock    : 连续行槽位
  |     +-- heapBlocks  : 当前 chunk 内变长 payload
  |     +-- heapBases   : StringView rebase 所需的 heap 基址信息
  |
  +-- Chunk 1
        |
        +-- rowBlock
        +-- heapBlocks
        +-- heapBases
```

一个 chunk 当前锚定一个 row block，并拥有这个 row block 中行所引用的 heap blocks。这个设计没有把 heap payload 切得极细，原因是现有写入路径通常先分配 row slot，再逐列写入，变长 payload 的最终大小在写行过程中才逐步确定。以 row block 为 chunk 边界，可以让写路径保持简单，也能让读回时 row block 和 heap blocks 作为一个一致单元处理。

代价也很直接：变长列的 pin 和释放粒度会比更细的 part 模型粗一些。对于大字符串或变长 payload 分布很不均匀的场景，heap block 的局部性和重读成本会更敏感。

## 写入流程：先保持热路径简单

写入阶段，RowContainer 仍然提供接近传统行容器的接口：分配一行，逐列 store，或者批量 append。

```text
appendBatch / appendRow
    |
    v
active segment
    |
    v
ensure writable chunk
    |
    +-- rowBlock 不够：Allocate 新 row block
    +-- heapBlock 不够：Allocate 新 heap block
    |
    v
写 fixed-width 字段、null bits、StringView
```

`Allocate()` 返回的是已经 pinned 的 `BufferHandle`，因此写入热路径可以直接拿到 `Ptr()` 填充内存。对于变长列，payload 写入 heap block，row 内部保存 `StringView`。

批量写入路径会按连续 row range 预留空间，减少逐行分配和初始化成本。这也是 benchmark 中 store 阶段收益明显的来源之一：BM RowContainer 不只是“能 spill”，写入路径本身也围绕块和批量追加做了整理。

## Flush 与 spill：释放指针，而不是丢掉身份

当一个 active segment 完成写入后，容器会 finalize 并 flush：

```text
finalize segment
    |
    +-- 标记为 finalized resident
    +-- 清理未使用 heap tail，避免写出未定义内容
    +-- 释放 rowBlock / heapBlock 的 BufferHandle
    +-- 保留每个 block 的 BlockHandle
    +-- 调用 SpillBlocks 主动写出可 spill block
    |
    v
finalized flushed
```

这一步有两个关键点。

第一，释放的是访问权，不是逻辑身份。`BufferHandle` 被清空后，旧指针不能再使用，但 `BlockHandle` 仍然保留在 chunk metadata 里，后续可以通过 pin 读回。

第二，主动 `SpillBlocks()` 不是唯一回收路径。BufferManager 也会在内存仲裁触发 reclaim 时，从 eviction queue 中选择 unpinned resident block 写出。主动 flush 更适合阶段边界明确的场景：写完一个 segment 后立即把它变成可读回、可释放的冷数据。

## 读回流程：按工作集选择 bulk 或局部加载

读回时，RowContainer 不假设所有数据都能一次回到内存。它提供两类典型路径。

第一类是 bulk read：如果一组 segments 的未加载 block 预计可以放入内存，就一次性加载。

```text
canBulkRead(segments)
    |
    +-- 估算未加载 bytes
    +-- MaybeReserve(bytes)
    +-- 释放探测 reservation

beginBulkReadSegments
    |
    +-- ensureSegmentsLoaded
    +-- BatchPin row/heap blocks
    +-- 返回 resident row pointers
```

这里的判断是保守的：只有仍持有 `BufferHandle` 的 block 才被认为已经加载。已经 unpin 但看起来仍 resident 的 block，会按可能需要重新加载计入预算，因为容量探测本身可能触发 reclaim。

第二类是局部读：调用方用 `RowId` 或 segment-local range 表示需要访问的行，读 session 去重到 chunk 粒度，再批量 pin 相关 row block 和 heap blocks。

```text
requested rows / ranges
    |
    v
locate chunks
    |
    v
BatchPin missing rowBlock + heapBlocks
    |
    v
必要时 rebase StringView
    |
    v
返回本批 resident row pointers
```

这条路径适合工作集远小于完整数据集的场景。它避免把所有 segment 一次性读回，也让上层可以在当前批次不再使用后释放 session 持有的 pin。

## 变长列读回：为什么需要 rebase

固定宽度字段读回后只要 row block 地址正确即可。变长列更麻烦：row block 里的 `StringView` 可能指向 heap block 的旧地址，而 heap block 重新 pin 回来后，payload 地址可能已经变化。

因此，加载 chunk 时需要同时处理 row block、heap blocks 和 heap base metadata：

```text
BatchPin rowBlock / heapBlocks
    |
    v
更新每个 BlockRef.ptr
    |
    v
根据 heap block 新基址修正 rowBlock 中的 StringView
    |
    v
如果 rowBlock 内容被修正，标记 dirty
```

`dirty` 状态很重要。一个 block 如果已经有 backing store 且内容没有变化，释放 resident payload 时可以直接丢弃；如果 rebase 或写入导致 resident 内容比 backing store 更新，就必须重新写回，否则下次读回会拿到旧地址或旧内容。

这也是 BM 状态机里区分 clean 和 dirty 的原因：

| 状态 | 处理方式 |
|---|---|
| clean resident + 有 backing | 可以直接 discard resident payload |
| dirty resident | 需要 spill/write back 后才能释放内存 |
| spilled | 后续 pin 时从 backing store 读回 |
| prefetching | 已提交读请求，pin 时等待结果 |

## Reclaim：把内存压力转成 block 级决策

BufferManager 在自己的 leaf memory pool 上安装 reclaimer。当上层内存仲裁希望释放内存时，reclaimer 调用 `Reclaim(targetBytes)`。

```text
Memory arbitration
    |
    v
BufferManagerReclaimer
    |
    v
BufferManager::Reclaim(targetBytes)
    |
    v
EvictionQueue::PopEvictable()
    |
    +-- generation 不匹配：跳过 stale entry
    +-- pinCount > 0：跳过
    +-- 非 InMemory：跳过
    |
    v
SpillWriteDriver
    |
    +-- BeginSpill: payload 移出 block
    +-- SubmitWriteBlock
    +-- 等待写完成
    +-- CompleteSpill: 记录 backing segment
```

eviction queue 使用 generation token 做延迟校验。block 每次 pin/unpin 或状态变化都会更新 sequence，队列里的旧 entry 不需要同步删除，pop 时发现过期再跳过即可。这个设计降低了热路径维护队列的复杂度。

写盘由 `SpillWriteDriver` 控制并发窗口。它不会无限制提交写请求，而是在 `maxReclaimWriteInflight` 内滚动提交和收割完成结果。这样可以在释放内存和避免 I/O 风暴之间取得平衡。

## SpillStore：文件、压缩和物理字节数

BufferManager 不直接拼写临时文件。它把 block payload 交给 `SpillStore`，后者完成三件事：

1. 根据压缩配置构造自描述 spill record。
2. 向文件分配器申请 `fd + offset + size`。
3. 通过 IO scheduler 提交 positional write。

读回时流程相反：

```text
Pin spilled block
    |
    v
SubmitReadBlock(segment, expectedRawSize)
    |
    v
读物理 spill record
    |
    v
解压到 BufferManager 的 MemoryPool
    |
    v
block 回到 InMemory + pinned
```

压缩收益取决于数据分布和 CPU 成本。LZ4、ZSTD 可以显著减少某些变长数据的物理写出，但并不保证总耗时一定下降。若数据本身不可压，或者压缩 CPU 成为主瓶颈，物理 I/O 下降可能被压缩耗时抵消。

## DiskIoScheduler：统一 I/O 背压

BM 的 spill read/write 通过统一的 `DiskIoScheduler` 提交。它对外返回 `std::future<IoResult>`，内部维护优先级队列、inflight registry 和 depth controller。

```text
submit(IoRequest)
    |
    +-- validate fd / buffer / offset / length
    +-- enqueue by priority
    |
    v
scheduler thread
    |
    +-- weighted fair dispatch
    +-- currentDepth 限制 inflight
    +-- backend submit
    +-- reap completions
    +-- fulfill promise
```

优先级分为 High、Medium、Low，默认策略是加权公平，而不是简单让低优先级饿死。有效 inflight depth 可以根据吞吐反馈调整，避免固定 queue depth 在不同磁盘和 workload 下表现僵硬。

这层的意义是把 I/O 管理从 RowContainer 中拿出来。RowContainer 不需要理解 io_uring，也不需要自己维护异步完成队列；它只需要通过 BufferManager 发起 pin、spill 或 prefetch。

## 性能结果：收益来自整条链路

一次单线程 end-to-end pipeline benchmark 覆盖了 store、spill write、spill read、read 四个阶段。每个 case 使用 25 GiB 逻辑数据量，比较原 RowContainer 和 BM RowContainer。

总体结果如下：

| 数据集 | 压缩 | 原总耗时 | BM 总耗时 | 加速比 | BM / 原 |
|---|---:|---:|---:|---:|---:|
| fixed | raw | 3.66 min | 39.04 s | 5.62x | 17.8% |
| fixed | lz4 | 3.80 min | 1.52 min | 2.49x | 40.1% |
| fixed | zstd | 6.10 min | 4.08 min | 1.50x | 66.8% |
| variable_small | raw | 2.57 min | 50.62 s | 3.05x | 32.8% |
| variable_small | lz4 | 3.63 min | 1.67 min | 2.17x | 46.2% |
| variable_small | zstd | 5.40 min | 3.56 min | 1.52x | 66.0% |
| variable_large | raw | 1.80 min | 30.57 s | 3.52x | 28.4% |
| variable_large | lz4 | 40.07 s | 24.31 s | 1.65x | 60.7% |
| variable_large | zstd | 46.01 s | 34.58 s | 1.33x | 75.1% |

几个结论比较明确。

第一，raw 场景收益最强，尤其是 fixed/raw，总耗时从 3.66 分钟降到 39.04 秒。这里没有压缩 CPU 干扰，BM RowContainer 在批量写入、块级 spill、块级读回上的收益更直接。

第二，spill read 是非常重要的收益来源。fixed/raw 的 spill read 阶段从 2.40 分钟降到 18.80 秒，提升约 7.67x；variable_large/raw 从 85.01 秒降到 13.45 秒，提升约 6.32x。这说明收益不只是写得快，更来自读回路径的块级组织和批量 pin。

第三，zstd 的总收益更保守。fixed/zstd 仍有 1.50x，variable_large/zstd 为 1.33x，但压缩 CPU 明显抵消了部分容器收益。对于这类场景，继续优化时需要区分“容器元数据和读写路径成本”与“压缩算法成本”，否则容易把瓶颈归因错。

第四，物理 spill 字节数并不总是更少。fixed/raw 和 variable_small/raw 下，BM 的物理写出略高于原实现；但总耗时仍显著下降。这说明 BM RowContainer 的收益不依赖“写出字节一定更少”，而来自更低的写入/读回组织成本、更好的批处理，以及更清晰的生命周期管理。

物理写出对比如下：

| 数据集 | 压缩 | 原 spill bytes | BM physical spill bytes | BM / 原 |
|---|---:|---:|---:|---:|
| fixed | raw | 26.25 GiB | 30.00 GiB | 114.3% |
| fixed | lz4 | 25.39 GiB | 25.86 GiB | 101.9% |
| fixed | zstd | 22.00 GiB | 21.69 GiB | 98.6% |
| variable_small | raw | 32.21 GiB | 37.74 GiB | 117.2% |
| variable_small | lz4 | 18.99 GiB | 19.87 GiB | 104.6% |
| variable_small | zstd | 15.27 GiB | 15.81 GiB | 103.5% |
| variable_large | raw | 25.41 GiB | 25.87 GiB | 101.8% |
| variable_large | lz4 | 1.15 GiB | 1.12 GiB | 97.2% |
| variable_large | zstd | 0.80 GiB | 0.78 GiB | 98.6% |

这个结果提醒我们：BufferManager 不是魔法压缩器。它真正改变的是临时数据的生命周期和访问路径。压缩能否带来额外收益，要看数据可压缩性、CPU 预算和 I/O 压力之间的平衡。

## 为什么这套设计能提升可维护性

可维护性的提升来自职责收敛。

过去，容器或算子容易把几类逻辑混在一起：

```text
行格式
  + 内存是否足够
  + spill 文件怎么写
  + 读回后地址如何恢复
  + 哪些数据可以释放
  + I/O 错误怎么传播
```

BM RowContainer 把它们拆开：

```text
RowContainer:
    负责 row layout、写入、比较、提取列、segment/chunk metadata

BufferManager:
    负责 block 状态机、pin/unpin、reclaim、resident/spilled 转换

SpillStore:
    负责 spill record、压缩、解压、文件 segment

DiskIoScheduler:
    负责异步 I/O、优先级、公平性、depth、stats
```

新的接入方不需要重新发明文件级 spill 状态机。它只需要决定自己的工作集如何切分成 block、什么时候释放 pin、什么时候适合 bulk read 或局部 read。

## 工程权衡和限制

这套设计也有明确成本。

第一，调用方必须尊重 pin 生命周期。`BufferHandle` 析构后，旧 `Ptr()` 不能再使用。任何长期保存裸 row 指针的代码，都需要迁移到 segment range、`RowId`、或者受 session 管理的短生命周期 resident 指针。

第二，chunk 粒度是折中。一个 chunk 绑定一个 row block 和若干 heap blocks，读写一致性清晰，但变长列场景下可能 pin 住比当前访问行更多的 payload。更细粒度的组织能降低局部读放大，但会提高 metadata 和写路径复杂度。

第三，读回不是完全免费的。`BatchPin` 可以合并多个 block 的读请求，但如果访问模式高度随机、每次只读很少行，pin、解压、StringView rebase 的固定成本仍然会显现。

第四，压缩需要按 workload 选择。LZ4 更偏低 CPU 开销，ZSTD 更偏压缩率；如果瓶颈是 CPU 而不是磁盘，强压缩可能让总耗时变差或收益变小。

第五，BufferManager 让内存行为更动态，也意味着调试需要更好的统计。单看某个容器的逻辑大小不够，还要看 pinned resident、unpinned resident、spilled、prefetching、spilling、物理读写字节、压缩耗时和 I/O 失败等指标。

## 适用场景

BM RowContainer 更适合以下场景：

- 临时数据量可能超过可用内存，但仍需要后续多阶段读取。
- 数据可以自然切分成 segment/chunk/block。
- 上层可以接受短生命周期 row 指针，或者能用 `RowId` / range 描述读集。
- spill read/write 成本在总耗时中占比较高。
- 需要统一 I/O 调度、压缩和统计，而不是每个容器各做一套。

收益不明显甚至不适合的场景包括：

- 数据集很小，永远 resident，生命周期管理成本可能超过收益。
- 访问模式极端随机，局部读放大和频繁 pin 成本很高。
- 上层必须长期保存裸指针且短期内无法改造。
- 压缩 CPU 已经是主瓶颈，进一步压缩无法换来足够 I/O 节省。

## 总结

BM RowContainer 的关键价值不是“多了一个 spill 实现”，而是把 row-based 临时数据纳入统一的 block 生命周期管理。

`BlockHandle` 保留逻辑身份，`BufferHandle` 管理短期访问权；segment/chunk/block 让 RowContainer 能在行语义和块级资源管理之间建立边界；SpillStore、file allocator、compression 和 DiskIoScheduler 则把临时数据的物理读写收敛到统一基础设施。

这套设计带来的性能收益来自整条链路：批量写入更紧凑，flush 后可主动释放，读回能按 block 批量 pin，clean block 可直接 discard，dirty block 才写回，I/O 和压缩也有统一策略。更重要的是，它让后续执行组件可以把精力放在“如何切分和消费工作集”上，而不是反复实现底层 spill 文件和内存回收状态机。
