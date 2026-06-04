# DuckDB 中 TupleDataCollection 与 BufferManager 的结合：实现原理与设计意图

DuckDB 的执行引擎通常以列式 `DataChunk` 为单位传递数据。一个 `DataChunk` 里每一列是一个 `Vector`，这对扫描、过滤、表达式计算非常友好。但有些算子不适合一直保持列式表示，比如排序、哈希聚合、哈希连接、去重、外部化中间结果等。这些场景经常需要把多列数据 materialize 成一行一行的布局，方便比较、哈希、移动、spill 或后续 gather。

`TupleDataCollection` 就是 DuckDB 内部用来保存这种 row-format 中间数据的容器。它不是普通的 `std::vector<Row>`，而是建立在 `BufferManager` 之上的 block-based 容器。它的核心目标是：让行式中间数据也进入 DuckDB 统一的内存管理体系，能够被 pin/unpin、受 memory limit 控制，并在必要时 spill 到临时文件。

---

## 1. 从列式 DataChunk 到行式 TupleDataCollection

外部看，`TupleDataCollection` 的语义比较简单：

```text
Append(DataChunk)
Scan(...) -> DataChunk
Gather(...)
Count()
ChunkCount()
FetchChunk(...)
Combine(...)
Unpin()
Reset()
```

也就是说，调用方通常把它当成一个“可追加、可扫描的 row-format tuple 容器”。调用方 append 的仍然是列式 `DataChunk`，scan 出来的也仍然是列式 `DataChunk`。行式布局主要是内部实现细节。

整体流程可以简化成：

```text
DataChunk
  col0: Vector
  col1: Vector
  col2: Vector
       |
       v
TupleDataCollection::Append
       |
       v
ToUnifiedFormat
       |
       v
ComputeHeapSizes
       |
       v
TupleDataAllocator::Build
       |
       v
Scatter
       |
       v
row_blocks + heap_blocks
```

其中：

```text
row_blocks:
  存每一行的固定长度部分

heap_blocks:
  存 VARCHAR、LIST、BLOB 等变长内容
```

例如一行里有一个 1KB 的字符串：

```text
逻辑行:
  key = 1
  name = "....1KB...."
  payload = 100

物理布局:

row block:
+------------------------------------------------+
| key | string_t header | string_t.ptr | payload |
+------------------------------------------------+
                         |
                         v
heap block:
+------------------------------------------------+
| 1KB string bytes                               |
+------------------------------------------------+
```

row block 里不会直接放完整 1KB 字符串，而是放 `string_t` 的定长部分和一个指向 heap block 的 pointer。真实字符串内容放在 heap block。

---

## 2. 为什么要通过 BufferManager？

如果这些行式中间数据直接用普通堆内存分配，问题会很明显：

```text
ORDER BY / GROUP BY / JOIN 的中间数据太大
        |
        v
普通 malloc 持有全部数据
        |
        v
内存不足时只能 OOM
```

DuckDB 选择让这些 row-format 数据走 `BufferManager`，于是数据被拆成一个个 block：

```text
TupleDataCollection
  ├── row block 0
  ├── row block 1
  ├── heap block 0
  ├── heap block 1
  └── ...
```

每个 block 有稳定的 `BlockHandle`。需要读写时，通过 `BufferManager::Pin` 得到 `BufferHandle`，再拿到当前内存地址。用完后释放 pin，BufferManager 就可以在内存压力下把 block 驱逐、复用或写到临时文件。

核心区别是：

```text
BlockHandle:
  表示这块数据逻辑上存在

BufferHandle:
  表示这块数据当前被 pin 在内存里，可以安全访问

Ptr():
  只有在 pin 期间才是安全地址
```

这让 row-format 中间结果也能参与 DuckDB 的统一内存预算和 spill 机制。

---

## 3. TupleDataCollection 的内部层级

`TupleDataCollection` 内部不是一个扁平数组，而是几层结构：

```text
TupleDataCollection
└── TupleDataSegment
    └── TupleDataChunk
        └── TupleDataChunkPart
            ├── row block slice
            └── heap block slice
```

可以按职责理解：

```text
TupleDataCollection:
  对外容器。提供 Append、Scan、Gather、Combine 等接口。

TupleDataSegment:
  内部数据段。拥有自己的 allocator、chunks、chunk_parts、pin handles。
  是 combine、scan、pin 生命周期的边界。

TupleDataChunk:
  逻辑扫描单位。类似 row-format 内部的 DataChunk。
  最多包含 STANDARD_VECTOR_SIZE 行。

TupleDataChunkPart:
  物理连续片段。描述一段 rows 在哪个 row block、哪个 heap block、什么 offset。
```

更直观地看：

```text
逻辑层:

TupleDataChunk C0
+------------------------------------------+
| row 0                                    |
| row 1                                    |
| ...                                      |
| row N                                    |
+------------------------------------------+

物理层:

C0
├── Part P0 -> row_block_0 offset A, heap_block_0 offset X, count 300
├── Part P1 -> row_block_0 offset B, heap_block_1 offset Y, count 400
└── Part P2 -> row_block_1 offset C, heap_block_1 offset Z, count 324
```

`TupleDataChunk` 是“扫描时的一批数据”，`TupleDataChunkPart` 是“这批数据在物理 block 里的连续切片”。

---

## 4. TupleDataSegment 为什么存在？

`TupleDataSegment` 的意义是把一组 allocator、blocks、chunks、parts 和 pinned handles 作为一个独立数据段管理。

它大致包含：

```text
TupleDataSegment
  allocator
  layout

  chunks
  chunk_parts

  count
  data_size

  pinned_row_handles
  pinned_heap_handles
```

为什么不直接让 `TupleDataCollection` 持有所有 blocks 和 chunks？一个重要原因是 `Combine`。DuckDB 经常会先在多个本地状态或分区里独立构建 row-format 数据，之后再合并。如果没有 segment，合并时可能需要复制 row/heap block，或者重写所有 part 里的 block index。

有了 segment，合并可以更像移动数据段：

```text
Before:

Collection A
└── Segment A0

Collection B
├── Segment B0
└── Segment B1

After A.Combine(B):

Collection A
├── Segment A0
├── Segment B0
└── Segment B1
```

每个 segment 自己带着 allocator、row blocks、heap blocks、chunks、parts，所以移动 segment 不需要重写内部索引。

---

## 5. TupleDataChunk 是什么语义？

`TupleDataChunk` 是内部的 vectorized scan unit。它不是直接暴露给普通调用方的对象，但 `TupleDataCollection` 的 scan、fetch、chunk index 语义都围绕它组织。

一个 `TupleDataChunk` 保存：

```text
part_ids
row_block_ids
heap_block_ids
count
lock
```

它的作用是：

```text
1. 把多个 TupleDataChunkPart 组织成一个逻辑 chunk；
2. 保证这个逻辑 chunk 的行数不超过 STANDARD_VECTOR_SIZE；
3. 记录扫描这个 chunk 需要哪些 row blocks；
4. 记录扫描这个 chunk 需要哪些 heap blocks；
5. 帮助 scan 时只 pin 当前 chunk 需要的 blocks；
6. 帮助释放不再需要的 pinned handles。
```

示意：

```text
TupleDataChunk C0
+------------------------------------------------+
| part_ids       = [P0, P1, P2]                  |
| row_block_ids  = {R0, R1}                      |
| heap_block_ids = {H0, H1}                      |
| count          = 1024                          |
+------------------------------------------------+

扫描 C0 时:
  pin R0, R1
  pin H0, H1
```

这样 DuckDB 不需要为了扫描一个 chunk 而 pin 住整个 `TupleDataCollection`。

---

## 6. TupleDataChunkPart 是什么语义？

`TupleDataChunkPart` 是物理连续性边界。它记录：

```text
row_block_index
row_block_offset
count

heap_block_index
heap_block_offset
total_heap_size
base_heap_ptr
lock
```

也就是：

```text
这几行 row 在哪个 row block 的哪一段？
这几行的变长数据在 哪个 heap block 的哪一段？
这几行有多少行？
这几行 row 里的 heap pointer 是基于哪个 heap base address 生成的？
```

为什么需要 part？因为一个逻辑 chunk 不一定能物理连续地放进一个 row block 或 heap block。

例如 row block 剩余空间不够：

```text
TupleDataChunk C0
├── Part P0: row_block_0 offset A, count 300
└── Part P1: row_block_1 offset 0, count 724
```

或者 heap block 剩余空间不够：

```text
TupleDataChunk C0
├── Part P0: row_block_0, heap_block_0, count 100
├── Part P1: row_block_0, heap_block_1, count 50
└── Part P2: row_block_1, heap_block_1, count 874
```

所以 `TupleDataChunkPart` 不只是为了 spill。它首先是物理切片单位。

---

## 7. Append 时如何分配 row/heap block？

append 时，`TupleDataAllocator::Build` 会创建或复用 row block 和 heap block，并生成一个或多个 `TupleDataChunkPart`。

简化流程：

```text
Append DataChunk
  |
  v
ComputeHeapSizes
  |
  v
Build
  |
  v
BuildChunkPart
  |
  +-- 找 row block
  +-- 确定 row_block_index / row_block_offset
  +-- 根据 row block 剩余空间确定 count
  |
  +-- 如果有变长数据:
        计算 total_heap_size
        找 heap block
        如果 heap block 放不下，就截断 count
        设置 heap_block_index / heap_block_offset
        设置 base_heap_ptr
```

如果某一行有很大的变长字段，比如 200MB 的字符串，DuckDB 不会把这 200MB 放进 row block，而是创建足够大的 heap block，让这个 part 可能只包含一行：

```text
Part P
+--------------------------------+
| count             = 1          |
| row_block_index   = R          |
| heap_block_index  = H          |
| total_heap_size   = 200MB      |
| base_heap_ptr     = 当前地址   |
+--------------------------------+
```

这说明 `TupleDataChunkPart` 也支持“大行导致一个 part 只装一行”的情况。

---

## 8. 大字符串的存储方式

以 `VARCHAR` 为例。小字符串可以 inline 在 `string_t` 里；大字符串会放到 heap block。

append/scatter 时，大致发生：

```text
source string = 1KB

1. 在 heap block 里找到 heap_location
2. 把 1KB bytes copy 到 heap_location
3. 在 row block 里写 string_t header
4. 在 row block 里写 string_t.ptr = heap_location
5. heap_location += string_size
```

示意：

```text
row block:
+------------------------------------------------+
| len=1024 | prefix | ptr = 0x10002400           |
+------------------------------------------------+
                          |
                          v
heap block:
base = 0x10000000
+------------------------------------------------+
| offset 0x2400: 1KB string bytes                |
+------------------------------------------------+
```

注意：`ptr = 0x10002400` 只是当前 heap block 被 pin 在 `0x10000000` 时的运行时地址。它不是永久语义。

---

## 9. 为什么 pointer 会失效？

BufferManager 允许 block unpin。unpin 后，如果内存紧张，heap block 可以被 evict/spill。之后再次 pin 同一个 heap block 时，它可能出现在不同的虚拟地址。

例如 append 时：

```text
heap_block_2 base = 0x10000000
row.string_t.ptr = 0x10002400
```

后来 block 被 unpin、spill，再重新 pin：

```text
heap_block_2 new base = 0x70000000
```

此时旧的：

```text
0x10002400
```

就不能再解引用了。

---

## 10. DuckDB 如何修 pointer？

DuckDB 当前实现不是给整个 `TupleDataCollection` 设置一个全局 `POINTER_MODE` 或 `OFFSET_MODE`。它是按 `TupleDataChunkPart` 记录 `base_heap_ptr`。

append 时，某个 part 记录：

```text
part.base_heap_ptr = 0x10000000
part.heap_block_offset = 0x2000
```

row 里的字符串指针是：

```text
row.string_t.ptr = 0x10002400
```

扫描这个 part 时，DuckDB 会重新 pin heap block，得到：

```text
new_base_heap_ptr = 0x70000000
```

然后比较：

```text
part.base_heap_ptr != new_base_heap_ptr
```

如果不同，就说明这个 part 中的 row pointers 可能过期，需要 rebase。

核心公式：

```text
old_heap_ptr = part.base_heap_ptr + part.heap_block_offset
new_heap_ptr = new_base_heap_ptr + part.heap_block_offset

diff = old_string_ptr - old_heap_ptr
new_string_ptr = new_heap_ptr + diff
```

带数字：

```text
part.base_heap_ptr     = 0x10000000
part.heap_block_offset = 0x2000

old_heap_ptr           = 0x10002000
old_string_ptr         = 0x10002400

diff = 0x10002400 - 0x10002000
     = 0x400

new_base_heap_ptr      = 0x70000000
new_heap_ptr           = 0x70002000

new_string_ptr         = 0x70002000 + 0x400
                       = 0x70002400
```

于是 row 中的指针从：

```text
0x10002400
```

被修成：

```text
0x70002400
```

ASCII：

```text
Before reload:

P.base_heap_ptr = 0x10000000
P.heap_offset   = 0x2000

row.ptr         = 0x10002400

old_heap_ptr    = 0x10002000
diff            = 0x400


After reload:

new_base        = 0x70000000
new_heap_ptr    = 0x70002000

row.ptr         = 0x70002400
```

这就是 DuckDB 在 `RecomputeHeapPointers` 中做的事。它本质上利用的是 block 内相对偏移，而不是相信旧虚拟地址一直有效。

---

## 11. 是否存在“spill 一半”？

存在。BufferManager 是 block 级管理的，某些 blocks 可以还在内存里，某些已经被 evict/spill，某些当前被 scan pin 住。

例如：

```text
TupleDataCollection
└── Segment
    ├── Part P0 -> row_block_0, heap_block_0
    ├── Part P1 -> row_block_0, heap_block_1
    ├── Part P2 -> row_block_1, heap_block_1
    └── Part P3 -> row_block_2, heap_block_2
```

某一时刻可能是：

```text
heap_block_0: still pinned
heap_block_1: evicted / spilled
heap_block_2: reloaded at new address
row_block_0:  pinned
row_block_1:  not pinned
row_block_2:  pinned
```

DuckDB 不需要把整个 collection 标记成某种全局状态。它只在访问某个 part 时检查：

```text
这个 part 的 heap block 当前 base 是否等于 part.base_heap_ptr？
```

如果不等，就只修这个 part。

```text
Scan P3:
  Pin(heap_block_2)
  new_base != P3.base_heap_ptr
  RecomputeHeapPointers(P3 only)
```

P0、P1、P2 不需要一起处理。

---

## 12. Scan 时如何组织？

scan 不是直接按 block 扫，也不是直接按 part 扫，而是按：

```text
segment_index, chunk_index
```

来扫描。

流程大致是：

```text
TupleDataCollection::Scan
  |
  v
NextScanIndex
  |
  v
ScanAtIndex(segment_index, chunk_index)
  |
  v
TupleDataAllocator::InitializeChunkState
  |
  +-- 找到当前 TupleDataChunk
  +-- 找到当前 chunk 的 parts
  +-- pin 当前 chunk 需要的 row blocks
  +-- pin 当前 chunk 需要的 heap blocks
  +-- 如果 heap base 变了，recompute pointers
  |
  v
Gather
  |
  v
输出 DataChunk
```

其中 `TupleDataChunk` 的 `row_block_ids` 和 `heap_block_ids` 用来决定当前 scan 需要哪些 blocks。当前 chunk 不需要的 blocks 可以释放 pin，释放后 BufferManager 就有机会在内存压力下 evict 它们。

---

## 13. TupleDataCollection 对外到底暴露什么？

对外它不是暴露 `TupleDataChunk` 对象，而是暴露更高层的容器语义：

```text
Append(DataChunk)
Scan(...) -> DataChunk
Gather(...)
Count()
ChunkCount()
FetchChunk(chunk_idx)
Seek(...)
Combine(...)
Unpin()
Reset()
```

调用方通常不直接操作 `TupleDataChunk` 或 `TupleDataChunkPart`。这些是内部组织 row-format 数据、支持 vectorized scan、block pin/unpin 和 pointer rebasing 的结构。

所以可以这样理解：

```text
TupleDataCollection:
  对外是“可 append/scan 的 row-format tuple 容器”。

TupleDataChunk:
  内部是“vectorized scan 单位”。

TupleDataChunkPart:
  内部是“物理连续切片”。

TupleDataSegment:
  内部是“独立 allocator / metadata / pin 生命周期的数据段”。
```

---

## 14. 整体设计意图

DuckDB 这样设计的目的可以总结为几件事。

第一，执行层仍然保持 vectorized 风格。外部 append/scan 仍然是 `DataChunk`，内部 row-format 只是为了特定算子的效率。

第二，row-format 数据进入 BufferManager 管理。中间结果不再是普通堆内存，而是 block-based，可以被 pin/unpin，也可以在内存压力下 spill。

第三，固定长度和变长数据分离。row block 保存固定长度 row，heap block 保存大字符串、list、blob 等变长内容。这既保持 row 比较紧凑，也支持大变长值。

第四，通过 `TupleDataChunkPart::base_heap_ptr` 解决 pointer 失效问题。row 里可以保留 CPU 友好的 pointer 形式；如果 heap block 被重新 pin 到新地址，扫描当前 part 时再局部修正。

第五，支持部分 spill。因为管理单位是 block 和 part，而不是整个 collection，所以一部分 block 可以在内存，另一部分可以 spill；访问到哪个 part，就修哪个 part。

第六，支持零拷贝 combine。`TupleDataSegment` 让多个独立构建的 row-format 数据段可以直接挂到同一个 collection 下，不需要复制所有 row/heap blocks 或重写 part 索引。

---

## 15. 一句话总结

`TupleDataCollection` 是 DuckDB 用来保存 row-format 中间数据的 BufferManager-backed 容器。它把列式 `DataChunk` scatter 成 row blocks 和 heap blocks，通过 `TupleDataSegment / TupleDataChunk / TupleDataChunkPart` 组织逻辑扫描单位和物理切片；通过 BufferManager 的 pin/unpin 参与统一内存管理和 spill；通过 `base_heap_ptr` 与 `RecomputeHeapPointers` 在 heap block 地址变化后修正变长字段指针。它的核心价值是：在保持 DuckDB vectorized 执行接口的同时，让排序、聚合、连接等算子的行式中间数据能够高效、可 spill、可合并、可按块管理。
