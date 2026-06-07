# RowContainer 与算子 Spill 机制说明

日期：2026-06-06

本文整理现有 `RowContainer` 如何被 Sort、Hash Agg、Hash Join 使用，以及这些算子在内存
压力下如何通过 `Spiller` 将数据写出到磁盘并在后续读回处理。

## 总体模型

`RowContainer` 是内存态 row store。它提供 row-format 存储能力，但不主动感知内存压力，
也不负责自动换页或自动从磁盘读回。

现有机制是 operator-level spill：

```text
算子处理输入
  -> 数据写入 RowContainer / HashTable
  -> 算子感知内存压力
  -> 算子调用 Spiller
  -> Spiller 从 RowContainer 取出 rows
  -> 数据写出到磁盘 spill file
  -> 算子清理内存态 RowContainer / HashTable
  -> 后续通过 spill reader 从磁盘读回继续计算
```

`RowContainer` 对外暴露的是 `char* row`。这些指针必须在当前进程内存中可直接解引用。
因此，现有 `RowContainer` 本身没有“row 不在内存但按需读回”的访问语义。

## Spiller 的写出形式

`Spiller` 使用 `RowContainer` 时主要有两类写出维度。

按是否排序：

- sorted spill：先按 key 排序，再写出到磁盘。Sort 和 Hash Agg 使用这种方式。
- unordered/direct spill：按 hash partition 直接写出，不要求 run 内有序。Hash Join 使用这种方式。

按落盘格式：

- RowVector spill：从 `RowContainer` 中 `extractColumn`，构造 `RowVector` 后写出。
- row-based spill：直接把 `RowContainer` row-format 数据序列化或压缩后写出。

因此可以理解为：

```text
RowContainer rows
  -> 可选排序
  -> RowVector 格式或 row-based 格式
  -> 磁盘 spill file
```

## Sort / OrderBy

### 输入阶段

Sort 算子通过 `SortBuffer` 接收输入。输入 `RowVector` 中的每一行都会写入 `RowContainer`：

```text
输入 RowVector
  -> RowContainer::newRow()
  -> RowContainer::storeColumn / store
  -> SortBuffer 保存 row 指针
```

如果没有发生 spill，`noMoreInput()` 时会从 `RowContainer` 中列出所有 rows，对 row 指针排序。
输出时再按排序后的 row 指针调用 `extractColumn` 生成输出 `RowVector`。

### Spill 写出

当输入阶段或输出阶段内存不足时，SortBuffer 会触发 spill。

输入阶段 spill：

```text
内存不足
  -> 创建 kOrderByInput Spiller
  -> Spiller 从 RowContainer listRows
  -> 按 sort key 排序 rows
  -> 写成 sorted run 到磁盘
  -> clear RowContainer
```

输出阶段 spill：

```text
已经产生 sortedRows_
  -> 发现剩余输出内存压力过大
  -> 创建 kOrderByOutput Spiller
  -> 把剩余 sortedRows_ 写成 sorted run 到磁盘
  -> clear RowContainer
```

### Spill 读回

输出阶段会打开磁盘上的多个 sorted run：

```text
多个磁盘 sorted run
  -> ordered spill reader
  -> 多路 merge
  -> 输出全局有序 RowVector
```

普通 RowVector spill 模式下，reader 读出 `RowVector` batch，SortBuffer 通过 `gatherCopy` 输出。
row-based spill 模式下，reader 读出 row-format 临时 batch，再通过 `rowToColumnVector`
转换成输出 `RowVector`。

Sort 的 spill 语义是：

```text
把内存中的 RowContainer 数据排序后写成磁盘 sorted run，
读回时对多个 sorted run 做多路归并，产出全局有序输出。
```

## Hash Agg / GroupingSet

### 输入阶段

Hash Agg 通过 hash table 管理 group。底层 group key 和 accumulator state 存在
`HashTable::rows()` 对应的 `RowContainer` 中。

```text
输入 RowVector
  -> hash table probe group key
  -> 找到已有 group row，或创建新 group row
  -> group key 写入 RowContainer
  -> accumulator state 写入或更新 RowContainer row
```

### Spill 写出

当聚合 hash table 或 `RowContainer` 内存压力过大时，GroupingSet 触发 spill：

```text
内存压力过大
  -> 创建 kAggregateInput Spiller
  -> 从 HashTable 的 RowContainer 取出所有 group rows
  -> 按 group key 排序
  -> 写成 sorted agg run 到磁盘
  -> clear HashTable / RowContainer
```

聚合输出阶段也可能触发 spill：

```text
输出阶段内存不足
  -> 创建 kAggregateOutput Spiller
  -> 从指定 RowContainerIterator 开始写出剩余 rows
  -> 写到磁盘
  -> clear HashTable / RowContainer
```

### Spill 读回

输入全部结束后，如果发生过 spill，GroupingSet 会打开磁盘上的多个 sorted agg run：

```text
多个 sorted agg run
  -> ordered spill reader / row-based ordered reader
  -> 按 group key 多路 merge
  -> 相同 group key 聚到一起
  -> 合并 accumulator state
  -> 输出最终或中间聚合结果
```

Hash Agg 使用排序不是为了最终输出有序，而是为了让相同 group key 在读回时相邻，从而可以
合并多个 spill run 中的 intermediate accumulator state。

Hash Agg 的 spill 语义是：

```text
把当前内存中的聚合状态按 group key 排序写到磁盘，
读回时通过 sorted merge 合并相同 group 的 accumulator state。
```

## Hash Join

### Build 阶段

Hash Join 的 build side 使用 `HashTable`，底层 build rows 存在 `RowContainer` 中。

```text
build side 输入 RowVector
  -> 计算 build key hash
  -> probe/insert build hash table
  -> build row 写入 HashTable 的 RowContainer
  -> hash table 保存指向 RowContainer row 的引用
```

### 第一次 Spill 写出

HashBuild 在内存不足时触发 group spill。当前实现不是选择一个 victim partition 局部写出，
而是进入全量 partitioned external join 模式。

触发已有 table spill 时：

```text
HashBuild 感知内存不足
  -> 调用无参 Spiller::spill()
  -> Spiller 标记所有 hash partitions 为 spilled
  -> 扫描当前 RowContainer 中所有 build rows
  -> 按 hash partition 分发到 spill runs
  -> 每个非空 partition 写出到磁盘
  -> clear 当前 HashTable / RowContainer
```

这里要注意：空 partition 可能不会生成实际文件，但运行期状态上所有 partition 都已经被标记为
spilled。

### 后续 Build 输入

第一次 spill 之后，当前 HashBuild 的所有 partitions 都是 spilled 状态。因此后续 build 输入
不会继续进入当前内存 hash table，而是直接按 partition 写到磁盘：

```text
后续 build RowVector
  -> 计算每行 hash partition
  -> partition 已 spilled
  -> 包成对应 partition 的 RowVector
  -> 直接写到磁盘 spill file
```

### Probe 阶段

Probe side 需要和 build side 使用相同的 hash partition 规则。

```text
probe side 输入 RowVector
  -> 计算 probe key partition
  -> 如果对应 build partition 已 spilled
       -> probe row 写到对应 probe spill partition
  -> 否则
       -> probe 当前内存 build hash table
```

### Spill 读回 / Restore

Hash Join 的读回不是 sorted merge，而是按 hash partition 分轮 restore：

```text
当前内存 build hash table 先被 probe 完
  -> HashJoinBridge 选择下一个 spilled build partition
  -> build side 从磁盘读回该 build partition
  -> 重建一个较小的 build hash table / RowContainer
  -> probe side 从磁盘读回对应 probe partition
  -> probe 并输出 join 结果
  -> 循环处理下一个 spilled partition
```

Hash Join 的 spill 语义是：

```text
按 hash partition 把 build/probe 两侧数据写到磁盘，
之后一次读回一个 partition，重建 build hash table 并 probe 对应 probe partition。
```

## 三类算子的对比

| 算子 | RowContainer 中保存什么 | Spill 写出方式 | 磁盘读回后的处理 |
| --- | --- | --- | --- |
| Sort / OrderBy | 排序 key 和 payload row | 按 sort key 排序，写 sorted run | 多路 merge sorted run，输出有序 RowVector |
| Hash Agg | group key 和 accumulator state | 按 group key 排序，写 sorted agg run | 多路 merge 相同 group key，合并 accumulator |
| Hash Join | build side rows | 按 hash partition 直接写出 | 一次 restore 一个 partition，重建 hash table 后 probe |

## 和 BmRowContainer 的关系

现有 `RowContainer + Spiller` 是算子级 spill：

```text
RowContainer 只负责内存态 row store。
算子负责感知内存压力。
Spiller 负责把算子选定的数据写到磁盘。
读回后由算子继续完成 sorted merge 或 hash partition restore。
```

`BmRowContainer` 如果基于 `BufferManager` 扩展，应优先解决的是容器级能力：

```text
稳定 RowId
block resident / spilled 状态管理
batch pin / batch read
block-level spill/read
```

但 sort run、aggregation state merge、hash join partition restore 仍然属于算子语义，不应完全
隐藏进通用 RowContainer 里。
