# BmRowContainer 算子集成设计

日期：2026-06-07

本文描述 Sort、Hash Agg、Hash Build / Hash Join 如何使用新版 `BmRowContainer`。底层类型、
生命周期、Segment / DataChunk / ChunkPart、pointer rebasing、flush、read session 和 release
语义以 `2026-06-07-bm-row-container-segment-design.md` 为准。

本文只定义算子和 RowContainer 的调用边界，不重新定义 RowContainer 内部状态。

## 1. 统一调用约束

算子只能依赖以下语义：

- `newRow()` / `newRow(partition)` 返回 `RowHandle { RowId id; char* ptr; }`。
- `ptr` 只在 resident 阶段或 read session 当前窗口内有效。
- `RowId` 是长期身份，可以跨 flush、unpin、reload 保存。
- 算子在 flush 对应 segment 后必须主动清空 cached pointers。
- `compare(const char*, const char*)` 和 `compareRows(...)` 只接受有效内存指针，不触发 IO。
- `flushActiveSegment()` / `flushActivePartitionSegment(partition)` 是全量、大批量下刷边界。
- `finalizeSortedRun(sortedRows, options)` 接收已经排好序的 resident `RowHandle` 序列，并返回
  `SortedRunId`。
- `beginBulkReadSegments(...)` 创建 session 时先尝试全量加载目标 segments，失败后进入 window read。
- `BulkReadSession::resolveRows(...)` 只在 session 内把一批 `RowId` 解析成当前有效的 `char*`；
  它不是全局单行随机访问接口。
- `beginMergeReadSegments(...)` 用于 sorted run 多路归并，只暴露 cursor 当前窗口中的 row pointer。
- `releaseSegment(s)` 只能在上层确认数据不会再被访问时调用。

算子不直接读取 `SortedRunMeta`，也不关心 sorted run 是 `kRowIdOrder` 还是 `kMaterializedOrder`。
两种 layout 的选择策略暂不在本文确定。

## 2. Sort 集成

Sort 仍以 resident pointer 排序为快路径。

### 无 flush 路径

```text
addInput
  -> row = container.newRow()
  -> container.store(..., row.ptr, ...)
  -> rows.push_back(row)

noMoreInput
  -> sort rows by row.ptr
  -> container.extractColumnResident(rows.ptrs, ...)
  -> output
```

这条路径和现有 `RowContainer` 接近：排序比较只使用 `char*`，不触发 IO。

### flush / external sort 路径

当上层感知内存压力并决定 flush 当前 working set：

```text
1. 使用 resident ptr 对当前 active rows 排序。
2. 调用 finalizeSortedRun(sortedRows, options)，得到 SortedRunId。
3. 清空这些 RowHandle 中的 ptr，只保留 RowId / SortedRunId。
4. RowContainer 创建新的 active segment，Sort 继续接收输入。
```

`finalizeSortedRun()` 可以生成两种 run：

- `kRowIdOrder`：只保存排序后的 `RowId` 顺序，flush source segment。
- `kMaterializedOrder`：按排序顺序写出物理有序 run segment。

Sort 不在这里依赖具体 layout。输出阶段统一使用 `MergeReadSession`：

```text
beginMergeReadSegments(all sorted runs)
  -> 每个 run 一个 SegmentCursor
  -> priority queue 比较 cursor.currentRow()
  -> pop winner
  -> gather winner 到输出 batch
  -> winner.advance()
```

Sort 不需要把所有 runs 全量加载回内存。它只要求每个 run 的 cursor 当前窗口有效。

## 3. Hash Agg 集成

Hash Agg resident 阶段仍然使用内存 hash table 和 resident group row。

```text
输入 row
  -> probe resident hash table
  -> 找到 group RowHandle 或创建新 group
  -> key 和 accumulator state 在 group.ptr 指向的 row 内更新
```

当上层感知内存压力并决定 flush 当前 resident groups：

```text
1. 收集当前 resident groups。
2. 使用 group.ptr 按 group key 排序。
3. 调用 finalizeSortedRun(sortedGroups, options)，得到 SortedRunId。
4. 清空 resident hash table 中对应 RowHandle.ptr。
5. 记录 SortedRunId。
6. 创建新的 active segment 和 resident hash table。
```

最终 merge：

```text
beginMergeReadSegments(all aggregate sorted runs)
  -> 每个 run 一个 cursor
  -> heap 按 group key 比较 cursor.currentRow()
  -> 相同 key 的 rows 连续取出
  -> 合并 accumulator spill state
  -> 输出最终或中间聚合结果
```

如果没有 flushed runs，Hash Agg 可以直接扫描 resident hash table 输出。

聚合函数如果使用外部内存或非 fixed accumulator，flushed row 中必须包含后续 merge 所需的 spill
representation。这个语义应与现有 `Accumulator::extractForSpill()` 保持一致。

## 4. Hash Build / Hash Join 集成

Hash Build 与 Sort/HashAgg 的主要区别是 partition 可以多次 flush。推荐模型：

```text
partition 0 -> active segment + finalized/flushed segments[]
partition 1 -> active segment + finalized/flushed segments[]
partition 2 -> active segment + finalized/flushed segments[]
partition 3 -> active segment + finalized/flushed segments[]
```

Build 输入：

```text
计算 build row partition
  -> row = container.newRow(partition)
  -> container.store(..., row.ptr, ...)
  -> 如果该 partition resident，插入 resident hash table
```

resident hash table bucket 不应该只保存裸指针，应同时保存长期身份和短期指针：

```cpp
struct BuildRowRef {
  RowId id;
  char* ptr; // resident 时可用；flush 后必须清空
};
```

Flush 某个 partition：

```text
flushActivePartitionSegment(partition)
  -> finalize 并下刷该 partition 当前 active segment
  -> 清空该 partition resident hash table 中对应 ptr
  -> partition.flushedSegments.push_back(segmentId)
  -> 为该 partition 创建新的 active segment
```

对 flushed partition，不保留一个只含 `RowId` 的 hash table 并在 probe 命中时随机读 row。正确模型是
以 partition 为批量处理边界：

- build 侧 partition 被 flush 后，该 partition 不再参与 resident probe。
- probe 输入如果落到 flushed partition，应进入同 partition 的 probe buffer/spill。
- 当要处理某个 flushed partition 时，先打开该 partition 的 build segments。
- 调用 `beginBulkReadSegments(partitionBuildSegments)`，优先尝试全量读回该 partition。
- 如果读回成功，可以用 `resolveRows()` 批量把 build `RowId` 转成当前 session 内有效的 `char*`，
  重建该 partition 的 resident hash table，然后顺序处理同 partition 的 probe rows。
- 如果仍然放不下，需要继续 repartition，或复用现有 external join 的多级 partition spill 流程。

超内存路径：

```text
build partition flush
  -> build partition segments finalized/flushed
  -> probe rows for that partition are buffered/spilled by partition

restore partition
  -> load build partition segments as a batch
  -> rebuild resident hash table for this partition
  -> stream/probe matching probe partition
  -> output
  -> release build/probe partition data when consumed
```

这里没有“hash table bucket 命中 RowId 后单独 pin 那一行”的路径。`RowId` 只在 partition restore 和
bulk materialize 内部使用。

Right/full join 需要输出未匹配 build rows 时：

- resident partition 直接扫描 resident hash table rows。
- flushed partition 通过 `BulkReadSession` 批量 materialize partition segments。

## 5. Release 使用方式

算子只有在确认数据不会再被访问时，才能调用 `releaseSegment(s)` 或设置
`ReadSessionOptions::releaseWhenConsumed`。

适合释放的场景：

- Sort 最终输出某个 sorted run 后，该 run 不再参与后续 merge。
- Hash Agg merge 完某个 run，且 source/materialized segments 不再被其他 session 引用。
- Hash Join restore 完某个 partition，并且该 partition 的 build/probe 数据已经输出完。
- 算子 clear、取消或提前结束。

不适合释放的场景：

- sorted run 还可能参与下一轮 merge。
- Hash Join 的 right/full join 还需要扫描未匹配 build rows。
- 仍有 active `BulkReadSession` 或 `MergeReadSession` 持有对应 segment。

## 6. 待定策略

以下策略暂不在文档中提前定死：

- Sort 的 sorted run 选择 `kRowIdOrder` 还是 `kMaterializedOrder`。
- Hash Agg 的 sorted run 选择 `kRowIdOrder` 还是 `kMaterializedOrder`。
- `SortedRunOptions` 是由算子显式传入，还是由 RowContainer 根据 run 大小、内存压力和 benchmark
  结果选择。
- Hash Join partition restore 放不下时，具体采用几级 repartition，以及现有 spiller 如何复用。

这些策略不应改变基础边界：resident 快路径只用指针；flush/read/merge 是显式批量边界；probe 和
compare 不做单 row IO。

## 7. 算子接入顺序建议

算子接入应独立于 RowContainer 本体实现推进。建议顺序：

1. 接入一个非 partitioned 使用方，验证 resident 写入、resident compare 和 resident extract。
2. 接入 Sort 的 flush / sorted run / merge cursor 路径，选择具体 `SortedRunLayout` 策略。
3. 接入 Hash Agg 的 group flush 和 sorted run merge 路径。
4. 接入 Hash Build 的 partitioned active segments 和 partition flush。
5. 接入 Hash Join 的 probe partition buffering、partition restore 和批量 rebuild resident hash table。
6. 接入 right/full join 未匹配 build rows 的批量输出路径。

这些步骤不属于 RowContainer 本体实现计划；它们用于在 RowContainer API 稳定后逐步迁移算子。
