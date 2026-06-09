# BmRowContainer 算子集成设计

日期：2026-06-07

本文描述 Sort、Hash Agg、Hash Build / Hash Join 如何使用新版 `BmRowContainer`。底层类型、
生命周期、Segment / DataChunk / ChunkPart、pointer rebasing、flush、read session 和 release
语义以 `2026-06-07-bm-row-container-segment-design.md` 为准。

本文只定义算子和 RowContainer 的调用边界，不重新定义 RowContainer 内部状态。

## 1. 统一调用约束

算子只能依赖以下语义：

- `newRow()` / `newRow(partition)` 返回 `char*`。
- `char*` 只在 resident 阶段、全量 read session 生命周期内，或 read session 当前窗口内有效。
- `RowId` 只在 `BulkReadSession::tryLoadAll()` 失败后暴露，是 window fallback 的 durable handle。
- 算子在 flush 对应 segment 后必须主动清空 cached pointers。
- `compare(const char*, const char*)` 和 `compareRows(...)` 只接受有效内存指针，不触发 IO。
- `flushActiveSegment()` / `flushActivePartitionSegment(partition)` 是全量、大批量下刷边界。
- `finalizeSortedRun(sortedRows, options)` 接收已经排好序的 resident `char*` 序列，并返回
  `SortedRunId`。
- `beginBulkReadSegments(...)` 创建 session 时先尝试全量加载目标 segments，失败后进入 window read。
- `BulkReadSession::tryLoadAll(rows, rowIds)` 成功只填 `rows`，失败只填 `rowIds`。
- `BulkReadSession::loadRows(...)` 是批量 window 慢路径；`loadRow(...)` 是显式单行慢路径。
- `beginMergeReadSegments(...)` 用于 sorted run 多路归并，只暴露 cursor 当前窗口中的 row pointer。
- `releaseSegment(s)` 只能在上层确认数据不会再被访问时调用。

算子不直接读取 `SortedRunMeta`。Sort 和 HashAgg 的 sorted run 统一使用物理有序的
`kMaterializedOrder`。

## 2. Sort 集成

Sort 仍以 resident pointer 排序为快路径。

### 无 flush 路径

```text
addInput
  -> row = container.newRow()
  -> container.store(..., row, ...)
  -> rows.push_back(row)

noMoreInput
  -> sort rows by char*
  -> container.extractColumnResident(rows, ...)
  -> output
```

这条路径和现有 `RowContainer` 接近：排序比较只使用 `char*`，不触发 IO。

### flush / external sort 路径

当上层感知内存压力并决定 flush 当前 working set：

```text
1. 使用 resident ptr 对当前 active rows 排序。
2. 调用 finalizeSortedRun(sortedRows, options)，得到 SortedRunId。
3. 清空这些 cached ptr，只保留 SortedRunId。
4. RowContainer 创建新的 active segment，Sort 继续接收输入。
```

`finalizeSortedRun()` 按排序顺序写出物理有序 run segment。输出阶段统一使用 `MergeReadSession`：

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
  -> 找到 group char* 或创建新 group
  -> key 和 accumulator state 在 group 指向的 row 内更新
```

当上层感知内存压力并决定 flush 当前 resident groups：

```text
1. 收集当前 resident groups。
2. 使用 group ptr 按 group key 排序。
3. 调用 finalizeSortedRun(sortedGroups, options)，得到 SortedRunId。
4. 清空 resident hash table 中对应 cached ptr。
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
  -> container.store(..., row, ...)
  -> 如果该 partition resident，插入 resident hash table
```

resident hash table bucket 保存 `char*`。flush 对应 partition 后，这些 pointer 必须清空；写入阶段不保存
RowId。

Flush 某个 partition：

```text
flushActivePartitionSegment(partition)
  -> finalize 并下刷该 partition 当前 active segment
  -> 清空该 partition resident hash table 中对应 ptr
  -> partition.flushedSegments.push_back(segmentId)
  -> 为该 partition 创建新的 active segment
```

对 flushed partition，不建议默认保留一个只含 `RowId` 的 hash table 并在 probe 命中时逐行读 row。
主模型仍然以 partition 为批量处理边界：

- build 侧 partition 被 flush 后，该 partition 不再参与 resident probe。
- probe 输入如果落到 flushed partition，应进入同 partition 的 probe buffer/spill。
- 当要处理某个 flushed partition 时，先打开该 partition 的 build segments。
- 调用 `beginBulkReadSegments(partitionBuildSegments)`，再调用 `tryLoadAll(rows, rowIds)`。
- 如果返回 `kLoadedPointers`，直接用 `rows` 重建该 partition 的 resident hash table，然后顺序处理同
  partition 的 probe rows。
- 如果返回 `kNeedWindowRead`，上层保存 `rowIds`。推荐用 `loadRows(rowIdBatch)` 批量访问；必要时可以
  用 `loadRow(rowId)` 走显式单行慢路径。
- 如果这种显式慢路径仍然不满足性能或内存要求，需要继续 repartition，或复用现有 external join 的
  多级 partition spill 流程。

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

这里的 `RowId` 不是写入阶段的常规 handle，而是 `tryLoadAll()` 失败后才构造的 fallback handle。
`loadRow()` 允许单行访问，但它是显式慢路径，可能触发该 row 所在 chunk 的 pin/load/rebase。

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

- Hash Join partition restore 放不下时，具体采用几级 repartition，以及现有 spiller 如何复用。
- Hash Join 在 window fallback 下，哪些 probe 形态允许使用 `loadRow()` 显式单行慢路径，哪些必须继续
  repartition。

这些策略不应改变基础边界：resident 快路径只用指针；Sort/HashAgg 使用 materialized sorted run；
HashBuild 以 partition replay 为主，`loadRow()` 只是显式慢路径；compare 不做 IO。

## 7. 算子接入顺序建议

算子接入应独立于 RowContainer 本体实现推进。建议顺序：

1. 接入一个非 partitioned 使用方，验证 resident 写入、resident compare 和 resident extract。
2. 接入 Sort 的 flush / materialized sorted run / merge cursor 路径。
3. 接入 Hash Agg 的 group flush 和 sorted run merge 路径。
4. 接入 Hash Build 的 partitioned active segments 和 partition flush。
5. 接入 Hash Join 的 probe partition buffering、partition restore 和批量 rebuild resident hash table。
6. 接入 right/full join 未匹配 build rows 的批量输出路径。

这些步骤不属于 RowContainer 本体实现计划；它们用于在 RowContainer API 稳定后逐步迁移算子。
