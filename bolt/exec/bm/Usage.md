# BM RowContainer 使用说明

本文面向接入 `BmRowContainer` 的执行算子，只描述算子需要依赖的接口和生命周期。
内部的 segment/chunk/block 组织、StringView rebase 和 BufferManager pin 细节不作为算子
接入约束。

公共入口：

- `BmRowContainer.h`
- `BmRowContainerRead.h`
- `BmRowContainerPublicTypes.h`

内部存储类型在 `BmSegmentTypes.h`，只供 `bm` 内部实现和白盒 UT 使用。

## 基本模型

`BmRowContainer` 是 row-based 临时数据容器。

- 写入阶段优先在内存中追加 row。
- 上层感知内存压力后调用 spill，把当前 active segment 交给 BufferManager 管理。
- spill 返回 `SegmentId`。后续读回、释放、partition 管理都以 `SegmentId` 为单位。
- resident 阶段使用 `char*` row 指针；不能全量 resident 时使用 `RowId` 句柄，再交给
  `ReadOnlyWindowReadSession` 批量转成只读 resident 指针。

## 创建

```cpp
using namespace bytedance::bolt::exec::bm;

BmRowContainer rows(
    {BIGINT(), VARCHAR()},
    {false, true},
    bufferManager,
    memory::bm::MemoryTag::kTesting);
```

`types` 和 `nullable` 必须一一对应。nullable 信息会参与 row layout 生成，非 nullable
列会走更短的快路径。

## 写入

使用 `appendRow()` + `store()` 写入一行：

```cpp
auto context = rows.appendRow(partitionId);
rows.store(context, decodedKey, sourceIndex, keyColumn);
rows.store(context, decodedPayload, sourceIndex, payloadColumn);
char* row = context.row();
```

`RowWriteContext` 只描述当前 row 的写入位置，不要跨 container、跨 spill 或异步流程保存。

## Resident 指针访问

比较和列提取都要求输入 row 指针当前 resident。

```cpp
int32_t result = rows.compare(leftRow, rightRow, column, flags);
int32_t rowResult = rows.compareRows(leftRow, rightRow, keyFlags);

rows.extractColumnResident(
    rowPtrs.data(),
    rowPtrs.size(),
    column,
    outputVector);
```

spill 后旧指针不再有效。读回后需要重新通过 `BulkReadSession`、
`ReadOnlyWindowReadSession` 或 `MergeReadSession` 获取指针。

## Spill

默认 partition：

```cpp
SegmentId segment = rows.spillActiveSegment();
```

多 partition：

```cpp
SegmentId segment = rows.spillActivePartitionSegment(partitionId);
```

同一个 partition 可以多次 spill，适合 Hash Build 这类分区写入场景：

```cpp
const auto& segments = rows.segmentsForPartition(partitionId);
```

## 全量读

如果 working set 预计可以全部 resident，先快速判断，再全量加载：

```cpp
std::vector<SegmentId> segments = ...;
folly::Range<const SegmentId*> range(segments.data(), segments.size());

if (rows.canBulkRead(range)) {
  auto bulk = rows.beginBulkReadSegments(range);
  std::vector<char*> rowPtrs = bulk.loadRows();
  // rowPtrs 可直接用于 compare / extractColumnResident。
}
```

`canBulkRead()` 只是快速判断。`BulkReadSession::loadRows()` 会真正 reserve 和 pin；如果期间内存状态
变化，仍可能抛异常。

`BulkReadSession::loadRows()` 返回的 `char*` 由 container 持有的 resident block 支撑。
Bulk 读不提供局部 eviction 能力；如果需要按窗口释放 working set，使用
`ReadOnlyWindowReadSession`。

## Window read

如果不能全量加载，先列出 `RowId`，再按算子自己的访问窗口批量加载。`RowId` 是
container 返回给读 session 的定位句柄，调用方不要解析其中字段。

```cpp
auto session = rows.beginReadOnlyWindowReadSegments(range);
std::vector<RowId> rowIds = session.listRowIds();

std::vector<RowId> needed = ...;
std::vector<const char*> rowPtrs = session.loadRows(
    folly::Range<const RowId*>(needed.data(), needed.size()));
```

单行接口是显式慢路径：

```cpp
const char* row = session.loadRow(rowId);
```

`ReadOnlyWindowReadSession` 只返回 `const char*`。读阶段如果需要释放当前窗口的 resident
内存但未来还要继续使用这些数据，调用 `session.evictLoadedChunks(targetBytes)`。eviction
以 chunk 为粒度：一个 chunk 的 row block 和 heap blocks 会一起释放。已经有 spill backing
且 clean 的 block 会直接丢弃；没有 backing 或被标记 dirty 的 block 会先写回。如果数据已经
完全消费，调用 release 接口。

## Reordered Segment 和 Merge Read

Sort / HashAgg 在内存中完成排序后，可以按排序后的 row 指针顺序物理写出一个可 merge 的
segment：

```cpp
SegmentId orderedSegment =
    rows.finalizeReorderedSegment(
        folly::Range<char* const*>(orderedRows.data(), orderedRows.size()));
```

多个有序 segment 使用 `MergeReadSession` 顺序读回：

```cpp
auto merge = rows.beginMergeReadSegments(range);

std::vector<char*> batch;
while (merge.next(batch, maxRows)) {
  rows.extractColumnResident(batch.data(), batch.size(), column, output);
}
```

`beginMergeReadSegments()` 只接受 `finalizeReorderedSegment()` 产生的有序 segment。普通
spill segment 不能直接进入 merge read。

merge read 默认是消费型读取：读完的 chunk 会在安全时机释放，避免后续内存压力下再次 spill
已经消费的数据。如果调用方需要重复读取，显式关闭读后释放：

```cpp
auto merge = rows.beginMergeReadSegments(range, false);
```

## 释放和 Window Evict

数据未来还可能再用，但当前需要释放 resident 内存：

```cpp
session.evictLoadedChunks(targetBytes);
```

数据已经不会再用：

```cpp
rows.releaseSegment(segment);
rows.releaseSegments(range);
```

## 常见接入方式

Sort / HashAgg：

1. `appendRow()` + `store()` 写入。
2. resident 阶段保留 row 指针并用 `compareRows()` 排序。
3. 排序后调用 `finalizeReorderedSegment()`。
4. 多个有序 segment 用 `beginMergeReadSegments()` 输出。

Hash Build：

1. 按 partition 写入：`appendRow(partition)` + `store()`。
2. 每个 partition 可以多次 `spillActivePartitionSegment(partition)`。
3. probe 或后续处理某个 partition 时，读取 `segmentsForPartition(partition)`。
4. 能全量加载则 `BulkReadSession::loadRows()`；不能则
   `ReadOnlyWindowReadSession::listRowIds()` + `ReadOnlyWindowReadSession::loadRows()`。
5. partition 完成后释放对应 segments。

## 使用约束

- spill 后不要继续使用旧 row 指针。
- `RowId` 不应由算子自行解析，应交回 `ReadOnlyWindowReadSession`。
- `compare()`、`compareRows()`、`extractColumnResident()` 都要求 row 指针 resident。
- `RowWriteContext` 只用于当前 row 的逐列 store。
- 当前常规快路径覆盖 fixed-width 类型、`VARCHAR` 和 `VARBINARY`；复杂类型不要作为接入假设。

## 开发和测试约定

重构和新增功能需要保持热路径性能稳定。不要为了隐藏实现细节引入 PImpl、虚调用、额外堆分配
或新的锁到 `appendRow()`、`store()`、`appendBatch()`、resident compare/extract 等热路径。
公共头可以保留必要的 hot-path detail，但新代码应尽量依赖更窄的公共类型头。

`BmRowContainer` 的正式 API 不暴露细粒度 trace metrics。线上问题定位优先使用
`BufferManagerStats`、`BufferManagerTagStats` 和 IO scheduler stats 这类大范围统计；
benchmark 可以在容器外层测量端到端阶段耗时，但不要把逐行、逐 block 或 ns 级阶段计数重新
放回 `appendRow()`、`store()`、`appendBatch()`、bulk read 等热路径。

## Benchmark 数据 profile

`bolt/exec/bm/benchmarks` 里的 RowContainer benchmark 使用三个 dataset profile：

- `fixed`：只包含 `BIGINT`、`INTEGER`、`DOUBLE`，不包含变长列。
- `variable`：包含一个 `VARCHAR` 列，字符串长度按 row id 确定性分布在 `1..64`，
  平均约 `32B`。可通过 `--bm_row_container_variable_max_string_length=64` 调整上限。
- `variable_large`：包含一个 `VARCHAR` 列，字符串固定为 `1024B`，用于保留大字符串
  copy/spill/compress/IO 压力场景。可通过 `--bm_row_container_large_string_length=1024`
  调整长度。

两个字符串长度 flag 可以在同一次运行中同时传入，但只分别作用于对应 profile：`variable`
只读取 `variable_max_string_length`，`variable_large` 只读取 `large_string_length`。
runner 脚本默认枚举并运行 binary 注册的全部 case，因此会同时跑 `fixed`、`variable` 和
`variable_large`。

UT 按行为域拆分：

- `BmRowContainerResidentTest.cpp`：resident 写入、比较、提取、nullable 和基础 layout 行为。
- `BmRowContainerReadTest.cpp`：bulk/window read、window eviction 和 StringView rebase。
- `BmRowContainerBatchTest.cpp`：`appendBatch()` fixed/string/null/chunk 跨越行为。
- `BmMergeReadSessionTest.cpp`：reordered segment 和 merge read 行为。
- `BmSegmentCollectionTest.cpp`：segment/chunk/block 内部存储行为。
- `BmPartitionTest.cpp`：partition spill 和 partition 边界。

新增 UT 优先放到对应行为域文件；如果新增一个独立行为域，再新增单独测试文件并更新
`tests/CMakeLists.txt`。
