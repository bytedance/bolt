# 07. 分块 Frame 支持路线

## 背景

当前普通 Window 路径的核心问题不是 RowContainer 存不下，而是 WindowFunction 可能会把一个很大的 frame 一次性 materialize 成列式 `Vector`。

以 aggregate window 为例，当前数据流大致是：

```text
RowContainer 行式数据
  -> WindowPartition::extractColumn(firstRow, numFrameRows)
  -> argVectors_ 列式数据
  -> Aggregate::addSingleGroupRawInput
  -> aggregateResultVector_
  -> output RowVector
```

其中最危险的是：

```cpp
fillArgVectors(firstRow, lastRow);
```

如果 `[firstRow, lastRow]` 覆盖的 frame 很大，`argVectors_` 会爆内存。BM 只能缓解原始输入行常驻内存问题，不能解决 `argVectors_` 一次性变大的问题。

因此，解决“单个 frame 放不下内存”必须让 WindowFunction 从“整段 frame materialize”变成“分块读取 frame、分块计算”。

## 统计依据

基于 `b.csv` 的统计：

```text
总量：308,581,737

ranking_or_metadata                  71.2117%
streamable_or_small_state_aggregate  19.8737%
point_or_small_lookup                 7.3942%
large_result_or_large_state           0.8037%
global_structure_or_sketch            0.7127%
unknown_or_needs_review               0.0040%
```

可以认为：

```text
适合分块或不需要大 frame materialize：98.4796%
需要额外状态或人工处理：1.5204%
```

## 总体策略

不要一开始承诺所有 WindowFunction 都支持超大 frame。按收益和复杂度分阶段：

```text
Phase 1: ranking + point lookup
Phase 2: streamable aggregate chunked frame
Phase 3: prefix/sliding frame 增量优化
Phase 4: collect/percentile/sketch 类函数单独治理
```

Phase 1 + Phase 2 做完后，按当前统计可覆盖约 `98.48%`。

---

## Phase 1：ranking / point lookup 不物化大 frame

### 覆盖率

```text
ranking_or_metadata   71.2117%
point_or_small_lookup  7.3942%
合计                  78.6059%
```

### 覆盖函数

ranking/metadata：

```text
row_number
rank
dense_rank
percent_rank
cume_dist
ntile
```

point/small lookup：

```text
lag
lead
first_value
last_value
nth_value
```

### 核心判断

这些函数不应该依赖完整 frame materialization：

- `row_number` 只依赖当前 partition offset。
- `rank/dense_rank/percent_rank/cume_dist` 主要依赖 peer group。
- `ntile` 依赖 partition row count 和当前 row index。
- `lag/lead` 只需要目标 row。
- `first_value/last_value/nth_value` 只需要 frame 内特定 row。

### 改动点

#### WindowPartition 增加点查/小范围读取能力

在 RowId/BM 场景下，提供：

```cpp
char* rowAt(vector_size_t partitionRow);

void extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize = false) const;
```

对于 RowId-backed partition，内部按小范围 pin：

```text
target RowId(s)
  -> pinRows()
  -> extract
  -> method return 后 unpin
```

#### WindowFunction 不保存裸 row 指针

这些函数可以继续写 `result` vector，但不能把 pin 后的 `char*` 保存到函数成员里。

### 验收标准

- ranking/value 类函数在 BM RowId partition 上结果与默认路径一致。
- frame 很大时，ranking 类不会触发整 frame `extractColumn()`。
- lag/lead/value 类只 pin 目标 row 或小范围 rows。

---

## Phase 2：streamable aggregate 分块 frame

### 覆盖率

```text
streamable_or_small_state_aggregate 19.8737%
累计覆盖                            98.4796%
```

### 覆盖函数

第一批：

```text
sum
count
avg
min
max
```

第二批可扩展：

```text
std/stddev/stddev_samp/stddev_pop
variance/var_samp/var_pop
corr
covar_samp/covar_pop
first/last
count_if
bit_or/bit_and
min_by/max_by
regr_*
```

### 核心改造

把 `AggregateWindowFunction` 从：

```text
fillArgVectors(firstRow, lastRow)
aggregate addInput over full argVectors
extract result
```

改成：

```text
reset aggregate state
for chunk in frame:
  extract chunk arg columns
  aggregate addInput over chunk
extract result
```

### 建议新增配置

```text
window-frame-chunk-rows
window-frame-chunk-bytes
```

第一版可以只用 rows 配置：

```text
window-frame-chunk-rows = 4096 或 8192
```

### 建议接口

可以不新增复杂接口，先复用现有 `WindowPartition::extractColumn()`，每次只传 chunk 范围：

```cpp
void fillArgVectorsChunk(vector_size_t chunkStart, vector_size_t chunkEnd) {
  vector_size_t numRows = chunkEnd - chunkStart + 1;
  for (int i = 0; i < argIndices_.size(); ++i) {
    if (argIndices_[i] == kConstantChannel) {
      continue;
    }
    BaseVector::prepareForReuse(argVectors_[i], numRows);
    partition_->extractColumn(
        argIndices_[i], chunkStart, numRows, 0, argVectors_[i]);
  }
}
```

### simpleAggregation 改造

当前 simple path 每个 output row 都可能重新计算 frame。改成 chunk 后：

```text
for each selected output row:
  resetAggregateGroup()
  for chunk in [frameStart, frameEnd]:
    fillArgVectorsChunk(chunkStart, chunkEnd)
    aggregate_->addSingleGroupRawInput(...)
  aggregate_->extractValues(...)
  copy to result[resultOffset + row]
```

特点：

- 最容易实现。
- 能解决 OOM。
- 性能可能一般，因为重叠 frame 会重复扫。

### incrementalAggregation 改造

对于可增量场景，当前逻辑会复用上一行/上一 batch 的 aggregate state。chunk 化后，仍然保留这个优化：

```text
if 可以复用 previous aggregate:
  只处理新增 [previousEnd + 1, currentEnd]
else:
  reset aggregate
  处理 [frameStart, currentEnd]
```

新增范围按 chunk 处理：

```text
for chunk in new range:
  fillArgVectorsChunk(chunk)
  addInput(chunk)
```

### 验收标准

- 构造一个单 frame 行数大于 `window-frame-chunk-rows` 的查询。
- `sum/count/avg/min/max` 不再一次性分配完整 frame 的 `argVectors_`。
- 输出与旧实现一致。
- chunk size 变小时结果不变。

---

## Phase 3：prefix/sliding frame 性能优化

### 覆盖率

覆盖率不新增，仍然是 Phase 2 的函数集合。

这一阶段目标是性能，不是功能。

### 为什么需要

Phase 2 的朴素 chunked simpleAggregation 能避免 OOM，但对于重叠 frame 可能重复计算很多次。

例如：

```sql
sum(x) over (
  partition by k
  order by ts
  rows between 100000 preceding and current row
)
```

朴素实现可能每行都扫一遍 100000 行。能跑但慢。

### 优化方向

#### Prefix frame

适用：

```text
UNBOUNDED PRECEDING -> CURRENT ROW
UNBOUNDED PRECEDING -> UNBOUNDED FOLLOWING
```

做法：

```text
state += 新进入的 rows
output state
```

对于 `UNBOUNDED FOLLOWING`，可以先扫完整 partition 得到一个结果，再复制给所有行。

#### Sliding frame

适用：

```text
ROWS BETWEEN K PRECEDING AND CURRENT ROW
ROWS BETWEEN K PRECEDING AND K FOLLOWING
```

做法：

```text
state += 新进入的 rows
state -= 过期的 rows
output state
```

这要求 aggregate 支持 retract/remove。优先支持：

```text
sum
count
avg
```

`min/max` 可以用 deque 或 multiset，但复杂度更高。

### 验收标准

- 大量重叠 frame 的查询不 OOM。
- 相比 Phase 2 朴素实现，重复 extract 次数明显下降。
- 对 `sum/count/avg` 的 sliding frame 结果正确。

---

## Phase 4：风险函数单独治理

### 剩余比例

```text
large_result_or_large_state 0.8037%
global_structure_or_sketch  0.7127%
unknown_or_needs_review     0.0040%
合计                        1.5204%
```

### 典型函数

```text
collect_set             0.4063%
collect_list            0.3846%
percentile              0.3521%
percentile_approx       0.2401%
approx_count_distinct   0.1052%
union_sketches          0.0151%
```

### 为什么不能靠简单分块解决

#### collect_set / collect_list

即使输入分块读取，结果或中间集合本身也可能非常大。

```text
frame 有 1 亿行
collect_set 结果可能有几千万个元素
```

分块只能降低输入读取内存，不能降低最终结果大小。

#### percentile / percentile_approx

需要分布、排序或 sketch state。最终结果可能很小，但中间结构可能很大。

#### approx_count_distinct

需要 sketch state。是否可控取决于 sketch 实现。

### 第一阶段策略

不为这些函数承诺超大 frame 支持。

更合理的是：

- 增加内存阈值。
- 在即将超过阈值时 fail-fast。
- 给出清晰错误。

示例错误：

```text
Window function collect_set requires a frame state larger than the configured
memory limit. Please reduce frame size, pre-aggregate input, or rewrite SQL.
```

### 后续可选方案

如果业务强依赖，再逐个实现：

- `collect_set`：spillable hash set。
- `collect_list`：disk-backed list builder。
- `percentile`：external sorter 或可 merge sketch。
- `approx_count_distinct`：确认 sketch 是否固定上界。

## 建议实施顺序

```text
1. RowId/BM + WindowPartition 小范围 pin
2. ranking/value 类确认不 materialize 大 frame
3. AggregateWindowFunction chunked simpleAggregation
4. AggregateWindowFunction chunked incrementalAggregation
5. prefix/sliding 优化
6. 风险函数 fail-fast
7. 风险函数按业务需要单独 externalize
```

## 最小可交付版本

最小可交付版本建议只包含：

- RowId/BM 读取链路。
- `WindowPartition::extractColumn()` 支持 chunk 级 pin。
- aggregate window 的 chunked `simpleAggregation`。
- aggregate window 的 chunked `incrementalAggregation`。
- 对 `collect_*`、`percentile*`、`approx_count_distinct` 等函数加 fail-fast 或保持旧路径限制。

该版本理论覆盖：

```text
ranking/value/streamable aggregate = 98.4796%
```

并能直接解决主要场景下“单个 frame 放不下内存”的问题。

