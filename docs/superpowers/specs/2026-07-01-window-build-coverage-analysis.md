# WindowBuild 覆盖场景估算

## 背景

本文基于 `docs/superpowers/specs/2026-06-29-window-usage-summary-analysis.md` 中的线上 window 用法统计，估算当前 `SortWindowBuild`、`StreamingWindowBuild`、`RowsStreamingWindowBuild`、`SpillableWindowBuild` 分别能覆盖多少场景。

本分析采用以下前提：

- 统计口径使用原报告的 `window_expr_cnt`，即 window/over 表达式数量，不是 distinct query 数。
- 原报告明确说明不同 pattern 的 `query_cnt` 不能直接相加，因此本文不估算去重 query 数。
- 假设我们一定会在 Window 算子之前加 sort 算子，因此 Window 看到的输入是 sorted input，`needSort_ = false`。
- 本文只讨论现有非 BM WindowBuild 的覆盖情况，不把 `BmStreamingWindowBuild` 纳入自动选择顺序。

原报告总量：

```text
total_window_expr_cnt = 512761
```

原报告“优化桶”中可明确归类的场景为：

```text
可判断场景 = total - named_window_unexpanded - unknown/noise
          = 512761 - 4644 - 1030
          = 507087
          = 98.893%
```

这里使用优化桶中的 `named window 未展开 = 4644` 和 `unknown/noise = 1030`，因为该表各桶数量可以闭合到总量。

注意：这个数字表示可归类、可用于估算 WindowBuild 选择的场景，没有扣除所有 WindowBuild 共同继承的执行限制。例如当前 Window 层不支持 `GROUPS` frame，也不支持 `RANGE k PRECEDING/FOLLOWING` 中的常量 k-bound。这些限制不属于某一个 WindowBuild 独有能力差异。

## 自动选择顺序

在 `Window::setWindowBuild()` 的默认选择逻辑中，简化后的顺序是：

1. 如果 `followedTopNum > 0 && needSort_`，使用 `SortWindowBuild`。
2. 如果启用 BM 且 `!needSort_` 且输入类型支持，使用 `BmStreamingWindowBuild`。
3. 如果 `supportRowsStreaming()`，使用 `RowsStreamingWindowBuild`。
4. 如果 `isSpillableWindowBuild()`，使用 `SpillableWindowBuild`。
5. 否则使用 `StreamingWindowBuild`。

在本文假设下，Window 前已经加 sort，所以 `needSort_ = false`。因此：

- `SortWindowBuild` 在实际自动选择中不会命中。
- `RowsStreamingWindowBuild`、`SpillableWindowBuild` 会优先吃掉各自支持的场景。
- 其余可执行场景由 `StreamingWindowBuild` 兜底。

## 覆盖估算汇总

| WindowBuild | 实际自动选择命中估算 | 占总 window 表达式 | 说明 |
|---|---:|---:|---|
| `SortWindowBuild` | `0` | `0%` | 前置 sort 后 `needSort_ = false`，实际不会选中该类。 |
| `RowsStreamingWindowBuild` | `243292` | `47.447%` | 主要是 `row_number + rank`。 |
| `SpillableWindowBuild` | 至少 `92961`，可量化上界 `111381` | `18.129% ~ 21.722%` | 强命中 whole-partition aggregate；`lag/lead` 只能算上界。 |
| `StreamingWindowBuild` | 约 `152414 ~ 170834` | `29.721% ~ 33.315%` | 前两者扣除后的兜底。 |

如果只讨论“语义能力”而不是实际自动选择，`SortWindowBuild` 和 `StreamingWindowBuild` 都是通用 WindowBuild，可以覆盖原报告中可判断的绝大多数场景：

```text
507087 / 512761 = 98.893%
```

但在“Window 前一定加 sort”的实际执行前提下，`SortWindowBuild` 不会作为 WindowBuild 被选中。

## RowsStreamingWindowBuild

`RowsStreamingWindowBuild` 的选择条件来自 `supportRowsStreaming()`：

- 所有 window function 的 metadata 都不能是 `ProcessingUnit::kPartition`。
- 如果函数不 ignore frame，则必须是默认 frame：`unbounded preceding` 到 `current row`。

当前注册元数据里，明确满足 rows streaming 的主要函数是：

- `row_number`
- `rank`

原报告函数分布：

```text
row_number = 234945
rank       =   8347
合计       = 243292
占比       = 243292 / 512761 = 47.447%
```

注意：`dense_rank`、`percent_rank`、`cume_dist`、`ntile` 虽然也是 ranking 类函数，但当前注册 metadata 默认是 `ProcessingUnit::kPartition`，不会命中 `RowsStreamingWindowBuild`。

## SpillableWindowBuild

`SpillableWindowBuild` 的选择条件来自 `isSpillableWindowBuild()`。

支持的 aggregate 函数：

- `sum`
- `count`
- `min`
- `max`
- `avg`

但要求 frame 是：

```text
unbounded preceding 到 unbounded following
```

支持的 non-aggregate 函数：

- `lag`
- `lead`

但要求：

- offset 为空或常量 1。
- 不带 `IGNORE NULLS`。
- 不能和 aggregate window function 混在同一个 Window operator 中。

原报告能直接量化的强命中场景是 whole-partition aggregate：

```text
聚合函数 whole-partition，无 order = 92961
占比 = 92961 / 512761 = 18.129%
```

`lag/lead` 在报告中合计：

```text
lag/lead 合计 = 18420
```

但报告没有拆出 offset 是否为 1、是否 `IGNORE NULLS`、是否和 aggregate 混用，因此这里只能作为可量化上界：

```text
Spillable 上界 = 92961 + 18420
              = 111381
              = 21.722%
```

实际命中应低于这个上界。

## StreamingWindowBuild

`StreamingWindowBuild` 是 sorted input 下的通用兜底路径。在本文假设下，它会接住：

- `RowsStreamingWindowBuild` 不支持的 ranking 函数，例如 `dense_rank`、`percent_rank`、`cume_dist`、`ntile`。
- `SpillableWindowBuild` 不支持的 aggregate frame，例如 running、bounded、implicit cumulative frame。
- value 函数，例如 `first_value`、`last_value`、`nth_value`。
- `lag/lead` 中 offset 不满足 Spillable 条件或带 `IGNORE NULLS` 的场景。
- collection / by_extreme / 其他 aggregate window。

按可判断场景扣除 RowsStreaming 和 Spillable 后估算：

```text
Streaming 高值 = 507087 - 243292 - 92961
              = 170834
              = 33.315%

Streaming 低值 = 507087 - 243292 - 111381
              = 152414
              = 29.721%
```

因此，`StreamingWindowBuild` 的实际兜底命中约为：

```text
152414 ~ 170834
29.721% ~ 33.315%
```

## SortWindowBuild

`SortWindowBuild` 本身是通用路径，语义上可以覆盖大部分 Window 场景；如果 Window 输入不是 sorted input，它负责在 WindowBuild 内部排序并构建 partition。

但本文前提是 Window 前一定加 sort。此时 Window 看到的是 sorted input，`needSort_ = false`，默认选择不会走 `SortWindowBuild`。

因此：

```text
实际自动选择命中 = 0
语义覆盖能力     = 507087 / 512761 = 98.893%
```

## 限制

本文估算受原报告粒度限制：

- 原报告是正则分类，不是完整 SQL AST 解析。
- `named window` 未展开，不能准确归入具体 WindowBuild。
- `unknown/noise` 不参与覆盖估算。
- `lag/lead` 缺少 offset、`IGNORE NULLS`、同 operator 混用信息，因此 Spillable 只能给上界。
- aggregate full-partition 显式 frame 的完整函数级数量没有单独列出，因此 Spillable 的强命中只使用 whole-partition 无 order 桶。
- 所有 WindowBuild 都共同受当前 Window 层限制影响，例如 `GROUPS` frame 和 `RANGE` 常量 k-bound；本文表格没有把这些共同限制从各 Build 的选择覆盖估算中逐项扣除。
- 本文没有计算 distinct query 覆盖率；需要回到原始明细按 `dorado_id` 去重后才能精确计算。

## 结论

在“Window 前一定加 sort”的前提下：

1. `SortWindowBuild` 实际自动选择命中为 `0`。
2. `RowsStreamingWindowBuild` 主要覆盖 `row_number + rank`，约 `47.447%`。
3. `SpillableWindowBuild` 至少覆盖 whole-partition aggregate，约 `18.129%`；考虑 `lag/lead` 上界后最多约 `21.722%`。
4. `StreamingWindowBuild` 作为 sorted input 通用兜底，约覆盖 `29.721% ~ 33.315%`。
5. 如果只看语义覆盖能力，`SortWindowBuild` 和 `StreamingWindowBuild` 都能覆盖约 `98.893%` 的可判断场景，但实际自动选择下前者不会被使用。
