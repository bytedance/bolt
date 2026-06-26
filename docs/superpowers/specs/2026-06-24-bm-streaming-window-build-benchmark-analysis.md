# BmStreamingWindowBuild 与 StreamingWindowBuild 测试结论

## 测试覆盖

当前 `BmStreamingWindowBuildTest` 覆盖以下场景：

- 基础 window 函数：`row_number`。
- 排名和值函数：`rank`、`dense_rank`、`percent_rank`、`cume_dist`、`ntile`、`lead`、`lag`、`first_value`、`last_value`。
- `IGNORE NULLS` value functions。
- aggregate window：running sum、bounded sum。
- 通过 query config 选择 `BmStreamingWindowBuild`。
- unsupported type 自动 fallback 到原始实现。
- store 阶段不主动 spill，只有显式 spill 或 operator reclaim 触发 spill。
- spill 后 reload 并继续输出正确结果。
- window 参数列按 chunk materialize。
- 大字符串场景下 peak memory 低于原始 `StreamingWindowBuild`。

当前结果：

- `BmStreamingWindowBuildTest.*`：`12/12` 通过。
- `bolt_exec_bm_test`：`55/55` 通过。

## Benchmark 场景

主 benchmark 是 `bolt_streaming_window_build_benchmark`，对比：

- `StreamingWindowBuild`
- `BmStreamingWindowBuild`

默认输入：

- `vectors = 8`
- `rows_per_vector = 1024`
- `total_rows = 8192`
- `partition_rows = 4096`
- `string_bytes = 1024`
- 默认不触发 spill，主要测试纯内存路径。

覆盖函数：

- `row_number`
- `rank`
- `lead`
- `running_sum`
- `bounded_sum`
- `count(varchar) over full partition`

另跑了一个大字符串单 partition 场景：

- `total_rows = 8192`
- `partition_rows = 8192`
- `string_bytes = 4096`

## 结果摘要

默认场景下：

| case | BmStreamingWindowBuild 速度 | peak memory 降低 |
| --- | ---: | ---: |
| `row_number` | `1.00x` | `10.42x` |
| `rank` | `0.97x` | `10.42x` |
| `lead` | `0.97x` | `10.31x` |
| `running_sum` | `0.71x` | `10.43x` |
| `bounded_sum` | `0.69x` | `10.43x` |
| `count(varchar full partition)` | `0.92x` | `2.97x` |

大字符串单 partition 场景下：

| case | BmStreamingWindowBuild 速度 | peak memory 降低 |
| --- | ---: | ---: |
| `row_number` | `1.07x` | `8.69x` |
| `rank` | `1.05x` | `8.69x` |
| `lead` | `1.05x` | `8.66x` |
| `running_sum` | `0.88x` | `8.69x` |
| `bounded_sum` | `0.88x` | `8.69x` |
| `count(varchar full partition)` | `1.39x` | `3.36x` |

## 结论

`BmStreamingWindowBuild` 已经能显著降低 peak memory。非 aggregate window 在纯内存路径下基本可以追平原始 `StreamingWindowBuild`，大字符串场景下甚至略快。

当前主要性能差距集中在 aggregate window：

- `running_sum` 和 `bounded_sum` 默认场景明显慢于原始实现。
- `BmAggregateWindow` 当前按最多 4096 行 materialize arg vectors，再调用现有 `Aggregate::addSingleGroupRawInput`。
- running frame 可以增量追加输入，但仍有 chunk materialize 开销。
- bounded frame / 非增量 frame 会重置 aggregate 后重复扫描 frame。

辅助 compare benchmark 说明：

- old RowContainer row-row compare 很快。
- BmRowContainer 每次抽 1 行再和 Vector compare 很慢。
- BmRowContainer 批量 extract 后再 Vector compare 明显更好。

因此，BM 路径需要避免单行 extract + compare / compute，优先使用批量化路径。

## 需要解决的问题

1. 当前 benchmark 主要覆盖 no-spill 纯内存路径，缺少独立的 spill/reclaim 性能 benchmark。
2. aggregate window 的 chunk materialize 路径有额外 compute 开销。
3. bounded frame 在 BM aggregate 路径中仍然会重复扫描 frame。
4. 纯内存场景还没有针对 aggregate 做 fast path。

## 建议方案

第一步，补充 benchmark 场景：

- 纯内存小 partition。
- 纯内存大 partition。
- 很多极小 partition。
- operator reclaim 触发 spill 后继续计算。
- 大 varchar aggregate。

第二步，增加 aggregate 纯内存 fast path：

- partition 未 spill 且内存足够时，一次性 materialize arg vectors。
- 复用原始 `StreamingWindowBuild` 更接近的计算路径。
- 目标是让纯内存 aggregate 场景接近原始实现性能。

第三步，保留并优化 chunk slow path：

- partition 已 spill 或内存不足时，继续按 chunk materialize。
- 复用 arg vector buffer。
- 多列一起批量 extract。
- running / unbounded frame 继续做增量 aggregate。
- bounded frame 暂时接受重复扫描，但确保重复扫描走批量路径，不走单行路径。

整体方向：

- 纯内存路径优先追求和原始 `StreamingWindowBuild` 性能接近。
- spill / 大 partition 路径优先控制 peak memory，并保证正确性。
