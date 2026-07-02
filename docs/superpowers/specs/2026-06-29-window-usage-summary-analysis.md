# Window 使用详情统计分析报告

## 背景

本报告基于 `/data00/home/wangxinshuo.db/bolt/window_usage_summary.sql` 的执行结果整理，结果文件为：

- `/data00/home/wangxinshuo.db/bolt/window_usage_summary.xlsx`

统计对象来自 `olap.tqs_query_task_etl_daily`，时间范围为：

- `date >= '20260601'`
- `date <= '20260630'`

当前 SQL 的分析方式是基于 Query 文本中的 `over` 片段进行正则分类。它不是完整语法解析器，因此报告中的统计更适合用于判断优化方向和优先级；少量 `Uknow`、注释、字符串字面量误命中属于已知噪声。

## 统计口径

结果表共有 `2626` 个聚合 pattern，覆盖 `512761` 个 `over` 片段/窗口表达式。

字段口径：

- `window_expr_cnt`：当前 pattern 下命中的 window/over 片段数量，是本报告排序和占比计算的主指标。
- `query_cnt`：当前 pattern 下的 `count(distinct dorado_id)`，同一个 SQL 可以命中多个 pattern，因此不同 pattern 的 `query_cnt` 不能直接相加。
- `window_func`：最终识别出的 window 函数；只有白名单函数才会进入该字段，否则为 `Uknow`。
- `generic_window_func_raw`：通用正则抓到的原始候选函数，仅用于诊断，不作为最终函数分类。
- `over_pattern_type`：`paren_window`、`named_window` 或 `Uknow`。
- `frame_unit/frame_shape/frame_start/frame_end`：基于 `over(...)` 中 frame 子句识别出的 frame 类型和边界。

## 核心结论

本轮 SQL 修改后，函数误识别问题已经基本解决。最终 `window_func` 中没有再出现 `from`、`as`、`cast`、`join`、`if`、`nvl`、`coalesce` 等明显非 window 函数。

整体识别质量如下：

| 类别 | window_expr_cnt | 占比 |
|---|---:|---:|
| 完整识别 | 505897 | 98.661% |
| 函数识别成功，但 named window 未展开 | 4474 | 0.873% |
| 最终 `window_func = Uknow` | 1200 | 0.234% |
| 函数识别成功，但 frame/spec 局部未知 | 1190 | 0.232% |

从优化工作看，后续重点非常集中：

1. `row_number/rank` 排序类占 `50.407%`，其中 `row_number over(partition by ... order by ...)` 单独占 `43.420%`。
2. 聚合类 window 占 `40.288%`，主要是 `sum/count/max/min/avg`。
3. `lag/lead` 和 value 类函数合计约 `5.206%`。
4. 显式 `rows/range` frame 只占 `3.984%`，数量不大，但实现和性能语义更复杂。
5. 剩余 `Uknow` 只有 `0.234%`，主要来自注释、字符串字面量、DDL comment 中的 `over` 噪声，不值得继续投入太多正则规则成本。

## 函数分布

Top window 函数如下：

| window_func | window_expr_cnt | 占比 |
|---|---:|---:|
| row_number | 234945 | 45.820% |
| sum | 83159 | 16.218% |
| count | 46240 | 9.018% |
| max | 46010 | 8.973% |
| min | 15060 | 2.937% |
| lag | 11143 | 2.173% |
| percent_rank | 9868 | 1.925% |
| avg | 9468 | 1.847% |
| rank | 8347 | 1.628% |
| collect_list | 8234 | 1.606% |
| collect_set | 7749 | 1.511% |
| lead | 7277 | 1.419% |
| first_value | 4190 | 0.817% |
| last_value | 4073 | 0.794% |
| percentile | 3224 | 0.629% |

按函数类别聚合：

| 类别 | window_expr_cnt | 占比 | 说明 |
|---|---:|---:|---|
| ranking | 258466 | 50.407% | `row_number/rank/dense_rank/percent_rank/cume_dist/ntile` |
| aggregate | 206579 | 40.288% | `sum/count/max/min/avg/percentile/stddev` 等 |
| offset | 18420 | 3.592% | `lag/lead` |
| collection | 16519 | 3.222% | `collect_list/collect_set/collect_map` 等 |
| value | 8274 | 1.614% | `first_value/last_value/nth_value` |
| by_extreme | 3302 | 0.644% | `max_by/min_by` |
| other_or_unknown | 1201 | 0.234% | `Uknow` 和极少量其他函数 |

结论：优化优先级应围绕 ranking 和 aggregate 两类展开，这两类合计约 `90.695%`。

## Partition / Order 分布

| partition/order pattern | window_expr_cnt | 占比 |
|---|---:|---:|
| has_partition_by + has_order_by | 374062 | 72.951% |
| has_partition_by + no_order_by | 103869 | 20.257% |
| no_partition_by + has_order_by | 18066 | 3.523% |
| no_partition_by + no_order_by | 10954 | 2.136% |
| Uknow + Uknow | 5810 | 1.133% |

最主要的形态是 `partition by + order by`，说明排序型 window 是绝对主场景。

其中最大单项 pattern：

| pattern | window_expr_cnt | 占比 |
|---|---:|---:|
| `row_number over(partition by ... order by ...)` | 222639 | 43.420% |
| `sum over(partition by ... no order)` | 37703 | 7.353% |
| `sum over(partition by ... order by ... no explicit frame)` | 28939 | 5.644% |
| `max over(partition by ... order by ... no explicit frame)` | 27160 | 5.297% |
| `count over(partition by ... no order)` | 26465 | 5.161% |

## Frame 分布

Frame unit 总体分布：

| frame_unit | window_expr_cnt | 占比 |
|---|---:|---:|
| no_frame | 484447 | 94.478% |
| rows | 12719 | 2.480% |
| range | 9727 | 1.897% |
| Uknow | 5810 | 1.133% |
| groups | 58 | 0.011% |

这里的 `no_frame` 表示 Query 文本中没有显式写 `rows/range/groups` frame 子句，不代表执行时没有 frame 语义。尤其是聚合函数带 `order by` 时，Spark 通常会使用默认累计窗口语义。

显式 frame 中的主要形态：

| frame pattern | window_expr_cnt | 占比 |
|---|---:|---:|
| `range between n preceding and n preceding` | 3953 | 0.771% |
| `range between n preceding and current row` | 3508 | 0.684% |
| `rows between unbounded preceding and current row` | 3336 | 0.651% |
| `rows between unbounded preceding and unbounded following` | 2999 | 0.585% |
| `rows between n preceding and current row` | 2311 | 0.451% |
| `range between current row and current row` | 1103 | 0.215% |
| `rows between n preceding and n preceding` | 816 | 0.159% |

结论：显式 frame 的总量不大，但包括滑动窗口、累计窗口、全窗口等多种语义。后续优化时可以先覆盖最常见的 `rows/range between ... and current row` 和 `unbounded preceding` 场景。

## Window 函数 + Frame 频率分析

把 `window_func` 和 frame 组合后看，整体仍然高度集中在 `no_frame`。这说明绝大多数 Query 没有显式写 `rows/range/groups`，优化时需要同时关注函数类别和 Spark 默认 frame 语义。

Top 组合如下：

| window_func | frame pattern | window_expr_cnt | 占比 |
|---|---|---:|---:|
| `row_number` | `no_frame` | 234750 | 45.782% |
| `sum` | `no_frame` | 73058 | 14.248% |
| `count` | `no_frame` | 45274 | 8.829% |
| `max` | `no_frame` | 44334 | 8.646% |
| `min` | `no_frame` | 14597 | 2.847% |
| `lag` | `no_frame` | 10723 | 2.091% |
| `percent_rank` | `no_frame` | 9868 | 1.924% |
| `rank` | `no_frame` | 8341 | 1.627% |
| `collect_set` | `no_frame` | 7222 | 1.408% |
| `collect_list` | `no_frame` | 7072 | 1.379% |
| `lead` | `no_frame` | 6706 | 1.308% |
| `first_value` | `no_frame` | 3443 | 0.671% |
| `percentile` | `no_frame` | 3220 | 0.628% |
| `avg` | `no_frame` | 3197 | 0.623% |
| `min_by` | `no_frame` | 3184 | 0.621% |
| `sum` | `rows between unbounded_preceding and current_row` | 2539 | 0.495% |
| `avg` | `range between n_preceding and n_preceding` | 2468 | 0.481% |
| `dense_rank` | `no_frame` | 2466 | 0.481% |
| `percentile_approx` | `no_frame` | 2082 | 0.406% |
| `last_value` | `Uknow` | 2075 | 0.405% |
| `ntile` | `no_frame` | 1750 | 0.341% |
| `avg` | `range between n_preceding and current_row` | 1597 | 0.311% |
| `sum` | `rows between n_preceding and current_row` | 1556 | 0.303% |
| `last_value` | `rows between unbounded_preceding and unbounded_following` | 1391 | 0.271% |
| `sum` | `range between n_preceding and current_row` | 1377 | 0.269% |

只看显式 frame，Top 组合如下：

| window_func | explicit frame pattern | window_expr_cnt | 占比 |
|---|---|---:|---:|
| `sum` | `rows between unbounded_preceding and current_row` | 2539 | 0.495% |
| `avg` | `range between n_preceding and n_preceding` | 2468 | 0.481% |
| `avg` | `range between n_preceding and current_row` | 1597 | 0.311% |
| `sum` | `rows between n_preceding and current_row` | 1556 | 0.303% |
| `last_value` | `rows between unbounded_preceding and unbounded_following` | 1391 | 0.271% |
| `sum` | `range between n_preceding and current_row` | 1377 | 0.269% |
| `avg` | `range between current_row and current_row` | 1103 | 0.215% |
| `sum` | `range between n_preceding and n_preceding` | 843 | 0.164% |
| `collect_list` | `rows between unbounded_preceding and unbounded_following` | 801 | 0.156% |
| `sum` | `rows between n_preceding and n_preceding` | 454 | 0.089% |
| `sum` | `rows between expr_preceding and Uknow` | 435 | 0.085% |
| `avg` | `rows between n_preceding and current_row` | 408 | 0.080% |
| `sum` | `rows between unbounded_preceding and unbounded_following` | 338 | 0.066% |
| `max` | `range between n_preceding and n_preceding` | 311 | 0.061% |
| `sum` | `rows between unbounded_preceding and n_preceding` | 300 | 0.059% |
| `max` | `rows between unbounded_preceding and current_row` | 241 | 0.047% |
| `first_value` | `rows between unbounded_preceding and unbounded_following` | 235 | 0.046% |
| `sum` | `rows between current_row and n_following` | 225 | 0.044% |
| `max` | `range between n_preceding and current_row` | 220 | 0.043% |
| `collect_set` | `range between n_preceding and n_preceding` | 214 | 0.042% |

这个维度下的结论：

1. `row_number + no_frame` 是最大组合，单项占 `45.782%`，仍然是最优先的优化入口。
2. 聚合函数的 `no_frame` 组合占比很高，尤其是 `sum/count/max/min + no_frame`，需要区分无 `order by` 的 whole-partition 聚合和有 `order by` 的隐式累计窗口。
3. 显式 frame 里最值得优先覆盖的是 `sum/avg + rows/range between ... and current_row`，以及 `last_value/collect_list + rows between unbounded_preceding and unbounded_following`。
4. `last_value + Uknow` 主要来自 named window 未展开，不代表 `last_value` 本身 frame 识别失败。

## 优化优先级拆解

按执行语义和优化价值拆分：

| 优化桶 | window_expr_cnt | 占比 | 建议优先级 |
|---|---:|---:|---|
| `row_number` partition+order TopN/去重核心场景 | 222639 | 43.420% | P0 |
| 聚合函数 whole-partition，无 order | 92961 | 18.129% | P0 |
| 聚合函数 ordered implicit frame | 83833 | 16.349% | P1 |
| 其他 ranking ordered no explicit frame | 30817 | 6.010% | P1 |
| `lag/lead/value` ordered | 23074 | 4.500% | P2 |
| 显式 `rows/range` frame | 20428 | 3.984% | P2 |
| named window 未展开 | 4644 | 0.906% | P3 |
| unknown/noise | 1030 | 0.201% | 不建议优先优化 |
| other | 33335 | 6.501% | 按后续 case 再拆 |

建议优化路线：

1. P0：优先处理 `row_number over(partition by ... order by ...)`，覆盖 TopN、去重、取最新/最早记录等核心 workload。
2. P0：处理 `sum/count/max/min over(partition by ...)` 且无 `order by` 的 whole-partition 聚合。
3. P1：处理聚合函数带 `order by` 但无显式 frame 的隐式累计窗口。
4. P1：扩展到 `rank/dense_rank/percent_rank/cume_dist/ntile`。
5. P2：处理 `lag/lead/first_value/last_value` 和显式 `rows/range` frame。
6. P3：如果后续需要更精确的 frame 统计，再考虑展开 named window。

## Uknow 与噪声分析

最终 `window_func = Uknow` 只有 `1200` 个，占 `0.234%`。

`Uknow` 原因分布：

| uknow_reason | window_expr_cnt | 占 Uknow | 占总量 |
|---|---:|---:|---:|
| `window_func_missing,over_pattern_type,partition_order,frame_unit,frame_shape,frame_start,frame_end` | 981 | 81.750% | 0.191% |
| `window_func_missing,named_window_unexpanded` | 170 | 14.167% | 0.033% |
| `window_func_missing` | 40 | 3.333% | 0.008% |
| `window_func_rejected` | 9 | 0.750% | 0.002% |

典型噪声来源：

- 字符串字面量：`message like '%publish not receive data over %'`
- 注释文本：`-- metric over click rate`
- DDL comment：`comment ' over s1 days'`
- 英文说明：`over the last 7 days`
- 业务枚举文本：`over clip level`

当前 SQL 是按 `over` 字符拆分 Query 文本，因此如果真实 SQL 中出现字符串或注释里的 `over`，会被当作候选片段。剩余 `Uknow` 主要是这种噪声，不代表真实 window 场景大量漏识别。

## 误识别修复效果

本轮最重要的改进是把最终函数识别从通用正则改成白名单识别。

修复后：

- `window_func` 中没有出现 `from/as/cast/join/if/nvl/coalesce` 等非 window 函数。
- `generic_window_func_raw` 中仍然会出现 `as/from/join/if/coalesce/cast` 等值，但这是诊断字段，不影响最终分类。
- `window_func = Uknow` 中只有 `9` 个来自 `window_func_rejected`，其中样例包括注释中的 `concat_ws(...) over (...)` 和 `within group (...) over ()` 这类非当前白名单/非 Spark 常规 window 表达。

因此，当前结果已经可以作为优化方向判断依据，不需要继续为了极少量 Query 扩展正则规则。

## Named Window 分析

`named_window` 总量为 `4644`，占 `0.906%`。

典型形态：

```sql
sum(x) over w
window w as (partition by ... order by ...)
```

当前 SQL 能识别出 `sum/lag/lead/last_value` 等函数，但不会展开 `window w as (...)` 的定义，因此 `partition_by_pattern/order_by_pattern/frame_*` 会归为 `Uknow`。

主要 named window 名称包括：

| name | window_expr_cnt | 占比 |
|---|---:|---:|
| wa | 2113 | 0.412% |
| w | 779 | 0.152% |
| daily | 372 | 0.073% |
| wd | 309 | 0.060% |
| w_order | 100 | 0.020% |
| w_7d | 64 | 0.013% |
| w_14d | 64 | 0.013% |
| w_30d | 64 | 0.013% |

结论：named window 占比不到 `1%`。如果只是指导优化重点，可以不展开；如果后续需要高精度 frame 分类，可以单独做 named window 展开。

## 对后续 Window 优化的指导

综合本次统计，优化工作建议按以下方向推进：

### P0：TopN / 去重 / 排序选行

目标 pattern：

```sql
row_number() over(partition by ... order by ...)
```

占比 `43.420%`，是最大单项场景。常见用途包括：

- 分组取第一条或最后一条
- 去重
- 每组 TopN
- 按时间或优先级选最新记录

这一类对排序、partition 分布、内存使用、spill 控制都敏感，优化收益最大。

### P0：Whole-partition 聚合窗口

目标 pattern：

```sql
sum(x) over(partition by ...)
count(x) over(partition by ...)
max(x) over(partition by ...)
min(x) over(partition by ...)
```

无 `order by` 的聚合类 window 占 `18.129%`。这类语义接近按 partition 聚合后回填到每行，适合重点关注：

- 是否可以降低重复计算
- partition 内状态复用
- 聚合结果广播/回填策略
- 与普通 aggregate + join 的执行差异

### P1：Ordered implicit frame 聚合

目标 pattern：

```sql
sum(x) over(partition by ... order by ...)
max(x) over(partition by ... order by ...)
count(x) over(partition by ... order by ...)
```

这类占 `16.349%`。虽然没有显式写 `rows/range`，但通常有默认累计窗口语义。优化时需要特别关注：

- 累计聚合状态复用
- 排序后顺序扫描
- frame 默认语义与显式 frame 的兼容性

### P1：其他 Ranking 函数

目标函数：

- `rank`
- `dense_rank`
- `percent_rank`
- `cume_dist`
- `ntile`

这类和 `row_number` 共享排序输入，但需要处理 peer group、tie、百分比、分桶等语义。可以在 `row_number` 优化稳定后扩展。

### P2：Offset / Value 函数

目标函数：

- `lag`
- `lead`
- `first_value`
- `last_value`

这类占比约 `5.206%`，数量不如 ranking/aggregate，但对 frame 和 null 处理语义更敏感。建议在 P0/P1 后推进。

### P2：显式 rows/range frame

显式 `rows/range` 合计约 `3.984%`。优先覆盖：

- `rows between unbounded preceding and current row`
- `rows between n preceding and current row`
- `range between n preceding and current row`
- `rows between unbounded preceding and unbounded following`

这类数量不大，但对滑动窗口状态、边界查找、range peer 处理有更高要求。

## 风险与限制

当前报告基于 SQL 文本正则解析，主要限制如下：

1. 不能完全识别字符串、注释、DDL comment 中的 `over`。
2. 不能展开 named window 定义。
3. 对复杂嵌套表达式、非标准 UDAF、`within group` 等语法无法做到语法级准确。
4. `frame_clause_tail` 当前基于固定长度前缀和正则判断，极少数复杂 frame 可能被归入 `frame_start/frame_end = Uknow`。
5. `query_cnt` 是 pattern 内 distinct SQL 数，不能跨 pattern 求和。

这些限制影响的是尾部少量 Query，不改变主要优化方向判断。

## 最终判断

这次统计已经足够用于制定后续 window 优化重点。继续修正正则可以降低少量 `Uknow`，但收益有限。

建议把优化资源集中在：

1. `row_number over(partition by ... order by ...)`
2. `sum/count/max/min/avg over(partition by ...)`
3. 聚合函数带 `order by` 的隐式累计窗口
4. `rank/dense_rank/percent_rank/cume_dist/ntile`
5. `lag/lead/first_value/last_value`
6. 常见显式 `rows/range` frame

其中前两类已经覆盖约 `61.549%` 的全部 window/over 片段，是最值得优先投入的方向。
