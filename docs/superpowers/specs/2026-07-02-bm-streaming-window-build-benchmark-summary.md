# BM StreamingWindowBuild Benchmark 总结

本文记录 `bolt/exec/bm/benchmarks/StreamingWindowBuildBenchmark.cpp` 在 2026-07-02 的最新 benchmark 和 perf 结果。

本轮结果基于当前工作区实现，包括 `43cc59aee3 perf: reduce BM streaming window overheads` 之后尚未提交的增量优化：

- `BmWindowPartition` 共享 schema，避免大量小 partition 反复复制 logical/physical type 元数据。
- `BmStreamingWindowBuild` 批量释放已消费行，降低 many tiny partition 下的 `popFrontRows` 固定开销。
- `BmAggregateWindow` 将 aggregate argument materialize batch 从 4096 调整为 1024，降低 ordered/running aggregate 的 SelectivityVector/DecodedVector 处理范围。

## Benchmark 范围

该 benchmark 比较两条路径：

- `StreamingWindowBuild`
- `BmStreamingWindowBuild`

输入由 `InputProfile` 控制：

- `partitionRows`：partition 大小。
- `peerGroupRows`：peer group 大小。
- `PayloadProfile`：fixed、nullable、large varchar。
- `SortProfile`：默认升序单排序键、`ASC NULLS FIRST` 多排序键、`DESC NULLS LAST` 多排序键。

默认 26 个 case 覆盖：

- ranking：`row_number`、`rank`、`dense_rank`、`percent_rank`、`cume_dist`、`ntile`。
- peer group：unique peer、128-row peer、whole-partition peer。
- partition shape：many tiny partition、unaligned partition、single large partition。
- aggregate：running、reverse-running、bounded ROWS、whole-partition no-order、ordered implicit frame。
- dynamic RANGE：`range between off preceding and off following`。
- value / offset：`first_value`、`last_value`、`nth_value`、`lead`、`lag`。
- `IGNORE NULLS` value / offset functions。
- collection / by-extreme / other aggregate：`collect_list`、`collect_set`、`max_by`、`min_by`、`percentile`。
- common explicit frames：`ROWS n PRECEDING ...`、`RANGE off PRECEDING ...`。
- sort variants：默认升序单排序键、`DESC NULLS LAST` 多排序键、`ASC NULLS FIRST` 多排序键。
- multi-function Window operator。
- large varchar full-partition `count(v)`。

## 运行配置

构建命令：

```bash
PATH=/data00/home/wangxinshuo.db/tools/cmake/bin:/data00/home/wangxinshuo.db/tools/miniconda3/bin:$PATH \
  cmake --build --preset conan-release --target bolt_streaming_window_build_benchmark --parallel 16
```

全量 benchmark 命令：

```bash
timeout 600s _build/Release/bolt/exec/bm/benchmarks/bolt_streaming_window_build_benchmark \
  --bm_min_iters=3 --bm_max_trials=8 --bm_max_secs=2
```

完整输出：

```text
/tmp/bolt_streaming_window_build_benchmark_20260702_after_opt.out
/tmp/bolt_streaming_window_build_benchmark_20260702_after_opt.err
```

运行结果：

```text
stdout lines = 137
stderr lines = 0
exit code    = 0
```

输入参数：

```text
vectors=8
rows_per_vector=1024
total_rows=8192
default_partition_rows=4096
unaligned_partition_rows=1025
string_bytes=1024
cases=26
include_bm_slow=false
```

本 benchmark 是默认内存对比。BM 路径启用了 spill 相关配置，但本轮没有触发内存压力，所有 case 的 `spilledBytes=0`、`spilledRows=0`。BM spill/reclaim slow path 仍需要独立 benchmark。

## 时间口径

本轮保留两组时间数据：

- folly `time/iter`：folly benchmark 框架自己的统计。
- benchmark 内部 `avg_wall_us`：在 `query.runWithoutResults(task)` 周围计时，并通过 `folly::BenchmarkSuspender` 排除数据准备和 result 记录等外层开销。

下方主结果表使用内部 `avg_wall_us`。folly `time/iter` 用于交叉验证。两者整体趋势一致，但在短耗时/边界 case 上仍有噪声差异。

精确到原始 `avg_wall_us` 时，本轮是 25/26 个 case 正向，`by_extreme_other_agg_no_order` 四舍五入显示 `1.00x`，但原始值是 Streaming `14647us`、BM `14660us`，BM 慢 `13us`，约 `0.09%`。因此如果要求“严格每个 case 都正向”，当前仍有这一个边界点需要继续处理。

folly `time/iter` 中需要注意的边界项：

| case | Streaming time/iter | BM time/iter | 备注 |
| --- | ---: | ---: | --- |
| `collection_agg_full_frame` | `6.94ms` | `7.33ms` | folly 口径 BM 略慢，内部 wall 为 `1.11x` |
| `by_extreme_other_agg_no_order` | `14.22ms` | `14.53ms` | folly 口径 BM 略慢，内部 wall 为 `1.00x` |
| `ignore_nulls_values` | `2.14ms` | `2.08ms` | 两组口径均小幅正向 |

## 结果摘要

`BM 耗时对比` 是 StreamingWindowBuild `avg_wall_us` / BmStreamingWindowBuild `avg_wall_us`。大于 1 表示 BM 更快，小于 1 表示 BM 更慢。

`Streaming peak / BM peak` 是 StreamingWindowBuild peak memory / BmStreamingWindowBuild peak memory。大于 1 表示 BM peak memory 更低。

| case | BM 耗时对比 | Streaming peak / BM peak | Streaming avg wall us | BM avg wall us | BM compute ms | BM peak bytes | BM extract ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `row_number_default` | `1.64x` | `15.20x` | `2006` | `1220` | `0.284` | `179968` | `0.001` |
| `rank_unique_peer` | `1.09x` | `15.20x` | `1619` | `1486` | `0.349` | `179968` | `0.001` |
| `rank_128_peer` | `1.17x` | `15.20x` | `1630` | `1397` | `0.287` | `179968` | `0.001` |
| `rank_full_peer` | `1.37x` | `15.20x` | `1649` | `1204` | `0.282` | `179968` | `0.001` |
| `many_tiny_row_number` | `1.57x` | `2.09x` | `4416` | `2815` | `1.540` | `180160` | `0.003` |
| `unaligned_running_sum` | `1.17x` | `13.61x` | `2847` | `2434` | `1.114` | `193280` | `0.001` |
| `single_part_running_sum` | `1.25x` | `13.36x` | `2397` | `1914` | `0.976` | `205696` | `0.002` |
| `reverse_running_sum` | `16.24x` | `13.55x` | `32049` | `1974` | `1.028` | `205696` | `0.001` |
| `bounded_rows_64_sum` | `1.17x` | `13.36x` | `4064` | `3468` | `2.525` | `205696` | `0.001` |
| `bounded_range_col_sum` | `11.50x` | `13.69x` | `2075419` | `180469` | `179.156` | `205952` | `0.003` |
| `extended_ranking_peer` | `1.14x` | `11.07x` | `1761` | `1548` | `0.348` | `253696` | `0.001` |
| `whole_partition_agg_no_order` | `1.53x` | `6.11x` | `2555` | `1665` | `0.623` | `529664` | `0.001` |
| `global_whole_agg_no_order` | `1.29x` | `7.77x` | `1974` | `1531` | `0.518` | `429056` | `0.002` |
| `implicit_ordered_agg` | `1.15x` | `5.78x` | `5870` | `5089` | `3.834` | `529664` | `0.002` |
| `plain_value_functions` | `1.17x` | `11.53x` | `1982` | `1693` | `0.444` | `242816` | `0.001` |
| `collection_agg_full_frame` | `1.11x` | `1.49x` | `8227` | `7384` | `5.673` | `403584` | `0.003` |
| `by_extreme_other_agg_no_order` | `1.00x` | `1.00x` | `14647` | `14660` | `9.160` | `25488000` | `0.003` |
| `explicit_common_frames` | `15.87x` | `8.46x` | `6118677` | `385567` | `383.887` | `345344` | `0.004` |
| `desc_null_multi_key_ranking` | `1.17x` | `9.44x` | `2088` | `1780` | `0.458` | `302848` | `0.002` |
| `desc_null_multi_key_agg` | `1.13x` | `6.30x` | `5060` | `4476` | `3.170` | `479360` | `0.002` |
| `asc_null_first_multi_key_value` | `1.16x` | `11.53x` | `1988` | `1709` | `0.428` | `242816` | `0.001` |
| `lead_lag_nullable` | `1.08x` | `12.07x` | `1828` | `1691` | `0.484` | `230912` | `0.001` |
| `ignore_nulls_values` | `1.03x` | `8.94x` | `2154` | `2090` | `0.825` | `321792` | `0.020` |
| `multi_function_mix` | `1.14x` | `6.72x` | `2814` | `2479` | `1.251` | `453120` | `0.002` |
| `multi_agg_types` | `1.18x` | `8.23x` | `3540` | `3010` | `1.798` | `356608` | `0.001` |
| `count_large_varchar_full` | `1.18x` | `4.86x` | `7349` | `6240` | `2.008` | `3425024` | `0.004` |

## 主要观察

内部 `avg_wall_us` 口径下，BM 在 25/26 个 case 中正向，剩余 `by_extreme_other_agg_no_order` 四舍五入为 `1.00x` 但原始值微负。多数 case 有明显优势。

优势最大的 case：

- `reverse_running_sum`：`16.24x`
- `explicit_common_frames`：`15.87x`
- `bounded_range_col_sum`：`11.50x`
- `row_number_default`：`1.64x`
- `many_tiny_row_number`：`1.57x`
- `whole_partition_agg_no_order`：`1.53x`

边界 case：

- `by_extreme_other_agg_no_order`：内部 wall 四舍五入为 `1.00x`，原始值 BM 慢 `13us`，folly `time/iter` 也小幅负向，属于当前唯一未严格正向的边界 case。
- `ignore_nulls_values`：`1.03x`，收益很小。
- `lead_lag_nullable`：`1.08x`，收益小但稳定正向。
- `rank_unique_peer`：`1.09x`，ranking 中收益最小。

内存方面，本轮 BM peak memory 全部不高于 StreamingWindowBuild。`by_extreme_other_agg_no_order` 从旧结果中的明显内存劣势变为基本持平：Streaming `25586304` bytes，BM `25488000` bytes。

aggregate 相关 case 相比旧结果已经整体转正：

- `implicit_ordered_agg`：`1.15x`
- `desc_null_multi_key_agg`：`1.13x`
- `multi_agg_types`：`1.18x`
- `whole_partition_agg_no_order`：`1.53x`
- `global_whole_agg_no_order`：`1.29x`

## Perf 热点分析

### 分析方法和限制

在全量 benchmark 之外，对 8 个代表性 BM case 运行：

```bash
perf stat -e task-clock,context-switches,cpu-migrations,page-faults \
  _build/Release/bolt/exec/bm/benchmarks/bolt_streaming_window_build_benchmark \
  --bm_regex='<case>_bm_streaming$' --bm_min_iters=5 --bm_max_trials=16 --bm_max_secs=2

perf record -F 99 -g \
  _build/Release/bolt/exec/bm/benchmarks/bolt_streaming_window_build_benchmark \
  --bm_regex='<case>_bm_streaming$' --bm_min_iters=5 --bm_max_trials=16 --bm_max_secs=2

perf report --stdio --no-children --sort=symbol
```

产物目录：

```text
/tmp/bolt-window-build-perf-20260702-after-opt
```

采样数：

| case | 选择原因 | samples |
| --- | --- | ---: |
| `implicit_ordered_agg` | 旧版 ordered aggregate 慢点，验证 batch 粒度优化后热点 | `4` |
| `desc_null_multi_key_agg` | 多排序键 ordered aggregate | `3` |
| `explicit_common_frames` | dynamic RANGE / explicit frame 绝对耗时最高 | `377` |
| `bounded_range_col_sum` | 单 dynamic RANGE aggregate | `260` |
| `collection_agg_full_frame` | 当前边界 aggregate case | `3` |
| `by_extreme_other_agg_no_order` | 当前 wall 持平、folly 小幅负向 case | `82` |
| `many_tiny_row_number` | 小 partition 固定开销代表 | `4` |
| `ignore_nulls_values` | value / offset nullable 逻辑代表 | `5` |

当前机器仍未使用硬件事件，所以没有 IPC/cache miss 结论。`perf stat` 软件事件如下：

| case | task-clock | CPUs utilized | page faults | user time | sys time |
| --- | ---: | ---: | ---: | ---: | ---: |
| `bounded_range_col_sum` | `2756.68 ms` | `0.998` | `5554` | `2.736974 s` | `0.020992 s` |
| `explicit_common_frames` | `3857.73 ms` | `0.997` | `5197` | `3.842680 s` | `0.016007 s` |
| `by_extreme_other_agg_no_order` | `1263.70 ms` | `1.034` | `17346` | `1.131077 s` | `0.156783 s` |
| `collection_agg_full_frame` | `659.24 ms` | `1.003` | `14597` | `0.599437 s` | `0.063576 s` |
| `implicit_ordered_agg` | `492.82 ms` | `0.992` | `15844` | `0.413001 s` | `0.083414 s` |
| `desc_null_multi_key_agg` | `448.01 ms` | `0.999` | `14902` | `0.392618 s` | `0.059040 s` |
| `many_tiny_row_number` | `307.77 ms` | `0.983` | `13406` | `0.244290 s` | `0.066999 s` |
| `ignore_nulls_values` | `257.64 ms` | `0.980` | `16481` | `0.195560 s` | `0.065528 s` |

### RANGE path：仍然是主要绝对热点

`bounded_range_col_sum` 和 `explicit_common_frames` 有足够样本，perf 结论稳定。两者热点都集中在 `BmRangeFrameBounds` 的 row-to-row compare / frame boundary search 上。

`bounded_range_col_sum` top symbols：

| symbol | overhead |
| --- | ---: |
| `BmRowContainer::compareNonNull` | `28.85%` |
| `BmRowContainer::compare` | `23.46%` |
| `BmRangeFrameBounds::searchFrameValue` | `12.69%` |
| `SumAggregateBase<long, long, long>::addSingleGroupRawInput` | `12.69%` |
| `BmRowContainer::extractColumnResident` | `11.54%` |

`explicit_common_frames` top symbols：

| symbol | overhead |
| --- | ---: |
| `BmRowContainer::compareNonNull` | `37.14%` |
| `BmRowContainer::compare` | `31.03%` |
| `BmRangeFrameBounds::searchFrameValue` | `14.59%` |
| `BmRangeFrameBounds::compute(...)::{lambda(int)#1}::_FUN` | `5.84%` |
| `BmRowContainer::extractColumnResident` | `5.31%` |
| `AverageAggregateBase<double, double, double>::addSingleGroupRawInput` | `2.65%` |

结论：

- RANGE 相关 case 虽然相对 StreamingWindowBuild 已经有 `11.50x` / `15.87x`，但仍然是 BM 路径最大的绝对耗时来源。
- 当前热点已经不再是旧版文档中的 `FlatVector::compare`，而是 BM resident row 的 `compare/compareNonNull` 和 `searchFrameValue` 本身。
- 下一步如果继续追求更高上限，应该优化 `BmRangeFrameBounds` 的边界搜索和 compare 调用次数，而不是先动 aggregate path。

### Aggregate path：旧慢点已转正，但边界 case 仍需关注

`implicit_ordered_agg` 和 `desc_null_multi_key_agg` 因为单次 perf record 运行很短，samples 只有 `4` 和 `3`，top symbol 已被框架/setup 噪声稀释，不能据此判断具体函数热点。

更可靠的结论来自 benchmark 阶段耗时：

- `implicit_ordered_agg`：BM compute `3.834ms`，Streaming compute `4.544ms`，wall `1.15x`。
- `desc_null_multi_key_agg`：BM compute `3.170ms`，Streaming compute `3.825ms`，wall `1.13x`。
- `multi_agg_types`：BM compute `1.798ms`，Streaming compute `2.305ms`，wall `1.18x`。

`BmAggregateWindow` 的 1024-row argument batch 调整有效降低了 ordered aggregate 的 compute cost。继续深入 perf 需要更长运行时间或更窄 micro benchmark，否则 `perf record` 样本不足。

### by-extreme / collection：allocator 压力仍存在

`by_extreme_other_agg_no_order` 本轮内部 wall 接近持平：Streaming `14647us`，BM `14660us`。folly `time/iter` 仍显示 BM 略慢：Streaming `14.22ms`，BM `14.53ms`。该 case 已没有 peak memory 劣势，但仍不是强优势。

`by_extreme_other_agg_no_order` perf top symbols：

| symbol | overhead |
| --- | ---: |
| `_int_malloc` | `8.54%` |
| `_int_free` | `6.10%` |
| `BmRowContainer::extractColumnResident` | `4.88%` |
| `BaseVector::ensureNullsCapacity` | `3.66%` |
| `FlatVector<StringView>::getBufferWithSpace` | `3.66%` |
| `HashStringAllocator::free` | `3.66%` |
| `MinMaxByAggregateBase<StringView, long, true, ...>::addSingleGroupRawInput` | `2.44%` |
| `MinMaxByAggregateBase<StringView, long, true, ...>::initializeNewGroups` | `2.44%` |

`collection_agg_full_frame` samples 只有 `3`，不能解读 top symbol。benchmark 指标显示它当前 wall `1.11x`，但 folly `time/iter` 仍略负。它和 by-extreme 都属于 allocator/string-heavy case，短运行下波动较大。

### fast case：perf record 样本不足

`many_tiny_row_number`、`ignore_nulls_values`、`implicit_ordered_agg`、`desc_null_multi_key_agg` 的 perf record 样本都只有 `3-5` 个。原因是这些 case 优化后单次运行很短，`perf record -F 99` 的统计窗口不足。

这些 case 的可靠判断应以内部 benchmark 指标为主：

- `many_tiny_row_number`：`1.57x`，批量 `popFrontRows` 和 schema 共享有效降低固定成本。
- `ignore_nulls_values`：`1.03x`，仍是边界正向。
- `implicit_ordered_agg`：`1.15x`。
- `desc_null_multi_key_agg`：`1.13x`。

如果要继续对这些 fast case 做 perf，需要把输入规模或 benchmark max seconds 单独放大。

## 结论

按内部 `avg_wall_us` 口径，本轮 `BmStreamingWindowBuild` 相对 `StreamingWindowBuild` 在默认 26 个 case 上达到 25 个正向、1 个四舍五入持平但原始值微负，多数 case 明显更快。若成功标准是严格“每个 case 原始 wall 都正向”，当前还差 `by_extreme_other_agg_no_order` 这一项。

最重要的改善：

- aggregate compute path 不再是整体短板。running、reverse-running、bounded ROWS、ordered implicit aggregate、多 aggregate、多排序键 aggregate 均已转正。
- `many_tiny_row_number` 从旧版劣势转为 `1.57x`，说明 tiny partition 固定开销优化有效。
- `by_extreme_other_agg_no_order` 从旧版耗时和内存双劣势转为基本持平，但还不是严格正向。
- value / offset 相关 case 都转为正向，但 `IGNORE NULLS` 仍然只是小幅优势。

仍需谨慎的点：

- `by_extreme_other_agg_no_order` 内部 wall 原始值仍微负，folly `time/iter` 也小幅负向。
- `collection_agg_full_frame` 内部 wall 正向，但 folly `time/iter` 小幅负向。
- `ignore_nulls_values` 只有 `1.03x`，安全边际很小。
- RANGE path 虽然相对优势巨大，但仍是 BM 绝对耗时最大来源。

## 后续优化方向

优先级最高：RANGE frame boundary search。

- 减少 `BmRangeFrameBounds::searchFrameValue` 中的 compare 调用次数。
- 对 fixed-width 单排序键 RANGE path 继续做专用 compare/search kernel。
- 尝试复用边界搜索过程中的 row/order-key 读取结果，降低 `extractColumnResident` 和 repeated compare 成本。

第二优先级：allocator/string-heavy aggregate。

- 拆分 `max_by`、`min_by`、`percentile` 的单独 benchmark，定位 by-extreme case 的边界来源。
- 检查 `StringView` result vector、null buffer、`HashStringAllocator` 的复用策略。
- 对 no-order full-frame complex aggregate 继续减少 repeated group initialization 和 string buffer allocate/free。

第三优先级：fast case 稳定性。

- 为 `ignore_nulls_values`、`collection_agg_full_frame`、`by_extreme_other_agg_no_order` 增加更长运行时间或更大输入规模的定向 benchmark，降低 folly/wall 口径差异。
- 如果目标是严格让 folly `time/iter` 也全正，需要优先处理 collection/by-extreme 两个边界 case。

spill/reclaim 场景仍不放入这个默认 benchmark，需要独立 benchmark 覆盖。
