# BM StreamingWindowBuild Benchmark 总结

本文记录 `bolt/exec/bm/benchmarks/StreamingWindowBuildBenchmark.cpp` 在 2026-07-02 的默认 benchmark 结果。

## Benchmark 范围

该 benchmark 比较两条路径：

- `StreamingWindowBuild`
- `BmStreamingWindowBuild`

输入由 `InputProfile` 控制：

- `partitionRows`：partition 大小。
- `peerGroupRows`：peer group 大小。
- `PayloadProfile`：fixed、nullable、large varchar。
- `SortProfile`：默认升序单排序键、`ASC NULLS FIRST` 多排序键、`DESC NULLS LAST` 多排序键。

每个 `BenchmarkCase` 可以包含多个 window functions，用于覆盖同一个 Window operator 内多个函数共享 partition/order 的场景。

## 覆盖场景

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
PATH=/data00/home/wangxinshuo.db/tools/miniconda3/bin:/data00/home/wangxinshuo.db/tools/cmake/bin:$PATH \
  cmake --build --preset conan-release --target bolt_streaming_window_build_benchmark
```

运行命令：

```bash
timeout 600s _build/Release/bolt/exec/bm/benchmarks/bolt_streaming_window_build_benchmark
```

完整输出：

```text
/tmp/bolt_streaming_window_build_benchmark_20260702.out
/tmp/bolt_streaming_window_build_benchmark_20260702.err
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

本 benchmark 是纯内存对比。BM 路径启用了 spill 相关配置，但默认运行没有触发内存压力，所有 case 的 `spilledBytes=0`、`spilledRows=0`。BM spill/reclaim slow path 需要单独 benchmark。

## 结果摘要

`BM 耗时对比` 是 StreamingWindowBuild avg wall / BmStreamingWindowBuild avg wall。大于 1 表示 BM 更快，小于 1 表示 BM 更慢。

`Streaming peak / BM peak` 是 StreamingWindowBuild peak memory / BmStreamingWindowBuild peak memory。大于 1 表示 BM peak memory 更低，小于 1 表示 BM peak memory 更高。

| case | BM 耗时对比 | Streaming peak / BM peak | BM avg wall us | BM peak bytes |
| --- | ---: | ---: | ---: | ---: |
| `row_number_default` | `1.42x` | `15.20x` | `1591` | `179968` |
| `rank_unique_peer` | `1.19x` | `15.20x` | `1862` | `179968` |
| `rank_128_peer` | `1.27x` | `15.20x` | `1768` | `179968` |
| `rank_full_peer` | `1.39x` | `15.20x` | `1588` | `179968` |
| `many_tiny_row_number` | `0.89x` | `2.09x` | `4309` | `180160` |
| `unaligned_running_sum` | `0.56x` | `14.56x` | `6246` | `180736` |
| `single_part_running_sum` | `0.52x` | `15.21x` | `5665` | `180736` |
| `reverse_running_sum` | `0.55x` | `12.09x` | `99893` | `230400` |
| `bounded_rows_64_sum` | `0.59x` | `15.02x` | `11160` | `183040` |
| `bounded_range_col_sum` | `6.09x` | `10.73x` | `343279` | `262656` |
| `extended_ranking_peer` | `1.23x` | `11.07x` | `1877` | `253696` |
| `whole_partition_agg_no_order` | `0.95x` | `4.77x` | `3459` | `677760` |
| `global_whole_agg_no_order` | `1.00x` | `6.31x` | `2729` | `527744` |
| `implicit_ordered_agg` | `0.30x` | `9.24x` | `22824` | `331136` |
| `plain_value_functions` | `0.92x` | `10.96x` | `2747` | `255360` |
| `collection_agg_full_frame` | `0.76x` | `1.49x` | `12589` | `403584` |
| `by_extreme_other_agg_no_order` | `0.42x` | `0.25x` | `40309` | `100690560` |
| `explicit_common_frames` | `7.96x` | `8.60x` | `784008` | `339456` |
| `desc_null_multi_key_ranking` | `1.15x` | `9.44x` | `2163` | `302848` |
| `desc_null_multi_key_agg` | `0.31x` | `9.88x` | `18928` | `305792` |
| `asc_null_first_multi_key_value` | `0.87x` | `10.96x` | `2749` | `255360` |
| `lead_lag_nullable` | `0.95x` | `11.45x` | `2580` | `243456` |
| `ignore_nulls_values` | `0.74x` | `8.63x` | `3754` | `333568` |
| `multi_function_mix` | `0.53x` | `6.22x` | `6813` | `489984` |
| `multi_agg_types` | `0.40x` | `8.25x` | `10613` | `356096` |
| `count_large_varchar_full` | `0.93x` | `2.95x` | `6518` | `5636992` |

## 主要观察

BM 更快的 case：

- `row_number_default`：`1.42x`
- `rank_unique_peer`：`1.19x`
- `rank_128_peer`：`1.27x`
- `rank_full_peer`：`1.39x`
- `extended_ranking_peer`：`1.23x`
- `desc_null_multi_key_ranking`：`1.15x`
- `bounded_range_col_sum`：`6.09x`
- `explicit_common_frames`：`7.96x`

接近持平的 case：

- `global_whole_agg_no_order`：`1.00x`
- `whole_partition_agg_no_order`：`0.95x`
- `lead_lag_nullable`：`0.95x`
- `plain_value_functions`：`0.92x`
- `count_large_varchar_full`：`0.93x`

BM 明显更慢的 case：

- `implicit_ordered_agg`：`0.30x`
- `desc_null_multi_key_agg`：`0.31x`
- `multi_agg_types`：`0.40x`
- `by_extreme_other_agg_no_order`：`0.42x`
- `multi_function_mix`：`0.53x`
- `single_part_running_sum`：`0.52x`
- `reverse_running_sum`：`0.55x`
- `unaligned_running_sum`：`0.56x`
- `bounded_rows_64_sum`：`0.59x`

内存方面，除 `by_extreme_other_agg_no_order` 外，BM peak memory 都低于 StreamingWindowBuild。`by_extreme_other_agg_no_order` 的 `Streaming peak / BM peak = 0.25x`，即 BM peak memory 更高。

## Perf 热点分析

### 分析方法和限制

在默认 benchmark 之外，对 5 个代表性 BM case 额外运行 `perf record -F 99 -g`，再用 `perf report --stdio --no-children --sort=symbol` 汇总符号级热点：

| case | 选择原因 | samples |
| --- | --- | ---: |
| `implicit_ordered_agg_bm_streaming` | ordered implicit aggregate 是 BM 明显慢点 | `898` |
| `explicit_common_frames_bm_streaming` | dynamic RANGE / explicit frame 是最大绝对耗时 case | `1113` |
| `by_extreme_other_agg_no_order_bm_streaming` | 同时存在耗时和 peak memory 劣势 | `830` |
| `extended_ranking_peer_bm_streaming` | ranking 快路径代表 | `131` |
| `ignore_nulls_values_bm_streaming` | value / offset nullable 逻辑代表 | `100` |

采样产物保存在：

```text
/tmp/bolt-window-implicit-ordered-agg.perf.report
/tmp/bolt-window-explicit-common-frames.perf.report
/tmp/bolt-window-by-extreme.perf.report
/tmp/bolt-window-extended-ranking.perf.report
/tmp/bolt-window-ignore-nulls.perf.report
```

当前运行环境不支持 `cycles`、`instructions`、`cache-misses`、`branches`、`branch-misses` 等硬件事件，因此没有使用 IPC 或 cache miss 推断。可用的软件事件显示这几个 case 基本都是单 CPU 计算型负载，`by_extreme_other_agg_no_order` 额外有更明显的系统态和 page fault 压力：

| case | task-clock | CPUs utilized | page faults | user time | sys time |
| --- | ---: | ---: | ---: | ---: | ---: |
| `implicit_ordered_agg` | `5322.43 ms` | `1.005` | `40046` | `5.149646 s` | `0.187241 s` |
| `explicit_common_frames` | `8112.27 ms` | `0.999` | `5987` | `8.079595 s` | `0.033989 s` |
| `by_extreme_other_agg_no_order` | `5715.16 ms` | `1.051` | `41497` | `5.082562 s` | `0.785744 s` |

### `explicit_common_frames`：RANGE 边界搜索和 order key 比较

`explicit_common_frames` 的 BM 单次 wall time 为 `785808 us`，其中 `compute_ms=783.679`，耗时几乎全部在 compute phase。

`perf report` 的主要符号：

| symbol | overhead |
| --- | ---: |
| `bytedance::bolt::FlatVector<long>::compare` | `49.51%` |
| `bytedance::bolt::exec::window::BmRangeFrameBounds::searchFrameValue<bool (*)(int)>` | `14.29%` |
| `bytedance::bolt::exec::bm::BmRowContainer::extractColumnResident` | `11.50%` |
| `bytedance::bolt::exec::window::BmWindowPartition::loadResidentRows` | `7.91%` |
| `bytedance::bolt::exec::window::BmRangeFrameBounds::compute(...)::{lambda(int)#1}::_FUN` | `2.43%` |

这说明 dynamic RANGE 的主要热点在 `BmRangeFrameBounds::searchFrameValue`。该路径反复 `loadRows`、`extractColumnFromRows`，再对 order key 做 `FlatVector::compare`。即使 `explicit_common_frames` 相比 StreamingWindowBuild 仍有 `7.96x` 收益，它也是 BM 路径绝对耗时最高的 case，优化优先级很高。

可细化的优化方向：

- 缓存或复用 frame boundary search 过程中读取过的 order key batch，降低 `extractColumnResident` 次数。
- 减少 `loadResidentRows` 为边界搜索重复构造 row pointer batch 的成本。
- 对 fixed-width 单排序键 RANGE path 增加专门比较路径，减少通用 `FlatVector::compare` 调用开销。

### `implicit_ordered_agg`：聚合推进和内存分配混合热点

`implicit_ordered_agg` 的 BM 单次 wall time 为 `22889 us`，其中 `compute_ms=21.079`，`add_ms=0.751`，`extract_ms=0.005`。瓶颈集中在 aggregate compute path。

`perf report` 的主要符号：

| symbol | overhead |
| --- | ---: |
| `bytedance::bolt::exec::window::(anonymous namespace)::BmAggregateWindowFunction::addFrameRows` | `14.37%` |
| `_int_free` | `9.13%` |
| `__pthread_mutex_unlock_usercnt` | `8.35%` |
| `_int_malloc` | `6.01%` |
| `__pthread_mutex_lock` | `5.90%` |
| `bytedance::bolt::AlignedBuffer::allocate<char>` | `3.79%` |
| `bytedance::bolt::Buffer::release` | `3.34%` |
| `bytedance::bolt::memory::MemoryPoolImpl::allocate` | `3.01%` |
| `bytedance::bolt::exec::window::(anonymous namespace)::BmAggregateWindowFunction::apply` | `1.78%` |

`addFrameRows` 下面还能看到 aggregate update 符号，例如 `SimpleNumericAggregate<...>::updateOneGroup...` 和 `SumAggregateBase<...>::addSingleGroupRawInput`。因此 slow aggregate 不是单纯的 row extraction 问题，而是 `BmAggregateWindowFunction::apply/addFrameRows` 的 frame batch 读取、aggregate update、临时 vector/buffer 生命周期共同构成热点。

这个结论也可以解释 `desc_null_multi_key_agg`、`multi_agg_types`、`running_sum`、`bounded_rows_64_sum` 等 aggregate case 的 BM 慢点：它们都主要消耗在 compute phase，而不是 input add 或 output extract。

可细化的优化方向：

- running / ordered implicit frame 优先做累计状态复用，避免每行重复推进相同 frame。
- `BmAggregateWindowFunction::addFrameRows` 复用参数列 vector、SelectivityVector 和 reader batch，减少 `AlignedBuffer::allocate`、`Buffer::release`、`MemoryPoolImpl::allocate/free`。
- 多 aggregate function 共享 frame 参数读取，避免同一 frame 对同一输入列重复 materialize。
- 对 fixed-width numeric aggregate 增加更直接的 batch update path，减少通用 vector/aggregate 接口层开销。

### `by_extreme_other_agg_no_order`：字符串和 allocator 压力

`by_extreme_other_agg_no_order` 的 BM 单次 wall time 为 `37905 us`，其中 `compute_ms=21.652`，`add_ms=0.905`，`extract_ms=0.014`。默认 benchmark 中它的 `BM 耗时对比=0.42x`，`Streaming peak / BM peak=0.25x`，是唯一同时存在耗时和 peak memory 劣势的 case。

`perf report` 的主要符号：

| symbol | overhead |
| --- | ---: |
| `_int_free` | `9.76%` |
| `malloc_consolidate` | `8.92%` |
| `_int_malloc` | `5.66%` |
| `__pthread_mutex_unlock_usercnt` | `5.54%` |
| `__pthread_mutex_lock` | `3.98%` |
| `bytedance::bolt::Buffer::release` | `3.13%` |
| `bytedance::bolt::SimpleVector<StringView>::resetDataDependentFlags` | `2.05%` |
| `bytedance::bolt::FlatVector<StringView>::getBufferWithSpace` | `1.69%` |
| `bytedance::bolt::memory::MemoryPoolImpl::allocate` | `1.57%` |
| `bytedance::bolt::exec::bm::BmRowContainer::extractColumnResident` | `1.33%` |
| `bytedance::bolt::HashStringAllocator::allocateFromFreeLists` | `1.08%` |
| `bytedance::bolt::HashStringAllocator::free` | `0.96%` |

软件 `perf stat` 里该 case 有 `41497` page faults 和 `0.785744 s` sys time，明显高于 `explicit_common_frames` 的 `5987` page faults 和 `0.033989 s` sys time。结合 `100690560` bytes peak memory，可以判断这里的主要问题是 aggregate state / result vector / string buffer 生命周期造成的 allocator 和 page fault 压力，而不只是 compute 算法本身。

可细化的优化方向：

- 拆分 `max_by`、`min_by`、`percentile` 的单独 case，确认 peak memory 主要来自哪类 accumulator。
- 检查 `StringView` result vector 和 `HashStringAllocator` 的复用策略，避免每轮 compute 大量 allocate/free。
- 对 no-order whole-partition by-extreme aggregate 尽量一次性构建 partition 级 state，减少按 output row 重建状态。

### fast case：perf 采样被 benchmark 框架噪声稀释

`extended_ranking_peer` 的 BM 单次 wall time 为 `2025 us`，`compute_ms=0.488`；`ignore_nulls_values` 的 BM 单次 wall time 为 `3975 us`，`compute_ms=2.266`。这两个 case 绝对耗时较低，`perf report` 的 top symbol 被 `folly::runBenchmarkGetNSPerIteration`、DuckDB parser、allocator、task/window 构造等 benchmark 框架和 setup 成本稀释。

因此对 ranking 和 value / offset 快路径，不应只根据当前 `perf report` top symbol 判断热点。更可靠的判断来自 benchmark 阶段耗时：

- ranking path 的 compute 成本已经很低，当前不是优先优化对象。
- `IGNORE NULLS` 的 compute 成本高于普通 value functions，但需要更长运行时间或更窄的微基准，才能把 null 跳转逻辑从 framework/setup 噪声里分离出来。

## 结论

BM 在 ranking 类场景表现稳定，包含普通 ranking、peer-heavy ranking、extended ranking、多排序键 ranking，均快于 `StreamingWindowBuild`，并显著降低 peak memory。

BM 对 dynamic RANGE 和 explicit common frame 的收益最大。`bounded_range_col_sum` 和 `explicit_common_frames` 的耗时对比分别为 `6.09x` 和 `7.96x`。

whole-partition no-order aggregate 基本接近持平，同时保持明显内存收益。global no-order aggregate 耗时持平，peak memory 降低 `6.31x`。

BM 的主要性能短板集中在 aggregate compute path。running、reverse-running、bounded ROWS、ordered implicit aggregate、多 aggregate、多排序键 aggregate 都明显慢于 StreamingWindowBuild。慢点主要体现在 `compute_ms`，不是 add input 或 extract column。`perf` 进一步指向 `BmAggregateWindowFunction::apply/addFrameRows`、aggregate update 以及临时 vector/buffer 分配释放。

value / offset 函数整体接近，但仍略慢。`IGNORE NULLS` value functions 慢于普通 value functions。

collection 和 by-extreme / percentile 场景需要继续拆分分析。`collection_agg_full_frame` 慢于 StreamingWindowBuild 但仍降低内存；`by_extreme_other_agg_no_order` 同时存在耗时和 peak memory 劣势，`perf` 指向 allocator、`StringView` vector 和 `HashStringAllocator` 压力。

## 后续优化方向

优先关注 dynamic RANGE frame boundary search：

- `BmRangeFrameBounds::searchFrameValue` 减少重复 `loadResidentRows` 和 `extractColumnResident`。
- fixed-width 单排序键走专门比较路径，降低 `FlatVector::compare` 开销。
- 复用边界搜索中的 order key batch，减少重复 materialize。

继续优化 aggregate window compute path：

- running / reverse-running frame 减少重复扫描。
- bounded ROWS frame 降低每行 frame materialize 和 aggregate 初始化成本。
- ordered implicit aggregate frame 复用累计聚合状态。
- 多 aggregate function 减少重复读取相同 frame 参数列。
- 多排序键 aggregate 分析 peer/frame 边界查找成本。
- `BmAggregateWindowFunction::addFrameRows` 复用参数列 vector、reader batch 和临时 buffer。

单独分析内存异常 case：

- `by_extreme_other_agg_no_order` 的 accumulator/state 内存。
- `percentile` 在 BM 分块 materialize 下的内存增长。
- `max_by/min_by` 的 result/value state 持有策略。
- `StringView` result vector 和 `HashStringAllocator` 的 allocate/free 频率。

继续优化 value / offset path：

- `IGNORE NULLS` 的 null 定位和跳转逻辑。
- 普通 value functions 的 frame 边界访问成本。
- 为 fast case 增加更窄的微基准或更长采样时间，降低 benchmark framework/setup 噪声。

spill/reclaim 场景不放入这个 benchmark，需要独立 benchmark 覆盖。
