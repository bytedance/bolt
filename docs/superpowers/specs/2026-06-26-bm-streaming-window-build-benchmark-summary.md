# BM StreamingWindowBuild Benchmark 总结

本文记录 `bolt/exec/bm/benchmarks/StreamingWindowBuildBenchmark.cpp` 的 benchmark 设计调整和默认运行结果。

## Benchmark 设计

原始 benchmark 主要是固定输入形态下比较 `StreamingWindowBuild` 和 `BmStreamingWindowBuild`，case 维度较少：

- 输入固定为 `d/v/p/s`。
- 默认只有 8192 行、2 个大 partition。
- 每个 case 只有一个 window function。
- 没有覆盖 nullable、tiny partition、跨 batch partition、single large partition、多函数共用 build、`IGNORE NULLS`、动态 `RANGE` 等路径。

调整后 benchmark 使用 `InputProfile` 描述输入形态：

- `partitionRows`：控制 partition 大小。
- `peerGroupRows`：控制 peer group 大小。
- `PayloadProfile`：控制 fixed、nullable、large varchar。

`BenchmarkCase` 支持多个 window functions，因此可以覆盖同一个 Window operator 内多个函数共享 partition/order 的情况。

## 覆盖场景

默认 case 覆盖以下方向：

- ranking / row number：`row_number`、`rank`。
- peer group：unique peer、128-row peer、whole-partition peer。
- partition shape：many tiny partition、unaligned partition、single large partition。
- aggregate frame：running、reverse-running、bounded ROWS。
- dynamic RANGE：`range between off preceding and off following`。
- nullable value function：`lead`、`lag`。
- `IGNORE NULLS`：`first_value`、`last_value`、`nth_value`、`lead`、`lag`。
- 多函数共用 build：ranking、value、aggregate 混合。
- 多 aggregate 类型：`sum`、`avg`、`min`、`max`。
- large varchar：`count(v)` full partition。

## 默认运行配置

命令：

```bash
timeout 240s _build/Release/bolt/exec/bm/benchmarks/bolt_streaming_window_build_benchmark
```

输入：

```text
vectors=8
rows_per_vector=1024
total_rows=8192
default_partition_rows=4096
unaligned_partition_rows=1025
string_bytes=1024
cases=15
include_bm_slow=false
```

本 benchmark 只比较纯内存场景。虽然 BM 路径启用了 spill 配置，但默认没有内存压力，因此 `spilledBytes=0`。BM spill/read slow path 应该用单独的 MemoryArbitrator/reclaim benchmark 衡量。

## 默认结果摘要

| case | BM 耗时对比 | peak memory 对比 |
| --- | ---: | ---: |
| `row_number_default` | `1.42x` | `15.20x` lower |
| `rank_unique_peer` | `1.22x` | `15.20x` lower |
| `rank_128_peer` | `1.26x` | `15.20x` lower |
| `rank_full_peer` | `1.42x` | `15.20x` lower |
| `many_tiny_row_number` | `0.99x` | `2.09x` lower |
| `unaligned_running_sum` | `0.57x` | `14.56x` lower |
| `single_part_running_sum` | `0.52x` | `15.21x` lower |
| `reverse_running_sum` | `0.55x` | `12.09x` lower |
| `bounded_rows_64_sum` | `0.59x` | `15.02x` lower |
| `bounded_range_col_sum` | `6.08x` | `10.73x` lower |
| `lead_lag_nullable` | `0.98x` | `11.45x` lower |
| `ignore_nulls_values` | `0.78x` | `8.63x` lower |
| `multi_function_mix` | `0.55x` | `6.22x` lower |
| `multi_agg_types` | `0.41x` | `8.25x` lower |
| `count_large_varchar_full` | `1.04x` | `2.95x` lower |

说明：

- `BM 耗时对比` 大于 1 表示 BM 更快，小于 1 表示 BM 更慢。
- `peak memory 对比` 表示 old peak memory / BM peak memory。

## 结论

BM 在以下场景表现较好：

- `row_number` / `rank` 等 ranking 路径，纯内存下已经能快于原始 StreamingWindowBuild。
- large varchar full partition 的 `count(v)`，整体接近持平略快，同时降低 peak memory。
- 动态 `RANGE` 列边界场景明显更快。

BM 在以下场景仍有明显性能差距：

- aggregate running frame。
- reverse-running frame。
- bounded ROWS frame。
- 多 aggregate function 共用同一个 Window operator。
- `IGNORE NULLS` value functions。

这些慢点主要体现在 `compute_ms`，不是 add input 或 extract column。说明当前 BM 路径的主要瓶颈在 window function 计算阶段的分块 materialize / frame 扫描 / aggregate update 逻辑，而不是行存储或列抽取。

大量 tiny partition 场景中，BM 基本持平但内存收益明显变小。这说明 tiny partition 下 per-partition 管理开销和计算固定开销会稀释 BM 的内存优势。

## 后续优化方向

优先关注 aggregate window compute path：

- running / reverse-running frame 应减少重复扫描。
- bounded ROWS frame 应降低每行 frame materialize 和 aggregate 初始化成本。
- 多 aggregate function 应减少重复读取相同 frame 参数列。
- `IGNORE NULLS` value functions 可继续优化 null 定位和跳转逻辑。

spill 场景需要单独 benchmark。当前这个 benchmark 的定位是纯内存 old/BM 可比性能，不适合混入 BM-only spill slow path。
