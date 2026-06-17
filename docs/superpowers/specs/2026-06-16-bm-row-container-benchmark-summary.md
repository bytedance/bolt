# 2026-06-16 BM RowContainer benchmark 总结与对比分析

## 数据来源

本文汇总以下三份分析文档：

- `docs/superpowers/specs/2026-06-16-bm-row-container-latest-benchmark-analysis.md`
- `docs/superpowers/specs/2026-06-16-bm-row-container-pipeline-benchmark-analysis.md`
- `docs/superpowers/specs/2026-06-16-bm-row-container-pipeline-thread-benchmark-analysis.md`

三份文档分别覆盖微基准、单线程端到端 pipeline、4-thread 端到端 pipeline。本文的目标是给出统一 summary，并对三类 benchmark 的结论做横向比较。

## 三类 benchmark 的定位

| 文档 | 测试粒度 | 数据量 | 主要用途 |
|---|---|---:|---|
| `latest-benchmark-analysis.md` | 微基准 | 25 GiB | 分别看 read、storeBatch、storeRow、spillRead、spillWrite 的单项能力 |
| `pipeline-benchmark-analysis.md` | 单线程端到端 | 25 GiB | 看 store -> spill write -> spill read -> read 全链路收益 |
| `pipeline-thread-benchmark-analysis.md` | 4 线程端到端 | 每线程 10 GiB，总 40 GiB | 看并发场景下端到端收益，以及共享 IO 层影响 |

## 总体结论

BM RowContainer 的改造在三个层面都体现了收益：

1. 微基准里，BM 在 read、storeBatch、storeRow、spillRead、spillWrite 的同类 case 中全部快于 old。
2. 单线程 pipeline 里，BM 在 6 个 fixed/variable + raw/lz4/zstd 组合中全部快于 old。
3. 4-thread pipeline 里，按正式口径 `sum_total_ms`，BM 也在 6 个组合中全部快于 old。

但 4-thread 场景暴露了一个新问题：按 wall time 看，`variable raw` BM 反而慢于 old，说明并发下共享 IO scheduler 或 raw IO 排队会影响实际端到端 wall time。

## 端到端收益对比

### 单线程 pipeline

| dataset | compression | old | BM | BM 加速比 |
|---|---|---:|---:|---:|
| fixed | raw | 219.36s | 40.55s | 5.41x |
| fixed | lz4 | 232.67s | 92.29s | 2.52x |
| fixed | zstd | 364.73s | 243.21s | 1.50x |
| variable | raw | 74.85s | 30.46s | 2.46x |
| variable | lz4 | 39.58s | 24.07s | 1.64x |
| variable | zstd | 45.61s | 34.93s | 1.31x |

### 4-thread pipeline

这里使用正式口径 `sum_total_ms`，也就是 4 个线程各自 store、spill write、spill read、read 阶段耗时之和。

| dataset | compression | old | BM | BM 加速比 |
|---|---|---:|---:|---:|
| fixed | raw | 461.27s | 266.91s | 1.73x |
| fixed | lz4 | 922.77s | 202.80s | 4.55x |
| fixed | zstd | 615.68s | 418.07s | 1.47x |
| variable | raw | 236.19s | 116.03s | 2.04x |
| variable | lz4 | 70.99s | 48.58s | 1.46x |
| variable | zstd | 79.44s | 64.23s | 1.24x |

单线程和 4-thread 的大方向一致：BM 都有收益，fixed 收益整体强于 variable，zstd 收益通常弱于 raw/lz4。

## Fixed 场景

fixed 是 BM 最稳的优势区间。

微基准里：

- `storeBatch fixed`：old 23.11s，BM 6.84s，3.38x。
- `spillRead fixed raw/lz4/zstd`：BM 分别约 7.81x、5.66x、1.98x。
- `spillWrite fixed raw/lz4/zstd`：BM 分别约 3.33x、1.54x、1.25x。

单线程 pipeline 里：

- fixed raw 总体 5.41x，是最强单线程端到端收益。
- fixed lz4 2.52x。
- fixed zstd 1.50x，主要被 zstd 压缩成本限制。

4-thread pipeline 里：

- fixed lz4 `sum_total_ms` 4.55x，表现最好。
- fixed raw `sum_total_ms` 1.73x，但 wall time 只有 1.06x。
- fixed zstd `sum_total_ms` 1.47x，仍然受 zstd 压缩成本影响。

结论：fixed 数据上，BM 的 batch append、spill read 都很强；压缩场景里收益会被压缩 CPU 成本稀释，尤其 zstd。

## Variable 场景

variable 的收益主要来自 spill 链路，不是 store。

微基准里：

- `storeBatch variable`：old 5.57s，BM 4.74s，只快 1.18x。
- `storeRow variable`：old 6.16s，BM 5.12s，只快 1.20x。
- variable spill read raw/lz4/zstd：BM 约 3.61x、1.84x、1.65x。
- variable spill write raw/lz4/zstd：BM 约 1.32x、1.96x、1.13x。

单线程 pipeline 里：

- variable raw：2.46x。
- variable lz4：1.64x。
- variable zstd：1.31x。

4-thread pipeline 里，按 `sum_total_ms`：

- variable raw：2.04x。
- variable lz4：1.46x。
- variable zstd：1.24x。

但是 4-thread 下 variable store 明显弱化：

- variable raw store：old 9.73s，BM 13.74s，BM 是 0.71x。
- variable lz4 store：old 9.67s，BM 13.61s，BM 是 0.71x。
- variable zstd store：old 9.67s，BM 13.37s，BM 是 0.72x。

结论：variable 场景端到端仍然是 BM 更快，但不是因为 store 快，而是 spill write/read 的收益抵消了 store 劣势。

## 压缩策略

fixed 数据：

- 压缩收益不明显。
- lz4/zstd 物理写出下降有限，但引入大量 CPU 成本。
- zstd 在 fixed 场景里尤其不划算，端到端收益最弱。

variable 数据：

- 压缩非常有效。
- 单线程 pipeline 中，BM variable raw 物理写出约 25.87 GiB，lz4 约 1.12 GiB，zstd 约 0.78 GiB。
- 4-thread pipeline 中，BM variable raw 物理写出约 41.41 GiB，lz4 约 1.78 GiB，zstd 约 1.26 GiB。

但 zstd 虽然压得更小，CPU 成本更高。综合看，variable lz4 是更均衡的选择。

## 4-thread 特有问题

4-thread 文档最重要的新发现是：`sum_total_ms` 和 `wall_ms` 的结论不完全一致。

按 `sum_total_ms`，BM 全部快。按 `wall_ms`，BM 在 `variable raw` 慢：

| 场景 | old wall | BM wall | 结论 |
|---|---:|---:|---|
| variable raw | 62.47s | 75.98s | BM 慢，0.82x |

这个场景里：

- BM `parallel_efficiency` 只有 1.527。
- old `parallel_efficiency` 是 3.781。
- BM process IO completed bytes 是 82.81 GiB。
- BM avg queue wait 是 4504ms。
- BM avg device latency 是 128.91ms。

说明 BM variable raw 虽然总工作量更少，但并发执行时被共享 IO 排队或线程进度不均衡拖慢了 wall time。

## 统一优化优先级

1. 4-thread raw IO wall time  
   特别是 `variable raw`。这是目前唯一出现 wall time 反向的场景，应优先看 IO scheduler 并发深度、请求粒度、raw spill read/write 以及线程间进度不均衡。

2. variable store  
   单线程里问题不明显，但 4-thread 里 BM variable store 明显慢于 old。后续如果 IO 被优化掉，variable store 会变成更明显瓶颈。

3. fixed zstd / compressed write  
   zstd 场景主要被压缩 CPU 成本限制。继续优化 BM 元数据路径收益有限，需要考虑压缩策略、压缩等级、是否默认使用 lz4。

4. newRow + store fixed 路径  
   微基准里 `storeRow fixed` 虽然快于 old，但仍远慢于 `storeBatch fixed`。如果 Window 仍大量走 `newRow + store`，这条路径仍值得优化。

5. 暂不优先优化 string rebase / heap tail zero  
   文档显示 variable spill read 的 string rebase 约 0.78s，占比不高；`zeroUnusedHeapTail` 约 28ms，也不是瓶颈。

## 最终判断

BM RowContainer 改造整体是有效的：微基准、单线程端到端、4-thread 工作量口径都证明了收益。

现在剩下的问题不再是“BM 有没有收益”，而是“并发 raw IO 下 wall time 能不能稳定体现收益”。短期最值得继续看的，是 4-thread `variable raw` 的 IO 排队和并发效率。
