# BM RowContainer Pipeline Benchmark 调查

本文记录 2026-06-10 新增端到端 pipeline benchmark 后的调查结论。pipeline benchmark 的目标是从算子视角观察完整路径成本：

```text
store -> spill write -> spill read -> extract/解析成 Vector
```

它不同于已有 Store、SpillWrite、SpillRead、Read 四类局部 benchmark。局部 benchmark 用来定位某个阶段；pipeline benchmark 用来回答“完整执行链路上，old RowContainer、BM RowContainer loaded read、BM RowContainer window read 分别表现如何”。

## 1. 新增 benchmark 口径

新增文件：

```text
bolt/exec/bm/benchmarks/BmRowContainerPipelineBenchmark.cpp
```

新增 benchmark：

| benchmark | 数据集 | 读模式 | 是否和 old 相对对比 |
| --- | --- | --- | --- |
| `pipelineOld(old_fixed)` | fixed | old row-based spill read | baseline |
| `pipelineBm(bm_loaded_fixed)` | fixed | `kLoadedPointers` | relative |
| `pipelineBm(bm_window_fixed)` | fixed | `kNeedWindowRead` | BM-only |
| `pipelineOld(old_variable)` | variable | old row-based spill read | baseline |
| `pipelineBm(bm_loaded_variable)` | variable | `kLoadedPointers` | relative |
| `pipelineBm(bm_window_variable)` | variable | `kNeedWindowRead` | BM-only |

本次按用户要求不做压缩算法对比，沿用 benchmark 默认压缩配置，也就是 ZSTD。

输入数据生成在计时外完成，计时范围包含：

1. 写入 RowContainer。
2. old spill 或 BM flush。
3. old spill read 或 BM read session。
4. extract 所有列到 Vector。

BM 两条读路径显式分开：

- `bm_loaded_*`：要求 `tryLoadAll()` 返回 `LoadAllResult::kLoadedPointers`。如果返回 `kNeedWindowRead`，benchmark 直接失败。
- `bm_window_*`：通过 `ReadSessionOptions::maxPinnedBytes = 1` 强制 `tryLoadAll()` 返回 `LoadAllResult::kNeedWindowRead`。如果返回 `kLoadedPointers`，benchmark 直接失败。

old RowContainer 不支持类似 BM 的 window read，因此 window read 场景只测 BM，不和 old 做相对对比。

window read 的窗口大小由以下 gflag 控制：

```text
--bm_row_container_pipeline_window_rows=65536
```

默认每次向 `BulkReadSession::loadRows()` 提交 65536 个 `RowId`。

## 2. 过程中发现并修复的问题

### 2.1 问题现象

新增 pipeline benchmark 后，`pipelineBm(bm_window_variable)` 在 64 MiB 输入下稳定崩溃：

```text
Signal 11 (SIGSEGV)
FlatVector<StringView>::setStringViewValue
BmRowContainer::extractColumnResident
extractBmRowsResident
pipelineBm
```

崩溃只发生在 window read + variable 数据集，说明 row 指针本身已经通过 `loadRows()` 得到，但 row 内 `StringView` 的 payload 指针可能没有正确指向当前 pinned heap block。

### 2.2 根因

`BmRowBlockLoader::pinChunk()` 之前只在全局 `BlockRef::ptr` 发生变化时才把 heap block 放入 `heapRebases`：

```cpp
if (oldBase != 0 && oldBase != newBase) {
  heapRebases[block.id] = {oldBase, newBase};
}
```

这个逻辑对 full load 基本成立，但对 window read 不成立。

window read 会分 chunk pin 数据。多个 chunk 可能共享同一个 heap block：

1. 第一个 chunk pin heap block 后，`BlockRef::ptr` 被更新为当前 pinned 地址。
2. 第二个 chunk 再 pin 同一个 heap block 时，`BlockRef::ptr` 可能已经等于当前 pinned 地址。
3. 但是第二个 chunk 中 row block 里的 `StringView` 仍然可能指向它自己的旧 `HeapBaseRef::baseAddress`。
4. 由于 `oldBase == newBase`，旧逻辑不会调用 `rebaseChunk()`。
5. 后续 `extractColumnResident()` 读取 `StringView` 时访问旧地址，触发崩溃。

### 2.3 修复

修复思路：

- `pinChunk()` 对 heap block 总是把当前 pinned base 传给 `rebaseChunk()`。
- `rebaseChunk()` 再按每个 `ChunkPartMeta::heapBases` 的 `baseAddress` 判断是否需要 rebase。
- 如果 part 记录的 base 已经等于当前 pinned base，就跳过。

也就是说，是否需要 rebase 不能只看全局 `BlockRef::ptr`，必须看当前 chunk part 自己记录的 heap base。

新增回归测试：

```text
BmRowContainerTest.WindowReadRebasesStringsAcrossChunks
```

该测试构造超过一个 chunk 的 VARCHAR 数据，强制 window read，然后一次性 load 多个 chunk 并 extract string，覆盖多个 chunk 共享 heap block 的场景。

验证结果：

```text
BmRowContainerTest.*: 8/8 passed
```

## 3. 256 MiB pipeline 结果

运行命令：

```bash
./_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark \
  --bm_row_container_data_bytes=268435456 \
  --bm_regex='pipeline.*' \
  > /tmp/bm-row-container-pipeline-256m-stdout.txt \
  2> /tmp/bm-row-container-pipeline-256m-stderr.txt
```

### 3.1 stdout 摘要

| 场景 | time/iter | 结论 |
| --- | ---: | --- |
| `pipelineOld(old_fixed)` | 3.74s | fixed baseline |
| `pipelineBm(bm_loaded_fixed)` | 2.91s | BM loaded 比 old 快约 28.6% |
| `pipelineBm(bm_window_fixed)` | 51.81s | BM window fixed 极慢 |
| `pipelineOld(old_variable)` | 385.54ms | variable baseline |
| `pipelineBm(bm_loaded_variable)` | 267.00ms | BM loaded 比 old 快约 44.4% |
| `pipelineBm(bm_window_variable)` | 337.81ms | BM window variable 慢于 loaded，但仍接近 old |

### 3.2 fixed metrics

| 指标 | old fixed | BM loaded fixed | BM window fixed |
| --- | ---: | ---: | ---: |
| rows | 13,421,773 | 13,421,773 | 13,421,773 |
| store | 410.074ms | 338.840ms | 338.625ms |
| spill write | 2,098.101ms | 1,926.684ms | 1,927.875ms |
| spill read | 1,164.670ms | 599.795ms | 49,466.573ms |
| extract | 69.692ms | 46.350ms | 52.403ms |
| total | 3,742.537ms | 2,911.669ms | 51,785.475ms |
| BM batch pins | - | 1 | 13,108 |
| BM pin reads | - | 77 | 77 |
| BM spill read bytes | - | 322,961,408 | 322,961,408 |
| BM physical read bytes | - | 233,624,009 | 233,008,289 |
| BM decompress | - | 501.367ms | 527.525ms |

fixed 的 loaded path 表现符合预期：store、spill write、spill read、extract 都比 old 更快。

fixed 的 window path 明显异常：物理读和解压量与 loaded path 接近，但 `spill_read_ms` 从 599.795ms 放大到 49,466.573ms。主要差异是 `bm_batch_pins` 从 1 次变成 13,108 次。

### 3.3 variable metrics

| 指标 | old variable | BM loaded variable | BM window variable |
| --- | ---: | ---: | ---: |
| rows | 257,123 | 257,123 | 257,123 |
| store | 62.541ms | 41.828ms | 71.746ms |
| spill write | 163.027ms | 148.107ms | 154.139ms |
| spill read | 133.484ms | 52.344ms | 101.346ms |
| extract | 26.898ms | 24.709ms | 24.949ms |
| total | 385.950ms | 266.988ms | 352.180ms |
| BM batch pins | - | 1 | 252 |
| BM pin reads | - | 66 | 66 |
| BM spill read bytes | - | 276,824,064 | 276,824,064 |
| BM physical read bytes | - | 8,560,115 | 8,447,091 |
| BM decompress | - | 43.770ms | 68.105ms |
| BM full-load rebase | - | 3.956ms | 0 in metrics |

variable window path 比 loaded 慢，但没有 fixed 那么严重。原因是 variable 数据集行数少很多，window read 只产生 252 次 batch pin，而 fixed 产生 13,108 次。

当前 window read 的 rebase metrics 没有统计到 `BulkLoadMetrics::rebaseStringViewsNs`，因为 `loadRows()` 还没有 metrics 参数，`pinChunk()` 内部调用 `rebaseChunk(..., nullptr)`。因此 variable window 的 rebase 成本现在被包含在 `spill_read_ms` 里，但没有被单独拆出。

## 4. perf 结果

尝试使用硬件事件：

```bash
perf stat -e cycles,instructions,cache-references,cache-misses,branches,branch-misses ...
```

结果为：

```text
<not supported> cycles:u
<not supported> instructions:u
<not supported> cache-references:u
<not supported> cache-misses:u
<not supported> branches:u
<not supported> branch-misses:u
```

说明当前机器或权限配置不支持这些硬件计数器。因此本次 perf 只能使用默认可用指标。

64 MiB fixed loaded：

```text
pipelineBmLoaded fixed total_ms=801.069
task-clock:u = 2027.23 ms
page-faults:u = 105602
time elapsed = 2.145s
```

64 MiB fixed window：

```text
pipelineBmWindow fixed total_ms=3901.721
task-clock:u = 4322.25 ms
page-faults:u = 93147
time elapsed = 4.354s
```

perf 默认指标与 benchmark metrics 方向一致：fixed window 的 CPU 时间显著高于 loaded。因为缺少 cycles/instructions/cache counters，本次不能进一步判断 IPC、cache miss 或分支层面的瓶颈，只能结合 BM metrics 判断主要问题在大量 BatchPin 调用。

## 5. 主要结论

### 5.1 loaded path 表现健康

在 256 MiB 数据下：

- BM loaded fixed：2.91s，对 old fixed 3.74s，有明显优势。
- BM loaded variable：267ms，对 old variable 386ms，有明显优势。

这说明端到端路径中，BM 的 resident store、flush、full load、extract 组合是有效的。

### 5.2 fixed window read 当前不可接受

fixed window read 的主要问题不是 IO 量，也不是解压量：

- BM loaded fixed physical read：233.6MB。
- BM window fixed physical read：233.0MB。
- BM loaded fixed decompress：501ms。
- BM window fixed decompress：528ms。

真正的差异来自 pin 调度方式：

- loaded：1 次 BatchPin，77 个 block。
- window：13,108 次 BatchPin，77 个实际 pin read。

这说明 window read 的 `loadRows()` 当前在按 chunk 循环调用 `pinChunk()`。即使底层实际 spill read 只有 77 次，也付出了 13,108 次 BatchPin 的管理成本。

### 5.3 variable window read 暂时可用，但观测还不完整

variable window read 的总耗时 352ms，接近 old 386ms，但慢于 BM loaded 267ms。

variable 的主要原因是 row 数较少，window read 只产生 252 次 BatchPin，不像 fixed 那样放大到 13,108 次。因此它没有暴露出同等严重的 BatchPin 管理成本。

不过 variable window 的 string rebase 时间还没有单独统计，后续优化前应该先补观测。

## 6. 优化建议

### 优先级 1：给 window read 做窗口级 BatchPin

当前 `BulkReadSession::loadRows()` 按 RowId 找到 chunk，然后对每个 chunk 调 `pinChunk()`。这导致一个 window 内有多少 chunk，就可能调用多少次 BatchPin。

建议新增窗口级加载路径：

```text
loadRows(rowIds)
  -> 收集 rowIds 涉及的 chunks
  -> 收集这些 chunks 需要的 rowBlocks / heapBlocks
  -> 按 BlockId 去重
  -> 一次 BatchPin
  -> 更新所有 BlockRef::ptr
  -> 对涉及的 chunks 做 rebase
  -> 构造 RowView
```

目标是让 fixed window 的 `bm_batch_pins` 从 13,108 降到接近 window 数或更少。256 MiB fixed 默认 205 个 windows，如果每个 window 一次 BatchPin，理论上应从 13,108 降到约 205。

### 优先级 2：window 内 block 去重

fixed 数据集中多个 chunk 会共享同一 row block。即使做了窗口级 BatchPin，如果不按 BlockId 去重，仍会重复提交同一个 block。

建议以 `BlockId` 作为去重 key，同时保留 `BlockRef*`，BatchPin 返回后统一更新 block pointer。

### 优先级 3：补 window read metrics

当前 BM window metrics 缺少以下拆分：

- `window_collect_chunks_ms`
- `window_collect_blocks_ms`
- `window_batch_pin_ms`
- `window_update_ptrs_ms`
- `window_rebase_strings_ms`
- `window_append_row_views_ms`
- `window_distinct_chunks`
- `window_distinct_blocks`

这些指标能直接验证优化是否生效，也能解释 variable window 的 string rebase 成本。

### 优先级 4：避免 window read 下重复 rebase

修复 StringView 崩溃后，`pinChunk()` 会更保守地把 heap block 传给 `rebaseChunk()`，确保正确性。后续如果 rebase 成本在 variable window 中变大，可以优化为：

- 按 chunk/part 记录当前已 rebase 到哪个 heap base。
- 如果 part 的 `heapBase.baseAddress` 已经等于当前 pinned base，跳过扫描 rows。
- 对多 heap part 使用 last-hit cache 或按 oldBase 排序后 `upper_bound` 查找。

当前 256 MiB variable loaded 的 full-load rebase 只有 3.956ms，说明 rebase 本身不是 loaded path 瓶颈。window path 是否需要进一步优化，需要先补 metrics。

### 优先级 5：必要时调整 benchmark window size

`--bm_row_container_pipeline_window_rows` 默认 65536。调大该值可以减少 window 次数，但不能解决每个 window 内按 chunk 多次 BatchPin 的根因。

因此建议先做窗口级 BatchPin，再根据真实算子访问模式调整默认 window rows。

## 7. 后续验证建议

完成窗口级 BatchPin 后，至少重新跑：

```bash
./_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark \
  --bm_row_container_data_bytes=268435456 \
  --bm_regex='pipeline.*'
```

重点观察：

- fixed window 的 `bm_batch_pins` 是否从 13,108 降到约 205 或更低。
- fixed window 的 `spill_read_ms` 是否接近 loaded path。
- variable window 的 `window_rebase_strings_ms` 是否可控。
- loaded path 是否没有回退。

如果机器允许硬件 perf counters，再补跑：

```bash
perf stat -e cycles,instructions,cache-references,cache-misses,branches,branch-misses \
  ./_build/Release/bolt/exec/bm/benchmarks/bolt_exec_bm_row_container_benchmark \
  --bm_row_container_data_bytes=67108864 \
  --bm_regex='.*bm_window_fixed.*'
```

本次机器返回 `<not supported>`，所以没有可用的 CPU/cache 硬件计数器结论。
