# BufferManager Parallel Sort Prefetch Benchmark 设计

## 背景

当前 `BufferManagerParallelSortBenchmark.cpp` 已经能验证以下能力：

- 8 个 worker 并发工作，每个 worker 拥有独立的 `BufferManager`。
- 所有 worker 共享一个 `ExecutionMemoryPool` 容量上限。
- 数据生成阶段通过 `BufferManager::MaybeReserve` 判断当前 active run 是否应当结束。
- 当内存压力出现时，依赖 `ExecutionMemoryPool` / `ListenableArbitrator` / `TaskMemoryManager` 路径自动触发 BM reclaimer spill。
- verify 阶段通过 k-way merge 读回所有 sorted run，验证全局输出顺序。

现有 verify 阶段的读回方式是每个 run cursor 消费完当前 block 后同步 `Pin` 下一个 block。这个路径能验证基础正确性，但没有利用 `BufferManager::BatchPin` 和 `BufferManager::Prefetch` 的能力，因此无法观察批量读回、异步预读对 merge 阶段的影响。

本设计新增一个独立 benchmark，在保留现有 parallel sort 数据生成、排序、spill/reclaim 路径的基础上，只优化 verify/k-way merge 阶段。

## 目标

新增 benchmark target：

```text
bolt_memory_bm_parallel_sort_prefetch_benchmark
```

新增源码：

```text
bolt/common/memory/bm/benchmark/BufferManagerParallelSortPrefetchBenchmark.cpp
```

目标能力：

- 基于现有 parallel sort benchmark，不改变 active run 形成、排序、spill 触发方式。
- verify 阶段使用 `BatchPin` 批量 pin 每个 run 的首个 block。
- verify 阶段使用 `Prefetch` 对每个 run 的后续 block 做异步预读。
- 保持现有“消费式 move `BlockHandle`”语义，避免已经读回且消费完的 block 因仍被 `SortedRun` 持有而再次进入可 spill 集合。
- 输出与 Prefetch / BatchPin 相关的状态统计和性能统计，方便与现有 benchmark 对比。

## 非目标

- 不修改 `BufferManager` 的语义。
- 不新增显式主动 spill 逻辑。
- 不绕过 `ExecutionMemoryPool` / `ListenableArbitrator` 的自动仲裁路径。
- 不在 verify 阶段同时 pin 每个 run 的多个 block。
- 不把当前 benchmark 重构成公共库。第一版允许复制现有 benchmark 的主体逻辑，以降低对现有 benchmark 的影响。

## Benchmark 参数

新 benchmark 使用独立 gflags，避免和现有 binary 共享同名 flag 时产生歧义。参数语义与现有 parallel sort benchmark 对齐：

```text
--bm_parallel_sort_prefetch_spill_dir
--bm_parallel_sort_prefetch_data_gb_per_thread
--bm_parallel_sort_prefetch_memory_gb
--bm_parallel_sort_prefetch_threads
--bm_parallel_sort_prefetch_allocate_size
--bm_parallel_sort_prefetch_seed
--bm_parallel_sort_prefetch_keep_spill_files
--bm_parallel_sort_prefetch_verify
```

新增参数：

```text
--bm_parallel_sort_prefetch_distance
```

含义：

- 每个 run cursor 最多向前提交多少个 block 的 `Prefetch`。
- 默认值建议为 `4`。
- 设置为 `0` 时关闭 Prefetch，但仍保留首批 `BatchPin`，方便做局部对比。

默认规模保持和现有 parallel sort benchmark 一致：

- worker 数：8
- 共享内存：8 GiB
- 每个 worker 数据量：10 GiB
- block size class：large

## 数据流

### 1. 数据生成和 run 形成

这部分复用现有逻辑：

1. 每个 worker 创建自己的 `SparkListenableArbitratorContext`、`TaskMemoryManager`、root memory pool 和 `BufferManager`。
2. 所有 worker 共享同一个 `ExecutionMemoryPool` 容量限制。
3. worker 从逻辑数据 generator 中持续拉取 uint64 数据，不一次性生成完整 10 GiB 数据。
4. 每次准备分配新 block 前调用 `manager->MaybeReserve(blockBytes)`。
5. 如果 `MaybeReserve` 返回 false 且 active run 非空，则排序当前 active run，然后释放其 `BufferHandle`，让这些 block 变成可被 BM reclaimer spill 的 resident unpinned block。
6. 后续内存压力由 `ExecutionMemoryPool` 仲裁路径触发 reclaim，不在 benchmark 里手动调用 `BufferManager::Reclaim`。

### 2. verify/k-way merge

新增一个 `PrefetchingRunCursor` 替代现有 `RunCursor`。

每个 cursor 只持有一个当前 pinned block：

```text
current BufferHandle
current values pointer
current count
current index
nextBlockIndex
prefetchDistance
```

cursor 不持有多个已 pin block，避免 verify 阶段的 resident 内存随 run 数和 prefetch distance 放大。

## BatchPin 设计

verify 初始化阶段：

1. 为每个非空 run 创建一个 cursor。
2. 从每个 cursor 中 move 出首个 `BlockHandle`。
3. 将所有首个 block 放入一个 vector。
4. 调用一次 `manager.BatchPin(firstBlocks)`。
5. 将返回的 `BufferHandle` 按顺序安装回对应 cursor。
6. 每个成功安装的 cursor 将首个 value 放入 heap。

这里使用 `BatchPin` 的原因：

- k-way merge 启动时天然需要每个 run 的第一个 block。
- 这些 block 之间没有依赖关系，可以一次性提交读请求。
- `BatchPin` 内部可以对多个 spilled block 先提交 read futures，再统一 wait/install。

失败语义沿用 `BatchPin` 当前语义：任意 pin 失败则异常向外传播，worker 标记失败，benchmark 汇总失败。

## Prefetch 设计

每个 cursor 在以下时机补齐 prefetch window：

- 初始首块 `BatchPin` 安装完成后。
- 每次当前 block 消费完，并成功 pin 下一个 block 后。

补齐逻辑：

```text
while submittedPrefetchIndex < nextBlockIndex + prefetchDistance:
    如果 run 中还有 block:
        收集该 block 的 shared_ptr<BlockHandle>
        submittedPrefetchIndex++
manager.Prefetch(collectedBlocks)
```

注意：

- `Prefetch` 是 hint，不改变 correctness。
- `Prefetch` 内部分配内存失败、文件错误或 IO submit 失败时不抛异常，BM 负责记录计数，block 保持 spilled 状态。
- 后续 `Pin` 同一个 block 时，如果 prefetch 已完成，可走更快路径；如果 prefetch 失败或还未完成，则 `Pin` 按 BM 语义同步读回或等待收割。
- benchmark 不依赖 Prefetch 一定成功。

## BlockHandle 消费语义

verify 阶段必须继续使用消费式 move：

```text
auto blockHandle = std::move(run.blocks[index].block);
```

原因：

- `BlockHandle` 的生命周期代表该 block 是否仍属于 benchmark 需要管理的数据集合。
- 当前 block 被 cursor pin 住时由 `BufferHandle` 维持访问。
- 当前 block 消费完后，cursor 释放 `BufferHandle`；如果 `SortedRun` 不再持有 `BlockHandle`，该 block 可以析构并释放资源。
- 如果 `SortedRun` 继续持有已经读回且消费完的 block，该 block unpin 后会重新变成 resident reclaimable，在后续内存压力下可能被再次 spill，导致 verify 阶段出现额外 write/read 干扰。

因此，Prefetch window 只提交还未消费的 block，但不会长期额外持有已经消费的 block。

## 内存语义

新增 benchmark 不主动调用 `BufferManager::Reclaim`。

内存控制仍然依赖：

- active run 阶段的 `MaybeReserve`。
- `ExecutionMemoryPool` cap。
- `ListenableArbitrator` 在内存压力下触发 `TaskMemoryManager` spill。
- `BufferManagerReclaimer` 将 unpinned resident block 下刷。

verify 阶段的额外内存来源：

- 当前每个 run cursor pinned 的一个 block。
- Prefetch 内部可能存在的 in-flight read buffer。

第一版只通过 `prefetchDistance` 控制 prefetch 的提交深度，不额外实现 prefetch memory budget。默认 `4` 是保守值。若后续发现 verify 阶段因为 in-flight prefetch buffer 造成明显内存压力，再考虑新增按字节限制的 prefetch budget。

## 观测指标

benchmark 输出应包含现有 parallel sort benchmark 的统计，并额外突出以下字段：

- `prefetch_distance`
- `batchPinCount`
- `prefetchCount`
- `pinCount`
- `pinInMemoryCount`
- `pinReadCount`
- `spillReadCount`
- `spillWriteCount`
- `spillReadBytes`
- `spillWriteBytes`
- `prefetchSubmitFailures`
- `prefetchIoFailures`
- `generate_and_run_sort_ms`
- `verify_ms`
- `verify_throughput_gib_per_s`
- `automatic_spill_triggers`
- `automatic_spill_requested_bytes`
- `automatic_spill_reclaimed_bytes`
- `automatic_spill_time_us`

这些指标用于回答：

- Prefetch 是否确实被提交。
- BatchPin 是否确实被调用。
- verify 阶段读 IO 是否从同步 `Pin` 转向 prefetch 命中。
- 自动 spill 是否仍然来自 ExecutionMemoryPool 仲裁路径。
- Prefetch 是否带来了额外失败或内存压力。

## 日志

保留现有 `VLOG(1)` 风格，并增加以下关键日志：

- worker 开始 verify 时的 run 数、block 数、prefetch distance。
- 初始 `BatchPin` 的 block 数。
- cursor 补发 prefetch 时的 run index、block range。
- cursor pin next block 时的 run index、block index。

日志只在 `--v=1 --logtostderr=1` 时打开，正常 benchmark 不应输出大量日志。

## 正确性校验

verify 仍然使用 k-way merge：

1. 每个 cursor 当前 value 进入 min-heap。
2. 每次 pop 最小值，检查不小于 previous value。
3. cursor advance 后如果仍有 value，则重新 push heap。
4. 最终 emitted value 数必须等于 worker 预期 value 数。

Prefetch 不参与 correctness 判断。即使所有 Prefetch 都失败，只要 `Pin` 成功，verify 仍应正确。

## 构建验证

实现后至少执行：

```bash
cmake --build --preset conan-release --target bolt_memory_bm_parallel_sort_prefetch_benchmark
```

如果当前 CMake 配置中没有新 target，需要重新配置 benchmark：

```bash
PATH=/data00/home/wangxinshuo.db/tools/miniconda3/bin:$PATH make benchmarks-build BOLT_CONAN_CONFIGURE_ONLY=1
```

然后再次构建新 target。

## 运行验证

先跑小规模 smoke：

```bash
./_build/Release/bolt/common/memory/bm/benchmark/bolt_memory_bm_parallel_sort_prefetch_benchmark \
  --bm_parallel_sort_prefetch_threads=2 \
  --bm_parallel_sort_prefetch_memory_gb=2 \
  --bm_parallel_sort_prefetch_data_gb_per_thread=1 \
  --bm_parallel_sort_prefetch_distance=4 \
  --logtostderr=1
```

再和现有 benchmark 做相同规模对比：

```bash
./_build/Release/bolt/common/memory/bm/benchmark/bolt_memory_bm_parallel_sort_benchmark \
  --bm_parallel_sort_threads=2 \
  --bm_parallel_sort_memory_gb=2 \
  --bm_parallel_sort_data_gb_per_thread=1 \
  --logtostderr=1
```

对比重点：

- 两者都应 `status value=ok`。
- 新 benchmark 的 `batchPinCount` 应大于 0。
- 新 benchmark 的 `prefetchCount` 在 prefetch distance 大于 0 时应大于 0。
- 新 benchmark 不应出现 `prefetchSubmitFailures` 或 `prefetchIoFailures` 的持续增长。
- `verify_ms` 是主要性能对比指标，`generate_and_run_sort_ms` 不应作为 Prefetch 优化效果判断依据。

## 已知取舍

- 第一版复制现有 benchmark 主体逻辑，避免为了复用而先做大规模 benchmark 框架重构。
- `BatchPin` 主要用于首批 block。k-way merge 后续每次通常只有一个 cursor 前进，强行攒批会让控制流复杂，并可能延迟 heap merge。
- `Prefetch` 是主要优化点，因为每个 run 内部访问是顺序的，适合提前提交异步读。
- 第一版不实现 prefetch bytes budget，只提供 `prefetchDistance` 控制提交深度。
- 第一版不改变 BM 的 `Prefetch` hint 语义，不把 Prefetch 失败升级为 benchmark 失败。
