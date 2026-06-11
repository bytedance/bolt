# HashAggr JIT 性能评测报告

## 1. 测试环境与方法

- **构建**：`Release` + spark 开关（`spark_compatible=True / enable_testutil=True /
  skip_test=False`），对齐 `make release_spark_with_test`；benchmark 单独
  `BOLT_BUILD_BENCHMARKS=ON`，未启用 `enable_perf`（gperftools 源码下载超时，folly
  benchmark 不依赖它）。
- **benchmark**：`bolt/exec/benchmarks/HashAggrJitBenchmark.cpp`，目标
  `bolt_hashaggr_jit_benchmark`。覆盖 sum/avg/min/count（width 4/8/16/32）、
  merge（partial+final）、decimal sum/avg（当前按 `PartialFinal` 路径评测）、
  double min/max、partial extract。
- **数据规模**：每用例 20 batch × 10000 行。
- **关键控制**：
  - JIT 模块为进程级 LRU 全局缓存，预热后**每个 JIT 函数仅编译一次**（已用 VLOG 验证
    每个函数名 compile 次数 = 1），编译开销不计入迭代。
  - 两条路径都先 warm-up 再计时；热路径调试日志默认静默（已降级为 `VLOG(1)`）。
  - speedup = nojit / jit，**> 1 表示 JIT 更快**。

运行命令：

```bash
# 低基数（聚合计算密集）
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --hashaggr_jit_benchmark_batches=20 --hashaggr_jit_benchmark_groups=100

# 高基数（哈希探测密集）
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --hashaggr_jit_benchmark_batches=20 --hashaggr_jit_benchmark_groups=10000
```

## 2. 低基数结果（groups=100）

| 聚合 | width4 | width8 | width16 | width32 |
|------|--------|--------|---------|---------|
| **count**（single） | **1.14x** | **1.27x** | **1.25x** | **1.33x** |
| **count**（merge）  | **1.15x** | **1.23x** | **1.19x** | **1.34x** |
| sum（single）       | 0.46x | 0.38x | 0.38x | 0.34x |
| sum（merge）        | 0.47x | 0.40x | 0.37x | 0.37x |
| avg（single）       | 0.52x | 0.46x | 0.44x | 0.45x |
| avg（merge）        | 0.54x | 0.49x | 0.45x | 0.45x |
| min（single）       | 0.49x | 0.42x | 0.38x | 0.39x |
| min（merge）        | 0.50x | 0.43x | 0.39x | 0.40x |

其他（width8）：decimal_sum **0.40x** · decimal_avg **0.75x** · double_min 0.57x ·
double_max 0.55x · partial_avg_extract 0.82x · partial_sum_extract 0.84x

> 38 个用例中：JIT 更快 8 个（全部是 count），更慢 30 个。groups=10 对照趋势一致（误差 < 5%）。

## 3. 高基数结果（groups=10000）

| 聚合 | width4 | width8 | width16 | width32 |
|------|--------|--------|---------|---------|
| **count**（single） | 1.08x | **1.44x** | **1.59x** | **1.60x** |
| **count**（merge）  | 0.94x | **1.14x** | **1.22x** | **1.29x** |
| sum（single）       | 0.66x | 0.71x | 0.74x | 0.71x |
| sum（merge）        | 0.68x | 0.71x | 0.72x | 0.73x |
| avg（single）       | 0.80x | 0.79x | 0.84x | 0.80x |
| avg（merge）        | 0.52x | 0.49x | 0.51x | 0.52x |
| min（single）       | 0.62x | 0.57x | 0.62x | 0.66x |
| min（merge）        | 0.68x | 0.60x | 0.64x | 0.64x |

其他（width8）：decimal_sum 0.88x · decimal_avg 0.88x · double_min 0.75x ·
double_max 0.63x · partial_avg_extract 0.74x · partial_sum_extract 0.84x

## 4. 关键发现

1. **只有 count 稳定正收益**：低基数 1.14–1.34x，高基数最高 1.60x，且随 fuse 宽度增大而提升。
   count 的 accumulator 最简单，融合循环省下的逐聚合函数调用/分支开销占主导。
2. **sum/avg/min/max/decimal 在 JIT 下更慢，且基数越低越慢**：sum 从高基数 0.71x 跌到
   低基数 0.34–0.46x。
3. **瓶颈在 JIT add 逐行路径，而非哈希探测**：groups=10 与 groups=100 的 JIT 绝对耗时
   几乎相同（如 width8_sum jit ≈ 5.0ms 两者一致），说明耗时与组数无关、只与行数相关——
   即**每行 add 成本** JIT 高于非 JIT 的向量化路径。这正是“低基数本应让 JIT 更受益”的
   预期被反转的根本原因。
4. **decimal_avg(0.75x) 曾优于 decimal_sum(0.40x)**：这组历史数据采集时，decimal_avg
   final 仍走非 JIT（Spark rescale 复杂逻辑），因此拖累相对较小。当前已补齐 final
   decimal avg extract JIT helper，新的 decimal_avg 结果需以 `PartialFinal` 基准重新观察。

## 5. 结论与建议

- **现状**：HashAggr JIT 当前仅对 **count** 类（轻 accumulator、宽融合）有明确收益；
  sum/avg/min/max/decimal 的 JIT 计算路径尚慢于现有向量化实现，**不建议默认开启**这些
  聚合的 JIT。
- **根因**：JIT add 内核的输入读取退化为**逐行外部 C 函数调用**，丧失了内联与向量化
  （详见第 6 章 perf 定位）。
- **后续可做**：
  1. 把输入读取从 `jit_GetDecodedValue*` 外部调用改为 **JIT 内联**（直接对 flat/identity
     映射的 raw buffer 做 GEP+load），让 LLVM 能向量化取值-累加循环；
  2. 按聚合类型设白名单（先只对 count 默认启用 JIT）；
  3. 对 avg-merge 的重路径专门优化。

## 6. perf 定位：sum/avg add 内核瓶颈

环境：`perf`（linux-tools-5.15）+ `perf_event_paranoid=1`，`-F 2999 --call-graph dwarf`，
对 `width16_sum`（fuse=16，groups=100）single-aggregation 的 JIT / 非 JIT 两条路径分别采样。

### 6.1 热点符号对比（self time，同一工作负载）

| 项 | JIT 路径 | 非 JIT 路径 |
|----|----------|-------------|
| 输入取值 | `jit_GetDecodedValueI64`（外部调用，逐行）**45.4%** | `jit_GetDecodedValueI64` 仅 **0.45%** |
| 累加内核 | `[JIT]` 匿名生成码合计 **~25%** | `SumAggregateBase<long,...>::addRawInput`（内联模板）**52.0%** |
| 哈希探测 | `arrayGroupProbe` 0.9% | `arrayGroupProbe` 2.2% |

### 6.2 根因分析

JIT add 内核的逐行循环（`HashAggrJit.cpp:685` 的 `genAddDenseIR`）对**每行每列**都生成一次
`CreateCall(jit_GetDecodedValueI64, {decoded, row})`（取值封装见 `loadDecodedValue`
`HashAggrJit.cpp:428`、helper 实现见 `RowContainer.cpp:1724`）。其代价：

1. **不可内联的跨边界调用**：每行付出 call/ret + 调用约定下的寄存器溢出；
2. `DecodedVector::valueAt<int64_t>(index)` 内部还要判断 identity-mapping、做 indices 间接寻址；
3. **阻断向量化**：取值-累加循环因夹着 opaque 外部调用，LLVM 无法做 SIMD/循环展开。

而非 JIT 路径走 `SumAggregateBase::addRawInput`，整批输入在编译期类型已知、`DecodedVector`
raw buffer 被**内联顺序读取**并可向量化，因此 `jit_GetDecodedValueI64` 在该路径几乎不出现（0.45%）。

**count 为何不受影响**：count（`countStar` 或仅计数）不读取输入值，在 add 内核里跳过取值与
null 检查（`HashAggrJit.cpp:697`），故没有 `jit_GetDecodedValue*` 开销，融合循环的省调用收益得以体现。

### 6.3 优化方向

最高优先级是**消除逐行外部取值调用**：对 flat / identity-mapping 的输入，在 JIT 内核里直接拿到
`DecodedVector` 的 `data()` 基址，用 `GEP + load`（dictionary 映射则内联 indices 间接寻址）替换
`jit_GetDecodedValue*` call，使整段取值-累加可被 LLVM 向量化——预期能把 sum/avg/min/max 的 JIT
路径从当前 0.4–0.8x 拉回到 ≥1x。

## 7. Direct Decoded Descriptor 优化验证

### 7.1 优化内容

本轮优化按第 6.3 节方向实现：`GroupingSet` 在每个 batch 为每个聚合输入准备一个轻量 descriptor，
JIT add 内核不再对 raw 单值输入逐行调用 `jit_GetDecodedValue*` / `jit_GetDecodedIsNull`，而是在 IR
内直接读取：

1. `values`：decoded 后 base vector 的 raw values 基址；
2. `indices`：top-level row -> base row 的映射。flat 为 identity mapping，dictionary / constant 也由
   `DecodedVector` 统一展开为同一套映射；
3. `nulls`：top-level row null bitmap。若为 null，IR 直接跳过 null check；
4. `decodedVector`：保留原始 `DecodedVector*`，仅用于 intermediate ROW merge 的 row-field helper。

这样同一段 JIT IR 可以同时覆盖 flat / dictionary / constant 输入编码，不需要按 batch encoding 重新
codegen；热循环中的普通数值读取变为 `index = indices[row]` + `values[index]`。

### 7.2 对比方法

为了衡量本次优化本身的收益，分别构建并运行了两版同一 benchmark：

- **baseline/helper-call 版本**：原实现，每行每列调用 `jit_GetDecodedValue*` / `jit_GetDecodedIsNull`；
- **optimized/direct-descriptor 版本**：当前实现，IR 内直接读取 descriptor。

运行命令：

```bash
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=5 \
  --bm_max_secs=3 \
  --bm_regex='(width8_(sum|avg|min|count|double_min|double_max)_(nojit|jit)|width8_decimal_(sum|avg)_(nojit|jit))'
```

测试数据规模仍为默认 20 batch × 10000 行，groups=10000，width=8。

### 7.3 JIT 自身优化收益

下表只比较两版 JIT 路径：

| case | helper-call JIT | direct-descriptor JIT | JIT 优化收益 |
|------|----------------:|----------------------:|-------------:|
| width8_sum | 6.78ms | 3.94ms | **快 41.9%** |
| width8_avg | 6.90ms | 4.69ms | **快 32.0%** |
| width8_min | 6.71ms | 4.05ms | **快 39.6%** |
| width8_count | 3.00ms | 2.96ms | 快 1.3% |
| width8_decimal_sum | 13.27ms | 9.52ms | **快 28.3%** |
| width8_decimal_avg | 18.08ms | 14.47ms | **快 20.0%** |
| width8_double_min | 6.77ms | 4.81ms | **快 29.0%** |
| width8_double_max | 6.77ms | 4.55ms | **快 32.8%** |

结论：消除 `jit_GetDecodedValue*` 外部 helper call 后，所有需要读取输入值的聚合都有明显收益，
幅度约 **20%–42%**。`count` 基本不读取 input value，因此收益很小，符合预期。

### 7.4 优化后 JIT vs no-JIT

下表比较当前 direct-descriptor JIT 与非 JIT 路径：

| case | no-JIT | direct-descriptor JIT | speedup = nojit / jit |
|------|-------:|----------------------:|----------------------:|
| width8_sum | 4.64ms | 3.94ms | **1.18x** |
| width8_avg | 5.29ms | 4.69ms | **1.13x** |
| width8_min | 3.75ms | 4.05ms | 0.93x |
| width8_count | 4.30ms | 2.96ms | **1.45x** |
| width8_decimal_sum | 11.96ms | 9.52ms | **1.26x** |
| width8_decimal_avg | 16.09ms | 14.47ms | **1.11x** |
| width8_double_min | 5.06ms | 4.81ms | **1.05x** |
| width8_double_max | 4.21ms | 4.55ms | 0.93x |

优化前，sum / avg / min / decimal / double min/max 的 JIT 路径大多慢于 no-JIT；优化后，sum、avg、
count、decimal_sum、decimal_avg、double_min 已经变为正收益。`width8_min` 和 `width8_double_max` 仍略慢，
后续需要继续看 min/max accumulator null 判断、compare 分支以及 NaN 处理逻辑。

### 7.5 更新后的结论

- 第 6 章定位的主要瓶颈（逐行不可内联 helper call）已被验证：去掉 helper call 后，需要读输入值的
  JIT 聚合普遍获得 **20%–42%** 的 JIT 内核收益。
- HashAggr JIT 不再只有 count 有收益；在当前 width8 / groups=10000 场景下，sum、avg、decimal、
  double_min 也已经超过 no-JIT。
- 剩余负收益集中在 min/max 类 case，下一步优化重点应转向比较更新逻辑本身，而不是 decoded value 读取。

## 8. Direct Descriptor 后的最新 perf 定位

### 8.1 为什么仍远低于 multi_sum POC 预期

multi_sum POC 的核心收益假设是：把 `sum(c1)..sum(cN)` 合并后，可以显著减少重复的 group/hash lookup，
并让 `NumArgs` 在编译期已知，从而获得 loop unrolling。该假设对 POC 成立，但和当前 Bolt
HashAggregation 的生产路径并不完全等价。

当前 `GroupingSet::addInputForActiveRows` 中，hash/group probe 在所有 aggregate 之前统一执行一次：
`prepareForGroupProbe` / `groupProbe` 先生成 `groups = lookup_->hits.data()`，之后才进入聚合函数循环
或 JIT chunk 执行。因此 **no-JIT 的多个 separate sums 已经共享同一次 hash lookup**，JIT 并不能像 POC
那样再节省 7 次或 15 次 hash lookup。JIT 当前主要节省的是每个 aggregate 独立 `addRawInput` 的函数
调度、decoded 读取和多次遍历 rows 的开销。

另外，benchmark 使用 `AssertQueryBuilder(...).copyResults()` 测的是完整查询路径，不是纯 add kernel：
它还包含 input hash/vector encoding、RowContainer 新 group 初始化、结果 extract、结果 RowVector copy、
task/benchmark 框架等共同开销。消除 `jit_GetDecodedValue*` 后，add kernel 已明显变快，但完整查询的
Amdahl 上限被这些公共开销压低。

相关代码位置：

- 单次 group probe：`bolt/exec/GroupingSet.cpp:344`
- probe 后统一进入 JIT chunk / aggregate function add：`bolt/exec/GroupingSet.cpp:375`
- JIT chunk 执行后 no-JIT aggregate 被跳过：`bolt/exec/GroupingSet.cpp:381`

### 8.2 perf 方法

由于当前机器上硬件 counter（cycles/cache-misses/L1/dTLB 等）不可用，本轮使用 software
`cpu-clock` 采样；`/usr/bin/perf` wrapper 找不到匹配 5.4 内核的 perf binary，实际使用
`/usr/lib/linux-tools-5.15.0-160/perf`。

为了减少 benchmark 初始化和 warm-up 对结果的污染，使用较大的 `--bm_min_iters`，让被测 case 的计时
阶段占主导。代表性命令：

```bash
/usr/lib/linux-tools-5.15.0-160/perf record -F 2999 \
  -o /tmp/bolt-width16-sum-jit-long.perf.data -- \
  ./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=1500 --bm_max_secs=30 --bm_regex='^width16_sum_jit$'

/usr/lib/linux-tools-5.15.0-160/perf record -F 2999 \
  -o /tmp/bolt-width16-sum-nojit-long.perf.data -- \
  ./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=1500 --bm_max_secs=30 --bm_regex='^width16_sum_nojit$'
```

### 8.3 最新 sum benchmark 结果

在 direct-descriptor 优化后，sum 的完整查询收益如下：

| case | no-JIT | JIT | speedup = nojit / jit |
|------|-------:|----:|----------------------:|
| width4_sum | 2.85ms | 2.73ms | **1.04x** |
| width8_sum | 4.68ms | 4.15ms | **1.13x** |
| width16_sum | 8.92ms | 7.48ms | **1.19x** |
| width32_sum | 17.05ms | 14.84ms | **1.15x** |

可以看到趋势与 POC 一致：中等宽度有收益；但收益幅度只有约 4%–19%，远低于 POC 中 8/16 列
约 42%–43% 的提升。根因是当前生产路径的 no-JIT baseline 已共享 hash probe，且完整查询包含较多
JIT 无法消除的公共开销。

### 8.4 width8_sum perf 热点

长时间采样下的 self-time 分类如下（`cpu-clock` samples）：

| 分类 | JIT | no-JIT | 说明 |
|------|----:|-------:|------|
| add kernel | **32.72%**（JIT generated add_dense） | **45.48%**（`SumAggregateBase::addRawInput`） | JIT add 已明显少于 no-JIT add |
| hash/vector encoding | 4.15% | 4.67% | 双方共同开销 |
| hash probe | 2.27% | 2.33% | 双方都只 probe 一次，JIT 不再有 POC 中的“省多次 lookup”收益 |
| result/input copy | 3.55% | 2.74% | 完整 `copyResults` 路径成本 |
| RowContainer new/store/init | 1.52% | 2.44% | 新 group / row storage 成本 |
| JIT extract setter | **4.35%** | 0.10% | JIT extract 仍调用 `jit_HashAggrSetFlatI64` helper |
| dynamic_cast/type dispatch | **6.62%** | 0.21% | JIT 路径额外的结果/类型处理开销 |

top symbols 中 JIT 路径最大热点已经从 `jit_GetDecodedValueI64` 迁移到 `[JIT]` 生成码本身；
`jit_GetDecodedValue*` 不再是主热点，说明第 7 章的 direct descriptor 已生效。

### 8.5 width16_sum perf 热点

width16 下趋势更明显：

| 分类 | JIT | no-JIT | 说明 |
|------|----:|-------:|------|
| add kernel | **35.51%**（JIT generated add_dense） | **53.87%**（`SumAggregateBase::addRawInput`） | JIT kernel 节省明显 |
| aggregate init | 0.02% | 4.99% | JIT fused init 基本消除了 per-aggregate init 热点 |
| hash/vector encoding | 1.69% | 2.46% | 共同开销 |
| hash probe | 1.90% | 1.28% | 共同开销；采样误差下同量级 |
| result/input copy | 4.24% | 3.82% | 完整查询共同成本 |
| JIT extract setter | **6.37%** | 0.08% | JIT extract helper 成为新热点之一 |
| dynamic_cast/type dispatch | **7.02%** | 0.17% | JIT 路径额外成本，抵消一部分 add kernel 收益 |

从绝对耗时估算，JIT add kernel 已从 no-JIT 的约 4.9ms（`8.92ms * 53.87%`）降到约 2.7ms
（`7.48ms * 35.51%`）。也就是说 add kernel 本身接近 **1.8x**，但完整查询最终只有 **1.19x**，
因为剩余时间被 probe、encoding、output materialization、copy 和 JIT extract helper/type dispatch 稀释。

### 8.6 最新瓶颈排序

1. **JIT 已无法再通过“少做 hash lookup”获得 POC 级收益**：Bolt baseline 本身已经 one probe for all
   aggregates，这是和 multi_sum POC 最大的结构差异。
2. **JIT add_dense 生成码仍是最大热点**：direct descriptor 去掉 helper call 后，热点回到真正的
   scalar RMW 聚合内核。当前每个 slot 每行仍要做 `indices[row]`、`values[index]`、accumulator null bit
   clear、old accumulator load、add、store；这些操作围绕 `groups[row]` 间接指针，LLVM 很难 SIMD 化。
3. **JIT extract 仍有 helper/type-dispatch 开销**：`jit_HashAggrSetFlatI64` 和 `__dynamic_cast` 在 JIT
   路径合计约 10%–13%，这是 direct descriptor 后的新显性瓶颈。no-JIT extract 使用 aggregate 自身的
   typed extract，开销低得多。
4. **完整 query benchmark 的公共成本很高**：hash/vector encoding、RowContainer、result copy、task 框架等
   不随 add kernel 优化而下降，限制最终端到端 speedup。

### 8.7 后续调优建议

1. **优化 JIT extract**：像 add path 一样为 extract 也传入 output descriptor（raw values/nulls），在 IR
   里直接写 FlatVector buffer，替换 `jit_HashAggrSetFlatI64` helper，并尽量避免 `dynamic_cast`。
2. **增加 flat/no-null 快路径**：当前为了同时支持 flat/dictionary/constant，所有输入都走 `indices[row]`。
   可以保持同一份 IR 兼容多 encoding，但在 loop preheader 根据 descriptor 判断 `indices` 是否 identity，
   分支到 flat 直读 `values[row]` 的 loop；dictionary/constant 再走 mapped loop。这样不需要按 batch
   encoding 重新 codegen，但可以让常见 flat case 少一次 indices load。
3. **消除 no-null 场景下的 per-row accumulator null clear**：sum 当前每行每 slot 都执行
   `clearAccumulatorNull`。对于 input 确认无 null 的 batch，可以考虑 batch-level 或 new-group-level 地清
   accumulator null，避免每行重复写 null bitmap。
4. **区分纯 add kernel benchmark 与完整 query benchmark**：POC 结论更接近 add kernel 层收益；生产端到端
   收益需要单独扣除 hash probe/output/copy 等公共成本。后续 benchmark 可以补一个只测 `GroupingSet` add
   的 microbenchmark，避免 `copyResults` 稀释定位。
5. **继续限制 fuse width 的甜点区间**：当前 width16/32 仍有收益，但并未出现 POC 的巨大收益。考虑先保持
   `maxFuseWidth=16` 或最多 32；更宽时需要结合 cache/TLB 数据重新评估。

## 9. JIT extract raw output descriptor 优化验证

### 9.1 优化内容

本轮继续优化第 8.6 节定位出的 extract 瓶颈：JIT extract 不再对普通 FLAT primitive 输出逐行调用
`jit_HashAggrSetFlat*` helper，而是由 `GroupingSet` 为每个 aggregate output 准备
`HashAggrJitOutput` descriptor：

1. `values`：`FlatVector<T>::mutableRawValues()`；
2. `nulls`：`BaseVector::mutableRawNulls()`；
3. `vector`：原始 `BaseVector*`，保留给 decimal / partial avg ROW 等复杂输出 helper fallback。

JIT extract IR 对 `Int8/Int16/Int32/Int64/Float/Double` 直接执行：

```text
values[row] = value
isNull ? clear null bitmap bit : set null bitmap bit
```

`Bool`、`Int128/decimal`、partial avg ROW output 暂不做 raw 写，仍通过 descriptor 中的 `vector` 走原 helper。

### 9.2 功能与性能验证命令

构建：

```bash
cmake --build --preset conan-release --target bolt_hashaggr_jit_benchmark --parallel 2
```

功能覆盖（sum/avg/min/count/decimal/double min-max）：

```bash
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=3 --bm_max_secs=2 \
  --bm_regex='^width8_(sum|avg|min|count|double_min|double_max|decimal_sum|decimal_avg)_(nojit|jit)$'
```

sum 宽度扫描：

```bash
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=20 --bm_max_secs=5 \
  --bm_regex='^width(4|8|16|32)_sum_(nojit|jit)$'
```


### 9.3 最新 width8 结果

| case | no-JIT | JIT | speedup = nojit / jit |
|------|-------:|----:|----------------------:|
| width8_sum | 4.61ms | 3.55ms | **1.30x** |
| width8_avg | 5.41ms | 4.18ms | **1.29x** |
| width8_min | 3.71ms | 3.48ms | **1.07x** |
| width8_count | 4.30ms | 2.41ms | **1.78x** |
| width8_decimal_sum | 12.13ms | 9.58ms | **1.27x** |
| width8_decimal_avg | 16.49ms | 14.77ms | **1.12x** |
| width8_double_min | 4.94ms | 4.23ms | **1.17x** |
| width8_double_max | 4.21ms | 3.81ms | **1.10x** |

对比第 7.4 节，`min` / `double_max` 已从略慢于 no-JIT 变为正收益；`sum`、`avg`、`count` 也继续提升。

### 9.4 最新 sum 宽度扫描

| case | no-JIT | JIT | speedup = nojit / jit |
|------|-------:|----:|----------------------:|
| width4_sum | 2.60ms | 2.44ms | **1.07x** |
| width8_sum | 4.65ms | 3.45ms | **1.35x** |
| width16_sum | 9.06ms | 6.06ms | **1.50x** |
| width32_sum | 17.28ms | 12.21ms | **1.42x** |

相比第 8.3 节（extract 优化前 width16_sum 约 1.19x、width32_sum 约 1.15x），raw output descriptor 后
宽聚合收益明显扩大，说明之前 extract helper/type-dispatch 确实抵消了大量 add_dense 的融合收益。


### 9.5 详细数据

```
$ ./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark 
============================================================================
[...]c/benchmarks/HashAggrJitBenchmark.cpp     relative  time/iter   iters/s
============================================================================
width4_sum_nojit                                            2.57ms    388.70
width4_sum_jit                                              2.40ms    416.48
----------------------------------------------------------------------------
width4_avg_nojit                                            3.27ms    306.05
width4_avg_jit                                              2.59ms    385.86
----------------------------------------------------------------------------
width4_min_nojit                                            2.40ms    417.30
width4_min_jit                                              2.42ms    413.22
----------------------------------------------------------------------------
width4_count_nojit                                          2.42ms    413.03
width4_count_jit                                            1.90ms    525.58
----------------------------------------------------------------------------
width4_merge_sum_nojit                                      3.60ms    277.57
width4_merge_sum_jit                                        3.29ms    303.98
----------------------------------------------------------------------------
width4_merge_avg_nojit                                      4.58ms    218.27
width4_merge_avg_jit                                        7.19ms    138.99
----------------------------------------------------------------------------
width4_merge_min_nojit                                      3.42ms    292.16
width4_merge_min_jit                                        3.33ms    300.62
----------------------------------------------------------------------------
width4_merge_count_nojit                                    3.37ms    296.86
width4_merge_count_jit                                      2.88ms    346.72
----------------------------------------------------------------------------
width8_sum_nojit                                            4.62ms    216.54
width8_sum_jit                                              3.36ms    297.77
----------------------------------------------------------------------------
width8_avg_nojit                                            5.37ms    186.22
width8_avg_jit                                              4.14ms    241.70
----------------------------------------------------------------------------
width8_min_nojit                                            3.70ms    270.01
width8_min_jit                                              3.50ms    285.84
----------------------------------------------------------------------------
width8_count_nojit                                          4.26ms    235.01
width8_count_jit                                            2.35ms    425.31
----------------------------------------------------------------------------
width8_merge_sum_nojit                                      6.18ms    161.79
width8_merge_sum_jit                                        4.60ms    217.60
----------------------------------------------------------------------------
width8_merge_avg_nojit                                      7.70ms    129.85
width8_merge_avg_jit                                       12.31ms     81.22
----------------------------------------------------------------------------
width8_merge_min_nojit                                      5.31ms    188.20
width8_merge_min_jit                                        4.83ms    206.90
----------------------------------------------------------------------------
width8_merge_count_nojit                                    5.72ms    174.70
width8_merge_count_jit                                      3.58ms    279.62
----------------------------------------------------------------------------
width16_sum_nojit                                           9.01ms    110.95
width16_sum_jit                                             5.93ms    168.53
----------------------------------------------------------------------------
width16_avg_nojit                                          10.53ms     94.95
width16_avg_jit                                             7.38ms    135.53
----------------------------------------------------------------------------
width16_min_nojit                                           7.92ms    126.22
width16_min_jit                                             6.24ms    160.20
----------------------------------------------------------------------------
width16_count_nojit                                         7.73ms    129.35
width16_count_jit                                           3.50ms    285.49
----------------------------------------------------------------------------
width16_merge_sum_nojit                                    11.44ms     87.44
width16_merge_sum_jit                                       7.58ms    131.87
----------------------------------------------------------------------------
width16_merge_avg_nojit                                    15.68ms     63.79
width16_merge_avg_jit                                      23.72ms     42.16
----------------------------------------------------------------------------
width16_merge_min_nojit                                    10.21ms     97.95
width16_merge_min_jit                                       7.94ms    125.98
----------------------------------------------------------------------------
width16_merge_count_nojit                                  10.10ms     98.97
width16_merge_count_jit                                     5.22ms    191.41
----------------------------------------------------------------------------
width32_sum_nojit                                          17.20ms     58.13
width32_sum_jit                                            12.08ms     82.76
----------------------------------------------------------------------------
width32_avg_nojit                                          19.42ms     51.48
width32_avg_jit                                            15.11ms     66.20
----------------------------------------------------------------------------
width32_min_nojit                                          15.56ms     64.26
width32_min_jit                                            12.53ms     79.78
----------------------------------------------------------------------------
width32_count_nojit                                        15.66ms     63.85
width32_count_jit                                           7.12ms    140.37
----------------------------------------------------------------------------
width32_merge_sum_nojit                                    23.30ms     42.91
width32_merge_sum_jit                                      16.24ms     61.59
----------------------------------------------------------------------------
width32_merge_avg_nojit                                    30.22ms     33.09
width32_merge_avg_jit                                      47.82ms     20.91
----------------------------------------------------------------------------
width32_merge_min_nojit                                    19.79ms     50.52
width32_merge_min_jit                                      15.78ms     63.37
----------------------------------------------------------------------------
width32_merge_count_nojit                                  19.32ms     51.75
width32_merge_count_jit                                    10.17ms     98.30
----------------------------------------------------------------------------
width8_decimal_sum_nojit                                   12.03ms     83.13
width8_decimal_sum_jit                                      9.83ms    101.71
----------------------------------------------------------------------------
width8_decimal_avg_nojit                                   16.29ms     61.38
width8_decimal_avg_jit                                     14.77ms     67.70
----------------------------------------------------------------------------
width8_double_min_nojit                                     5.05ms    197.94
width8_double_min_jit                                       4.16ms    240.16
----------------------------------------------------------------------------
width8_double_max_nojit                                     4.18ms    239.18
width8_double_max_jit                                       3.84ms    260.33
----------------------------------------------------------------------------
width8_high_card_partial_avg_extract_nojit                 61.78ms     16.19
width8_high_card_partial_avg_extract_jit                   80.29ms     12.46
----------------------------------------------------------------------------
width8_high_card_partial_sum_extract_nojit                 27.36ms     36.54
width8_high_card_partial_sum_extract_jit                   23.51ms     42.54
----------------------------------------------------------------------------
```



### 9.6 当前剩余瓶颈分析

从完整 benchmark 结果看，direct decoded input descriptor 和 raw output descriptor 已经解决了此前最明显的
两类 helper 开销：`jit_GetDecodedValue*` 输入读取 helper，以及 `jit_HashAggrSetFlat*` / `dynamic_cast`
输出写 helper。普通 FLAT primitive 聚合现在基本都已经转为正收益，但仍有几类结构性瓶颈。

#### 9.6.1 最大负收益：merge avg 的 ROW intermediate 路径

目前最明显的回退集中在 `merge_avg_jit`：

| case | no-JIT | JIT | speedup = nojit / jit |
|------|-------:|----:|----------------------:|
| width4_merge_avg | 4.58ms | 7.19ms | **0.64x** |
| width8_merge_avg | 7.70ms | 12.31ms | **0.63x** |
| width16_merge_avg | 15.68ms | 23.72ms | **0.66x** |
| width32_merge_avg | 30.22ms | 47.82ms | **0.63x** |

这个比例在不同 width 下非常稳定，说明不是 benchmark 噪声，而是路径本身还没有被优化。根因是 avg merge
的 intermediate input 是 `ROW(sum, count)`，没有完全吃到 raw decoded descriptor 优化：普通数值输入已经能
通过 `values + indices + nulls` 在 JIT IR 中直接 load，但 ROW field 读取仍然需要类似
`jit_GetDecodedRowFieldDouble` / `jit_GetDecodedRowFieldI64` / `jit_GetDecodedRowFieldIsNull` 的 helper 或
DecodedVector row-field 路径。

partial avg extract 也印证了这个结论：

| case | no-JIT | JIT | speedup = nojit / jit |
|------|-------:|----:|----------------------:|
| width8_high_card_partial_avg_extract | 61.78ms | 80.29ms | **0.77x** |
| width8_high_card_partial_sum_extract | 27.36ms | 23.51ms | **1.16x** |

partial sum 输出是 FLAT，已经受益于 raw output descriptor；partial avg 输出是 ROW，目前仍走 helper fallback，
因此仍然更慢。

**建议**：短期可考虑禁用 `merge_avg_jit` 和 `partial_avg_extract_jit`；长期需要为 ROW input/output 增加
descriptor，把 `sum` / `count` 两个 child vector 的 raw values/nulls 直接传给 JIT。

#### 9.6.2 主路径瓶颈：JIT add_dense 仍是 row-based scalar RMW loop

普通 sum/avg/count 已经有明显正收益：

| 聚合 | width4 | width8 | width16 | width32 |
|------|-------:|-------:|--------:|--------:|
| sum | 1.07x | 1.38x | 1.52x | 1.42x |
| avg | 1.26x | 1.30x | 1.43x | 1.29x |
| count | 1.27x | 1.81x | 2.21x | 2.20x |

count 收益显著高于 sum/avg，因为 count 不需要读取 input value，也不需要做加法以外的复杂状态维护。sum/avg
的剩余成本主要回到 JIT 生成码本身：

```text
group = groups[row]
index = indices[row]
value = values[index]
load accumulator
clear accumulator null bit
add / update count
store accumulator
```

这仍然是 row-based scalar read-modify-write loop：`groups[row]` 是间接指针访问，`indices[row]` 即使在 flat
input 下也要额外 load，accumulator 存在 RowContainer row storage 中而不是连续 columnar buffer，LLVM 很难
做 SIMD 化。

**建议**：优先做 flat/no-null add_dense 快路径。在 input 是 flat identity mapping 且没有 null 时，直接生成
`value = values[row]`，跳过 `indices[row]` 和 input null 分支。

#### 9.6.3 小 width 收益有限：公共固定成本占比高

width4 下收益明显弱于 width8/16/32：

| case | speedup |
|------|--------:|
| width4_sum | 1.07x |
| width4_min | 0.99x |
| width4_count | 1.27x |

width4 中可 fusion 的 aggregate 数量少，JIT 能省下的 per-aggregate dispatch / loop traversal 不多，但 descriptor
准备、JIT chunk 调用、result vector resize、output materialization、RowContainer / hash probe / copyResults 等
完整 query 公共成本仍然存在。

**建议**：默认启用策略上应更偏向 width8+ 或 count/sum 这类收益稳定的 case；低 width case 需要结合实际
query 成本谨慎启用。

#### 9.6.4 min/max 收益较小：compare 与 null-init 分支仍偏重

min/max 已经转为正收益，但弱于 sum/count：

| case | speedup |
|------|--------:|
| width8_min | 1.06x |
| width16_min | 1.27x |
| width32_min | 1.24x |
| width8_double_min | 1.21x |
| width8_double_max | 1.09x |

min/max 每行更新不仅要读取 input value，还要处理 accumulator 是否 null、首次 non-null 初始化、compare 分支；
double min/max 还可能受 NaN / ordering 语义影响。相比 sum 的简单加法，这些分支更难被 LLVM 优化。

**建议**：后续可为 no-null + accumulator initialized 场景生成更简单的 compare-only 快路径。

#### 9.6.5 decimal 仍受复杂 overflow/precision 逻辑限制

decimal 现在已经是正收益，但幅度有限：

| case | no-JIT | JIT | speedup = nojit / jit |
|------|-------:|----:|----------------------:|
| width8_decimal_sum | 12.03ms | 9.83ms | **1.22x** |
| width8_decimal_avg | 16.29ms | 14.77ms | **1.10x** |

decimal update/extract 仍包含 int128 accumulator、overflow state、precision/scale 检查、final extract overflow
处理以及 decimal avg rescale 等复杂逻辑，无法像 primitive sum 一样完全变成简单 raw load/store。

**建议**：decimal 可以继续专项优化，但优先级低于 ROW avg 路径和 flat/no-null add_dense 快路径。

#### 9.6.6 当前瓶颈优先级

1. **P0：ROW avg 路径**：`merge_avg_jit` 和 `partial_avg_extract_jit` 是目前唯一大幅负收益路径。短期禁用，
   长期做 ROW input/output descriptor。
2. **P1：flat/no-null add_dense 快路径**：减少 `indices[row]` 间接读取和 null 分支，继续提升 sum/avg/min 主路径。
3. **P2：减少 per-row accumulator null clear**：对于 no-null input 或 accumulator 已初始化场景，把 null clear 从
   per-row 下沉到更粗粒度。
4. **P3：min/max compare 快路径**：减少 accumulator null/init 分支。
5. **P4：decimal 专项优化**：拆解 overflow/precision helper，但收益优先级相对靠后。

### 9.7 perf 验证

代表性命令：

```bash
/usr/lib/linux-tools-5.15.0-160/perf record -F 2999 \
  -o /tmp/bolt-width16-sum-jit-outputdesc.perf.data -- \
  ./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=200 --bm_max_secs=8 --bm_regex='^width16_sum_jit$'

/usr/lib/linux-tools-5.15.0-160/perf report \
  -i /tmp/bolt-width16-sum-jit-outputdesc.perf.data \
  --stdio --no-children --sort symbol --percent-limit 0 \
  | grep -E 'jit_HashAggrSetFlatI64|dynamic_cast|__dynamic|__do_dyncast|HashAggrSetFlat'
```

结果：`jit_HashAggrSetFlatI64` / `HashAggrSetFlat*` 不再出现在 perf report 中；`__dynamic_cast` 降到
约 **0.27%**，`__do_dyncast` 合计约 **0.15%**。对比第 8.5 节，extract helper 与 dynamic_cast/type
dispatch 从 JIT 路径约 **13%** 的显性热点降为噪声级别。

### 9.8 更新后的结论

- direct decoded input descriptor 解决了 add_dense 的外部取值 helper；raw output descriptor 继续解决了
  extract 的 per-row setter helper / dynamic_cast。
- 当前 width8 常见 FLAT primitive 数值聚合已全部为正收益；sum 宽度扫描在 width16 达到约 **1.52x**，更接近
  最初 multi_sum POC 的方向性预期。
- 截至 raw output descriptor 阶段，最大遗留问题是 ROW intermediate/output：`merge_avg_jit` 和
  `partial_avg_extract_jit` 仍显著慢于 no-JIT；第 10 章继续更新了 ROW descriptor 优化后的最新结果。
- 主路径剩余瓶颈回到真正的 JIT add_dense 生成码、hash/vector encoding、RowContainer 和 result copy 等公共成本；
  后续若继续优化，优先考虑 flat/no-null add_dense 快路径、减少 per-row accumulator null clear，以及拆分纯
  `GroupingSet` add microbenchmark 来单独观察 kernel 收益。

## 10. ROW avg input/output descriptor 优化验证

### 10.1 优化内容

针对第 9.6.1 节的 P0 瓶颈，本轮为 avg 的 `ROW(sum, count)` intermediate input/output 增加了 raw descriptor：

1. `HashAggrJitDecodedInput` 增加 `rowField0Values/nulls`、`rowField1Values/nulls`，用于 avg merge 直接读取
   partial 输出的 `sum` / `count` child FlatVector；
2. `HashAggrJitOutput` 增加同名 row field 指针，用于 partial avg extract 直接写 `sum` / `count` child FlatVector；
3. `loadDecodedRowField` / `isDecodedRowFieldNull` 对 field 0/1 走 `GEP + load` / raw null bitmap，避免逐行
   `DecodedVector` ROW field helper；
4. `emitPartialAvgResult` 在存在 row field raw output 时直接写 child values 和 row null bitmap，保留 helper fallback
   以覆盖非预期编码。

这个优化利用了 partial avg 的数据流约束：`addIntermediateResults` 的输入来自 `extractAccumulator`，而
`extractAccumulator` 输出的 ROW child 均为 FLAT，因此 avg merge 拆 ROW 时只需要支持 child FlatVector 快路径。

关键代码位置：

- ROW input/output descriptor 字段：`bolt/jit/aggregation/HashAggrJit.h:61`
- ROW field raw load/null check：`bolt/jit/aggregation/HashAggrJit.cpp:698` / `bolt/jit/aggregation/HashAggrJit.cpp:723`
- partial avg ROW raw output 写入：`bolt/jit/aggregation/HashAggrJit.cpp:795`
- avg merge ROW child raw input 填充：`bolt/exec/GroupingSet.cpp:120`
- partial avg ROW child raw output 填充：`bolt/exec/GroupingSet.cpp:148`
- JIT extract output descriptor 准备：`bolt/exec/GroupingSet.cpp:1194`

### 10.2 验证命令

构建：

```bash
cmake --build --preset conan-release --target bolt_hashaggr_jit_benchmark --parallel 1
```

完整 benchmark：

```bash
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark --bm_min_iters=5
```

P0 专项复测：

```bash
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_regex='width(4|8|16|32)_merge_avg' --bm_min_iters=20

./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_regex='width8_high_card_partial_avg_extract' --bm_min_iters=50 --bm_max_secs=10
```

### 10.3 merge_avg 修复结果

ROW input descriptor 后，`merge_avg_jit` 从此前稳定 0.63–0.66x 的最大负收益路径，变为稳定正收益：

| case | 修复前 speedup | 修复后 no-JIT | 修复后 JIT | 修复后 speedup |
|------|---------------:|--------------:|-----------:|---------------:|
| width4_merge_avg | 0.64x | 4.63ms | 4.02ms | **1.15x** |
| width8_merge_avg | 0.63x | 7.86ms | 5.88ms | **1.34x** |
| width16_merge_avg | 0.66x | 14.83ms | 10.56ms | **1.40x** |
| width32_merge_avg | 0.63x | 31.13ms | 24.21ms | **1.29x** |

完整 benchmark 的同类结果也保持正收益：width4/8/16/32 分别约 **1.23x / 1.30x / 1.41x / 1.37x**。

### 10.4 partial_avg_extract 结果

partial avg extract 的 ROW output helper 已被 raw child 写入替换，较第 9.5 节中 80.29ms 的 JIT 路径有明显改善；
但在当前完整查询 benchmark 中，端到端仍有波动且长跑仍略慢于 no-JIT：

| case | 第 9.5 节 JIT | 修复后 no-JIT | 修复后 JIT | 修复后 speedup |
|------|--------------:|--------------:|-----------:|---------------:|
| width8_high_card_partial_avg_extract | 80.29ms | 60.17ms | 68.19ms | 0.88x |

对照同一轮 partial sum extract：

| case | no-JIT | JIT | speedup |
|------|-------:|----:|--------:|
| width8_high_card_partial_sum_extract | 27.50ms | 22.50ms | **1.22x** |

因此，本轮对 partial avg extract 的结论是：ROW output helper 瓶颈已被削弱，但该 case 的端到端性能还没有稳定转正。
剩余成本大概率不再只是 ROW child 写出，而是 high-cardinality 场景下每行新 group 初始化、avg accumulator
`sum+count` 更新、RowVector 输出物化以及完整 `copyResults` 公共成本共同导致。

### 10.5 完整 benchmark 快照

完整 benchmark 命令：

```bash
./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark --bm_min_iters=5
```

本轮完整结果的 speedup 汇总如下（speedup = no-JIT / JIT，**> 1 表示 JIT 更快**）：

| 聚合 | width4 | width8 | width16 | width32 |
|------|-------:|-------:|--------:|--------:|
| sum（single） | **1.04x** | **1.34x** | **1.48x** | **1.47x** |
| avg（single） | **1.22x** | **1.27x** | **1.45x** | **1.30x** |
| min（single） | **1.01x** | **1.07x** | **1.27x** | **1.27x** |
| count（single） | **1.28x** | **1.77x** | **2.20x** | **2.28x** |
| sum（merge） | **1.08x** | **1.34x** | **1.49x** | **1.43x** |
| avg（merge） | **1.23x** | **1.30x** | **1.41x** | **1.37x** |
| min（merge） | **1.03x** | **1.11x** | **1.28x** | **1.33x** |
| count（merge） | **1.17x** | **1.57x** | **1.86x** | **1.97x** |

其他 width8 用例：

| case | no-JIT | JIT | speedup |
|------|-------:|----:|--------:|
| width8_decimal_sum | 12.05ms | 9.60ms | **1.26x** |
| width8_decimal_avg | 16.15ms | 14.60ms | **1.11x** |
| width8_double_min | 4.95ms | 4.15ms | **1.19x** |
| width8_double_max | 4.13ms | 3.86ms | **1.07x** |
| width8_high_card_partial_avg_extract | 58.15ms | 63.36ms | 0.92x |
| width8_high_card_partial_sum_extract | 24.48ms | 21.34ms | **1.15x** |

结论：ROW avg input/output descriptor 之后，完整 benchmark 中除 `partial_avg_extract` 外，当前覆盖的主要
single / merge primitive 聚合均已转为正收益；`count` 和宽 `sum/avg/merge_avg` 收益最稳定。

### 10.6 更新后的瓶颈优先级

1. **P0 已基本解决：merge_avg ROW input**。`merge_avg_jit` 已从 0.63–0.66x 拉升到 1.15–1.40x，是本轮最主要收益。
2. **P1：partial_avg_extract 仍需继续拆解**。ROW output raw descriptor 已降低 JIT 绝对耗时，但端到端仍约 0.88x；
   下一步需要用 perf 区分 add/update、新 group 初始化、ROW output materialization 和 `copyResults` 的占比。
3. **P2：flat/no-null add_dense 快路径**。普通 sum/avg/min 主路径仍有 `indices[row]` 和 per-row null 处理成本。
4. **P3：减少 per-row accumulator null clear**。对于 no-null input 或 accumulator 已初始化场景，把 null clear 从 per-row
   下沉到更粗粒度。
5. **P4：min/max compare 和 decimal 专项优化**。收益优先级低于 partial avg extract 和 add_dense 主路径。

### 10.7 本轮结论

- avg merge 的 ROW intermediate input 已吃到 raw descriptor 优化，最大负收益 case 已转正。
- partial avg extract 的 ROW output helper 已优化，但 benchmark 仍显示端到端略慢，需要继续 perf 定位剩余成本。
- HashAggr JIT 当前更适合 sum/count/avg merge 这类宽融合场景；partial avg extract 暂不应作为默认开启 JIT 的依据。

## 11. P1：partial_avg_extract 火焰图定位

### 11.1 perf 采集与火焰图生成方法

对 `width8_high_card_partial_avg_extract` 的 JIT / no-JIT 两条路径分别采样，并生成火焰图。

采样（`-F 999 --call-graph dwarf`）：

```bash
# JIT 路径
/usr/lib/linux-tools-5.15.0-160/perf record -F 999 --call-graph dwarf \
  -o /tmp/bolt-partial-avg-extract-jit.perf.data -- \
  ./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=200 --bm_max_secs=20 \
  --bm_regex='^width8_high_card_partial_avg_extract_jit$'

# no-JIT 路径
/usr/lib/linux-tools-5.15.0-160/perf record -F 999 --call-graph dwarf \
  -o /tmp/bolt-partial-avg-extract-nojit.perf.data -- \
  ./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark \
  --bm_min_iters=200 --bm_max_secs=20 \
  --bm_regex='^width8_high_card_partial_avg_extract_nojit$'
```

折叠栈并用 FlameGraph 生成 SVG：

```bash
FG=/data00/home/liyang.127/FlameGraph

/usr/lib/linux-tools-5.15.0-160/perf script -i /tmp/bolt-partial-avg-extract-jit.perf.data \
  | $FG/stackcollapse-perf.pl > /tmp/partial-avg-extract-jit.folded
$FG/flamegraph.pl --title "width8_high_card_partial_avg_extract JIT" \
  /tmp/partial-avg-extract-jit.folded \
  > doc/hashaggr-jit-partial-avg-extract-jit-flamegraph.svg

/usr/lib/linux-tools-5.15.0-160/perf script -i /tmp/bolt-partial-avg-extract-nojit.perf.data \
  | $FG/stackcollapse-perf.pl > /tmp/partial-avg-extract-nojit.folded
$FG/flamegraph.pl --title "width8_high_card_partial_avg_extract no-JIT" \
  /tmp/partial-avg-extract-nojit.folded \
  > doc/hashaggr-jit-partial-avg-extract-nojit-flamegraph.svg
```

> 选用 `--call-graph dwarf` 而非 fp/lbr：Release 二进制开启 `-fomit-frame-pointer`，帧指针回溯会断栈；
> 该机器硬件 PMU/LBR 也不可用（只能用 software `cpu-clock`），dwarf 基于 `.eh_frame`/CFI 回溯，
> 在优化过且含 JIT 匿名段的二进制上能还原完整调用栈，适合做火焰图，代价是数据量大、采样频率需调低到 999。

产物火焰图：

- `doc/hashaggr-jit-partial-avg-extract-jit-flamegraph.svg`
- `doc/hashaggr-jit-partial-avg-extract-nojit-flamegraph.svg`

### 11.2 采样结构与噪声剥离

本次火焰图有两个需要先剥离的结构性噪声，否则会误判热点：

1. **JIT 后台编译线程**：JIT 编译发生在 `CPUThreadPool0` 上的 `llvm::orc::*` / `PassManager` /
   `SelectionDAG` 调用链，在原始火焰图里占比很大，但它属于一次性 plan 编译开销（LRU 缓存命中后不再编译），
   不计入热路径。剥离方式是过滤掉 `llvm::orc` / `*PassManager` / `SelectionDAG` / `MachineFunction` 等编译栈。
2. **benchmark 主线程**：`bolt_hashaggr_j` 主线程几乎全是 plan 解析（`Parser::parse`、`parseTypeSignature`）
   和动态链接 setup（`elf_dynamic_do_Rela`、`do_lookup_x`），是 query 构建噪声，真正的算子执行在
   `CPUThreadPool0` 执行线程上。

剥离后，对执行线程上的真实算子热点做 leaf 归类对比（self time，已排除编译栈）。

### 11.3 执行线程热点对比

| leaf 热点 | JIT | no-JIT | 说明 |
|----------|----:|-------:|------|
| `[perf-*.map]`（JIT 生成码） | 9.7% | 9.7%（no-JIT 是其它匿名段） | JIT add/extract 生成码 |
| `clear_page_erms` | 8.9% | 9.7% | 内核清零新申请页 |
| `arrayGroupProbe` | 4.8% | 1.6% | hash 探测 |
| `AverageAggregateBase::addRawInput` | 3.2% | 5.6% | avg accumulator 更新 |
| `SumAggregateBase::addRawInput` | 3.2% | 2.4% | 子聚合更新 |
| `MinAggregate::addRawInput` | 2.4% | 4.0% | 子聚合更新 |
| `__memset_avx512` / `get_page_from_freelist` / `_int_malloc` | 合计 ~6% | 合计 ~5% | 新 group 内存分配 |
| `RowContainer::initializeRow` / `HashStringAllocator::clear` | ~3% | ~3% | 新 group 初始化 |
| `RowContainer::extractColumn` | 1.6% | 1.6% | 结果列抽取 |
| `VectorHasher::makeValueIdsFlatNoNulls` | 1.6% | 1.6% | key 编码 |

关键观察：

1. **两条路径的执行线程热点几乎重合**：top 热点都是 `clear_page_erms` + 内存分配 + `arrayGroupProbe` +
   各 accumulator 的 `addRawInput`，extract 相关符号（`extractColumn` / ROW child 写出）self time 都不到 2%。
2. **extract 已经不是这个 case 的瓶颈**：第 10 章 ROW output raw descriptor 已把 extract helper 削掉，
   火焰图里 extract 已沉到噪声级别。partial avg extract 端到端略慢，**不是 extract kernel 导致的**。
3. **真正的成本是 high-cardinality 的新 group 物化**：该 case groups=batches×batch_size（每行一个新组），
   每个新 group 都要 `clear_page` + `malloc` + `RowContainer::initializeRow`，这部分是 JIT/no-JIT 共有的固定成本，
   且占执行线程相当大比例。JIT 在这部分没有任何优化空间。
4. **JIT 反而在 `arrayGroupProbe` 上采样更高（4.8% vs 1.6%）**：在「每行新组」的极端高基数下，JIT chunk 的
   group probe 调用方式相对 no-JIT 没有优势，叠加 add kernel 节省有限，导致端到端被新组物化稀释后呈现约 0.9x。

### 11.4 P1 结论

- partial_avg_extract 的 ROW output 瓶颈（第 9/10 章定位的 helper / dynamic_cast）已被 raw descriptor 解决，
  火焰图确认 extract self time 已 <2%。
- 该 case 当前端到端约 0.9x 的剩余差距**不在 JIT 可优化范围内**：主导成本是 high-cardinality「每行新 group」
  带来的 `clear_page` / 内存分配 / `RowContainer::initializeRow` / `arrayGroupProbe`，JIT 与 no-JIT 共享这部分开销，
  JIT 能优化的 add/extract kernel 占比已被压得很低。
- 因此 P1 的处理结论是：**partial_avg_extract 不再作为独立优化项继续深挖**。它代表的是「聚合计算占比极低、
  新组物化占比极高」的负向场景，应通过**白名单/启发式**避免对这类 high-cardinality partial 聚合启用 JIT，
  而不是继续优化 extract 本身。
- 真正还能换来 add kernel 收益的是 P2（flat/no-null add_dense 快路径）和 P3（下沉 per-row accumulator null clear），
  它们作用于计算占比高的 case，优先级高于继续打磨 partial_avg_extract。

附上此次优化后的benchmark report

```
$ ./_build/Release/bolt/exec/benchmarks/bolt_hashaggr_jit_benchmark  
============================================================================
[...]c/benchmarks/HashAggrJitBenchmark.cpp     relative  time/iter   iters/s
============================================================================
width4_sum_nojit                                            2.58ms    387.72
width4_sum_jit                                              2.36ms    422.95
----------------------------------------------------------------------------
width4_avg_nojit                                            3.15ms    317.76
width4_avg_jit                                              2.69ms    372.41
----------------------------------------------------------------------------
width4_min_nojit                                            2.46ms    407.31
width4_min_jit                                              2.47ms    405.55
----------------------------------------------------------------------------
width4_count_nojit                                          2.40ms    416.38
width4_count_jit                                            1.91ms    524.51
----------------------------------------------------------------------------
width4_merge_sum_nojit                                      3.70ms    270.06
width4_merge_sum_jit                                        3.31ms    302.23
----------------------------------------------------------------------------
width4_merge_avg_nojit                                      4.53ms    220.94
width4_merge_avg_jit                                        3.67ms    272.67
----------------------------------------------------------------------------
width4_merge_min_nojit                                      3.54ms    282.70
width4_merge_min_jit                                        3.37ms    296.70
----------------------------------------------------------------------------
width4_merge_count_nojit                                    3.43ms    291.51
width4_merge_count_jit                                      2.87ms    348.60
----------------------------------------------------------------------------
width8_sum_nojit                                            4.60ms    217.19
width8_sum_jit                                              3.41ms    293.12
----------------------------------------------------------------------------
width8_avg_nojit                                            5.27ms    189.92
width8_avg_jit                                              4.10ms    244.09
----------------------------------------------------------------------------
width8_min_nojit                                            3.76ms    266.31
width8_min_jit                                              3.53ms    283.64
----------------------------------------------------------------------------
width8_count_nojit                                          4.31ms    231.77
width8_count_jit                                            2.37ms    422.36
----------------------------------------------------------------------------
width8_merge_sum_nojit                                      6.17ms    162.07
width8_merge_sum_jit                                        4.78ms    209.35
----------------------------------------------------------------------------
width8_merge_avg_nojit                                      7.26ms    137.75
width8_merge_avg_jit                                        5.67ms    176.25
----------------------------------------------------------------------------
width8_merge_min_nojit                                      5.26ms    189.99
width8_merge_min_jit                                        4.82ms    207.44
----------------------------------------------------------------------------
width8_merge_count_nojit                                    5.73ms    174.57
width8_merge_count_jit                                      3.55ms    281.72
----------------------------------------------------------------------------
width16_sum_nojit                                           8.90ms    112.38
width16_sum_jit                                             5.97ms    167.52
----------------------------------------------------------------------------
width16_avg_nojit                                          10.59ms     94.47
width16_avg_jit                                             7.29ms    137.08
----------------------------------------------------------------------------
width16_min_nojit                                           7.66ms    130.62
width16_min_jit                                             6.34ms    157.71
----------------------------------------------------------------------------
width16_count_nojit                                         7.70ms    129.92
width16_count_jit                                           3.51ms    284.55
----------------------------------------------------------------------------
width16_merge_sum_nojit                                    11.17ms     89.55
width16_merge_sum_jit                                       7.29ms    137.12
----------------------------------------------------------------------------
width16_merge_avg_nojit                                    14.74ms     67.86
width16_merge_avg_jit                                      10.45ms     95.71
----------------------------------------------------------------------------
width16_merge_min_nojit                                    10.53ms     94.92
width16_merge_min_jit                                       8.39ms    119.26
----------------------------------------------------------------------------
width16_merge_count_nojit                                  10.02ms     99.78
width16_merge_count_jit                                     5.30ms    188.84
----------------------------------------------------------------------------
width32_sum_nojit                                          17.49ms     57.17
width32_sum_jit                                            12.44ms     80.38
----------------------------------------------------------------------------
width32_avg_nojit                                          20.01ms     49.97
width32_avg_jit                                            15.68ms     63.77
----------------------------------------------------------------------------
width32_min_nojit                                          15.48ms     64.62
width32_min_jit                                            12.96ms     77.16
----------------------------------------------------------------------------
width32_count_nojit                                        16.00ms     62.52
width32_count_jit                                           7.39ms    135.35
----------------------------------------------------------------------------
width32_merge_sum_nojit                                    22.50ms     44.45
width32_merge_sum_jit                                      17.12ms     58.41
----------------------------------------------------------------------------
width32_merge_avg_nojit                                    27.85ms     35.90
width32_merge_avg_jit                                      21.32ms     46.90
----------------------------------------------------------------------------
width32_merge_min_nojit                                    20.59ms     48.56
width32_merge_min_jit                                      17.22ms     58.09
----------------------------------------------------------------------------
width32_merge_count_nojit                                  19.84ms     50.39
width32_merge_count_jit                                    11.76ms     85.03
----------------------------------------------------------------------------
width8_decimal_sum_nojit                                   11.94ms     83.76
width8_decimal_sum_jit                                      9.86ms    101.41
----------------------------------------------------------------------------
width8_decimal_avg_nojit                                   16.31ms     61.30
width8_decimal_avg_jit                                     14.75ms     67.81
----------------------------------------------------------------------------
width8_double_min_nojit                                     4.97ms    201.05
width8_double_min_jit                                       4.17ms    239.76
----------------------------------------------------------------------------
width8_double_max_nojit                                     4.29ms    233.10
width8_double_max_jit                                       3.95ms    253.45
----------------------------------------------------------------------------
width8_high_card_partial_avg_extract_nojit                 62.12ms     16.10
width8_high_card_partial_avg_extract_jit                   68.54ms     14.59
----------------------------------------------------------------------------
width8_high_card_partial_sum_extract_nojit                 25.27ms     39.58
width8_high_card_partial_sum_extract_jit                   23.10ms     43.29
----------------------------------------------------------------------------
```

## 12. P2：flat/identity add_dense 快路径验证（已验证无收益，回退）

### 12.1 优化假设

第 9.6.2 / 第 11 章曾把 add_dense 主路径的 `indices[row]` 间接寻址列为 P2 优化点：
flat（identity mapping）输入时 `indices[row] == row`，`loadDecodedValue`
（`bolt/jit/aggregation/HashAggrJit.cpp`）每行先 `index = indices[row]` 再
`values[index]`，这一级 load 在 flat 下被认为是可省的冗余，预期省掉后能让取值-累加
循环更利于向量化。

### 12.2 实现与验证

按方案 A 实现：

1. `HashAggrJitDecodedInput` 增 `identityMapping` 标记字段；
2. `GroupingSet` 在准备 descriptor 时用 `DecodedVector::isIdentityMapping()` 填标记；
3. `loadDecodedValue` 在 IR 里据标记选择直接用 `row` 还是 `indices[row]`。

验证：

- **功能**：编译通过；dump add_dense IR 确认 identity 分支正确生成、descriptor
  trailing bool 的 offset 读取正确（`align 1`）。spark aggregate JIT 单测
  （`bolt_functions_spark_aggregates_test`，`--gtest_filter='*hashAggrJit*'`）
  3 passed / 2 failed，其中 2 个失败（`hashAggrJitMergeAndExtract`、
  `hashAggrJitAllNullGroup`）经 `git stash` 对比确认是**基线既有 bug，与 P2 无关**，
  P2 未引入新回归。
- **性能**：分别构建 baseline / P2 两个 benchmark binary，交替多轮对比
  `width8/16/32` 的 sum/avg/min jit 耗时。

### 12.3 实测结果

| 实现方式 | 相对基线 | 说明 |
|----------|----------|------|
| select 版（IR 内 `select` 选 index） | **慢约 3–6%** | `select` 仍无条件 load `indices[row]`，额外多算 flag load + select，净增指令 |
| branch 版（控制流跳过 `indices[row]` load） | **基本持平** | 多轮差异均在 ±1–2% 噪声内，无可测收益，且增加 IR 复杂度 |

以 `width16_sum_jit` 三轮交替为例（branch 版）：base 6.48 / 6.24 / 6.48ms，
P2 6.20 / 6.31 / 6.25ms——互有高低，落在噪声范围内。

### 12.4 结论

- **P2 在当前硬件 / 工作负载上没有可测收益，改动已全部回退到基线。**
- 根因：第 11 章把 `indices[row]` 当瓶颈的假设在实测中不成立。flat 输入下
  `indices` 是连续数组的顺序读，**硬件预取使其几乎零成本**，省掉它换不来收益；
  select 版反而因多余指令小幅变慢。
- 按「只做直接必要、不过度工程」的原则，无收益且增加复杂度的改动不保留。
- 后续若再优化 add_dense 主路径，方向应转向真正的访存瓶颈（如 accumulator 在
  RowContainer 中的非连续布局），而非已被预取覆盖的 `indices[row]` 间接寻址。
- P3（下沉 per-row accumulator null clear）的待确认正确性约束（新组创建与首次更新
  是否同 batch）经评估不成立、争议较大，暂缓，不在本轮实施。

---

## 13. Decimal sum/avg add/merge 纯 IR 化

### 13.1 背景

decimal sum/avg 的 add/merge 主路径此前不是真正的 inline IR：每行通过
`CreateCall(jit_HashAggrUpdate/MergeDecimal*)` 把 i128 加法 + 溢出检测转交 C++
runtime helper（`jitHashAggrAddWithOverflow`）。即 IR 只完成 decode + 路由，真正
算子在跨函数调用里执行——付出了 LLVM 的代价却没拿到 inline 红利。

### 13.2 改动

- 新增 `HashAggrJitCodegen::emitDecimalAddWithOverflow`：纯 IR 实现 i128
  `CreateAdd` + 溢出检测（`(a>0&&b>0&&r<0)||(a<0&&b<0&&r>=0)`，≤8 条 IR），
  溢出计数用 `posOverflow - negOverflow` 累加。
- `DecimalSumOps` / `DecimalAvgOps` 的 init/add-raw/add-merge 全部改为纯 IR：
  - init：直接 store sum/overflow/(count|isEmpty)，替代 `jit_HashAggrInitDecimal*`。
  - add/merge：`emitDecimalAddWithOverflow` + IR 内 `++count` / `isEmpty &&=`。
  - state 字段访问用 `offsetof(JitDecimal*State, field)` 派生 offset，避免硬编码。
- 删除不再被调用的 `jit_HashAggrInit/Update/MergeDecimal*` runtime helper 及其
  builtin 声明、`jitHashAggrAddWithOverflow`。
- per-row 的跨函数调用从 N 次降为 0（add 主路径全部内联到循环体）。

### 13.3 性能（width8，bm_min_iters=50）

| case | 改前 jit | 改后 jit | nojit（参考） | 改善 |
|------|----------|----------|----------------|------|
| width8_decimal_sum | 9.86ms | **9.01ms** | 11.79ms | ~9% |
| width8_decimal_avg | 14.75ms | **13.88ms** | 16.72ms | ~6% |

- nojit 基线基本不变，说明提升来自 JIT 侧 add/merge 内联，而非环境波动。
- 多轮测量 decimal_sum_jit 稳定在 8.1–9.0ms 区间（取决于机器负载），均优于改前。
- 收益幅度小于「翻倍」的乐观预期：i128 算术本身有成本，且热循环还有 group
  寻址 / null 处理开销，per-row call 的消除只压缩了其中一部分。

### 13.4 正确性

- decimal 专项单测全部通过：`decimalSum` / `decimalGlobalSumOverflow` /
  `decimalGroupBySumOverflow` / `decimalLargeCountRowsOverflow` /
  `decimalSomeGroupsAllnullValues`（覆盖溢出、全 null 组等关键路径）。
- extract 的 decimal 计算（依赖 `DecimalUtil` 精度判定、每组一次、非热路径）
  保留 runtime helper，不在本次范围。

---

## 14. partial avg extract 去掉运行时 fast/helper 分支

### 14.1 背景

`emitPartialAvgResult` 此前在 IR 里有 `hasRawRowOutput ? fast : helper` 的运行时
分支（3 个 BasicBlock + 1 条件跳转）：当 partial avg 输出 ROW 的 sum/count 子字段
为 FLAT 时走直写 fast 路径，否则回退 `jit_HashAggrSetPartialAvgDouble` helper。
但该分支判定的是**循环不变量**（`rowField0Values` 在整个 extract 调用内不变）。

### 14.2 改动

- 把 fast/helper 的选择从「运行时」前移到「extract 准入」：
  `fillHashAggrJitPartialAvgOutput` 改为返回 bool，当 ROW 子字段非 FLAT
  （dictionary/constant 包装）时返回 false；`runHashAggrJitExtractChunks` 据此
  令 `canRunChunk=false`、回退非 JIT 并打 VLOG（`skipReason="partial avg row
  fields are not flat"`）。
- 这样保证进入 JIT 的 chunk 其 rowField0/1 必被填充，IR 里直接走纯 fast 路径。
- `emitPartialAvgResult` 删除运行时分支与 3 个 BasicBlock；删除不再被调用的
  `jit_HashAggrSetPartialAvgDouble` runtime helper、builtin 声明及其
  `ComplexVector.h` include。

### 14.3 性能（bm_min_iters=50，基线=分支版，优化=纯 fast）

| case | 基线 | 优化后(2 轮) | 变化 |
|------|------|--------------|------|
| width8_avg_jit | 4.26ms | 4.15 / 4.21ms | ~持平–3% |
| width16_avg_jit | 8.75ms | 7.55 / 7.58ms | ~14% |
| width8_merge_avg_jit | 6.22ms | 5.75 / 6.17ms | 波动，约 0–8% |
| width16_merge_avg_jit | 11.44ms | 10.85 / 10.70ms | ~5–6% |
| width8_high_card_partial_avg_extract_jit | 74.13ms | 70.00 / 68.84ms | ~6–7% |

- 整体小幅改善或持平，无回归。改善幅度有限且部分用例有运行间波动——符合预期：
  被删的分支是循环不变量，LLVM LICM + 分支预测本就覆盖了大部分开销，去掉它主要
  减少了 codegen 出的 BasicBlock 数与少量恒命中的比较/跳转。
- 价值更多在**正确性与可维护性**：把「子字段非 FLAT」从 IR 兜底分支收敛为 plan
  阶段的显式准入回退，IR 不再生成永远走同一侧的运行时分叉。


### 14.4 正确性

- partial avg / average 相关单测通过：`hashAggrJitPartialAvgExtractAccumulators`
  （直接覆盖本次 fast 路径）、`avgDecimal` / `avgAllNulls` /
  `rowBasedSpillDecimalAvg` / `hashAggrJitDecimalSumAndFloatingMinMax` /
  `hashAggrJitSplitsContiguousSegments`。
- 无新增回归（`hashAggrJitMergeAndExtract` / `hashAggrJitAllNullGroup` 仍 FAIL，
  系既有 P0 bug，见 todolist，与本次无关）。

### 14.5 当前性能


```
============================================================================
[...]c/benchmarks/HashAggrJitBenchmark.cpp     relative  time/iter   iters/s
============================================================================
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
width8_sum_nojit                                            4.75ms    210.73
width8_sum_jit                                              3.49ms    286.89
----------------------------------------------------------------------------
width8_avg_nojit                                            5.36ms    186.61
width8_avg_jit                                              4.17ms    239.53
----------------------------------------------------------------------------
width8_min_nojit                                            3.75ms    266.36
width8_min_jit                                              3.47ms    288.20
----------------------------------------------------------------------------
width8_count_nojit                                          4.39ms    227.95
width8_count_jit                                            2.34ms    427.14
----------------------------------------------------------------------------
width8_merge_sum_nojit                                      6.22ms    160.77
width8_merge_sum_jit                                        4.75ms    210.69
----------------------------------------------------------------------------
width8_merge_avg_nojit                                      7.59ms    131.72
width8_merge_avg_jit                                        5.84ms    171.16
----------------------------------------------------------------------------
width8_merge_min_nojit                                      5.64ms    177.34
width8_merge_min_jit                                        4.89ms    204.57
----------------------------------------------------------------------------
width8_merge_count_nojit                                    6.08ms    164.52
width8_merge_count_jit                                      3.68ms    271.96
----------------------------------------------------------------------------
width16_sum_nojit                                           8.94ms    111.84
width16_sum_jit                                             6.03ms    165.78
----------------------------------------------------------------------------
width16_avg_nojit                                          10.71ms     93.37
width16_avg_jit                                             7.39ms    135.30
----------------------------------------------------------------------------
width16_min_nojit                                           7.62ms    131.30
width16_min_jit                                             6.22ms    160.80
----------------------------------------------------------------------------
width16_count_nojit                                         7.87ms    127.05
width16_count_jit                                           3.62ms    275.88
----------------------------------------------------------------------------
width16_merge_sum_nojit                                    11.47ms     87.15
width16_merge_sum_jit                                       7.79ms    128.42
----------------------------------------------------------------------------
width16_merge_avg_nojit                                    14.40ms     69.45
width16_merge_avg_jit                                      10.53ms     94.95
----------------------------------------------------------------------------
width16_merge_min_nojit                                    10.14ms     98.61
width16_merge_min_jit                                       7.73ms    129.41
----------------------------------------------------------------------------
width16_merge_count_nojit                                   9.62ms    103.94
width16_merge_count_jit                                     5.16ms    193.66
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
width8_high_card_sum_nojit                                 40.01ms     25.00
width8_high_card_sum_jit                                   31.61ms     31.63
----------------------------------------------------------------------------
width8_high_card_avg_nojit                                 46.37ms     21.56
width8_high_card_avg_jit                                   38.18ms     26.19
----------------------------------------------------------------------------
width8_high_card_min_nojit                                 37.29ms     26.82
width8_high_card_min_jit                                   30.62ms     32.66
----------------------------------------------------------------------------
width8_high_card_count_nojit                               34.43ms     29.04
width8_high_card_count_jit                                 30.06ms     33.27
----------------------------------------------------------------------------
width8_high_card_merge_sum_nojit                           61.33ms     16.31
width8_high_card_merge_sum_jit                             52.05ms     19.21
----------------------------------------------------------------------------
width8_high_card_merge_avg_nojit                           94.24ms     10.61
width8_high_card_merge_avg_jit                             78.64ms     12.72
----------------------------------------------------------------------------
width8_high_card_merge_min_nojit                           62.70ms     15.95
width8_high_card_merge_min_jit                             51.41ms     19.45
----------------------------------------------------------------------------
width8_high_card_merge_count_nojit                         57.81ms     17.30
width8_high_card_merge_count_jit                           53.43ms     18.72
----------------------------------------------------------------------------
width16_high_card_sum_nojit                                70.22ms     14.24
width16_high_card_sum_jit                                  55.12ms     18.14
----------------------------------------------------------------------------
width16_high_card_avg_nojit                                84.58ms     11.82
width16_high_card_avg_jit                                  66.30ms     15.08
----------------------------------------------------------------------------
width16_high_card_min_nojit                                67.12ms     14.90
width16_high_card_min_jit                                  54.09ms     18.49
----------------------------------------------------------------------------
width16_high_card_count_nojit                              59.38ms     16.84
width16_high_card_count_jit                                47.88ms     20.89
----------------------------------------------------------------------------
width16_high_card_merge_sum_nojit                         113.95ms      8.78
width16_high_card_merge_sum_jit                            93.46ms     10.70
----------------------------------------------------------------------------
width16_high_card_merge_avg_nojit                         157.39ms      6.35
width16_high_card_merge_avg_jit                           137.35ms      7.28
----------------------------------------------------------------------------
width16_high_card_merge_min_nojit                         110.10ms      9.08
width16_high_card_merge_min_jit                            91.93ms     10.88
----------------------------------------------------------------------------
width16_high_card_merge_count_nojit                       103.19ms      9.69
width16_high_card_merge_count_jit                          88.89ms     11.25
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
width8_decimal_sum_nojit                                   12.15ms     82.32
width8_decimal_sum_jit                                      8.16ms    122.56
----------------------------------------------------------------------------
width8_decimal_avg_nojit                                   16.20ms     61.74
width8_decimal_avg_jit                                      9.55ms    104.73
----------------------------------------------------------------------------
width16_decimal_sum_nojit                                  23.55ms     42.46
width16_decimal_sum_jit                                    16.41ms     60.93
----------------------------------------------------------------------------
width16_decimal_avg_nojit                                  32.43ms     30.83
width16_decimal_avg_jit                                    19.10ms     52.36
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
width8_double_min_nojit                                     4.97ms    201.41
width8_double_min_jit                                       4.19ms    238.53
----------------------------------------------------------------------------
width8_double_max_nojit                                     4.22ms    236.76
width8_double_max_jit                                       3.89ms    257.11
----------------------------------------------------------------------------
width8_merge_double_min_nojit                               6.96ms    143.68
width8_merge_double_min_jit                                 5.71ms    175.20
----------------------------------------------------------------------------
width8_merge_double_max_nojit                               6.01ms    166.45
width8_merge_double_max_jit                                 5.10ms    195.97
----------------------------------------------------------------------------
width16_double_min_nojit                                    9.93ms    100.71
width16_double_min_jit                                      7.81ms    128.06
----------------------------------------------------------------------------
width16_double_max_nojit                                    8.75ms    114.27
width16_double_max_jit                                      7.18ms    139.30
----------------------------------------------------------------------------
width16_merge_double_min_nojit                             12.39ms     80.74
width16_merge_double_min_jit                                9.53ms    104.88
----------------------------------------------------------------------------
width16_merge_double_max_nojit                             10.93ms     91.50
width16_merge_double_max_jit                                9.04ms    110.68
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
width8_high_card_partial_avg_nojit                         56.15ms     17.81
width8_high_card_partial_avg_jit                           61.69ms     16.21
----------------------------------------------------------------------------
width8_high_card_partial_sum_nojit                         25.49ms     39.23
width8_high_card_partial_sum_jit                           22.00ms     45.46
----------------------------------------------------------------------------
width16_high_card_partial_avg_nojit                        99.27ms     10.07
width16_high_card_partial_avg_jit                         114.96ms      8.70
----------------------------------------------------------------------------
width16_high_card_partial_sum_nojit                        48.90ms     20.45
width16_high_card_partial_sum_jit                          41.31ms     24.21
----------------------------------------------------------------------------
----------------------------------------------------------------------------
----------------------------------------------------------------------------
```

## 15. decimal_avg final extract JIT 补齐说明

### 15.1 背景

在本轮修复前，decimal avg 只有 partial extract 走 JIT helper，final extract 仍留在 non-JIT 路径：

- planner/codegen 侧通过 `canCompileDecimalAvgExtract(..., partialOutput)` 仅允许 partial path；
- runtime 侧 `jit_HashAggrExtractFinalDecimalAvg` 是空 stub，仅用于 link 成功；
- 因此历史上部分 `decimal_avg` benchmark 结果，实际测到的是“JIT add/merge + non-JIT final extract”的混合路径。

这也是第 4 章里“decimal_avg 曾优于 decimal_sum”的一个背景因素：当时 decimal avg 没有承担 final decimal
rescale/divide 的 JIT extract 成本。

### 15.2 本轮实现

本轮已补齐 final decimal avg extract 的 JIT 支持，策略是**继续保持 helper 模式**，不把 Spark decimal avg 的
divide / overflow / precision-rescale 逻辑直接展开成 LLVM IR。

具体改动：

1. **放开 codegen**：`decimal avg` 的 extract 现在 partial / final 都允许编译；
2. **扩展 helper ABI**：avg extract helper 额外接收最终结果 decimal 的 `resultPrecision/resultScale`；
3. **实现 final runtime helper**：在 runtime 中镜像 non-JIT `computeAvg` 语义：
   - `adjustSumForOverflow`
   - `divideWithRoundUp`
   - `rescaleWithRoundUp`
   - short / long decimal 分类型写回 `FlatVector`
4. **benchmark 口径更新**：`HashAggrJitBenchmark` 中 `decimal_sum/decimal_avg` 统一按 `PartialFinal` 路径评测，
   避免继续把 decimal avg 记成“只测 partial + 非 JIT final extract”的旧口径。

### 15.3 功能验证

本轮未新增一组完整 benchmark 数据表，但已完成功能与构建验证：

- 构建通过：`bolt_thrustjit`、`bolt_exec`、`bolt_functions_spark_aggregates_test`
- Average 相关测试通过：
  - `AverageAggregationTest.avgAllNulls`
  - `AverageAggregationTest.avgDecimal`
  - `AverageAggregationTest.avgDecimalWithMultipleRowVectors`
  - `AverageAggregationTest.rowBasedSpillDecimalAvg`

说明 final decimal avg extract JIT 至少已经满足当前 Spark avg 语义下的基础正确性要求：

- `count == 0` 输出 null；
- sum overflow 无法修正时输出 null；
- divide / rescale overflow 时输出 null；
- short decimal 与 long decimal 结果类型都可写回。

### 15.4 对阅读本报告的影响

1. **第 2/3/4 章中的早期 decimal_avg 结论需要加注理解**：这些历史结论产生时，final decimal avg extract 还未走 JIT。
2. **后续若继续比较 decimal_avg 的 JIT/no-JIT 收益，应以当前 `PartialFinal` benchmark 口径为准**。
3. **当前 decimal_avg benchmark 的收益解释更完整**：它现在同时覆盖 JIT add、JIT merge 和 JIT final extract，
   比之前更接近真实生产路径。

换句话说：从这一节之后，文档里关于 decimal avg 的性能讨论应默认理解为“**final extract JIT 已补齐**”的版本；
如果引用更早的数据，需要显式说明那是旧口径历史快照。
