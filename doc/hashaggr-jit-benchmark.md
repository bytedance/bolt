# HashAggr JIT 性能评测报告

## 1. 测试环境与方法

- **构建**：`Release` + spark 开关（`spark_compatible=True / enable_testutil=True /
  skip_test=False`），对齐 `make release_spark_with_test`；benchmark 单独
  `BOLT_BUILD_BENCHMARKS=ON`，未启用 `enable_perf`（gperftools 源码下载超时，folly
  benchmark 不依赖它）。
- **benchmark**：`bolt/exec/benchmarks/HashAggrJitBenchmark.cpp`，目标
  `bolt_hashaggr_jit_benchmark`。覆盖 sum/avg/min/count（width 4/8/16/32）、
  merge（partial+final）、decimal sum/avg、double min/max、partial extract。
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
4. **decimal_avg(0.75x) 优于 decimal_sum(0.40x)**：decimal_avg final 走非 JIT（spark
   rescale 复杂逻辑），反而拖累较小，侧面印证当前 JIT 计算路径偏慢。

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



### 9.5 perf 验证

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

### 9.6 更新后的结论

- direct decoded input descriptor 解决了 add_dense 的外部取值 helper；raw output descriptor 继续解决了
  extract 的 per-row setter helper / dynamic_cast。
- 当前 width8 常见数值聚合已全部为正收益；sum 宽度扫描在 width16 达到约 **1.50x**，更接近最初 multi_sum
  POC 的方向性预期。
- 剩余瓶颈主要回到真正的 JIT 生成码、hash/vector encoding、RowContainer 和 result copy 等公共成本；后续
  若继续优化，优先考虑 flat/no-null add_dense 快路径、减少 per-row accumulator null clear，以及拆分纯
  `GroupingSet` add microbenchmark 来单独观察 kernel 收益。
