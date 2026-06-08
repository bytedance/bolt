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
