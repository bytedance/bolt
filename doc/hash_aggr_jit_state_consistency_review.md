# Bolt HashAggrJit — JIT vs 非 JIT 中间状态一致性审计

> 审计分支：`origin/hash_aggr_jit`（原审计 HEAD: `2b6de6e186 remove useless code`）
> 比对范围：`bolt/jit/aggregation/`、`bolt/exec/GroupingSet.{cpp,h}`、`bolt/functions/{lib,sparksql,prestosql}/aggregates/`
> 审计目标：所有支持 JIT 的聚合算子，在所有支持的输入参数类型下，JIT 与非 JIT 的中间状态字节布局与运行时语义是否完全等价。

> **【复核更新 @ 当前工作区】** 本文档原始审计基于 `2b6de6e186`，下文已就地标注每条结论在当前工作区的状态：
> - **B3 已解决（更优方式）**：decimal/avg 的 JIT state 现为 `using` 别名直接指向非 JIT 继承的同一 POD 布局基类（`DecimalAccumulatorLayout.h` / `SumCount.h`），布局已是**同一类型**而非镜像，cross-assert 已多余。
> - **B6 已过时**：Int128/Bool extract 已支持、`canCompileMinMaxExtract` 及整个 `CanExtractFn` 已删除。
> - **B1/B2 已解决**：decimal sum/avg extract 已在 codegen 期按实际输出精度选择 short/long 专用 runtime helper，runtime 内不再保留 `longDecimal` 分支；`precision/scale` 与 `auxPrecision/auxScale` 已统一为 intermediate/final 语义。
> - **B4 已解决（最小闭环）**：decimal sum/avg merge partial row 的 sum 字段读取 kind 已改为按 `precision` 推导，不再复用原始输入列 `inputKind`。
> - **B5/B7 仍有效**：仅为防回归测试/断言加固类，P3。

---

## 结论一句话
累加器**字节布局**已通过单一权威 POD 布局对齐；`decimal_sum` / `decimal_avg` 的短/长 decimal extract 与 partial merge 位宽判断也已修复。当前剩余项主要是 B5/B7 这类防回归测试或断言加固，不再有已知 P0/P1 的 JIT/非 JIT 状态不一致问题。

---

## A. 字节级一致性表

| 算子 | JIT 结构 | 非 JIT 结构 | 布局 | 等价？ |
|---|---|---|---|---|
| `avg` | `AvgAccumulatorLayout{double sum; i64 count}` `AvgOps.cpp:16` | `SumCount<double>` `AverageAggregateBase.h:81` | 16B | ✅ **【已修】** JIT 端 `using AvgAccumulatorLayout = SumCount<double>`，同一类型 |
| `count(*/col)` | 单 `i64` `CountOps.cpp:18` | `sizeof(i64)` `CountAggregate.cpp:49` | 8B | ✅ |
| `sum` (int/float) | `TAccumulator` `SumOps.cpp:14` | `TAccumulator` `SumAggregateBase.h:78` | 同 | ✅ |
| `min/max` (非 i128) | `T` `MinMaxOps.cpp:14` | `T` `MinMaxAggregates.cpp:93` | 同 | ✅ |
| `decimal sum` | `JitDecimalSumState{i128 sum; i64 overflow; bool isEmpty}` `HashAggrJitDecimalState.h:16` | `DecimalSum{i128 sum; i64 overflow; bool isEmpty}` `DecimalSumAggregate.h:37` | 32B | ✅ **【已修】** JIT `using` 别名指向 `DecimalSumAccumulatorLayout`，`DecimalSum` 继承之，单一权威布局 |
| `decimal avg` | `JitDecimalAvgState{i128 sum; i64 count; i64 overflow}` `HashAggrJitDecimalState.h:22` | `LongDecimalWithOverflowState` 字段同 `DecimalAggregate.h:45` | 32B | ✅ **【已修】** 同上，`using` → `LongDecimalWithOverflowLayout`，继承复用 |

---

## B. 真实差异（按严重度）

### ✅ B1. `decimal_sum` partial/final extract 硬编码 `int128` 输出（已解决）

> **【复核 @ 当前工作区：已解决】** `emitDecimalSumExtract` 已在 codegen 期按实际输出 decimal 精度选择专用 helper：partial 看 `precision`，final 看 `auxPrecision`。短 decimal 调 `jit_HashAggrExtract*ShortDecimalSum` 写 `FlatVector<int64_t>`，长 decimal 调 `jit_HashAggrExtract*LongDecimalSum` 写 `FlatVector<int128_t>`；decimal sum 的 descriptor 中 `auxPrecision/auxScale` 与 `precision/scale` 镜像为同一个 sum type。

历史旧代码（已删除）曾在 `HashAggrDecimalRuntime.cpp` 中硬编码：
```cpp
vector->as<FlatVector<int128_t>>()                       // final: 直接吃 raw vector，不看 longDecimal 参数
rowVector->childAt(0)->asFlatVector<int128_t>()          // partial 同上
```

- 调用方 `emitDecimalSumExtract` 不再传 `longDecimal`；short/long 选择已下沉为不同 runtime symbol。
- `canCompileDecimalSumExtract` (`DecimalSumOps.cpp:118`) 无条件 `return true`，没有任何回退门。
- Spark 注册签名 `r_precision = min(38, a_precision + 10)`（`SumAggregate.cpp:117`），factory 在 `sumType->isShortDecimal()` 时显式构造 `DecimalSumAggregate<int64_t, int64_t>`（`SumAggregate.cpp:158`）。
- 结果：`sum(DECIMAL(5,2))` / `sum(DECIMAL(8,3))` 的结果列就是 `FlatVector<int64_t>`，JIT 那里 `dynamic_cast<FlatVector<int128_t>>` 得到 `nullptr` → 空指针 set/setNull → **段错误 / 堆破坏**。
- `GroupingSet.cpp:1304-1308` 的注释已显式承认 *"decimal avg's accumulatorKind is Int128 while its final result is a short decimal (FlatVector<int64_t>)"*——但那只保护 scalar output 自动按 vector 真实 type 推 kind 的路径；走 runtime helper 自己再 cast 一次的路径完全没保护到。

### ✅ B2. `decimal_avg` partial extract 同病（已解决）

> **【复核 @ 当前工作区：已解决】** `emitDecimalAvgExtract` 已在 codegen 期选择 short/long 专用 runtime helper：partial 看中间 sum 精度 `precision`，final 看结果精度 `auxPrecision`。`jit_HashAggrExtractPartial*DecimalAvg` 与 final 路径一样，分别写短/长 decimal sum child。

历史问题：`HashAggrDecimalRuntime.cpp` 曾硬写 `asFlatVector<int128_t>`。Spark AVG 第二条签名 `ROW(DECIMAL(a_precision, a_scale), BIGINT)` 会沿用入参精度——短 decimal 时 partial 输出是 `int64` sum vector，旧实现会 crash。当前已通过 short/long 专用 helper 消除该风险，且 runtime 内无无效分支。

### 🟡 B3. JIT/非 JIT 结构没有跨层 `static_assert`（Major）

> **【复核 @ `b4b99b5553`：已解决（更优方式），无需再加 static_assert】** 现已抽出零依赖 POD 布局基类（`DecimalAccumulatorLayout.h` 的 `DecimalSumAccumulatorLayout`/`LongDecimalWithOverflowLayout`、`SumCount.h` 的 `SumCount`）：非 JIT 结构 `DecimalSum`/`LongDecimalWithOverflowState` **继承**之，JIT 端 `JitDecimalSumState`/`JitDecimalAvgState`/`AvgAccumulatorLayout` 用 `using` **别名同一基类**。两侧已是**同一类型**而非镜像副本，sizeof/offsetof 必然相等，cross-assert 已多余；各处保留 `static_assert(is_standard_layout_v)` 防止派生类误加数据成员破坏布局。

`HashAggrJitDecimalState.h:28-29` 只断言了 `is_standard_layout`，缺：
```cpp
static_assert(sizeof(JitDecimalSumState) == sizeof(sparksql::DecimalSum));
static_assert(offsetof(JitDecimalSumState, sum) == offsetof(sparksql::DecimalSum, sum));
// overflow / isEmpty 同理；
// JitDecimalAvgState vs LongDecimalWithOverflowState 同理；
// AvgAccumulatorLayout vs SumCount<double> 同理。
```
ABI 完全靠手工同步，加 4 行最便宜也最实在。

### ✅ B4. row 输入 stride 仍按 plan 端 `slot.desc.inputKind`（已解决）

> **【复核 @ 当前工作区：已解决】** 对 decimal sum/avg 的 partial merge，row field 0 是中间 sum decimal，真实位宽由中间精度 `precision` 决定，而不是原始输入列 `slot.desc.inputKind`。当前 `DecimalSumOps.cpp` / `DecimalAvgOps.cpp` 均通过 `decimalKindForPrecision(slot.desc.precision)` 读取 row field 并 cast 到 accumulator `Int128`。

历史问题：`DecimalSumOps.cpp` / `DecimalAvgOps.cpp` 读 row field 曾用 `slot.desc.inputKind`；runtime `fillHashAggrJitRowInputRuntime` 又按 vector 真实类型再反推一次。两侧不一致时 stride 错。当前在不扩 descriptor 的前提下，先利用已有 `precision` 作为 codegen 期 single source of truth，消除了 decimal partial row 的位宽漂移。

### 🟢 B5. `MinAggregate<float/double>` 初值不同（语义等价，但容易看错）

> **【复核 @ `b4b99b5553`：已验证 JIT == 非 JIT，Spark 下也一致；仅需补防回归测试】**
> 注意前提：Spark 与 Presto 的 min/max **共用同一份 `registerMinMax` + 同一个 `MinAggregate`/`MaxAggregate` + 同一份 JIT op**，所以确实需要验，但结论是一致的。
> - 非 JIT 权威比较是 `SimpleVector::comparePrimitiveAsc`（`SimpleVector.h:368-380`）：**NaN 视为最大**（NaN 排在所有非 NaN 之后），且该语义**不随 `SPARK_COMPATIBLE` 改变**——Spark/Presto 统一。
> - JIT op（`MinMaxOps.cpp:45-59`）逐组合等价于 NaN=最大：
>   - Min：`(oldIsNan && !valueIsNan) || (!valueIsNan && old>value)` → 避开 NaN，仅全 NaN 时结果为 NaN；
>   - Max：`!oldIsNan && (valueIsNan || old<value)` → 倾向选 NaN。
>   - 对 {NaN,非NaN} 全部四种组合手工核对，结果与 `comparePrimitiveAsc` 完全一致。
> - 初值 `0.0`（JIT）/ `NaN`（非 JIT Presto）都不参与比较：首条非 null 输入必定 `nullState=true` 无条件覆盖初值（`shouldStore = nullState || better`）。
> - **结论**：B5 不是 bug，语义在 Spark 与 Presto 下均一致。剩余价值仅为**防回归**：补 `max(NaN,5.0)`/`min(NaN,5.0)` 的 JIT vs 非 JIT 对照用例钉死等价性，优先级 P3。

- 非 JIT Presto: `kInitialValue_ = NaN` (`MinMaxAggregates.cpp:367-371`)
- JIT: 统一写 `0.0` (`MinMaxOps.cpp:21`)；靠 `shouldStore = nullState || better` 让第一条非 null 输入无条件覆盖
- 我手算了所有 NaN/非 NaN 组合，Presto 下结果一致；**但 Spark MinMax 的 NaN 排序语义和 Presto 不同**，如果 Spark 也走同一份 JIT op，需要再验。

### 🟢 B6. `MinMax<int128>` 混合路径

> **【复核 @ `b4b99b5553`：已过时】** Int128/Bool 的 extract 已实现，`canCompileMinMaxExtract` 及整个 `CanExtractFn` 已删除，extract 不再走非 JIT 混合路径。B6 描述的现象不复存在。null 槽布局一致性的 NOTE 注释建议仍可保留参考。

`canCompileMinMaxExtract` (`MinMaxOps.cpp:74-79`) 对 `Int128`/`Bool` 返回 `false`，extract 走非 JIT，init/update 走 JIT。当前 `slot.nullByte/nullMask` 来自 `RowContainer::nullByte/Mask`（`GroupingSet.cpp:766`），和 `exec::Aggregate::isNull` 一致——OK，但建议加一条 NOTE 注释防止后续重构改 null 槽布局踩坑。

### 🟢 B7. 整数 `sum` 溢出

> **【复核 @ `b4b99b5553`：Spark 语义下不是 bug，结论一致】**
> - 走 JIT 的整数 sum **只有 Spark**：Presto 的 sum 未注册 `supportsHashAggrJit`（prestosql 下仅 Count/MinMax 接入 JIT），因此不存在"Presto sum 复用 JIT"的实际路径。注意 sum 与 min/max 不同——min/max 是 Spark/Presto 共用注册，sum 各自独立注册（Spark 有自己的 `registerSumAggregate`）。
> - 非 JIT Spark sum：`setSumAggOverflowCheckFlag(false)`（`SumAggregate.cpp:224`）→ `Overflow=true` 分支 → 静默回绕。
> - JIT Spark sum：整数走 `CreateAdd`（`SumOps.cpp:46-47`）→ 静默回绕。
> - **结论**：两者完全一致（都静默回绕），Spark 下无差异。同事建议的 `BOLT_CHECK(Overflow==true)` 仅为防止未来误改全局 flag 的护栏，属可选 P3。

- 非 JIT 默认 `CHECK_ADD` 抛异常；Spark 在 `registerSumAggregate` 显式 `setSumAggOverflowCheckFlag(false)` → `Overflow=true` → 静默回绕（`SumAggregateBase.h:190-197`、`SumAggregate.cpp:222`）。
- JIT 永远 `CreateAdd` 静默回绕（`SumOps.cpp:46-47`）。
- 当前只有 Spark 注册了 `supportsHashAggrJit`，**结论一致**。建议 JIT 入口加一条 `BOLT_CHECK(Overflow)` 防止后续误改全局 flag 让 Presto 路径也复用 JIT。

---

## C. 输入类型覆盖矩阵（按算子）

| 算子 | 非 JIT 支持 | JIT 支持 | 不一致点 |
|---|---|---|---|
| `avg` | numeric + short/long decimal | numeric only（decimal 走另一条） | OK，gate 正确（`AverageAggregate.cpp:53-54` raw decimal 显式 false） |
| `count(*)/(col)` | 全部 | numeric + short/long decimal + hugeint | OK |
| `sum` | 同 avg | 非 decimal numeric + hugeint | OK |
| `min/max` | 全部 | numeric + short/long decimal + hugeint | OK；long decimal extract 走非 JIT (B6) |
| `decimal_sum` | short/long decimal raw + `ROW<decimal,bool>` intermediate | 同左 | ✅ B1/B4 已修 |
| `decimal_avg` | short/long decimal raw + `ROW<decimal,bigint>` intermediate | 同左 | ✅ B2/B4 已修 |

---

## D. 最终判定（Verdict）

> **【复核 @ 当前工作区】** 下表反映原审计；当前状态：累加器布局一致性已升级为"单一权威类型"（B3 解决）；B1/B2 的 Partial/Final extract 短 decimal 崩溃已修；B4 的 decimal partial merge row-field kind 漂移已修。剩余 B5/B7 均为 P3 加固项。

| 维度 | 一致？ |
|---|---|
| Per-group 累加器结构体字节布局 | ✅ 全部一致 |
| 初值 + null bit 语义 | ✅ 一致（Presto MinMax 已校对；Spark MinMax NaN 排序待验） |
| Update 单点累加语义 | ✅ 一致（Spark 整数 sum 在 `Overflow=true` 下也一致） |
| Merge intermediate 语义 | ✅ B4 已修：decimal sum/avg partial row sum 字段按 `precision` 推导 kind |
| Partial extract → ROW 输出 | ✅ B1/B2 已修：decimal sum/avg 在 codegen 期选择 short/long 专用 helper |
| Final extract → 标量输出 | ✅ B1 已修：decimal sum final 在 codegen 期选择 short/long 专用 helper |
| 类型覆盖矩阵 | ✅ decimal 短/长结果不再依赖回退规避 crash |

---

## E. 最小修复清单（按优先级）

> **【复核 @ 当前工作区：当前优先级总览】**
> - **已解决**：B1 / B2 —— runtime helper 短 decimal 崩溃；B4 —— decimal partial row 输入 stride 漂移。
> - **P3**：B5 / B7 —— 防御性测试/断言加固。
> - **已解决/过时（无需再做）**：B3（已用单一权威布局根除）、B6（canExtract 已删、Int128/Bool extract 已支持）。

1. **B1/B2 修复（已完成）**
   `emitDecimalSumExtract` / `emitDecimalAvgExtract` 已按 partial/final 的实际输出精度选择 short/long 专用 runtime helper，避免把 `longDecimal` 作为外部 C++ helper 参数导致 runtime 内保留无效分支。

2. **B3 修复（廉价护栏）**
   在 `HashAggrJitDecimalState.h` 同时 include 两侧头（`DecimalSumAggregate.h` / `DecimalAggregate.h` / `AverageAggregateBase.h`），加 `static_assert(sizeof, offsetof)` 跨层断言。AvgState 同理（与 `SumCount<double>` cross-check）。

3. **B4 修复（已完成最小闭环）**
   decimal partial merge 的 sum 字段已按 `precision` 推导为 `Int64/Int128`，并用该 kind 做 row-field read 和 cast。后续若要泛化到所有 ROW 字段，可再考虑 `HashAggrJitDescriptor.rowInputFields[i].kind`。

4. **B5 加固**
   补一份 Spark MinMax NaN-排序的对照测试（`max(NaN, 5.0)` / `min(NaN, 5.0)` JIT vs 非 JIT 必须完全一致），目前只校对了 Presto。

5. **B7 加固**
   JIT 整数 sum slot 入口加 `BOLT_CHECK(Overflow == true)`，防止后续被静默改坏。

---

## 附录：关键文件一览

| 路径 | 作用 |
|---|---|
| `bolt/jit/aggregation/HashAggrJit.{h,cpp}` | JIT 主框架、IR codegen、runtime 装载 |
| `bolt/jit/aggregation/HashAggrJitTypes.h` | `HashAggrJitDescriptor` / `HashAggrJitSlot` / 输入输出 runtime 结构体 |
| `bolt/jit/aggregation/HashAggrJitDecimalState.h` | `JitDecimalSumState` / `JitDecimalAvgState`（**缺 cross-assert**） |
| `bolt/jit/aggregation/ops/{Avg,Count,Sum,MinMax,DecimalSum,DecimalAvg}Ops.cpp` | 各算子的 init/update/merge/extract 编译规则 |
| `bolt/jit/aggregation/runtime/HashAggrDecimalRuntime.cpp` | decimal sum/avg extract 的 C++ 运行时 helper（B1/B2 已修） |
| `bolt/exec/GroupingSet.cpp` | JIT chunk 调度、runtime fill、回退判断 |
| `bolt/functions/sparksql/aggregates/DecimalSumAggregate.h` | `DecimalSum` 非 JIT 结构 + `supportsHashAggrJit` |
| `bolt/functions/lib/aggregates/DecimalAggregate.h` | `LongDecimalWithOverflowState` 非 JIT 结构 |
| `bolt/functions/lib/aggregates/AverageAggregateBase.h` | `SumCount<TAccumulator>` 非 JIT 结构 |
| `bolt/functions/lib/aggregates/SumAggregateBase.h` | 整数 sum `CHECK_ADD` 与全局 `Overflow` flag |
| `bolt/functions/sparksql/aggregates/{SumAggregate,AverageAggregate}.cpp` | Spark sum/avg 注册 + JIT gate |
| `bolt/functions/prestosql/aggregates/{MinMaxAggregates,CountAggregate}.cpp` | Presto MinMax/Count 注册 + JIT gate |
