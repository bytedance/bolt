# HashAggr JIT TODO List

## Resolved（已处理，保留遗留风险备忘）

### HashAggrJitOps 散布在各 aggregate + Aggregate.h 硬依赖 LLVM 头

**问题**
- `Aggregate.h` `#include HashAggrJit.h`，后者 `#include <llvm/IR/IRBuilder.h>` 等重头，导致
  所有 include `Aggregate.h` 的 TU（JIT 开启时）被拖进 LLVM IR 头，编译时间膨胀。
- 每个 aggregate 子类内嵌一组 `compileHashAggrJit*` static codegen，依赖 IRBuilder；
  `DecimalSumAggregate.h` 模板头塞了 ~120 行 codegen，每个实例化点重复展开。
- runtime helper（`jit_HashAggrSetFlat*` 等）散落在 `RowContainer.cpp`，decimal extract
  helper 散落在 `SumAggregate.cpp` / `AverageAggregate.cpp`。

**本次处理（四点 + 遗留点，已编译验证）**
1. 剥离 LLVM 头出 `Aggregate.h`：
   - 新建 `bolt/jit/aggregation/HashAggrJitTypes.h`（纯 metadata，无 LLVM）：
     state / decoded&output 描述符 / planContext / enum / `HashAggrJitDescriptor`
     （`ops` 持有前向声明的 `HashAggrJitOps*`）/ `HashAggrJitSlot` / 三个自由函数声明
     / `getXxxOps()` 声明。
   - `HashAggrJit.h` 改为 `#include HashAggrJitTypes.h` + 仅保留 codegen-only
     （`HashAggrJitOps` / `HashAggrJitExtractTarget` / `HashAggrJitCodegen` / `HashAggrJitChunk`）。
   - `Aggregate.h` 的 include 改为 `HashAggrJitTypes.h`，LLVM 头不再进公共头。
2. 各 aggregate codegen 迁到 `bolt/jit/aggregation/ops/*Ops.cpp`：
   `CountOps / MinMaxOps / SumOps / AvgOps / DecimalSumOps / DecimalAvgOps`，各 `getXxxOps()`；
   编入 `bolt_thrustjit`。aggregate 子类只留 `supportsHashAggrJit` + `createHashAggrJitDescriptor`
   （`.ops = jit::getXxxOps()`）。须留类内：MinMax 的虚函数 `jitKind()`、Decimal 的
   `sumType_` / `resultType()` 依赖。
3. runtime helper 迁到 `bolt/jit/aggregation/runtime/`：
   - `HashAggrRuntime.cpp`：`jit_HashAggrResizeVector` / `SetFlat*` / `SetPartialAvgDouble`
     （原在 `RowContainer.cpp`）。
   - `HashAggrDecimalRuntime.cpp`（遗留点）：`jit_HashAggrExtract{Final,Partial}Decimal{Sum,Avg}`
     + `jitDecimalSumComputeFinal`（原在 `SumAggregate.cpp` / `AverageAggregate.cpp`）。
   - 两文件只依赖 vector + `bolt/type/DecimalUtil.h`，编入 `bolt_exec`（同符号空间、
     `ENABLE_EXPORTS`，仍 extern "C" + visibility default，dlsym 可解析）。
4. 编译验证：`bolt_thrustjit` / `bolt_exec` / `bolt_aggregates` /
   `bolt_functions_spark_aggregates` 均通过；nm 确认 `jit_HashAggr*` 12 个符号在新文件
   以 `T` 导出，旧文件无残留定义。

**未做 / 遗留**
- 链接级端到端单测运行验证未做（当前为 release 纯库配置，无可执行 target）。
  需要时配 `release_with_test` 跑 HashAggr JIT 单测。
- 已知 `bolt/functions/sparksql/aggregates/CMakeLists.txt` 里 `SumAggregate.cpp` 被列两次
  （历史问题，非本次范围，未改）。

### Descriptor ↔ Slot 字段重复 / positional init 易错

**问题**
- `HashAggrJitDescriptor` 与 `HashAggrJitSlot` 字段大量重复（slot 仅多 row-layout 字段），
  各 aggregate 以 positional 方式构造 descriptor（连续 bool 靠人眼对位，易出低级 bug），
  `createHashAggrJitSlot` 又逐字段 copy boilerplate。

**本次处理**
- 建议1：6 处 `createHashAggrJitDescriptor` 全改 C++20 designated initializer
  （`.kind=`、`.inputKind=` …），消除 positional 对位风险；未 reorder 字段。
- 建议2：`HashAggrJitSlot` 改为内嵌 `HashAggrJitDescriptor desc`，只保留
  `aggregateIndex/offset/nullByte/nullMask` + `desc`；`createHashAggrJitSlot`
  缩为 4 字段 + `.desc = descriptor`。IR 端与 `functionName()` 等约 70 处
  `slot.<trait>` 统一改成 `slot.desc.<trait>`；`offset/nullByte/nullMask` 仍在顶层。
- 验证：无残留旧式访问、无 `descriptor.desc`、无 `.desc.offset` 误改、无双重 `.desc.desc`、
  `HashAggrJitDescriptor::signature()`（裸字段名）未受影响。未重新编译。

### 删除 JIT init 对 `Aggregate::numNulls_` 的同步（commit f74cc21160）

**背景**
- 旧机制：JIT initGroup 直接写 group 的 null bit，但不碰 `Aggregate::numNulls_`；
  而非 JIT extract 的 `isNull()` 依赖 `numNulls_` 短路（为 0 时直接判非 null）。
- 为弥合差异，曾引入 `HashAggrJitDescriptor/Slot::initSetsNull` 标志 +
  `Aggregate::addNumNulls()`，由 `GroupingSet` 在 JIT init 后手工补账。
- 该机制最初动机：partial agg 中「add 走 JIT、extract 走非 JIT」时的 null diff。
  现在 add/extract 均支持 JIT，价值大幅下降，且属跨层补丁、封装差。

**本次处理**
- 已删除：`Aggregate::addNumNulls()`、`GroupingSet` 中的 `initSetsNull → addNumNulls`
  补账循环、`HashAggrJitDescriptor/Slot::initSetsNull` 字段、各 aggregate 构造处的
  `/*initSetsNull=*/` 实参。

**遗留风险（需后续验证 / 补强）**
- 当前 add/extract 仍是 best-effort，存在静默回落非 JIT 的口子，最典型是 **spill**：
  - extract 在 `hasSpilled()` / `supportRowBasedOutput_` 时整体跳过 JIT。
  - encoding 不符预期、distinct/mask/sortingKeys 等也会 fallback。
- 风险场景：某 slot 用了 JIT add（init 只写 null bit、未维护 `numNulls_`），但运行时
  回落非 JIT extract → `isNull()` 因 `numNulls_==0` 短路，把「全 null 组」误判为非 null
  → 输出 0 而非 null（静默错数据）。守护用例：`hashAggrJitAllNullGroup`。

**后续待办（择机）**
- 重点回归：带 spill 的 partial agg（尤其全 null 组）。
- 选一条强化方向之一：
  - 做法 1（plan-time 硬门槛）：只有「add + extract 全程 JIT 有保证」的 slot 才进 JIT
    init/add，会 fallback 的（含可能 spill）一开始就不走 JIT。语义最干净。
  - 做法 2（fallback 现算）：保留 fallback，但在非 JIT extract 入口扫一遍 null bit 重建
    `numNulls_`，spill 场景也安全，改动小。

## Pending

### [P0] JIT add/merge+extract 路径正确性 bug，被 test 链接丢符号长期掩盖

**现象**
- 单测 `SumAggregationTest.hashAggrJitMergeAndExtract` 与
  `SumAggregationTest.hashAggrJitAllNullGroup`（均为 partial+final 两阶段、非 decimal）
  在 JIT 路径**真正执行**时结果错误：
  - `hashAggrJitAllNullGroup`：group sum 期望 12，得 0。
  - `hashAggrJitMergeAndExtract`：sum/avg/min 全 null、count 全 0，相当于 add 完全没生效。
- JIT 模块成功编译执行（无 "Symbols not found" / 无 fallback 日志），是**执行结果错**，
  不是回退。

**根因定位（已用 git stash 二分确认）**
- 与本轮 decimal IR 化改动**无关**：在干净 HEAD 上、仅加一个把 runtime 符号
  （如 `jit_HashAggrResizeVector`）拉进 test 可执行的 link anchor，这两个用例即 FAIL。
- 真正背景：commit `4cbfc5e590`（runtime helper 迁出 `RowContainer.cpp` 到独立 .o）后，
  这些 `jit_HashAggr*` 符号**未被 test/可执行链接**（无 C++ 引用，.o 被链接器丢弃）。
  于是 JIT 在 test 二进制里 materialize 失败 → **静默回退非 JIT** → 结果恰好正确 →
  **掩盖了 JIT 路径本身的既有正确性 bug**。
- 本轮 decimal 改动新增的 link anchor 把这些符号拉回可执行，JIT 路径终于被真正执行，
  从而**暴露**（非引入）该 bug。

**潜在影响（需进一步确认）**
- 若生产可执行同样没有引用这些 runtime .o，则 HashAggr JIT 在生产里可能**根本没在跑**
  （一直静默回退非 JIT）。需要核实生产链接是否包含这些符号。
- 一旦修复链接（让 JIT 真正执行），这个 add/merge+extract 正确性 bug 会立刻显现，
  必须在「启用 JIT 执行」之前先修。

**后续待办**
- 定位 add/merge+extract 在两阶段非 decimal 场景下结果归零/全 null 的根因
  （疑点：partial extract 与 final merge 的累加器布局 / null 语义，可能与
  commit `f74cc21160` 删除 `numNulls_` 同步相关——`allNullGroup` 正是该语义守护用例）。
- 当前 decimal 改动保留了 link anchor（benchmark 需要它，否则 JIT 符号缺失）；
  注意 anchor 会让上述 bug 在跑相关单测时显现为 FAILED。

**⚠️ 合入注意**
- 本轮 decimal IR 改动保留了 link anchor，启用后 JIT 路径会真正执行，导致
  `hashAggrJitMergeAndExtract` / `hashAggrJitAllNullGroup` 两个单测**变红（FAILED）**。
- 这不是 decimal 改动引入的回归，而是上述既有 bug 被暴露；但**合入前必须先修该 P0 bug，
  否则 CI 会红**。两个选项：
  1. 先修 add/merge+extract 正确性 bug，再合入（推荐）。
  2. 临时移除 link anchor —— 但那样 benchmark 里 JIT 符号又会解析失败、JIT 回退，
     decimal 性能改善无法体现。
- 简言之：**link anchor + 既有 bug 是绑定的**，要么一起修好，要么都先不动。

### [P2] chunk 同时 codegen `add_dense` 和 `add_dense_no_null`，编译时间与产物 ×2

**现状**
- 每个 chunk 在 `compile()` 里生成两份 add 函数，仅 `checkInputNulls` 不同：
  - `bolt/jit/aggregation/HashAggrJit.cpp:1281-1282`
- 两者差异 100% 在 `genAddDenseIR` 内的 null-check 分支：
  - `bolt/jit/aggregation/HashAggrJit.cpp:1016-1029`、`bolt/jit/aggregation/HashAggrJit.cpp:1040`
- 运行时按 batch 级 `inputsMayHaveNulls` 选函数指针，batch 内 stable。

**评估结论**
- 问题真实：codegen 时间 ~×2。
- 但**非 P0**：编译是 per-chunk 一次性、结果缓存在 `module_`/`addDense_`/`addDenseNoNull_`
  (`bolt/jit/aggregation/HashAggrJit.cpp:1301-1304`)，运行热路径只调用其中一个函数，
  不存在运行期代码膨胀。影响的是编译延迟，不是执行性能。建议定级 **P2**。

**为什么 pending**
- 是否值得改，取决于生产实际 workload，目前未知。

**决策需要的数据**
- JIT 编译耗时占比 / chunk 编译次数。
- `inputsMayHaveNulls == false` 的 batch 实际占比。

**候选方案**
- 维持现状：若编译耗时占比可忽略，不改。
- 推荐（建议2，lazy）：默认只编 `add_dense`，仅当出现 `inputsMayHaveNulls == false`
  的 batch 时再 lazy 编 `add_dense_no_null`；未就绪前 fallback 到 `add_dense`
  （对 no-null 输入同样正确，仅损失少量性能）→ 砍掉常见场景一半编译量，零正确性风险。
- 不推荐（建议1，运行期 i1 参数）：会让 no-null 热路径丢失编译期 dead-branch 消除，反而变慢。
- 高成本（建议3，alwaysinline + wrapper）：理论最优但需重写 add codegen 结构，
  回归面大，仅为省一次性编译，性价比低。
