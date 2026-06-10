# HashAggr JIT TODO List

## Resolved（已处理，保留遗留风险备忘）

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
