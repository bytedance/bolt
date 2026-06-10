# HashAggr JIT TODO List

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
