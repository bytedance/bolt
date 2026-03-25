## Name

Bolt Agent Runtime — 面向 Agent-to-Agent 数据工作流的 AI Ready 执行引擎

## Author

ming.cheng

## Category

Data for AI

## Description

### 背景与动机

Bolt 其实已经具备了很多面向 Agent 的执行层基础能力：

- 可移植的 Plan 表示与 Substrait 转换层
- 可组合、可扩展的执行引擎与较完整的算子能力
- Task / Operator 粒度的 Trace 基础设施
- 支持自定义 PlanNode 的 Python 扩展能力
- Native Shuffle 与多运行时场景的落地经验

今天，这些能力主要是为框架接入和人类开发者服务的。但下一阶段的数据系统集成形态会发生变化：上游调用方越来越可能是一个 AI Agent，它负责生成计划、修补查询、或者提出优化策略；下游消费者也越来越可能是另一个 AI Agent，它负责诊断回归、解释失败原因、或者决定接下来的动作。

这意味着执行引擎需要优化的目标也变了。

当上下游用户都是 Agent 时，算子支持校验已经不是主要缺口。真正缺的是一个机器友好的 Runtime Contract：

- 上游 Agent 需要一种确定性的方式来提交任务，并且可以增量地修改它。
- 下游 Agent 需要的是结构化运行时反馈，而不是截图、日志和人工排查。
- 调试 Agent 需要的是可重放的执行胶囊，而不是模糊的“当时大概发生了什么”。

**Bolt Agent Runtime 的核心定位，是把 Bolt 从一个面向人类的执行后端，升级成一个具备结构化诊断、可重放、可打补丁能力的 AI Ready 执行引擎。**

### 技术架构

```text
Agent Planner / Optimizer
  -> Bolt Execution Request
     - Substrait plan
     - split info
     - session / connector properties
     - execution goal (latency / cost / memory / determinism)
  -> Bolt PlanNode conversion
  -> Bolt Execution Engine
  -> Result stream + Execution Critique + Replay Capsule
  -> Agent Debugger / Tuner / Auditor
```

这个方案的核心，是把两种能力合并成一个面向 Agent 的 Runtime Surface：

1. **Execution Critique**：执行结束后，Bolt 输出一份机器可消费的诊断结论，说明发生了什么、下一步该尝试什么。
2. **Replay Capsule**：Bolt 输出一个确定性的执行胶囊，让另一个 Agent 可以复现、检查、对比，并在此基础上继续 patch。

这不是再造一个 Planner，也不是再包一层聊天机器人，而是在 Bolt 现有执行引擎、Substrait 与 Trace 基础之上，定义一套新的执行契约。

### 核心价值：让 Bolt 成为 Agent 可以推理的 Runtime

> 核心问题不是 “Agent 已经会生成 Plan 了，还缺什么？” 真正缺的是一个能够解释自己、复现自己、并接受局部修改的执行层。

传统执行引擎优化的是人类排障路径：

- 人写 SQL 或提交框架任务
- 引擎执行
- 人去看日志、指标、trace、stack trace
- 人猜测根因
- 人改写后再次提交

这条链路对 Agent 来说太慢、太松散，也太不结构化。

Bolt Agent Runtime 会把交互方式改成：

- Agent 提交 plan + execution intent
- Bolt 执行并输出结构化 critique
- Bolt 同时产出 replayable runtime capsule
- 下游 Agent 基于 capsule 和 critique 提出 patch 或 retry 策略
- Bolt 只重跑变化部分，或者确定性回放

这样 Bolt 就不只是一个快的执行引擎，而会变成优化 Agent、测试 Agent、调试 Agent、自治数据工作流的执行底座。

**1. Execution Critique：把运行时信号变成 Agent 可行动的反馈**

**传统方式（高度依赖人工）：**
- 人工查看 operator trace
- 人工从日志中推断时间和内存到底花在哪
- 人工猜测性能下降来自 skew、repartition、scan selectivity 还是 UDF 行为

**有了 Bolt Agent Runtime：**

Bolt 已经具备 trace metadata 和 operator summary 的基础能力。缺的是把这些底层信号提升成一个下游 Agent 可以直接消费的 critique 对象。

```json
{
  "query_id": "q_20260324_001",
  "status": "SUCCESS",
  "critique": {
    "dominant_operators": [
      {
        "plan_node_id": "17",
        "operator_type": "HashAggregation",
        "symptom": "high_peak_memory",
        "evidence": {
          "peak_memory_bytes": 8589934592,
          "input_rows": 187000000
        },
        "suggested_actions": [
          "increase_partition_count",
          "rewrite_to_two_phase_aggregation",
          "prefer_pre_aggregation_before_shuffle"
        ]
      },
      {
        "plan_node_id": "11",
        "operator_type": "TableScan",
        "symptom": "low_filter_selectivity",
        "evidence": {
          "raw_input_rows": 250000000,
          "input_rows": 240000000
        },
        "suggested_actions": [
          "check_filter_pushdown",
          "consider_partition_pruning"
        ]
      }
    ],
    "global_findings": [
      "shuffle_dominated_runtime",
      "memory_pressure_likely_contributed_to_latency"
    ]
  }
}
```

下游优化 Agent 可以直接基于这份输出继续工作：

```bash
bolt-agent run plan.json --critique-out critique.json

cat critique.json | llm "
你是一个 Bolt 物理计划调优 Agent。
请根据这份执行 critique，给出一个最小且安全的 plan patch，
目标是在不改变语义的前提下降低内存压力。只返回 patch JSON。"
```

**2. Replay Capsule：让每一次执行都可以被另一个 Agent 可靠复现**

**传统方式（高度依赖人工）：**
- 先去问原始提交人当时用了哪些 session config
- 再从日志里猜测原始 plan 长什么样
- 再换一份数据子集试着复现，希望能撞到同样的问题

**有了 Bolt Agent Runtime：**

每一次执行都可以产出一个 replay capsule，打包下游调试 Agent 或审计 Agent 复现运行所需的关键上下文：

- serialized plan
- split metadata
- connector properties
- query config
- trace summaries
- 可选的 sampled input 或 operator input snapshot

```json
{
  "capsule_version": 1,
  "query_id": "q_20260324_001",
  "plan": "substrait_plan_bytes_or_json",
  "splits": {
    "17": {
      "partition_index": 0,
      "paths": ["hits/part-000.parquet"],
      "starts": [0],
      "lengths": [268435456]
    }
  },
  "connector_properties": {
    "hive.s3.endpoint": "masked",
    "session.timezone": "UTC"
  },
  "query_config": {
    "task.max-drivers-per-task": "8"
  },
  "trace_summary": {
    "dominant_operator": "HashAggregation",
    "peak_memory_bytes": 8589934592
  }
}
```

之后下游 Agent 就可以直接复现或对比：

```bash
bolt-agent replay capsule.json --trace-out replay-trace.json

cat replay-trace.json | llm "
这是一次对历史慢查询的 replay。
请结合原始 critique 判断问题是稳定复现、数据相关，还是配置相关。"
```

**3. Patch-Oriented Execution：让 Agent 可以针对 Plan 做局部编辑，而不是整单重提**

**传统方式（高度依赖人工）：**
- 一个很小的逻辑或物理修改，也要重新提交完整查询
- 未变部分仍然整条链路重跑
- 人工记录每一轮到底改了什么

**有了 Bolt Agent Runtime：**

Agent 可以提交 plan patch，而不是完整替换整个 plan：

```json
{
  "base_capsule": "capsule.json",
  "patch": {
    "op": "replace_subtree",
    "target_plan_node_id": "17",
    "new_subplan": {
      "type": "two_phase_hash_aggregation",
      "grouping_keys": ["URL"],
      "aggregate_functions": ["count"]
    }
  }
}
```

```bash
bolt-agent patch capsule.json patch.json --critique-out patched-critique.json
```

这就能形成一个非常紧凑的优化闭环：critique -> patch -> rerun -> compare。

**4. 面向 Agent 的失败语义**

在 Agent-to-Agent 集成场景里，stack trace 本身价值有限。Bolt 应该输出结构化失败对象，至少包含：

- failure category
- 是否可重试
- plan node id
- operator type
- 局部证据
- replay capsule 路径
- 建议的下一步动作

```json
{
  "status": "FAILED",
  "error": {
    "category": "RESOURCE_EXHAUSTED",
    "retriable": true,
    "plan_node_id": "17",
    "operator_type": "HashAggregation",
    "message": "peak memory exceeded limit",
    "suggested_actions": [
      "increase_partition_count",
      "enable_spill",
      "retry_with_lower_concurrency"
    ],
    "replay_capsule": "capsules/q_20260324_001.json"
  }
}
```

这样一个恢复 Agent 就可以在不读日志、不做人工排查的情况下决定下一步动作。

### 核心转变：Bolt 成为 Agent 的 Execution Coprocessor

| 任务 | 没有 Agent Runtime | 有 Bolt Agent Runtime |
|------|--------------------|-----------------------|
| 性能诊断 | 人工读 trace 然后猜 | Bolt 直接输出结构化 critique 和建议动作 |
| 调试复现 | 人工重建环境 | 通过 replay capsule 精确复现 |
| 优化闭环 | 每次 tweak 都整单重提 | Agent 基于 capsule 提交 patch |
| 失败处理 | stack trace + logs | 结构化失败对象 + retry guidance |
| 审计 / 校验 | 人工收集指标 | 机器可消费的 execution receipt 和 trace summary |

真正的价值并不是 Bolt “多了一个 CLI”，而是 Bolt 变成了一个 **机器可推理的执行引擎**：

- 上游 Agent 可以确定性地提交任务
- 下游 Agent 可以结构化地诊断和优化它
- replay 和 patch API 让整个闭环又紧又稳

这才是系统层面上 “AI Ready 执行引擎” 应该具备的含义。

## Demo Code

### Demo 1：执行一个 Plan，并输出 Critique + Capsule

```bash
bolt-agent run \
  --plan clickbench_q7.substrait.json \
  --splits hits_splits.json \
  --goal minimize_latency \
  --result-out result.arrow \
  --critique-out critique.json \
  --capsule-out capsule.json
```

示例返回：

```json
{
  "status": "SUCCESS",
  "result_path": "result.arrow",
  "critique_path": "critique.json",
  "capsule_path": "capsule.json"
}
```

### Demo 2：LLM 驱动的调优闭环

```bash
bolt-agent run \
  --plan clickbench_q7.substrait.json \
  --splits hits_splits.json \
  --critique-out critique.json \
  --capsule-out capsule.json

cat critique.json | llm "
你是一个 Bolt 物理优化 Agent。
请读取这份 critique，生成一个最小 JSON patch 来降低内存压力。
优先考虑 repartition 或 two-phase aggregation，而不是语义改写。"
> patch.json

bolt-agent patch \
  --capsule capsule.json \
  --patch patch.json \
  --critique-out patched-critique.json \
  --capsule-out patched-capsule.json
```

### Demo 3：确定性 Debug Replay

```bash
bolt-agent replay \
  --capsule capsule.json \
  --trace-out replay-trace.json \
  --result-out replay.arrow

cat replay-trace.json | llm "
请比较这次 replay trace 和原始 critique。
这个慢点是否稳定复现？主导算子是否仍然相同？"
```

### Demo 4：C++ Runtime Surface

执行运行时接口可以非常贴近 Bolt 现有架构：

```cpp
struct AgentExecutionRequest {
  std::string queryId;
  std::string serializedSubstraitPlan;
  std::unordered_map<std::string, std::string> queryConfig;
  std::unordered_map<std::string, std::string> connectorProperties;
  std::string executionGoal;
};

struct AgentExecutionResponse {
  std::string resultPath;
  std::string critiquePath;
  std::string capsulePath;
};

class AgentRuntime {
 public:
  AgentExecutionResponse run(const AgentExecutionRequest& request);
  AgentExecutionResponse replay(const std::string& capsulePath);
  AgentExecutionResponse patch(
      const std::string& capsulePath,
      const std::string& patchPayload);
};
```

第一版实现可以直接复用 Bolt 现有模块：

- `bolt/substrait/*` 负责 plan ingestion
- `bolt/exec/Trace.h` 以及 task/operator trace reader/writer 负责提取运行时证据
- `bolt/python/PlanNode.*` 可在需要时承载 Agent 编写的扩展逻辑

### Demo 5：基于现有 Trace Summary 生成 Critique

```cpp
Critique buildCritique(
    const std::vector<exec::trace::OperatorTraceSummary>& summaries) {
  Critique critique;
  for (const auto& summary : summaries) {
    if (summary.peakMemory > memoryThreshold) {
      critique.globalFindings.push_back("memory_pressure_detected");
      critique.operatorFindings.push_back(OperatorFinding{
          .opType = summary.opType,
          .symptom = "high_peak_memory",
          .suggestedActions = {
              "increase_partition_count",
              "enable_spill",
              "rewrite_to_two_phase_aggregation"}});
    }

    if (summary.rawInputRows > 0 &&
        summary.inputRows * 100 / summary.rawInputRows > 95) {
      critique.operatorFindings.push_back(OperatorFinding{
          .opType = summary.opType,
          .symptom = "low_filter_selectivity",
          .suggestedActions = {
              "check_filter_pushdown",
              "consider_partition_pruning"}});
    }
  }
  return critique;
}
```

这正是一个很薄、但很高价值的语义层：把 Bolt 已有的底层运行时指标，转换成 Agent 可直接行动的反馈。

## Execution Plan (Optional)

### Phase 1：Runtime Contract MVP（2-3 周，1 人）

**目标**：让一次 Bolt 执行对另一个 Agent 来说既可复现，又可解释。

- 增加一个轻量的 `bolt-agent` 入口，接收 serialized plan 和 split metadata
- 执行后输出结果，以及 `critique.json` 和 `capsule.json`
- 复用现有 trace 基础设施，定义第一版 critique schema
- 支持从 capsule 发起 replay

**交付物**：一个端到端 demo，展示 Agent 提交 plan、拿到 critique，并基于 capsule 复现执行。

### Phase 2：Patch Loop（2-4 周，1 人）

**目标**：支持 Agent 驱动的迭代优化。

- 定义最小 patch schema：replace subtree、update property、change partitioning
- 支持基于已有 capsule 应用 patch
- 对比原始 critique 与 patched critique
- 增加一个简单的 “recommended next actions” 启发式层

**交付物**：针对代表性 benchmark query，跑通 critique -> patch -> rerun -> compare 闭环。

### Phase 3：Failure / Audit Surface（2-3 周，1 人）

**目标**：让 Bolt 能够安全地进入自治工作流。

- 为运行时异常输出结构化 failure object
- 增加 execution receipt，供 audit agent 使用
- 在 capsule 中对 connector secrets 做 mask / redact
- 增加 deterministic replay、trace sampling、retention 等策略开关

**交付物**：面向生产化集成场景的 Agent 友好错误处理与审计能力。

### Long-term Vision

> Bolt Agent Runtime 会把 Bolt 变成一个 Agent 可以执行、检查、打补丁、重放、并持续改进的 Runtime。

长期来看，这会让 Bolt 不只是 Spark 或 Python 下的一层执行后端，而是一个通用的执行底座，可服务于：

- optimizer agents
- debugging agents
- regression triage agents
- benchmark / evaluation agents
- autonomous data product workflows

最有价值的 AI 系统，不会停留在 “生成一个 Plan” 这一步。它们会在执行之后继续闭环。而 Bolt 可以通过“让执行结果对机器来说可解释、可复现、可 patch”，占住这一层的核心位置。
