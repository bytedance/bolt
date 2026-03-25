## Name

Bolt Agent Runtime — An AI-Ready Execution Engine for Agent-to-Agent Data Workflows

## Author

ming.cheng

## Category

Data for AI

## Description

### Background & Motivation

Bolt already has many of the hard systems primitives needed for agent-facing execution:

- A portable plan representation and Substrait conversion layer
- A composable execution engine with rich operator coverage
- Native trace infrastructure at task and operator granularity
- Python extensibility for custom plan nodes
- Native shuffle and multi-runtime deployment experience

Today, these capabilities are mostly exposed for frameworks and human developers. But the next wave of data systems integration will be different: the upstream caller is increasingly an AI agent that generates plans, patches queries, or proposes optimization strategies, and the downstream consumer is also an AI agent that diagnoses regressions, explains failures, or decides the next action in a workflow.

That changes what the execution engine must optimize for.

When both upstream and downstream users are agents, operator support validation is no longer the key missing piece. The key gap is a machine-native runtime contract:

- Upstream agents need a deterministic way to submit work and refine it incrementally.
- Downstream agents need structured runtime feedback, not screenshots, logs, or manual investigation.
- Debugging agents need replayable execution capsules, not vague descriptions of what might have happened.

**Bolt Agent Runtime's core positioning: turn Bolt from a human-oriented execution backend into a fully AI-ready execution engine with structured critique, replay, and patch-friendly execution APIs.**

### Technical Architecture

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

The core idea is to combine two capabilities into a single agent-native runtime surface:

1. **Execution Critique**: after a run, Bolt produces a machine-readable diagnosis of what happened and what to try next.
2. **Replay Capsule**: Bolt emits a deterministic bundle that another agent can use to reproduce, inspect, compare, and patch the run.

This is not another planner and not another chatbot wrapper. It is a new execution contract built on top of Bolt's existing engine, Substrait, and trace infrastructure.

### Core Value: Make Bolt a Runtime That Agents Can Reason About

> Core question: if an agent can already generate plans, what is still missing? The answer is an execution layer that can explain itself, reproduce itself, and accept targeted edits.

Traditional execution engines are optimized for human loops:

- human writes SQL or submits a framework job
- engine runs
- human reads logs, metrics, traces, and stack traces
- human guesses the root cause
- human rewrites and retries

That loop is too slow and too unstructured for agent-native workflows.

Bolt Agent Runtime changes the interface:

- agent submits plan + intent
- Bolt executes and emits structured critique
- Bolt emits a replayable runtime capsule
- downstream agent proposes a patch or retry strategy
- Bolt reruns only the changed part or replays deterministically

This makes Bolt usable not just as a fast engine, but as an execution substrate for optimization agents, testing agents, debugging agents, and autonomous data workflows.

**1. Execution Critique: Turn Runtime Signals into Agent-Actionable Feedback**

**Traditional approach (human-heavy):**
- Inspect operator traces manually
- Read logs to infer where time and memory went
- Guess whether slowdown came from skew, repartition, scan selectivity, or UDF behavior

**With Bolt Agent Runtime:**

Bolt already captures trace metadata and operator summaries. The missing layer is to lift these into a structured critique object that downstream agents can consume directly.

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

A downstream optimizer agent can act on this immediately:

```bash
bolt-agent run plan.json --critique-out critique.json

cat critique.json | llm "
You are a physical plan tuning agent.
Given this Bolt execution critique, propose the smallest safe patch
that reduces memory pressure without changing query semantics.
Return only a plan patch payload."
```

**2. Replay Capsule: Make Every Run Reproducible for Another Agent**

**Traditional approach (human-heavy):**
- Ask the original engineer what session configs they used
- Try to reconstruct the failing plan from logs
- Re-run on a different dataset slice and hope the failure reproduces

**With Bolt Agent Runtime:**

Each execution can emit a replay capsule containing the exact runtime ingredients needed by a downstream debugging or auditing agent:

- serialized plan
- split metadata
- connector properties
- query config
- trace summaries
- optional sampled inputs or operator input snapshots

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

A downstream agent can then reproduce or compare the run:

```bash
bolt-agent replay capsule.json --trace-out replay-trace.json

cat replay-trace.json | llm "
This is a replay of a previously slow Bolt execution.
Compare it with the original critique and decide whether the issue is stable,
data-dependent, or configuration-dependent."
```

**3. Patch-Oriented Execution: Let Agents Edit Plans Without Resubmitting Everything**

**Traditional approach (human-heavy):**
- Re-submit full queries after tiny logical or physical edits
- Recompute unchanged parts
- Manually keep track of what changed between attempts

**With Bolt Agent Runtime:**

Agents should be able to submit a plan patch instead of a full replacement:

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

This gives optimization agents a tight feedback loop: critique -> patch -> rerun -> compare.

**4. Agent-Native Failure Surfaces**

For agent-to-agent integration, stack traces are not enough. Bolt should emit a structured failure object that includes:

- failure category
- whether retry is safe
- plan node id
- operator type
- localized evidence
- replay capsule path
- suggested next steps

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

That lets a recovery agent decide what to do without reading logs or reverse engineering runtime behavior.

### The Core Shift: Bolt Becomes an Execution Coprocessor for Agents

| Task | Without Agent Runtime | With Bolt Agent Runtime |
|------|-----------------------|-------------------------|
| Performance diagnosis | Human reads traces and guesses | Bolt emits structured critique with suggested actions |
| Debugging | Human reconstructs environment manually | Replay capsule reproduces the exact run |
| Optimization loop | Full resubmission after each tweak | Agents send plan patches against a captured capsule |
| Failure handling | Stack traces and logs | Structured failure objects with retry guidance |
| Audit / verification | Manual metric collection | Machine-readable execution receipts and trace summaries |

The essential value is not that Bolt gains another CLI. The essential value is that Bolt becomes a **machine-reasonable execution engine**:

- upstream agents can submit deterministic work
- downstream agents can diagnose and improve it
- replay and patch APIs make the loop tight and reliable

That is what "AI-ready execution engine" should mean at the systems layer.

## Demo Code

### Demo 1: Run a Plan and Emit Critique + Capsule

```bash
bolt-agent run \
  --plan clickbench_q7.substrait.json \
  --splits hits_splits.json \
  --goal minimize_latency \
  --result-out result.arrow \
  --critique-out critique.json \
  --capsule-out capsule.json
```

Example response:

```json
{
  "status": "SUCCESS",
  "result_path": "result.arrow",
  "critique_path": "critique.json",
  "capsule_path": "capsule.json"
}
```

### Demo 2: LLM-Driven Tuning Loop

```bash
bolt-agent run \
  --plan clickbench_q7.substrait.json \
  --splits hits_splits.json \
  --critique-out critique.json \
  --capsule-out capsule.json

cat critique.json | llm "
You are a Bolt physical optimization agent.
Read this critique and produce a minimal JSON patch that reduces memory pressure.
Prefer repartition or two-phase aggregation over semantic rewrites."
> patch.json

bolt-agent patch \
  --capsule capsule.json \
  --patch patch.json \
  --critique-out patched-critique.json \
  --capsule-out patched-capsule.json
```

### Demo 3: Deterministic Debug Replay

```bash
bolt-agent replay \
  --capsule capsule.json \
  --trace-out replay-trace.json \
  --result-out replay.arrow

cat replay-trace.json | llm "
Compare this replay trace with the original critique.
Is the slowdown stable? Which operator remains dominant?"
```

### Demo 4: C++ Runtime Surface

The execution runtime can stay close to Bolt's existing architecture:

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

A first implementation can be built on top of existing Bolt components:

- `bolt/substrait/*` for plan ingestion
- `bolt/exec/Trace.h` and task/operator trace readers and writers for runtime evidence
- `bolt/python/PlanNode.*` for agent-authored extensions when needed

### Demo 5: Critique Construction from Existing Trace Summaries

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

This is exactly the kind of thin semantic layer that can convert Bolt's existing runtime metrics into agent-actionable feedback.

## Execution Plan (Optional)

### Phase 1: Runtime Contract MVP (2-3 weeks, 1 person)

**Goal**: make one Bolt execution reproducible and explainable to another agent.

- Add a small `bolt-agent` entry point that accepts a serialized plan and split metadata
- Emit result output plus `critique.json` and `capsule.json`
- Reuse existing trace infrastructure to build the first critique schema
- Support replay from a saved capsule

**Deliverable**: one end-to-end demo where an agent runs a plan, receives critique, and replays the run from capsule.

### Phase 2: Patch Loop (2-4 weeks, 1 person)

**Goal**: support iterative agent optimization.

- Define a minimal patch schema: replace subtree, update property, change partitioning
- Apply patches against a saved base capsule
- Compare original and patched critique side by side
- Add a simple "recommended next actions" heuristic layer

**Deliverable**: critique -> patch -> rerun -> compare loop for a representative benchmark query.

### Phase 3: Failure and Audit Surfaces (2-3 weeks, 1 person)

**Goal**: make Bolt trustworthy in autonomous workflows.

- Emit structured failure objects for runtime exceptions
- Add execution receipts for audit agents
- Mask or redact connector secrets in capsules
- Add policy knobs for deterministic replay, trace sampling, and retention

**Deliverable**: agent-friendly error handling and auditability for production-style integration.

### Long-term Vision

> Bolt Agent Runtime turns Bolt into a runtime that agents can execute against, inspect, patch, replay, and improve.

Long term, this positions Bolt as more than a backend under Spark or Python. It becomes a general-purpose execution substrate for:

- optimizer agents
- debugging agents
- regression triage agents
- benchmark and evaluation agents
- autonomous data product workflows

The most valuable AI systems will not just generate plans. They will close the loop after execution. Bolt can own that layer by making execution deterministic, inspectable, and patchable for machines.
