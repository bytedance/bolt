# Bolt Agent Runtime API Demo

This document is a short demo script for showing Bolt as an AI-facing execution
API instead of a human-only query engine.

The demo goal is simple:

1. start the API server
2. submit a real Bolt-backed run
3. inspect the machine-readable artifacts
4. replay or patch the run like an upstream/downstream agent workflow

## Demo Story

Use this framing in the demo:

- An upstream agent sends a structured execution request instead of ad hoc SQL.
- Bolt executes the request and emits structured artifacts.
- A downstream agent reads `result`, `critique`, and `capsule`.
- The downstream agent can replay the same run or submit a patch without
  rebuilding the whole request.

This is the core "AI-ready execution engine" story.

## Prerequisites

Build the real adapter binary:

```bash
cmake --build _build/Release --target bolt_agent_runtime_executor -j 8
```

Expected binary:

```bash
_build/Release/bolt/bolt_agent_runtime_executor
```

## Step 1: Start The Server

```bash
python3 demo/agent_runtime_api/server.py \
  --host 127.0.0.1 \
  --port 8091 \
  --adapter-bin _build/Release/bolt/bolt_agent_runtime_executor
```

Expected startup log:

```text
Bolt Agent Runtime API listening on http://127.0.0.1:8091 using adapter ...
```

## Step 2: Health Check

```bash
curl -s http://127.0.0.1:8091/healthz
```

Expected response:

```json
{
  "adapter_bin": "/abs/path/to/_build/Release/bolt/bolt_agent_runtime_executor",
  "status": "ok"
}
```

Talking point:

- The API is not using a synthetic runtime here. It is wired to the real Bolt
  adapter binary.

## Step 3: Submit A Run

Use the provided sample request:

- [sample_request.json](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime_api/sample_request.json)

```bash
curl -s http://127.0.0.1:8091/runs \
  -X POST \
  -H 'Content-Type: application/json' \
  --data @demo/agent_runtime_api/sample_request.json
```

Example response shape:

```json
{
  "artifacts": {
    "capsule": "http://127.0.0.1:8091/artifacts/RUN_ID/capsule",
    "critique": "http://127.0.0.1:8091/artifacts/RUN_ID/critique",
    "request": "http://127.0.0.1:8091/artifacts/RUN_ID/request",
    "result": "http://127.0.0.1:8091/artifacts/RUN_ID/result"
  },
  "capsule_url": "http://127.0.0.1:8091/capsules/RUN_ID",
  "mode": "run",
  "query_id": "group_by_demo",
  "run_id": "RUN_ID",
  "runtime_ms": 356,
  "status": "SUCCESS"
}
```

Talking points:

- The caller gets stable artifact URLs, not just a terminal printout.
- This is what makes the engine easy for another agent to consume.

## Step 4: Inspect The Result Artifact

Replace `RUN_ID` with the value from `POST /runs`.

```bash
curl -s http://127.0.0.1:8091/artifacts/RUN_ID/result
```

Example result:

```json
{
  "dominant_operator": null,
  "peak_memory_bytes": 0,
  "plan_string": "-- TopN ...",
  "query_id": "group_by_demo",
  "row_count": 3,
  "rows": [
    "{a, 1500000, 100000}",
    "{c, 750000, 100000}",
    "{b, 250000, 50000}"
  ],
  "runtime_ms": 356,
  "status": "SUCCESS"
}
```

Talking points:

- The API returns rows plus execution metadata.
- This is already better suited for agent workflows than a human-only CLI.

## Step 5: Inspect The Critique Artifact

```bash
curl -s http://127.0.0.1:8091/artifacts/RUN_ID/critique
```

Example result:

```json
{
  "dominant_operators": [],
  "global_findings": [
    "latency_above_interactive_budget"
  ],
  "goal": "minimize_latency",
  "operator_findings": [],
  "query_id": "group_by_demo",
  "recommended_next_actions": [],
  "status": "SUCCESS"
}
```

Talking points:

- `critique.json` is the agent-facing interpretation layer.
- A downstream optimizer or debugging agent can read this without parsing logs.
- Today the critique is still lightweight; the API contract is the important
  demo surface.

## Step 6: Inspect The Capsule Artifact

```bash
curl -s http://127.0.0.1:8091/artifacts/RUN_ID/capsule
```

What to highlight:

- the original request
- replayable metadata
- trace location / execution context

Talking point:

- A capsule is the handoff artifact between agents. It is what makes replay and
  patch workflows deterministic.

## Step 7: Replay The Same Run

```bash
curl -s http://127.0.0.1:8091/replays \
  -X POST \
  -H 'Content-Type: application/json' \
  --data "{\"capsule_id\":\"RUN_ID\"}"
```

Talking points:

- Replay demonstrates deterministic re-execution from the capsule.
- A debugging agent does not need the original planning agent to restate the
  request.

### Real Replay Example

We replayed this capsule:

```text
run_1774397062_5d610364
```

Request:

```bash
curl -s -X POST http://127.0.0.1:8091/replays \
  -H 'Content-Type: application/json' \
  --data '{"capsule_id":"run_1774397062_5d610364"}'
```

Actual replay response:

```json
{
  "mode": "replay",
  "query_id": "group_by_demo",
  "run_id": "replay_1774462867_5a46fdf5",
  "replay_of": "run_1774397062_5d610364",
  "runtime_ms": 1150,
  "status": "SUCCESS"
}
```

Replay result:

```json
{
  "query_id": "group_by_demo",
  "row_count": 3,
  "rows": [
    "{a, 1500000, 100000}",
    "{c, 750000, 100000}",
    "{b, 250000, 50000}"
  ],
  "runtime_ms": 1150,
  "status": "SUCCESS"
}
```

Replay critique:

```json
{
  "global_findings": [
    "latency_above_interactive_budget"
  ],
  "query_id": "group_by_demo",
  "status": "SUCCESS"
}
```

## Step 8: Patch The Run

```bash
curl -s http://127.0.0.1:8091/patches \
  -X POST \
  -H 'Content-Type: application/json' \
  --data @- <<'JSON'
{
  "capsule_id": "RUN_ID",
  "patch": {
    "op": "retune_aggregation",
    "strategy": "partial_final",
    "max_drivers": 4
  }
}
JSON
```

Talking points:

- A downstream agent can ask for a targeted patch rather than resubmitting
  everything.
- This is the bridge from "execution engine" to "agent execution coprocessor."

### Real Patch Example

We applied this simple patch:

```json
{
  "capsule_id": "run_1774397062_5d610364",
  "patch": {
    "op": "retune_aggregation",
    "strategy": "partial_final",
    "max_drivers": 4
  }
}
```

Request:

```bash
curl -s -X POST http://127.0.0.1:8091/patches \
  -H 'Content-Type: application/json' \
  --data '{
    "capsule_id":"run_1774397062_5d610364",
    "patch":{
      "op":"retune_aggregation",
      "strategy":"partial_final",
      "max_drivers":4
    }
  }'
```

Actual patch response:

```json
{
  "mode": "patch",
  "query_id": "group_by_demo_patched",
  "run_id": "patch_1774462867_53cf5a94",
  "base_capsule_id": "run_1774397062_5d610364",
  "runtime_ms": 1071,
  "status": "SUCCESS"
}
```

Patched result:

```json
{
  "query_id": "group_by_demo_patched",
  "row_count": 3,
  "rows": [
    "{a, 1500000, 100000}",
    "{c, 750000, 100000}",
    "{b, 250000, 50000}"
  ],
  "runtime_ms": 1071,
  "status": "SUCCESS"
}
```

Patched critique:

```json
{
  "global_findings": [
    "latency_above_interactive_budget"
  ],
  "query_id": "group_by_demo_patched",
  "status": "SUCCESS"
}
```

The key thing to show live is that the physical plan shape changed. In the
patched capsule, the aggregation became a two-stage plan:

```text
-- TopN[4]...
  -- Aggregation[3][FINAL ...]
    -- Aggregation[2][PARTIAL ...]
      -- Filter[1]...
        -- Values[0]...
```

The patched capsule also records the patch explicitly:

```json
{
  "applied_patch": {
    "max_drivers": 4,
    "op": "retune_aggregation",
    "strategy": "partial_final"
  }
}
```

That is the clearest demo moment for agent workflows:

- result rows stay stable
- execution strategy changes
- the new execution contract is captured in a fresh capsule

## Suggested Live Demo Flow

If you want a tight 3-5 minute demo, use this sequence:

1. show `GET /healthz`
2. show `POST /runs`
3. open `result.json`
4. open `critique.json`
5. explain `capsule.json`
6. show `POST /replays`
7. show `POST /patches`

Use this one-line summary near the end:

> Bolt is no longer just something a human points SQL at. It can now act like a
> machine-consumable execution service for upstream and downstream agents.

## Files Used In This Demo

- [server.py](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime_api/server.py)
- [sample_request.json](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime_api/sample_request.json)
- [AgentRuntimeExecutor.cpp](/Users/bytedance/work/oss/mcheng/bolt/bolt/tool/AgentRuntimeExecutor.cpp)
- [README.md](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime_api/README.md)
