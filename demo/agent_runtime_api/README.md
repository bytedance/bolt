# Bolt Agent Runtime API

This standalone API is now backed by a real Bolt execution adapter instead of
the earlier simulated runtime.

The API server calls a small C++ executor binary:

- [`bolt/tool/AgentRuntimeExecutor.cpp`](/Users/bytedance/work/oss/mcheng/bolt/bolt/tool/AgentRuntimeExecutor.cpp)

That binary builds a real Bolt plan using `PlanBuilder`, executes it through
Bolt's query runtime, enables Bolt trace capture, and returns the result rows
plus trace metadata. The Python API then packages that into:

- `result.json`
- `critique.json`
- `capsule.json`

## What The Real Adapter Supports

The adapter intentionally starts with a narrow but real subset:

- inline `Values` input
- optional `filter`
- optional `project`
- optional `aggregation`
- optional `order_by`
- optional `limit`

This is enough to demonstrate a true agent loop on top of Bolt:

- submit execution
- inspect real Bolt traces
- replay the same request
- patch aggregation strategy and rerun

## Build The Adapter

Build the new example target:

```bash
cmake --build _build/Release/_build/Release --target bolt_agent_runtime_executor -j 8
```

If your local build output path differs, pass it explicitly to the server using
`--adapter-bin`.

## Start The Server

```bash
python3 demo/agent_runtime_api/server.py \
  --host 127.0.0.1 \
  --port 8088 \
  --adapter-bin _build/Release/_build/Release/bolt/bolt_agent_runtime_executor
```

## Request Shape

Example request:

```json
{
  "query_id": "group_by_demo",
  "goal": "minimize_latency",
  "repeat_times": 50000,
  "max_drivers": 1,
  "input": {
    "schema": [
      {"name": "category", "type": "VARCHAR"},
      {"name": "value", "type": "BIGINT"}
    ],
    "rows": [
      ["a", 10],
      ["a", 20],
      ["b", 5],
      ["c", 7],
      ["c", 8],
      ["d", 1]
    ]
  },
  "plan": {
    "filter": "value >= 5",
    "aggregation": {
      "group_by": ["category"],
      "aggregates": [
        "sum(value) AS total_value",
        "count(*) AS row_count"
      ],
      "strategy": "single"
    },
    "order_by": ["total_value DESC"],
    "limit": {
      "offset": 0,
      "count": 10
    }
  }
}
```

## Endpoints

- `GET /healthz`
- `POST /runs`
- `POST /replays`
- `POST /patches`
- `GET /runs/{run_id}`
- `GET /capsules/{capsule_id}`
- `GET /artifacts/{run_id}/{artifact_name}`

Supported artifact names are:

- `request`
- `result`
- `critique`
- `capsule`

## Create A Run

```bash
curl -s http://127.0.0.1:8088/runs \
  -X POST \
  -H 'Content-Type: application/json' \
  --data @demo/agent_runtime_api/sample_request.json
```

## Replay A Run

Replace `RUN_ID` with the run id returned by `POST /runs`.

```bash
curl -s http://127.0.0.1:8088/replays \
  -X POST \
  -H 'Content-Type: application/json' \
  --data "{\"capsule_id\":\"RUN_ID\"}"
```

## Apply A Patch

Replace `RUN_ID` with the run id returned by `POST /runs`.

```bash
curl -s http://127.0.0.1:8088/patches \
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

## Storage Layout

The standalone project persists state under:

- [`demo/agent_runtime_api/data/`](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime_api/data)

Each run stores:

- `request.json`
- `result.json`
- `critique.json`
- `capsule.json`
- `adapter_request.json`
- `trace/`

## Notes

This is real Bolt execution now, but it is still a demo-oriented API:

- inline input only
- no auth
- local filesystem storage only
- no scheduler or queue
- patch support is intentionally narrow

That said, the execution path is no longer synthetic. The critique and replay
artifacts are built from actual Bolt runs and actual Bolt trace files.
