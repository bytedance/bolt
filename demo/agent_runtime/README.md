# Bolt Agent Runtime Demo

This demo shows a workable version of the agent-runtime concept described in:

- [`doc/ai-ready-execution-engine.md`](/Users/bytedance/work/oss/mcheng/bolt/doc/ai-ready-execution-engine.md)
- [`doc/ai-ready-execution-engine.zh.md`](/Users/bytedance/work/oss/mcheng/bolt/doc/ai-ready-execution-engine.zh.md)

It simulates a Bolt agent-facing execution loop with three commands:

- `run`: execute a plan and emit `result.json`, `critique.json`, and `capsule.json`
- `replay`: replay the captured capsule deterministically
- `patch`: apply a plan patch to the captured capsule and rerun

The demo is intentionally lightweight and uses only the Python standard library.
It does not require building Bolt.

## Files

- [`scripts/bolt_agent_demo.py`](/Users/bytedance/work/oss/mcheng/bolt/scripts/bolt_agent_demo.py): runnable CLI
- [`demo/agent_runtime/sample_plan.json`](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime/sample_plan.json): sample execution request
- [`demo/agent_runtime/sample_patch.json`](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime/sample_patch.json): sample optimization patch
- [`demo/agent_runtime/outputs/`](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime/outputs): generated outputs

## Quick Start

Run the original plan:

```bash
python3 scripts/bolt_agent_demo.py run \
  --plan demo/agent_runtime/sample_plan.json \
  --goal minimize_latency \
  --result-out demo/agent_runtime/outputs/result.json \
  --critique-out demo/agent_runtime/outputs/critique.json \
  --capsule-out demo/agent_runtime/outputs/capsule.json
```

Replay the captured run:

```bash
python3 scripts/bolt_agent_demo.py replay \
  --capsule demo/agent_runtime/outputs/capsule.json \
  --result-out demo/agent_runtime/outputs/replay_result.json \
  --trace-out demo/agent_runtime/outputs/replay_trace.json
```

Apply a patch that turns the aggregation into a two-phase aggregation with
higher partition count:

```bash
python3 scripts/bolt_agent_demo.py patch \
  --capsule demo/agent_runtime/outputs/capsule.json \
  --patch demo/agent_runtime/sample_patch.json \
  --result-out demo/agent_runtime/outputs/patched_result.json \
  --critique-out demo/agent_runtime/outputs/patched_critique.json \
  --capsule-out demo/agent_runtime/outputs/patched_capsule.json
```

## What To Look At

The interesting artifacts are:

- [`demo/agent_runtime/outputs/critique.json`](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime/outputs/critique.json):
  operator findings, global findings, and recommended next actions
- [`demo/agent_runtime/outputs/capsule.json`](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime/outputs/capsule.json):
  replayable runtime state with redacted connector properties
- [`demo/agent_runtime/outputs/patched_critique.json`](/Users/bytedance/work/oss/mcheng/bolt/demo/agent_runtime/outputs/patched_critique.json):
  the before/after effect of the patch

## Demo Model

This demo does not execute real Bolt operators. Instead, it:

1. Reads a simplified plan JSON
2. Simulates operator metrics such as memory, runtime, spill, and shuffle
3. Lifts these metrics into an agent-friendly critique
4. Emits a replay capsule that can be rerun or patched

That keeps the demo runnable today while staying close to the real Bolt design:

- plan ingestion
- operator-level trace summary
- structured critique
- replay capsule
- patch-based optimization loop
