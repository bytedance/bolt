---
layout: post
title: "Adaptive Task Parallelism in Bolt"
date: 2026-07-02
author: "Haiyan Gu, Guangxin Wang"
parent: Blog
nav_order: 3
---

Bolt's adaptive task parallelism adjusts task concurrency at stage granularity
based on measured CPU utilization, memory pressure, and input throughput. The
mechanism keeps Spark as the task scheduler and moves the concurrency decision
into the Bolt executor, where task-level runtime statistics are already
available. On a sample of more than 1,000 production Spark on Bolt tasks, the
feature delivered an average end-to-end speedup of 1.27x.
{: .note }

## 1. Background

Spark on Bolt workloads often run with CPU utilization in the 50%-55% range.
The main limiter is usually not compute itself, but time spent outside CPU,
primarily in I/O and memory access. Increasing task concurrency is a direct way
to raise CPU utilization, but a single static concurrency value is a poor fit
across stages and queries: some stages are memory-bound, some are I/O-bound,
and only some benefit from higher concurrency at all.

Adaptive task parallelism addresses this by deciding concurrency during stage
execution, using signals that the executor can observe locally at low cost. The
design keeps two constraints from the existing execution model:

- Spark remains responsible for task scheduling. Bolt does not take over Spark
  scheduling semantics.
- Stages that are not safe to adjust, such as memory-tight or spill-prone
  stages, stay at the default concurrency.

## 2. Design Choice

Two directions were considered.

The first was Bolt-side self-scheduling. Spark would hand Bolt more tasks than
the configured concurrency, and Bolt would hold the excess tasks in a local
queue, starting them based on runtime feedback.

The second was to let Bolt decide concurrency while Spark continues to schedule
tasks. Bolt reports the current target concurrency back through task metrics,
and the Spark driver adjusts subsequent task dispatch accordingly.

The current implementation takes the second path. This avoids changing Spark
 task lifecycle semantics, including task timing, speculative execution, and UI
metrics, while still allowing the concurrency decision to use fine-grained
runtime statistics inside the Bolt executor. Bolt-side self-scheduling remains
available as an experimental option, but it is not the default path.

![Bolt decides concurrency, Spark schedules tasks]({{ site.baseurl }}/assets/images/adaptive-task-parallelism-flow.svg)

## 3. Execution Path

The core logic lives in `ExecutorTaskScheduler`, implemented in
`bolt/exec/ExecutorTaskScheduler.{h,cpp}`. It is a process-scope singleton:
one instance per Bolt executor process, shared by tasks running concurrently in
that executor. It owns the state machine, the sampling window, and the
concurrency decision.

Bolt tasks connect to the scheduler through two points:

- `Task::create` reads the current target concurrency when dynamic concurrency
  adjustment is enabled.
- `Task::terminate` reports runtime statistics to the scheduler and triggers
  `scheduleNewTasksIfAny`, which may call `decideConcurrencyLocked()` when a
  decision is due.

### 3.1 State Machine

The scheduler maintains four states per stage:

- `kInit`: the initial state, before any task has been observed for decision
  making.
- `kSampling`: finished-task statistics are collected. Once the number of
  tracked tasks reaches `numTrackingTaskThreshold_`, the scheduler computes a
  candidate concurrency.
- `kRevising`: the candidate concurrency has been applied, and subsequent task
  results are observed to confirm that throughput does not regress.
- `kCompleted`: the concurrency decision for the current stage is stable, and
  later task completions no longer change it.

```cpp
enum SchedulerState { kInit, kSampling, kRevising, kCompleted };
```

![ExecutorTaskScheduler state machine]({{ site.baseurl }}/assets/images/adaptive-task-parallelism-state.svg)

Before the scheduler reaches `kCompleted`, two conditions send the stage back to
a safe baseline:

- Memory reclaim was observed during sampling. The scheduler treats the stage
  as close to a memory limit and falls back to `defaultConcurrency_`.
- Throughput regressed after the adjustment. The scheduler also falls back to
  `defaultConcurrency_` in that case.

### 3.2 Concurrency Decision

`decideConcurrencyLocked()` uses three signals: CPU utilization, memory
headroom, and input throughput. The core estimate considers both CPU headroom
and memory headroom, and applies a cap to keep a single adjustment from being
too aggressive:

```cpp
double estimatedResourceMul = estimatedMulOnResource();
int32_t newConcurrency = std::min(
    (int32_t)std::round(estimatedResourceMul * defaultConcurrency_),
    3 * defaultConcurrency_);
```

`estimatedMulOnResource()` takes the more conservative value between a
CPU-target-to-current-utilization ratio and a memory-headroom ratio, so the
final adjustment is bounded by whichever resource is tighter. In the current
implementation, the cap is `3 * defaultConcurrency_`. When task concurrency is
adjusted, the scheduler also resizes the I/O executor thread pool through
`ioExecutor_->setNumThreads`, so I/O parallelism stays aligned with task
concurrency.

### 3.3 Task Lifecycle Integration

On task creation, the scheduler exposes the current target concurrency. On task
termination, `Task::terminate` calls
`ExecutorTaskScheduler::scheduleNewTasksIfAny`, which updates runtime
statistics, advances the state machine, and invokes
`decideConcurrencyLocked()` when required.

The current concurrency and a monotonically increasing version are returned to
Spark through task metrics. The version exists to handle out-of-order reports:
if metrics from different tasks reach the driver in different orders, the driver
can ignore stale values and avoid switching concurrency based on old decisions.

## 4. Configuration and Observability

The feature is mainly controlled by two configuration items:

| Configuration | Default | Description |
| --- | --- | --- |
| `spark.gluten.sql.columnar.backend.bolt.dynamicConcurrencyAdjustment.enabled` | `false` | Master switch for adaptive task parallelism. |
| `spark.gluten.sql.columnar.backend.bolt.boltTaskScheduling.enabled` | `false` | Switch for Bolt-side self-scheduling. When enabled, Bolt maintains a local task queue. Default is off. |

Another option,
`spark.gluten.sql.columnar.backend.bolt.dynamicDefaultConcurrency`, supplies the
initial default concurrency used as the baseline for later adjustment.

The scheduler reports current concurrency as a runtime stat named
`dynamicConcurrency`. It is currently attached to source operators such as
`TableScan` and `ValueStream`:

```cpp
lockedStats->addRuntimeStat(
    "dynamicConcurrency", RuntimeCounter(this->getConcurrency()));
```

A companion adjustment counter is used as a version indicator rather than a
duration or data-size metric. In practice, both the current concurrency and its
version need to be read together: the former shows what the executor is using,
and the latter shows whether that value comes from the latest decision.

## 5. Performance Results

Adaptive task parallelism was evaluated on a set of production Spark on Bolt
tasks. The sample was filtered by CPU and memory headroom criteria and covered
more than 1,000 online tasks, with an average end-to-end speedup of 1.27x.

This result matches the intended behavior. Concurrency is raised only when the
executor's runtime signals indicate remaining headroom. For stages with higher
memory pressure, stages that observed memory reclaim during sampling, or stages
where throughput did not improve after adjustment, the scheduler keeps or
returns to the default concurrency.

## 6. Summary

Adaptive task parallelism does not change Spark's scheduling model. It moves the
concurrency decision into the Bolt executor, where task-level runtime
statistics are already available. `ExecutorTaskScheduler` samples, decides, and
revises at stage granularity; once memory pressure or throughput regression is
observed, it returns to the default concurrency. The reported concurrency is
versioned so that the Spark driver can adopt updates safely.

The goal is straightforward: raise concurrency for stages with low CPU
utilization and remaining memory headroom, and stay with the default behavior
for stages where the gain is uncertain or the memory risk is higher.
