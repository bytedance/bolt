---
layout: post
title: "From Correctness to Utilization: Moving Memory Management into Bolt and Calibrating Quota with RSS"
date: 2026-07-03
author: "Bolt Community"
parent: Blog
nav_order: 6
---

The first goal of moving Bolt memory management into Bolt was not performance, but correctness: reserve, repay, spill, and OOM decisions for off-heap memory had to move out of the cross-language path between Spark/Gluten and Bolt, and converge into one closed loop on the Bolt side.

After this loop was in place, experiments exposed another problem: the logical Quota was already exhausted, while the process RSS was still clearly below the requested off-heap budget. In other words, the system became tight on accounted memory too early, but physical resident memory had not actually been filled. RSS-based Quota calibration was designed in this context. It does not break the real memory budget of the container or process. Instead, it uses RSS feedback to correct an overly conservative logical Quota, so the execution path falls into fewer ineffective spills and premature OOMs.

## The Problem Started with Correctness

The memory management path in Spark on Gluten originally crossed both JVM and C++ sides. Bolt operators allocated memory on the C++ side, while memory accounting and task-level arbitration crossed JNI and returned to Gluten/Spark through `MemoryTarget`, `MemoryConsumer`, and `TaskMemoryManager`.

Abstracted as one path, it looked roughly like this:

```text
Bolt / Velox operator requests memory
        |
        v
Bolt MemoryPool / Arbitrator
        |
        v
JNI AllocationListener
        |
        v
Gluten MemoryTarget.borrow()
        |
        v
Spark TaskMemoryManager.acquireExecutionMemory()
        |
        v
Spark ExecutionMemoryPool
        |
        +-- grant: increase logical accounting
        |
        +-- not enough: trigger spill or throw OOM
```

This path worked, but it carried several engineering risks:

1. **Distributed state**: the Bolt side knew the concrete `MemoryPool`, operator, and spill state, while the Spark side owned task-level Quota. The two sides were synchronized through a JNI listener.
2. **Unsafe failure paths**: allocation failure, spill failure, OOM, and cross-language exceptions could all interrupt the control flow. If one layer had already updated accounting while another layer did not fully roll it back, memory accounting could become inconsistent.
3. **Mixed failure semantics**: APIs such as `maybeReserve` are better suited to returning success or failure, but in the old path some failures propagated as exceptions. Once exceptions are used as normal control flow, keeping memory accounting symmetric on every path becomes difficult.
4. **Spill decisions were too far from execution state**: the Bolt execution side is the component that really knows which Bolt pools can shrink and which operators can spill. When task-level management lives on the JVM side, the decision path becomes longer and harder to keep consistent.

Therefore, the core goal of moving memory management into Bolt was to let Bolt own the task-level memory management structures and put memory allocation, release, spill, shrink, and OOM decisions into the same C++ control flow.

## The Closed Loop After the Move

After the move, the Bolt side added several abstractions similar to Spark execution memory:

| Module | Responsibility |
|---|---|
| `ExecutionMemoryPool` | Executor-level off-heap Quota pool that allocates execution memory by active task count |
| `TaskMemoryManager` | Task-level entry point for execution memory allocation; triggers spill for the current task when Quota is insufficient |
| `MemoryConsumer` / `ConsumerTargetBridge` | Connects Spark-style consumers with the Gluten-style `MemoryTarget` tree |
| `TreeMemoryTargetNode` | Maintains hierarchical memory accounting, child nodes, spillers, and virtual statistic nodes |
| `ManagedAllocationListener` | Receives capacity changes from the Bolt allocator / arbitrator and calls `borrow/repay` |
| `ListenableArbitrator` | Notifies the listener when Bolt `MemoryPool` grows or shrinks, synchronizing task-level Quota |
| `BoltMemoryManagerHolder` | Holds the Bolt `MemoryManager`, Arrow pool, listener, target, and spiller in one place |

The overall structure can be simplified as:

```text
                         executor level
              +------------------------------+
              |      ExecutionMemoryPool     |
              |  poolSize / active tasks /   |
              |  dynamic quota extension     |
              +---------------+--------------+
                              |
                              v
                           task level
              +------------------------------+
              |       TaskMemoryManager      |
              |  acquire -> spill -> retry   |
              +---------------+--------------+
                              |
                              v
                          target tree
              +------------------------------+
              |     ConsumerTargetBridge     |
              |       root TreeTarget        |
              +---------------+--------------+
                              |
               +--------------+--------------+
               |                             |
               v                             v
       +---------------+             +---------------+
       | normal target |             | over-acquire  |
       |   operator    |             | backup target |
       +-------+-------+             +---------------+
               |
               v
       +----------------+
       | SpillTrigger   |
       | shrink / spill |
       +----------------+
```

The grow path of Bolt `MemoryPool` also became:

```text
operator allocate / maybeReserve
        |
        v
MemoryPoolForGluten::maybeReserve()
        |
        v
ListenableArbitrator::growCapacity()
        |
        v
ManagedAllocationListener::reserve()
        |
        v
MemoryTarget.borrow()
        |
        v
TaskMemoryManager.acquireExecutionMemory()
        |
        +-- ExecutionMemoryPool grant
        |
        +-- insufficient grant:
              SpillTrigger shrinks first, then spills
              reacquire after memory is released
              if still insufficient, return actual granted bytes
```

The most important change here is not the class names, but the failure semantics: `ManagedAllocationListener::reserve()` directly returns the granted bytes, and allocation failure no longer depends on exceptions as normal control flow. This lets the upper-level `maybeReserve()` decide whether to continue based on the return value, instead of compensating memory accounting during exception unwinding.

At the same time, when `TaskMemoryManager` is destructed, it releases all remaining Quota for that task from `ExecutionMemoryPool`; when `MemoryConsumer` is destructed, it checks whether its own `used_` has returned to zero; when `BoltMemoryManager` is destroyed, it checks whether the Bolt pool, Arrow pool, and retained child pools still have unreleased memory. These checks are not performance optimizations. They make it observable and diagnosable whether memory accounting is closed.

## Why Preserve Spark's 1/N and 1/2N Policy

If Quota were implemented as a simple global counter, tasks that start earlier could easily grab a large amount of memory, while later tasks would spill frequently. Spark's execution memory design has a key constraint: when there are `N` active tasks in an executor, each task should have a chance to obtain about `1/2N` of the memory before spilling, and usually no more than `1/N`.

Bolt's `ExecutionMemoryPool` preserves this idea:

```text
active task count = N

maxMemoryPerTask = poolSize / N
minMemoryPerTask = poolSize / (2N)

If the current task has not reached minMemoryPerTask,
and the pool does not have enough free memory yet,
wait for other tasks to release memory instead of failing immediately.
```

This policy addresses fairness and stability, not maximum throughput for a single task. It prevents a task from consuming too much Quota just because it started earlier, and it also reduces sharp oscillations when the number of active tasks changes.

Engineering-wise, a wait timeout was also added. Spark's original implementation can wait without a timeout, but in a cross-component scenario, if another lock blocks the release path while a task is waiting, a deadlock may form. Bolt adds an upper bound to the minimum-memory wait. After timeout, it returns failure and lets the upper layer proceed to spill or OOM.

## The Second Problem Exposed by Experiments: Quota Is Not RSS

Moving memory management into Bolt solved the question of whether accounting was correct, but experiments revealed another phenomenon: logical Quota looked tight, while RSS was far below the off-heap budget requested by the process.

A typical case was that the logical memory usage of a task was already close to 1.4 GB, but the process RSS at failure time was only about 1.09 GB. After explicitly writing through allocated memory, RSS for similar tasks increased noticeably. This shows that at least part of the logical allocation did not translate into an equal amount of resident physical pages.

This is not rare. In off-heap memory management, there are at least three kinds of gaps:

1. **Allocated but untouched pages**: virtual address space or allocator accounting has increased, but physical pages may not have faulted into RSS yet.
2. **Reserved capacity that does not carry data**: to avoid frequent grow operations, `MemoryPool` reserves capacity by granularity. Reservation is not the same as real data usage.
3. **Capacity slack in operator internals**: structures such as hash tables, row containers, and vector buffers may hold extra capacity because of their growth strategies.

After marking and checking a batch of allocation sites on release, the memory that was not really used was clearly not a single-site problem:

| Observation | Value |
|---|---:|
| Allocation call sites with potential waste | 785 |
| Estimated total waste | 479 MB |
| Waste from top 10 call sites | 80 MB |
| Waste from top 50 call sites | 268 MB |
| Waste from top 100 call sites | 389 MB |
| Maximum waste from one call site | 8 MB |
| Minimum waste from one call site | 61 B |

This shows that fixing one operator or one allocation site cannot systematically solve low utilization. A more stable approach is to acknowledge that logical Quota and RSS have a mapping ratio, then dynamically calibrate that ratio while the task is running.

## RSS Quota Calibration Is Not Memory Overcommit

First, three concepts need to be separated:

| Concept | Meaning |
|---|---|
| `PoolSize` | The real off-heap budget configured by the user or executor |
| `Quota` | The limit used by Bolt memory management for logical allocation and arbitration |
| `RSS` | The current resident memory of the process, reported by the operating system |

Initially, `Quota = PoolSize`. But if off-heap RSS is only 6 GB when logical allocation reaches 10 GB, using 10 GB as a hard Quota will trigger spill or OOM too early. RSS Quota calibration increases the logical Quota without breaking the real memory budget, so RSS can move closer to `PoolSize`.

Therefore, the more accurate name is Quota overcommit, not memory overcommit:

```text
Memory overcommit:
  actual RSS exceeds the physical memory requested or allowed by the container

Quota overcommit:
  logical Quota is larger than the configured PoolSize,
  in order to offset gaps between logical allocation and actual RSS,
  so actual RSS approaches but does not exceed PoolSize
```

The core RSS calibration decision can be written as:

```text
countUsed = logical Quota already granted by ExecutionMemoryPool
rss       = max(32 MB, processRSS - onHeapRSS)

mapping ratio = rss / countUsed
gap ratio     = countUsed / rss
```

If `rss / countUsed` is clearly low, the logical Quota contains gaps. If `rss` is already close to or above `PoolSize`, Quota must not be expanded further.

## Why Not PID or eBPF

This problem can be framed as feedback control: runtime observes RSS continuously and adjusts Quota so RSS approaches `PoolSize`. The most direct idea is PID or PI control, but it has two obvious problems:

1. Parameters are hard to tune. Spark SQL workloads vary widely, and one set of control parameters is hard to fit scan, aggregation, join, shuffle, writer, and other scenarios at the same time.
2. Sampling cost is high. If RSS is read on every memory allocation, operating-system queries and control logic enter a high-frequency path.

An eBPF-based approach is theoretically more precise, because it can observe physical page allocation from the kernel side. But it is also more expensive: it depends on kernel capabilities, is more complex to deploy, and if the execution model expands from a single thread to more threads in the future, attribution becomes harder.

The final choice is repeated proportional calibration: the logic is simple, the sampling frequency is controllable, and problems are easier to trace.

## How Repeated Proportional Calibration Works

Dynamic Quota calibration is triggered in `ExecutionMemoryPool::acquireMemory()`. It does not read RSS on every allocation. Instead, it attempts sampling under two conditions:

1. The current request cannot obtain enough Quota.
2. Dynamic Quota has already been triggered, and cumulative Quota growth has exceeded the sampling threshold.

The simplified decision flow is:

```text
memory request
  |
  v
try to grant by the 1/N policy in ExecutionMemoryPool
  |
  +-- grant is enough -> return
  |
  +-- grant is insufficient or sampling threshold is reached
          |
          v
      read process RSS and on-heap usage
          |
          v
      offHeapRSS = max(32 MB, processRSS - onHeapRSS)
          |
          v
      check whether the mapping ratio is in a reasonable range
          |
          +-- reasonable -> do not adjust Quota
          |
          +-- unreasonable
                  |
                  v
          ratio = clamp(countUsed / offHeapRSS,
                        extendMinRatio,
                        extendMaxRatio)
                  |
                  v
          poolExtendSize = PoolSize * (ratio - 1) * scale
                  |
                  v
          if the change exceeds the threshold, update logical Quota
```

The default parameters can be summarized as:

| Parameter | Default | Purpose |
|---|---:|---|
| `quotaTriggerRatio` | 0.5 | Start observing only after Quota usage reaches a certain ratio, avoiding inaccurate early sampling |
| `rssMinRatio` | 0.9 | If RSS / logical usage is below this value, treat it as a clear gap |
| `rssMaxRatio` | 1.0 | Once RSS is close to `PoolSize`, stop expanding further |
| `extendMinRatio` | 1.0 | Lower bound for Quota expansion; 1.0 means no expansion |
| `extendMaxRatio` | 6.0 | Upper bound for Quota expansion, preventing aggressive estimation |
| `extendScaleRatio` | 1.0 | Scales the computed expansion size |
| `sampleRatio` | 0.05 | Allow another sample after cumulative Quota growth exceeds 5% of `PoolSize` |
| `changeThresholdRatio` | 0.0 | Controls how much the new extension must differ from the old one before updating |
| `logPrintFreq` | 0.05 | Controls log frequency so logging itself does not become overhead |

This algorithm is not a one-shot decision. The first sample may be affected by the current operator, input batch, or allocator state. Therefore, if Quota later grows by a certain amount again, the algorithm samples again and corrects `poolExtendSize`.

## Key Safeguards: Prevent Calibration from Polluting the Signal

RSS calibration sounds straightforward, but several engineering details determine whether it remains stable.

First, when dynamic Quota is enabled, `MemoryPoolForGluten::maybeReserve()` releases unused reservation:

```text
maybeReserve(size)
  -> reserve upward by 8 MB granularity
  -> if dynamic Quota is enabled:
       releaseThreadSafe(0, false)
       release capacity that was reserved but not used
```

This prevents reserved-but-unused Quota from being mistaken as real logical usage, which would overestimate the gap ratio.

Second, RSS is a process-level metric, so on-heap usage must be subtracted:

```text
offHeapRSS = processRSS - onHeapRSS
```

If the on-heap estimate is abnormally large, the code falls back to a 32 MB lower bound to avoid a negative or excessively small denominator.

Third, some critical memory-pressure paths temporarily disable dynamic Quota. For example, when `HashBuild` performs extra admission reservation before probe, it uses a scoped guard to disable dynamic expansion, preventing Quota from being expanded further just because the code is checking memory pressure. This prevents the control logic from chasing itself.

Fourth, when dynamic Quota is triggered, it records `borrowFromRssWatermarkBytes`. Later, when `HashBuild` performs probe admission, it considers this watermark together with the reclaim watermark:

```text
admissionWatermark =
  min(non-zero reclaimWatermark,
      non-zero borrowFromRssWatermark,
      configuredTaskQuota fallback)
```

The meaning is that the build phase can usually still spill, while the probe phase has less room for remediation. If RSS calibration has already shown that this task encountered memory pressure, memory should be reserved earlier and more conservatively before probe.

## Where the Performance Gains Come From

RSS Quota calibration does not directly optimize operator algorithms. Its benefits mainly come from three places:

1. **Fewer premature spills**: logical Quota is no longer exhausted too early because of untouched pages or reservation gaps.
2. **Fewer ineffective shrink/spill loops**: when some spills can release very few bytes or even 0 bytes, forcing more spill work is not useful.
3. **Higher actual memory utilization**: RSS moves closer to the requested off-heap budget, so the same resources can hold more intermediate state.

In a replay of tasks that spilled while memory utilization was low, the overall trend was:

| Metric | Median Change | Notes |
|---|---:|---|
| Peak memory utilization | +20% | Original changes in measurable samples were 9%, 11%, 17%, 31%, 34%, and 47% |
| Spill rows | -20% | Some cases dropped significantly, while some were almost unchanged |
| Spill time | -30% | Generally consistent with the trend in spill rows |
| Spill bytes | -20% | Generally consistent with the trend in spill rows |

In another double-run experiment, after excluding failed groups and all-zero groups, there were 12 valid groups:

| Metric | Result |
|---|---:|
| Total spill rows | Down 24% |
| Total spill time | Down 16% |

This result better reflects the two-stage benefit: moving memory management into Bolt first reduced failures caused by correctness issues; RSS calibration then improved spill behavior in low-utilization scenarios.

## Costs and Boundaries

This design has clear boundaries.

First, RSS is a process-level metric, not a task-level metric. The current implementation approximates the mapping using task logical usage and process off-heap RSS, so it is suitable when workloads inside an executor are relatively homogeneous and the main pressure comes from off-heap memory. If on-heap usage fluctuates heavily, or if memory behavior differs greatly across tasks, the mapping ratio becomes coarse.

Second, sampling timing affects the result. If sampling happens too early, many pages may not have been touched yet and the gap ratio may be overestimated. If sampling happens too late, the task may already have entered a spill or OOM path. `quotaTriggerRatio` and `sampleRatio` are used to trade off accuracy and overhead.

Third, Quota expansion is capped, but it is still a statistical strategy. The default `extendMaxRatio = 6.0` prevents one estimate from expanding Quota too aggressively. Repeated sampling and the change threshold can reduce oscillation. However, if the RSS signal itself is unstable, expansion may still be insufficient or excessive.

Fourth, this does not replace operator-level optimization. If spills are caused by too few hash partitions, data skew, recursive spill, insufficient probe admission, or an operator that cannot spill effectively, RSS calibration can only mitigate part of the problem and cannot remove the root cause.

Fifth, it does not break the real memory budget. The goal is to make RSS approach `PoolSize`, not exceed it. In production, cgroup kills, RSS peaks, spill bytes, spill time, and task failure rate still need to be monitored.

The suitable scenarios can be summarized as:

| Scenario | Suitable? |
|---|---|
| High logical Quota, low RSS, frequent spill | Yes |
| High logical Quota, low RSS, OOM after reducing memory | Yes |
| The root cause is accounting error from the cross-language exception path | Move memory management into Bolt first |
| RSS is already close to `PoolSize` | Quota should not be expanded further |
| Spill comes from severe data skew or an unreasonable partitioning strategy | Needs operator or partitioning optimization as well |
| RSS is dominated by on-heap fluctuations | Tune and observe carefully |

## Summary

This optimization can be understood in two layers.

The first layer is correctness: moving Bolt memory management into Bolt converges the memory accounting path that used to be scattered across Spark, Gluten, JNI, and Bolt onto the C++ side. Reserve, repay, spill, shrink, and OOM now go through one rollback-friendly and observable control flow. This solves the question of whether accounting can become wrong.

The second layer is utilization: after the correctness loop was established, experiments found a systematic gap between logical Quota and RSS. RSS-based Quota calibration does not allow real memory overuse. It uses actual resident memory to correct logical Quota, so requested resources can actually be used by the execution path. This solves the question of whether the accounting, although correct, is too conservative.

The relationship between the two should not be reversed. Without the correctness loop after moving memory management into Bolt, RSS calibration would be hard to land stably. Without RSS calibration, the moved memory management would be more correct, but could still spill too early in low-RSS, high-Quota scenarios. The final benefit comes from the combination of both stages: first make memory accounting correct, then make that accounting better reflect real physical memory usage.
