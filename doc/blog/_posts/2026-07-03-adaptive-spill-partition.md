---
layout: post
title: "Adaptive Spill Partitioning: Using Runtime Row Counts to Reduce Recursive HashJoin Spill"
date: 2026-07-03
author: "Bolt Community"
parent: Blog
nav_order: 8
---

When the build side of a HashJoin exceeds its memory budget, the hash table has to be split by hash partition and written to disk. The basic mechanism is straightforward: split build rows into partitions, read partitions back one by one, rebuild the hash table for each partition, and then match it with the probe side.

The hard part is choosing the partition count. If the first spill produces partitions that are still too large, reading back a partition will exceed memory again. The engine then has to repartition and spill that partition again. Data goes through repeated write, read, repartition, and write cycles, which can multiply spill size, I/O, compression work, and hash computation.

Adaptive spill partitioning does not simply make the partition count larger by default. Instead, when the first memory pressure signal appears, it uses runtime row-count information and the observed HashBuild memory state to estimate a better spill partition count. A static configuration problem becomes a runtime decision: how many build rows can the current memory budget approximately hold, how many build rows are expected in total, and therefore how many partitions are needed so that each partition is more likely to fit in memory when restored.

## Why Fixed Partition Counts Amplify Spill

Traditional HashJoin spill is usually controlled by `joinPartitionBits`:

```text
partition_count = 2 ^ joinPartitionBits
```

If `joinPartitionBits = 2`, the first spill creates only 4 partitions. If the build side is much larger than the memory available to the task, each partition can still exceed memory after the first split.

The execution path then looks like this:

```text
Build side rows
      |
      v
HashBuild runs out of memory
      |
      v
Split into 4 partitions with fixed bits and write them to disk
      |
      v
Read partition 0 back
      |
      v
partition 0 still does not fit in memory
      |
      v
Split again into 0-0, 0-1, 0-2, 0-3 and write them to disk
      |
      v
Other partitions repeat the same process
```

The cost of this recursive spill is not just a few extra files. Each spill level adds hash repartitioning, serialization, compression, decompression, reader construction, file metadata management, and CPU work during restore. For a large build side, total spill writes can approach two or three times the original data size, or even more.

The fixed-partition approach lacks two pieces of information:

- How many rows the whole build side contains.
- How many rows the current task was able to process before the first spill.

The first spill point gives the engine the second signal. If the upstream plan can also provide the first signal, the engine can choose a partition count that is much closer to the actual data scale.

## Key Insight: The First Spill Is a Capacity Sample

The first spill is not only a failure signal. It is also a useful capacity sample.

During HashBuild, if memory pressure appears after `processedRowCount` rows have been processed, the engine can treat that point as an approximation of:

```text
How many build rows this HashBuild can hold under the current memory budget
```

If the upstream side also provides the total build-side row count `totalRowCount`, the target partition count can be estimated as:

```text
target_partitions ~= totalRowCount / rows_fit_in_memory
```

Here, `rows_fit_in_memory` does not come from a static knob. It comes from runtime state. The implementation also caps this capacity estimate with the hash table bucket limit, so that the estimated number of rows per partition is not overly optimistic.

For example, suppose the build side is estimated to contain 10 million rows, and the first memory shortage appears around 800 thousand processed rows. With a fixed 4-way split, each partition would still average 2.5 million rows and would likely spill again during restore. Adaptive partitioning tends to raise the partition count toward a size such as 16 or 32, so that each partition is closer to the capacity observed at the first spill point.

This idea has two important boundaries:

- Row count is only a proxy for capacity, not byte size. Wide rows, variable-length fields, and hash table overhead all affect real memory usage.
- More partitions reduce the average partition size, but they do not eliminate hot partitions caused by hash key skew.

For that reason, the mechanism is a conservative enhancement: it only increases partition bits when the estimate is larger than the current configuration. It never lowers the existing partition count.

## How Row Counts Reach HashBuild

Adaptive partitioning depends on a lightweight data contract. An upstream operator exposes total row count and processed row count through runtime metrics. When HashBuild needs a spill decision, it scans preceding operators to find those metrics.

The flow can be simplified as follows:

```text
Spark / Gluten collects or estimates row counts
          |
          v
Substrait / native plan carries a row-count hint
          |
          v
Bolt source or blocking operator writes runtime metrics
          |
          v
OperatorCtx scans backward from the current HashBuild operator
          |
          v
HashBuild reads totalRowCount / processedRowCount
          |
          v
SpillConfig partition bits are updated
          |
          v
Spiller writes partitions with the new hash bit range
```

The relevant runtime metrics have the following meaning:

| Metric | Meaning |
|---|---|
| `kCanUsedToEstimateHashBuildPartitionNum` | This operator has row-count information usable for HashBuild partition estimation |
| `kTotalRowCount` | The total output row count represented by the operator, or an upstream total-row estimate |
| `kHasBeenProcessedRowCount` | The number of rows already emitted or processed toward the downstream operator |

Different operators provide these metrics in different ways:

| Operator Type | Row-Count Source |
|---|---|
| TableScan | Row-count estimate carried by the scan plan; processed rows are updated as output batches are produced |
| ValueStream / shuffle input | Total row count carried by the upstream stream input; processed rows are accumulated as RowVectors are emitted |
| Sort | As a blocking operator, input rows can represent total rows after input is fully consumed; output rows represent current output progress |
| HashAggregation | In non-spill cases, group count and output rows can be used; in spill cases, spill-row statistics can be used |

HashBuild does not depend on a specific source operator. It only needs a preceding operator in the same driver to expose these three metrics. `OperatorCtx` scans backward from the current operator and uses the first provider marked as usable for estimation.

This keeps the row-count path loosely coupled. Gluten, TableScan, shuffle input, and blocking operators can each provide the best row count they know, while HashBuild consumes the same runtime metric contract.

## First Spill: Adjusting `joinPartitionBits`

When HashBuild first fails to reserve enough memory for input, it tries to calculate new join partition bits. The simplified flow is:

```text
HashBuild::ensureInputFits(input)
      |
      v
reserveMemory(input) fails
      |
      v
If this is the first spill:
      |
      +--> Read totalRowCount / processedRowCount from preceding operators
      |
      +--> Record spillThreshold = current RowContainer row count
      |
      +--> Record memoryUsedForFirstSpill = current pool usage
      |
      +--> Estimate the target partition count
      |
      +--> If estimated bits exceed current joinPartitionBits:
              update joinPartitionBits
              resetSpiller()
              setupSpiller()
      |
      v
requestSpill(input)
```

The core computation can be expressed in pseudocode close to the implementation:

```cpp
rowsInMem = rowContainer.numRows();
readRuntimeMetric(totalRowCount, processedRowCount);

spillThreshold = rowsInMem;
memoryUsedForFirstSpill = pool.currentBytes();

if (totalRowCount == 0 || processedRowCount == 0) {
  return unchanged;
}

maxRowsPerPartition =
    min(maxHashTableBucketCount, processedRowCount);

targetPartitions =
    ceil(totalRowCount / maxRowsPerPartition);

estimatedBits = conservativeBitLength(targetPartitions);

if (targetPartitions is close to a small power-of-two boundary) {
  estimatedBits += 1;
}

if (estimatedBits > joinPartitionBits) {
  joinPartitionBits = min(estimatedBits, maxAllowedBits);
}
```

There are several details worth calling out.

First, `processedRowCount` is used as a capacity signal, but not blindly. The implementation takes the smaller value between `processedRowCount` and `maxHashTableBucketCount`, which prevents the estimated per-partition capacity from exceeding a reasonable structural limit of the hash table.

Second, the partition count must eventually be converted into bits because Spiller works with hash bit ranges. The implementation rounds conservatively instead of trying to find the mathematically smallest `2^n`. If the target partition count is already close to the upper bound of a small power-of-two range, it adds one more bit to leave room for skew, row-width changes, and estimation error.

Third, the algorithm only increases bits:

```text
estimatedBits <= current joinPartitionBits
        |
        v
keep the existing configuration
```

This rule matters. The adaptive logic enhances the fixed configuration; it does not replace all of its semantics. If row-count information is missing, the estimate is too small, or the current configuration is already sufficient, HashBuild continues to use the existing bits.

## Restoring a Partition: Adjusting `joinRepartitionBits`

The first-spill decision only answers whether the first split is fine-grained enough. Execution still needs to handle another case: a spilled partition may still be too large when it is restored.

For this reason, when HashBuild sets up the reader for a spilled partition, it also estimates `joinRepartitionBits` for the next repartition level.

The inputs differ between the first spill and the restore path:

| Phase | Total-Size Signal | Capacity Signal | Field Adjusted |
|---|---|---|---|
| First spill | Total build-side row count `totalRowCount` | Current processed row count `processedRowCount` | `joinPartitionBits` |
| Restoring a spilled partition | Maximum row count of the current partition `maxPartitionRowCount` | First-spill threshold `spillThreshold` | `joinRepartitionBits` |

The restore logic can be understood as:

```text
Restoring partition P
      |
      v
P may still contain more rows than the first-spill capacity sample
      |
      v
Estimate next-level partitions with maxPartitionRowCount / spillThreshold
      |
      v
If more bits are needed, increase joinRepartitionBits
```

This path directly targets recursive spill. If the first split is already sufficient, the restored partition does not need to be split again. If a partition is still too large, the next repartition level uses a more suitable number of bits instead of mechanically reusing the initial configuration.

The implementation also uses the remaining hash-bit range as an upper bound to avoid overflowing the partition bit range:

```text
startBit = hash bit offset already consumed by the current partition
maxAllowedBits = 64 - startBit
joinRepartitionBits <= maxAllowedBits
```

`maxSpillLevel` still applies. Once the maximum spill level is exceeded, the system stops continuing recursive spill, which prevents unbounded splitting under extreme data volume or severe skew.

## `SpillConfig` Is Where the Decision Takes Effect

Adaptive partitioning does not bypass Spiller or manipulate files directly. It modifies the partition-bit fields in `SpillConfig`, then lets the existing Spiller operate with the new hash bit range.

The relevant fields are:

| Field | Purpose |
|---|---|
| `startPartitionBit` | Starting hash bit used for spill partitioning |
| `joinPartitionBits` | Partition bits used by the first HashJoin spill |
| `joinRepartitionBits` | Partition bits used for repartition when spill level is greater than 0 |
| `maxSpillLevel` | Maximum allowed level of recursive spill |
| `spillPartitionsAdaptiveThreshold` | Threshold controlling extra conservative growth for small partition counts |

`setupSpiller()` turns these fields into a concrete `HashBitRange`:

```text
First spill:
  [startPartitionBit, startPartitionBit + joinPartitionBits)

Repartition after restore:
  [startBit, startBit + joinRepartitionBits)
```

The adaptive logic only changes how many hash bits are used to split build rows. It does not change join semantics or the basic spill and restore model.

## Already-Spilled Partitions Go Directly to Disk

Adaptive bits work well with another HashBuild spill behavior: once a partition is marked as spilled, new input rows belonging to that partition are written directly to disk instead of being inserted into the in-memory hash table again.

In simplified form:

```text
New input RowVector
      |
      v
Compute partition with the current hash bit range
      |
      +-- partition has not spilled
      |       |
      |       v
      |   insert into in-memory RowContainer / hash table
      |
      +-- partition has spilled
              |
              v
          append directly to the spill file
```

When the partition count is more reasonable, this direct-to-disk routing also becomes finer grained. It reduces the chance that data already known to require disk repeatedly inflates the in-memory hash table, and it lowers the risk of spilling again during restore.

## The Same Row-Count Signal Can Guide Spill Compression

The row-count path is also reused for spill compression decisions.

At the first spill, if the current spill configuration does not explicitly specify a compression algorithm, Bolt estimates the final spill size from current memory usage and the row-count ratio:

```text
estimatedSpillSize =
    currentUsage * totalRowCount / (processedRowCount + 1)
```

It then selects compression by threshold:

| Estimated Spill Size | Compression Strategy |
|---:|---|
| Below the low threshold | Keep the existing configuration, usually without forcing compression |
| At or above the low threshold | Use LZ4, favoring low CPU overhead |
| At or above the high threshold | Use ZSTD, favoring a better compression ratio |

The default thresholds are:

| Threshold | Default |
|---|---:|
| Low compression threshold | 4GB |
| High compression threshold | 20GB |

This makes runtime row count a more general execution signal. At the first sign of memory pressure, the engine not only knows that the current input does not fit; it can also estimate how much data the full build side may spill. Small spill avoids unnecessary compression CPU, while large spill prioritizes reducing I/O.

## Where the Performance Gain Comes From

The performance gain comes mainly from avoiding repeated work, not from making a single spill operation inherently faster.

| Scenario | Typical Behavior With Fixed Bits | Expected Change With Adaptive Bits | Source of Gain |
|---|---|---|---|
| Build side far exceeds memory, hash distribution is relatively even | Multiple recursive spill levels | More likely to finish at the first or a lower spill level | Less repeated writing, reading, and hash repartitioning |
| Default partition count is clearly too small | A restored partition still does not fit | Partition count is raised before the first spill | Fewer rows per partition |
| Large spill with no explicit compression setting | Large amounts of data may be spilled uncompressed | LZ4 or ZSTD is selected based on estimated size | Better tradeoff between CPU and I/O |
| Row-count information is missing, or estimated bits do not exceed current bits | Fixed configuration is used | Behavior is essentially unchanged | Stable fallback path |
| Severe key skew | Hot partitions may still be too large | Only partially mitigated | Still requires a skew-aware mechanism |

The before-and-after execution paths can be summarized as:

```text
Fixed partitioning:

large build side
    |
    v
4 partitions
    |
    +--> P0 too large -> repartition -> spill again
    +--> P1 too large -> repartition -> spill again
    +--> P2 too large -> repartition -> spill again
    +--> P3 too large -> repartition -> spill again

Adaptive partitioning:

large build side
    |
    v
estimate rows per memory budget
    |
    v
more partitions at first spill
    |
    +--> smaller P0 -> restore likely fits
    +--> smaller P1 -> restore likely fits
    +--> smaller P2 -> restore likely fits
    +--> smaller Pn -> restore likely fits
```

The most useful observable signals are:

- Lower `spillLevel`.
- Fewer recursive spill events.
- Less secondary amplification in spill read/write bytes.
- Less cascading growth in spill files or runs.
- `useAdaptiveSpill`, which indicates whether HashBuild actually used adaptive bits.
- In large-spill cases, compression may change from NONE to LZ4 or ZSTD.

These signals are more explanatory than total elapsed time alone. Total runtime is also affected by disk behavior, compression libraries, CPU, data skew, probe-side size, and scheduling concurrency. A reduction in recursive spill depth and repeated I/O directly shows whether the optimization hit the intended problem.

## Correctness and Fallback

Adaptive partitioning does not change HashJoin matching semantics. It only changes how many hash bits are used to split build-side data during spill. Each row still enters a partition based on its hash value, and restore still rebuilds the hash table partition by partition.

When required information is missing, the logic falls back to the existing configuration:

| Condition | Behavior |
|---|---|
| No usable runtime metric is found | Do not adjust bits |
| `totalRowCount` or `processedRowCount` is 0 | Do not adjust bits |
| Estimated bits do not exceed current bits | Keep the current configuration |
| The current task has multiple drivers | Do not use this row-count traversal path |
| Remaining hash bits are insufficient | Clamp to the upper bound or stop increasing bits |
| Maximum spill level is exceeded | Stop continuing recursive spill |
| Spill compression is explicitly configured | Do not override the user-specified compression strategy |

This fallback behavior keeps the optimization additive: it improves execution when enough information is available, and preserves the original behavior when it is not.

## Engineering Tradeoffs

### Row Count Is Not Byte Size

HashBuild memory pressure is fundamentally driven by bytes, hash table overhead, row width, variable-length fields, and allocator behavior. Row count is only a low-cost proxy.

If the first part of the data contains narrow rows but the later part contains many wide strings or complex values, the `processedRowCount` observed at the first spill may overestimate how many rows a partition can hold, and later spill can still happen. Conversely, if the earlier rows are wider, the estimate may be conservative and produce more partitions than necessary.

In this design, being somewhat conservative is usually preferable to being too aggressive. More partitions have management cost, but too few partitions directly cause recursive spill.

### Too Many Partitions Also Cost Something

Increasing bits is not always better. More partitions can introduce:

- More partition buffers.
- More spill runs or file fragments.
- More metadata.
- More small I/O.
- More scheduling and reader-management work during restore.

This is why the implementation has safeguards such as `spillPartitionsAdaptiveThreshold`, the remaining hash-bit upper bound, and `maxSpillLevel`. They keep adaptation within a reasonable range and avoid trading recursive spill for excessive fragmentation.

### Skew Cannot Be Solved Only by Adding Partitions

If join keys are heavily skewed, one hot key can still produce a partition far larger than the average. Increasing the partition count reduces average partition size under a reasonably even distribution, but it cannot split rows for the same hot key by itself.

Those cases need additional skew-aware strategies, such as special handling for hot keys or a join-strategy rewrite at the planning layer. Adaptive spill partitioning reduces recursive spill risk for ordinary large HashJoins, but it is not a complete skew-join solution.

### Multi-Driver Execution Needs More Careful Statistics

The current row-count traversal path only applies to single-driver execution. With multiple drivers, each driver may observe different `processedRowCount`, memory capacity, input splits, and hash distribution. Treating one driver's local progress as global capacity can easily mislead the estimate.

Supporting multiple drivers requires additional per-driver or task-level aggregated statistics, such as:

```text
global total row count
per-driver processed row count
per-driver memory usage
per-driver spill threshold
partition-level row distribution
```

Before that aggregation exists, restricting this path to a single driver is conservative but reasonable.

## When It Applies

This optimization is most useful when:

- The HashJoin build side is much larger than the memory available to a single task.
- The default `joinPartitionBits` is too small and recursive spill is likely.
- Build-key distribution is relatively even, without extreme hot keys.
- The upstream path can provide a credible total row count, and preceding operators can update processed row count continuously.
- Spill I/O or compression cost is already a query bottleneck.

It is less useful, or needs caution, when:

- The build side already fits in memory and does not spill.
- The fixed partition bits are already large enough.
- Row-count estimates are missing or clearly unreliable.
- Row width varies so much that row count is weakly correlated with memory footprint.
- Join keys are severely skewed and hot partitions dominate memory pressure.
- Too many partitions create more small-file and metadata cost than the recursive spill they avoid.

## Summary

Adaptive spill partitioning addresses a specific but expensive HashJoin spill problem: fixed partition counts cannot see the real build-side scale or the task's observed memory capacity, so partitions can remain too large and trigger recursive spill.

The core idea is to treat the first spill as a runtime capacity sample. The engine uses upstream total row count and current processed row count to estimate better partition bits. The first spill adjusts `joinPartitionBits`; restoring a spilled partition uses that partition's row count and the first-spill threshold to adjust `joinRepartitionBits`. The same row-count signal can also estimate spill size and select a better compression strategy for large spill cases.

The goal is not to make HashJoin never spill. It is to reduce unnecessary repeated spill. The design keeps the original configuration as a lower bound, falls back automatically when information is missing, and controls risk with hash-bit limits, spill level, and partition thresholds. For large build sides with undersized fixed partition counts and obvious recursive spill, it moves execution closer to the desired path: split once at a reasonable granularity, restore partitions one by one, and avoid paying for repeated I/O and CPU work.
