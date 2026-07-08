---
layout: post
title: "Row-Based Spill: Aligning the On-Disk Format with the Execution Format"
date: 2026-07-03
author: "Bolt Community"
parent: Blog
nav_order: 9
---

The goal of row-based spill is straightforward: when an operator's primary execution state is already row-oriented, it should not temporarily convert that state back to a columnar format just to spill. Instead, rows in RowContainer are written to disk in a format that stays close to their in-memory layout. This reduces row-column conversion, columnar serialization, multi-column random access, and intermediate object construction during restore.

This is not as simple as changing the spill file format from columnar to row-oriented. Spill sits between memory management, operator state, serialization, sorted merge, and aggregate function interfaces. If any one part is handled poorly, the saved conversion cost can move into restore, comparison, or accumulator handling. A practical row-based spill design needs to answer three questions:

- How can in-memory rows be written to disk safely, especially variable-width data such as strings and complex types?
- How can RowContainer-based operators such as HashBuild and HashAggregation reuse the same spill capability?
- After faster writes, can restore and merge still preserve batch processing instead of falling back to row-at-a-time interpretation?

## Spill Bottlenecks Are Not Just I/O

In large-scale execution engines, spill is often understood as an I/O problem: memory is insufficient, data is written to disk, and the engine reads it back later. In a vectorized execution engine, however, the expensive part is often not the disk write itself, but the data-shape conversion before and after that write.

Take operators that use RowContainer to maintain state. Their in-memory working state is usually row-oriented:

- HashBuild organizes the build side into RowContainer and a hash table so the probe phase can access rows randomly.
- HashAggregation stores group keys and accumulators in RowContainer and continuously updates aggregate state.
- Operators such as Sort and Window also organize data into row formats at certain stages for comparison, sorting, or window computation.

If the spill framework only supports columnar files, the execution path has to convert formats back and forth:

```text
input RowVector
    |
    v
operator-internal RowContainer
    |
    | memory pressure, spill required
    v
temporary RowVector / columnar batches
    |
    v
columnar serialization and disk write
    |
    | restore
    v
read RowVector
    |
    v
rebuild RowContainer
```

The problem with this path is not that columnar format is bad. Columnar layout is a good fit for scan, projection, filter, and batched expression evaluation. The problem is that when both the input and output sides of spill are already row-oriented, forcing a columnar intermediate format introduces extra work:

- Rows in RowContainer must be split back into multiple vectors.
- Each column needs to gather and copy cells according to partition indexes.
- Variable-width strings and complex types go through additional serialization paths.
- During restore, RowVector data has to be written back into RowContainer.
- After multi-way merge, if rows can only be passed to aggregate functions one at a time, the path creates many virtual calls and SelectivityVector construction costs.

The HashBuild spill flame graph shows this directly: hot spots concentrate around call chains such as `HashBuild::spillPartition`, `SpillState::appendToPartition`, `SpillFileList::write`, and `serializer::presto`. Pure file writing is only part of the cost.

This means spill bottlenecks cannot be treated as I/O alone. Row-based spill targets the CPU and memory-copy costs in serialization, format conversion, and aggregate-state updates after merge.

## Core Idea: Keep the Spill Format Close to RowContainer

The central judgment behind row-based spill is:

> If an operator ultimately needs to restore data into RowContainer, the intermediate format on disk should stay as close as possible to RowContainer instead of going through a columnar detour.

A row in RowContainer can be roughly split into two parts:

```text
+------+-----------+--------------+-------------------+---------+--------------+
| keys | flag bits | accumulators | dependent columns | rowSize | next pointer |
+------+-----------+--------------+-------------------+---------+--------------+
      fixed-width area
```

The fixed-width area stores keys, flag bits, aggregate accumulators, dependent columns, row size, hash-chain pointers, and related metadata. For fixed-width types, this part is already contiguous memory. For variable-width types, a RowContainer row only stores a view or pointer:

- Short strings can be inlined in `StringView`.
- Long strings, varbinary values, and complex type payloads are managed by an allocator, with the row storing views that point to the actual content.
- Complex types are usually serialized into contiguous payloads and referenced by in-row views.

Therefore, row-based spill cannot simply write the in-row fixed area as-is. Otherwise, pointers become invalid after restore. The actual on-disk format needs to combine the fixed-width area and variable-width payload into self-contained row records:

```text
in-memory RowContainer row

+---------------- fixed area ----------------+
| key | flags | acc | StringView(ptr,len) ... |
+--------------------------------------------+
                       |
                       v
             variable-width payload in allocator

row-based spill record

+---------------- fixed area ----------------+---------------- payload ---------------+
| key | flags | acc | StringView(offset,len) | string bytes | complex bytes | ...      |
+--------------------------------------------+----------------------------------------+
```

When writing, Spiller builds a row-format description from RowContainer column information, offsets, alignment, row-size position, and the order of variable-width columns. For each row, it:

1. Copies the in-row fixed-width area.
2. Appends non-inlined variable-width content to the end of the current row record.
3. Makes the views in the row record able to point back to that payload after readback.
4. Writes rows to spill files in batches and compresses them when configured.

When reading back, the row-based reader reads a batch of row bytes, decompresses them if needed, restores in-row views according to the same row-format description, and directly returns a set of `char*` row pointers. The restore side does not need to construct RowVector first and then write RowVector data back into RowContainer.

```text
disk file
    |
    v
RowBasedSpillReadFile
    |
    | read block, decompress, deserialize row views
    v
vector<char*> rows
    |
    +--> HashBuild restore
    |
    +--> HashAgg ordered merge
```

## Overall Architecture

Row-based spill does not replace the whole Spiller. It adds a write mode inside the existing Spiller framework. Spiller still collects data, organizes it by partition, sorts it when needed, writes files, and creates readers and merge streams.

You can think of it as two modes sharing the same scheduling framework:

```text
                    +------------------+
                    |     Operator     |
                    | HashBuild/HashAgg|
                    +---------+--------+
                              |
                              v
                    +------------------+
                    |   RowContainer   |
                    +---------+--------+
                              |
                              v
                    +------------------+
                    |     Spiller      |
                    +---------+--------+
                              |
             +----------------+----------------+
             |                                 |
             v                                 v
     RowVector spill mode              RowContainer spill mode
             |                                 |
             v                                 v
   extract RowVector batches          serialize rows directly
             |                                 |
             v                                 v
      columnar spill files             row-based spill files
```

Configuration-wise, row-based spill has three states:

| Mode | Meaning | Typical trade-off |
|---|---|---|
| `disable` | Use the original columnar spill path | Conservative, best compatibility |
| `raw` | Write rows to disk without compressing row bytes | Lower CPU overhead, potentially larger files |
| `compression` | Write rows to disk and compress row bytes | Lower I/O, additional compression/decompression CPU |

At the file level, row-based spill writes data in blocks. Each block starts with two length fields:

```text
+-------------------+-----------------+------------------------+
| uncompressed size | compressed size | row bytes or compressed |
+-------------------+-----------------+------------------------+
```

Without compression, the two lengths are the same. With compression, the reader first decompresses contiguous row bytes and then restores in-row views row by row. This allows the reader to read in batches, decompress in batches, and return row pointers in batches while avoiding one system call or small-object allocation per row.

## HashBuild: Avoiding a Columnar Intermediate

HashBuild's execution state is naturally row-oriented. The input is RowVector, but the build side ultimately goes into RowContainer and a hash table. Under memory pressure, the original columnar spill path roughly looks like this:

```text
input RowVector
    |
    v
RowContainer / hash table
    |
    | spill
    v
extract RowVector by partition
    |
    v
columnar serialization and disk write
    |
    | restore
    v
read RowVector
    |
    v
call addInput again, rebuild RowContainer / hash table
```

With row-based spill, the key change is that build rows already in RowContainer are no longer split back into RowVector. They are written directly in row format, and after restore the read rows go straight into HashBuild's row-input path.

```text
input RowVector
    |
    v
RowContainer / hash table
    |
    | row-based spill
    v
partitioned row records
    |
    v
row-based spill files
    |
    | restore
    v
vector<char*> rows
    |
    v
HashBuild add spilled row input
```

The gains come from several places:

- In-memory build rows no longer go through `RowContainer -> RowVector`.
- Restore no longer goes through the full `RowVector -> RowContainer` columnar intermediate.
- For many columns, strings, and complex types, writing is organized around one row at a time, reducing random access from multi-column gather.
- For fixed-width data that is already in row format, writing is closer to contiguous memory copy.

The original columnar path still needs to stay. Not every HashBuild scenario is suitable for row-based spill. For example, with multiple builders, multiple drivers, or hybrid join, row-pointer ownership, restore ordering, and concurrency coordination are more complex. A practical implementation usually enables this first for the single-builder, single-driver, non-hybrid-join path to keep correctness and rollout controllable.

## HashAggregation: Accumulators and Merge Are the Hard Parts

Row-based spill for HashAggregation is more complex because RowContainer stores not only group keys but also accumulators for each aggregate function.

The aggregation execution flow can be simplified as:

```text
input batch
    |
    v
probe group key
    |
    +--> new group: create RowContainer row, initialize accumulators
    |
    +--> existing group: update accumulators in the row
```

When the number of groups or memory usage exceeds a threshold, group rows in RowContainer need to be spilled. A typical columnar spill path is:

```text
RowContainer groups
    |
    v
sort by key
    |
    v
extract keys and accumulators into RowVector
    |
    v
write columnar data to disk
    |
    | merge read
    v
RowVector rows
    |
    v
update final accumulators / output results
```

Row-based spill changes the middle two steps:

```text
RowContainer groups
    |
    v
sort by key
    |
    v
row-based write
    |
    | row-based ordered merge
    v
spilled rows
    |
    v
extract accumulators in batches and update groups
```

There are two key points here.

First, accumulators must be safe to spill. Fixed-width accumulators can be written together with the in-row fixed area. Variable-width accumulators need aggregate functions to provide serialization/deserialization support so extra memory can be packed into the row record payload. Otherwise, the spill file would only contain in-process pointers that cannot be restored.

Therefore, HashAggregation is suitable for row-based spill only when these conditions hold:

- The aggregate function accumulator is fixed-size or explicitly supports accumulator serde.
- The aggregate function does not have state such as sorting keys that requires additional ordering semantics.
- The merge phase can correctly extract intermediate aggregate results from row records.

Second, the merge phase must not degrade into single-row updates. In earlier columnar spill paths, after multi-way merge, each output row could come from a different file and easily fall into row-at-a-time paths such as `addSingleGroupIntermediateResults`. Every row and every aggregate function would trigger one update, making virtual-call and temporary selection-vector overhead very visible.

The row-based spill merge path first collects a batch of spilled rows that belong to the current output window. It then extracts accumulators from those rows into vectors by aggregate function and calls batch interfaces to update target groups:

```text
row-based ordered streams
    |
    v
multi-way merge by key
    |
    v
collect a batch of rows and corresponding groups
    |
    v
extract accumulator vectors
    |
    v
addIntermediateResults(group rows, batch vectors)
    |
    v
extract final output
```

This preserves the benefit of row-oriented spill while keeping aggregate functions on their existing vector interfaces as much as possible. It avoids the trap where data is row-oriented on disk but aggregate functions can only consume it one row at a time.

## Why It Is Faster

The benefits of row-based spill fall into four categories.

First, it reduces format conversion. For native RowContainer state, the original path needs `row -> column` before spill and `column -> row` after restore. Row-based spill makes the on-disk format a recoverable row record, reducing intermediate RowVector construction and decomposition.

Second, it reduces multi-column gather and copy. Columnar spill needs to collect cells for each column according to partition indexes and then write column buffers into output streams. With many columns, strings, or complex types, this process can easily hurt locality. Row-oriented writes organize data around rows: the fixed-width part is contiguous, and variable-width payloads are appended sequentially.

Third, it lowers row-at-a-time overhead in the aggregation merge phase. HashAggregation can extract accumulators from spilled rows into vectors in batches and then update accumulators through batch interfaces, reducing virtual calls and temporary object construction.

Fourth, it reduces intermediate objects during restore. HashBuild restore can obtain row pointers from the row-based reader and return directly to the row-input path. HashAgg merge can compare rows and extract accumulators directly on row-based ordered streams.

All these gains depend on one premise: the operator's main working state is truly RowContainer. If a path is already purely columnar, or if spilled data is ultimately consumed directly by a columnar operator, row-based spill may not be the better choice.

## Performance Observations

### General Row Spill Microbench

In a mixed-data workload with 10 columns, the data contains 5 fixed-width columns, 4 string columns, and 1 `array<bigint>` column. The batch size is 4096 rows, and string length is about 50 bytes. The following table shows results for several representative partition counts:

| Partition count | Column-to-column spill | Row-to-column spill | Row-to-row spill | Row-to-row vs. column-to-column | Row-to-row vs. row-to-column |
|---:|---:|---:|---:|---:|---:|
| 64 | 3.54s | 4.76s | 2.77s | 1.28x | 1.72x |
| 128 | 5.18s | 5.45s | 3.37s | 1.54x | 1.62x |
| 256 | 5.96s | 5.48s | 3.38s | 1.77x | 1.62x |

On this mixed-data workload, row-to-row spill is roughly `1.58x ~ 1.83x` faster than row-to-column spill and roughly `1.15x ~ 1.76x` faster than column-to-column spill. For typical production partition counts in the tens to low hundreds, the advantage of row-based spill is relatively stable.

With all variable-width strings, the gain from row-based spill is more visible:

| Scenario | Row-to-row vs. row-to-column | Row-to-row vs. column-to-column |
|---|---:|---:|
| 10 varchar columns, batch size 4096 | 1.81x ~ 2.05x | 1.16x ~ 1.46x |

This matches expectations: the more variable-width columns there are, the easier it is for columnar gather, serialization, and repeated copying costs to grow.

### Local HashAggregation Benchmark

In aggregate-function benchmarks, row-based spill improves most numeric aggregate functions by roughly `1.3x ~ 1.9x`, with string-related scenarios reaching close to `4x` in the best cases.

| Aggregation scenario | Observed trend |
|---|---|
| Numeric aggregates such as `count` / `sum` / `avg` / `min` / `max` | Most cases are 1.3x ~ 1.9x |
| Aggregates with slightly more complex state, such as `stddev` | Most cases still show stable improvement |
| String-related aggregates | Some cases approach 4x |

This should not be interpreted as "row-based spill makes aggregate functions themselves faster." More precisely, the gain comes from less format conversion during spill and merge, fewer row-at-a-time intermediate-result updates, and better memory locality. The actual compute logic of aggregate functions does not change because of the spill format.

### Production Aggregation Jobs

In the following production results, HashAgg time is accumulated operator time across tasks, while stage median and total job time are wall-clock metrics. They cannot be divided directly, but together they show the overall trend.

| Job | Metric | Column spill | Row-based spill | Change |
|---|---|---:|---:|---:|
| A | Stage 3 Regular HashAgg accumulated time | 449.88h | 165.86h | About 2.7x |
| A | Stage 6 Regular HashAgg accumulated time | 56.89h | 25.59h | About 2.2x |
| A | Total job time | 2.9h | 1.2h | About 2.4x |
| B | Stage 6 Regular HashAgg accumulated time | 116.99h | 56.12h | About 2.1x |
| B | Total job time | 55min | 33min | About 1.7x |

These results show that row-based spill benefits can carry through to job-level latency, but the size of the improvement depends on many factors: the share of total time spent in spill, aggregate function types, number of key columns, string ratio, stage parallelism, and whether other bottlenecks remain.

### Comparison After Column Spill Batch Optimization

Columnar spill can also be optimized. For example, after merge, gathering multi-way input back into batches and then passing those batches to aggregate functions can significantly reduce row-at-a-time update overhead. This optimization recovers part of the HashAgg merge cost, but it still has to deal with the columnar intermediate format.

In one aggregation job, the accumulated times for the three paths are:

| Metric | Column spill | Row-based spill | Column spill batch optimization |
|---|---:|---:|---:|
| aggregation total | 369.29h | 140.94h | 177.59h |
| groupingSet output time | 269.45h | 60.40h | 75.24h |
| agg update for output | Not separately tracked | 23.78h | 4.20h |
| spill total | 71.91h | 47.28h | 72.77h |
| convert for spilling | 22.06h | 0h | 21.56h |
| read for spilling | 7.20h | 4.16h | 6.74h |
| sort for spilling | 23.13h | 29.32h | 24.26h |

This comparison is representative: the column batch optimization makes aggregation output updates faster, but format-conversion costs such as `convert for spilling` still remain. The advantage of row-based spill does not come only from batch updates; it also comes from an on-disk format that stays closer to RowContainer.

## Engineering Trade-Offs

Row-based spill is not an unconditional replacement for columnar spill. It has clear applicability boundaries.

### Suitable Scenarios

Row-based spill is a better fit in these cases:

- The operator's in-memory state is already RowContainer.
- Restore after spill still needs to return to RowContainer.
- The data has many columns, or contains many strings, varbinary values, or complex types.
- HashAggregation accumulators can be serialized safely.
- Spill accounts for a large share of total job time.

In these scenarios, row-based spill focuses the optimization on the most expensive conversion path.

### Unsuitable or Limited-Benefit Scenarios

In the following scenarios, the benefit may be small or row-based spill may not be appropriate:

- Data rarely spills, or the amount of spilled data is small.
- Downstream consumption is naturally columnar and does not need RowContainer after readback.
- Aggregate functions have sorting keys, or accumulators do not support safe serde.
- HashBuild scenarios with more complex row-pointer ownership and restore coordination, such as multiple builders, multiple drivers, or hybrid join.
- In compression mode, CPU becomes the bottleneck while I/O is not the main bottleneck.
- Columnar spill has already removed the main row-at-a-time overhead through techniques such as batch gather, and format conversion is not a large share of the cost.

These boundaries matter. The goal of row-based spill is not to make every spill row-oriented. It is to avoid making intermediate state from row-oriented operators pay unnecessary columnar conversion costs just because it needs to be written to disk.

### Where Correctness Risks Concentrate

When implementing row-based spill, correctness risks mainly concentrate around data ownership and serde contracts.

Variable-width data cannot write in-process pointers directly to disk. Spill records must be self-contained, and after readback they must be able to rebuild `StringView` or complex-type views.

Accumulator serde must guarantee that:

- Serialized data contains all variable-width state needed to restore the accumulator.
- Deserialization does not depend on allocator memory that has already become invalid.
- The sizes, alignment, and offsets of the fixed-width area and variable-width payload are consistent.
- The semantics of `extractAccumulators`, `addIntermediateResults`, and final output remain unchanged.

Sorting and merge must also use comparison semantics consistent with RowContainer, including edge cases around nulls, strings, floating-point values, and complex types. Otherwise, even if row-based files are written faster, the merge phase may break result ordering or group boundaries.

## Summary

Row-based spill addresses a practical engineering problem: when execution state is already row-oriented, the spill path should not force it into a columnar format and then back again.

Its core benefits come from:

- Reducing `RowContainer <-> RowVector` conversion.
- Lowering multi-column gather, serialization, and restore costs.
- Letting the HashBuild readback path return directly to row input.
- Letting HashAggregation still update accumulators in batches after row-based merge.

Its boundaries are equally clear: row-based spill brings stable benefits only when operator state, on-disk format, and restore path form a closed loop. For naturally columnar paths, aggregate functions without accumulator serde, or execution modes with complex concurrent restore relationships, keeping columnar spill or enabling row-based spill selectively is safer.

A good spill design is not just about writing data to disk. It should avoid meaningless data-shape movement when memory is tight. That is the value of row-based spill: the intermediate format on disk serves the execution state itself, instead of forcing execution state to adapt to an unsuitable spill format.
