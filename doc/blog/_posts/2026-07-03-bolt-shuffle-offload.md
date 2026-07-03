---
layout: post
title: "Bolt Shuffle Offload: Turning Gluten Shuffle into Bolt Operators"
date: 2026-07-03
author: "Zhang Xiaofeng"
parent: Blog
nav_order: 5
---

Shuffle is one of the heaviest boundaries in a Spark SQL execution pipeline. In a Gluten + Bolt columnar path, if shuffle stays outside the Bolt plan, data has to move back and forth across Spark iterators, Gluten wrappers, and native shuffle helpers. That makes execution boundaries, memory ownership, and metrics attribution harder to reason about.

Bolt Shuffle Offload moves the expensive shuffle data path into Bolt execution. The shuffle writer and reader become a sink operator and a source operator in the Bolt plan, while Spark continues to own shuffle scheduling, metadata, and fault-tolerance semantics.

## Design

The design can be summarized in one sentence: **Spark keeps the shuffle protocol, and Bolt takes over shuffle data processing.**

Spark SQL plans still contain shuffle exchanges. Spark still owns stage boundaries, map output metadata, block fetch, retry, and AQE compatibility. The change is in the execution path: shuffle write is no longer an external writer after the Bolt pipeline, and shuffle read is no longer just a pre-deserialized batch input for Bolt. They are inserted into the Bolt plan as a writer sink operator and a reader source operator.

With this split, Bolt can handle partition splitting, compression and decompression, serialization and deserialization, spill, memory management, and metrics inside its own task lifecycle, without changing the external Spark shuffle semantics.

## Why Shuffle Offload Matters

Shuffle combines repartitioning, serialization, compression, disk write, network fetch, deserialization, and memory management. In the Gluten + Bolt columnar path, keeping shuffle driven by Spark/Gluten iterators has several direct costs:

- **More execution boundaries**: after a Bolt operator produces a batch, control returns to the Scala shuffle writer and then crosses JNI again into native shuffle logic.
- **More complex data ownership**: batches move through Spark iterators, Gluten wrappers, and native writer/reader code, making reference management and release paths more complicated.
- **Fragmented memory and spill management**: shuffle buffers, compression buffers, upstream operator memory, and spill policies have to be coordinated across Spark, Gluten, and Bolt.
- **Harder integration with the Bolt pipeline**: when shuffle is outside the Bolt plan, it cannot naturally reuse Bolt task lifecycle, operator metrics, memory pools, and profiling support.

Shuffle Offload does not try to rewrite the Spark shuffle protocol. Instead, it moves the data-processing part of shuffle into Bolt and makes it a first-class operator in the Bolt execution plan.

## Responsibility Split

Bolt Shuffle Offload keeps Spark shuffle semantics intact. The Spark SQL plan, AQE, map output tracker, block manager, and shuffle metadata remain compatible with the existing Spark behavior.

At a high level, the responsibilities are split as follows:

| Layer | Responsibilities |
| --- | --- |
| Spark / Gluten | Spark plan conversion, `ColumnarShuffleDependency`, shuffle manager integration, metadata commit, `MapStatus`, block fetch, retry, AQE compatibility |
| JNI Adapter | Passing `ShuffleWriterInfo` / `ShuffleReaderInfo`, native iterator handles, reader stream wrappers, metrics, and partition lengths |
| Bolt Execution | `SparkShuffleWriterNode`, `SparkShuffleReaderNode`, partition split, compression/decompression, serialization/deserialization, spill, memory pool, operator lifecycle |

## Execution Flow

The key idea is simple: Spark still owns the shuffle protocol and stage semantics, Gluten translates Spark shuffle information into native metadata, and Bolt performs the actual data processing. The important change is that shuffle write/read no longer sit outside the Bolt plan. They become a sink operator and a source operator in the Bolt plan.

### Writer Side

**Before offload**, when a `ShuffleMapTask` reaches shuffle write, the Bolt whole-stage plan first produces upstream results as `ColumnarBatch`. Control then returns to Spark/Gluten's `ColumnarShuffleWriter`. The writer pulls those batches one by one and calls the native shuffle writer through JNI. The data path looks like this:

```text
Bolt operator chain
  -> ColumnarBatch returned to Spark/Gluten
  -> ColumnarShuffleWriter
  -> per-batch JNI write
  -> native shuffle writer
  -> shuffle data file
```

The main issue is that the execution boundary cuts through the pipeline. Bolt performs the upstream computation, and the native shuffle writer can still split, compress, and write files, but it is driven batch by batch by the Spark/Gluten iterator and is not part of the Bolt operator chain.

**After offload**, writer execution can be understood in four steps.

First, Gluten checks whether the writer input can be offloaded. `ColumnarShuffleWriter.internalWrite()` matches the incoming records against `WholeStageIteratorWrapper[Product2[K, V]]`. This wrapper indicates that the shuffle writer input comes from Bolt whole-stage execution and carries a native iterator handle. Without that handle, the writer cannot locate the native whole-stage plan that has already been built.

Second, Gluten injects the shuffle writer into the Bolt plan. `ColumnarShuffleWriter` builds a `ShuffleWriterInfo` from the Spark shuffle dependency, including partitioning, partition count, codec, data file, local directories, and memory parameters. It then calls `addShuffleWriter(iterHandle, info)` through JNI. On the C++ side, Bolt uses `iterHandle` to find the corresponding `ResultIterator`, unwraps the internal `WholeStageResultIterator`, and calls `WholeStageResultIterator.addShuffleWriter()`. This step does not process data. It wraps the original Bolt plan root with a writer plan node:

```text
SparkShuffleWriterNode
  +- original Bolt plan
```

`SparkShuffleWriterNode` is plan-node metadata. It is not a data edge between a Spark writer and the Bolt operator chain.

Third, execution starts. Spark/Gluten triggers the native task through `wholeStageIteratorWrapper.next()`. Bolt creates the runtime operator chain from the new plan. The original computation still produces `RowVector`, but the final output no longer returns to Spark as `ColumnarBatch`. It flows directly into the Bolt shuffle writer sink:

```text
Bolt operator chain
  -> Bolt ShuffleWriter operator
  -> shuffle data file
```

At this point, partition split, compression, spill, and shuffle data file writes all happen inside Bolt execution. The Scala iterator no longer drives a JNI writer call for every batch.

Fourth, the result is returned to Spark. When the Bolt shuffle writer finishes, it produces partition lengths and metrics and writes them into `ShuffleWriterResult` through a callback. Spark/Gluten then calls `getShuffleWriterResult()` and uses `writeMetadataFileAndCommit()` to generate the shuffle metadata and `MapStatus` required by Spark. Spark still owns metadata commit, but the data write has already happened inside the Bolt writer operator.

### Reader Side

**Before offload**, after a reduce task creates `ColumnarShuffleReader`, Spark uses `ShuffleBlockFetcherIterator` to fetch local or remote shuffle blocks. The Gluten serializer decompresses and deserializes the block stream into `ColumnarBatch` on the Spark/Gluten iterator side. The Bolt final stage then receives the data through a regular `ColumnarBatchInIterator`. From the Bolt plan's point of view, the input is just a `ValueStreamNode`:

```text
Spark block fetch
  -> Gluten deserialize to ColumnarBatch
  -> ColumnarBatchInIterator
  -> ValueStreamNode
  -> Bolt operator chain
```

In this path, Bolt only consumes batches that have already been materialized. It does not know that the input comes from a shuffle reader, so decompression, deserialization, and batch construction cannot be part of the Bolt operator lifecycle.

**After offload**, reader execution also has several stages.

The first stage is still Spark block fetch. `ColumnarShuffleReader` continues to create a `ShuffleBlockFetcherIterator` through the Spark shuffle protocol. Remote block fetch, retry, checksum, and related semantics remain in Spark. The difference is that the reader does not immediately deserialize the block stream into `ColumnarBatch`. Instead, it packages the Spark-fetched `wrappedStreams` together with the native `ShuffleReaderInfo` into `ShuffleReaderIteratorWrapper`.

The second stage passes this Java/Scala wrapper into the Bolt native stage. When Bolt creates the native iterator, `BoltIteratorApi` inspects the input iterator. If the input comes from a shuffle reader and its delegate is `ShuffleReaderIteratorWrapper`, Bolt creates a `ShuffleReaderInIterator`. On the JNI side, this iterator is wrapped as `ShuffleReaderWrapperedIterator`. The C++ wrapper keeps a reference to the Java wrapper and exposes methods to read `ShuffleReaderInfo`, mark the reader as offloaded, and update metrics.

The third stage happens during Substrait-to-Bolt plan conversion. A regular input is converted to `ValueStreamNode`, which means Bolt reads from an external batch iterator. If the `ReadRel` input iterator is `ShuffleReaderWrapperedIterator`, the converter parses `ShuffleReaderInfo` and creates a `SparkShuffleReaderNode` instead. The wrapper is also marked with `markAsOffloaded()`, so the Spark/Gluten side does not continue down the normal deserialization path.

The fourth stage is runtime stream pulling. Downstream Bolt operators pull from the reader source, and the runtime reader behind `SparkShuffleReaderNode` calls `ReaderStreamIteratorWrapper.nextStream()` to get the next stream:

```text
Bolt downstream operator
  -> Bolt ShuffleReader operator
  -> ReaderStreamIteratorWrapper.nextStream()
  -> Spark-fetched block stream
  -> BoltShuffleReader decompress + deserialize
  -> RowVector
  -> downstream Bolt operator
```

`ReaderStreamIteratorWrapper` calls back through `ShuffleReaderWrapperedIterator` to the Java-side `ShuffleStreamReader`, which returns the next `InputStream` from the blocks already fetched by Spark. That stream is then passed to `BoltShuffleReader.readStream()`. Bolt performs decompression, deserialization, and `RowVector` construction, and then produces data for downstream Bolt operators.

Finally, Bolt closes the reader and reports metrics back. After the Bolt reader finishes, metrics such as output rows, batch count, decompression time, and deserialization time are written back to Spark SQL metrics through `updateMetrics()`. The path keeps the Spark shuffle protocol boundary, while moving the CPU-heavy decompression and deserialization work into the Bolt reader operator.

## Task Cancellation

Moving shuffle into Bolt also changes how Spark task cancellation reaches native execution. In the regular Gluten iterator path, Spark cancellation is usually observed at iterator boundaries: Spark wraps the input with `InterruptibleIterator`, and each `hasNext` / `next` call has a chance to notice that the task has been killed. That works reasonably well when Spark or Gluten is still pulling one batch at a time.

After shuffle offload, the most expensive part of the task can run inside a Bolt `Task`. This is especially visible on the writer side: once `SparkShuffleWriterNode` is appended to the plan, Spark/Gluten triggers the native task and then waits for the Bolt writer operator to finish. In parallel writer mode, Bolt starts native drivers and `WholeStageResultIterator` waits on the Bolt task completion future. During that time there may be no regular batch returned to Spark and no per-batch JNI writer call where Spark's iterator cancellation can be checked.

This is why the Bolt backend needs a native-side bridge such as `TaskStatusListener`. The listener keeps a weak reference to the running Bolt task and a global reference to the corresponding Spark `TaskContext`. A background listener thread periodically calls `TaskContext.getKillReason()`. If Spark has killed the task, the listener can call `task->requestCancel()` on the Bolt task and track the returned cancellation future until the native task has stopped.

The purpose of this listener is not to change Spark's cancellation semantics. It bridges an existing Spark task state into a native execution context that no longer returns to Spark at every batch boundary. Without this bridge, a killed Spark task could leave the Bolt task running until the native operator chain naturally finishes or the iterator is destroyed, wasting CPU, memory, spill, and shuffle I/O resources.

## How to Enable

The inside-Bolt path is controlled by `spark.gluten.shuffle.inside.bolt`:

```text
set spark.gluten.shuffle.inside.bolt=true;
```

## When Offload Does Not Apply

Shuffle Offload depends on a specific connection between Spark/Gluten iterators and the Bolt native plan. Enabling the configuration does not mean every shuffle will become a Bolt operator.

There are two main limitations on the writer side.

First, the shuffle writer input must come from Bolt whole-stage execution. In code, `ColumnarShuffleWriter.internalWrite()` must match `WholeStageIteratorWrapper[Product2[K, V]]`. Only then can the writer obtain the native `iterHandle` and call `addShuffleWriter(iterHandle, ShuffleWriterInfo)` to append `SparkShuffleWriterNode` to `WholeStageResultIterator`. If the operator before shuffle writer is not Bolt whole-stage output, such as when the preceding plan is not an offloadable WholeStageCodegen / Bolt operator chain, the input iterator is just a regular batch iterator and the writer falls back to the normal native shuffle writer path.

Second, range partitioning currently does not use writer offload. `ExecUtil` has a separate branch for `RangePartitioning`: it creates a `RangePartitioner` on the Spark/Gluten side, computes the partition id, and appends the pid column to the batch. This branch does not enter the `spark.gluten.shuffle.inside.bolt` path that preserves `WholeStageIteratorWrapper`. As a result, by the time execution reaches `ColumnarShuffleWriter`, the records are not `WholeStageIteratorWrapper`, `combinedWrite()` cannot be triggered, and the writer uses the regular path.

The reader-side limitation is more direct: the shuffle reader must be immediately followed by a Bolt operator, or more precisely, the shuffle reader output must become an input to a Bolt native stage. Only in that case can `ShuffleReaderIteratorWrapper` be wrapped as `ShuffleReaderInIterator` in `BoltIteratorApi`, and later recognized during Substrait `ReadRel` conversion and replaced with `SparkShuffleReaderNode`. If the shuffle reader is followed by a non-Bolt operator, Spark/Gluten consumes it through the regular iterator path and no Bolt shuffle reader source operator is built.

## A Benefit Example

The following samples come from a pair of before/after reader-side flame graphs. They show how, after the shuffle reader enters the Bolt operator chain, memory allocation moves from a longer Gluten/Spark-side path to Bolt's internal MemoryPool path.

| Sample point | Before offload | After offload |
| --- | --- | --- |
| Total samples | 23,263 samples | 32,001 samples |
| Reader deserialization | `TaskDeserializationStream.readValue`: 12,227 samples / 52.56% | `BoltColumnarBatchDeserializer::next`: 676 samples / 2.11% |
| Reader stream/output | `ShuffleReaderJniWrapper.readStream`: 4,347 samples / 18.69% | `SparkShuffleReader::getOutput`: 1,079 samples / 3.37% |
| Arrow memory pool | `ArrowMemoryPool::Allocate`: 3,800 samples / 16.33% | `BoltArrowMemoryPool::Allocate`: 14 samples / 0.04%<br>`BoltArrowMemoryPool::Free`: 201 samples / 0.63% |
| Allocation path | `ListenableMemoryAllocator::allocateAligned`: 3,793 samples / 16.30% | `MemoryPoolImpl::allocate`: 56 samples / 0.17%<br>`MemoryPoolImpl::allocateContiguous`: 4 samples / 0.01% |
| Release path | `ListenableMemoryAllocator::free`: 5,697 samples / 24.49% | `MemoryPoolImpl::free`: 226 samples / 0.71%<br>`MemoryPoolImpl::freeContiguous`: 41 samples / 0.13% |

This change comes from moving reader execution into Bolt. Before offload, the block stream fetched by Spark is decompressed, deserialized, and materialized as `ColumnarBatch` in the Gluten reader iterator. Memory allocation roughly goes through `ArrowMemoryPool -> ListenableMemoryAllocator -> Spark memory listener/accounting`. Each allocation and release has to pass through Gluten/Spark-side memory listening, accounting, and aligned allocation logic. The downstream Bolt plan only sees an already prepared batch, so it cannot reuse the memory pool owned by the Bolt operator.

After offload, Spark still fetches the block, but the block stream is passed through `ShuffleReaderIteratorWrapper` to Bolt's `SparkShuffleReaderNode`. Decompression, deserialization, and `RowVector` construction happen inside the Bolt reader operator. Allocation lands on `BoltArrowMemoryPool -> MemoryPoolImpl`, managed by the Bolt task memory pool. Compared with the previous long path, this path is shorter and its accounting is aligned with the Bolt operator lifecycle, so allocator overhead takes a much smaller share of the reader hot path.

## Summary

The main value of Bolt Shuffle Offload is that it moves the expensive shuffle data path into the Bolt plan while preserving Spark shuffle protocol and scheduling semantics. From Spark's point of view, the external shuffle behavior stays the same. From the execution engine's point of view, shuffle write and read become first-class Bolt operators that can use Bolt memory management, spill handling, metrics, and task lifecycle.
