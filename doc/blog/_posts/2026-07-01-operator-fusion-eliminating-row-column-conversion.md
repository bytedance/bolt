---
layout: post
title: "Operator Fusion: Reducing Data Layout Conversions"
date: 2026-07-01
author: "Haiyan Gu, Jinyuan Zhang"
parent: Blog
nav_order: 4
---

Bolt's operator fusion optimizations reduce the format-conversion and
intermediate-materialization overhead introduced at physical operator
boundaries. Aggregation + Shuffle fusion avoids the
`RowContainer -> RowVector -> CompactRow` round trip before row-based shuffle
write. Sort + Window fusion keeps sorting inside the Window operator when the
input is not already ordered. In representative production-style double-run
tests, Aggregation + Shuffle fusion reduced the overall agg + shuffle stage
time by roughly 18% to 25%, while shuffle size ranged from roughly unchanged to
about 6% larger.
{: .note }

## 1. Background

Bolt's execution engine is primarily columnar at operator boundaries. This
representation works well for vectorized execution, projection, filtering, and
many downstream consumers. However, not every operator is naturally columnar
internally.

Hash aggregation is a typical example. During aggregation, grouping keys and
accumulator states are maintained in hash-table-backed row-oriented structures.
When aggregation output is immediately consumed by row-based shuffle, forcing
the data through a normal columnar output batch introduces an avoidable round
trip:

```text
RowContainer -> RowVector -> CompactRow
```

The `RowVector` in the middle is useful for a generic columnar boundary, but in
this path it is not the representation that the next operator ultimately needs.
The shuffle writer will rebuild a row-oriented payload again.

Operator fusion in Bolt targets this kind of boundary cost. It keeps the
physical plan semantics unchanged, while allowing adjacent operators to share a
more suitable intermediate representation.

![Aggregation and Shuffle before and after fusion]({{ site.baseurl }}/assets/images/operator-fusion-before-after.svg)

## 2. Aggregation + Shuffle

Consider a common distributed aggregation path:

```text
partial aggregation -> exchange -> final aggregation
```

In Bolt, partial aggregation maintains grouping keys and accumulator states in
`RowContainer`. When the next stage is row-based shuffle, the conventional path
first emits a normal `RowVector`, and the shuffle writer then serializes that
data back into row format.

This is where Aggregation + Shuffle fusion applies. Instead of eagerly
flattening the aggregation state into a normal columnar batch, partial
aggregation can emit a `CompositeRowVector`.

### 2.1 CompositeRowVector

`CompositeRowVector` separates two parts of the output:

- Grouping keys remain accessible as columns.
- Accumulator state is kept as row payload.

This design matters because shuffle still needs column access to grouping keys
for projection and partition hashing. At the same time, the accumulator payload
does not need to be reconstructed from columns before row-based shuffle write.

![CompositeRowVector layout]({{ site.baseurl }}/assets/images/operator-fusion-composite-vector.svg)

`CompositeRowVector` does not make the whole output row-based. It keeps the key
columns visible to the vectorized path, while preserving the aggregation state
in a format that can be copied into shuffle frames more directly.

The composite output path is intentionally constrained. It is used only for
partial aggregation, requires the row-based spill layout to be available,
excludes global and distinct aggregation shapes, requires compatible row
alignment, and is enabled only when the accumulator side is large enough
compared with the grouping-key side. These conditions keep the optimization
focused on cases where the saved conversion cost is likely to matter.

### 2.2 Execution Path

With fusion enabled, the main change happens between partial aggregation and
shuffle write:

1. Partial aggregation builds its state in `RowContainer`.
2. Instead of materializing all aggregation output as a normal `RowVector`, it
   emits `CompositeRowVector`.
3. Projection and partitioning continue to read grouping keys as columns.
4. The row-based shuffle writer splits the composite vector by partition and
   writes the accumulator payload into shuffle frames.
5. The shuffle reader still produces a normal columnar batch for final
   aggregation.

The last step is intentionally unchanged. Final aggregation continues to consume
columnar input. The optimization mainly removes the
`RowContainer -> RowVector -> CompactRow` round trip before shuffle write.

There is also an important compatibility detail. Partial aggregation may
abandon the optimized path in some cases. Therefore, shuffle writer and shuffle
reader need to handle both ordinary `RowVector` and `CompositeRowVector`. Bolt
records layout information in shuffle frame metadata, so the reader can parse
each frame according to its actual layout.

![Adaptive shuffle layout]({{ site.baseurl }}/assets/images/operator-fusion-adaptive-shuffle.svg)

This makes the optimization adaptive rather than invasive. The external
exchange semantics remain unchanged, while the optimized path avoids unnecessary
reconstruction when the producer and consumer can share a better physical
representation.

## 3. Sort + Window

Window functions have a different pattern. They are sensitive to ordering:
`partition by` and `order by` define the input order required by the window
operator. In a conventional plan, sorting and window evaluation may be separated
by an operator boundary. The sort operator materializes ordered intermediate
data, and the window operator consumes it later.

Bolt fuses this boundary by making sorting an internal capability of the Window
operator when the input is not already sorted.

At runtime, Window checks whether the plan marks its input as already sorted:

```cpp
needSort_ = !(windowNode->inputsSorted());
```

If the input already satisfies the ordering requirement, Window uses a streaming
path and evaluates window functions directly. Otherwise, sorting is performed
inside the Window operator through the sort-window build path, and the sorted
rows are consumed by the same operator to compute window results.

![Sort and Window fusion]({{ site.baseurl }}/assets/images/operator-fusion-sort-window.svg)

This is also operator fusion, but it is different from Aggregation + Shuffle
fusion. It does not rely on `CompositeRowVector`, and it is not controlled by
the composite aggregation configuration. Its goal is to avoid a separate
Sort-to-Window boundary and keep sorting, buffering, and window evaluation
within one physical operator when sorting is required.

The benefit is that ordered data does not need to be handed off through an
additional materialized boundary before window evaluation. For large partitions
or complex window functions, reducing that boundary can lower memory movement
and simplify the execution path.

## 4. Performance Results

In representative production-style double-run tests, Aggregation + Shuffle
fusion showed stable improvement on the overall agg + shuffle stage. End-to-end
stage time for the agg + shuffle portion decreased by roughly 18% to 25%.

The largest gain was concentrated between partial aggregation and shuffle write,
where elapsed time decreased by roughly 28% to 38%. The shuffle read + final
aggregation side also improved to different degrees, with reductions of roughly
4% to 22%. This matches the optimization target: once the
`RowContainer -> RowVector -> CompactRow` round trip is reduced, the most direct
benefit appears on the data reorganization path before shuffle write.

The main cost is reflected in shuffle size. In the tests, shuffle size ranged
from roughly unchanged to about 6% larger, mainly due to differences in
row-payload layout. Compared with the reduction in stage time, this size
increase is within an acceptable range.

## 5. Configuration and Observability

Aggregation + Shuffle composite output is controlled by the following
configuration items:

| Configuration | Default | Scope | Description |
| --- | --- | --- | --- |
| `hashaggregation_composite_output_enabled` | `false` | Aggregation + Shuffle | Master switch for composite aggregation output. |
| `hashaggregation_composite_output_accumulator_ratio` | `5` | Aggregation + Shuffle | Trigger threshold based on the ratio between accumulator count and grouping-key count. |

Whether the composite path is actually used can be observed through the runtime
statistic `aggregationOutputCompositeVector`.

Sort + Window does not use `CompositeRowVector`, and it does not have a
corresponding composite switch. Whether sorting is needed is determined by
`WindowNode::inputsSorted()`: if the input already satisfies the required
ordering, Window runs in the streaming path; otherwise, sorting is performed
inside the Window operator.

## 6. Summary

Operator fusion in Bolt is not a single implementation technique. It is a design
principle for removing unnecessary work across adjacent physical operators.

Aggregation + Shuffle fusion focuses on data representation. It lets partial
aggregation pass row-oriented accumulator payloads to row-based shuffle without
first rebuilding them as ordinary columnar output and then converting them back
to row format.

Sort + Window fusion focuses on operator boundaries. It moves sorting into
Window when the input is not already sorted, so that sorting and window
evaluation can share the same physical execution context.

The common point is the same: do not materialize an intermediate representation
only because an operator boundary exists. When adjacent operators naturally
agree on part of the data organization, Bolt keeps that agreement in the
execution path and removes the redundant conversion around it.
