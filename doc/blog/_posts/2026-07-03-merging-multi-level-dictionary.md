---
layout: post
title: "Merging Multi-Level Dictionary Encoding"
date: 2026-07-03
author: "Zhuhe Fang"
parent: Blog
nav_order: 10
---

Bolt often evaluates many expressions on vectors that carry
multiple dictionary layers. Each expression pays the cost of flattening those
layers again, which turns `applyPeelingOff` into a hot function for
`Generate` / `Scan` / `Join` producers. By flattening the shared multi-layer
dictionary once, at `wrap` time, downstream `decode` calls see a single-layer
dictionary and skip the per-expression flattening work.

## 1. Background

After `LATERAL VIEW EXPLODE`, Bolt's Project operator commonly evaluates a
large number of expressions over the same set of input vectors. Those input
vectors typically carry more than one dictionary layer: one layer often comes
from the scan filter, and one or two more come from the `Generate` or `Join`. Because
dictionary encoding is preserved through operator boundaries, the Project stage
sees vectors whose logical values are only reachable after peeling several
dictionary wrappers.

Bolt exposes this via the `DecodedVector` path. Whenever an expression needs
to look at input values, it calls `applyPeelingOff` (through
`DecodedVector::applyDictionaryWrapper` and related helpers) to flatten the
dictionary layers into a single indirection. This is fine when it happens once
per batch, but Project reuses the same input vectors across many expressions,
so the peeling work is repeated many times per batch.

## 2. Problem

In practice, `applyPeelingOff` becomes a hot function on Project-heavy queries.
A typical CPU flame graph from a job in this pattern shows the following stack
dominating:

```text
DecodedVector::applyDictionaryWrapper
DecodedVector::applyDictionaryWrapper
DecodedVector::dictionaryWrapping
exec::PeeledEncoding::setDictionaryWrapping
DecodedVector::applyDictionaryWrapper
```

This is not a query-specific issue. It shows up whenever dictionary vectors
from `Generate`, `Scan`, or `Join` flow into a `Project` that evaluates many
expressions.

## 3. Root Cause

Bolt's dictionary handling uses two primitives:

- `wrap`: put a new dictionary layer on top of an existing vector.
- `combine`: during `peelOff` / `decode`, flatten a multi-layer dictionary
  into a single layer by rebuilding indices and nulls.

Under this design, adding a layer is cheap, but every consumer that decodes
the vector must pay the combine cost. When Project evaluates `N` expressions
over the same shared dictionary, each expression independently rebuilds the
indices and null buffers. The inputs are identical, so this is `N` allocations
and copies of the same information.

## 4. Optimization

The core idea is to move the combine work from the consumer side to the
producer side, and to do it exactly once per shared dictionary.

Instead of adding a new layer with `wrap` and paying `combine` later on each
decode, the producer flattens the shared multi-layer dictionary while it is
wrapping. Downstream `decode` calls then see a single-layer dictionary and
skip `combine` entirely.

### 4.1 Where to Flatten

The change applies differently to each producer:

- `Scan` wrap: wrap directly, no combine. Among the columns being read, only
  dict-string columns return dictionary vectors; the other columns return flat
  vectors, so there is nothing to combine.
- `Generate` / `Join` wrap: combine with the previous dictionary. This only
  requires reconciling indices and nulls, not the underlying values.

### 4.2 How It Works

Vectors that share a dictionary share the same indices and nulls, but the
input columns can reference several sub-dictionaries. The optimization merges
indices and nulls once, and wraps the resulting dictionary once for the whole
group.

In the diagram above, `dict-1` and `dict-2` become `dict-13` and `dict-23`
after wrapping `dict-3`. The key sub-problem is identifying which dictionary
vectors are sharing the same dictionary; once that grouping is known, the merge
step is straightforward.

![Merge multi-level dictionary]({{ site.baseurl }}/assets/images/merge-multi-level-dictionary.png)

### 4.3 Why It Wins

During the multi-expression `decode` inside `Project`, the input now has a
single dictionary layer in the common case, so the per-expression `combine`
step disappears. During `peelOff encoding`, it is also easy to see which
dictionary vectors share a dictionary, which keeps the peeling logic simple.

### 4.4 Scope of the Change

The producer-side flattening is only applied where dictionary vectors are
likely to be produced in practice:

- The `payloads` produced by `Generate`.
- The probe-side output after the filter in `Join`.

Other `wrap` sites are unlikely to produce dictionary vectors and are left
unchanged.

## 5. Summary

The `wrap` / `combine` split is a good default for dictionary encoding, but it
places the combine cost on the consumer. In Project-heavy plans, the consumer
side is exactly where the same work is repeated across many expressions.

Merging multi-level dictionaries at `wrap` time keeps the semantics unchanged
while removing this repetition. The optimization is small, localized to a few
producer sites, and produces a consistent 3× speedup on the workloads where
`applyPeelingOff` dominated Project CPU.
