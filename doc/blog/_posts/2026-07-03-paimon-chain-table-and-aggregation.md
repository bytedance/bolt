---
layout: post
title: "Reading Paimon Chain Tables and Aggregation Tables in Bolt"
date: 2026-07-03
author: "Weixin Xu"
parent: Blog
nav_order: 2
---

Bolt can read Paimon chain tables and aggregation tables through the direct
Paimon connector path. The important boundary is simple: Paimon resolves
Paimon table semantics, and Bolt executes the vectors returned by the scan.
{: .note }

## 1. Background

Paimon primary-key tables are often used for mutable lakehouse data. Two table
types make the scan boundary especially important:

- Chain tables keep full data and incremental data in different branches.
- Aggregation tables merge rows with the same primary key by applying
  configured aggregate functions to value columns.

Neither feature is just a Parquet file read. A correct scan may need Paimon
metadata columns, primary-key ordering, sequence fields, merge engines, and
branch fallback rules. Query operators above the scan should not need to know
which files or branches produced the final row.

This follows the direction described in
[bytedance/bolt#14](https://github.com/bytedance/bolt/issues/14): Bolt already
had Paimon support, but integrating the Paimon C++ SDK (`paimon-cpp`) gives
Bolt a cleaner place to delegate Paimon table semantics.

## 2. Two Read Paths

Bolt has two useful ways to read Paimon data:

- Hive connector path: Bolt reads the underlying files and applies
  Paimon-aware merge logic in the Hive scan layer. This fits file-oriented
  scans that already arrive as Hive splits.
- Paimon connector path: Bolt asks `paimon-cpp` to produce the logical Paimon
  scan output, then imports the result into Bolt vectors. This fits table
  semantics such as chain-table branch fallback and aggregation merge engines.

The Hive connector path reuses Bolt's mature Hive, filesystem, and DWIO reader
stack. That reuse is valuable, but it also means Bolt must understand more of
Paimon's internal scan contract.

The Paimon connector path keeps that contract inside `paimon-cpp`. Bolt
deserializes the Paimon splits, creates a Paimon reader, reads Arrow batches,
and imports those batches into Bolt vectors with the expected output schema.

![Bolt Paimon read path boundaries]({{ site.baseurl }}/assets/images/paimon-read-paths.svg)

## 3. Chain Tables

A chain table is enabled with `chain-table.enabled`. It stores full data in a
`snapshot` branch and incremental data in a `delta` branch, then configures
scan fallback to combine them at read time.

```sql
CREATE TABLE default.t (
  t1 STRING,
  t2 STRING,
  t3 STRING,
  dt STRING
) PARTITIONED BY (dt) WITH (
  'chain-table.enabled' = 'true',
  'primary-key' = 'dt,t1',
  'sequence.field' = 't2',
  'bucket-key' = 't1',
  'bucket' = '2'
);

CALL sys.create_branch('default.t', 'snapshot');
CALL sys.create_branch('default.t', 'delta');

ALTER TABLE default.t SET (
  'scan.fallback-snapshot-branch' = 'snapshot',
  'scan.fallback-delta-branch' = 'delta'
);
```

From Bolt's point of view, this is still a table scan. The connector receives
serialized Paimon split bytes, lets `paimon-cpp` deserialize and plan the read,
and returns the final rows as a `RowVector`. Branch selection and merge-on-read
behavior stay in Paimon; Bolt owns output schema mapping and the remaining
query pipeline.

## 4. Aggregation Tables

An aggregation table uses the `aggregation` merge engine. Non-primary-key
columns can choose aggregate functions through
`fields.<field-name>.aggregate-function`.

```sql
CREATE TABLE sales_metrics (
  id BIGINT,
  sales BIGINT,
  price DOUBLE,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'merge-engine' = 'aggregation',
  'bucket' = '1',
  'fields.sales.aggregate-function' = 'sum',
  'fields.price.aggregate-function' = 'max'
);
```

If the table receives these records:

```text
id:    1, 2, 3
sales: 2, 3, 1
price: 10.0, 20.0, 15.0

id:    1, 3
sales: 1, 2
price: 15.0, 25.0
```

The scan result should already contain the merged values:

```text
id:    1, 2, 3
sales: 3, 3, 3
price: 15.0, 20.0, 25.0
```

With the direct Paimon connector path, `paimon-cpp` owns that merge result.
Bolt does not need to re-derive the aggregation table semantics before running
normal operators above the scan.

![Paimon chain and aggregation table scan semantics]({{ site.baseurl }}/assets/images/paimon-chain-aggregation-semantics.svg)

## 5. Connector Details That Matter

The direct path is intentionally small at the Bolt/Paimon boundary:

- `paimon-cpp` handles split deserialization, table reads, branch fallback, and
  merge-engine behavior.
- Arrow batches are imported into Bolt vectors through the Arrow C Data
  Interface.
- Bolt passes query memory and connector/session options into the read context.
- The connector re-wraps imported vectors with Bolt's requested output type so
  upstream operators see the expected column names and types.

One correctness detail is worth calling out. Some Paimon paths expose the
internal `_VALUE_KIND` metadata column together with user columns. When that
field appears in the imported batch, the connector drops it before returning the
query output. The execution plan should see the logical table schema, not
storage-internal helper columns.

## 6. Filter Pushdown

The direct connector path can still translate supported Bolt filters into
Paimon predicates when the predicate is safe to evaluate inside the Paimon
reader. Unsupported or remaining filters stay in Bolt above the scan.

This keeps the execution model clean: Paimon resolves table-level semantics,
and Bolt preserves the normal query pipeline for predicates and operators that
are not pushed into the Paimon reader.

## 7. Summary

Chain tables and aggregation tables both depend on Paimon's logical table
semantics. Reading them through the direct Paimon connector keeps those
semantics in `paimon-cpp`, while Bolt keeps responsibility for memory
accounting, vector import, schema wrapping, filter translation, and downstream
execution.

The result is a clearer connector boundary for mutable Paimon workloads: Paimon
resolves Paimon tables, and Bolt runs the resulting vectors efficiently.
