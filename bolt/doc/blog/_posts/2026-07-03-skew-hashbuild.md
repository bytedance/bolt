---
layout: post
title: "Skew HashBuild in Bolt"
date: 2026-07-03
author: "Haiyan Gu"
parent: Blog
nav_order: 7
---

HashJoin OOM is not caused only by large input size or delayed spilling. It can
also happen when build-side skew is concentrated in a single spilled partition.
Bolt handles this case by splitting that skewed partition into smaller
sub-ranges in the spill / restore path, so HashBuild and HashProbe can proceed
incrementally and the peak memory of each restore step stays lower.
{: .note }

## 1. Failure Scenario

This issue appears in the spill path of HashJoin. After build-side data is
partitioned by hash and written into multiple spill partitions, skew may still
be concentrated in one partition. If restore follows the normal path, that
partition is read back as a whole and rebuilt into the hash table in one shot.
The memory peak of that HashBuild can then become much higher than for the
other partitions and trigger OOM at that restore point.

## 2. Execution Flow

The execution path in the current code is straightforward:

1. `HashBuild::prepareForFirstSpill()` estimates the restore row budget before
   the first spill writes any files.
2. `Spiller` passes that budget to each eligible partition's `SpillWriter`,
   which rolls files at write-block boundaries.
3. `Spiller::finishSpill()` calls `trySplitSkewPartition()` to compare partition
   sizes and row counts with averages and group skewed partitions' files into
   sub-ranges.
4. HashBuild restores one sub-range at a time.
5. HashProbe replays the corresponding probe partition for each sub-range,
   carrying match state across ranges where outer join semantics require it.

Here, "range partition" is closer to ordered processing over sub-ranges. It is
not a new repartition by key range. It is a split over the file set that
belongs to a skewed spill partition.

## 3. How skewed partitions are identified

Before spilling, `HashBuild::prepareForFirstSpill()` sets the restore row
budget in `Spiller::maxRowsInMemory_`. With skew support enabled, `Spiller`
passes this budget to each partition's writer as `targetFileRows_`.
`SpillWriter::ensureFile()` rolls the file before the next block if the
current file exceeds its byte-size target or reaches its nonzero row target.
The row target is soft: whole blocks may exceed it, and zero disables
row-based rollover. These file boundaries support later range splitting.

Skew detection happens in `Spiller::trySplitSkewPartition()`. It runs during
`finishSpill`, recomputes file size and row count for the spilled partitions,
and compares them with the average. When a partition exceeds the internal skew
threshold, it is marked as skewed and enters the split path:

```cpp
if (fileSizeRatio >= threshold || rowCountRatio >= threshold) {
  rangePartitioningApplicable_ = true;
  skewed = true;
  ...
}
```

## 4. How a skewed partition becomes sub-ranges

The split is not implemented by rereading data and repartitioning by key.
Instead, Bolt shards the existing `SpillPartition` directly.
`SpillPartition::split()` distributes `files_` across multiple shards, and each
shard remains a `SpillPartition`.

```cpp
std::vector<std::unique_ptr<SpillPartition>> SpillPartition::split(
    int numShards) {
  std::vector<std::unique_ptr<SpillPartition>> shards(numShards);
  const auto numFilesPerShard = files_.size() / numShards;
  int32_t numRemainingFiles = files_.size() % numShards;
  ...
  shards[shard] = std::make_unique<SpillPartition>(id_, std::move(files));
}
```

To let restore recognize these shards, `SpillPartitionId` carries two extra
fields: `subRangePartitionNumber_` and `isLastSubRange_`.

```cpp
SpillPartitionId(
    uint8_t partitionBitOffset,
    int32_t partitionNumber,
    int32_t subRangePartitionNumber = dummySubRangePartitionNumber,
    bool isLast = false)
```

The actual split happens in `Spiller::doSkewPartitionSplit()`:

```cpp
auto splitedSpillPartitions =
    partitionSet[key]->split(std::min(estimatedPartitions, pNumFiles));
...
splitedSpillPartitions[i]->setSubRangePartitionNumber(i, isLast);
const SpillPartitionId partitionId(bitOffset, partitionNumber, i, isLast);
partitionSet.emplace(partitionId, std::move(splitedSpillPartitions[i]));
```

As a result, restore no longer sees only one
`[partitionBitOffset, partitionNumber]`, but multiple
`[partitionBitOffset, partitionNumber, subRangePartitionNumber, isLast]`
entries. In practice, "range partition" here means assigning sub-range ids over
one skewed spill partition's file set, not repartitioning the value space.

## 5. How HashBuild uses these sub-ranges

Build-side skew handling has explicit conditions:

- it is enabled only for first-level spill;
- it is enabled only when the current task has one build-side builder and one
  driver;
- it applies only to supported join types, currently Inner, Left, Right, and
  Full.

Once `finishSpill` detects that the spiller used range partitioning, `HashBuild`
records a runtime stat:

```cpp
if (spiller_->rangePartitioningApplicable()) {
  addRuntimeStat("skewedHashBuildUsingRangePartition", RuntimeCounter(1));
}
```

This is one of the most direct runtime signals for confirming that a HashBuild
spill actually entered the skewed-partition split path.

## 6. How HashProbe preserves join semantics

The more delicate part is on the probe side. Build no longer restores a full
partition in one shot. It restores multiple sub-ranges in sequence. If probe
results from each sub-range were emitted independently, left / full outer join
semantics would break because unmatched probe rows could be emitted too early or
more than once.

The current code ties miss handling to whether the restored sub-range is the
last one.

In `HashProbe::setupSpillRestorForRangePartition()`:

```cpp
bool isLastOne = restoredPartitionId->isLastSubRangePartition();
reuseSpillReader_ = !isLastOne;
probeRangePartition_ = true;
includingMiss_ = isLastOne;
```

That means:

- if the current restore step is not the last sub-range, unmatched probe rows
  are not emitted yet;
- only when the last sub-range is processed can the remaining unmatched probe
  rows be emitted as null-extended results.

For left / full joins, this still requires probe-side match state to persist
across sub-ranges. The code uses `matchFlagSpiller_` for that purpose. When the
current sub-range is not the last one, probe match flags are spilled and reused
later. When the last sub-range is reached, that extra spill state is no longer
needed.

```cpp
if (needLastProbeSideOutput()) {
  if (isLastOne) {
    matchFlagSpiller_.reset();
  } else {
    matchFlagSpiller_ = std::make_unique<Spiller>(
        Spiller::Type::kHashJoinProbeMatchFlag,
        matchFlagType_,
        &(spillConfig_.value()));
  }
}
```

During probe, if the current sub-range is still not the last one, Bolt updates
and spills probe match flags instead of emitting unmatched rows immediately:

```cpp
if (probeRangePartition_ && needLastProbeSideOutput() && !includingMiss_) {
  if (!joinNode_->filter()) {
    updateAndSpillProbeMatchFlags(accumulatedMatchFlag_, numInput, true);
  } else {
    prepareMatchFlag(numInput, tmpMatchFlagForFilter_);
    updateAndSpillProbeMatchFlags(tmpMatchFlagForFilter_, numInput, false);
  }
}
```

At the last sub-range, `table_->listJoinResults()` uses the accumulated match
flags to decide which probe rows still need null-extended outer-join output:

```cpp
if (probeRangePartition_ && needLastProbeSideOutput() && includingMiss_) {
  numOut = table_->listJoinResults(
      results_,
      joinIncludesMissesFromLeft(joinType_),
      mapping,
      folly::Range(outputTableRows_.data(), outputTableRows_.size()),
      accumulatedMatchFlag_->childAt(0).get());
}
```

For filter cases, the implementation also has a dedicated branch that ORs
`tmpMatchFlagForFilter_` with `accumulatedMatchFlag_` before spilling:

```cpp
void HashProbe::mergeAndSpillProbeMatchFlags() {
  bits::orBits(
      const_cast<uint64_t*>(target), const_cast<uint64_t*>(right), 0, size);
  matchFlagSpiller_->spill(0, accumulatedMatchFlag_);
}
```

Inner join is simpler because it does not need unmatched probe rows or match
flags across sub-ranges. Left / Full outer join is more involved because
unmatched rows must wait until the last sub-range, and probe-side match state
must carry across restore steps. Right join is also supported by
`HashBuild::supportSkewPartition()`, though its detailed semantics are not the
focus here.

## 7. Summary

Skew HashBuild addresses the case where a heavily skewed partition causes the
memory peak of build restore to concentrate at one point and trigger OOM. Bolt
prepares file boundaries during writing, identifies skewed partitions when
spilling finishes, and splits them into sub-ranges for incremental restoration.

It does not introduce a new join algorithm or require an exact skew-key phase
before plan rewrite. It reuses the existing spill files and turns one skewed
partition into multiple restore units. The main complexity is in preserving
probe-side semantics: Inner join stays simple, while Left / Full outer join uses
`includingMiss_`, `matchFlagSpiller_`, and last-sub-range null extension to
keep the result correct.
