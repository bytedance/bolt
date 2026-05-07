/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

// End-to-end shuffle-writer throughput benchmark. Measures wall time and
// bytes-on-wire for complex-type payloads with the lazy codec inactive
// (baseline — writer serialises ArrayVector/MapVector per batch) vs active
// (writer receives LazyComplexVector already encoded by Driver-level
// inputLazyModes and ships the inner VARBINARY bytes unchanged).
//
// Usage:
//   bolt_shuffle_writer_lazy_benchmark \
//     --rows=200000 --batches=20 --partitions=4 --payload_cols=2 \
//     --container_len=8 --shuffle_mode=1
//
// Each run prints the two variants' total time, bytes written, and a
// speedup ratio. The same input is driven through both runs so the
// comparison isolates the writer step.

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/sparksql/tests/MemoryTestUtils.h"
#include "bolt/core/PlanNode.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/row/CompactRowLazyCodec.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioning.h"
#include "bolt/vector/LazyComplexCodec.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int32(rows, 200'000, "Total rows per mapper.");
DEFINE_int32(batches, 20, "Number of batches (rows / batches rows per batch).");
DEFINE_int32(partitions, 4, "Number of output partitions.");
DEFINE_int32(payload_cols, 2, "Complex payload columns (array<real> each).");
DEFINE_int32(container_len, 8, "Array element count per row.");
DEFINE_int32(
    shuffle_mode,
    1,
    "0=Adaptive 1=V1 2=V2 3=RowBased (forceShuffleWriterType).");
DEFINE_string(partitioning, "hash", "'single', 'rr', 'hash' or 'range'.");
DEFINE_int32(iterations, 3, "Runs per variant (best wall time reported).");
DEFINE_bool(
    compress,
    true,
    "Enable LZ4_FRAME compression on the partition writer.");
DEFINE_bool(
    variable_length,
    false,
    "Vary array length up to container_len (true) or keep it fixed (false).");

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::shuffle::sparksql;
using namespace bytedance::bolt::memory::sparksql;
using namespace bytedance::bolt::memory::sparksql::test;

namespace {

struct RunResult {
  double wallMs{0};
  int64_t totalBytesWritten{0};
  int64_t rawPartitionBytes{0};
  int64_t inputRows{0};
  // Two-bucket cost model:
  //   encode = wall - writer  (operator + Driver + addInput-side lazy work)
  //   writer = shuffleWriteTime (= totalSplitTime + stopTime; all work
  //            inside BoltShuffleWriter regardless of which phase paid)
  int64_t encodeNs{0};
  int64_t writerNs{0};
};

RowTypePtr makeSchema(int32_t payloadCols) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(payloadCols + 2);
  types.reserve(payloadCols + 2);
  names.emplace_back("pid");
  types.emplace_back(INTEGER());
  names.emplace_back("k");
  types.emplace_back(BIGINT());
  for (int i = 0; i < payloadCols; ++i) {
    names.emplace_back("v" + std::to_string(i));
    types.emplace_back(ARRAY(REAL()));
  }
  return ROW(std::move(names), std::move(types));
}

// Partition-ID generator so hash/range tests have a well-defined column 0.
VectorPtr
makePidVector(memory::MemoryPool* pool, int32_t size, int32_t numPartitions) {
  auto pids = BaseVector::create<FlatVector<int32_t>>(INTEGER(), size, pool);
  auto* raw = pids->mutableRawValues();
  for (int32_t i = 0; i < size; ++i) {
    raw[i] = i % numPartitions;
  }
  return pids;
}

std::vector<RowVectorPtr> makeInputs(
    const RowTypePtr& schema,
    memory::MemoryPool* pool,
    int32_t totalRows,
    int32_t numBatches,
    int32_t containerLen,
    int32_t numPartitions) {
  const int32_t batchSize = totalRows / numBatches;

  VectorFuzzer::Options opts;
  opts.vectorSize = batchSize;
  opts.nullRatio = 0.05;
  opts.containerLength = containerLen;
  opts.containerVariableLength = FLAGS_variable_length;
  // Raise the batch-wide element cap so containerLength is honoured for
  // all batch sizes. Default 10000 caps total elements across the batch,
  // which would clip avg length silently.
  opts.complexElementsMaxSize =
      static_cast<size_t>(batchSize) * containerLen * 4 + 1024;
  VectorFuzzer fuzzer(opts, pool, /*seed=*/99);

  std::vector<RowVectorPtr> out;
  out.reserve(numBatches);
  for (int32_t b = 0; b < numBatches; ++b) {
    auto base = fuzzer.fuzzInputRow(schema);
    // Replace pid (col 0) with a deterministic mod-numPartitions column.
    std::vector<VectorPtr> children = base->children();
    children[0] = makePidVector(pool, batchSize, numPartitions);
    out.emplace_back(std::make_shared<RowVector>(
        pool, schema, /*nulls=*/nullptr, batchSize, std::move(children)));
  }
  return out;
}

// Simulates an upstream that already produces LazyComplexVector children
// (e.g. TableScan with the codec active, or a preceding RowContainer
// operator whose output was allocateLazyAwareRowVector-ed). Each complex
// column is replaced by a LazyComplexVector wrapping its CompactRow
// encoded bytes, so a downstream shuffle-writer sees zero serialisation
// cost beyond the wire-swap.
std::vector<RowVectorPtr> preEncodeInputs(
    const std::vector<RowVectorPtr>& src,
    memory::MemoryPool* pool) {
  row::ensureCompactRowLazyCodecRegistered();
  LazyComplexCodec::setActiveFormat("compact_row");
  const auto* codec = LazyComplexCodec::activeCodec();
  BOLT_CHECK_NOT_NULL(codec);
  std::vector<RowVectorPtr> out;
  out.reserve(src.size());
  for (const auto& batch : src) {
    std::vector<VectorPtr> children = batch->children();
    for (auto& c : children) {
      if (!c) {
        continue;
      }
      const auto& t = c->type();
      if (t->isRow() || t->isArray() || t->isMap()) {
        c = encodeToLazy(c, pool, *codec);
      }
    }
    out.emplace_back(std::make_shared<RowVector>(
        pool,
        batch->type(),
        batch->nulls(),
        batch->size(),
        std::move(children)));
  }
  LazyComplexCodec::setActiveFormat("");
  return out;
}

RunResult runOnce(
    const std::vector<RowVectorPtr>& inputs,
    const RowTypePtr& schema,
    int32_t numPartitions,
    int32_t shuffleMode,
    const std::string& partitioning,
    bool lazyActive) {
  BOLT_CHECK_GE(inputs.size(), 1);
  RunResult result;
  result.inputRows = 0;
  for (const auto& b : inputs) {
    result.inputRows += b->size();
  }

  // Scope-activate the codec for the lazy variant. The Driver reads
  // LazyComplexCodec::activeCodec() per batch, so scope is enough.
  std::string prevName = LazyComplexCodec::activeCodec()
      ? std::string(LazyComplexCodec::activeCodec()->name())
      : std::string();
  if (lazyActive) {
    row::ensureCompactRowLazyCodecRegistered();
    LazyComplexCodec::setActiveFormat("compact_row");
  } else {
    LazyComplexCodec::setActiveFormat("");
  }

  auto tempDir = TempDirectoryPath::create();
  std::string localDir = tempDir->path + "/local_dir";
  std::filesystem::create_directories(localDir);
  std::string dataFile = tempDir->path + "/shuffle_data.bin";

  constexpr int64_t kMemoryLimit = 4LL * 1024 * 1024 * 1024;
  auto memHolder = TestMemoryManagerHolder::create(kMemoryLimit);

  ShuffleWriterOptions writerOptions;
  writerOptions.partitioning = toPartitioning(partitioning);
  writerOptions.partitionWriterOptions.numPartitions = numPartitions;
  writerOptions.forceShuffleWriterType = shuffleMode;
  writerOptions.partitionWriterOptions.partitionWriterType =
      PartitionWriterType::kLocal;
  writerOptions.taskAttemptId = memHolder->taskAttemptId();
  writerOptions.partitionWriterOptions.shuffleBufferSize =
      kDefaultShuffleWriterBufferSize;
  writerOptions.partitionWriterOptions.dataFile = dataFile;
  writerOptions.partitionWriterOptions.configuredDirs = {localDir};
  writerOptions.partitionWriterOptions.numSubDirs = 1;
  if (!FLAGS_compress) {
    writerOptions.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
  }

  ShuffleWriterMetrics metrics;
  auto reportCallback = [&](const ShuffleWriterMetrics& m) { metrics = m; };

  auto sourceNode = PlanBuilder().values(inputs).planNode();
  auto writerNode = std::make_shared<SparkShuffleWriterNode>(
      core::PlanNodeId("writer"), writerOptions, reportCallback, sourceNode);

  CursorParameters params;
  params.planNode = writerNode;
  params.serialExecution = true;
  params.queryCtx = core::QueryCtx::create(
      nullptr,
      core::QueryConfig{{}},
      {},
      cache::AsyncDataCache::getInstance(),
      memHolder->rootPool());

  auto t0 = std::chrono::steady_clock::now();
  auto cursor = TaskCursor::create(params);
  while (cursor->moveNext()) {
  }
  auto t1 = std::chrono::steady_clock::now();

  result.wallMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
  result.totalBytesWritten = metrics.totalBytesWritten;
  if (!metrics.rawPartitionLengths.empty()) {
    result.rawPartitionBytes = 0;
    for (auto b : metrics.rawPartitionLengths) {
      result.rawPartitionBytes += b;
    }
  }
  // shuffleWriteTime = stopTime + totalSplitTime = all work inside
  // BoltShuffleWriter across split() and stop() phases.
  const int64_t wallNs = static_cast<int64_t>(result.wallMs * 1'000'000.0);
  result.writerNs = metrics.shuffleWriteTime;
  result.encodeNs = std::max<int64_t>(0, wallNs - metrics.shuffleWriteTime);

  // Restore codec state for the next run.
  LazyComplexCodec::setActiveFormat(prevName);
  return result;
}

RunResult bestOf(
    int iterations,
    const std::vector<RowVectorPtr>& inputs,
    const RowTypePtr& schema,
    int32_t numPartitions,
    int32_t shuffleMode,
    const std::string& partitioning,
    bool lazyActive) {
  RunResult best;
  best.wallMs = std::numeric_limits<double>::infinity();
  for (int i = 0; i < iterations; ++i) {
    auto r = runOnce(
        inputs, schema, numPartitions, shuffleMode, partitioning, lazyActive);
    if (r.wallMs < best.wallMs) {
      best = r;
    }
  }
  return best;
}

void print(const RunResult& r, const char* label) {
  auto ms = [](int64_t ns) { return ns / 1'000'000.0; };
  // Two buckets:
  //   encode = operator+Driver+lazy addInput (wall - writer)
  //   writer = BoltShuffleWriter total (split + stop)
  // raw  = sum of rawPartitionLengths (pre-compression)
  // comp = totalBytesWritten (post-compression)
  std::printf(
      "%-10s wall=%7.2f  encode=%6.2f  writer=%6.2f  raw=%ld  comp=%ld\n",
      label,
      r.wallMs,
      ms(r.encodeNs),
      ms(r.writerNs),
      r.rawPartitionBytes,
      r.totalBytesWritten);
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});
  filesystems::registerLocalFileSystem();
  Operator::registerOperator(std::make_unique<SparkShuffleWriterTranslator>());

  const auto schema = makeSchema(FLAGS_payload_cols);

  // Build input batches once and reuse across both variants. A dedicated
  // pool keeps them alive through the two runs.
  auto poolHolder = memory::memoryManager()->addLeafPool("bench_input");
  const auto inputs = makeInputs(
      schema,
      poolHolder.get(),
      FLAGS_rows,
      FLAGS_batches,
      FLAGS_container_len,
      FLAGS_partitions);

  std::printf(
      "Config: rows=%d batches=%d partitions=%d payload_cols=%d "
      "container_len=%d shuffle_mode=%d partitioning=%s\n",
      FLAGS_rows,
      FLAGS_batches,
      FLAGS_partitions,
      FLAGS_payload_cols,
      FLAGS_container_len,
      FLAGS_shuffle_mode,
      FLAGS_partitioning.c_str());

  auto baseline = bestOf(
      FLAGS_iterations,
      inputs,
      schema,
      FLAGS_partitions,
      FLAGS_shuffle_mode,
      FLAGS_partitioning,
      /*lazyActive=*/false);
  print(baseline, "baseline");

  // The lazy codec is active but upstream emitted regular complex
  // children — the Driver's kForceLazy pass encodes them per batch at
  // the writer's addInput seam. Measures "codec on but no prior
  // encoding" (worst case for lazy; pays encode + wire-swap).
  auto lazyEncodeHere = bestOf(
      FLAGS_iterations,
      inputs,
      schema,
      FLAGS_partitions,
      FLAGS_shuffle_mode,
      FLAGS_partitioning,
      /*lazyActive=*/true);
  print(lazyEncodeHere, "lazy+enc");

  // Upstream already produced LazyComplexVector (e.g. TableScan with
  // lazy active). Driver dispatch is a no-op; writer just does the
  // wire-swap and ships bytes. The realistic scenario for the feature.
  auto preEncoded = preEncodeInputs(inputs, poolHolder.get());
  auto lazyPreEncoded = bestOf(
      FLAGS_iterations,
      preEncoded,
      schema,
      FLAGS_partitions,
      FLAGS_shuffle_mode,
      FLAGS_partitioning,
      /*lazyActive=*/true);
  print(lazyPreEncoded, "lazy+pre");

  auto speedup = [&](const RunResult& r) {
    return r.wallMs > 0 ? baseline.wallMs / r.wallMs : 0.0;
  };
  auto rawRatio = [&](const RunResult& r) {
    return r.rawPartitionBytes > 0
        ? static_cast<double>(baseline.rawPartitionBytes) /
            static_cast<double>(r.rawPartitionBytes)
        : 0.0;
  };
  auto compRatio = [&](const RunResult& r) {
    return r.totalBytesWritten > 0
        ? static_cast<double>(baseline.totalBytesWritten) /
            static_cast<double>(r.totalBytesWritten)
        : 0.0;
  };
  std::printf(
      "\nlazy+enc  vs baseline  wall_speedup=%.2fx  raw_ratio=%.2fx  comp_ratio=%.2fx\n",
      speedup(lazyEncodeHere),
      rawRatio(lazyEncodeHere),
      compRatio(lazyEncodeHere));
  std::printf(
      "lazy+pre  vs baseline  wall_speedup=%.2fx  raw_ratio=%.2fx  comp_ratio=%.2fx\n",
      speedup(lazyPreEncoded),
      rawRatio(lazyPreEncoded),
      compRatio(lazyPreEncoded));
  return 0;
}
