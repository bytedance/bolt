/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <chrono>
#include <ctime>
#include <iostream>
#include <limits>
#include <random>

#include <folly/init/Init.h>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

DEFINE_int32(rows, 16384, "Rows per input batch");
DEFINE_int32(groups, 0, "Distinct keys per input batch; zero means rows");

DEFINE_bool(final, false, "Consume partial output with final aggregation");
DEFINE_int32(columns, 128, "Number of independent sum aggregates");
DEFINE_int32(key_bytes, 0, "String key prefix bytes; zero uses integer keys");
DEFINE_bool(monotonic, true, "Use monotonic RowContainer string allocation");
DEFINE_int32(batches, 4, "Number of input batches");
DEFINE_int32(enabled, -1, "Dynamic spill: -1 alternates off/on, 0 off, 1 on");
DEFINE_int32(trials, 5, "Measured off/on pairs after one warmup pair");
DEFINE_int64(
    memory_limit,
    0,
    "Query memory limit in bytes; zero means unlimited");
DEFINE_int64(output_bytes, 1 << 20, "Preferred output batch bytes");
DEFINE_int64(
    spill_threshold,
    0,
    "Spill memory threshold; zero forces each input chunk to spill");
DEFINE_int64(run_rows, 12 << 20, "Maximum spill run rows");
DEFINE_string(spill_mode, "raw", "disable, raw or compression");
DEFINE_bool(partial, true, "Use partial aggregation");
DEFINE_bool(composite, false, "Enable composite partial output");
DEFINE_bool(overlap, false, "Use the same grouping keys in each input batch");
DEFINE_bool(jit, false, "Enable spill JIT");

class DynamicFinalSpillBenchmark : public OperatorTestBase {
 public:
  void TestBody() override {}
  void run() {
    SetUp();
    runBenchmark();
    TearDown();
  }

 private:
  void runBenchmark() {
    std::vector<std::string> aggregates;
    for (int c = 1; c <= FLAGS_columns; ++c) {
      aggregates.push_back(fmt::format("sum(c{})", c));
    }
    std::vector<RowVectorPtr> input;
    for (int batch = 0; batch < FLAGS_batches; ++batch) {
      std::vector<int64_t> keys(FLAGS_rows);
      const auto groups = FLAGS_groups > 0 ? FLAGS_groups : FLAGS_rows;
      for (int row = 0; row < FLAGS_rows; ++row) {
        keys[row] = row % groups;
      }
      std::mt19937 rng(42 + batch);
      std::shuffle(keys.begin(), keys.end(), rng);
      if (!FLAGS_overlap) {
        for (auto& key : keys) {
          key = key * FLAGS_batches + batch;
        }
      }
      std::vector<VectorPtr> children;
      if (FLAGS_key_bytes > 0) {
        children.push_back(
            makeFlatVector<std::string>(FLAGS_rows, [&](auto row) {
              return std::string(FLAGS_key_bytes, 'x') +
                  fmt::format("{:012}", keys[row]);
            }));
      } else {
        children.push_back(makeFlatVector<int64_t>(keys));
      }
      for (int c = 1; c <= FLAGS_columns; ++c) {
        children.push_back(
            makeFlatVector<int64_t>(FLAGS_rows, [c](auto) { return c; }));
      }
      input.push_back(makeRowVector(children));
    }
    auto builder = PlanBuilder().values(input);
    if (FLAGS_partial) {
      builder.partialAggregation({"c0"}, aggregates);
    } else {
      builder.singleAggregation({"c0"}, aggregates);
    }
    const auto id = builder.planNode()->id();
    // Composite accumulators must be consumed by a compatible operator.
    if (FLAGS_composite || FLAGS_final) {
      BOLT_CHECK(FLAGS_partial);
      builder.finalAggregation();
    }
    std::cout
        << "trial,enabled,wall_ms,cpu_ms,add_ms,finish_ms,output_ms,"
           "spill_ms,spilled_rows,spilled_bytes,peak_bytes,first_output_pool_bytes,"
           "max_output_retained_bytes,output_vectors,operator_output_bytes,rows,checksum\n";
    for (int trial = -1; trial < FLAGS_trials; ++trial) {
      for (int turn = 0; turn < (FLAGS_enabled < 0 ? 2 : 1); ++turn) {
        // Alternating AB/BA controls drift without changing input vectors.
        const bool enabled = FLAGS_enabled < 0 ? (std::max(trial, 0) + turn) % 2
                                               : FLAGS_enabled != 0;
        auto dir = TempDirectoryPath::create();
        CursorParameters params;
        params.planNode = builder.planNode();
        params.serialExecution = true;
        params.copyResult = false;
        if (FLAGS_memory_limit > 0) {
          params.queryCtx = core::QueryCtx::create();
          params.queryCtx->testingOverrideMemoryPool(
              memory::memoryManager()->addRootPool(
                  params.queryCtx->queryId(), FLAGS_memory_limit));
        }
        params.spillDirectory = dir->path;
        params.queryConfigs = {
            {core::QueryConfig::kSpillEnabled, "true"},
            {core::QueryConfig::kUseMonoAlloc,
             FLAGS_monotonic ? "true" : "false"},
            {core::QueryConfig::kAggregationSpillEnabled, "true"},
            {core::QueryConfig::kPreferPartialAggregationSpill, "true"},
            {core::QueryConfig::kTestingSpillPct,
             FLAGS_spill_threshold == 0 ? "100" : "0"},
            {core::QueryConfig::kAggregationSpillMemoryThreshold,
             std::to_string(FLAGS_spill_threshold)},
            {core::QueryConfig::kAggregationDynamicFinalSpillEnabled,
             enabled ? "true" : "false"},
            {core::QueryConfig::kHashAggregationCompositeOutputEnabled,
             FLAGS_composite ? "true" : "false"},
            {core::QueryConfig::kHashAggregationCompositeOutputAccumulatorRatio,
             "1"},
            {core::QueryConfig::kHashAggregationUniqueRowOpt, "true"},
            {core::QueryConfig::kPreferredOutputBatchBytes,
             std::to_string(FLAGS_output_bytes)},
            {core::QueryConfig::kMaxSpillRunRows,
             std::to_string(FLAGS_run_rows)},
            {core::QueryConfig::kRowBasedSpillMode, FLAGS_spill_mode},
            {core::QueryConfig::kJitLevel, FLAGS_jit ? "-1" : "0"},
        };
        auto cursor = TaskCursor::create(params);
        const auto start = std::chrono::steady_clock::now();
        const auto cpuStart = std::clock();
        uint64_t rows = 0;
        uint64_t checksum = 0;
        uint64_t keyChecksum = 0;
        uint64_t retainedBytes = 0;
        uint64_t firstOutputBytes = 0;
        while (cursor->moveNext()) {
          const auto& output = cursor->current();
          if (rows == 0) {
            cursor->task()->testingVisitDrivers([&](Driver* driver) {
              if (auto* op = driver->findOperator(id)) {
                firstOutputBytes += op->pool()->currentBytes();
              }
            });
          }
          retainedBytes = std::max(retainedBytes, output->retainedSize());
          rows += output->size();
          DecodedVector keys(*output->childAt(0));
          for (vector_size_t row = 0; row < output->size(); ++row) {
            keyChecksum += FLAGS_key_bytes > 0
                ? std::stoull(keys.valueAt<StringView>(row).str().substr(
                      FLAGS_key_bytes))
                : keys.valueAt<int64_t>(row);
          }
          // Materialize/consume all columns, as a downstream writer does.
          // This also verifies every aggregate, including composite output.
          for (int c = 1; c <= FLAGS_columns; ++c) {
            DecodedVector values(*output->childAt(c));
            for (int r = 0; r < output->size(); ++r) {
              BOLT_CHECK(!values.isNullAt(r));
              BOLT_CHECK_EQ(
                  values.valueAt<int64_t>(r),
                  int64_t(c) * (FLAGS_overlap ? FLAGS_batches : 1) *
                      (FLAGS_groups > 0 ? FLAGS_rows / FLAGS_groups : 1));
              checksum += values.valueAt<int64_t>(r);
            }
          }
        }
        const auto wallMs = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - start)
                                .count();
        const auto cpuMs = 1000.0 * (std::clock() - cpuStart) / CLOCKS_PER_SEC;
        BOLT_CHECK_EQ(
            rows,
            (FLAGS_groups > 0 ? FLAGS_groups : FLAGS_rows) *
                (FLAGS_overlap ? 1 : FLAGS_batches));
        BOLT_CHECK_EQ(keyChecksum, rows * (rows - 1) / 2);
        const auto stats = cursor->task()->taskStats();
        for (const auto& pipeline : stats.pipelineStats) {
          for (const auto& op : pipeline.operatorStats) {
            if (op.planNodeId != id) {
              continue;
            }
            BOLT_CHECK_GT(op.spilledRows, 0);
            BOLT_CHECK_EQ(
                op.runtimeStats.at("aggregationOutputCompositeVector").sum,
                FLAGS_partial && FLAGS_composite);
            const auto spillMs = op.runtimeStats.at("spillTotalTime").sum / 1e6;
            std::cout << trial << ',' << enabled << ',' << wallMs << ','
                      << cpuMs << ',' << op.addInputTiming.wallNanos / 1e6
                      << ',' << op.finishTiming.wallNanos / 1e6 << ','
                      << op.getOutputTiming.wallNanos / 1e6 << ',' << spillMs
                      << ',' << op.spilledRows << ',' << op.spilledBytes << ','
                      << op.memoryStats.peakTotalMemoryReservation << ','
                      << firstOutputBytes << ',' << retainedBytes << ','
                      << op.outputVectors << ',' << op.outputBytes << ','
                      << rows << ',' << checksum << '\n';
          }
        }
        auto task = cursor->task();
        cursor.reset();
        deleteTaskAndCheckSpillDirectory(task);
      }
    }
  }
};

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  FLAGS_minloglevel = 2;
  BOLT_CHECK_GT(FLAGS_rows, 0);
  BOLT_CHECK_GT(FLAGS_columns, 0);
  BOLT_CHECK_GT(FLAGS_batches, 1);
  BOLT_CHECK_GE(FLAGS_enabled, -1);
  BOLT_CHECK_LE(FLAGS_enabled, 1);
  BOLT_CHECK_GE(FLAGS_groups, 0);
  BOLT_CHECK_GE(FLAGS_key_bytes, 0);
  BOLT_CHECK_GE(FLAGS_trials, 0);
  BOLT_CHECK_GE(FLAGS_memory_limit, 0);
  BOLT_CHECK_GE(FLAGS_spill_threshold, 0);
  BOLT_CHECK_GE(FLAGS_run_rows, 0);
  BOLT_CHECK_GT(FLAGS_output_bytes, 0);
  BOLT_CHECK_LE(FLAGS_output_bytes, std::numeric_limits<int32_t>::max());
  if (FLAGS_groups > 0) {
    BOLT_CHECK_EQ(FLAGS_rows % FLAGS_groups, 0);
  }
  filesystems::registerLocalFileSystem();
  OperatorTestBase::SetUpTestCase();
  {
    DynamicFinalSpillBenchmark benchmark;
    benchmark.run();
  }
  OperatorTestBase::TearDownTestCase();
}
