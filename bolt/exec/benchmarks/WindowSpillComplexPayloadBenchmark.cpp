/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

// Schema variant: uniform array<float>.
// k1/k2/k3 = bigint sort keys; vN = array<float> length 256, K columns.
//
// Section A: N=1..5 window-count scaling (4 payload columns, 1M rows) — kept
//            for historical comparison.
// Section B: K=8,16,32,64 payload-column scaling (N=1 window, 200K rows).

#include <chrono>
#include <map>
#include <thread>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

DEFINE_int64(
    delay_ms,
    0,
    "Total ms from process start until the first benchmark iteration begins. "
    "Pre-generates all datasets, then sleeps `delay_ms - gen_time_ms` so the "
    "benchmark body begins exactly `delay_ms` after process start. Pair with "
    "`perf record --delay=<same_value>` so perf sampling starts at iteration 1 "
    "and excludes data-gen. If `delay_ms` < measured gen time, skips the sleep "
    "and logs a warning.");

#include "bolt/common/memory/Memory.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/vector/LazyComplexCodec.h"
#include "bolt/vector/LazyComplexVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/ScopedActiveLazyFormat.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

namespace bytedance::bolt::exec::benchmark {
namespace {

// Master dataset configuration. One dataset serves BOTH Section A (N-window
// scaling) and Section B (K-column scaling). Benchmarks build lightweight
// slice views that share the same underlying column VectorPtrs — zero data
// copying between variants.
struct MasterDatasetConfig {
  int numRows = 200'000; // shared row count across all benchmarks
  int batchSize = 4096;
  int arrayLen = 256; // floats per array — 4B × 256 = 1 KB/col/row
  int maxPayloadCols = 64; // widest K we slice views from
};

struct BenchState {
  std::shared_ptr<memory::MemoryPool> pool;

  // Master dataset — full width (maxPayloadCols) + full row count. Every
  // benchmark variant's view points into these underlying VectorPtrs.
  std::vector<RowVectorPtr> master;

  // Cached views derived from `master` — constructed at setup time. Keyed by
  // payload-column count (= number of v_i columns to include, starting from
  // v1). All views share the same BIGINT key VectorPtrs and array VectorPtrs
  // with `master`; the only allocation is the slim wrapper RowVector per K.
  std::map<int /*numPayloadCols*/, std::vector<RowVectorPtr>> viewsByCols;

  std::chrono::milliseconds genDurationMs{0};
};

BenchState& benchState() {
  static BenchState s;
  return s;
}

// ---- Schema helpers -------------------------------------------------------

RowTypePtr schema(int numPayloadCols) {
  std::vector<std::string> names = {"k1", "k2", "k3"};
  std::vector<TypePtr> types = {BIGINT(), BIGINT(), BIGINT()};
  for (int i = 0; i < numPayloadCols; ++i) {
    names.push_back("v" + std::to_string(i + 1));
    types.push_back(ARRAY(REAL()));
  }
  return ROW(std::move(names), std::move(types));
}

// ---- Batch generation -----------------------------------------------------

// Generate the master dataset: `numRows` total rows with `maxPayloadCols`
// array<float> payload columns + 3 bigint sort keys. Every benchmark variant
// views a slice of this one dataset (see makeViews) — no redundant fuzzing.
// Key fuzzer uses seed=43, nullRatio=0 (deterministic order).
// Payload fuzzer uses seed=42, containerLength=arrayLen.
std::vector<RowVectorPtr> makeMaster(
    const MasterDatasetConfig& cfg,
    memory::MemoryPool* pool) {
  VectorFuzzer::Options keyOpts;
  keyOpts.vectorSize = cfg.batchSize;
  keyOpts.nullRatio = 0.0;
  VectorFuzzer keyFuzzer(keyOpts, pool, /*seed=*/43);

  VectorFuzzer::Options payloadOpts;
  payloadOpts.vectorSize = cfg.batchSize;
  payloadOpts.nullRatio = 0.05;
  payloadOpts.containerLength = cfg.arrayLen;
  VectorFuzzer payloadFuzzer(payloadOpts, pool, /*seed=*/42);

  bolt::test::VectorMaker maker(pool);
  auto masterSchema = schema(cfg.maxPayloadCols);
  const int numBatches = (cfg.numRows + cfg.batchSize - 1) / cfg.batchSize;
  std::vector<RowVectorPtr> out;
  out.reserve(numBatches);

  for (int i = 0; i < numBatches; ++i) {
    std::vector<VectorPtr> cols;
    cols.push_back(keyFuzzer.fuzzFlat(BIGINT(), cfg.batchSize));
    cols.push_back(keyFuzzer.fuzzFlat(BIGINT(), cfg.batchSize));
    cols.push_back(keyFuzzer.fuzzFlat(BIGINT(), cfg.batchSize));
    for (int j = 0; j < cfg.maxPayloadCols; ++j) {
      cols.push_back(payloadFuzzer.fuzzFlat(ARRAY(REAL()), cfg.batchSize));
    }
    out.push_back(maker.rowVector(masterSchema->names(), cols));
  }
  return out;
}

// Build K-payload-column views over the master dataset. Each view batch is
// a new RowVector containing the 3 key VectorPtrs + the first `numPayloadCols`
// payload VectorPtrs from the corresponding master batch. No element data is
// copied — the underlying child vectors are shared.
std::vector<RowVectorPtr> makeViews(
    const std::vector<RowVectorPtr>& master,
    int numPayloadCols,
    memory::MemoryPool* pool) {
  auto s = schema(numPayloadCols);
  std::vector<RowVectorPtr> out;
  out.reserve(master.size());
  for (const auto& batch : master) {
    std::vector<VectorPtr> children;
    children.reserve(3 + numPayloadCols);
    // 3 key columns.
    children.push_back(batch->childAt(0));
    children.push_back(batch->childAt(1));
    children.push_back(batch->childAt(2));
    // First `numPayloadCols` payload columns.
    for (int j = 0; j < numPayloadCols; ++j) {
      children.push_back(batch->childAt(3 + j));
    }
    out.push_back(std::make_shared<RowVector>(
        pool,
        s,
        /*nulls*/ nullptr,
        batch->size(),
        std::move(children)));
  }
  return out;
}

// ---- Sink helper ----------------------------------------------------------

void forceDecode(const RowVectorPtr& out, memory::MemoryPool* pool) {
  if (!out) {
    return;
  }
  auto decoded = decodeLazyColumns(out, pool);
  // Touch the decoded RowVector so the compiler can't optimize the call away.
  folly::doNotOptimizeAway(decoded->size());
}

// ---- Pipeline runner -------------------------------------------------------

void runPipeline(
    const std::vector<RowVectorPtr>& batches,
    int windowCount,
    memory::MemoryPool* pool) {
  // Cycle through k1/k2/k3 for any N — forces each window to re-sort and
  // re-materialize the RowContainer, exercising the SerDe path once per step.
  static const std::array<const char*, 3> sortKeys = {"k1", "k2", "k3"};

  PlanBuilder builder;
  builder.values(batches);
  for (int i = 0; i < windowCount; ++i) {
    const std::string expr =
        std::string("row_number() over (order by ") + sortKeys[i % 3] + ")";
    builder.window({expr});
  }
  auto plan = builder.planNode();

  // TestWindowInjection forces SortWindowBuild to avoid the pre-existing
  // RowsStreamingWindowBuild correctness bug with complex payload types.
  // No spill is configured — this is a pure in-memory run.
  TestWindowInjection windowInjection(WindowBuildType::kSortWindowBuild);

  // Use readBatches (copyResult=false) so LazyComplexVector children aren't
  // copied through MultiThreadedTaskCursor's ArrayVector::copy path — that
  // path asserts encoding==encoding and would crash on lazy output.
  std::shared_ptr<Task> task;
  auto batchesOut = AssertQueryBuilder(plan).readBatches(task);
  for (const auto& batch : batchesOut) {
    forceDecode(batch, pool);
  }
}

// Returns a K-column view of the master dataset, building & caching on first
// call. Zero-copy — shares master's underlying column VectorPtrs.
const std::vector<RowVectorPtr>& viewsForCols(int numPayloadCols) {
  auto& state = benchState();
  auto it = state.viewsByCols.find(numPayloadCols);
  if (it == state.viewsByCols.end()) {
    state.viewsByCols[numPayloadCols] =
        makeViews(state.master, numPayloadCols, state.pool.get());
    it = state.viewsByCols.find(numPayloadCols);
  }
  return it->second;
}

} // namespace
} // namespace bytedance::bolt::exec::benchmark

using namespace bytedance::bolt::exec::benchmark;

// ===========================================================================
// Section A — window-count scaling (4 payload cols, 1M rows)
// Kept for historical comparison with earlier runs.
// ===========================================================================

// N=1
BENCHMARK(chainedWindows_1_baseline) {
  runPipeline(viewsForCols(4), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_RELATIVE(chainedWindows_1_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(4), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

// N=2
BENCHMARK(chainedWindows_2_baseline) {
  runPipeline(viewsForCols(4), /*windowCount=*/2, benchState().pool.get());
}
BENCHMARK_RELATIVE(chainedWindows_2_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(4), /*windowCount=*/2, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

// N=3
BENCHMARK(chainedWindows_3_baseline) {
  runPipeline(viewsForCols(4), /*windowCount=*/3, benchState().pool.get());
}
BENCHMARK_RELATIVE(chainedWindows_3_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(4), /*windowCount=*/3, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

// N=4
BENCHMARK(chainedWindows_4_baseline) {
  runPipeline(viewsForCols(4), /*windowCount=*/4, benchState().pool.get());
}
BENCHMARK_RELATIVE(chainedWindows_4_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(4), /*windowCount=*/4, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

// N=5
BENCHMARK(chainedWindows_5_baseline) {
  runPipeline(viewsForCols(4), /*windowCount=*/5, benchState().pool.get());
}
BENCHMARK_RELATIVE(chainedWindows_5_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(4), /*windowCount=*/5, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

// ===========================================================================
// Section B — payload-column scaling (N=1 window fixed, 200K rows)
// K = 8, 16, 32, 64 array<float> columns; sort keys k1/k2/k3 unchanged.
// Theory: SerDe cost scales with K while sort cost (bigint keys) is constant,
// so speedup ratio should rise with K.
// ===========================================================================

BENCHMARK(payloadCols_8_baseline) {
  runPipeline(viewsForCols(8), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_RELATIVE(payloadCols_8_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(8), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

BENCHMARK(payloadCols_16_baseline) {
  runPipeline(viewsForCols(16), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_RELATIVE(payloadCols_16_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(16), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

BENCHMARK(payloadCols_32_baseline) {
  runPipeline(viewsForCols(32), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_RELATIVE(payloadCols_32_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(32), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_DRAW_LINE();

BENCHMARK(payloadCols_64_baseline) {
  runPipeline(viewsForCols(64), /*windowCount=*/1, benchState().pool.get());
}
BENCHMARK_RELATIVE(payloadCols_64_lazy) {
  bytedance::bolt::test::ScopedActiveLazyFormat lazy("compact_row");
  runPipeline(viewsForCols(64), /*windowCount=*/1, benchState().pool.get());
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  window::prestosql::registerAllWindowFunctions();

  auto& state = benchState();
  state.pool = memory::memoryManager()->addLeafPool("benchmark_leaf");

  // Generate ONE master dataset at full width. All benchmark variants build
  // lightweight views from it — no redundant fuzzing across K variants.
  MasterDatasetConfig cfg;
  auto genStart = std::chrono::steady_clock::now();
  state.master = makeMaster(cfg, state.pool.get());

  // Pre-construct slice views for every K used by Section A + Section B.
  // View construction is O(num_batches) pointer copies — nearly free.
  // K=4 is used by Section A's chainedWindows_*; K=8/16/32/64 are Section B.
  for (int numPayloadCols : {4, 8, 16, 32, 64}) {
    (void)viewsForCols(numPayloadCols);
  }

  state.genDurationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - genStart);

  std::cerr << "[setup] all data-gen complete in "
            << state.genDurationMs.count() << " ms\n";

  if (FLAGS_delay_ms > 0) {
    const auto remainingMs = FLAGS_delay_ms - state.genDurationMs.count();
    if (remainingMs > 0) {
      std::cerr << "[setup] sleeping " << remainingMs
                << " ms so the benchmark body begins " << FLAGS_delay_ms
                << " ms after process start — matches `perf record --delay="
                << FLAGS_delay_ms << "`\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(remainingMs));
    } else {
      std::cerr << "[setup] WARNING: --delay_ms=" << FLAGS_delay_ms
                << " is less than data-gen time ("
                << state.genDurationMs.count()
                << " ms). Skipping sleep; perf sampling will include the last "
                << (-remainingMs) << " ms of data-gen.\n";
    }
  } else {
    const auto suggested = state.genDurationMs.count() + 500;
    std::cerr << "[setup] tip: pass --delay_ms=" << suggested
              << " and `perf record --delay=" << suggested
              << "` to exclude data-gen from the profile (gen + 500ms "
                 "margin).\n";
  }

  folly::runBenchmarks();
  return 0;
}
