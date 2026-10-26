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

/// Write-path comparison of the shuffle writers on identical, large inputs.
///
/// Every benchmark drives one writer lifecycle (construct, split every batch
/// of a scenario, stop) and returns the rows processed, so folly reports
/// time per row. Split and stop are both timed: the writers place their
/// compression and writing differently (V1 evicts during split once its
/// buffers fill, Cell keeps cells resident until a spill or stop), so split
/// alone does not compare them. Construction and teardown run under a
/// suspender.
///
/// Each scenario holds about --input_mb of input (1 GiB by default), enough
/// to amortize what a writer provisions up front (first cells, partition
/// buffers); smaller inputs measure that provisioning instead of
/// throughput. Scenarios are built on first use, one resident at a time. A
/// summary table printed at exit splits each lifecycle into split and stop
/// and adds bytes written.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <map>
#include <random>

#include <arrow/io/memory.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"

DEFINE_int64(
    input_mb,
    1024,
    "Input per scenario in MiB, as the estimateFlatSize of its batches.");
DEFINE_bool(
    phases,
    false,
    "Skip the folly benchmarks; print a writer/reader phase breakdown for "
    "--phases_scenario across Cell(dict on/off)/V1/V2.");
DEFINE_string(
    phases_scenario,
    "strdict_P1024",
    "Scenario the --phases breakdown runs on.");

using namespace bytedance::bolt;
using namespace bytedance::bolt::shuffle::sparksql;

namespace {

constexpr int32_t kRowsPerBatch = 4096;
constexpr int64_t kMemLimit = 1LL << 30;
/// Chunk-memory spill threshold of the CellCap variants, below the cell data
/// of the scenarios they run on (fixed4 ~600MB, strings ~600MB at the
/// default input), so the lifecycle spills and merges.
constexpr int64_t kCellCapBytes = 256LL << 20;

struct Scenario {
  std::string name;
  int32_t numPartitions;
  std::vector<RowVectorPtr> batches; // pid column first
};

uint64_t clockNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

memory::MemoryPool* leafPool() {
  // Cell writers automatically size cells from this pool's capacity.
  static std::shared_ptr<memory::MemoryPool> pool =
      memory::memoryManager()->addLeafPool("cell_split_benchmark");
  return pool.get();
}

template <typename T>
VectorPtr makeFlatNullable(
    const std::vector<T>& values,
    int32_t nullPercent,
    std::mt19937& rng);

template <typename T>
VectorPtr makeFlat(const std::vector<T>& values) {
  auto buffer = AlignedBuffer::allocate<T>(values.size(), leafPool());
  ::memcpy(
      buffer->template asMutable<T>(),
      values.data(),
      values.size() * sizeof(T));
  TypePtr type;
  if constexpr (std::is_same_v<T, int64_t>) {
    type = BIGINT();
  } else if constexpr (std::is_same_v<T, int32_t>) {
    type = INTEGER();
  } else {
    static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>);
  }
  return std::make_shared<FlatVector<T>>(
      leafPool(),
      type,
      nullptr,
      values.size(),
      std::move(buffer),
      std::vector<BufferPtr>{});
}

template <typename T>
VectorPtr makeFlatNullable(
    const std::vector<T>& values,
    int32_t nullPercent,
    std::mt19937& rng,
    bool defensiveNulls = false) {
  auto vector = makeFlat(values);
  if (nullPercent == 0 && defensiveNulls) {
    // An all-set buffer: mayHaveNulls() true, zero actual nulls.
    auto nulls = AlignedBuffer::allocate<bool>(values.size(), leafPool());
    ::memset(
        nulls->template asMutable<uint8_t>(), 0xFF, (values.size() + 7) / 8);
    vector->setNulls(nulls);
    return vector;
  }
  if (nullPercent == 0) {
    return vector;
  }
  auto nulls = AlignedBuffer::allocate<bool>(values.size(), leafPool());
  auto* rawNulls = nulls->template asMutable<uint8_t>();
  ::memset(rawNulls, 0xFF, (values.size() + 7) / 8);
  for (size_t i = 0; i < values.size(); ++i) {
    if (static_cast<int32_t>(rng() % 100) < nullPercent) {
      rawNulls[i / 8] &= static_cast<uint8_t>(~(1u << (i % 8)));
    }
  }
  vector->setNulls(nulls);
  return vector;
}

VectorPtr makeStrings(
    const std::vector<std::string>& values,
    const std::vector<bool>& nulls) {
  auto views = AlignedBuffer::allocate<StringView>(values.size(), leafPool());
  auto* rawViews = views->asMutable<StringView>();
  size_t total = 0;
  for (const auto& value : values) {
    total += value.size();
  }
  auto chars = AlignedBuffer::allocate<char>(total, leafPool());
  auto* rawChars = chars->asMutable<char>();
  BufferPtr nullBuffer = nullptr;
  uint8_t* rawNulls = nullptr;
  if (!nulls.empty()) {
    nullBuffer = AlignedBuffer::allocate<bool>(values.size(), leafPool());
    rawNulls = nullBuffer->asMutable<uint8_t>();
    ::memset(rawNulls, 0xFF, (values.size() + 7) / 8);
  }
  size_t offset = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!nulls.empty() && nulls[i]) {
      rawNulls[i / 8] &= static_cast<uint8_t>(~(1u << (i % 8)));
      rawViews[i] = StringView();
      continue;
    }
    ::memcpy(rawChars + offset, values[i].data(), values[i].size());
    rawViews[i] =
        StringView(rawChars + offset, static_cast<int32_t>(values[i].size()));
    offset += values[i].size();
  }
  std::vector<BufferPtr> stringBuffers{chars};
  return std::make_shared<FlatVector<StringView>>(
      leafPool(),
      VARCHAR(),
      std::move(nullBuffer),
      values.size(),
      std::move(views),
      std::move(stringBuffers));
}

RowVectorPtr makeBatch(
    std::vector<std::string> names,
    std::vector<VectorPtr> children) {
  std::vector<TypePtr> types;
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  names.insert(names.begin(), "pid");
  return std::make_shared<RowVector>(
      leafPool(),
      ROW(std::move(names), std::move(types)),
      BufferPtr(nullptr),
      children[1]->size(),
      std::move(children));
}

VectorPtr makePids(int32_t numPartitions, std::mt19937& rng) {
  std::vector<int32_t> pids(kRowsPerBatch);
  for (auto& pid : pids) {
    pid = rng() % numPartitions;
  }
  return makeFlat(pids);
}

/// Batches from `makeNext(batchIndex)` until they hold --input_mb.
std::vector<RowVectorPtr> fillToInputSize(
    const std::function<RowVectorPtr(int32_t)>& makeNext) {
  const int64_t target = FLAGS_input_mb << 20;
  std::vector<RowVectorPtr> batches;
  int64_t bytes = 0;
  for (int32_t b = 0; bytes < target; ++b) {
    batches.push_back(makeNext(b));
    bytes += batches.back()->estimateFlatSize();
  }
  return batches;
}

/// Frame-of-reference friendly bigints, bit-packable ints.
std::vector<RowVectorPtr> makeFixedBatches(
    int32_t numPartitions,
    int32_t nullPercent = 0,
    bool addAllNullColumn = false,
    bool defensiveNulls = false) {
  std::mt19937 rng(17);
  return fillToInputSize([&](int32_t b) {
    std::vector<int64_t> ids(kRowsPerBatch);
    std::vector<int64_t> timestamps(kRowsPerBatch);
    std::vector<int32_t> smalls(kRowsPerBatch);
    std::vector<int32_t> randoms(kRowsPerBatch);
    for (int i = 0; i < kRowsPerBatch; ++i) {
      ids[i] = 7'000'000'000LL + static_cast<int64_t>(b) * kRowsPerBatch + i;
      timestamps[i] = 1'726'000'000'000LL + (rng() % 3'600'000);
      smalls[i] = rng() % 1000;
      randoms[i] = static_cast<int32_t>(rng());
    }
    std::vector<std::string> names{"id", "ts", "small", "rnd"};
    std::vector<VectorPtr> children{
        makePids(numPartitions, rng),
        makeFlatNullable(ids, nullPercent, rng, defensiveNulls),
        makeFlatNullable(timestamps, nullPercent, rng, defensiveNulls),
        makeFlatNullable(smalls, nullPercent, rng, defensiveNulls),
        makeFlatNullable(randoms, nullPercent, rng, defensiveNulls)};
    if (addAllNullColumn) {
      names.push_back("dead");
      children.push_back(
          BaseVector::createNullConstant(BIGINT(), kRowsPerBatch, leafPool()));
    }
    return makeBatch(std::move(names), std::move(children));
  });
}

/// A wide table: kWideColumns bigints, the first two with 1% sparse nulls.
/// Null bitmaps sized per partition for every column would fill the null
/// memory limit long before the partition byte bound.
std::vector<RowVectorPtr> makeWideSparseNullBatches(int32_t numPartitions) {
  constexpr int32_t kWideColumns = 100;
  std::mt19937 rng(29);
  return fillToInputSize([&](int32_t b) {
    std::vector<std::string> names;
    std::vector<VectorPtr> children{makePids(numPartitions, rng)};
    std::vector<int64_t> values(kRowsPerBatch);
    for (int32_t col = 0; col < kWideColumns; ++col) {
      for (int i = 0; i < kRowsPerBatch; ++i) {
        values[i] = static_cast<int64_t>(b) * kRowsPerBatch + i + col;
      }
      names.push_back("c" + std::to_string(col));
      children.push_back(
          makeFlatNullable<int64_t>(values, col < 2 ? 1 : 0, rng, false));
    }
    return makeBatch(std::move(names), std::move(children));
  });
}

std::vector<RowVectorPtr> makeStringBatches(int32_t numPartitions) {
  std::mt19937 rng(23);
  return fillToInputSize([&](int32_t /*b*/) {
    std::vector<int64_t> keys(kRowsPerBatch);
    std::vector<std::string> values(kRowsPerBatch);
    std::vector<bool> nulls(kRowsPerBatch);
    for (int i = 0; i < kRowsPerBatch; ++i) {
      keys[i] = static_cast<int64_t>(rng());
      nulls[i] = rng() % 10 == 0;
      values[i].assign(8 + rng() % 92, static_cast<char>('a' + rng() % 26));
    }
    return makeBatch(
        {"key", "value"},
        {makePids(numPartitions, rng),
         makeFlat(keys),
         makeStrings(values, nulls)});
  });
}

/// Low-cardinality strings: 12 distinct 4-char values (60 serialized
/// bytes, inside the single-dictionary probe criterion). `clustered` feeds
/// the values in runs of 64, the shape the MRU check is built for.
std::vector<RowVectorPtr> makeDictStringBatches(
    int32_t numPartitions,
    bool clustered) {
  std::mt19937 rng(31);
  static const std::vector<std::string> vocab = {
      "aaaa",
      "bbbb",
      "cccc",
      "dddd",
      "eeee",
      "ffff",
      "gggg",
      "hhhh",
      "iiii",
      "jjjj",
      "kkkk",
      "llll"};
  return fillToInputSize([&](int32_t /*b*/) {
    std::vector<int64_t> keys(kRowsPerBatch);
    std::vector<std::string> values(kRowsPerBatch);
    std::vector<bool> nulls(kRowsPerBatch);
    for (int i = 0; i < kRowsPerBatch; ++i) {
      keys[i] = static_cast<int64_t>(rng());
      nulls[i] = rng() % 10 == 0;
      values[i] = clustered ? vocab[(i / 64) % vocab.size()]
                            : vocab[rng() % vocab.size()];
    }
    return makeBatch(
        {"key", "value"},
        {makePids(numPartitions, rng),
         makeFlat(keys),
         makeStrings(values, nulls)});
  });
}

/// Production shape: tens of thousands of partitions, dictionary-wrapped
/// inputs (the scan-side norm, forcing the indexed row loop), a
/// mixed-length dictionary vocabulary and nulls. With so many partitions
/// every destination line and tail cell goes cold between touches.
std::vector<RowVectorPtr> makeProdBatches(int32_t numPartitions) {
  std::mt19937 rng(41);
  const std::vector<std::string> vocab = {
      "us",
      "euro",
      "yuan",
      "franc",
      "krona",
      "peso",
      "real",
      "rupee",
      "won",
      "zloty",
      "baht",
      "lira"};
  return fillToInputSize([&](int32_t /*b*/) {
    std::vector<int64_t> keys(kRowsPerBatch);
    std::vector<std::string> values(kRowsPerBatch);
    std::vector<bool> nulls(kRowsPerBatch);
    for (int i = 0; i < kRowsPerBatch; ++i) {
      keys[i] = static_cast<int64_t>(rng());
      nulls[i] = rng() % 10 == 0;
      values[i] = vocab[rng() % vocab.size()];
    }
    auto indices =
        AlignedBuffer::allocate<vector_size_t>(kRowsPerBatch, leafPool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (int i = 0; i < kRowsPerBatch; ++i) {
      rawIndices[i] = i;
    }
    std::shuffle(rawIndices, rawIndices + kRowsPerBatch, rng);
    const auto wrap = [&](VectorPtr base) {
      return BaseVector::wrapInDictionary(
          nullptr, indices, kRowsPerBatch, std::move(base));
    };
    return makeBatch(
        {"key", "cur"},
        {makePids(numPartitions, rng),
         wrap(makeFlat(keys)),
         wrap(makeStrings(values, nulls))});
  });
}

struct ScenarioSpec {
  const char* name;
  int32_t numPartitions;
  std::function<std::vector<RowVectorPtr>(int32_t numPartitions)> make;
};

const std::vector<ScenarioSpec>& scenarioSpecs() {
  static const std::vector<ScenarioSpec> specs = {
      // Spark's default partition count, then wider fan-outs.
      {"fixed4_P200", 200, [](int32_t p) { return makeFixedBatches(p); }},
      {"fixed4_P1024", 1024, [](int32_t p) { return makeFixedBatches(p); }},
      {"fixed4_P4096", 4096, [](int32_t p) { return makeFixedBatches(p); }},
      // Null-cost isolation: fixed4_P1024 with nulls on every column.
      {"fixed4nulls10_P1024",
       1024,
       [](int32_t p) { return makeFixedBatches(p, 10); }},
      {"fixed4nulls40_P1024",
       1024,
       [](int32_t p) { return makeFixedBatches(p, 40); }},
      // fixed4_P1024 plus one all-null constant column: its cost should be
      // near zero (whole-batch counted nulls, no bitmap, no values).
      {"fixed4deadcol_P1024",
       1024,
       [](int32_t p) { return makeFixedBatches(p, 0, true); }},
      // Stale nulls buffers on every column (all-set, zero actual nulls):
      // the batch classification must keep the no-null row loop.
      {"fixed4stalenulls_P1024",
       1024,
       [](int32_t p) { return makeFixedBatches(p, 0, false, true); }},
      {"wide100sparse_P200", 200, makeWideSparseNullBatches},
      {"strings_P1024", 1024, makeStringBatches},
      // Dictionary-friendly strings, random and clustered order.
      {"strdict_P1024",
       1024,
       [](int32_t p) { return makeDictStringBatches(p, false); }},
      {"strdictclus_P1024",
       1024,
       [](int32_t p) { return makeDictStringBatches(p, true); }},
      // Production shape: huge P, indexed inputs, mixed-length vocabulary.
      {"prod_P30000", 30000, makeProdBatches},
  };
  return specs;
}

/// main() resets it before returning: static destruction would otherwise
/// free its vectors after the pool they came from.
std::unique_ptr<Scenario>& residentScenario() {
  static std::unique_ptr<Scenario> current;
  return current;
}

/// The named scenario, built on first use. Only one is resident: the
/// benchmarks of a scenario run back to back, and at --input_mb several
/// would not fit in memory together.
const Scenario& getScenario(const std::string& name) {
  auto& current = residentScenario();
  if (current == nullptr || current->name != name) {
    current.reset();
    const auto& specs = scenarioSpecs();
    const auto it =
        std::find_if(specs.begin(), specs.end(), [&](const auto& spec) {
          return name == spec.name;
        });
    BOLT_CHECK(it != specs.end(), "unknown scenario {}", name);
    current = std::make_unique<Scenario>(
        Scenario{it->name, it->numPartitions, it->make(it->numPartitions)});
  }
  return *current;
}

/// Shallow copies of a scenario's batches for one lifecycle. The legacy
/// writers flatten their input's children in place (ensureFlatten); without
/// copies every later lifecycle would split pre-flattened input.
std::vector<RowVectorPtr> lifecycleBatches(const Scenario& scenario) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(scenario.batches.size());
  for (const auto& batch : scenario.batches) {
    batches.push_back(std::make_shared<RowVector>(
        leafPool(),
        batch->type(),
        BufferPtr(nullptr),
        batch->size(),
        batch->children()));
  }
  return batches;
}

struct Written {
  int64_t rows{0};
  int64_t bytes{0};
  int64_t splitNs{0};
  int64_t stopNs{0};
  int64_t compressNs{0};
  int64_t writeNs{0};
  int64_t evictedBytes{0};
  int64_t windows{0};
};

std::map<std::string, Written>& results() {
  static std::map<std::string, Written> map;
  return map;
}

/// One writer lifecycle over a scenario; returns rows written. A nonzero
/// `cellCapBytes` sets the Cell chunk-memory spill threshold.
size_t runWriter(
    int32_t writerType,
    const char* scenarioName,
    bool disableDict = false,
    int64_t cellCapBytes = 0) {
  const Scenario* scenario = nullptr;
  std::vector<RowVectorPtr> batches;
  std::shared_ptr<ShuffleWriter> writer;
  std::string dataFile;
  std::unique_ptr<BoltArrowMemoryPool> arrowPool;
  BENCHMARK_SUSPEND {
    scenario = &getScenario(scenarioName);
    batches = lifecycleBatches(*scenario);
    // Earlier lifecycles' dirty pages would otherwise be written back
    // during this one's stop().
    ::sync();
    char pathTemplate[] = "/tmp/bolt_cell_split_bench_XXXXXX";
    const int fd = ::mkstemp(pathTemplate);
    BOLT_CHECK_GE(fd, 0);
    ::close(fd);
    dataFile = pathTemplate;

    static const std::string spillDir = [] {
      char dirTemplate[] = "/tmp/bolt_cell_split_bench_dir_XXXXXX";
      const char* dir = ::mkdtemp(dirTemplate);
      BOLT_CHECK_NOT_NULL(dir);
      return std::string(dir);
    }();
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.forceShuffleWriterType = writerType;
    options.partitionWriterOptions.numPartitions = scenario->numPartitions;
    options.partitionWriterOptions.dataFile = dataFile;
    options.partitionWriterOptions.configuredDirs = {spillDir};
    options.partitionWriterOptions.numSubDirs = 1;
    options.cellOptions.enableStringDictionary = !disableDict;
    options.cellOptions.cellMemoryCapBytes = cellCapBytes;
    arrowPool = std::make_unique<BoltArrowMemoryPool>(leafPool());
    const auto& first = scenario->batches[0];
    writer = BoltShuffleWriter::createShuffleWriter(
        options,
        first->type()->size() - 1,
        first->size(),
        first->estimateFlatSize(),
        kMemLimit,
        leafPool(),
        arrowPool.get());
  }
  size_t rows = 0;
  const uint64_t splitStart = clockNs();
  for (const auto& batch : batches) {
    const auto status = writer->split(batch, kMemLimit);
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    rows += batch->size();
  }
  const uint64_t stopStart = clockNs();
  const auto status = writer->stop();
  const uint64_t stopEnd = clockNs();
  BOLT_CHECK(status.ok(), "{}", status.ToString());
  BENCHMARK_SUSPEND {
    std::string label = writerType == 4 ? "Cell"
        : writerType == 2               ? "V2"
                                        : "V1";
    if (disableDict) {
      label += "-nodict";
    }
    if (cellCapBytes > 0) {
      label += "-cap" + std::to_string(cellCapBytes >> 20) + "M";
    }
    const auto& metrics = writer->metrics();
    auto& slot = results()[scenario->name + "/" + label];
    slot.rows = static_cast<int64_t>(rows);
    slot.bytes = metrics.totalBytesWritten;
    slot.splitNs = stopStart - splitStart;
    slot.stopNs = stopEnd - stopStart;
    slot.compressNs = metrics.totalCompressTime;
    slot.writeNs = metrics.totalWriteTime;
    slot.evictedBytes = metrics.totalBytesEvicted;
    slot.windows = metrics.spillCount;
    writer.reset();
    arrowPool.reset();
    batches.clear();
    ::unlink(dataFile.c_str());
    ::unlink((dataFile + ".cellspill").c_str());
  }
  return rows;
}

#define WRITE_BENCH(scenario)                \
  BENCHMARK_MULTI(write_##scenario##_Cell) { \
    return runWriter(4, #scenario);          \
  }                                          \
  BENCHMARK_MULTI(write_##scenario##_V1) {   \
    return runWriter(1, #scenario);          \
  }

#define WRITE_BENCH_V2(scenario)           \
  BENCHMARK_MULTI(write_##scenario##_V2) { \
    return runWriter(2, #scenario);        \
  }

#define WRITE_BENCH_NODICT(scenario)                      \
  BENCHMARK_MULTI(write_##scenario##_CellNoDict) {        \
    return runWriter(4, #scenario, /*disableDict=*/true); \
  }

/// Cell under a chunk-memory threshold below its input, so the lifecycle
/// spills and merges as a memory-bound task does.
#define WRITE_BENCH_CAP(scenario)                                         \
  BENCHMARK_MULTI(write_##scenario##_CellCap) {                           \
    return runWriter(4, #scenario, /*disableDict=*/false, kCellCapBytes); \
  }

WRITE_BENCH(fixed4_P200)
WRITE_BENCH_V2(fixed4_P200)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(fixed4_P1024)
WRITE_BENCH_V2(fixed4_P1024)
WRITE_BENCH_CAP(fixed4_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(fixed4_P4096)
WRITE_BENCH_V2(fixed4_P4096)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(fixed4nulls10_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(fixed4nulls40_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(fixed4deadcol_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(fixed4stalenulls_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(wide100sparse_P200)

WRITE_BENCH(strings_P1024)
WRITE_BENCH_V2(strings_P1024)
WRITE_BENCH_CAP(strings_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(strdict_P1024)
WRITE_BENCH_NODICT(strdict_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(strdictclus_P1024)
WRITE_BENCH_NODICT(strdictclus_P1024)
BENCHMARK_DRAW_LINE();
WRITE_BENCH(prod_P30000)
WRITE_BENCH_NODICT(prod_P30000)
BENCHMARK_DRAW_LINE();

// ---- Phase breakdown (--phases) -------------------------------------------

RowTypePtr dataRowType(const Scenario& scenario) {
  const auto& row = scenario.batches[0]->type()->asRow();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (uint32_t i = 1; i < row.size(); ++i) {
    names.push_back(row.nameOf(i));
    types.push_back(row.childAt(i));
  }
  return ROW(std::move(names), std::move(types));
}

/// One arrow stream per partition slice of the in-memory data file.
class BufferStreamIterator final : public ReaderStreamIterator {
 public:
  BufferStreamIterator(
      const std::string* file,
      const std::vector<int64_t>& lengths)
      : file_(file), lengths_(lengths) {}

  std::shared_ptr<arrow::io::InputStream> nextStream(
      arrow::MemoryPool* /*pool*/) override {
    while (next_ < lengths_.size() && lengths_[next_] == 0) {
      offset_ += lengths_[next_++];
    }
    if (next_ == lengths_.size()) {
      return nullptr;
    }
    auto stream = std::make_shared<arrow::io::BufferReader>(
        reinterpret_cast<const uint8_t*>(file_->data()) + offset_,
        lengths_[next_]);
    offset_ += lengths_[next_++];
    return stream;
  }

  void close() override {}
  void updateMetrics(
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t) override {}

 private:
  const std::string* const file_;
  const std::vector<int64_t>& lengths_;
  size_t next_{0};
  int64_t offset_{0};
};

/// Delegating codec that meters decompression, the seam the cell reader
/// cannot meter itself.
class TimingCodec final : public Codec {
 public:
  explicit TimingCodec(Codec* inner)
      : Codec(CodecType::LZ4_FRAME, CodecOptions{CodecBackend::NONE}),
        inner_(inner) {}

  int64_t compress(const uint8_t* in, int64_t n, uint8_t* out, int64_t cap)
      override {
    return inner_->compress(in, n, out, cap);
  }

  int64_t decompress(const uint8_t* in, int64_t n, uint8_t* out, int64_t cap)
      override {
    const uint64_t start = clockNs();
    const auto result = inner_->decompress(in, n, out, cap);
    decompressNs += clockNs() - start;
    return result;
  }

  int64_t maxCompressedLen(int64_t n) const override {
    return inner_->maxCompressedLen(n);
  }

  int32_t defaultCompressionLevel() const override {
    return inner_->defaultCompressionLevel();
  }

  uint64_t decompressNs{0};

 private:
  Codec* const inner_;
};

struct WriterPhases {
  double splitMs{0};
  double stopMs{0};
  double compressMs{0};
  double writeMs{0};
  double evictMs{0};
  int64_t bytes{0};
  int64_t rows{0};
  std::vector<int64_t> partitionLengths;
  std::string file; // the data file, read back for the reader phase
};

struct ReaderPhases {
  double wallMs{0};
  double decompressMs{0};
  double decodeMs{0}; // payload -> vectors (cell decode / legacy deserialize)
  double mergeMs{0}; // legacy payload merge; zero for cell
  double glueMs{0}; // wall minus the metered parts
  int64_t rows{0};
};

WriterPhases runWriterPhases(
    int32_t writerType,
    bool disableDict,
    const Scenario& scenario) {
  WriterPhases result;
  ::sync();
  char pathTemplate[] = "/tmp/bolt_cell_phase_bench_XXXXXX";
  const int fd = ::mkstemp(pathTemplate);
  BOLT_CHECK_GE(fd, 0);
  ::close(fd);
  const std::string dataFile = pathTemplate;

  static const std::string spillDir = [] {
    char dirTemplate[] = "/tmp/bolt_cell_phase_bench_dir_XXXXXX";
    const char* dir = ::mkdtemp(dirTemplate);
    BOLT_CHECK_NOT_NULL(dir);
    return std::string(dir);
  }();
  ShuffleWriterOptions options;
  options.partitioning = Partitioning::kHash;
  options.forceShuffleWriterType = writerType;
  options.partitionWriterOptions.numPartitions = scenario.numPartitions;
  options.partitionWriterOptions.dataFile = dataFile;
  options.partitionWriterOptions.configuredDirs = {spillDir};
  options.partitionWriterOptions.numSubDirs = 1;
  options.cellOptions.enableStringDictionary = !disableDict;
  auto arrowPool = std::make_unique<BoltArrowMemoryPool>(leafPool());
  const auto& first = scenario.batches[0];
  auto writer = BoltShuffleWriter::createShuffleWriter(
      options,
      first->type()->size() - 1,
      first->size(),
      first->estimateFlatSize(),
      kMemLimit,
      leafPool(),
      arrowPool.get());

  const auto batches = lifecycleBatches(scenario);
  uint64_t splitNs = 0;
  for (const auto& batch : batches) {
    const uint64_t start = clockNs();
    const auto status = writer->split(batch, kMemLimit);
    splitNs += clockNs() - start;
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    result.rows += batch->size();
  }
  const uint64_t stopStart = clockNs();
  const auto status = writer->stop();
  const uint64_t stopNs = clockNs() - stopStart;
  BOLT_CHECK(status.ok(), "{}", status.ToString());

  const auto& metrics = writer->metrics();
  result.splitMs = splitNs / 1e6;
  result.stopMs = stopNs / 1e6;
  result.compressMs = metrics.totalCompressTime / 1e6;
  result.writeMs = metrics.totalWriteTime / 1e6;
  result.evictMs = metrics.totalEvictTime / 1e6;
  result.bytes = metrics.totalBytesWritten;
  result.partitionLengths = metrics.partitionLengths;
  {
    std::ifstream in(dataFile, std::ios::binary);
    result.file.assign(
        std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }
  writer.reset();
  arrowPool.reset();
  ::unlink(dataFile.c_str());
  ::unlink((dataFile + ".cellspill").c_str());
  return result;
}

ReaderPhases runCellReaderPhases(
    const Scenario& scenario,
    const WriterPhases& written) {
  ReaderPhases result;
  const auto rowType = dataRowType(scenario);
  auto codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
  TimingCodec timing(codec.get());
  auto streams = std::make_shared<BufferStreamIterator>(
      &written.file, written.partitionLengths);
  cell::CellShuffleReader reader(
      streams,
      rowType,
      &timing,
      arrow::default_memory_pool(),
      leafPool(),
      /*batchSize=*/4096,
      /*batchByteSize=*/1 << 20);
  const uint64_t start = clockNs();
  while (auto batch = reader.next()) {
    result.rows += batch->size();
  }
  result.wallMs = (clockNs() - start) / 1e6;
  result.decompressMs = timing.decompressNs / 1e6;
  result.decodeMs = (reader.decodeTimeNs() - timing.decompressNs) / 1e6;
  result.glueMs = result.wallMs - reader.decodeTimeNs() / 1e6;
  return result;
}

ReaderPhases runLegacyReaderPhases(
    int32_t writerType,
    const Scenario& scenario,
    const WriterPhases& written) {
  ReaderPhases result;
  const auto rowType = dataRowType(scenario);
  auto schema = boltTypeToArrowSchema(rowType, leafPool());
  std::shared_ptr<Codec> codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
  BoltColumnarBatchDeserializerFactory factory(
      schema,
      codec,
      rowType,
      /*batchSize=*/4096,
      /*shuffleBatchByteSize=*/1 << 20,
      arrow::default_memory_pool(),
      leafPool());
  factory.setShuffleWriterType(writerType);
  factory.setNumPartitions(scenario.numPartitions);
  factory.setpartitioningShortName("hash");

  const uint64_t start = clockNs();
  int64_t offset = 0;
  for (const auto length : written.partitionLengths) {
    if (length == 0) {
      continue;
    }
    auto in = std::make_shared<arrow::io::BufferReader>(
        reinterpret_cast<const uint8_t*>(written.file.data()) + offset, length);
    offset += length;
    auto deserializer = factory.createDeserializer(std::move(in));
    while (auto batch = deserializer->next()) {
      result.rows += batch->size();
    }
  }
  result.wallMs = (clockNs() - start) / 1e6;
  result.decompressMs = factory.getDecompressTime() / 1e6;
  result.decodeMs = factory.getDeserializeTime() / 1e6;
  result.mergeMs = factory.getMergeTime() / 1e6;
  result.glueMs =
      result.wallMs - result.decompressMs - result.decodeMs - result.mergeMs;
  return result;
}

void runPhaseTable() {
  const auto& scenario = getScenario(FLAGS_phases_scenario);
  struct Variant {
    const char* name;
    int32_t writerType;
    bool disableDict;
  };
  const Variant variants[] = {
      {"Cell-dict", 4, false},
      {"Cell-nodict", 4, true},
      {"V1", 1, false},
      {"V2", 2, false}};

  printf(
      "\nscenario %s: %d batches x %d rows, P=%d\n\n",
      scenario.name.c_str(),
      static_cast<int>(scenario.batches.size()),
      kRowsPerBatch,
      scenario.numPartitions);
  printf(
      "%-12s %9s %9s | %9s %9s %9s %9s | %12s\n",
      "writer",
      "splitMs",
      "stopMs",
      "cmprsMs",
      "writeMs",
      "evictMs",
      "glueMs",
      "bytesWritten");
  std::vector<std::pair<Variant, WriterPhases>> writes;
  for (const auto& variant : variants) {
    // Best of three lifecycles, chosen by split+stop.
    WriterPhases best;
    for (int rep = 0; rep < 3; ++rep) {
      auto phases =
          runWriterPhases(variant.writerType, variant.disableDict, scenario);
      if (rep == 0 ||
          phases.splitMs + phases.stopMs < best.splitMs + best.stopMs) {
        best = std::move(phases);
      }
    }
    // Glue: lifecycle time not metered by any bucket (assembly, scans).
    const double glue = best.splitMs + best.stopMs - best.compressMs -
        best.writeMs - best.evictMs;
    printf(
        "%-12s %9.2f %9.2f | %9.2f %9.2f %9.2f %9.2f | %12ld\n",
        variant.name,
        best.splitMs,
        best.stopMs,
        best.compressMs,
        best.writeMs,
        best.evictMs,
        glue,
        static_cast<long>(best.bytes));
    writes.emplace_back(variant, std::move(best));
  }

  printf(
      "\n%-12s %9s | %9s %9s %9s %9s | %10s %9s\n",
      "reader",
      "wallMs",
      "dcmpMs",
      "decodeMs",
      "mergeMs",
      "glueMs",
      "rows",
      "nsPerRow");
  for (const auto& [variant, written] : writes) {
    ReaderPhases best;
    for (int rep = 0; rep < 3; ++rep) {
      auto phases = variant.writerType == 4
          ? runCellReaderPhases(scenario, written)
          : runLegacyReaderPhases(variant.writerType, scenario, written);
      BOLT_CHECK_EQ(phases.rows, written.rows, "reader dropped rows");
      if (rep == 0 || phases.wallMs < best.wallMs) {
        best = std::move(phases);
      }
    }
    printf(
        "%-12s %9.2f | %9.2f %9.2f %9.2f %9.2f | %10ld %9.2f\n",
        variant.name,
        best.wallMs,
        best.decompressMs,
        best.decodeMs,
        best.mergeMs,
        best.glueMs,
        static_cast<long>(best.rows),
        best.rows > 0 ? best.wallMs * 1e6 / best.rows : 0.0);
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::Options options;
  options.allocatorCapacity = 16LL << 30;
  memory::MemoryManager::initialize(options);
  if (FLAGS_phases) {
    runPhaseTable();
    residentScenario().reset();
    return 0;
  }
  folly::runBenchmarks();
  residentScenario().reset();

  // The last lifecycle of each benchmark.
  printf(
      "\n%-36s %10s %12s %9s %9s %10s %9s %7s %7s\n",
      "scenario/writer",
      "rows",
      "bytesWritten",
      "splitMs",
      "stopMs",
      "compressMs",
      "writeMs",
      "evictMB",
      "windows");
  for (const auto& [name, written] : results()) {
    printf(
        "%-36s %10ld %12ld %9.1f %9.1f %10.1f %9.1f %7.0f %7ld\n",
        name.c_str(),
        static_cast<long>(written.rows),
        static_cast<long>(written.bytes),
        written.splitNs / 1e6,
        written.stopNs / 1e6,
        written.compressNs / 1e6,
        written.writeNs / 1e6,
        written.evictedBytes / 1048576.0,
        static_cast<long>(written.windows));
  }
  return 0;
}
