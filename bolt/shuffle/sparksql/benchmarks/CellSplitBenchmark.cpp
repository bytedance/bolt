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

/// Split-path comparison of the shuffle writers on identical inputs.
///
/// Every benchmark drives one writer lifecycle (construct, split a fixed set
/// of batches, stop) and returns the rows processed, so folly reports
/// time per row. stop() and construction run under a suspender: the numbers
/// are the split path. A summary table printed at exit adds what the split
/// numbers cannot show: bytes written per writer per scenario.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

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

DEFINE_bool(
    phases,
    false,
    "Skip the folly benchmarks; print a writer/reader phase breakdown for "
    "the dictionary string scenario across Cell(dict on/off)/V1/V2.");

using namespace bytedance::bolt;
using namespace bytedance::bolt::shuffle::sparksql;

namespace {

constexpr int32_t kRowsPerBatch = 4096;
constexpr int32_t kBatches = 96;
constexpr int64_t kMemLimit = 1LL << 30;

struct Scenario {
  std::string name;
  int32_t numPartitions;
  std::vector<RowVectorPtr> batches; // pid column first
};

memory::MemoryPool* leafPool() {
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
  ::memcpy(buffer->template asMutable<T>(), values.data(), values.size() * sizeof(T));
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

/// Frame-of-reference friendly bigints, bit-packable ints.
Scenario makeFixedScenario(
    int32_t numPartitions,
    int32_t numBatches = kBatches,
    const char* tag = "",
    int32_t nullPercent = 0,
    bool addAllNullColumn = false,
    bool defensiveNulls = false) {
  std::mt19937 rng(17);
  Scenario scenario{
      "fixed4" + std::string(tag) + "_P" + std::to_string(numPartitions),
      numPartitions,
      {}};
  for (int b = 0; b < numBatches; ++b) {
    std::vector<int64_t> ids(kRowsPerBatch);
    std::vector<int64_t> timestamps(kRowsPerBatch);
    std::vector<int32_t> smalls(kRowsPerBatch);
    std::vector<int32_t> randoms(kRowsPerBatch);
    for (int i = 0; i < kRowsPerBatch; ++i) {
      ids[i] = 7'000'000'000LL + b * kRowsPerBatch + i;
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
    scenario.batches.push_back(makeBatch(std::move(names), std::move(children)));
  }
  return scenario;
}

Scenario makeStringScenario(int32_t numPartitions) {
  std::mt19937 rng(23);
  Scenario scenario{
      "strings_P" + std::to_string(numPartitions), numPartitions, {}};
  for (int b = 0; b < kBatches; ++b) {
    std::vector<int64_t> keys(kRowsPerBatch);
    std::vector<std::string> values(kRowsPerBatch);
    std::vector<bool> nulls(kRowsPerBatch);
    for (int i = 0; i < kRowsPerBatch; ++i) {
      keys[i] = static_cast<int64_t>(rng());
      nulls[i] = rng() % 10 == 0;
      values[i].assign(8 + rng() % 92, static_cast<char>('a' + rng() % 26));
    }
    scenario.batches.push_back(makeBatch(
        {"key", "value"},
        {makePids(numPartitions, rng),
         makeFlat(keys),
         makeStrings(values, nulls)}));
  }
  return scenario;
}

/// Low-cardinality strings: 12 distinct 4-char values (60 serialized
/// bytes, inside the single-dictionary probe criterion). `clustered` feeds
/// the values in runs of 64, the shape the MRU check is built for.
Scenario makeDictStringScenario(
    int32_t numPartitions,
    int32_t numBatches,
    const char* tag,
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
  Scenario scenario{
      std::string("strdict") + tag + "_P" + std::to_string(numPartitions),
      numPartitions,
      {}};
  for (int b = 0; b < numBatches; ++b) {
    std::vector<int64_t> keys(kRowsPerBatch);
    std::vector<std::string> values(kRowsPerBatch);
    std::vector<bool> nulls(kRowsPerBatch);
    for (int i = 0; i < kRowsPerBatch; ++i) {
      keys[i] = static_cast<int64_t>(rng());
      nulls[i] = rng() % 10 == 0;
      values[i] = clustered ? vocab[(i / 64) % vocab.size()]
                            : vocab[rng() % vocab.size()];
    }
    scenario.batches.push_back(makeBatch(
        {"key", "value"},
        {makePids(numPartitions, rng),
         makeFlat(keys),
         makeStrings(values, nulls)}));
  }
  return scenario;
}

const std::vector<Scenario>& scenarios() {
  static std::vector<Scenario> all = [] {
    std::vector<Scenario> s;
    s.push_back(makeFixedScenario(64));
    s.push_back(makeFixedScenario(1024));
    s.push_back(makeFixedScenario(4096));
    s.push_back(makeStringScenario(1024));
    // Large-volume variants: ~100MB of raw column data per writer lifecycle,
    // so the destinations blow past the LLC and the split loop's memory
    // behavior, not per-lifecycle provisioning, dominates.
    s.push_back(makeFixedScenario(1024, 1024, "big"));
    s.push_back(makeFixedScenario(4096, 1024, "big"));
    // Null-cost isolation: identical shape to fixed4big_P1024 with nulls on
    // every column.
    s.push_back(makeFixedScenario(1024, 1024, "nulls10big", 10));
    s.push_back(makeFixedScenario(1024, 1024, "nulls40big", 40));
    // fixed4big plus one all-null constant column: its cost should be near
    // zero (whole-batch counted nulls, no bitmap, no values).
    s.push_back(makeFixedScenario(1024, 1024, "deadcolbig", 0, true));
    // Stale nulls buffers on every column (all-set, zero actual nulls): the
    // batch classification must keep the no-null row loop.
    s.push_back(makeFixedScenario(1024, 1024, "stalenullsbig", 0, false, true));
    // Dictionary-friendly strings, random and clustered order.
    s.push_back(makeDictStringScenario(1024, 1024, "big", false));
    s.push_back(makeDictStringScenario(1024, 1024, "clusbig", true));
    return s;
  }();
  return all;
}

struct Written {
  int64_t bytes{0};
  int64_t rawBytes{0};
  int64_t compressNs{0};
  int64_t writeNs{0};
  int64_t rows{0};
};

std::map<std::string, Written>& results() {
  static std::map<std::string, Written> map;
  return map;
}

/// One writer lifecycle over a scenario; returns rows split.
size_t runWriter(
    int32_t writerType,
    const Scenario& scenario,
    bool disableDict = false) {
  std::shared_ptr<ShuffleWriter> writer;
  std::string dataFile;
  std::unique_ptr<BoltArrowMemoryPool> arrowPool;
  BENCHMARK_SUSPEND {
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
    options.partitionWriterOptions.numPartitions = scenario.numPartitions;
    options.partitionWriterOptions.dataFile = dataFile;
    options.partitionWriterOptions.configuredDirs = {spillDir};
    options.partitionWriterOptions.numSubDirs = 1;
    // A production-typical sizing budget; the benchmark pool's capacity is
    // not a meaningful signal for it.
    options.cellOptions.cellMemoryBudgetBytes = 512LL << 20;
    options.cellOptions.enableStringDictionary = !disableDict;
    arrowPool = std::make_unique<BoltArrowMemoryPool>(leafPool());
    const auto& first = scenario.batches[0];
    writer = BoltShuffleWriter::create(
        options,
        asRowType(first->type()),
        first->type()->size() - 1,
        first->size(),
        first->estimateFlatSize(),
        kMemLimit,
        leafPool(),
        arrowPool.get());
  }
  size_t rows = 0;
  for (const auto& batch : scenario.batches) {
    const auto status = writer->split(batch, kMemLimit);
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    rows += batch->size();
  }
  BENCHMARK_SUSPEND {
    const auto status = writer->stop();
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    const auto& metrics = writer->metrics();
    auto& slot = results()
        [scenario.name + "/" +
         (writerType == 4 ? (disableDict ? "Cell-nodict" : "Cell")
                          : writerType == 2 ? "V2" : "V1")];
    slot.bytes = metrics.totalBytesWritten;
    int64_t raw = 0;
    for (const auto length : metrics.rawPartitionLengths) {
      raw += length;
    }
    slot.rawBytes = raw;
    slot.compressNs = metrics.totalCompressTime;
    slot.writeNs = metrics.totalWriteTime;
    slot.rows = static_cast<int64_t>(rows);
    writer.reset();
    arrowPool.reset();
    ::unlink(dataFile.c_str());
    ::unlink((dataFile + ".cellspill").c_str());
  }
  return rows;
}

#define CELL_SPLIT_BENCH(scenarioIndex, suffix)                          \
  BENCHMARK_MULTI(split_##suffix##_Cell) {                               \
    return runWriter(4, scenarios()[scenarioIndex]);                     \
  }                                                                      \
  BENCHMARK_MULTI(split_##suffix##_V1) {                        \
    return runWriter(1, scenarios()[scenarioIndex]);                     \
  }                                                                      \
  BENCHMARK_MULTI(split_##suffix##_V2) {                        \
    return runWriter(2, scenarios()[scenarioIndex]);                     \
  }                                                                      \
  BENCHMARK_DRAW_LINE();

CELL_SPLIT_BENCH(0, fixed4_P64)
CELL_SPLIT_BENCH(1, fixed4_P1024)
CELL_SPLIT_BENCH(2, fixed4_P4096)
CELL_SPLIT_BENCH(3, strings_P1024)
CELL_SPLIT_BENCH(4, fixed4big_P1024)
CELL_SPLIT_BENCH(5, fixed4big_P4096)
CELL_SPLIT_BENCH(6, fixed4nulls10big_P1024)
CELL_SPLIT_BENCH(7, fixed4nulls40big_P1024)
CELL_SPLIT_BENCH(8, fixed4deadcolbig_P1024)
CELL_SPLIT_BENCH(9, fixed4stalenullsbig_P1024)

BENCHMARK_MULTI(split_strdictbig_P1024_Cell) {
  return runWriter(4, scenarios()[10]);
}
BENCHMARK_MULTI(split_strdictbig_P1024_CellNoDict) {
  return runWriter(4, scenarios()[10], /*disableDict=*/true);
}
BENCHMARK_MULTI(split_strdictbig_P1024_V1) {
  return runWriter(1, scenarios()[10]);
}
BENCHMARK_DRAW_LINE();
BENCHMARK_MULTI(split_strdictclusbig_P1024_Cell) {
  return runWriter(4, scenarios()[11]);
}
BENCHMARK_MULTI(split_strdictclusbig_P1024_CellNoDict) {
  return runWriter(4, scenarios()[11], /*disableDict=*/true);
}
BENCHMARK_MULTI(split_strdictclusbig_P1024_V1) {
  return runWriter(1, scenarios()[11]);
}
BENCHMARK_DRAW_LINE();

// ---- Phase breakdown (--phases) -------------------------------------------

uint64_t phaseNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

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
  void updateMetrics(int64_t, int64_t, int64_t, int64_t, int64_t, int64_t,
                     int64_t, int64_t) override {}

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
    const uint64_t start = phaseNowNs();
    const auto result = inner_->decompress(in, n, out, cap);
    decompressNs += phaseNowNs() - start;
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
  options.cellOptions.cellMemoryBudgetBytes = 512LL << 20;
  options.cellOptions.enableStringDictionary = !disableDict;
  auto arrowPool = std::make_unique<BoltArrowMemoryPool>(leafPool());
  const auto& first = scenario.batches[0];
  auto writer = BoltShuffleWriter::create(
      options,
      asRowType(first->type()),
      first->type()->size() - 1,
      first->size(),
      first->estimateFlatSize(),
      kMemLimit,
      leafPool(),
      arrowPool.get());

  uint64_t splitNs = 0;
  for (const auto& batch : scenario.batches) {
    const uint64_t start = phaseNowNs();
    const auto status = writer->split(batch, kMemLimit);
    splitNs += phaseNowNs() - start;
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    result.rows += batch->size();
  }
  const uint64_t stopStart = phaseNowNs();
  const auto status = writer->stop();
  const uint64_t stopNs = phaseNowNs() - stopStart;
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
      cell::CellLayout::create(rowType),
      &timing,
      arrow::default_memory_pool(),
      leafPool(),
      /*batchSize=*/4096,
      /*batchByteSize=*/1 << 20);
  const uint64_t start = phaseNowNs();
  while (auto batch = reader.next()) {
    result.rows += batch->size();
  }
  result.wallMs = (phaseNowNs() - start) / 1e6;
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

  const uint64_t start = phaseNowNs();
  int64_t offset = 0;
  for (const auto length : written.partitionLengths) {
    if (length == 0) {
      continue;
    }
    auto in = std::make_shared<arrow::io::BufferReader>(
        reinterpret_cast<const uint8_t*>(written.file.data()) + offset,
        length);
    offset += length;
    auto deserializer = factory.createDeserializer(std::move(in));
    while (auto batch = deserializer->next()) {
      result.rows += batch->size();
    }
  }
  result.wallMs = (phaseNowNs() - start) / 1e6;
  result.decompressMs = factory.getDecompressTime() / 1e6;
  result.decodeMs = factory.getDeserializeTime() / 1e6;
  result.mergeMs = factory.getMergeTime() / 1e6;
  result.glueMs =
      result.wallMs - result.decompressMs - result.decodeMs - result.mergeMs;
  return result;
}

void runPhaseTable() {
  const auto& scenario = scenarios()[10]; // strdictbig_P1024
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
    return 0;
  }
  folly::runBenchmarks();

  printf(
      "\n%-32s %12s %12s %11s %11s %11s\n",
      "scenario/writer",
      "bytesWritten",
      "rawBytes",
      "compressMs",
      "cmpNsPerRow",
      "writeMs");
  for (const auto& [name, written] : results()) {
    printf(
        "%-32s %12ld %12ld %11.2f %11.2f %11.2f\n",
        name.c_str(),
        static_cast<long>(written.bytes),
        static_cast<long>(written.rawBytes),
        written.compressNs / 1e6,
        written.rows > 0 ? static_cast<double>(written.compressNs) / written.rows
                         : 0.0,
        written.writeNs / 1e6);
  }
  return 0;
}
