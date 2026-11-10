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

#include <gtest/gtest.h>

#include <unistd.h>
#include <array>
#include <cstdio>
#include <functional>
#include <random>
#include <set>
#include <span>

#include <fmt/format.h>

#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/tests/CellNestedTestUtils.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

/// The exact bytes the Cell writer produces, pinned. For flat schemas,
/// every flat type in every null shape and input encoding; string
/// dictionaries, their successors and their fallbacks; partition and
/// null-memory seals, spills and both outputs, alone and together; at
/// several partition counts, uncompressed and LZ4. For nested schemas,
/// arrays, maps and structs four levels deep in every input encoding,
/// struct fields with no column on the wire, partition seals, and the
/// adapter's binary column. A change to the writer that is meant to leave
/// its bytes alone must keep every hash.
///
/// Nothing here depends on the machine or the clock: the input comes from
/// fixed seeds through the standard engines alone, and the writer runs on a
/// pool of fixed capacity, which alone sizes its cells. The LZ4 cases also
/// pin the LZ4 library's output.
namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

using test::ShuffleInput;

/// FNV-1a, a fixed function of the bytes.
uint64_t fnv1a(uint64_t hash, const void* data, size_t size) {
  const auto* bytes = static_cast<const uint8_t*>(data);
  for (size_t i = 0; i < size; ++i) {
    hash = (hash ^ bytes[i]) * 0x100000001b3ULL;
  }
  return hash;
}

uint64_t fnv1a(uint64_t hash, uint64_t value) {
  uint8_t bytes[8];
  for (int i = 0; i < 8; ++i) {
    bytes[i] = static_cast<uint8_t>(value >> (8 * i));
  }
  return fnv1a(hash, bytes, sizeof(bytes));
}

/// The hash of what `written` holds: each partition's length, from an RSS
/// client the number and sizes of its pushes, and its bytes.
uint64_t hashOf(const test::WrittenShuffle& written, bool rss) {
  uint64_t hash = 0xcbf29ce484222325ULL;
  for (size_t pid = 0; pid < written.partitions.size(); ++pid) {
    hash = fnv1a(hash, written.metrics.partitionLengths[pid]);
    if (rss) {
      hash = fnv1a(hash, written.pushes[pid].size());
      for (const auto& push : written.pushes[pid]) {
        hash = fnv1a(hash, push.size());
      }
    }
    hash = fnv1a(
        hash, written.partitions[pid].data(), written.partitions[pid].size());
  }
  return hash;
}

/// How a column holds its rows in one batch.
enum class Shape {
  kNoNulls,
  kSomeNulls,
  /// A nulls buffer with every row set: no row is null.
  kStaleNulls,
  /// A flat vector whose every row is null.
  kAllNull,
  kConstantNull,
  /// Some rows null in the base and some in the wrapping.
  kDictionary,
  kDictionaryNoNulls,
  /// Every row null through the wrapping.
  kDictionaryAllNull,
  kConstant,
  /// Loads as a dictionary over values with nulls.
  kLazy,
  kCount,
};

constexpr uint32_t kShapes = static_cast<uint32_t>(Shape::kCount);

/// What a batch of the input is for.
enum class BatchKind {
  /// The first batch, which decides the string dictionaries.
  kFirst,
  /// One of kShapes batches that give every column every shape once.
  kCycled,
  /// Too few rows for the shapes to matter: none, one, 64.
  kSmall,
  /// Every column null throughout, by dictionary wrapping.
  kWrappedNull,
  /// Every column null throughout, by constants.
  kConstantNull,
};

struct BatchPlan {
  vector_size_t rows;
  BatchKind kind;
};

/// The input's batches, in order. The low-cardinality column takes a first
/// vocabulary up to kSecondVocabularyBatch, a second one up to
/// kLongValuesBatch, and from there both with longer words and now and then
/// a value too long for any dictionary.
constexpr BatchPlan kBatches[] = {
    {2048, BatchKind::kFirst},
    {1000, BatchKind::kCycled},
    {3333, BatchKind::kCycled},
    {1, BatchKind::kSmall},
    {4096, BatchKind::kCycled},
    {777, BatchKind::kWrappedNull},
    {0, BatchKind::kSmall},
    {2500, BatchKind::kCycled},
    {4096, BatchKind::kCycled},
    {64, BatchKind::kSmall},
    {1500, BatchKind::kCycled},
    {3000, BatchKind::kCycled},
    {1200, BatchKind::kConstantNull},
    {2048, BatchKind::kCycled},
    {2600, BatchKind::kCycled},
    {1800, BatchKind::kCycled},
};
constexpr uint32_t kSecondVocabularyBatch = 10;
constexpr uint32_t kLongValuesBatch = 13;

/// One shuffle's writer configuration.
struct Scenario {
  const char* name;
  int32_t numPartitions;
  bool lz4;
  bool rss;
  std::function<void(CellShuffleOptions&)> tune;
  /// Whether windows seal, and whether a local writer spills.
  bool seals;
  bool spills;
};

/// The recorded output of a scenario: total bytes and their hash.
struct Golden {
  const char* name;
  int64_t bytes;
  uint64_t hash;
};

/// Expects `actual` to be `recorded`, scenario by scenario, and else gives
/// the table as the writer now produces it.
void expectRecorded(
    const std::vector<Golden>& actual,
    std::span<const Golden> recorded) {
  std::string table;
  bool same = actual.size() == recorded.size();
  for (size_t i = 0; i < actual.size(); ++i) {
    const auto& golden = actual[i];
    const Golden* expected =
        i < recorded.size() && std::string_view(recorded[i].name) == golden.name
        ? &recorded[i]
        : nullptr;
    if (expected == nullptr) {
      same = false;
      ADD_FAILURE() << golden.name << ": not recorded";
    } else if (
        golden.bytes != expected->bytes || golden.hash != expected->hash) {
      same = false;
      ADD_FAILURE() << golden.name << ": " << golden.bytes << " bytes, hash "
                    << fmt::format("{:#018x}", golden.hash) << "; recorded "
                    << expected->bytes << " bytes, hash "
                    << fmt::format("{:#018x}", expected->hash);
    }
    table += fmt::format(
        "      {{\"{}\", {}, {:#018x}ULL}},\n",
        golden.name,
        golden.bytes,
        golden.hash);
  }
  // Only for a deliberate change of the writer's bytes: the table as the
  // writer now produces it.
  EXPECT_TRUE(same) << "now:\n" << table;
}

/// A nested shuffle: the input's schema and the options of the maker that
/// makes it, and the writer's configuration.
struct NestedScenario {
  const char* name;
  RowTypePtr type;
  test::NestedVectorMaker::Options options;
  /// How batch b holds column c; flat when not given.
  std::function<test::ColumnEncoding(uint32_t, uint32_t)> encodingOf;
  int32_t numPartitions;
  bool lz4;
  bool rss;
  std::function<void(CellShuffleOptions&)> tune;
  /// Whether payloads seal before the end.
  bool seals;
};

/// The rows of a nested scenario's batches, small ones among them, and its
/// seed.
constexpr vector_size_t kNestedBatches[] = {1500, 1, 0, 2500, 7, 900, 3};
constexpr uint64_t kNestedSeed = 0x6e57ed;
/// The capacity of the writer's pool, its memory limit.
constexpr int64_t kWriterPoolBytes = int64_t{8} << 30;

class CellGoldenTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_cell_golden_test_XXXXXX";
    const int fd = ::mkstemp(pathTemplate);
    ASSERT_GE(fd, 0);
    ::close(fd);
    dataFile_ = pathTemplate;
    // The writer sizes its cells from its pool's capacity: a fixed one, so
    // nothing of the process's memory configuration reaches the bytes.
    writerRoot_ =
        memory::memoryManager()->addRootPool("cellGolden", kWriterPoolBytes);
    writerPool_ = writerRoot_->addLeafChild("writer");
  }

  void TearDown() override {
    removeFiles();
  }

  void removeFiles() {
    test::removeShuffleFiles(dataFile_);
  }

  /// `n` rows, each the next `value()`.
  template <typename T>
  VectorPtr
  fill(const TypePtr& type, vector_size_t n, const std::function<T()>& value) {
    auto vector = BaseVector::create<FlatVector<T>>(type, n, pool());
    for (vector_size_t row = 0; row < n; ++row) {
      vector->set(row, value());
    }
    return vector;
  }

  std::string randomString(size_t length, std::mt19937_64& rng) {
    std::string value(length, ' ');
    for (auto& c : value) {
      c = static_cast<char>('a' + rng() % 26);
    }
    return value;
  }

  /// `n` values of the column of batch `batch`, no nulls.
  VectorPtr values(
      const std::string& name,
      const TypePtr& type,
      vector_size_t n,
      uint32_t batch,
      std::mt19937_64& rng) {
    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return fill<bool>(type, n, [&] { return (rng() & 1) == 1; });
      case TypeKind::TINYINT:
        return fill<int8_t>(
            type, n, [&] { return static_cast<int8_t>(rng()); });
      case TypeKind::SMALLINT:
        return fill<int16_t>(
            type, n, [&] { return static_cast<int16_t>(rng() % 3000); });
      case TypeKind::INTEGER:
        if (type->isDate()) {
          return fill<int32_t>(type, n, [&] {
            return static_cast<int32_t>(18'000 + rng() % 4000);
          });
        }
        return fill<int32_t>(
            type, n, [&] { return static_cast<int32_t>(rng()); });
      case TypeKind::BIGINT:
        if (type->isDecimal()) {
          return fill<int64_t>(type, n, [&] {
            return static_cast<int64_t>(rng() % 19'999'999'999) - 9'999'999'999;
          });
        }
        // A base with narrow offsets: FOR blocks.
        return fill<int64_t>(type, n, [&] {
          return 7'000'000'000'000LL + static_cast<int64_t>(rng() % 100'000);
        });
      case TypeKind::HUGEINT:
        return fill<int128_t>(type, n, [&] {
          const auto high = static_cast<int64_t>(rng() % 1'000'000'000'000);
          const auto low = static_cast<int64_t>(rng() % 1'000'000'000'000);
          return static_cast<int128_t>(high) * 1'000'000'000'000 - low;
        });
      case TypeKind::REAL:
        return fill<float>(type, n, [&] {
          return static_cast<float>(static_cast<int32_t>(rng() % 100'000)) / 4;
        });
      case TypeKind::DOUBLE:
        return fill<double>(type, n, [&] {
          return static_cast<double>(static_cast<int64_t>(rng())) / 1024;
        });
      case TypeKind::TIMESTAMP:
        // Nanoseconds: flat timestamps keep all sixteen bytes.
        return fill<Timestamp>(type, n, [&] {
          const auto seconds =
              static_cast<int64_t>(rng() % 4'000'000'000) - 2'000'000'000;
          const auto nanos = rng() % 1'000'000'000;
          return Timestamp(seconds, nanos);
        });
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        if (name == "lowcard") {
          // Eight words of mixed lengths in 41 serialized bytes: the probe
          // turns the dictionary on. Then twelve one-letter words: past
          // sixteen entries a partition's dictionary closes, and one that
          // matched enough rows goes on in a successor of letters alone.
          // Last, those mixed with longer words, which fill a dictionary's
          // bytes first, and now and then a value too long for any
          // dictionary, so partitions fall back to raw bytes.
          static const std::vector<std::string> first = {
              "north", "south", "east", "west", "up", "down", "left", "right"};
          static const std::vector<std::string> second = {
              "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};
          static const std::vector<std::string> third = {
              "mississippi", "amazonriver", "yangtzeriverdelta", "danube"};
          std::string storage;
          return fill<StringView>(type, n, [&] {
            if (batch < kSecondVocabularyBatch) {
              return StringView(first[rng() % first.size()]);
            }
            if (batch < kLongValuesBatch) {
              return StringView(second[rng() % second.size()]);
            }
            const auto pick = rng() % 16;
            if (pick == 0) {
              storage = randomString(70, rng);
              return StringView(storage);
            }
            const auto& vocab = pick <= 5 ? first : pick <= 10 ? second : third;
            return StringView(vocab[rng() % vocab.size()]);
          });
        }
        // Mostly inline, some across cache lines.
        std::string storage;
        return fill<StringView>(type, n, [&] {
          const size_t length = rng() % 8 == 0 ? 64 + rng() % 256 : rng() % 24;
          storage = randomString(length, rng);
          return StringView(storage);
        });
      }
      default:
        BOLT_UNREACHABLE();
    }
  }

  BufferPtr
  randomNulls(vector_size_t n, uint32_t percent, std::mt19937_64& rng) {
    auto nulls = AlignedBuffer::allocate<bool>(n, pool(), bits::kNotNull);
    auto* raw = nulls->asMutable<uint64_t>();
    for (vector_size_t row = 0; row < n; ++row) {
      if (rng() % 100 < percent) {
        bits::setNull(raw, row);
      }
    }
    return nulls;
  }

  BufferPtr
  randomIndices(vector_size_t n, vector_size_t baseSize, std::mt19937_64& rng) {
    auto indices = allocateIndices(n, pool());
    auto* raw = indices->asMutable<vector_size_t>();
    for (vector_size_t row = 0; row < n; ++row) {
      raw[row] = static_cast<vector_size_t>(rng() % baseSize);
    }
    return indices;
  }

  VectorPtr column(
      const std::string& name,
      const TypePtr& type,
      Shape shape,
      vector_size_t n,
      uint32_t batch,
      std::mt19937_64& rng) {
    if (type->kind() == TypeKind::UNKNOWN) {
      return BaseVector::createNullConstant(type, n, pool());
    }
    const vector_size_t baseSize = n / 2 + 1;
    switch (shape) {
      case Shape::kNoNulls:
        return values(name, type, n, batch, rng);
      case Shape::kSomeNulls: {
        auto vector = values(name, type, n, batch, rng);
        vector->setNulls(randomNulls(n, 20, rng));
        return vector;
      }
      case Shape::kStaleNulls: {
        auto vector = values(name, type, n, batch, rng);
        vector->setNulls(
            AlignedBuffer::allocate<bool>(n, pool(), bits::kNotNull));
        return vector;
      }
      case Shape::kAllNull: {
        auto vector = values(name, type, n, batch, rng);
        vector->setNulls(AlignedBuffer::allocate<bool>(n, pool(), bits::kNull));
        return vector;
      }
      case Shape::kConstantNull:
        return BaseVector::createNullConstant(type, n, pool());
      case Shape::kDictionary: {
        auto base = values(name, type, baseSize, batch, rng);
        base->setNulls(randomNulls(baseSize, 20, rng));
        auto indices = randomIndices(n, baseSize, rng);
        auto nulls = randomNulls(n, 10, rng);
        return BaseVector::wrapInDictionary(
            std::move(nulls), std::move(indices), n, std::move(base));
      }
      case Shape::kDictionaryNoNulls: {
        auto base = values(name, type, baseSize, batch, rng);
        auto indices = randomIndices(n, baseSize, rng);
        return BaseVector::wrapInDictionary(
            nullptr, std::move(indices), n, std::move(base));
      }
      case Shape::kDictionaryAllNull: {
        auto base = values(name, type, baseSize, batch, rng);
        auto indices = randomIndices(n, baseSize, rng);
        return BaseVector::wrapInDictionary(
            AlignedBuffer::allocate<bool>(n, pool(), bits::kNull),
            std::move(indices),
            n,
            std::move(base));
      }
      case Shape::kConstant: {
        auto base = values(name, type, baseSize, batch, rng);
        const auto index = static_cast<vector_size_t>(rng() % baseSize);
        return BaseVector::wrapInConstant(n, index, std::move(base));
      }
      case Shape::kLazy: {
        auto vector = values(name, type, n, batch, rng);
        vector->setNulls(randomNulls(n, 20, rng));
        return wrapInLazyDictionary(std::move(vector));
      }
      case Shape::kCount:
        break;
    }
    BOLT_UNREACHABLE();
  }

  /// Every flat type: each fixed width, both decimals, timestamps, random
  /// and low-cardinality strings, binary, and an UNKNOWN column between
  /// the others.
  static RowTypePtr rowType() {
    return ROW(
        {"b",
         "tiny",
         "small",
         "int",
         "big",
         "huge",
         "real",
         "dbl",
         "str",
         "lowcard",
         "bin",
         "unknown",
         "ts",
         "date",
         "dec",
         "longdec"},
        {BOOLEAN(),
         TINYINT(),
         SMALLINT(),
         INTEGER(),
         BIGINT(),
         HUGEINT(),
         REAL(),
         DOUBLE(),
         VARCHAR(),
         VARCHAR(),
         VARBINARY(),
         UNKNOWN(),
         TIMESTAMP(),
         DATE(),
         DECIMAL(10, 2),
         DECIMAL(30, 3)});
  }

  /// The batches of kBatches. The cycled batch k holds column c in shape
  /// (k + c) % kShapes; `shapes` gets, per column, the shapes the cycled
  /// batches gave it.
  ShuffleInput makeInput(
      int32_t numPartitions,
      std::vector<std::set<Shape>>* shapes = nullptr) {
    std::mt19937_64 rng(0x601de7);
    const auto type = rowType();
    ShuffleInput input;
    uint32_t cycled = 0;
    for (uint32_t batch = 0; batch < std::size(kBatches); ++batch) {
      const auto [n, kind] = kBatches[batch];
      std::vector<VectorPtr> children;
      for (uint32_t col = 0; col < type->size(); ++col) {
        Shape shape;
        switch (kind) {
          case BatchKind::kFirst:
            shape = col % 2 == 0 ? Shape::kNoNulls : Shape::kSomeNulls;
            break;
          case BatchKind::kCycled:
            shape = static_cast<Shape>((cycled + col) % kShapes);
            if (shapes != nullptr) {
              shapes->resize(type->size());
              (*shapes)[col].insert(shape);
            }
            break;
          case BatchKind::kSmall:
            shape = static_cast<Shape>((batch + col) % kShapes);
            break;
          case BatchKind::kWrappedNull:
            shape = Shape::kDictionaryAllNull;
            break;
          case BatchKind::kConstantNull:
            shape = Shape::kConstantNull;
            break;
        }
        children.push_back(column(
            type->nameOf(col), type->childAt(col), shape, n, batch, rng));
      }
      cycled += kind == BatchKind::kCycled;
      input.data.push_back(std::make_shared<RowVector>(
          pool(), type, nullptr, n, std::move(children)));
      auto& pids = input.pids.emplace_back(n);
      for (auto& pid : pids) {
        pid = static_cast<int32_t>(rng() % numPartitions);
      }
    }
    return input;
  }

  /// Writes the scenario's shuffle, checks it reads back and returns its
  /// golden record.
  Golden run(const Scenario& scenario) {
    removeFiles();
    const auto input = makeInput(scenario.numPartitions);
    auto options = test::shuffleOptions(
        dataFile_,
        scenario.numPartitions,
        scenario.lz4 ? arrow::Compression::LZ4_FRAME
                     : arrow::Compression::UNCOMPRESSED,
        scenario.rss);
    // Seals and spills through the spill file on disk, as recorded: with
    // memory to spare, spill memory would keep the seals off the disk.
    options.cellOptions.spillMemoryBytes = 0;
    if (scenario.tune) {
      scenario.tune(options.cellOptions);
    }
    const auto written = test::writeShuffle(options, input, writerPool_.get());
    const auto& metrics = written.metrics;
    if (scenario.seals) {
      EXPECT_GT(metrics.spillCount, 0) << scenario.name;
    } else {
      EXPECT_EQ(metrics.spillCount, 0) << scenario.name;
    }
    if (!scenario.rss) {
      EXPECT_EQ(
          metrics.totalBytesEvicted > 0, scenario.spills || scenario.seals)
          << scenario.name;
    }
    // The low-cardinality column goes through dictionaries and their
    // fallback, unless dictionaries are off.
    const bool dictionaries = options.cellOptions.enableStringDictionary;
    EXPECT_EQ(metrics.dictionaryMatchedRows > 0, dictionaries) << scenario.name;
    EXPECT_EQ(metrics.dictionaryFallbackRows > 0, dictionaries)
        << scenario.name;

    const auto codec = scenario.lz4 ? test::lz4Codec() : nullptr;
    const auto rows = input.rowsOf(scenario.numPartitions);
    for (int32_t pid = 0; pid < scenario.numPartitions; ++pid) {
      SCOPED_TRACE(scenario.name);
      const auto batches = test::readPartition(
          written.partitions[pid],
          rowType(),
          codec.get(),
          pool(),
          1000,
          1 << 20);
      test::expectPartitionRows(rows[pid], batches, pid, 1000);
    }
    return {
        scenario.name,
        metrics.totalBytesWritten,
        hashOf(written, scenario.rss)};
  }

  /// Writes the nested scenario's shuffle of kNestedBatches, checks that it
  /// takes the way its options say and reads back, and returns its golden
  /// record.
  Golden run(const NestedScenario& scenario) {
    removeFiles();
    const auto input = test::makeInput(
        scenario.type,
        scenario.options,
        {std::begin(kNestedBatches), std::end(kNestedBatches)},
        scenario.numPartitions,
        kNestedSeed,
        pool(),
        scenario.encodingOf);
    auto options = test::shuffleOptions(
        dataFile_,
        scenario.numPartitions,
        scenario.lz4 ? arrow::Compression::LZ4_FRAME
                     : arrow::Compression::UNCOMPRESSED,
        scenario.rss);
    if (scenario.tune) {
      scenario.tune(options.cellOptions);
    }
    const auto written = test::writeShuffle(options, input, writerPool_.get());
    const auto& metrics = written.metrics;
    // A nested layout, or the adapter's binary column.
    const bool native = options.cellOptions.nativeNestedTypes;
    EXPECT_EQ(metrics.flattenTime > 0, native) << scenario.name;
    EXPECT_EQ(metrics.convertTime > 0, !native) << scenario.name;
    EXPECT_EQ(metrics.spillCount > 0, scenario.seals) << scenario.name;

    const auto codec = scenario.lz4 ? test::lz4Codec() : nullptr;
    const auto rows = input.rowsOf(scenario.numPartitions);
    for (int32_t pid = 0; pid < scenario.numPartitions; ++pid) {
      SCOPED_TRACE(scenario.name);
      const auto batches = test::readPartition(
          written.partitions[pid],
          scenario.type,
          codec.get(),
          pool(),
          1000,
          1 << 20,
          native);
      test::expectPartitionRows(rows[pid], batches, pid, 1000);
    }
    return {
        scenario.name,
        metrics.totalBytesWritten,
        hashOf(written, scenario.rss)};
  }

  std::string dataFile_;
  std::shared_ptr<memory::MemoryPool> writerRoot_;
  std::shared_ptr<memory::MemoryPool> writerPool_;
};

TEST_F(CellGoldenTest, inputHoldsEveryShape) {
  // What the golden input is meant to hold, so that a change to it cannot
  // quietly weaken what the hashes pin.
  std::vector<std::set<Shape>> shapes;
  const auto input = makeInput(7, &shapes);
  const auto type = rowType();
  ASSERT_EQ(shapes.size(), type->size());
  for (uint32_t col = 0; col < type->size(); ++col) {
    if (type->childAt(col)->kind() != TypeKind::UNKNOWN) {
      EXPECT_EQ(shapes[col].size(), kShapes) << type->nameOf(col);
    }
  }
  // The low-cardinality column has values with each vocabulary, and long
  // ones with the last.
  const auto lowcard = type->getChildIdx("lowcard");
  std::array<int64_t, 3> values{};
  int64_t longValues = 0;
  for (uint32_t batch = 0; batch < input.data.size(); ++batch) {
    const auto phase = batch < kSecondVocabularyBatch ? 0
        : batch < kLongValuesBatch                    ? 1
                                                      : 2;
    const auto& column =
        BaseVector::loadedVectorShared(input.data[batch]->childAt(lowcard));
    DecodedVector decoded(*column);
    for (vector_size_t row = 0; row < column->size(); ++row) {
      if (!decoded.isNullAt(row)) {
        ++values[phase];
        longValues += decoded.valueAt<StringView>(row).size() > 63;
      }
    }
  }
  for (const auto count : values) {
    EXPECT_GE(count, 1000);
  }
  EXPECT_GT(longValues, 0);
}

TEST_F(CellGoldenTest, flatSchemasKeepTheirBytes) {
  const auto partitionSeals = [](CellShuffleOptions& options) {
    options.partitionSealBytes = 64 << 10;
  };
  const auto nullSeals = [](CellShuffleOptions& options) {
    options.nullMemLimitBytes = 1 << 10;
  };
  const auto spills = [](CellShuffleOptions& options) {
    options.cellMemorySpillRatio = test::spillRatio(4 << 20, kWriterPoolBytes);
  };
  const std::vector<Scenario> scenarios = {
      {"p1_none", 1, false, false, nullptr, false, false},
      {"p7_lz4", 7, true, false, nullptr, false, false},
      {"p1024_lz4", 1024, true, false, nullptr, false, false},
      {"p7_none_rss", 7, false, true, nullptr, false, false},
      {"p1024_lz4_rss", 1024, true, true, nullptr, false, false},
      {"p7_lz4_partition_seals", 7, true, false, partitionSeals, true, false},
      {"p7_lz4_partition_seals_rss",
       7,
       true,
       true,
       partitionSeals,
       true,
       false},
      {"p7_none_null_seals", 7, false, false, nullSeals, true, false},
      {"p7_lz4_null_seals_rss", 7, true, true, nullSeals, true, false},
      {"p7_lz4_spills", 7, true, false, spills, false, true},
      {"p1024_none_spills", 1024, false, false, spills, false, true},
      {"p7_lz4_spills_rss", 7, true, true, spills, true, false},
      // One partition: payloads sealed past their size and windows past
      // their null memory (5 and 1 times). A single window never needs a
      // second chunk, so no cap applies here.
      {"p1_lz4_seals",
       1,
       true,
       false,
       [](CellShuffleOptions& options) {
         options.partitionSealBytes = 256 << 10;
         options.nullMemLimitBytes = 4 << 10;
       },
       true,
       false},
      // Every limit at once, each firing between the others: 21 growth
      // spills, 4 partition seals and a null seal.
      {"p7_lz4_every_limit",
       7,
       true,
       false,
       [](CellShuffleOptions& options) {
         options.cellMemorySpillRatio =
             test::spillRatio(4 << 20, kWriterPoolBytes);
         options.partitionSealBytes = 96 << 10;
         options.nullMemLimitBytes = 8 << 10;
       },
       true,
       true},
      {"p7_lz4_no_dictionary",
       7,
       true,
       false,
       [](CellShuffleOptions& options) {
         options.enableStringDictionary = false;
       },
       false,
       false},
      {"p7_lz4_one_shot",
       7,
       true,
       false,
       [](CellShuffleOptions& options) {
         options.streamCompression = false;
         options.adaptiveCompression = false;
       },
       false,
       false},
  };
  // Recorded since the writer packs wide values at whole bytes
  // (kExactPackMaxBits, kBigintPlainMinBits).
  static const Golden kGolden[] = {
      {"p1_none", 2517475, 0x43f7afffad8f6688ULL},
      {"p7_lz4", 2246101, 0x4530bbef09b209e6ULL},
      {"p1024_lz4", 3064977, 0x20cd39208da207d3ULL},
      {"p7_none_rss", 2535284, 0x58eb7a981b31244bULL},
      {"p1024_lz4_rss", 3064977, 0xf652df9796bf540dULL},
      {"p7_lz4_partition_seals", 2246778, 0xfbdcd816d8c8920cULL},
      {"p7_lz4_partition_seals_rss", 2246778, 0xeb80ff5ef947c8feULL},
      {"p7_none_null_seals", 2542676, 0xbba839d809099ae6ULL},
      {"p7_lz4_null_seals_rss", 2297123, 0xa746b56b725d20adULL},
      {"p7_lz4_spills", 2251769, 0xeecc1d2e93fd4156ULL},
      {"p1024_none_spills", 8635904, 0x2714cd1c35846e2aULL},
      {"p7_lz4_spills_rss", 2255058, 0xc79faec398d8df82ULL},
      {"p1_lz4_seals", 2254213, 0xa98fc2436554c9cbULL},
      {"p7_lz4_every_limit", 2285074, 0x1b47d371375a0ca8ULL},
      {"p7_lz4_no_dictionary", 2268893, 0xbca4ebf73b93e31aULL},
      {"p7_lz4_one_shot", 2224722, 0x77773523d15b6ad3ULL},
  };
  std::vector<Golden> actual;
  for (const auto& scenario : scenarios) {
    actual.push_back(run(scenario));
  }
  expectRecorded(actual, kGolden);
}

TEST_F(CellGoldenTest, nestedSchemasKeepTheirBytes) {
  // Columns four levels deep, in every encoding at the top level and
  // wrapped below it; struct fields with no column on the wire and structs
  // in arrays, pushed to an RSS client; payloads sealed after every batch,
  // one level's containers all null, so that some payloads of the small
  // batches hold no position of it; and the four-level columns through the
  // adapter's binary column.
  test::NestedVectorMaker::Options wrapped;
  wrapped.nullPercent = {10, 20, 10};
  wrapped.wrapPercent = 50;
  wrapped.scatteredPercent = 50;
  const auto everyEncoding = [](uint32_t batch, uint32_t col) {
    return static_cast<test::ColumnEncoding>(
        (batch + col) % static_cast<uint32_t>(test::ColumnEncoding::kCount));
  };
  test::NestedVectorMaker::Options structNulls;
  structNulls.nullPercent = {10, 30};
  test::NestedVectorMaker::Options nullLevel;
  nullLevel.nullPercent = {10, 20, 10, 100};
  nullLevel.maxLength = 3;
  const auto unknownFields = ROW(
      {"r", "ar", "m", "o", "sa"},
      {ROW({"u", "a"}, {UNKNOWN(), INTEGER()}),
       ARRAY(ROW({"u", "a", "s"}, {UNKNOWN(), INTEGER(), VARCHAR()})),
       MAP(INTEGER(), ROW({"a", "u"}, {BIGINT(), UNKNOWN()})),
       ROW({"u", "w"}, {UNKNOWN(), UNKNOWN()}),
       ARRAY(ROW({"x", "l"}, {DOUBLE(), ARRAY(ROW({"z"}, {SMALLINT()}))}))});
  const std::vector<NestedScenario> scenarios = {
      {"deep_rows_p7_lz4",
       test::deepRowType(),
       wrapped,
       everyEncoding,
       7,
       true,
       false,
       nullptr,
       false},
      {"unknown_fields_p7_none_rss",
       unknownFields,
       structNulls,
       nullptr,
       7,
       false,
       true,
       nullptr,
       false},
      {"deep_nested_p7_lz4_partition_seals",
       test::deepNestedType(),
       nullLevel,
       nullptr,
       7,
       true,
       false,
       [](CellShuffleOptions& options) { options.partitionSealBytes = 1; },
       true},
      {"deep_rows_p7_lz4_adapter",
       test::deepRowType(),
       wrapped,
       everyEncoding,
       7,
       true,
       false,
       [](CellShuffleOptions& options) { options.nativeNestedTypes = false; },
       false},
  };
  // Recorded since the writer packs wide values at whole bytes
  // (kExactPackMaxBits, kBigintPlainMinBits).
  static const Golden kGolden[] = {
      {"deep_rows_p7_lz4", 751799, 0xab09260e8c2915daULL},
      {"unknown_fields_p7_none_rss", 273202, 0xc77ba98d9695581fULL},
      {"deep_nested_p7_lz4_partition_seals", 473433, 0x1f560d7c6d0bb239ULL},
      {"deep_rows_p7_lz4_adapter", 1340921, 0x5fe55cc0018ef204ULL},
  };
  std::vector<Golden> actual;
  for (const auto& scenario : scenarios) {
    actual.push_back(run(scenario));
  }
  expectRecorded(actual, kGolden);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
