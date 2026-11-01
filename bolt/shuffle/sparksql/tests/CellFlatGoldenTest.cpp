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

#include <fmt/format.h>

#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

/// The exact bytes the Cell writer produces for flat schemas, pinned: every
/// flat type in every null shape and input encoding; string dictionaries,
/// their successors and their fallbacks; partition and null-memory seals,
/// spills and both outputs, alone and together; at several partition
/// counts, uncompressed and LZ4. A change to the writer that is meant to
/// leave flat schemas alone must keep every hash.
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

class CellFlatGoldenTest : public testing::Test,
                           public bolt::test::VectorTestBase {
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
    writerRoot_ = memory::memoryManager()->addRootPool(
        "cellFlatGolden", int64_t{8} << 30);
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

    uint64_t hash = 0xcbf29ce484222325ULL;
    for (int32_t pid = 0; pid < scenario.numPartitions; ++pid) {
      hash = fnv1a(hash, metrics.partitionLengths[pid]);
      if (scenario.rss) {
        hash = fnv1a(hash, written.pushes[pid].size());
        for (const auto& push : written.pushes[pid]) {
          hash = fnv1a(hash, push.size());
        }
      }
      hash = fnv1a(
          hash, written.partitions[pid].data(), written.partitions[pid].size());
    }

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
    return {scenario.name, metrics.totalBytesWritten, hash};
  }

  std::string dataFile_;
  std::shared_ptr<memory::MemoryPool> writerRoot_;
  std::shared_ptr<memory::MemoryPool> writerPool_;
};

TEST_F(CellFlatGoldenTest, inputHoldsEveryShape) {
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

TEST_F(CellFlatGoldenTest, flatSchemasKeepTheirBytes) {
  const auto partitionSeals = [](CellShuffleOptions& options) {
    options.checkpointPartitionBytes = 64 << 10;
  };
  const auto nullSeals = [](CellShuffleOptions& options) {
    options.nullMemLimitBytes = 1 << 10;
  };
  const auto spills = [](CellShuffleOptions& options) {
    options.cellMemoryCapBytes = 4 << 20;
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
         options.checkpointPartitionBytes = 256 << 10;
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
         options.cellMemoryCapBytes = 4 << 20;
         options.checkpointPartitionBytes = 96 << 10;
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
  // Recorded from the writer at f5398ad6.
  static const Golden kGolden[] = {
      {"p1_none", 2492180, 0xe33662948707f9dfULL},
      {"p7_lz4", 2224672, 0xc66f85f1f0af1d01ULL},
      {"p1024_lz4", 3043168, 0x7d456e4c1d9f6f38ULL},
      {"p7_none_rss", 2509940, 0xf820603beb81fba2ULL},
      {"p1024_lz4_rss", 3043168, 0x3159607ee4a0f36bULL},
      {"p7_lz4_partition_seals", 2223619, 0x17e8b08e70e88c73ULL},
      {"p7_lz4_partition_seals_rss", 2223619, 0x6ce7ecbffd180568ULL},
      {"p7_none_null_seals", 2517492, 0x1106137322994b0bULL},
      {"p7_lz4_null_seals_rss", 2195802, 0x96c55930ef2c427cULL},
      {"p7_lz4_spills", 2225204, 0xc5b10e613bdb4177ULL},
      {"p1024_none_spills", 8607376, 0x461a472c16799b04ULL},
      {"p7_lz4_spills_rss", 2205597, 0xd4066274994f0980ULL},
      {"p1_lz4_seals", 2231503, 0x922c06cea263f3dbULL},
      {"p7_lz4_every_limit", 2272804, 0x0d17bc0d75507742ULL},
      {"p7_lz4_no_dictionary", 2247464, 0x6250503b4b2248f5ULL},
      {"p7_lz4_one_shot", 2204206, 0x0938934d484a8294ULL},
  };
  std::string actual;
  bool same = std::size(kGolden) == scenarios.size();
  for (size_t i = 0; i < scenarios.size(); ++i) {
    const auto golden = run(scenarios[i]);
    const Golden* recorded = i < std::size(kGolden) &&
            std::string_view(kGolden[i].name) == golden.name
        ? &kGolden[i]
        : nullptr;
    if (recorded == nullptr) {
      same = false;
      ADD_FAILURE() << golden.name << ": not recorded";
    } else if (
        golden.bytes != recorded->bytes || golden.hash != recorded->hash) {
      same = false;
      ADD_FAILURE() << golden.name << ": " << golden.bytes << " bytes, hash "
                    << fmt::format("{:#018x}", golden.hash) << "; recorded "
                    << recorded->bytes << " bytes, hash "
                    << fmt::format("{:#018x}", recorded->hash);
    }
    actual += fmt::format(
        "      {{\"{}\", {}, {:#018x}ULL}},\n",
        golden.name,
        golden.bytes,
        golden.hash);
  }
  // Only for a deliberate format change: the table as the writer now
  // produces it.
  EXPECT_TRUE(same) << "now:\n" << actual;
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
