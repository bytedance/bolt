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
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <random>

#include <arrow/io/memory.h>
#include <fmt/format.h>

#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

/// ARRAY, MAP and ROW columns through the Cell shuffle, checked against
/// their input and against V1: every partition reads back its rows, in
/// order and with every map's entries in their order, whatever the
/// nesting, the nulls at each level, the input's encodings, the partition
/// count, codec, output, spills, seals and read batch sizes. The tests see
/// the shuffle only from the outside, so they hold for any way the Cell
/// writer and reader carry nested values.
namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

using test::ShuffleInput;

/// Vectors of any Cell type, nested to any depth, from a seed: nulls at
/// each level at a rate of its own, empty containers, null containers that
/// keep their elements, containers anywhere in their elements, map keys in
/// no order, and dictionaries, constants and slices inside the containers.
class NestedVectorMaker {
 public:
  struct Options {
    /// Percent of null rows at each nesting level, the top one first; the
    /// last applies below as well.
    std::vector<uint32_t> nullPercent{10};
    /// A container holds up to this many elements.
    vector_size_t maxLength{4};
    /// Percent of the containers that are empty.
    uint32_t emptyPercent{20};
    /// Percent of the null containers that keep a size, their elements
    /// still in the elements vector.
    uint32_t sizedNullPercent{50};
    /// Percent of the containers placed anywhere in their elements:
    /// overlapping one another and out of order.
    uint32_t scatteredPercent{0};
    /// Percent of the vectors below the top level that come wrapped: in a
    /// dictionary with nulls of its own, a constant, or a slice; map keys,
    /// in a dictionary that reorders them.
    uint32_t wrapPercent{0};
  };

  NestedVectorMaker(memory::MemoryPool* pool, uint64_t seed, Options options)
      : pool_(pool), rng_(seed), options_(std::move(options)) {}

  /// `size` rows of `type` at nesting level `depth`.
  VectorPtr make(const TypePtr& type, vector_size_t size, uint32_t depth = 0) {
    switch (type->kind()) {
      case TypeKind::ARRAY:
        return makeArray(type, size, depth);
      case TypeKind::MAP:
        return makeMap(type, size, depth);
      case TypeKind::ROW:
        return makeRow(type, size, depth);
      default:
        return makeLeaf(type, size, depth);
    }
  }

  std::mt19937_64& rng() {
    return rng_;
  }

 private:
  bool chance(uint32_t percent) {
    return rng_() % 100 < percent;
  }

  BufferPtr nulls(vector_size_t size, uint32_t depth) {
    const auto& rates = options_.nullPercent;
    const auto percent = rates[std::min<size_t>(depth, rates.size() - 1)];
    if (percent == 0) {
      return nullptr;
    }
    auto nulls = AlignedBuffer::allocate<bool>(size, pool_, bits::kNotNull);
    auto* raw = nulls->asMutable<uint64_t>();
    for (vector_size_t row = 0; row < size; ++row) {
      if (chance(percent)) {
        bits::setNull(raw, row);
      }
    }
    return nulls;
  }

  /// A vector below the top level: made as is, or wrapped.
  VectorPtr child(const TypePtr& type, vector_size_t size, uint32_t depth) {
    if (size == 0 || !chance(options_.wrapPercent)) {
      return make(type, size, depth);
    }
    switch (rng_() % 3) {
      case 0: {
        const vector_size_t baseSize = size / 2 + 1;
        auto base = make(type, baseSize, depth);
        auto indices = allocateIndices(size, pool_);
        auto* raw = indices->asMutable<vector_size_t>();
        for (vector_size_t row = 0; row < size; ++row) {
          raw[row] = static_cast<vector_size_t>(rng_() % baseSize);
        }
        return BaseVector::wrapInDictionary(
            nulls(size, depth), std::move(indices), size, std::move(base));
      }
      case 1: {
        auto base = make(type, 3, depth);
        return BaseVector::wrapInConstant(
            size, static_cast<vector_size_t>(rng_() % 3), std::move(base));
      }
      default:
        return make(type, size + 10, depth)->slice(5, size);
    }
  }

  /// Offsets and sizes of `size` containers, their nulls given; returns
  /// the size of their elements vector.
  vector_size_t containers(
      vector_size_t size,
      const BufferPtr& entryNulls,
      BufferPtr& offsets,
      BufferPtr& sizes) {
    std::vector<vector_size_t> lengths(size);
    vector_size_t total = 0;
    for (vector_size_t row = 0; row < size; ++row) {
      const bool isNull = entryNulls != nullptr &&
          bits::isBitNull(entryNulls->as<uint64_t>(), row);
      if ((isNull && !chance(options_.sizedNullPercent)) ||
          options_.maxLength == 0 || chance(options_.emptyPercent)) {
        lengths[row] = 0;
      } else {
        lengths[row] =
            1 + static_cast<vector_size_t>(rng_() % options_.maxLength);
      }
      total += lengths[row];
    }
    offsets = allocateOffsets(size, pool_);
    sizes = allocateSizes(size, pool_);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    vector_size_t at = 0;
    for (vector_size_t row = 0; row < size; ++row) {
      rawSizes[row] = lengths[row];
      rawOffsets[row] = at;
      if (chance(options_.scatteredPercent)) {
        rawOffsets[row] =
            static_cast<vector_size_t>(rng_() % (total - lengths[row] + 1));
      }
      at += lengths[row];
    }
    return total;
  }

  VectorPtr makeArray(const TypePtr& type, vector_size_t size, uint32_t depth) {
    auto entryNulls = nulls(size, depth);
    BufferPtr offsets;
    BufferPtr sizes;
    const auto total = containers(size, entryNulls, offsets, sizes);
    auto elements = child(type->childAt(0), total, depth + 1);
    return std::make_shared<ArrayVector>(
        pool_,
        type,
        std::move(entryNulls),
        size,
        std::move(offsets),
        std::move(sizes),
        std::move(elements));
  }

  VectorPtr makeMap(const TypePtr& type, vector_size_t size, uint32_t depth) {
    auto entryNulls = nulls(size, depth);
    BufferPtr offsets;
    BufferPtr sizes;
    const auto total = containers(size, entryNulls, offsets, sizes);
    auto keys = makeKeys(type->childAt(0), total);
    auto values = child(type->childAt(1), total, depth + 1);
    return std::make_shared<MapVector>(
        pool_,
        type,
        std::move(entryNulls),
        size,
        std::move(offsets),
        std::move(sizes),
        std::move(keys),
        std::move(values));
  }

  /// 0 to `size` - 1 in random order.
  std::vector<vector_size_t> permutation(vector_size_t size) {
    std::vector<vector_size_t> order(size);
    for (vector_size_t i = 0; i < size; ++i) {
      const auto j = static_cast<vector_size_t>(rng_() % (i + 1));
      order[i] = order[j];
      order[j] = i;
    }
    return order;
  }

  /// Keys never null and all different, so that no map holds one twice
  /// wherever its entries lie, in no order: a map's entries keep theirs
  /// only if the shuffle keeps it.
  VectorPtr makeKeys(const TypePtr& type, vector_size_t size) {
    const auto base = static_cast<int64_t>(rng_() % 1'000'000);
    const auto order = permutation(size);
    VectorPtr keys;
    switch (type->kind()) {
      case TypeKind::INTEGER: {
        auto flat = BaseVector::create<FlatVector<int32_t>>(type, size, pool_);
        for (vector_size_t row = 0; row < size; ++row) {
          flat->set(row, static_cast<int32_t>(base + order[row]));
        }
        keys = std::move(flat);
        break;
      }
      case TypeKind::BIGINT: {
        auto flat = BaseVector::create<FlatVector<int64_t>>(type, size, pool_);
        for (vector_size_t row = 0; row < size; ++row) {
          flat->set(row, (base + order[row]) * 7919);
        }
        keys = std::move(flat);
        break;
      }
      case TypeKind::VARCHAR: {
        auto flat =
            BaseVector::create<FlatVector<StringView>>(type, size, pool_);
        for (vector_size_t row = 0; row < size; ++row) {
          // Some inline, some not.
          const auto key = fmt::format(
              "{}{}",
              base + order[row],
              std::string(rng_() % 2 == 0 ? 2 : 20, 'k'));
          flat->set(row, StringView(key));
        }
        keys = std::move(flat);
        break;
      }
      default:
        BOLT_FAIL("no map keys of {}", type->toString());
    }
    if (size == 0 || !chance(options_.wrapPercent)) {
      return keys;
    }
    // A dictionary of distinct indices keeps the keys distinct.
    const auto shuffled = permutation(size);
    auto indices = allocateIndices(size, pool_);
    std::copy(
        shuffled.begin(), shuffled.end(), indices->asMutable<vector_size_t>());
    return BaseVector::wrapInDictionary(
        nullptr, std::move(indices), size, std::move(keys));
  }

  VectorPtr makeRow(const TypePtr& type, vector_size_t size, uint32_t depth) {
    auto rowNulls = nulls(size, depth);
    std::vector<VectorPtr> fields;
    for (uint32_t i = 0; i < type->size(); ++i) {
      fields.push_back(child(type->childAt(i), size, depth + 1));
    }
    return std::make_shared<RowVector>(
        pool_, type, std::move(rowNulls), size, std::move(fields));
  }

  template <typename T>
  VectorPtr flat(
      const TypePtr& type,
      vector_size_t size,
      const std::function<T()>& value) {
    auto vector = BaseVector::create<FlatVector<T>>(type, size, pool_);
    for (vector_size_t row = 0; row < size; ++row) {
      vector->set(row, value());
    }
    return vector;
  }

  VectorPtr leafValues(const TypePtr& type, vector_size_t size) {
    auto& rng = rng_;
    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return flat<bool>(type, size, [&] { return (rng() & 1) == 1; });
      case TypeKind::TINYINT:
        return flat<int8_t>(
            type, size, [&] { return static_cast<int8_t>(rng()); });
      case TypeKind::SMALLINT:
        return flat<int16_t>(
            type, size, [&] { return static_cast<int16_t>(rng()); });
      case TypeKind::INTEGER:
        return flat<int32_t>(type, size, [&] {
          return type->isDate() ? static_cast<int32_t>(rng() % 40'000)
                                : static_cast<int32_t>(rng());
        });
      case TypeKind::BIGINT:
        return flat<int64_t>(type, size, [&] {
          return type->isDecimal()
              ? static_cast<int64_t>(rng() % 19'999'999'999) - 9'999'999'999
              : static_cast<int64_t>(rng());
        });
      case TypeKind::HUGEINT:
        return flat<int128_t>(type, size, [&] {
          const auto high = static_cast<int64_t>(rng() % 1'000'000'000'000);
          const auto low = static_cast<int64_t>(rng() % 1'000'000'000'000);
          return static_cast<int128_t>(high) * 1'000'000'000'000 - low;
        });
      case TypeKind::REAL:
        return flat<float>(type, size, [&] {
          return static_cast<float>(static_cast<int32_t>(rng() % 100'000)) / 8;
        });
      case TypeKind::DOUBLE:
        return flat<double>(type, size, [&] {
          return static_cast<double>(static_cast<int64_t>(rng() % 1'000'000)) /
              64;
        });
      case TypeKind::TIMESTAMP:
        // Microseconds: what the shuffle keeps of a nested timestamp today.
        return flat<Timestamp>(type, size, [&] {
          const auto seconds =
              static_cast<int64_t>(rng() % 4'000'000'000) - 2'000'000'000;
          const auto micros = rng() % 1'000'000;
          return Timestamp(seconds, micros * 1000);
        });
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        std::string value;
        return flat<StringView>(type, size, [&] {
          const size_t length = rng() % 8 == 0 ? 50 + rng() % 200 : rng() % 16;
          value.resize(length);
          for (auto& c : value) {
            c = static_cast<char>('a' + rng() % 26);
          }
          return StringView(value);
        });
      }
      default:
        BOLT_FAIL("no values of {}", type->toString());
    }
  }

  VectorPtr makeLeaf(const TypePtr& type, vector_size_t size, uint32_t depth) {
    if (type->kind() == TypeKind::UNKNOWN) {
      return BaseVector::createNullConstant(type, size, pool_);
    }
    auto vector = leafValues(type, size);
    vector->setNulls(nulls(size, depth));
    return vector;
  }

  memory::MemoryPool* const pool_;
  std::mt19937_64 rng_;
  const Options options_;
};

/// How a top-level column holds its rows in a batch.
enum class Encoding {
  kFlat,
  kDictionary,
  kConstant,
  kConstantNull,
  kSlice,
  kLazy,
  kLazyDictionary,
  kCount,
};

/// Every type a nested column can hold at its leaves.
std::vector<TypePtr> leafTypes() {
  return {
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      HUGEINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
      DECIMAL(10, 2),
      DECIMAL(30, 3),
      UNKNOWN()};
}

/// Whether V1 carries `type`: all but a bare HUGEINT, which no Arrow type
/// describes.
bool v1Carries(const TypePtr& type) {
  if (type->kind() == TypeKind::HUGEINT && !type->isDecimal()) {
    return false;
  }
  for (uint32_t i = 0; i < type->size(); ++i) {
    if (!v1Carries(type->childAt(i))) {
      return false;
    }
  }
  return true;
}

/// Nested columns four levels deep, and one flat column beside them.
RowTypePtr deepRowType() {
  const auto point = ROW({"x", "y"}, {INTEGER(), TIMESTAMP()});
  return ROW(
      {"id", "arrays", "maps", "rows", "pointMaps", "mixed", "nestedMaps"},
      {BIGINT(),
       ARRAY(ARRAY(ARRAY(ARRAY(SMALLINT())))),
       MAP(VARCHAR(), ARRAY(MAP(INTEGER(), point))),
       ROW({"a"},
           {ROW(
               {"b"}, {ROW({"c"}, {ROW({"d", "e"}, {BIGINT(), VARCHAR()})})})}),
       ARRAY(MAP(VARCHAR(), ARRAY(point))),
       ROW({"arr", "n"},
           {ARRAY(ROW({"m"}, {MAP(BIGINT(), VARCHAR())})), INTEGER()}),
       MAP(BIGINT(), MAP(VARCHAR(), MAP(INTEGER(), DOUBLE())))});
}

/// "P=<partitions> nulls a% b% ..." for a list of per-level null rates.
std::string nullTrace(
    int32_t numPartitions,
    const std::vector<uint32_t>& rates) {
  std::string trace = fmt::format("P={} nulls", numPartitions);
  for (const auto rate : rates) {
    trace += fmt::format(" {}%", rate);
  }
  return trace;
}

class CellNestedTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  /// One way to read a partition back: rows and bytes per batch.
  struct ReadShape {
    int32_t batchSize;
    int64_t batchByteSize;
  };

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_cell_nested_test_XXXXXX";
    ASSERT_NE(::mkdtemp(pathTemplate), nullptr);
    dir_ = pathTemplate;
    dataFile_ = dir_ + "/data";
  }

  void TearDown() override {
    std::filesystem::remove_all(dir_);
  }

  void removeFiles() {
    ::unlink(dataFile_.c_str());
    ::unlink((dataFile_ + ".cell.spill").c_str());
    ::unlink((dataFile_ + ".raw.spill").c_str());
  }

  /// A top-level column of `type`, `size` rows, held as `encoding`.
  VectorPtr column(
      NestedVectorMaker& maker,
      const TypePtr& type,
      vector_size_t size,
      Encoding encoding) {
    auto& rng = maker.rng();
    switch (encoding) {
      case Encoding::kFlat:
        return maker.make(type, size);
      case Encoding::kDictionary:
      case Encoding::kLazyDictionary: {
        const vector_size_t baseSize = size / 2 + 1;
        auto base = maker.make(type, baseSize);
        auto indices = allocateIndices(size, pool());
        auto* raw = indices->asMutable<vector_size_t>();
        for (vector_size_t row = 0; row < size; ++row) {
          raw[row] = static_cast<vector_size_t>(rng() % baseSize);
        }
        auto nulls =
            AlignedBuffer::allocate<bool>(size, pool(), bits::kNotNull);
        for (vector_size_t row = 0; row < size; ++row) {
          if (rng() % 10 == 0) {
            bits::setNull(nulls->asMutable<uint64_t>(), row);
          }
        }
        auto dictionary = BaseVector::wrapInDictionary(
            std::move(nulls), std::move(indices), size, std::move(base));
        return encoding == Encoding::kLazyDictionary
            ? wrapInLazyDictionary(std::move(dictionary))
            : dictionary;
      }
      case Encoding::kConstant: {
        auto base = maker.make(type, 4);
        return BaseVector::wrapInConstant(
            size, static_cast<vector_size_t>(rng() % 4), std::move(base));
      }
      case Encoding::kConstantNull:
        return BaseVector::createNullConstant(type, size, pool());
      case Encoding::kSlice:
        return maker.make(type, size + 20)->slice(7, size);
      case Encoding::kLazy:
        return wrapInLazyDictionary(maker.make(type, size));
      case Encoding::kCount:
        break;
    }
    BOLT_UNREACHABLE();
  }

  /// Batches of `rows` rows each of `type`, spread over `numPartitions`;
  /// the columns of batch b are held as `encodingOf(b, column)`.
  ShuffleInput makeInput(
      const RowTypePtr& type,
      NestedVectorMaker::Options options,
      const std::vector<vector_size_t>& rows,
      int32_t numPartitions,
      uint64_t seed,
      const std::function<Encoding(uint32_t, uint32_t)>& encodingOf = nullptr) {
    NestedVectorMaker maker(pool(), seed, std::move(options));
    ShuffleInput input;
    for (uint32_t batch = 0; batch < rows.size(); ++batch) {
      std::vector<VectorPtr> children;
      for (uint32_t col = 0; col < type->size(); ++col) {
        children.push_back(column(
            maker,
            type->childAt(col),
            rows[batch],
            encodingOf ? encodingOf(batch, col) : Encoding::kFlat));
      }
      input.data.push_back(std::make_shared<RowVector>(
          pool(), type, nullptr, rows[batch], std::move(children)));
      auto& pids = input.pids.emplace_back(rows[batch]);
      for (auto& pid : pids) {
        pid = static_cast<int32_t>(maker.rng()() % numPartitions);
      }
    }
    return input;
  }

  /// What V1 reads back of `input`, per partition: the reference that the
  /// Cell output must match besides the input.
  std::vector<std::vector<RowVectorPtr>> v1Partitions(
      const ShuffleInput& input,
      int32_t numPartitions) {
    constexpr int64_t kMemLimit = int64_t{1} << 30;
    removeFiles();
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.forceShuffleWriterType =
        static_cast<int32_t>(ShuffleWriterType::V1);
    options.partitionWriterOptions.numPartitions = numPartitions;
    options.partitionWriterOptions.dataFile = dataFile_;
    options.partitionWriterOptions.configuredDirs = {dir_};
    options.partitionWriterOptions.numSubDirs = 1;
    options.partitionWriterOptions.compressionType =
        arrow::Compression::LZ4_FRAME;
    const auto rowType = asRowType(input.data[0]->type());
    std::vector<int64_t> lengths;
    {
      auto writer = BoltShuffleWriter::createShuffleWriter(
          options,
          rowType->size(),
          input.data[0]->size(),
          input.data[0]->estimateFlatSize(),
          kMemLimit,
          pool(),
          arrow::default_memory_pool());
      for (size_t batch = 0; batch < input.data.size(); ++batch) {
        const auto& data = input.data[batch];
        if (data->size() == 0) {
          continue;
        }
        // V1 flattens its input in place, nested vectors included: it gets
        // a flat copy, and the input keeps its encodings.
        std::vector<VectorPtr> children;
        for (const auto& child : data->children()) {
          children.push_back(
              BaseVector::copy(*BaseVector::loadedVectorShared(child)));
        }
        const auto status = writer->split(
            test::withPid(
                input.pids[batch],
                std::make_shared<RowVector>(
                    pool(), rowType, nullptr, data->size(), children),
                pool()),
            kMemLimit);
        BOLT_CHECK(status.ok(), "{}", status.ToString());
      }
      const auto status = writer->stop();
      BOLT_CHECK(status.ok(), "{}", status.ToString());
      lengths = writer->metrics().partitionLengths;
    }
    std::ifstream in(dataFile_, std::ios::binary);
    const std::string file(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    BoltColumnarBatchDeserializerFactory factory(
        boltTypeToArrowSchema(rowType, pool()),
        createCodec(
            arrow::Compression::LZ4_FRAME,
            CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true}),
        rowType,
        4096,
        1 << 20,
        arrow::default_memory_pool(),
        pool());
    factory.setShuffleWriterType(static_cast<int32_t>(ShuffleWriterType::V1));
    factory.setNumPartitions(numPartitions);
    factory.setpartitioningShortName("hash");
    std::vector<std::vector<RowVectorPtr>> partitions(numPartitions);
    int64_t offset = 0;
    for (int32_t pid = 0; pid < numPartitions; ++pid) {
      if (lengths[pid] == 0) {
        continue;
      }
      BOLT_CHECK_LE(static_cast<size_t>(offset + lengths[pid]), file.size());
      auto deserializer =
          factory.createDeserializer(std::make_shared<arrow::io::BufferReader>(
              reinterpret_cast<const uint8_t*>(file.data()) + offset,
              lengths[pid]));
      offset += lengths[pid];
      while (auto batch = deserializer->next()) {
        partitions[pid].push_back(std::move(batch));
      }
    }
    return partitions;
  }

  /// Writes `input` to a local file and to an RSS client, uncompressed and
  /// with LZ4, and reads every partition back at every read shape,
  /// expecting the input's rows in order and, when V1 carries the schema,
  /// V1's. `tune` adjusts the writer of each output, `check` sees what
  /// each writer reports.
  void roundTrip(
      const ShuffleInput& input,
      int32_t numPartitions,
      const std::function<void(CellShuffleOptions&, bool rss)>& tune = nullptr,
      const std::function<void(const ShuffleWriterMetrics&, bool rss)>& check =
          nullptr,
      const std::vector<ReadShape>& readShapes =
          {{4096, 1 << 20}, {1, 1 << 20}, {100, 1}, {1000, 0}}) {
    const auto rowType = asRowType(input.data[0]->type());
    const auto rows = input.rowsOf(numPartitions);
    const auto lz4 = createCodec(
        arrow::Compression::LZ4_FRAME,
        CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
    // Every Cell writer first, so that the first one sees the input's lazy
    // vectors unloaded.
    struct CellOutput {
      bool rss;
      bool compressed;
      test::WrittenShuffle written;
    };
    std::vector<CellOutput> outputs;
    for (const bool rss : {false, true}) {
      for (const bool compressed : {false, true}) {
        removeFiles();
        ShuffleWriterOptions options;
        options.partitioning = Partitioning::kHash;
        options.partitionWriterOptions.numPartitions = numPartitions;
        options.partitionWriterOptions.dataFile = dataFile_;
        options.partitionWriterOptions.compressionType = compressed
            ? arrow::Compression::LZ4_FRAME
            : arrow::Compression::UNCOMPRESSED;
        if (rss) {
          options.partitionWriterOptions.partitionWriterType =
              PartitionWriterType::kCeleborn;
          options.partitionWriterOptions.rssClient =
              std::make_shared<test::RecordingRssClient>();
        }
        if (tune) {
          tune(options.cellOptions, rss);
        }
        auto written = test::writeShuffle(options, input, pool());
        if (check) {
          SCOPED_TRACE(rss ? "rss" : "local");
          check(written.metrics, rss);
        }
        outputs.push_back({rss, compressed, std::move(written)});
      }
    }
    // V1 must read back the input as well, or it is no reference.
    const bool withV1 = v1Carries(rowType);
    const auto v1 = withV1 ? v1Partitions(input, numPartitions)
                           : std::vector<std::vector<RowVectorPtr>>{};
    std::vector<std::vector<test::RowRef>> v1Rows;
    for (int32_t pid = 0; withV1 && pid < numPartitions; ++pid) {
      SCOPED_TRACE("V1 against the input");
      test::expectPartitionRows(
          rows[pid], v1[pid], pid, std::numeric_limits<int32_t>::max());
      if (HasFatalFailure()) {
        return;
      }
      v1Rows.push_back(test::rowsIn(v1[pid]));
    }
    for (const auto& output : outputs) {
      SCOPED_TRACE(fmt::format(
          "{} {}",
          output.rss ? "rss" : "local",
          output.compressed ? "lz4" : "none"));
      for (const auto& shape : readShapes) {
        SCOPED_TRACE(fmt::format(
            "batches of {} rows, {} bytes",
            shape.batchSize,
            shape.batchByteSize));
        for (int32_t pid = 0; pid < numPartitions; ++pid) {
          const auto batches = test::readPartition(
              output.written.partitions[pid],
              rowType,
              output.compressed ? lz4.get() : nullptr,
              pool(),
              shape.batchSize,
              shape.batchByteSize);
          {
            SCOPED_TRACE("against the input");
            test::expectPartitionRows(rows[pid], batches, pid, shape.batchSize);
          }
          if (withV1) {
            SCOPED_TRACE("against V1");
            test::expectPartitionRows(
                v1Rows[pid], batches, pid, shape.batchSize);
          }
          if (HasFatalFailure()) {
            return;
          }
        }
      }
    }
  }

  /// Holds the data file and, should V1 spill, its spill files.
  std::string dir_;
  std::string dataFile_;
};

TEST_F(CellNestedTest, everyLeafTypeInsideEveryContainer) {
  // Each leaf type in an array, as the values of a map and as a struct
  // field, beside flat columns; nulls at no, some, half and every row of
  // every level. The leaves V1 cannot carry go on their own, held to the
  // input alone.
  std::vector<TypePtr> withV1;
  std::vector<TypePtr> withoutV1;
  for (const auto& leaf : leafTypes()) {
    (v1Carries(leaf) ? withV1 : withoutV1).push_back(leaf);
  }
  for (const auto& leaves : {withV1, withoutV1}) {
    std::vector<std::string> names{"id", "s"};
    std::vector<TypePtr> types{BIGINT(), VARCHAR()};
    std::vector<std::string> fieldNames;
    for (uint32_t i = 0; i < leaves.size(); ++i) {
      names.push_back(fmt::format("a{}", i));
      types.push_back(ARRAY(leaves[i]));
      names.push_back(fmt::format("m{}", i));
      types.push_back(MAP(INTEGER(), leaves[i]));
      fieldNames.push_back(fmt::format("f{}", i));
    }
    names.push_back("r");
    types.push_back(ROW(std::move(fieldNames), std::vector<TypePtr>(leaves)));
    const auto type = ROW(std::move(names), std::move(types));
    SCOPED_TRACE(type->toString());
    for (const uint32_t nulls : {0, 10, 50, 100}) {
      SCOPED_TRACE(fmt::format("{}% nulls", nulls));
      NestedVectorMaker::Options options;
      options.nullPercent = {nulls};
      const auto input = makeInput(type, options, {600, 1, 0, 900}, 7, nulls);
      roundTrip(input, 7);
    }
  }
}

TEST_F(CellNestedTest, fourLevelsAtEveryNullRate) {
  // Arrays, maps and structs four deep, alone and mixed; the rates list
  // each level's nulls from the top.
  const std::vector<std::vector<uint32_t>> rates = {
      {0}, {10}, {50}, {100}, {0, 100}, {0, 0, 0, 100}, {50, 0, 10, 0, 50}};
  for (size_t i = 0; i < rates.size(); ++i) {
    SCOPED_TRACE(nullTrace(7, rates[i]));
    NestedVectorMaker::Options options;
    options.nullPercent = rates[i];
    options.maxLength = 3;
    const auto input =
        makeInput(deepRowType(), options, {800, 3, 1200}, 7, 100 + i);
    roundTrip(input, 7);
    if (HasFatalFailure()) {
      return;
    }
  }
}

TEST_F(CellNestedTest, fourLevelsAtEveryPartitionCount) {
  // One partition, and a few rows in each of 1024.
  const std::vector<std::vector<uint32_t>> rates = {
      {10}, {0, 100}, {50, 0, 10, 0, 50}};
  for (const int32_t numPartitions : {1, 1024}) {
    for (size_t i = 0; i < rates.size(); ++i) {
      SCOPED_TRACE(nullTrace(numPartitions, rates[i]));
      NestedVectorMaker::Options options;
      options.nullPercent = rates[i];
      options.maxLength = 3;
      const auto input = makeInput(
          deepRowType(), options, {800, 3, 1200}, numPartitions, 200 + i);
      roundTrip(
          input, numPartitions, nullptr, nullptr, {{4096, 1 << 20}, {100, 1}});
      if (HasFatalFailure()) {
        return;
      }
    }
  }
}

TEST_F(CellNestedTest, emptyAndNullContainers) {
  // Containers all empty; every null container keeping its elements;
  // containers all null, or full of null elements.
  NestedVectorMaker::Options empty;
  empty.maxLength = 0;
  NestedVectorMaker::Options sizedNulls;
  sizedNulls.nullPercent = {50};
  sizedNulls.sizedNullPercent = 100;
  sizedNulls.emptyPercent = 0;
  NestedVectorMaker::Options allNull;
  allNull.nullPercent = {100};
  allNull.sizedNullPercent = 100;
  NestedVectorMaker::Options nullElements;
  nullElements.nullPercent = {0, 100};
  nullElements.emptyPercent = 0;
  const std::vector<std::pair<const char*, NestedVectorMaker::Options>> shapes =
      {{"empty", empty},
       {"sized nulls", sizedNulls},
       {"all null", allNull},
       {"null elements", nullElements}};
  for (const auto& [name, options] : shapes) {
    SCOPED_TRACE(name);
    const auto input = makeInput(deepRowType(), options, {500, 700}, 7, 3);
    roundTrip(input, 7);
  }
}

TEST_F(CellNestedTest, wrappedAndOverlappingInput) {
  // Top-level columns flat, in dictionaries with nulls of their own,
  // constant, constant null, sliced and lazy, a different one per batch
  // and column; inside, half the vectors wrapped and half the containers
  // overlapping and out of order.
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20, 10};
  options.wrapPercent = 50;
  options.scatteredPercent = 50;
  const auto encodingOf = [](uint32_t batch, uint32_t col) {
    return static_cast<Encoding>(
        (batch + col) % static_cast<uint32_t>(Encoding::kCount));
  };
  for (const int32_t numPartitions : {1, 7}) {
    SCOPED_TRACE(numPartitions);
    const auto input = makeInput(
        deepRowType(),
        options,
        {300, 500, 1, 700, 400, 600, 800},
        numPartitions,
        21,
        encodingOf);
    roundTrip(input, numPartitions);
  }
}

TEST_F(CellNestedTest, spillsAndSeals) {
  // Enough rows for the writer to spill under a small cap, seal payloads
  // past a small size and windows past a small null memory: each alone and
  // all together; on an RSS client every one is a window pushed whole. Half
  // the rows go to partition 0, whose payload passes a bound well before
  // the others' do.
  const auto type =
      ROW({"id", "s", "tags", "attrs", "st"},
          {BIGINT(),
           VARCHAR(),
           ARRAY(VARCHAR()),
           MAP(INTEGER(), ARRAY(BIGINT())),
           ROW({"a", "b", "c"}, {INTEGER(), VARCHAR(), ARRAY(DOUBLE())})});
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  options.maxLength = 6;
  auto input =
      makeInput(type, options, std::vector<vector_size_t>(12, 2048), 7, 41);
  for (auto& pids : input.pids) {
    for (size_t row = 0; row < pids.size(); row += 2) {
      pids[row] = 0;
    }
  }
  struct Limit {
    const char* name;
    std::function<void(CellShuffleOptions&, bool rss)> tune;
    bool localSeals;
  };
  const std::vector<Limit> limits = {
      {"spills",
       [](CellShuffleOptions& cell, bool) {
         cell.cellMemoryCapBytes = 4 << 20;
       },
       false},
      {"partition seals",
       [](CellShuffleOptions& cell, bool) {
         cell.checkpointPartitionBytes = 1;
       },
       true},
      {"null seals",
       [](CellShuffleOptions& cell, bool) { cell.nullMemLimitBytes = 1; },
       true},
      // Each limit firing between the others: locally a growth spill, two
      // seals of partition 0 and a null seal; on RSS, pushes at the cap and
      // 18 seals of partition 0 between them, the pushed windows too small
      // for a null seal.
      {"all",
       [](CellShuffleOptions& cell, bool rss) {
         cell.cellMemoryCapBytes = 4 << 20;
         cell.checkpointPartitionBytes = rss ? 128 << 10 : 1 << 20;
         cell.nullMemLimitBytes = 4 << 10;
         cell.coalesceMergedRuns = true;
       },
       true}};
  for (const auto& limit : limits) {
    SCOPED_TRACE(limit.name);
    roundTrip(
        input,
        7,
        limit.tune,
        [&](const ShuffleWriterMetrics& metrics, bool rss) {
          if (rss) {
            EXPECT_GT(metrics.spillCount, 0);
            return;
          }
          EXPECT_GT(metrics.totalBytesEvicted, 0);
          if (limit.localSeals) {
            EXPECT_GT(metrics.spillCount, 0);
          }
        },
        {{4096, 1 << 20}, {333, 1}});
  }
}

TEST_F(CellNestedTest, timestampsInsideContainers) {
  // Microsecond values: before the epoch, at it, far from it.
  std::vector<Timestamp> values = {
      Timestamp(0, 0),
      Timestamp(-1, 999'999'000),
      Timestamp(-62'135'596'800, 0),
      Timestamp(253'402'300'799, 999'999'000),
      Timestamp(1'700'000'000, 123'456'000)};
  std::mt19937_64 rng(5);
  for (int i = 0; i < 2000; ++i) {
    const auto seconds =
        static_cast<int64_t>(rng() % 200'000'000'000) - 100'000'000'000;
    values.push_back(Timestamp(seconds, (rng() % 1'000'000) * 1000));
  }
  const auto n = static_cast<vector_size_t>(values.size());
  auto flat = makeFlatVector<Timestamp>(
      n, [&](auto row) { return values[row]; }, nullEvery(9));
  auto arrays = makeArrayVector(
      [&] {
        std::vector<vector_size_t> offsets;
        for (vector_size_t row = 0; row < n; row += 4) {
          offsets.push_back(row);
        }
        return offsets;
      }(),
      flat);
  auto maps = makeMapVector(
      [&] {
        std::vector<vector_size_t> offsets;
        for (vector_size_t row = 0; row < n; row += 3) {
          offsets.push_back(row);
        }
        return offsets;
      }(),
      makeFlatVector<int32_t>(n, [](auto row) { return row; }),
      flat);
  const auto rows = std::min(arrays->size(), maps->size());
  const auto data = makeRowVector(
      {"arr", "map", "st"},
      {arrays->slice(0, rows),
       maps->slice(0, rows),
       makeRowVector({"t"}, {flat->slice(0, rows)})});
  ShuffleInput input;
  input.data = {data};
  input.pids.emplace_back(rows);
  for (vector_size_t row = 0; row < rows; ++row) {
    input.pids[0][row] = row % 3;
  }
  roundTrip(input, 3);
}

TEST_F(CellNestedTest, largeContainers) {
  // Rows of tens of thousands of elements among small ones, read back one
  // row and one append at a time as well.
  const auto type =
      ROW({"id", "values", "map"},
          {BIGINT(), ARRAY(BIGINT()), MAP(VARCHAR(), INTEGER())});
  NestedVectorMaker::Options small;
  small.nullPercent = {5};
  small.maxLength = 2;
  auto input = makeInput(type, small, {100, 100}, 7, 77);
  NestedVectorMaker::Options large;
  large.nullPercent = {0, 5};
  large.maxLength = 30'000;
  large.emptyPercent = 0;
  const auto big = makeInput(type, large, {6}, 7, 78);
  input.data.insert(input.data.begin() + 1, big.data[0]);
  input.pids.insert(input.pids.begin() + 1, big.pids[0]);
  roundTrip(
      input, 7, nullptr, nullptr, {{4096, 1 << 20}, {1, 1 << 20}, {1000, 1}});
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
