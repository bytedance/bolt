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
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <random>

#include <arrow/io/memory.h>
#include <fmt/format.h>

#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellAssembler.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellFlattener.h"
#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/LazyVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

/// ARRAY, MAP and ROW columns through the Cell shuffle, checked against
/// their input and against V1: every partition reads back its rows, in
/// order and with every map's entries in their order, whatever the
/// nesting, the nulls at each level, the input's encodings, the partition
/// count, codec, output, spills, seals and read batch sizes. The round trips
/// see the shuffle only from the outside, so they hold for any way the Cell
/// writer and reader carry nested values; each checks only which way a
/// schema takes - a nested layout, or the adapter's binary column. The last
/// tests check the nested layout's pieces directly: its flattening, the
/// flattener across seals, and what the assembler accepts.
namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

using test::ShuffleInput;

/// Decompression over an engine codec, as the reader wires it.
class TestDecompressor final : public CellDecompressor {
 public:
  explicit TestDecompressor(Codec* codec) : codec_(codec) {}

  bool decompress(
      const uint8_t* data,
      size_t size,
      uint8_t* out,
      size_t decodedSize) override {
    return codec_->decompress(
               data,
               static_cast<int64_t>(size),
               out,
               static_cast<int64_t>(decodedSize)) ==
        static_cast<int64_t>(decodedSize);
  }

 private:
  Codec* const codec_;
};

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

/// The arrays and maps of deepRowType(), four levels deep, with no ROW at
/// any depth: a nested layout's.
RowTypePtr deepNestedType() {
  return ROW(
      {"id", "arrays", "maps", "mixed", "nestedMaps", "s"},
      {BIGINT(),
       ARRAY(ARRAY(ARRAY(ARRAY(SMALLINT())))),
       MAP(VARCHAR(), ARRAY(MAP(INTEGER(), TIMESTAMP()))),
       ARRAY(MAP(VARCHAR(), ARRAY(DOUBLE()))),
       MAP(BIGINT(), MAP(VARCHAR(), MAP(INTEGER(), DOUBLE()))),
       VARCHAR()});
}

/// Whether some column of `type` is an ARRAY, MAP or ROW.
bool hasComplexColumns(const RowTypePtr& type) {
  for (const auto& child : type->children()) {
    if (child->kind() == TypeKind::ARRAY || child->kind() == TypeKind::MAP ||
        child->kind() == TypeKind::ROW) {
      return true;
    }
  }
  return false;
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
  /// V1's. Each writer must have flattened the complex columns when the
  /// schema takes a nested layout, else converted them. `tune` adjusts the
  /// writer of each output, `check` sees what each writer wrote and
  /// reports.
  void roundTrip(
      const ShuffleInput& input,
      int32_t numPartitions,
      const std::function<void(CellShuffleOptions&, bool rss)>& tune = nullptr,
      const std::function<void(const test::WrittenShuffle&, bool rss)>& check =
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
        {
          SCOPED_TRACE(rss ? "rss" : "local");
          if (CellNestedLayout::supports(rowType)) {
            EXPECT_GT(written.metrics.flattenTime, 0);
            EXPECT_EQ(written.metrics.convertTime, 0);
          } else if (hasComplexColumns(rowType)) {
            EXPECT_GT(written.metrics.convertTime, 0);
            EXPECT_EQ(written.metrics.flattenTime, 0);
          }
          if (check) {
            check(written, rss);
          }
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

  /// Every batch a CellAssembler of `type`, appending at most
  /// `appendPositions` positions at a time, reads from one partition's
  /// uncompressed bytes.
  std::vector<RowVectorPtr> assemblePartition(
      const std::string& bytes,
      const RowTypePtr& type,
      uint32_t appendPositions,
      vector_size_t batchSize,
      int64_t byteBudget = 1 << 20,
      CellDecodeLimits limits = {}) {
    CellAssembler assembler(type, nullptr, pool(), limits, appendPositions);
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    const auto nextPayload = [&] {
      if (source.atEnd()) {
        return false;
      }
      uint8_t kind;
      BOLT_CHECK(source.read(&kind, 1));
      BOLT_CHECK_EQ(kind, static_cast<uint8_t>(CellBlockKind::kPayload));
      std::string error;
      BOLT_CHECK(assembler.beginPayload(source, error), "{}", error);
      return true;
    };
    std::vector<RowVectorPtr> batches;
    while (auto batch =
               assembler.nextBatch(batchSize, byteBudget, nextPayload)) {
      batch->validate({});
      batches.push_back(std::move(batch));
    }
    return batches;
  }

  /// `data` as aggregation state rows behind a pid column of `pids`: each
  /// row [size i32][CompactRow bytes] in a CompositeRowVector.
  std::shared_ptr<CompositeRowVector> makeComposite(
      const std::vector<int32_t>& pids,
      const RowVectorPtr& data) {
    ShuffleColumnarToRowConverter converter(
        asRowType(data->type()), pool(), row::RowFormat::COMPACT);
    auto stats =
        converter.getWithStats(data, std::numeric_limits<int64_t>::max());
    std::vector<std::vector<uint8_t*>> rows(1);
    std::vector<uint32_t> partitions(data->size(), 0);
    std::vector<int64_t> partitionBytes(1, 0);
    converter.convert(stats, partitions, rows, partitionBytes);
    const auto ordinary = test::withPid(pids, data, pool());
    auto children = ordinary->children();
    auto composite = std::make_shared<CompositeRowVector>(
        ordinary->type(),
        data->size(),
        pool(),
        std::make_unique<SelectivityVector>(children.size(), true),
        std::move(children));
    composite->allocateRows(partitionBytes[0]);
    RowInfoTracker tracker(composite.get(), 0, data->size());
    for (vector_size_t i = 0; i < data->size(); ++i) {
      const auto rowSize =
          *reinterpret_cast<int32_t*>(rows[0][i]) + kSizeOfRowHeader;
      auto* row = composite->newRow();
      ::memcpy(row, rows[0][i], rowSize);
      composite->store(i, row);
      composite->advance(rowSize);
    }
    return composite;
  }

  /// The rows of a CompositeRowVector the reader returned, decoded.
  RowVectorPtr compositeRows(
      const RowTypePtr& type,
      const RowVectorPtr& batch) {
    const auto composite = std::dynamic_pointer_cast<CompositeRowVector>(batch);
    BOLT_CHECK_NOT_NULL(composite);
    std::vector<std::string_view> rows;
    for (vector_size_t row = 0; row < composite->size(); ++row) {
      auto* data = composite->rowAt(row);
      auto* end = row + 1 < composite->size()
          ? composite->rowAt(row + 1)
          : composite->rowAt(0) + composite->totalRowSize();
      rows.emplace_back(data, end - data);
    }
    ShuffleRowToColumnarConverter converter(
        type, pool(), row::RowFormat::COMPACT);
    return converter.convert(rows);
  }

  /// The row count of each payload in one partition's bytes of a nested
  /// layout `type`, uncompressed or with LZ4, read without decoding a
  /// value.
  std::vector<uint32_t> payloadRows(
      const std::string& bytes,
      const RowTypePtr& type) {
    const CellNestedLayout nested(type);
    const auto lz4 = createCodec(
        arrow::Compression::LZ4_FRAME,
        CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
    TestDecompressor decompressor(lz4.get());
    CellPayloadDecoder reader(nested.layout(), &decompressor, pool());
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    std::vector<uint32_t> rows;
    while (!source.atEnd()) {
      uint8_t kind;
      BOLT_CHECK(source.read(&kind, 1));
      BOLT_CHECK_EQ(kind, static_cast<uint8_t>(CellBlockKind::kPayload));
      uint32_t rowCount = 0;
      std::string error;
      BOLT_CHECK(reader.readPayload(source, rowCount, error), "{}", error);
      rows.push_back(rowCount);
    }
    return rows;
  }

  /// Holds the data file and, should V1 spill, its spill files.
  std::string dir_;
  std::string dataFile_;
};

TEST_F(CellNestedTest, everyLeafTypeInsideEveryContainer) {
  // Each leaf type in an array, as the values of a map and as a struct
  // field, beside flat columns; nulls at no, some, half and every row of
  // every level. The leaves V1 cannot carry go on their own, held to the
  // input alone. Without the struct the schema takes a nested layout.
  std::vector<TypePtr> withV1;
  std::vector<TypePtr> withoutV1;
  for (const auto& leaf : leafTypes()) {
    (v1Carries(leaf) ? withV1 : withoutV1).push_back(leaf);
  }
  for (const bool withRow : {true, false}) {
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
      if (withRow) {
        names.push_back("r");
        types.push_back(
            ROW(std::move(fieldNames), std::vector<TypePtr>(leaves)));
      }
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
}

TEST_F(CellNestedTest, fourLevelsAtEveryNullRate) {
  // Arrays, maps and structs four deep, alone and mixed, and arrays and
  // maps alone; the rates list each level's nulls from the top.
  const std::vector<std::vector<uint32_t>> rates = {
      {0}, {10}, {50}, {100}, {0, 100}, {0, 0, 0, 100}, {50, 0, 10, 0, 50}};
  for (const auto& type : {deepRowType(), deepNestedType()}) {
    for (size_t i = 0; i < rates.size(); ++i) {
      SCOPED_TRACE(nullTrace(7, rates[i]) + " " + type->toString());
      NestedVectorMaker::Options options;
      options.nullPercent = rates[i];
      options.maxLength = 3;
      const auto input = makeInput(type, options, {800, 3, 1200}, 7, 100 + i);
      roundTrip(input, 7);
      if (HasFatalFailure()) {
        return;
      }
    }
  }
}

TEST_F(CellNestedTest, fourLevelsAtEveryPartitionCount) {
  // One partition, and a few rows in each of 1024.
  const std::vector<std::vector<uint32_t>> rates = {
      {10}, {0, 100}, {50, 0, 10, 0, 50}};
  for (const auto& type : {deepRowType(), deepNestedType()}) {
    for (const int32_t numPartitions : {1, 1024}) {
      for (size_t i = 0; i < rates.size(); ++i) {
        SCOPED_TRACE(
            nullTrace(numPartitions, rates[i]) + " " + type->toString());
        NestedVectorMaker::Options options;
        options.nullPercent = rates[i];
        options.maxLength = 3;
        const auto input =
            makeInput(type, options, {800, 3, 1200}, numPartitions, 200 + i);
        roundTrip(
            input,
            numPartitions,
            nullptr,
            nullptr,
            {{4096, 1 << 20}, {100, 1}});
        if (HasFatalFailure()) {
          return;
        }
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
  for (const auto& type : {deepRowType(), deepNestedType()}) {
    for (const auto& [name, options] : shapes) {
      SCOPED_TRACE(fmt::format("{} {}", name, type->toString()));
      const auto input = makeInput(type, options, {500, 700}, 7, 3);
      roundTrip(input, 7);
    }
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
  for (const auto& type : {deepRowType(), deepNestedType()}) {
    for (const int32_t numPartitions : {1, 7}) {
      SCOPED_TRACE(fmt::format("P={} {}", numPartitions, type->toString()));
      const auto input = makeInput(
          type,
          options,
          {300, 500, 1, 700, 400, 600, 800},
          numPartitions,
          21,
          encodingOf);
      roundTrip(input, numPartitions);
    }
  }
}

TEST_F(CellNestedTest, spillsAndSeals) {
  // Enough rows for the writer to spill under a small cap, seal payloads
  // past a small size and windows past a small null memory: each alone and
  // all together; on an RSS client a spill is a window pushed whole. Half
  // the rows go to partition 0, whose payload passes a bound well before
  // the others' do: past a bound only it reaches, its payloads close while
  // the others' stay open. Without the struct the schema takes a nested
  // layout.
  const auto withRow =
      ROW({"id", "s", "tags", "attrs", "st"},
          {BIGINT(),
           VARCHAR(),
           ARRAY(VARCHAR()),
           MAP(INTEGER(), ARRAY(BIGINT())),
           ROW({"a", "b", "c"}, {INTEGER(), VARCHAR(), ARRAY(DOUBLE())})});
  const auto nested =
      ROW({"id", "s", "tags", "attrs", "lists"},
          {BIGINT(),
           VARCHAR(),
           ARRAY(VARCHAR()),
           MAP(INTEGER(), ARRAY(BIGINT())),
           ARRAY(MAP(INTEGER(), ARRAY(DOUBLE())))});
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  options.maxLength = 6;
  for (const auto& type : {withRow, nested}) {
    SCOPED_TRACE(type->toString());
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
      /// Some partitions' payloads close while others' stay open.
      bool partial{false};
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
        {"seals of partition 0",
         [](CellShuffleOptions& cell, bool rss) {
           cell.checkpointPartitionBytes = rss ? 128 << 10 : 1 << 20;
         },
         true,
         true},
        {"null seals",
         [](CellShuffleOptions& cell, bool) { cell.nullMemLimitBytes = 1; },
         true},
        // Each limit firing between the others. For the struct schema,
        // locally a growth spill, two seals of partition 0 and a null seal;
        // on RSS, pushes at the cap and 18 seals of partition 0 between them,
        // the pushed windows too small for a null seal. Without it, growth
        // spills and a null seal per batch, and pushes at the cap.
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
          [&](const test::WrittenShuffle& written, bool rss) {
            const auto& metrics = written.metrics;
            if (limit.partial && CellNestedLayout::supports(type)) {
              // Partition 0 sealed more often than some other partition,
              // whose payload stayed open across its seals.
              std::vector<size_t> payloads;
              for (const auto& bytes : written.partitions) {
                payloads.push_back(payloadRows(bytes, type).size());
              }
              EXPECT_GT(payloads[0], 1);
              EXPECT_LT(
                  *std::min_element(payloads.begin(), payloads.end()),
                  payloads[0]);
            }
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
  // With the struct and, in a nested layout, without.
  for (const bool withRow : {true, false}) {
    std::vector<std::string> names{"arr", "map"};
    std::vector<VectorPtr> children{
        arrays->slice(0, rows), maps->slice(0, rows)};
    if (withRow) {
      names.push_back("st");
      children.push_back(makeRowVector({"t"}, {flat->slice(0, rows)}));
    }
    ShuffleInput input;
    input.data = {makeRowVector(names, children)};
    input.pids.emplace_back(rows);
    for (vector_size_t row = 0; row < rows; ++row) {
      input.pids[0][row] = row % 3;
    }
    roundTrip(input, 3);
  }
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

TEST_F(CellNestedTest, elementsPastAChunk) {
  // Containers of up to 150000 elements among small ones: an entry's
  // elements go into several of the writer's chunks, a chunk holds the
  // elements of many entries, and a batch of a space more positions than one
  // append takes. Partition 0 takes both large rows. Read back in batches of
  // every row, of one row, of one append and with no byte target.
  const auto type =
      ROW({"id", "values", "map", "nested"},
          {BIGINT(),
           ARRAY(BIGINT()),
           MAP(INTEGER(), BIGINT()),
           ARRAY(ARRAY(SMALLINT()))});
  NestedVectorMaker::Options small;
  small.nullPercent = {5, 5, 5};
  small.maxLength = 3;
  auto input = makeInput(type, small, {300, 300}, 7, 81);
  const std::vector<vector_size_t> sizes{150'000, 70'000, 1};
  // The nested column's rows hold 100000 arrays of one element each.
  const auto inner = makeArrayVector<int16_t>(
      300'000,
      [](auto) { return 1; },
      [](auto idx) { return static_cast<int16_t>(idx % 30'011); },
      nullptr,
      [](auto idx) { return idx % 13 == 0; });
  const auto big = makeRowVector(
      {"id", "values", "map", "nested"},
      {makeFlatVector<int64_t>(3, [](auto row) { return row * 101; }),
       makeArrayVector<int64_t>(
           3,
           [&](auto row) { return sizes[row]; },
           [](auto idx) { return idx * 7919; },
           nullptr,
           [](auto idx) { return idx % 11 == 0; }),
       makeMapVector<int32_t, int64_t>(
           3,
           [](auto row) { return row == 0 ? 80'000 : 1; },
           [](auto idx) { return static_cast<int32_t>(idx); },
           [](auto idx) { return idx * 3; }),
       makeArrayVector({0, 100'000, 200'000}, inner)});
  input.data.insert(input.data.begin() + 1, big);
  input.pids.insert(input.pids.begin() + 1, {0, 0, 1});
  roundTrip(
      input,
      7,
      nullptr,
      nullptr,
      {{4096, 1 << 30}, {1, 1 << 20}, {1000, 1}, {1000, 0}});
}

TEST_F(CellNestedTest, readsAtEveryByteBudget) {
  // As for flat payloads, a byte target at or below zero reads one append
  // per batch, and a large one every row up to the batch size, across
  // payloads. A positive one also bounds each append, by the payload's
  // bytes per row, every space's together: a tiny one reads a row per
  // batch, one between them cuts batches by their bytes.
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  const auto input =
      makeInput(deepNestedType(), options, {700, 900, 300}, 7, 91);
  roundTrip(
      input,
      7,
      nullptr,
      nullptr,
      {{1000, 0}, {1000, -1}, {64, 1}, {4096, 3000}, {5000, 1 << 30}});

  // Three payloads of 1000 rows of an id and 10 elements each: a row
  // decodes into 8 + 4 bytes at the rows' positions and 10 * 8 at the
  // elements', 92 in all.
  ShuffleInput uniform;
  for (int64_t batch = 0; batch < 3; ++batch) {
    uniform.data.push_back(makeRowVector(
        {"id", "a"},
        {makeFlatVector<int64_t>(
             1000, [&](auto row) { return batch * 1000 + row; }),
         makeArrayVector<int64_t>(
             1000,
             [](auto) { return 10; },
             [&](auto idx) { return batch * 10'000 + idx; })}));
    uniform.pids.emplace_back(1000, 0);
  }
  const auto type = asRowType(uniform.data[0]->type());
  removeFiles();
  ShuffleWriterOptions writerOptions;
  writerOptions.partitioning = Partitioning::kHash;
  writerOptions.partitionWriterOptions.numPartitions = 1;
  writerOptions.partitionWriterOptions.dataFile = dataFile_;
  writerOptions.partitionWriterOptions.compressionType =
      arrow::Compression::UNCOMPRESSED;
  writerOptions.cellOptions.checkpointPartitionBytes = 1;
  const auto bytes =
      test::writeShuffle(writerOptions, uniform, pool()).partitions[0];
  ASSERT_EQ(payloadRows(bytes, type), (std::vector<uint32_t>(3, 1000)));
  const auto expected = uniform.rowsOf(1)[0];
  const auto batchSizes = [&](int32_t batchSize, int64_t budget) {
    const auto batches =
        test::readPartition(bytes, type, nullptr, pool(), batchSize, budget);
    test::expectPartitionRows(expected, batches, 0, batchSize);
    std::vector<vector_size_t> sizes;
    for (const auto& batch : batches) {
      sizes.push_back(batch->size());
    }
    return sizes;
  };
  for (const int64_t budget : {0, -1}) {
    SCOPED_TRACE(budget);
    // One append a batch, within its payload.
    std::vector<vector_size_t> appends;
    for (int payload = 0; payload < 3; ++payload) {
      appends.insert(appends.end(), {300, 300, 300, 100});
    }
    EXPECT_EQ(batchSizes(300, budget), appends);
  }
  // Full batches, across payloads.
  EXPECT_EQ(batchSizes(300, 1 << 30), std::vector<vector_size_t>(10, 300));
  // 920 bytes are 10 rows, every space's bytes counted; 1 byte, one row.
  EXPECT_EQ(batchSizes(4096, 920), std::vector<vector_size_t>(300, 10));
  EXPECT_EQ(batchSizes(4096, 1), std::vector<vector_size_t>(3000, 1));
}

TEST_F(CellNestedTest, readerTimesTheAssembly) {
  // Reading a nested layout's payloads, counting their positions and
  // assembling the batches are the reader's decode time.
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  const auto input = makeInput(deepNestedType(), options, {2000}, 1, 93);
  removeFiles();
  ShuffleWriterOptions writerOptions;
  writerOptions.partitioning = Partitioning::kHash;
  writerOptions.partitionWriterOptions.numPartitions = 1;
  writerOptions.partitionWriterOptions.dataFile = dataFile_;
  writerOptions.partitionWriterOptions.compressionType =
      arrow::Compression::UNCOMPRESSED;
  const auto bytes =
      test::writeShuffle(writerOptions, input, pool()).partitions[0];
  CellShuffleReader reader(
      std::make_shared<sparksql::test::MemoryReaderStreamIterator>(
          std::vector<std::vector<char>>{
              std::vector<char>(bytes.begin(), bytes.end())}),
      asRowType(input.data[0]->type()),
      nullptr,
      arrow::default_memory_pool(),
      pool(),
      100,
      1 << 20);
  vector_size_t rows = 0;
  while (auto batch = reader.next()) {
    rows += batch->size();
  }
  EXPECT_EQ(rows, 2000);
  EXPECT_GT(reader.decodeTimeNs(), 0);
}

TEST_F(CellNestedTest, compositeBlocksBetweenPayloads) {
  // Aggregation state rows between ordinary batches of a nested layout: a
  // composite block ends the batch being assembled and comes out on its
  // own, and the payload after it starts the next batch. Each kind's rows
  // read back in order, locally and from an RSS client, at every read
  // shape.
  const auto type =
      ROW({"k", "a", "m"},
          {BIGINT(), ARRAY(BIGINT()), MAP(INTEGER(), ARRAY(VARCHAR()))});
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20, 10};
  options.maxLength = 5;
  constexpr int32_t kPartitions = 3;
  // Whether each batch goes in as composite rows.
  const std::vector<bool> composite{
      false, true, false, false, true, true, false};
  std::vector<vector_size_t> sizes;
  for (size_t batch = 0; batch < composite.size(); ++batch) {
    sizes.push_back(300 + 100 * static_cast<vector_size_t>(batch));
  }
  const auto input = makeInput(type, options, sizes, kPartitions, 5);
  // Per kind, ordinary first, and partition: the rows in order.
  std::vector<std::vector<test::RowRef>> expected[2];
  for (auto& rows : expected) {
    rows.resize(kPartitions);
  }
  for (size_t batch = 0; batch < composite.size(); ++batch) {
    for (vector_size_t row = 0; row < input.data[batch]->size(); ++row) {
      expected[composite[batch]][input.pids[batch][row]].emplace_back(
          input.data[batch].get(), row);
    }
  }
  for (const bool rss : {false, true}) {
    SCOPED_TRACE(rss ? "rss" : "local");
    removeFiles();
    ShuffleWriterOptions writerOptions;
    writerOptions.partitioning = Partitioning::kHash;
    writerOptions.partitionWriterOptions.numPartitions = kPartitions;
    writerOptions.partitionWriterOptions.dataFile = dataFile_;
    writerOptions.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
    const auto client = std::make_shared<test::RecordingRssClient>();
    if (rss) {
      writerOptions.partitionWriterOptions.partitionWriterType =
          PartitionWriterType::kCeleborn;
      writerOptions.partitionWriterOptions.rssClient = client;
    }
    std::vector<std::string> partitions(kPartitions);
    {
      CellShuffleWriter writer(
          writerOptions, pool(), arrow::default_memory_pool());
      for (size_t batch = 0; batch < composite.size(); ++batch) {
        const RowVectorPtr rv = composite[batch]
            ? makeComposite(input.pids[batch], input.data[batch])
            : test::withPid(input.pids[batch], input.data[batch], pool());
        ASSERT_TRUE(writer.split(rv, 0).ok());
      }
      ASSERT_TRUE(writer.stop().ok());
      EXPECT_GT(writer.metrics().flattenTime, 0);
      if (rss) {
        for (const auto& [pid, blobs] : client->pushes) {
          for (const auto& blob : blobs) {
            partitions[pid] += blob;
          }
        }
      } else {
        std::ifstream in(dataFile_, std::ios::binary);
        const std::string file(
            (std::istreambuf_iterator<char>(in)),
            std::istreambuf_iterator<char>());
        uint64_t offset = 0;
        for (int32_t pid = 0; pid < kPartitions; ++pid) {
          const auto length = writer.metrics().partitionLengths[pid];
          partitions[pid] = file.substr(offset, length);
          offset += length;
        }
        EXPECT_EQ(offset, file.size());
      }
    }
    for (const auto& shape : std::vector<ReadShape>{
             {4096, 1 << 30}, {1000, 0}, {7, 1}, {250, 2000}}) {
      SCOPED_TRACE(fmt::format(
          "batches of {} rows, {} bytes",
          shape.batchSize,
          shape.batchByteSize));
      for (int32_t pid = 0; pid < kPartitions; ++pid) {
        const auto& bytes = partitions[pid];
        CellShuffleReader reader(
            std::make_shared<sparksql::test::MemoryReaderStreamIterator>(
                std::vector<std::vector<char>>{
                    std::vector<char>(bytes.begin(), bytes.end())}),
            type,
            nullptr,
            arrow::default_memory_pool(),
            pool(),
            shape.batchSize,
            shape.batchByteSize);
        std::vector<RowVectorPtr> batches[2];
        while (auto batch = reader.next()) {
          if (RowVector::isComposite(batch)) {
            ASSERT_LE(batch->size(), shape.batchSize);
            batches[1].push_back(compositeRows(type, batch));
          } else {
            batch->validate({});
            batches[0].push_back(std::move(batch));
          }
        }
        test::expectPartitionRows(
            expected[0][pid], batches[0], pid, shape.batchSize);
        {
          SCOPED_TRACE("composite rows");
          test::expectPartitionRows(
              expected[1][pid], batches[1], pid, shape.batchSize);
        }
        if (HasFatalFailure()) {
          return;
        }
      }
    }
  }
}

TEST_F(CellNestedTest, nanosecondsInsideContainers) {
  // A nested layout keeps every nanosecond of a timestamp, at any depth, as
  // a flat column does.
  std::vector<Timestamp> values = {
      Timestamp(0, 1),
      Timestamp(-1, 999'999'999),
      Timestamp(1'700'000'000, 123'456'789),
      Timestamp(-62'135'596'800, 7)};
  std::mt19937_64 rng(9);
  for (int i = 0; i < 3000; ++i) {
    const auto seconds =
        static_cast<int64_t>(rng() % 200'000'000'000) - 100'000'000'000;
    values.push_back(Timestamp(seconds, rng() % 1'000'000'000));
  }
  const auto n = static_cast<vector_size_t>(values.size());
  auto flat = makeFlatVector<Timestamp>(
      n, [&](auto row) { return values[row]; }, nullEvery(7));
  const auto rows = n / 4;
  const auto data = makeRowVector(
      {"arr", "map"},
      {makeArrayVector<Timestamp>(
           rows,
           [](auto) { return 4; },
           [&](auto idx) { return values[idx]; },
           nullEvery(9)),
       makeMapVector(
           [&] {
             std::vector<vector_size_t> offsets;
             for (vector_size_t row = 0; row < rows; ++row) {
               offsets.push_back(row * 2);
             }
             return offsets;
           }(),
           makeFlatVector<int32_t>(rows * 2, [](auto idx) { return idx; }),
           makeArrayVector(
               [&] {
                 std::vector<vector_size_t> offsets;
                 for (vector_size_t row = 0; row < rows * 2; ++row) {
                   offsets.push_back(row * 2);
                 }
                 return offsets;
               }(),
               flat))});
  ShuffleInput input;
  input.data = {data};
  input.pids.emplace_back(rows);
  for (vector_size_t row = 0; row < rows; ++row) {
    input.pids[0][row] = row % 3;
  }
  removeFiles();
  ShuffleWriterOptions options;
  options.partitioning = Partitioning::kHash;
  options.partitionWriterOptions.numPartitions = 3;
  options.partitionWriterOptions.dataFile = dataFile_;
  options.partitionWriterOptions.compressionType =
      arrow::Compression::UNCOMPRESSED;
  const auto written = test::writeShuffle(options, input, pool());
  EXPECT_GT(written.metrics.flattenTime, 0);
  const auto expected = input.rowsOf(3);
  for (int32_t pid = 0; pid < 3; ++pid) {
    const auto batches = test::readPartition(
        written.partitions[pid],
        asRowType(data->type()),
        nullptr,
        pool(),
        4096,
        1 << 20);
    test::expectPartitionRows(expected[pid], batches, pid, 4096);
  }
}

TEST_F(CellNestedTest, dictionaryProbeTakesTheRowsStrings) {
  // A string column at the rows' positions goes into dictionary form as in
  // a flat schema; the strings in arrays, at positions of their own, are
  // never probed: every dictionary row is one of the rows'.
  constexpr vector_size_t kRows = 4096;
  const char* const kWords[] = {"red", "green", "blue", "cyan"};
  const auto data = makeRowVector(
      {"s", "tags"},
      {makeFlatVector<StringView>(
           kRows, [&](auto row) { return StringView(kWords[row % 4]); }),
       makeArrayVector<StringView>(
           kRows,
           [](auto row) { return 1 + row % 3; },
           [&](auto idx) { return StringView(kWords[idx % 4]); })});
  ShuffleInput input;
  input.data = {data};
  input.pids.emplace_back(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    input.pids[0][row] = row % 5;
  }
  roundTrip(
      input,
      5,
      nullptr,
      [&](const test::WrittenShuffle& written, bool) {
        EXPECT_EQ(written.metrics.dictionaryMatchedRows, kRows);
        EXPECT_EQ(written.metrics.dictionaryFallbackRows, 0);
      },
      {{4096, 1 << 20}});
}

TEST_F(CellNestedTest, unknownElementsPastTheRowLimit) {
  // Arrays and maps of UNKNOWN carry lengths only: rows of 2^30 elements
  // cost nothing to write or read. Three such rows in one partition hold
  // more elements than a payload may, so the writer closes a window before
  // each, whether the rows come in one batch or one by one; and a batch
  // holding a payload's elements takes no next payload whose elements would
  // take its space past kMaxPositions, so it ends there.
  constexpr vector_size_t kLength = 1 << 30;
  const std::vector<vector_size_t> lengths{kLength, 5, kLength, kLength};
  const auto rows = static_cast<vector_size_t>(lengths.size());
  for (const bool map : {false, true}) {
    auto offsets = allocateOffsets(rows, pool());
    auto sizes = allocateSizes(rows, pool());
    for (vector_size_t row = 0; row < rows; ++row) {
      offsets->asMutable<vector_size_t>()[row] = 0;
      sizes->asMutable<vector_size_t>()[row] = lengths[row];
    }
    auto elements = BaseVector::createNullConstant(UNKNOWN(), kLength, pool());
    VectorPtr column = map ? VectorPtr(std::make_shared<MapVector>(
                                 pool(),
                                 MAP(UNKNOWN(), UNKNOWN()),
                                 nullptr,
                                 rows,
                                 offsets,
                                 sizes,
                                 elements,
                                 elements))
                           : VectorPtr(std::make_shared<ArrayVector>(
                                 pool(),
                                 ARRAY(UNKNOWN()),
                                 nullptr,
                                 rows,
                                 offsets,
                                 sizes,
                                 elements));
    const auto data = makeRowVector(
        {"id", "c"},
        {makeFlatVector<int64_t>(rows, [](auto row) { return row; }), column});
    const auto type = asRowType(data->type());
    for (const bool oneBatch : {true, false}) {
      for (const bool rss : {false, true}) {
        SCOPED_TRACE(fmt::format(
            "{} {} {}",
            map ? "map" : "array",
            oneBatch ? "one batch" : "a batch per row",
            rss ? "rss" : "local"));
        ShuffleInput input;
        if (oneBatch) {
          input.data = {data};
          input.pids = {std::vector<int32_t>(rows, 0)};
        } else {
          for (vector_size_t row = 0; row < rows; ++row) {
            input.data.push_back(
                std::dynamic_pointer_cast<RowVector>(data->slice(row, 1)));
            input.pids.push_back({0});
          }
        }
        removeFiles();
        ShuffleWriterOptions options;
        options.partitioning = Partitioning::kHash;
        options.partitionWriterOptions.numPartitions = 1;
        options.partitionWriterOptions.dataFile = dataFile_;
        if (rss) {
          options.partitionWriterOptions.partitionWriterType =
              PartitionWriterType::kCeleborn;
          options.partitionWriterOptions.rssClient =
              std::make_shared<test::RecordingRssClient>();
        }
        const auto written = test::writeShuffle(options, input, pool());
        EXPECT_GT(written.metrics.flattenTime, 0);
        const auto batches = test::readPartition(
            written.partitions[0],
            type,
            createCodec(
                arrow::Compression::LZ4_FRAME,
                CodecOptions{
                    CodecBackend::NONE, kDefaultCompressionLevel, true})
                .get(),
            pool(),
            4096,
            1 << 20);
        // A payload per window: rows 0 and 1, row 2, row 3.
        ASSERT_EQ(batches.size(), 3);
        const std::vector<vector_size_t> batchRows{2, 1, 1};
        vector_size_t next = 0;
        for (size_t b = 0; b < batches.size(); ++b) {
          ASSERT_EQ(batches[b]->size(), batchRows[b]);
          const auto* ids = batches[b]->childAt(0)->asFlatVector<int64_t>();
          const auto* containers =
              batches[b]->childAt(1)->as<ArrayVectorBase>();
          ASSERT_NE(containers, nullptr);
          uint64_t total = 0;
          for (vector_size_t row = 0; row < batches[b]->size(); ++row, ++next) {
            EXPECT_EQ(ids->valueAt(row), next);
            EXPECT_FALSE(containers->isNullAt(row));
            EXPECT_EQ(containers->sizeAt(row), lengths[next]);
            total += containers->sizeAt(row);
          }
          const auto& values = map
              ? batches[b]->childAt(1)->as<MapVector>()->mapValues()
              : batches[b]->childAt(1)->as<ArrayVector>()->elements();
          EXPECT_EQ(values->size(), total);
          EXPECT_TRUE(values->isNullAt(0));
        }
      }
    }
  }
}

TEST_F(CellNestedTest, nullElementsPastTheRowLimit) {
  // The rows of unknownElementsPastTheRowLimit with null BIGINT elements: a
  // space whose column is on the wire closes windows by its positions as
  // well, however few bytes they take. The payloads are parsed, not read:
  // their elements would take 8 GiB.
  constexpr vector_size_t kLength = 1 << 30;
  const std::vector<vector_size_t> lengths{kLength, 5, kLength, kLength};
  const auto rows = static_cast<vector_size_t>(lengths.size());
  auto offsets = allocateOffsets(rows, pool());
  auto sizes = allocateSizes(rows, pool());
  for (vector_size_t row = 0; row < rows; ++row) {
    sizes->asMutable<vector_size_t>()[row] = lengths[row];
  }
  const auto data = makeRowVector(
      {"id", "c"},
      {makeFlatVector<int64_t>(rows, [](auto row) { return row; }),
       std::make_shared<ArrayVector>(
           pool(),
           ARRAY(BIGINT()),
           nullptr,
           rows,
           offsets,
           sizes,
           BaseVector::createNullConstant(BIGINT(), kLength, pool()))});
  const auto type = asRowType(data->type());
  for (const bool oneBatch : {true, false}) {
    for (const bool rss : {false, true}) {
      SCOPED_TRACE(fmt::format(
          "{} {}",
          oneBatch ? "one batch" : "a batch per row",
          rss ? "rss" : "local"));
      ShuffleInput input;
      if (oneBatch) {
        input.data = {data};
        input.pids = {std::vector<int32_t>(rows, 0)};
      } else {
        for (vector_size_t row = 0; row < rows; ++row) {
          input.data.push_back(
              std::dynamic_pointer_cast<RowVector>(data->slice(row, 1)));
          input.pids.push_back({0});
        }
      }
      removeFiles();
      ShuffleWriterOptions options;
      options.partitioning = Partitioning::kHash;
      options.partitionWriterOptions.numPartitions = 1;
      options.partitionWriterOptions.dataFile = dataFile_;
      if (rss) {
        options.partitionWriterOptions.partitionWriterType =
            PartitionWriterType::kCeleborn;
        options.partitionWriterOptions.rssClient =
            std::make_shared<test::RecordingRssClient>();
      }
      const auto written = test::writeShuffle(options, input, pool());
      // A payload per window: rows 0 and 1, row 2, row 3.
      EXPECT_EQ(
          payloadRows(written.partitions[0], type),
          (std::vector<uint32_t>{2, 1, 1}));
    }
  }
}

TEST_F(CellNestedTest, positionsUpToTheLimit) {
  // A container's space holds kMaxPositions positions in a payload, the
  // most whose null bits one vector holds in whole bytes: a row of that
  // many elements is written and read back whole, a row of one more fails.
  // Two payloads whose elements add up to exactly that share a batch; with
  // one element more, the second starts the next batch.
  constexpr auto kMax =
      static_cast<vector_size_t>(CellNestedLayout::kMaxPositions);
  const auto arrayOf = [&](vector_size_t length) {
    auto offsets = allocateOffsets(1, pool());
    auto sizes = allocateSizes(1, pool());
    sizes->asMutable<vector_size_t>()[0] = length;
    return makeRowVector(
        {"c"},
        {std::make_shared<ArrayVector>(
            pool(),
            ARRAY(UNKNOWN()),
            nullptr,
            1,
            offsets,
            sizes,
            BaseVector::createNullConstant(UNKNOWN(), length, pool()))});
  };
  const auto type = asRowType(arrayOf(1)->type());
  const auto options = [&] {
    removeFiles();
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.partitionWriterOptions.numPartitions = 1;
    options.partitionWriterOptions.dataFile = dataFile_;
    options.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
    return options;
  };
  // One row's partition bytes.
  const auto bytesOf = [&](vector_size_t length) {
    ShuffleInput input;
    input.data = {arrayOf(length)};
    input.pids = {{0}};
    return test::writeShuffle(options(), input, pool()).partitions[0];
  };
  {
    const auto batches =
        test::readPartition(bytesOf(kMax), type, nullptr, pool(), 4096, 0);
    ASSERT_EQ(batches.size(), 1);
    const auto* arrays = batches[0]->childAt(0)->as<ArrayVector>();
    EXPECT_EQ(arrays->sizeAt(0), kMax);
    EXPECT_EQ(arrays->elements()->size(), kMax);
  }
  {
    CellShuffleWriter writer(options(), pool(), arrow::default_memory_pool());
    EXPECT_THROW(
        (void)writer.split(test::withPid({0}, arrayOf(kMax + 1), pool()), 0),
        BoltUserError);
  }
  for (const vector_size_t extra : {0, 1}) {
    SCOPED_TRACE(fmt::format("{} past the limit", extra));
    const vector_size_t first = 1 << 30;
    const auto batches = test::readPartition(
        bytesOf(first) + bytesOf(kMax - first + extra),
        type,
        nullptr,
        pool(),
        4096,
        1 << 20);
    ASSERT_EQ(batches.size(), extra == 0 ? 1 : 2);
    EXPECT_EQ(batches[0]->size(), extra == 0 ? 2 : 1);
  }
}

TEST_F(CellNestedTest, constantArraysPastThePositionLimit) {
  // A constant array repeats its elements wherever it is referenced: four
  // references to one of 2^30 elements are 2^32 positions in one row, none
  // in 32 bits; 2^20 references to an array of 2^20 references to one of
  // 2^10 elements are 2^50. The writer counts in 64 bits, stops once past
  // the limit and fails the row, having split nothing of it.
  const auto innerOf = [&](vector_size_t length, const VectorPtr& elements) {
    auto offsets = allocateOffsets(1, pool());
    auto sizes = allocateSizes(1, pool());
    offsets->asMutable<vector_size_t>()[0] = 0;
    sizes->asMutable<vector_size_t>()[0] = length;
    return std::make_shared<ArrayVector>(
        pool(), ARRAY(elements->type()), nullptr, 1, offsets, sizes, elements);
  };
  const auto repeat = [&](vector_size_t times, const VectorPtr& vector) {
    return innerOf(times, BaseVector::wrapInConstant(times, 0, vector));
  };
  const auto leaf = BaseVector::wrapInConstant(
      1 << 30, 0, makeFlatVector<int64_t>(std::vector<int64_t>{7}));
  const std::vector<VectorPtr> columns{
      repeat(4, innerOf(1 << 30, leaf)),
      repeat(1 << 20, repeat(1 << 20, innerOf(1 << 10, leaf)))};
  for (const auto& column : columns) {
    SCOPED_TRACE(column->type()->toString());
    removeFiles();
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.partitionWriterOptions.numPartitions = 2;
    options.partitionWriterOptions.dataFile = dataFile_;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    const auto data = makeRowVector({"a"}, {column});
    EXPECT_THROW(
        (void)writer.split(test::withPid({1}, data, pool()), 0), BoltUserError);
  }
}

/// Loads `values`, first calling `onLoad`: memory pressure arriving while
/// the writer decodes a batch.
class CallbackLoader final : public VectorLoader {
 public:
  CallbackLoader(VectorPtr values, std::function<void()> onLoad)
      : values_(std::move(values)), onLoad_(std::move(onLoad)) {}

 protected:
  void loadInternal(
      RowSet /*rows*/,
      ValueHook* /*hook*/,
      vector_size_t /*resultSize*/,
      VectorPtr* result) override {
    onLoad_();
    *result = values_;
  }

 private:
  VectorPtr values_;
  std::function<void()> onLoad_;
};

TEST_F(CellNestedTest, pushWhileABatchIsDecoded) {
  // On a remote shuffle, partition 0's window holds kMaxPositions - 100
  // UNKNOWN elements, and the next row brings 200: the window must close
  // before it. A reclaim pushes it before the row comes, or while the
  // writer decodes the row - loading a lazy vector inside a container, the
  // split not begun - and either way the row then goes into the next
  // window, which it fits.
  constexpr auto kBig =
      static_cast<vector_size_t>(CellNestedLayout::kMaxPositions) - 100;
  constexpr vector_size_t kRows = 2000;
  // `rows` arrays of `elements`, the first holding `length` of them.
  const auto arrays =
      [&](vector_size_t rows, vector_size_t length, VectorPtr elements) {
        auto offsets = allocateOffsets(rows, pool());
        auto sizes = allocateSizes(rows, pool());
        sizes->asMutable<vector_size_t>()[0] = length;
        return std::make_shared<ArrayVector>(
            pool(),
            ARRAY(elements->type()),
            nullptr,
            rows,
            offsets,
            sizes,
            std::move(elements));
      };
  for (const bool whileDecoding : {false, true}) {
    SCOPED_TRACE(whileDecoding ? "while decoding the row" : "before the row");
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.partitionWriterOptions.numPartitions = 1;
    options.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
    options.partitionWriterOptions.partitionWriterType =
        PartitionWriterType::kCeleborn;
    const auto client = std::make_shared<test::RecordingRssClient>();
    options.partitionWriterOptions.rssClient = client;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    // 16 MB of strings: a window worth a push.
    const std::string text(8 << 10, 'q');
    const auto first = makeRowVector(
        {"s", "a", "b"},
        {makeFlatVector<StringView>(
             kRows, [&](auto) { return StringView(text); }),
         arrays(
             kRows,
             kBig,
             BaseVector::createNullConstant(UNKNOWN(), kBig, pool())),
         arrays(kRows, 0, makeFlatVector<int64_t>(std::vector<int64_t>{}))});
    ASSERT_TRUE(
        writer
            .split(
                test::withPid(std::vector<int32_t>(kRows, 0), first, pool()), 0)
            .ok());
    int reclaims = 0;
    const auto reclaim = [&] {
      int64_t freed = 0;
      ASSERT_TRUE(writer.reclaimFixedSize(int64_t{1} << 30, &freed).ok());
      ++reclaims;
    };
    VectorPtr values = makeFlatVector<int64_t>({7, 8, 9});
    if (whileDecoding) {
      values = std::make_shared<LazyVector>(
          pool(),
          BIGINT(),
          3,
          std::make_unique<CallbackLoader>(values, reclaim));
    } else {
      reclaim();
    }
    const auto row = makeRowVector(
        {"s", "a", "b"},
        {makeFlatVector<StringView>({"x"}),
         arrays(1, 200, BaseVector::createNullConstant(UNKNOWN(), 200, pool())),
         arrays(1, 3, values)});
    ASSERT_TRUE(writer.split(test::withPid({0}, row, pool()), 0).ok());
    EXPECT_EQ(reclaims, 1);
    ASSERT_TRUE(writer.stop().ok());
    std::string bytes;
    for (const auto& blob : client->pushes[0]) {
      bytes += blob;
    }
    const auto type = asRowType(row->type());
    // The pushed window, then the row's.
    EXPECT_EQ(payloadRows(bytes, type), (std::vector<uint32_t>{kRows, 1}));
    const auto batches =
        test::readPartition(bytes, type, nullptr, pool(), 4096, 1 << 20);
    std::vector<vector_size_t> lengths;
    for (const auto& batch : batches) {
      const auto* a = batch->childAt(1)->as<ArrayVector>();
      for (vector_size_t i = 0; i < batch->size(); ++i) {
        lengths.push_back(a->sizeAt(i));
      }
    }
    ASSERT_EQ(lengths.size(), static_cast<size_t>(kRows) + 1);
    EXPECT_EQ(lengths[0], kBig);
    EXPECT_EQ(lengths[kRows], 200);
    const auto& last = batches.back();
    const auto* b = last->childAt(2)->as<ArrayVector>();
    ASSERT_EQ(b->sizeAt(last->size() - 1), 3);
    const auto* elements = b->elements()->asFlatVector<int64_t>();
    const auto offset = b->offsetAt(last->size() - 1);
    EXPECT_EQ(elements->valueAt(offset), 7);
    EXPECT_EQ(elements->valueAt(offset + 2), 9);
  }
}

TEST_F(CellNestedTest, fuzzedSchemasMatchV1) {
  // Random arrays and maps over every leaf type V1 carries, up to three
  // deep, from VectorFuzzer in the encodings it makes, lazy ones included:
  // what V1 reads back, the Cell shuffle must.
  const std::vector<TypePtr> leaves{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
      DECIMAL(10, 2),
      DECIMAL(30, 3)};
  const std::vector<TypePtr> keys{INTEGER(), BIGINT(), VARCHAR()};
  std::mt19937_64 rng(2026);
  std::function<TypePtr(uint32_t)> randomType = [&](uint32_t depth) -> TypePtr {
    const auto pick = rng() % 4;
    if (depth == 0 || pick >= 2) {
      return leaves[rng() % leaves.size()];
    }
    if (pick == 0) {
      return ARRAY(randomType(depth - 1));
    }
    return MAP(keys[rng() % keys.size()], randomType(depth - 1));
  };
  for (uint32_t iteration = 0; iteration < 10; ++iteration) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    const auto numColumns = 1 + rng() % 4;
    for (uint32_t col = 0; col < numColumns; ++col) {
      names.push_back(fmt::format("c{}", col));
      types.push_back(col == 0 ? ARRAY(randomType(2)) : randomType(3));
    }
    const auto type = ROW(std::move(names), std::move(types));
    SCOPED_TRACE(fmt::format("iteration {}: {}", iteration, type->toString()));
    ASSERT_TRUE(CellNestedLayout::supports(type));
    VectorFuzzer::Options fuzzerOptions;
    fuzzerOptions.vectorSize = 300;
    fuzzerOptions.nullRatio = 0.05 * (iteration % 5);
    fuzzerOptions.containerLength = 6;
    fuzzerOptions.complexElementsMaxSize = 3000;
    fuzzerOptions.stringLength = 20;
    fuzzerOptions.stringVariableLength = true;
    fuzzerOptions.allowLazyVector = iteration % 2 == 1;
    // What V1 keeps of a timestamp.
    fuzzerOptions.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
    fuzzerOptions.timestampFormat = {
        VectorFuzzer::Options::TimestampFormat::kStandard};
    VectorFuzzer fuzzer(fuzzerOptions, pool(), 7 + iteration);
    ShuffleInput input;
    for (uint32_t batch = 0; batch < 3; ++batch) {
      input.data.push_back(fuzzer.fuzzInputRow(type));
      auto& pids = input.pids.emplace_back(input.data.back()->size());
      for (auto& pid : pids) {
        pid = static_cast<int32_t>(rng() % 7);
      }
    }
    roundTrip(input, 7, nullptr, nullptr, {{4096, 1 << 20}, {7, 100}});
    if (HasFatalFailure()) {
      return;
    }
  }
}

TEST_F(CellNestedTest, flattenTimeIsNoPartOfTheSplit) {
  // A nested layout's batches are flattened, not converted: flattenTime
  // counts the flattening and splitTime the rest of each split, as V1
  // counts them, so that together they fit in the time split() took.
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 20};
  options.wrapPercent = 50;
  options.scatteredPercent = 30;
  const auto input = makeInput(
      deepNestedType(),
      options,
      {2000, 2000, 2000},
      7,
      111,
      [](uint32_t batch, uint32_t col) {
        return (batch + col) % 2 == 0 ? Encoding::kDictionary : Encoding::kFlat;
      });
  for (const bool rss : {false, true}) {
    SCOPED_TRACE(rss ? "rss" : "local");
    removeFiles();
    ShuffleWriterOptions writerOptions;
    writerOptions.partitioning = Partitioning::kHash;
    writerOptions.partitionWriterOptions.numPartitions = 7;
    writerOptions.partitionWriterOptions.dataFile = dataFile_;
    if (rss) {
      writerOptions.partitionWriterOptions.partitionWriterType =
          PartitionWriterType::kCeleborn;
      writerOptions.partitionWriterOptions.rssClient =
          std::make_shared<test::RecordingRssClient>();
    }
    CellShuffleWriter writer(
        writerOptions, pool(), arrow::default_memory_pool());
    int64_t wall = 0;
    for (size_t batch = 0; batch < input.data.size(); ++batch) {
      const auto rv =
          test::withPid(input.pids[batch], input.data[batch], pool());
      const auto start = std::chrono::steady_clock::now();
      ASSERT_TRUE(writer.split(rv, 0).ok());
      wall += std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
    }
    ASSERT_TRUE(writer.stop().ok());
    const auto& metrics = writer.metrics();
    EXPECT_GT(metrics.flattenTime, 0);
    EXPECT_GT(metrics.splitTime, 0);
    EXPECT_EQ(metrics.convertTime, 0);
    EXPECT_LE(metrics.splitTime + metrics.flattenTime, wall);
  }
}

TEST_F(CellNestedTest, nestedLayoutFlattensInPreOrder) {
  // A nested layout needs an ARRAY or MAP column, no ROW at any depth,
  // leaves Cell carries and at most 65535 columns once flattened.
  EXPECT_FALSE(CellNestedLayout::supports(ROW({"a"}, {BIGINT()})));
  EXPECT_FALSE(CellNestedLayout::supports(
      ROW({"a", "r"}, {ARRAY(BIGINT()), ROW({"x"}, {INTEGER()})})));
  EXPECT_FALSE(CellNestedLayout::supports(
      ROW({"a"}, {MAP(INTEGER(), ARRAY(ROW({"x"}, {INTEGER()})))})));
  EXPECT_FALSE(CellNestedLayout::supports(ROW({"a"}, {ARRAY(VARIANT())})));
  EXPECT_TRUE(CellNestedLayout::supports(ROW({"a"}, {ARRAY(UNKNOWN())})));
  EXPECT_TRUE(
      CellNestedLayout::supports(ROW({"m"}, {MAP(UNKNOWN(), UNKNOWN())})));
  for (const uint32_t extra : {1, 2}) {
    // 32767 arrays flatten into 65534 columns.
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (uint32_t i = 0; i < 32'767 + extra; ++i) {
      names.push_back(fmt::format("c{}", i));
      types.push_back(i < 32'767 ? TypePtr(ARRAY(BIGINT())) : BIGINT());
    }
    EXPECT_EQ(
        CellNestedLayout::supports(ROW(std::move(names), std::move(types))),
        extra == 1);
  }

  // m MAP<VARCHAR, ARRAY<UNKNOWN>>, k BIGINT, a ARRAY<ARRAY<DATE>>: nodes
  // and columns in pre-order, a space per container in pre-order.
  using Kind = CellNestedLayout::Kind;
  constexpr auto kNone = CellNestedLayout::kNone;
  const CellNestedLayout layout(
      ROW({"m", "k", "a"},
          {MAP(VARCHAR(), ARRAY(UNKNOWN())), BIGINT(), ARRAY(ARRAY(DATE()))}));
  ASSERT_EQ(layout.numNodes(), 8);
  struct ExpectedNode {
    Kind kind;
    TypeKind column;
    uint32_t space;
    uint32_t spaceColumn;
    uint32_t ownedSpace;
    std::vector<uint32_t> children;
  };
  const std::vector<ExpectedNode> nodes{
      {Kind::kMap, TypeKind::INTEGER, 0, 0, 1, {1, 2}},
      {Kind::kLeaf, TypeKind::VARCHAR, 1, 0, kNone, {}},
      {Kind::kArray, TypeKind::INTEGER, 1, 1, 2, {3}},
      {Kind::kLeaf, TypeKind::UNKNOWN, 2, 0, kNone, {}},
      {Kind::kLeaf, TypeKind::BIGINT, 0, 1, kNone, {}},
      {Kind::kArray, TypeKind::INTEGER, 0, 2, 3, {6}},
      {Kind::kArray, TypeKind::INTEGER, 3, 0, 4, {7}},
      {Kind::kLeaf, TypeKind::INTEGER, 4, 0, kNone, {}}};
  for (uint32_t n = 0; n < nodes.size(); ++n) {
    SCOPED_TRACE(fmt::format("node {}", n));
    const auto& node = layout.node(n);
    EXPECT_EQ(node.kind, nodes[n].kind);
    EXPECT_EQ(layout.layout().rowType()->childAt(n)->kind(), nodes[n].column);
    EXPECT_EQ(node.space, nodes[n].space);
    EXPECT_EQ(node.spaceColumn, nodes[n].spaceColumn);
    EXPECT_EQ(node.ownedSpace, nodes[n].ownedSpace);
    EXPECT_EQ(node.children, nodes[n].children);
  }
  EXPECT_EQ(layout.topNodes(), (std::vector<uint32_t>{0, 4, 5}));
  // A leaf keeps its logical type, in the layout and its space's.
  EXPECT_TRUE(layout.layout().rowType()->childAt(7)->isDate());
  EXPECT_TRUE(layout.space(4).rowType->childAt(0)->isDate());

  struct ExpectedSpace {
    uint32_t owner;
    uint32_t depth;
    std::vector<uint32_t> columns;
    std::vector<uint32_t> splitColumns;
    std::vector<uint32_t> containers;
  };
  const std::vector<ExpectedSpace> spaces{
      {kNone, 0, {0, 4, 5}, {0, 4, 5}, {0, 5}},
      {0, 1, {1, 2}, {1, 2}, {2}},
      {2, 2, {3}, {}, {}},
      {5, 1, {6}, {6}, {6}},
      {6, 2, {7}, {7}, {}}};
  ASSERT_EQ(layout.numSpaces(), spaces.size());
  EXPECT_EQ(layout.maxDepth(), 2);
  for (uint32_t s = 0; s < spaces.size(); ++s) {
    SCOPED_TRACE(fmt::format("space {}", s));
    const auto& space = layout.space(s);
    EXPECT_EQ(space.owner, spaces[s].owner);
    EXPECT_EQ(space.depth, spaces[s].depth);
    EXPECT_EQ(space.columns, spaces[s].columns);
    EXPECT_EQ(space.splitColumns, spaces[s].splitColumns);
    EXPECT_EQ(space.containers, spaces[s].containers);
    ASSERT_EQ(space.rowType->size(), space.columns.size());
    for (uint32_t i = 0; i < space.columns.size(); ++i) {
      EXPECT_EQ(
          space.rowType->childAt(i)->kind(),
          layout.layout().rowType()->childAt(space.columns[i])->kind());
    }
  }
  // The UNKNOWN column has no wire column: wire columns from 3 on are the
  // next layout column's.
  ASSERT_EQ(layout.layout().numWireColumns(), 7);
  EXPECT_EQ(layout.wireSpaces(), (std::vector<uint32_t>{0, 1, 1, 0, 0, 3, 4}));
}

TEST_F(CellNestedTest, flattenerFollowsSealsBeforeTheSplit) {
  // On a remote shuffle a push may close payloads after prepare() and
  // before split(): the split still starts the elements of each closed
  // partition at its new payload's first position, and of each other one
  // where its open payload's left off. A seal of the largest partition
  // alone, and later a window seal, come between the two calls here, the
  // elements having nulls for wrong positions to show; every payload reads
  // back whole. With chunks of 5 elements an entry's elements span chunks
  // wherever it holds more, and appends of 3 positions cut every space's
  // batch in pieces.
  const auto type =
      ROW({"a", "m"}, {ARRAY(BIGINT()), MAP(INTEGER(), ARRAY(VARCHAR()))});
  constexpr int32_t kPartitions = 3;
  NestedVectorMaker::Options options;
  options.nullPercent = {10, 30, 30};
  options.maxLength = 8;
  auto input = makeInput(type, options, {400, 300, 500, 200}, kPartitions, 61);
  // Half the rows go to partition 0.
  for (auto& pids : input.pids) {
    for (size_t row = 0; row < pids.size(); ++row) {
      pids[row] = row % 4 < 2 ? 0 : static_cast<int32_t>(row % 4) - 1;
    }
  }
  const auto expected = input.rowsOf(kPartitions);
  const CellNestedLayout nested(type);
  const CellLayout layout = nested.layout();
  for (const uint32_t chunkPositions : {CellFlattener::kChunkPositions, 5u}) {
    SCOPED_TRACE(fmt::format("chunks of {}", chunkPositions));
    removeFiles();
    ShuffleWriterOptions writerOptions;
    writerOptions.partitioning = Partitioning::kHash;
    writerOptions.partitionWriterOptions.numPartitions = kPartitions;
    writerOptions.partitionWriterOptions.dataFile = dataFile_;
    writerOptions.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
    ShuffleWriterMetrics metrics;
    {
      LocalRawOutput raw(writerOptions.partitionWriterOptions);
      LocalCellOutput output(
          raw,
          &layout,
          writerOptions.cellOptions,
          writerOptions.partitionWriterOptions,
          pool());
      ChunkAllocator allocator(pool(), 16 << 10, 256);
      CellSplitter splitter(layout, kPartitions, allocator, output, *pool());
      CellFlattener flattener(
          nested, splitter, kPartitions, *pool(), chunkPositions);
      for (size_t batch = 0; batch < input.data.size(); ++batch) {
        const auto rv =
            test::withPid(input.pids[batch], input.data[batch], pool());
        const std::vector<uint32_t> pids(
            input.pids[batch].begin(), input.pids[batch].end());
        std::vector<uint32_t> rows(kPartitions, 0);
        for (const auto pid : pids) {
          ++rows[pid];
        }
        flattener.prepare(*rv);
        ASSERT_FALSE(flattener.exceedsPositions());
        if (batch == 1) {
          // Partition 0's payload closes; the others' stay open.
          EXPECT_EQ(
              splitter.sealPartitionsOver(splitter.maxPartitionBytes() - 1), 1);
          EXPECT_EQ(splitter.windowRowCounts()[0], 0);
          EXPECT_GT(splitter.windowRowCounts()[1], 0);
        } else if (batch == 3) {
          splitter.sealWindow();
        }
        flattener.split(pids.data(), rows.data());
      }
      EXPECT_EQ(splitter.sealCount(), 2);
      splitter.finish(metrics);
    }
    std::ifstream in(dataFile_, std::ios::binary);
    const std::string file(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    uint64_t offset = 0;
    for (int32_t pid = 0; pid < kPartitions; ++pid) {
      const auto length = static_cast<uint64_t>(metrics.partitionLengths[pid]);
      const auto bytes = file.substr(offset, length);
      offset += length;
      test::expectPartitionRows(
          expected[pid],
          test::readPartition(bytes, type, nullptr, pool(), 4096, 1 << 20),
          pid,
          4096);
      for (const vector_size_t batchSize : {4096, 7}) {
        SCOPED_TRACE(fmt::format("appends of 3, batches of {}", batchSize));
        test::expectPartitionRows(
            expected[pid],
            assemblePartition(bytes, type, 3, batchSize),
            pid,
            batchSize);
      }
    }
    EXPECT_EQ(offset, file.size());
  }
}

/// One payload as the format lays it out, its kind byte excluded: `rows`
/// rows, `nullBody` as it is, no dictionary, and `streams` in one stored
/// run.
std::string storedPayload(
    uint32_t rows,
    const std::string& nullBody,
    uint32_t numWireColumns,
    const std::vector<std::string>& streams) {
  std::string out;
  const auto put = [&](auto value) {
    out.append(reinterpret_cast<const char*>(&value), sizeof(value));
  };
  put(rows);
  put(uint32_t{1});
  put(uint64_t{0});
  put(static_cast<uint32_t>(nullBody.size()));
  put(uint32_t{0});
  out += nullBody;
  out.append((numWireColumns + 7) / 8, '\0');
  uint64_t total = 0;
  for (const auto& stream : streams) {
    total += stream.size();
  }
  out.push_back(static_cast<char>(RunLayout::kCombinedUncompressed));
  put(total);
  for (const auto& stream : streams) {
    put(static_cast<uint64_t>(stream.size()));
  }
  for (const auto& stream : streams) {
    out += stream;
  }
  return out;
}

template <typename T>
std::string encoded(const std::vector<T>& values) {
  std::string out;
  encodeStream<T>(values.data(), values.size(), out);
  return out;
}

TEST_F(CellNestedTest, assemblerChecksTheLengths) {
  // ARRAY<BIGINT> payloads made by hand, nulls nowhere: a length must not
  // be negative, the lengths of a space must add up to at most
  // kMaxPositions, counted in 64 bits, and their stream must hold exactly
  // the entries'. Each fails the payload as it begins, before any value is
  // read; lengths promising more elements than there are fail the batch.
  const auto type = ROW({"a"}, {ARRAY(BIGINT())});
  EXPECT_EQ(
      makeCellAssembler(ROW({"a"}, {BIGINT()}), nullptr, pool()), nullptr);
  EXPECT_EQ(
      makeCellAssembler(
          ROW({"a", "r"}, {ARRAY(BIGINT()), ROW({"x"}, {INTEGER()})}),
          nullptr,
          pool()),
      nullptr);
  // Both wire columns kNoNull.
  const std::string noNulls(1, '\x05');
  const auto begin = [&](CellBatchSource& assembler,
                         const std::string& payload,
                         std::string& error) {
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    const bool ok = assembler.beginPayload(source, error);
    EXPECT_TRUE(!ok || source.atEnd());
    return ok;
  };
  const auto noMore = [] { return false; };
  std::string error;
  {
    auto assembler = makeCellAssembler(type, nullptr, pool());
    ASSERT_NE(assembler, nullptr);
    ASSERT_TRUE(begin(
        *assembler,
        storedPayload(
            3,
            noNulls,
            2,
            {encoded<int32_t>({2, 0, 1}), encoded<int64_t>({10, 20, 30})}),
        error))
        << error;
    const auto batch = assembler->nextBatch(10, 1 << 20, noMore);
    ASSERT_NE(batch, nullptr);
    batch->validate({});
    test::expectPartitionRows(
        test::rowsIn({makeRowVector(
            {"a"}, {makeArrayVector<int64_t>({{10, 20}, {}, {30}})})}),
        {batch},
        0,
        10);
    EXPECT_EQ(assembler->nextBatch(10, 1 << 20, noMore), nullptr);
  }
  constexpr auto kMax = static_cast<int32_t>(CellNestedLayout::kMaxPositions);
  constexpr auto kInt32Max = std::numeric_limits<int32_t>::max();
  {
    // kMaxPositions null elements: the payload begins.
    auto assembler = makeCellAssembler(type, nullptr, pool());
    EXPECT_TRUE(begin(
        *assembler,
        storedPayload(
            1, std::string(1, '\x01'), 2, {encoded<int32_t>({kMax}), ""}),
        error))
        << error;
  }
  const std::vector<int32_t> wide{100, 2000, 30000};
  auto cut = encoded<int32_t>(wide);
  cut.pop_back();
  std::vector<int32_t> twoBlocks(20, 3);
  twoBlocks[5] = 9;
  const auto firstBlock =
      encoded<int32_t>({twoBlocks.begin(), twoBlocks.begin() + 16});
  const std::vector<std::tuple<const char*, std::string, std::string>> cases{
      {"a negative length",
       storedPayload(
           3,
           noNulls,
           2,
           {encoded<int32_t>({2, -1, 1}), encoded<int64_t>({10, 20, 30})}),
       "negative container length"},
      {"lengths past the payload's positions",
       storedPayload(2, noNulls, 2, {encoded<int32_t>({kInt32Max, 1}), ""}),
       "container lengths beyond the payload's positions"},
      {"lengths one past the positions a space holds",
       storedPayload(1, noNulls, 2, {encoded<int32_t>({kMax + 1}), ""}),
       "container lengths beyond the payload's positions"},
      {"lengths past 32 bits",
       storedPayload(
           3, noNulls, 2, {encoded<int32_t>({kInt32Max, kInt32Max, 2}), ""}),
       "container lengths beyond the payload's positions"},
      {"a length stream cut short",
       storedPayload(3, noNulls, 2, {cut, ""}),
       "malformed container length stream"},
      {"a length stream a block short",
       storedPayload(20, noNulls, 2, {firstBlock, ""}),
       "malformed container length stream"},
      {"a length stream past its lengths",
       storedPayload(
           3,
           noNulls,
           2,
           {encoded<int32_t>({1, 2, 3}) + encoded<int32_t>({4}),
            encoded<int64_t>({1, 2, 3, 4, 5, 6})}),
       "malformed container length stream"}};
  for (const auto& [name, payload, expected] : cases) {
    SCOPED_TRACE(name);
    auto assembler = makeCellAssembler(type, nullptr, pool());
    EXPECT_FALSE(begin(*assembler, payload, error));
    EXPECT_EQ(error, expected);
  }
  {
    // Four elements promised, three there: the elements' stream fails the
    // batch once they are read.
    auto assembler = makeCellAssembler(type, nullptr, pool());
    ASSERT_TRUE(begin(
        *assembler,
        storedPayload(
            2,
            noNulls,
            2,
            {encoded<int32_t>({2, 2}),
             encoded<int64_t>({1'000'000'007, -2'000'000'011, 3'000'000'019})}),
        error))
        << error;
    EXPECT_THROW(assembler->nextBatch(10, 1 << 20, noMore), BoltException);
  }
}

TEST_F(CellNestedTest, assemblerBoundsDecodedBytesTogether) {
  // maxDecodedBytes bounds every space's decoder together. Four arrays of
  // 1000 null elements: their payload is a few bytes, but the elements'
  // decoder may hold a bitmap of 4000 positions, and its batch 4000
  // values and nulls. As the payload begins, the bitmaps every column of
  // every space may take must fit the bound; as a batch grows, every
  // space's values, nulls and offsets with room to double, besides those
  // bitmaps. A batch the bound has no room in for every row takes fewer;
  // only a row that passes the bound alone fails the read.
  const auto type = ROW({"a"}, {ARRAY(BIGINT())});
  // The lengths kNoNull, the elements kAllNull.
  const auto payload = storedPayload(
      4,
      std::string(1, '\x01'),
      2,
      {encoded<int32_t>({1000, 1000, 1000, 1000}), ""});
  const auto noMore = [] { return false; };
  // Bitmaps of 4 and 4000 positions, at most 1 + 8 and 500 + 8 bytes.
  constexpr uint64_t kBitmaps = 9 + 508;
  // A row of 4 + 4 bytes and a null bit and its elements of 8 bytes and a
  // null bit each, doubled; the four rows.
  constexpr uint64_t kRow = 2 * ((8 + 1) + (1000 * 8 + 125));
  constexpr uint64_t kBatch = 2 * ((4 * 8 + 1) + (4000 * 8 + 500));
  // The bound, and the batches the payload comes out in: none when the
  // read fails.
  const std::vector<std::pair<uint64_t, std::vector<vector_size_t>>> cases{
      {kBitmaps - 1, {}},
      {kBitmaps + kRow - 1, {}},
      {kBitmaps + kRow, {1, 1, 1, 1}},
      {kBitmaps + kBatch - 1, {3, 1}},
      {kBitmaps + kBatch, {4}}};
  for (const auto& [bound, expected] : cases) {
    SCOPED_TRACE(bound);
    CellDecodeLimits limits;
    limits.maxDecodedBytes = bound;
    auto assembler = makeCellAssembler(type, nullptr, pool(), limits);
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    std::string error;
    if (bound < kBitmaps) {
      EXPECT_FALSE(assembler->beginPayload(source, error));
      EXPECT_EQ(error, "null bitmaps beyond sanity bound");
      continue;
    }
    ASSERT_TRUE(assembler->beginPayload(source, error)) << error;
    if (expected.empty()) {
      EXPECT_THROW(assembler->nextBatch(4096, 1 << 30, noMore), BoltException);
      continue;
    }
    std::vector<vector_size_t> sizes;
    while (const auto batch = assembler->nextBatch(4096, 1 << 30, noMore)) {
      batch->validate({});
      const auto* arrays = batch->childAt(0)->as<ArrayVector>();
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        EXPECT_EQ(arrays->sizeAt(row), 1000);
      }
      EXPECT_EQ(arrays->elements()->size(), 1000 * batch->size());
      EXPECT_TRUE(arrays->elements()->isNullAt(arrays->elements()->size() - 1));
      sizes.push_back(batch->size());
    }
    EXPECT_EQ(sizes, expected);
  }

  for (const auto& [bound, batchSize] :
       std::vector<std::pair<uint64_t, vector_size_t>>{
           {kBitmaps + kBatch, 4},
           {kBitmaps + kBatch, 4096},
           {100'000, 4096}}) {
    // A payload joins a batch only whole: the payload twice, at the bound
    // that reads it in one batch and at one with room beside it for half
    // its rows, comes out in a batch each.
    SCOPED_TRACE(fmt::format("bound {}, batches of {}", bound, batchSize));
    CellDecodeLimits limits;
    limits.maxDecodedBytes = bound;
    auto assembler = makeCellAssembler(type, nullptr, pool(), limits);
    const std::string twice = payload + payload;
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(twice.data()), twice.size());
    const auto nextPayload = [&] {
      if (source.atEnd()) {
        return false;
      }
      std::string error;
      BOLT_CHECK(assembler->beginPayload(source, error), "{}", error);
      return true;
    };
    for (int i = 0; i < 2; ++i) {
      SCOPED_TRACE(i);
      const auto batch = assembler->nextBatch(batchSize, 1 << 30, nextPayload);
      ASSERT_NE(batch, nullptr);
      batch->validate({});
      EXPECT_EQ(batch->size(), 4);
    }
    EXPECT_EQ(assembler->nextBatch(batchSize, 1 << 30, nextPayload), nullptr);
  }
}

/// Bytes the buffers of `vector` and of the vectors below it hold, string
/// buffers included.
uint64_t heldBytes(const BaseVector& vector) {
  uint64_t bytes = vector.nulls() != nullptr ? vector.nulls()->size() : 0;
  if (const auto* row = vector.as<RowVector>()) {
    for (const auto& child : row->children()) {
      bytes += heldBytes(*child);
    }
    return bytes;
  }
  if (const auto* containers = vector.as<ArrayVectorBase>()) {
    bytes += containers->offsets()->size() + containers->sizes()->size();
    if (const auto* array = vector.as<ArrayVector>()) {
      return bytes + heldBytes(*array->elements());
    }
    const auto* map = vector.as<MapVector>();
    return bytes + heldBytes(*map->mapKeys()) + heldBytes(*map->mapValues());
  }
  if (vector.isFlatEncoding() && vector.values() != nullptr) {
    bytes += vector.values()->size();
    if (const auto* strings = vector.asFlatVector<StringView>()) {
      for (const auto& buffer : strings->stringBuffers()) {
        bytes += buffer->size();
      }
    }
  }
  return bytes;
}

TEST_F(CellNestedTest, assemblerBoundsTheBatchItHolds) {
  // At the least maxDecodedBytes that reads them, a batch's buffers fit the
  // bound: here the elements of two payloads, 1000 and then 1 null TINYINT,
  // whose batch grows from 1000 positions to 2000 - values and nulls.
  const auto type = ROW({"a"}, {ARRAY(TINYINT())});
  const auto payloadOf = [](int32_t length) {
    return std::string(1, static_cast<char>(CellBlockKind::kPayload)) +
        storedPayload(
               1, std::string(1, '\x01'), 2, {encoded<int32_t>({length}), ""});
  };
  const std::string bytes = payloadOf(1000) + payloadOf(1);
  // The batch read at `bound`, or nullptr when the bound rejects it.
  const auto read = [&](uint64_t bound) -> RowVectorPtr {
    CellDecodeLimits limits;
    limits.maxDecodedBytes = bound;
    auto assembler = makeCellAssembler(type, nullptr, pool(), limits);
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    const auto nextPayload = [&] {
      uint8_t kind;
      std::string error;
      return !source.atEnd() && source.read(&kind, 1) &&
          assembler->beginPayload(source, error);
    };
    try {
      return assembler->nextBatch(2, 1 << 30, nextPayload);
    } catch (const BoltException&) {
      return nullptr;
    }
  };
  uint64_t low = 1;
  uint64_t high = 1 << 20;
  ASSERT_NE(read(high), nullptr);
  while (low < high) {
    const uint64_t mid = (low + high) / 2;
    const auto batch = read(mid);
    if (batch != nullptr && batch->size() == 2) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  const auto batch = read(low);
  ASSERT_NE(batch, nullptr);
  batch->validate({});
  ASSERT_EQ(batch->size(), 2);
  EXPECT_EQ(batch->childAt(0)->as<ArrayVector>()->elements()->size(), 1001);
  EXPECT_LE(heldBytes(*batch), low);
}

TEST_F(CellNestedTest, assemblerClosesBatchesAtTheBound) {
  // Rows of 1000 strings of 100 bytes in an array, 100 KB of strings and
  // 16 KB of views each, as the writer writes them: three payloads of a
  // row, and one payload of the three rows. Whatever the batch size, a
  // batch closes before a payload or rows that would take it past
  // maxDecodedBytes, their strings counted: at 150 or 200 KiB a row per
  // batch, at 300 KiB two rows and then one, at 1 MiB all three, as long as
  // no byte budget cuts the batch first; a budget that cuts it after a row
  // leaves the strings read out of what the rest of the payload takes. The
  // one payload's runs alone pass 200 KiB, which fails it as it begins, as
  // it would a flat payload. Six rows of 500 strings in one payload come
  // out four and two at 310000 bytes: the strings of the first four count
  // against the room left. A small batch's buffers stay within the bound.
  std::vector<std::string> letters;
  for (char letter = 'a'; letter <= 'z'; ++letter) {
    letters.emplace_back(100, letter);
  }
  const auto rowsOf =
      [&](int first, vector_size_t rows, vector_size_t strings = 1000) {
        return makeRowVector(
            {"a"},
            {makeArrayVector<StringView>(
                rows,
                [&](auto) { return strings; },
                [&](auto idx) {
                  return StringView(letters[(first + idx) % 26]);
                })});
      };
  const auto write = [&](const ShuffleInput& input, int64_t partitionBytes) {
    removeFiles();
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.partitionWriterOptions.numPartitions = 1;
    options.partitionWriterOptions.dataFile = dataFile_;
    options.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
    options.cellOptions.checkpointPartitionBytes = partitionBytes;
    options.cellOptions.enableStringDictionary = false;
    return test::writeShuffle(options, input, pool()).partitions[0];
  };
  ShuffleInput oneRowEach;
  for (int batch = 0; batch < 3; ++batch) {
    oneRowEach.data.push_back(rowsOf(batch, 1));
    oneRowEach.pids.push_back({0});
  }
  ShuffleInput threeRows;
  threeRows.data = {rowsOf(0, 3)};
  threeRows.pids = {{0, 0, 0}};
  ShuffleInput sixRows;
  sixRows.data = {rowsOf(0, 6, 500)};
  sixRows.pids = {std::vector<int32_t>(6, 0)};
  const auto type = asRowType(threeRows.data[0]->type());
  // Per bound and byte budget, the batches the rows come out in; none: the
  // read fails.
  struct Read {
    uint64_t bound;
    int64_t budget;
    std::vector<vector_size_t> batches;
  };
  constexpr int64_t kNoBudget = 1 << 30;
  constexpr int64_t kRowBudget = 120'000;
  const std::vector<
      std::tuple<const char*, ShuffleInput, std::string, std::vector<Read>>>
      inputs{
          {"a payload per row",
           oneRowEach,
           write(oneRowEach, 1),
           {{150 << 10, kNoBudget, {1, 1, 1}},
            {200 << 10, kNoBudget, {1, 1, 1}},
            {300 << 10, kNoBudget, {2, 1}},
            {300 << 10, kRowBudget, {2, 1}},
            {1 << 20, kNoBudget, {3}}}},
          {"one payload",
           threeRows,
           write(threeRows, 1 << 30),
           {{200 << 10, kNoBudget, {}},
            {300 << 10, kNoBudget, {2, 1}},
            {300 << 10, kRowBudget, {2, 1}},
            {1 << 20, kNoBudget, {3}}}},
          {"one payload of six rows",
           sixRows,
           write(sixRows, 1 << 30),
           {{310'000, kNoBudget, {4, 2}}, {1 << 20, kNoBudget, {6}}}}};
  ASSERT_EQ(
      payloadRows(std::get<2>(inputs[0]), type),
      (std::vector<uint32_t>{1, 1, 1}));
  ASSERT_EQ(
      payloadRows(std::get<2>(inputs[1]), type), (std::vector<uint32_t>{3}));
  ASSERT_EQ(
      payloadRows(std::get<2>(inputs[2]), type), (std::vector<uint32_t>{6}));
  for (const auto& [name, input, bytes, reads] : inputs) {
    for (const auto& read : reads) {
      for (const vector_size_t batchSize : {8, 4096}) {
        SCOPED_TRACE(fmt::format(
            "{}, bound {}, budget {}, batches of {} rows",
            name,
            read.bound,
            read.budget,
            batchSize));
        CellDecodeLimits limits;
        limits.maxDecodedBytes = read.bound;
        const auto assemble = [&] {
          return assemblePartition(
              bytes,
              type,
              CellAssembler::kAppendPositions,
              batchSize,
              read.budget,
              limits);
        };
        if (read.batches.empty()) {
          EXPECT_THROW(assemble(), BoltException);
          continue;
        }
        const auto batches = assemble();
        test::expectPartitionRows(input.rowsOf(1)[0], batches, 0, batchSize);
        std::vector<vector_size_t> sizes;
        for (const auto& batch : batches) {
          sizes.push_back(batch->size());
          if (batchSize == 8) {
            EXPECT_LE(heldBytes(*batch), read.bound);
          }
        }
        EXPECT_EQ(sizes, read.batches);
      }
    }
  }
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
