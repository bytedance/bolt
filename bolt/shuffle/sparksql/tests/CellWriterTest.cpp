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

#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <random>
#include <stdexcept>
#include <type_traits>

#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/RawPartitionOutput.h"
#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"
#include "bolt/shuffle/sparksql/tests/MemoryReaderStreamIterator.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

/// Decompression seam over the engine codec, mirroring the reader node's
/// wiring.
class TestDecompressor : public cell::CellDecompressor {
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

/// Observes synchronous output calls and can simulate reclaim or failure.
class RecordingCellOutput final : public CellOutput {
 public:
  bool sealsResidentCells() const override {
    return keepsCells;
  }

  void spillRun(const CellWindowInput& in) override {
    ++spillCalls;
    if (onSpill) {
      onSpill(in);
    }
  }

  void sealWindow(const CellWindowInput& in) override {
    ++sealCalls;
    if (onSeal) {
      onSeal(in);
    }
  }

  void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics&) override {
    ++finalizeCalls;
    if (onFinalize) {
      onFinalize(in, windowHasData);
    }
  }

  int64_t bytesEvicted() const override {
    return 0;
  }

  /// Seals then find the window's cells resident, as the RSS output does.
  bool keepsCells{false};
  uint32_t spillCalls{0};
  uint32_t sealCalls{0};
  uint32_t finalizeCalls{0};
  std::function<void(const CellWindowInput&)> onSpill;
  std::function<void(const CellWindowInput&)> onSeal;
  std::function<void(const CellWindowInput&, bool)> onFinalize;
};

/// Keeps every push as its own blob, so a test can check that each one is
/// exactly one whole payload.
class RecordingRssClient final : public RssClient {
 public:
  int32_t pushPartitionData(int32_t partitionId, char* bytes, int64_t size)
      override {
    BOLT_CHECK_EQ(stops, 0, "push after stop");
    pushes[partitionId].emplace_back(bytes, size);
    ++numPushes;
    return static_cast<int32_t>(size);
  }

  void stop() override {
    ++stops;
  }

  std::map<int32_t, std::vector<std::string>> pushes;
  uint64_t numPushes{0};
  int32_t stops{0};
};

class CellWriterTest : public testing::Test, public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_cell_writer_test_XXXXXX";
    const int fd = ::mkstemp(pathTemplate);
    ASSERT_GE(fd, 0);
    ::close(fd);
    dataFile_ = pathTemplate;
  }

  void TearDown() override {
    ::unlink(dataFile_.c_str());
    ::unlink((dataFile_ + ".cell.spill").c_str());
    ::unlink((dataFile_ + ".raw.spill").c_str());
  }

  /// With no configured directory, the outputs' spill files sit next to the
  /// data file.
  bool spillFilesExist() const {
    return std::filesystem::exists(dataFile_ + ".cell.spill") ||
        std::filesystem::exists(dataFile_ + ".raw.spill");
  }

  ShuffleWriterOptions makeOptions(int32_t numPartitions) {
    ShuffleWriterOptions options;
    options.partitioning = Partitioning::kHash;
    options.partitionWriterOptions.numPartitions = numPartitions;
    options.partitionWriterOptions.dataFile = dataFile_;
    return options;
  }

  /// Options for a Celeborn shuffle recorded by `client`.
  ShuffleWriterOptions makeCelebornOptions(
      int32_t numPartitions,
      const std::shared_ptr<RecordingRssClient>& client) {
    auto options = makeOptions(numPartitions);
    options.partitionWriterOptions.partitionWriterType =
        PartitionWriterType::kCeleborn;
    options.partitionWriterOptions.rssClient = client;
    return options;
  }

  /// Prepends an explicit pid column (values already in [0, P)).
  RowVectorPtr withPid(
      const std::vector<int32_t>& pids,
      const RowVectorPtr& data) {
    std::vector<std::string> names{"pid"};
    std::vector<TypePtr> types{INTEGER()};
    std::vector<VectorPtr> children{makeFlatVector<int32_t>(pids)};
    const auto& row = data->type()->asRow();
    for (uint32_t i = 0; i < row.size(); ++i) {
      names.push_back(row.nameOf(i));
      types.push_back(row.childAt(i));
      children.push_back(data->childAt(i));
    }
    return std::make_shared<RowVector>(
        pool(),
        ROW(std::move(names), std::move(types)),
        BufferPtr(nullptr),
        data->size(),
        std::move(children));
  }

  /// `input` as consecutive batches of at most `rows` rows, beside their
  /// slices of `pids`; a lazy child stays lazy in every batch. With
  /// checkpointPartitionBytes = 1, every batch boundary then closes a
  /// window: one payload per batch and partition.
  std::pair<std::vector<std::vector<int32_t>>, std::vector<RowVectorPtr>>
  sliceBatches(
      const RowVectorPtr& input,
      const std::vector<int32_t>& pids,
      vector_size_t rows) {
    std::vector<std::vector<int32_t>> pidSlices;
    std::vector<RowVectorPtr> batches;
    for (vector_size_t begin = 0; begin < input->size(); begin += rows) {
      const auto length = std::min(rows, input->size() - begin);
      std::vector<VectorPtr> children;
      for (auto& child : input->children()) {
        children.push_back(
            child->isLazy() ? wrapInLazyDictionary(
                                  child->loadedVector()->slice(begin, length))
                            : child->slice(begin, length));
      }
      batches.push_back(std::make_shared<RowVector>(
          pool(), input->type(), nullptr, length, std::move(children)));
      pidSlices.emplace_back(
          pids.begin() + begin, pids.begin() + begin + length);
    }
    return {std::move(pidSlices), std::move(batches)};
  }

  VectorPtr makeNullableStrings(int n, std::mt19937& rng) {
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      if (rng() % 5 == 0) {
        views[i] = std::nullopt;
        continue;
      }
      storage[i].assign(rng() % 90, static_cast<char>('a' + rng() % 26));
      views[i] = StringView(storage[i]);
    }
    return makeNullableFlatVector<StringView>(views);
  }

  std::string readFile() {
    std::ifstream in(dataFile_, std::ios::binary);
    return std::string(
        std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }

  /// Builds the borrowed decoded batch for direct splitter lifecycle tests.
  void splitOnePartition(CellSplitter& splitter, const RowVectorPtr& rows) {
    const uint32_t numRows = rows->size();
    std::vector<DecodedVector> decoded(rows->childrenSize());
    std::vector<BatchNullClass> nullClass(decoded.size());
    for (uint32_t col = 0; col < decoded.size(); ++col) {
      decoded[col].decode(*rows->childAt(col));
      uint32_t nullCount = 0;
      for (uint32_t row = 0; row < numRows; ++row) {
        nullCount += decoded[col].isNullAt(row);
      }
      nullClass[col] = nullCount == 0 ? BatchNullClass::kNoNulls
          : nullCount == numRows      ? BatchNullClass::kAllNull
                                      : BatchNullClass::kSomeNulls;
    }
    std::vector<uint32_t> pids(numRows, 0);
    std::vector<uint32_t> rowIndex(numRows);
    for (uint32_t row = 0; row < numRows; ++row) {
      rowIndex[row] = row;
    }
    SplitBatch batch{
        decoded.data(),
        pids.data(),
        &numRows,
        nullClass.data(),
        numRows,
        rowIndex.data()};
    splitter.split(batch);
  }

  /// Splits `block`, whose children are the layout columns `columns` of a
  /// layout of `numColumns`, into the partitions `pids` at `positionBase`
  /// (nullptr: the window's rows). Returns its rows per partition.
  std::vector<uint32_t> splitColumns(
      CellSplitter& splitter,
      uint32_t numColumns,
      uint32_t numPartitions,
      const std::vector<uint32_t>& columns,
      const RowVectorPtr& block,
      const std::vector<uint32_t>& pids,
      const uint32_t* positionBase) {
    const uint32_t numRows = block->size();
    std::vector<DecodedVector> decoded(numColumns);
    std::vector<BatchNullClass> nullClass(numColumns);
    for (uint32_t i = 0; i < columns.size(); ++i) {
      if (block->childAt(i)->typeKind() == TypeKind::UNKNOWN) {
        continue;
      }
      auto& column = decoded[columns[i]];
      column.decode(*block->childAt(i));
      uint32_t nullCount = 0;
      for (uint32_t row = 0; row < numRows; ++row) {
        nullCount += column.isNullAt(row);
      }
      if (nullCount == 0) {
        nullClass[columns[i]] = BatchNullClass::kNoNulls;
      } else if (nullCount == numRows) {
        nullClass[columns[i]] = BatchNullClass::kAllNull;
      } else {
        nullClass[columns[i]] = BatchNullClass::kSomeNulls;
      }
    }
    std::vector<uint32_t> added(numPartitions, 0);
    std::vector<uint32_t> rowIndex(numRows);
    for (uint32_t row = 0; row < numRows; ++row) {
      rowIndex[row] = added[pids[row]]++;
    }
    SplitBatch batch{
        decoded.data(),
        pids.data(),
        added.data(),
        nullClass.data(),
        numRows,
        rowIndex.data()};
    batch.columns = columns.data();
    batch.numColumns = columns.size();
    batch.positionBase = positionBase;
    splitter.split(batch);
    return added;
  }

  /// Column `col` of `rowType` in round `round` of a column-group test:
  /// `pids.size()` values of the round, the position and the column, none
  /// of them null when `fill` is 'V', some when 'S' and all when 'N'. The
  /// last column is a VARCHAR whose first value in partition 0 in round 0
  /// takes 40 KiB, enough for that partition alone to pass a partition seal.
  VectorPtr groupColumn(
      const RowTypePtr& rowType,
      uint32_t round,
      uint32_t col,
      char fill,
      const std::vector<uint32_t>& pids) {
    static const char* const kWords[] = {"ab", "cd", "ef", "gh"};
    const auto n = static_cast<vector_size_t>(pids.size());
    const auto null = [&](vector_size_t k) {
      return fill == 'N' || (fill == 'S' && (k + col) % 3 == 1);
    };
    const auto value = [&](vector_size_t k) {
      return int64_t{round} * 7919 + k * 31 + col;
    };
    switch (rowType->childAt(col)->kind()) {
      case TypeKind::BIGINT:
        return makeFlatVector<int64_t>(
            n, [&](auto k) { return value(k) * 1000003; }, null);
      case TypeKind::UNKNOWN:
        return BaseVector::createNullConstant(UNKNOWN(), n, pool());
      case TypeKind::INTEGER:
        return makeFlatVector<int32_t>(
            n,
            [&](auto k) { return static_cast<int32_t>(value(k)) - 999; },
            null);
      case TypeKind::BOOLEAN:
        return makeFlatVector<bool>(
            n, [&](auto k) { return value(k) % 3 == 0; }, null);
      case TypeKind::DOUBLE:
        return makeFlatVector<double>(
            n, [&](auto k) { return value(k) * 0.25; }, null);
      case TypeKind::TIMESTAMP:
        return makeFlatVector<Timestamp>(
            n,
            [&](auto k) {
              return Timestamp(value(k), value(k) * 7919 % 1'000'000'000);
            },
            null);
      case TypeKind::VARCHAR:
        break;
      default:
        BOLT_UNREACHABLE();
    }
    if (col + 1 < rowType->size()) {
      return makeFlatVector<StringView>(
          n, [&](auto k) { return StringView(kWords[value(k) % 4]); }, null);
    }
    std::string text;
    bool placed = round != 0;
    return makeFlatVector<StringView>(
        n,
        [&](auto k) {
          if (!placed && pids[k] == 0 && !null(k)) {
            text.assign(40 << 10, 'z');
            placed = true;
          } else {
            text.assign(value(k) % 23, static_cast<char>('a' + k % 26));
          }
          return StringView(text);
        },
        null);
  }

  /// The partitions the splitter's seals emptied since it counted `seals`,
  /// which it brings up to date: their positions start over.
  static std::vector<uint32_t> emptiedBySeals(
      const CellSplitter& splitter,
      uint32_t numPartitions,
      uint64_t& seals) {
    std::vector<uint32_t> emptied;
    if (splitter.sealCount() == seals) {
      return emptied;
    }
    seals = splitter.sealCount();
    for (uint32_t pid = 0; pid < numPartitions; ++pid) {
      if (splitter.windowRowCounts()[pid] == 0) {
        emptied.push_back(pid);
      }
    }
    return emptied;
  }

  /// The layout of the columns `columns` of `rowType`, in that order.
  static CellLayout subsetLayout(
      const RowTypePtr& rowType,
      const std::vector<uint32_t>& columns) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (const auto col : columns) {
      names.push_back(rowType->nameOf(col));
      types.push_back(rowType->childAt(col));
    }
    return CellLayout::create(ROW(std::move(names), std::move(types)));
  }

  /// Reads partition `pid` of the window `in` back in column groups: group
  /// g holds the layout columns groups[g], ascending, at positions[g]
  /// positions. Builds the partition's null body as the outputs do, gives
  /// each wire column's tag and expands each kRawNull column's records over
  /// its group's positions; then decodes each group as a payload of its own
  /// - the group's tags, records and resident streams - into `vectors`.
  void readColumnGroups(
      const CellWindowInput& in,
      uint32_t pid,
      const std::vector<std::vector<uint32_t>>& groups,
      const std::vector<uint32_t>& positions,
      std::vector<NullTag>& tags,
      std::vector<RowVectorPtr>& vectors) {
    const auto& layout = *in.layout;
    const uint32_t numWire = layout.numWireColumns();
    std::vector<uint32_t> groupOf(
        layout.numColumns(), static_cast<uint32_t>(groups.size()));
    for (uint32_t g = 0; g < groups.size(); ++g) {
      for (const auto col : groups[g]) {
        groupOf[col] = g;
      }
    }
    BufferPtr body;
    buildNullBody(in, pid, in.rowCounts[pid], body, pool());
    ASSERT_NE(body, nullptr);
    const auto* data = body->as<uint8_t>();
    const uint8_t* cursor = data + nullTagBytes(numWire);
    const uint8_t* const end = data + body->size();
    tags.resize(numWire);
    std::vector<std::string> records(numWire);
    for (uint32_t wire = 0; wire < numWire; ++wire) {
      tags[wire] = getNullTag(data, wire);
      if (tags[wire] != NullTag::kRawNull) {
        continue;
      }
      const uint32_t count = positions[groupOf[layout.logicalColumn(wire)]];
      std::vector<uint8_t> bitmap(count / 8 + 8);
      uint32_t nonNull;
      std::string error;
      const auto* start = cursor;
      ASSERT_TRUE(
          expandNullRecords(cursor, end, count, bitmap.data(), nonNull, error))
          << "wire column " << wire << ": " << error;
      records[wire].assign(
          reinterpret_cast<const char*>(start), cursor - start);
    }
    ASSERT_EQ(cursor, end);

    vectors.clear();
    for (uint32_t g = 0; g < groups.size(); ++g) {
      const auto groupLayout = subsetLayout(layout.rowType(), groups[g]);
      const uint32_t groupWire = groupLayout.numWireColumns();
      std::string nullBody(nullTagBytes(groupWire), '\0');
      std::string encodingTags((groupWire + 7) / 8, '\0');
      std::string groupRecords;
      for (uint32_t i = 0; i < groups[g].size(); ++i) {
        const uint32_t col = groups[g][i];
        if (layout.isUnknownColumn(col)) {
          continue;
        }
        const uint32_t wire = layout.wireColumn(col);
        const uint32_t to = groupLayout.wireColumn(i);
        setNullTag(reinterpret_cast<uint8_t*>(nullBody.data()), to, tags[wire]);
        groupRecords += records[wire];
        if ((in.encodingTags[wire / 8] >> (wire % 8)) & 1) {
          encodingTags[to / 8] |= static_cast<char>(1 << (to % 8));
        }
      }
      nullBody += groupRecords;
      std::string streams;
      std::vector<uint64_t> streamSizes;
      for (uint32_t s = 0; s < layout.numStreams(); ++s) {
        if (groupOf[layout.stream(s).column] != g) {
          continue;
        }
        const size_t start = streams.size();
        in.cells->scan(pid, s, [&](const char* bytes, uint32_t size) {
          streams.append(bytes, size);
        });
        streamSizes.push_back(streams.size() - start);
      }
      ASSERT_EQ(streamSizes.size(), groupLayout.numStreams());
      std::string payload;
      const auto put = [&](auto value) {
        payload.append(reinterpret_cast<const char*>(&value), sizeof(value));
      };
      put(positions[g]);
      put(uint32_t{1}); // runs
      put(uint64_t{0}); // variable bytes
      put(static_cast<uint32_t>(nullBody.size()));
      put(uint32_t{0}); // stored as it is
      payload += nullBody;
      payload += encodingTags;
      put(static_cast<uint8_t>(RunLayout::kCombinedUncompressed));
      put(static_cast<uint64_t>(streams.size()));
      for (const auto size : streamSizes) {
        put(size);
      }
      payload += streams;
      CellPayloadDecoder decoder(groupLayout, nullptr, pool());
      MemoryByteSource source(
          reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
      RowVectorPtr vector;
      std::string error;
      ASSERT_TRUE(decoder.decode(source, vector, error))
          << "group " << g << ": " << error;
      ASSERT_TRUE(source.atEnd());
      vectors.push_back(std::move(vector));
    }
  }

  RowVectorPtr decodeComplex(
      const CellShuffleTypeAdapter& adapter,
      const RowVectorPtr& input) {
    vector_size_t offset = 0;
    auto result = adapter.decodeNext(input, offset, pool());
    EXPECT_EQ(offset, input->size());
    return result;
  }

  /// Decodes every payload of one partition's byte range, in payload order;
  /// each opens with its kind byte.
  std::vector<RowVectorPtr> decodePartition(
      const CellLayout& layout,
      const std::string& file,
      uint64_t offset,
      uint64_t length) {
    std::vector<RowVectorPtr> payloads;
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(file.data()) + offset, length);
    // Compressed runs use LZ4; stored runs bypass the decompressor.
    auto codec = createCodec(
        arrow::Compression::LZ4_FRAME,
        CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
    TestDecompressor decompressor(codec.get());
    CellPayloadDecoder decoder(layout, &decompressor, pool());
    while (!source.atEnd()) {
      uint8_t kind;
      EXPECT_TRUE(source.read(&kind, 1));
      EXPECT_EQ(kind, static_cast<uint8_t>(CellBlockKind::kPayload));
      RowVectorPtr decoded;
      std::string error;
      EXPECT_TRUE(decoder.decode(source, decoded, error)) << error;
      if (decoded == nullptr) {
        break;
      }
      payloads.push_back(std::move(decoded));
    }
    return payloads;
  }

  /// `n` rows of 1KB single-letter strings beside a small bigint - the
  /// strings compress far below their size. Row 0 goes to the last
  /// partition with an 8-byte string, the rest spreads over the others.
  RowVectorPtr makeCompressibleBatch(
      int n,
      int32_t numPartitions,
      std::mt19937& rng,
      std::vector<int32_t>& pids) {
    pids.resize(n);
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      pids[i] = i == 0 ? numPartitions - 1 : rng() % (numPartitions - 1);
      storage[i].assign(i == 0 ? 8 : 1024, static_cast<char>('a' + i % 4));
      views[i] = StringView(storage[i]);
    }
    return makeRowVector(
        {"v", "s"},
        {makeFlatVector<int64_t>(n, [](auto row) { return row % 977; }),
         makeNullableFlatVector<StringView>(views)});
  }

  /// `data` as HashAggregation composite output: pid column first, each
  /// row [size i32][CompactRow bytes] like a serialized aggregation state.
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
    const auto ordinary = withPid(pids, data);
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

  /// The rows of a CompositeRowVector read back from the shuffle, decoded.
  RowVectorPtr decodeCompositeRows(
      const RowTypePtr& rowType,
      const RowVectorPtr& batch) {
    auto composite = std::dynamic_pointer_cast<CompositeRowVector>(batch);
    EXPECT_NE(composite, nullptr);
    std::vector<std::string_view> rows;
    for (vector_size_t row = 0; row < composite->size(); ++row) {
      auto* data = composite->rowAt(row);
      auto* end = row + 1 < composite->size()
          ? composite->rowAt(row + 1)
          : composite->rowAt(0) + composite->totalRowSize();
      rows.emplace_back(data, end - data);
    }
    ShuffleRowToColumnarConverter converter(
        rowType, pool(), row::RowFormat::COMPACT);
    return converter.convert(rows);
  }

  /// The run count of every payload in one partition's byte range.
  std::vector<uint32_t> payloadRunCounts(
      const CellLayout& layout,
      const std::string& file,
      uint64_t offset,
      uint64_t length) {
    std::vector<uint32_t> runCounts;
    const uint64_t end = offset + length;
    const uint64_t tagBytes = (layout.numWireColumns() + 7) / 8;
    const uint64_t numStreams = layout.numStreams();
    while (offset < end) {
      ++offset; // the block kind byte
      uint32_t runCount;
      uint32_t nullStored;
      ::memcpy(&runCount, file.data() + offset + 4, sizeof(runCount));
      ::memcpy(&nullStored, file.data() + offset + 16, sizeof(nullStored));
      offset += 24 + nullStored + tagBytes;
      for (uint32_t run = 0; run < runCount; ++run) {
        // A SEPARATE run has a stored size per stream, the others one.
        const bool separate = static_cast<uint8_t>(file[offset]) ==
            static_cast<uint8_t>(RunLayout::kSeparate);
        const uint64_t storedCount = separate ? numStreams : 1;
        uint64_t stored = 0;
        for (uint64_t i = 0; i < storedCount; ++i) {
          uint64_t size;
          ::memcpy(&size, file.data() + offset + 1 + 8 * i, sizeof(size));
          stored += size;
        }
        offset += 1 + 8 * (storedCount + numStreams) + stored;
      }
      runCounts.push_back(runCount);
    }
    EXPECT_EQ(offset, end);
    return runCounts;
  }

  /// Runs the writer over the batches and verifies, per partition, that the
  /// decoded rows equal the input rows routed to it, in order.
  void roundTrip(
      const ShuffleWriterOptions& options,
      const std::vector<std::vector<int32_t>>& pidsPerBatch,
      const std::vector<RowVectorPtr>& batches,
      CellShuffleWriter* preBuilt = nullptr,
      std::function<void(CellShuffleWriter&, uint32_t)> betweenBatches = {}) {
    const auto numPartitions = options.partitionWriterOptions.numPartitions;
    std::unique_ptr<CellShuffleWriter> owned;
    CellShuffleWriter* writer = preBuilt;
    if (writer == nullptr) {
      owned = std::make_unique<CellShuffleWriter>(
          options, pool(), arrow::default_memory_pool());
      writer = owned.get();
    }
    for (uint32_t i = 0; i < batches.size(); ++i) {
      ASSERT_TRUE(writer->split(withPid(pidsPerBatch[i], batches[i]), 0).ok());
      if (betweenBatches) {
        betweenBatches(*writer, i);
      }
    }
    ASSERT_TRUE(writer->stop().ok());

    const auto& metrics = writer->metrics();
    const auto layout = CellLayout::create(asRowType(batches[0]->type()));
    ASSERT_EQ(metrics.partitionLengths.size(), numPartitions);
    int64_t sum = 0;
    for (const auto length : metrics.partitionLengths) {
      sum += length;
    }
    EXPECT_EQ(sum, metrics.totalBytesWritten);
    std::vector<std::vector<RowVectorPtr>> payloads(numPartitions);
    if (options.partitionWriterOptions.partitionWriterType ==
        PartitionWriterType::kCeleborn) {
      // Every push is exactly one whole payload, and nothing touches disk.
      const auto client = std::dynamic_pointer_cast<RecordingRssClient>(
          options.partitionWriterOptions.rssClient);
      ASSERT_NE(client, nullptr);
      EXPECT_EQ(client->stops, 1);
      EXPECT_TRUE(readFile().empty());
      EXPECT_FALSE(spillFilesExist());
      for (int32_t pid = 0; pid < numPartitions; ++pid) {
        int64_t pushed = 0;
        for (const auto& blob : client->pushes[pid]) {
          auto decoded = decodePartition(layout, blob, 0, blob.size());
          ASSERT_EQ(decoded.size(), 1) << "partition " << pid;
          payloads[pid].push_back(std::move(decoded[0]));
          pushed += static_cast<int64_t>(blob.size());
        }
        EXPECT_EQ(metrics.partitionLengths[pid], pushed) << "partition " << pid;
      }
    } else {
      const auto file = readFile();
      EXPECT_EQ(static_cast<int64_t>(file.size()), metrics.totalBytesWritten);
      uint64_t offset = 0;
      for (int32_t pid = 0; pid < numPartitions; ++pid) {
        payloads[pid] = decodePartition(
            layout, file, offset, metrics.partitionLengths[pid]);
        offset += metrics.partitionLengths[pid];
      }
    }

    // Expected rows per partition, in arrival order.
    std::vector<std::vector<std::pair<uint32_t, vector_size_t>>> expected(
        numPartitions);
    for (uint32_t i = 0; i < batches.size(); ++i) {
      for (vector_size_t row = 0; row < batches[i]->size(); ++row) {
        expected[pidsPerBatch[i][row]].emplace_back(i, row);
      }
    }
    for (int32_t pid = 0; pid < numPartitions; ++pid) {
      size_t k = 0;
      for (const auto& decoded : payloads[pid]) {
        for (vector_size_t row = 0; row < decoded->size(); ++row, ++k) {
          ASSERT_LT(k, expected[pid].size()) << "partition " << pid;
          const auto [batchIdx, sourceRow] = expected[pid][k];
          EXPECT_TRUE(
              batches[batchIdx]->equalValueAt(decoded.get(), sourceRow, row))
              << "partition " << pid << " row " << k << ": expected "
              << batches[batchIdx]->toString(sourceRow) << ", got "
              << decoded->toString(row);
        }
      }
      EXPECT_EQ(k, expected[pid].size()) << "partition " << pid;
    }
  }

  std::string dataFile_;
};

TEST_F(CellWriterTest, schemaLessFactoryWritesCellPayloads) {
  constexpr int32_t kRows = 64;
  auto options = makeOptions(2);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  const auto rows = makeRowVector(
      {makeFlatVector<int64_t>(kRows, [](auto row) { return row * 7; })});
  auto writer = BoltShuffleWriter::createShuffleWriter(
      options,
      rows->childrenSize(),
      rows->size(),
      rows->estimateFlatSize(),
      1 << 20,
      pool(),
      arrow::default_memory_pool());
  auto* cellWriter = dynamic_cast<CellShuffleWriter*>(writer.get());
  ASSERT_NE(cellWriter, nullptr);
  std::vector<int32_t> pids(kRows);
  for (int32_t row = 0; row < kRows; ++row) {
    pids[row] = row % 2;
  }
  // Decode the factory-selected writer's output with the Cell payload reader.
  roundTrip(options, {pids}, {rows}, cellWriter);
}

TEST_F(CellWriterTest, legacyCreateKeepsItsInterfaceAndRejectsCell) {
  // The default factory keeps its original signature and return type, so
  // integrations holding a shared_ptr<BoltShuffleWriter> keep compiling.
  static_assert(std::is_same_v<
                decltype(BoltShuffleWriter::create(
                    std::declval<const ShuffleWriterOptions&>(),
                    0,
                    0,
                    0,
                    0,
                    nullptr,
                    nullptr)),
                std::shared_ptr<BoltShuffleWriter>>);
  auto options = makeOptions(2);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  EXPECT_THROW(
      BoltShuffleWriter::create(
          options, 1, 1, 0, 1 << 20, pool(), arrow::default_memory_pool()),
      BoltException);
}

TEST_F(CellWriterTest, pidOnlyRoundTripsAsHeaderOnlyPayloads) {
  constexpr vector_size_t kRows = 101;
  auto options = makeOptions(3);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  auto writer = BoltShuffleWriter::createShuffleWriter(
      options, 0, kRows, 0, 1 << 20, pool(), arrow::default_memory_pool());
  auto* cellWriter = dynamic_cast<CellShuffleWriter*>(writer.get());
  ASSERT_NE(cellWriter, nullptr);
  auto data = std::make_shared<RowVector>(
      pool(), ROW({}, {}), nullptr, kRows, std::vector<VectorPtr>{});
  std::vector<int32_t> pids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    pids[row] = row % 2; // partition 2 stays empty
  }
  roundTrip(options, {pids}, {data}, cellWriter);
  // One kind byte and 24-byte header per non-empty partition, carrying the
  // row count.
  EXPECT_EQ(
      cellWriter->metrics().partitionLengths,
      (std::vector<int64_t>{25, 25, 0}));
  const auto bytes = readFile();
  EXPECT_EQ(static_cast<uint8_t>(bytes[0]), 0);
  EXPECT_EQ(static_cast<uint8_t>(bytes[1]), 51);
  EXPECT_EQ(static_cast<uint8_t>(bytes[26]), 50);
}

TEST_F(CellWriterTest, timestampKeepsAllSixteenBytes) {
  constexpr int32_t n = 1500;
  const std::vector<Timestamp> extremes = {
      Timestamp(Timestamp::kMinSeconds, 0),
      Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos),
      Timestamp(-1, 1),
      Timestamp(0, 999'999'999)};
  auto timestamps = makeFlatVector<Timestamp>(
      n,
      [&](auto row) {
        return row < extremes.size()
            ? extremes[row]
            // Nanosecond precision that a microsecond encoding would lose.
            : Timestamp(1'700'000'000 + row * 37, (row * 7919) % 1'000'000'000);
      },
      [](auto row) { return row % 11 == 5; });
  auto data = makeRowVector({timestamps});
  std::vector<int32_t> pids(n);
  for (int32_t row = 0; row < n; ++row) {
    pids[row] = row % 4;
  }
  auto options = makeOptions(4);
  options.cellOptions.checkpointPartitionBytes = 1;
  for (const auto& input :
       {data,
        makeRowVector({BaseVector::wrapInDictionary(
            nullptr,
            makeIndices(n, [](auto row) { return n - row - 1; }),
            n,
            timestamps)}),
        makeRowVector({wrapInLazyDictionary(timestamps)}),
        makeRowVector({BaseVector::wrapInConstant(n, 1, timestamps)})}) {
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    const auto [pidSlices, batches] = sliceBatches(input, pids, 256);
    roundTrip(options, pidSlices, batches, &writer);
  }

  // Spilled, merged and all-null batches: both streams stay in lockstep
  // across Run boundaries and window seals. High-entropy values keep the
  // encoded streams large enough to cross the chunk cap several times.
  constexpr vector_size_t kBig = 250'000;
  std::mt19937_64 rng(97);
  std::vector<RowVectorPtr> batches;
  std::vector<std::vector<int32_t>> bigPids;
  for (int b = 0; b < 4; ++b) {
    if (b == 1) {
      batches.push_back(makeRowVector(
          {BaseVector::createNullConstant(TIMESTAMP(), kBig, pool())}));
    } else {
      batches.push_back(makeRowVector({makeFlatVector<Timestamp>(
          kBig,
          [&](auto) {
            return Timestamp(
                static_cast<int64_t>(rng() % 4'000'000'000'000) -
                    2'000'000'000'000,
                rng() % 1'000'000'000);
          },
          [](auto row) { return row % 13 == 3; })}));
    }
    std::vector<int32_t> batchPids(kBig);
    for (auto& pid : batchPids) {
      pid = rng() % 4;
    }
    bigPids.push_back(std::move(batchPids));
  }
  auto spillOptions = makeOptions(4);
  // One chunk of cells at most: every further growth spills a Run first.
  spillOptions.cellOptions.cellMemoryCapBytes = 4 << 20;
  spillOptions.cellOptions.checkpointPartitionBytes = 1 << 20;
  CellShuffleWriter spilling(
      spillOptions, pool(), arrow::default_memory_pool());
  roundTrip(spillOptions, bigPids, batches, &spilling);
  EXPECT_GT(spilling.metrics().totalBytesEvicted, 0);
  EXPECT_GT(spilling.metrics().spillCount, 0);
  // Every window spills at least one Run before its null bodies.
  EXPECT_GE(spilling.metrics().totalSpillFiles, spilling.metrics().spillCount);
}

TEST_F(CellWriterTest, rejectsOutOfRangeTimestamp) {
  const auto layout = CellLayout::create(ROW({TIMESTAMP()}));
  const auto payload = [](int64_t seconds, int64_t nanos) {
    std::string secondsStream;
    std::string nanosStream;
    encodeStream<int64_t>(&seconds, 1, secondsStream);
    encodeStream<int64_t>(&nanos, 1, nanosStream);
    std::string bytes(24, '\0');
    bytes[0] = 1; // row_count
    bytes[4] = 1; // run_count
    bytes[16] = 1; // null_stored_size: one tags byte
    bytes.push_back(0x01); // NO_NULL
    bytes.push_back(0x00); // encoding tags
    bytes.push_back(static_cast<char>(RunLayout::kCombinedUncompressed));
    const auto appendU64 = [&](uint64_t value) {
      bytes.append(reinterpret_cast<const char*>(&value), 8);
    };
    appendU64(secondsStream.size() + nanosStream.size());
    appendU64(secondsStream.size());
    appendU64(nanosStream.size());
    return bytes + secondsStream + nanosStream;
  };
  CellPayloadDecoder decoder(layout, nullptr, pool());
  const auto decode = [&](const std::string& bytes, RowVectorPtr& output) {
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    std::string error;
    return decoder.decode(source, output, error);
  };
  RowVectorPtr output;
  ASSERT_TRUE(decode(payload(-5, 999'999'999), output));
  EXPECT_EQ(
      output->childAt(0)->as<FlatVector<Timestamp>>()->valueAt(0),
      Timestamp(-5, 999'999'999));
  EXPECT_FALSE(decode(payload(0, 1'000'000'000), output));
  EXPECT_FALSE(decode(payload(0, -1), output));
  EXPECT_FALSE(decode(payload(Timestamp::kMaxSeconds + 1, 0), output));
}

TEST_F(CellWriterTest, lazyInputIsSlicedByLoadedBytes) {
  // Lazy children estimate as zero bytes until loaded; the byte bound must
  // still split a batch whose loaded size exceeds it.
  constexpr vector_size_t kRows = 3300;
  constexpr int32_t kValueBytes = 64 << 10;
  auto chars = AlignedBuffer::allocate<char>(
      static_cast<size_t>(kRows) * kValueBytes, pool());
  ::memset(chars->asMutable<char>(), 'q', chars->size());
  auto views = AlignedBuffer::allocate<StringView>(kRows, pool());
  for (vector_size_t row = 0; row < kRows; ++row) {
    views->asMutable<StringView>()[row] = StringView(
        chars->as<char>() + static_cast<size_t>(row) * kValueBytes,
        kValueBytes);
  }
  auto strings = std::make_shared<FlatVector<StringView>>(
      pool(),
      VARBINARY(),
      nullptr,
      kRows,
      views,
      std::vector<BufferPtr>{chars});
  auto lazy = makeRowVector({wrapInLazyDictionary(strings)});
  ASSERT_GT(kRows * int64_t{kValueBytes}, kMaxShuffleWriterBatchBytes);
  auto options = makeOptions(1);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  ASSERT_TRUE(
      writer.split(withPid(std::vector<int32_t>(kRows, 0), lazy), 0).ok());
  EXPECT_GE(writer.metrics().totalInputBatches, 2);
  ASSERT_TRUE(writer.stop().ok());
}

TEST_F(CellWriterTest, variantSchemasFailWithoutChangingWriterType) {
  auto options = makeOptions(2);
  options.forceShuffleWriterType =
      static_cast<int32_t>(ShuffleWriterType::Cell);
  const std::vector<TypePtr> unsupported = {
      VARIANT(), ARRAY(VARIANT()), ROW({"nested"}, {VARIANT()})};
  for (const auto& type : unsupported) {
    SCOPED_TRACE(type->toString());
    const auto logicalType = ROW({"pid", "v"}, {INTEGER(), type});
    // The operator's logical-to-physical boundary must reject nested VARIANT
    // as well as a top-level VARIANT, before converting complex values.
    EXPECT_THROW(CellShuffleTypeAdapter{logicalType}, BoltException);

    auto writer = BoltShuffleWriter::createShuffleWriter(
        options, 1, 1, 0, 1 << 20, pool(), arrow::default_memory_pool());
    ASSERT_NE(dynamic_cast<CellShuffleWriter*>(writer.get()), nullptr);
    const auto rows = makeRowVector({BaseVector::create(type, 1, pool())});
    // The writer's own adapter rejects it instead of silently producing a
    // different payload format.
    EXPECT_THROW(writer->split(withPid({0}, rows), 0), BoltException);
  }
}

TEST_F(CellWriterTest, splitterCanSpillWhileFlushingCacheTails) {
  const auto rows = makeRowVector(
      {makeFlatVector<double>({12.5}), makeFlatVector<double>({6.25})});
  const auto layout = CellLayout::create(asRowType(rows->type()));
  for (const bool seal : {true, false}) {
    SCOPED_TRACE(seal ? "seal" : "finish");
    CellSplitter* activeSplitter = nullptr;
    bool inGrowCallback = false;
    uint32_t spillsFromGrow = 0;
    // Two cells fit a chunk. The second column's tail needs another cell
    // while append holds one unlinked, so reclaim must drain the first tail.
    ChunkAllocator allocator(pool(), 16 << 10, 8 << 10, [&]() {
      ASSERT_NE(activeSplitter, nullptr);
      EXPECT_FALSE(activeSplitter->isWritingOutput());
      inGrowCallback = true;
      activeSplitter->spillRun();
      inGrowCallback = false;
    });
    RecordingCellOutput output;
    CellSplitter splitter(layout, 1, allocator, output, *pool());
    activeSplitter = &splitter;
    std::vector<std::string> streams(layout.numStreams());
    const auto collect = [&](const CellWindowInput& in) {
      EXPECT_TRUE(splitter.isWritingOutput());
      for (uint32_t stream = 0; stream < streams.size(); ++stream) {
        in.cells->scan(0, stream, [&](const char* data, uint32_t bytes) {
          streams[stream].append(data, bytes);
        });
      }
    };
    output.onSpill = [&](const CellWindowInput& in) {
      spillsFromGrow += inGrowCallback;
      collect(in);
    };
    output.onSeal = [&](const CellWindowInput& in) {
      EXPECT_EQ(in.rowCounts[0], 1);
      EXPECT_EQ(in.cells->totalBytes(), 0);
    };
    output.onFinalize = [&](const CellWindowInput& in, bool windowHasData) {
      EXPECT_TRUE(windowHasData);
      EXPECT_EQ(in.rowCounts[0], 1);
      collect(in);
    };
    splitOnePartition(splitter, rows);
    EXPECT_EQ(output.spillCalls, 0);
    if (seal) {
      splitter.sealWindow();
    } else {
      ShuffleWriterMetrics metrics;
      splitter.finish(metrics);
    }
    EXPECT_GT(spillsFromGrow, 0);
    EXPECT_FALSE(splitter.isWritingOutput());
    EXPECT_EQ(output.sealCalls, seal ? 1 : 0);
    EXPECT_EQ(output.finalizeCalls, seal ? 0 : 1);
    for (uint32_t stream = 0; stream < streams.size(); ++stream) {
      ASSERT_EQ(streams[stream].size(), sizeof(double));
      double value;
      ::memcpy(&value, streams[stream].data(), sizeof(value));
      EXPECT_EQ(value, stream == 0 ? 12.5 : 6.25);
    }
  }
}

TEST_F(CellWriterTest, splitterOutputGuardRecoversAfterFailure) {
  const auto rows = makeRowVector(
      {makeFlatVector<double>(8, [](auto row) { return row + 0.5; })});
  const auto layout = CellLayout::create(asRowType(rows->type()));
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  RecordingCellOutput output;
  CellSplitter splitter(layout, 1, allocator, output, *pool());
  std::vector<std::string> attempts;
  output.onSpill = [&](const CellWindowInput& in) {
    EXPECT_TRUE(splitter.isWritingOutput());
    std::string bytes;
    in.cells->scan(0, 0, [&](const char* data, uint32_t size) {
      bytes.append(data, size);
    });
    attempts.push_back(std::move(bytes));
    if (output.spillCalls == 1) {
      // Simulate reclaim during output allocation, then an output failure.
      splitter.spillRun();
      EXPECT_EQ(output.spillCalls, 1);
      throw std::runtime_error("injected output failure");
    }
  };
  splitOnePartition(splitter, rows);
  EXPECT_THROW(splitter.spillRun(), std::runtime_error);
  EXPECT_FALSE(splitter.isWritingOutput());
  EXPECT_TRUE(splitter.hasWindowRows());
  EXPECT_EQ(splitter.maxWindowRows(), 8);
  splitter.spillRun();
  ASSERT_EQ(attempts.size(), 2);
  EXPECT_EQ(attempts[0].size(), 8 * sizeof(double));
  EXPECT_EQ(attempts[1], attempts[0]);
  EXPECT_FALSE(splitter.isWritingOutput());
  splitter.spillRun();
  EXPECT_EQ(output.spillCalls, 2); // successful output released the cells
}

TEST_F(CellWriterTest, splitterSealsWindowAndFinishesResidentData) {
  const auto layout = CellLayout::create(ROW({"s"}, {VARCHAR()}));
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  RecordingCellOutput output;
  CellSplitter splitter(layout, 1, allocator, output, *pool());
  splitter.enableDictionary(0);
  output.onSeal = [&](const CellWindowInput& in) {
    EXPECT_EQ(in.rowCounts[0], 3);
    EXPECT_EQ(in.variableBytes[0], 81);
    EXPECT_EQ(in.encodingTags[0], 1);
    const auto summary = in.nulls->summarize(0, 0, in.rowCounts[0]);
    EXPECT_EQ(summary.tag, NullTag::kRawNull);
    EXPECT_EQ(summary.nonNullCount, 2);
    // One record: the first row null, the rows past it non-null.
    std::vector<uint8_t> records(in.nulls->recordsBytes(0, 0));
    const uint32_t col = 0;
    in.nulls->emitRecords(0, &col, 1, records.data());
    const uint8_t* cursor = records.data();
    // A byte of bitmap, and room for the whole word the expansion writes.
    uint8_t bitmap[8] = {};
    uint32_t nonNull = 0;
    std::string error;
    ASSERT_TRUE(expandNullRecords(
        cursor,
        records.data() + records.size(),
        in.rowCounts[0],
        bitmap,
        nonNull,
        error))
        << error;
    EXPECT_EQ(bitmap[0], 0b110);
    EXPECT_EQ(nonNull, 2);
  };
  splitOnePartition(
      splitter,
      makeRowVector({makeNullableFlatVector<StringView>(
          {std::nullopt, StringView("a")})}));
  const std::string longValue(80, 'x');
  splitOnePartition(
      splitter,
      makeRowVector({makeFlatVector<StringView>({StringView(longValue)})}));
  EXPECT_EQ(splitter.maxWindowRows(), 3);
  splitter.sealWindow();
  EXPECT_EQ(output.sealCalls, 1);
  EXPECT_FALSE(splitter.hasWindowRows());
  EXPECT_EQ(splitter.maxWindowRows(), 0);
  EXPECT_EQ(splitter.maxPartitionBytes(), 0);
  EXPECT_EQ(splitter.nullBytes(), 0);

  output.onFinalize = [&](const CellWindowInput& in, bool windowHasData) {
    EXPECT_TRUE(windowHasData);
    EXPECT_EQ(in.rowCounts[0], 2);
    EXPECT_EQ(in.variableBytes[0], 2);
    EXPECT_EQ(in.encodingTags[0], 1); // dictionary choice survives the seal
    const auto summary = in.nulls->summarize(0, 0, in.rowCounts[0]);
    EXPECT_EQ(summary.tag, NullTag::kNoNull);
    EXPECT_EQ(summary.nonNullCount, 2);
    std::string dictionary;
    in.cells->scan(0, 1, [&](const char* data, uint32_t size) {
      dictionary.append(data, size);
    });
    // The previous window demoted to fallback; this one starts a fresh
    // dictionary containing only "b", followed by its terminating row count.
    ASSERT_EQ(dictionary.size(), 7);
    EXPECT_EQ(dictionary[0], 1);
    EXPECT_EQ(dictionary[1], 'b');
    EXPECT_EQ(static_cast<uint8_t>(dictionary[2]), kDictLastMarker);
    uint32_t matchedRows;
    ::memcpy(&matchedRows, dictionary.data() + 3, sizeof(matchedRows));
    EXPECT_EQ(matchedRows, 2);
  };
  splitOnePartition(
      splitter,
      makeRowVector(
          {makeFlatVector<StringView>({StringView("b"), StringView("b")})}));
  const auto spillsBeforeFinish = output.spillCalls;
  const auto sealsBeforeFinish = output.sealCalls;
  ShuffleWriterMetrics metrics;
  splitter.finish(metrics);
  EXPECT_EQ(output.finalizeCalls, 1);
  EXPECT_EQ(output.spillCalls, spillsBeforeFinish);
  EXPECT_EQ(output.sealCalls, sealsBeforeFinish);
  EXPECT_EQ(metrics.dictionaryMatchedRows, 3);
  EXPECT_EQ(metrics.dictionaryFallbackRows, 1);
}

TEST_F(CellWriterTest, splitterKeepsColumnGroupsAtTheirOwnPositions) {
  // Three column groups of one flat layout split into the same windows: the
  // rows, and two groups at positions the test counts itself - counts per
  // partition other than its rows, and none at all in one partition each.
  // Every group's null tags and records must hold over its own positions,
  // through all-null blocks of mixed columns, all-null columns turning
  // mixed, first nulls arriving late, and partition and window seals. The
  // UNKNOWN column puts the wire ids one behind the layout ids from there on.
  const auto rowType =
      ROW({"r", "u", "a0", "a1", "a2", "b0", "b1", "b2"},
          {BIGINT(),
           UNKNOWN(),
           INTEGER(),
           VARCHAR(),
           BOOLEAN(),
           DOUBLE(),
           TIMESTAMP(),
           VARCHAR()});
  const auto layout = CellLayout::create(rowType);
  constexpr uint32_t kPartitions = 4;
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  RecordingCellOutput output;
  output.keepsCells = true;
  CellSplitter splitter(layout, kPartitions, allocator, output, *pool());
  splitter.enableDictionary(3);

  struct Group {
    std::vector<uint32_t> columns;
    /// The test's position counts per partition; none for the rows.
    std::vector<uint32_t> counts;
    /// The partition that never takes a position of the group.
    uint32_t skipped;
    /// Per partition, the (block, row) at each position of its payload.
    std::vector<std::vector<std::pair<RowVectorPtr, vector_size_t>>> expected;
  };
  std::vector<Group> groups(3);
  groups[0] = {{0}, {}, kPartitions, {}};
  groups[1] = {{1, 2, 3, 4}, std::vector<uint32_t>(kPartitions, 0), 1, {}};
  groups[2] = {{5, 6, 7}, std::vector<uint32_t>(kPartitions, 0), 2, {}};
  for (auto& group : groups) {
    group.expected.resize(kPartitions);
  }
  // Wire columns r, a0, a1, a2, b0, b1, b2.
  const auto* countsA = groups[1].counts.data();
  const auto* countsB = groups[2].counts.data();
  splitter.setColumnPositions(
      {nullptr, countsA, countsA, countsA, countsB, countsB, countsB});

  // The tag of a column of `size` positions, `nulls` of them null.
  const auto tagOf = [](size_t nulls, size_t size) {
    if (nulls == 0) {
      return NullTag::kNoNull;
    }
    return nulls == size ? NullTag::kAllNull : NullTag::kRawNull;
  };
  uint32_t payloads = 0;
  const auto check = [&](const CellWindowInput& in) {
    for (uint32_t pid = 0; pid < kPartitions; ++pid) {
      if (in.rowCounts[pid] == 0 ||
          (in.sealPartitions != nullptr && in.sealPartitions[pid] == 0)) {
        continue;
      }
      SCOPED_TRACE("partition " + std::to_string(pid));
      ++payloads;
      std::vector<std::vector<uint32_t>> columns;
      std::vector<uint32_t> positions;
      for (const auto& group : groups) {
        columns.push_back(group.columns);
        positions.push_back(
            group.counts.empty() ? in.rowCounts[pid] : group.counts[pid]);
        ASSERT_EQ(positions.back(), group.expected[pid].size());
      }
      std::vector<NullTag> tags;
      std::vector<RowVectorPtr> vectors;
      ASSERT_NO_FATAL_FAILURE(
          readColumnGroups(in, pid, columns, positions, tags, vectors));
      for (uint32_t g = 0; g < groups.size(); ++g) {
        const auto& expected = groups[g].expected[pid];
        for (uint32_t i = 0; i < columns[g].size(); ++i) {
          const uint32_t col = columns[g][i];
          if (layout.isUnknownColumn(col)) {
            continue;
          }
          size_t nulls = 0;
          for (const auto& [block, row] : expected) {
            nulls += block->childAt(i)->isNullAt(row);
          }
          EXPECT_EQ(tags[layout.wireColumn(col)], tagOf(nulls, expected.size()))
              << "column " << col;
        }
        ASSERT_EQ(vectors[g]->size(), expected.size());
        for (vector_size_t row = 0; row < vectors[g]->size(); ++row) {
          const auto& [block, sourceRow] = expected[row];
          ASSERT_TRUE(block->equalValueAt(vectors[g].get(), sourceRow, row))
              << "group " << g << " position " << row << ": expected "
              << block->toString(sourceRow) << ", got "
              << vectors[g]->toString(row);
        }
      }
    }
  };
  output.onSeal = check;
  output.onFinalize = [&](const CellWindowInput& in, bool windowHasData) {
    EXPECT_TRUE(windowHasData);
    check(in);
  };

  // Per round, a letter per layout column r, u, a0, a1, a2, b0, b1, b2:
  // values only (V), some nulls (S) or all null (N).
  const char* const kFills[] = {
      "VNSSVNVS",
      "SNNSSNVV",
      "VNSVNSVS",
      "VNVSSVSN",
      "SNNNVNVS",
      "VNNSSNSV",
  };
  std::mt19937 rng(17);
  // A seal restarts the positions of the partitions it emptied, which the
  // caller learns from the seal count.
  uint64_t seals = 0;
  const auto splitRound = [&](uint32_t round, uint32_t numRows, bool withP3) {
    for (const auto pid : emptiedBySeals(splitter, kPartitions, seals)) {
      for (auto& group : groups) {
        if (!group.counts.empty()) {
          group.counts[pid] = 0;
        }
        group.expected[pid].clear();
      }
    }
    std::vector<uint32_t> rowPids(numRows);
    for (auto& pid : rowPids) {
      pid = rng() % (withP3 ? 4 : 3);
    }
    for (uint32_t g = 0; g < groups.size(); ++g) {
      auto& group = groups[g];
      // Each row takes up to 3 (A) or 2 (B) positions of the group.
      std::vector<uint32_t> pids;
      for (const auto pid : rowPids) {
        if (group.counts.empty()) {
          pids.push_back(pid);
        } else if (pid != group.skipped) {
          pids.insert(pids.end(), rng() % (g == 1 ? 4 : 3), pid);
        }
      }
      if (pids.empty()) {
        continue;
      }
      std::vector<std::string> names;
      std::vector<VectorPtr> children;
      for (const auto col : group.columns) {
        names.push_back(rowType->nameOf(col));
        children.push_back(
            groupColumn(rowType, round, col, kFills[round][col], pids));
      }
      const auto block = makeRowVector(names, children);
      const auto added = splitColumns(
          splitter,
          layout.numColumns(),
          kPartitions,
          group.columns,
          block,
          pids,
          group.counts.empty() ? nullptr : group.counts.data());
      for (uint32_t pid = 0; !group.counts.empty() && pid < kPartitions;
           ++pid) {
        group.counts[pid] += added[pid];
      }
      for (vector_size_t k = 0; k < block->size(); ++k) {
        group.expected[pids[k]].emplace_back(block, k);
      }
    }
  };

  splitRound(0, 300, false);
  splitRound(1, 150, false);
  EXPECT_EQ(splitter.sealPartitionsOver(32 << 10), 1);
  EXPECT_EQ(output.sealCalls, 1);
  EXPECT_EQ(payloads, 1);
  EXPECT_EQ(splitter.sealCount(), 1);
  EXPECT_EQ(splitter.windowRowCounts()[0], 0);
  EXPECT_GT(splitter.windowRowCounts()[1], 0);
  splitRound(2, 400, true);
  splitRound(3, 64, true);
  splitter.sealWindow();
  EXPECT_EQ(output.sealCalls, 2);
  EXPECT_EQ(payloads, 5);
  EXPECT_EQ(splitter.sealCount(), 2);
  EXPECT_FALSE(splitter.hasWindowRows());
  splitRound(4, 200, false);
  splitRound(5, 250, true);
  ShuffleWriterMetrics metrics;
  splitter.finish(metrics);
  EXPECT_EQ(output.finalizeCalls, 1);
  EXPECT_EQ(payloads, 9);
  EXPECT_EQ(splitter.sealCount(), 2);
  EXPECT_EQ(output.spillCalls, 0);
}

TEST_F(CellWriterTest, splitsAtCallerPositionsLeaveTheWindowRows) {
  // Splits at a caller's positions add no rows to the window: neither its
  // partitions' counts nor whether it holds rows at all - positions alone
  // seal nothing.
  const auto rowType = ROW({"a", "b"}, {BIGINT(), INTEGER()});
  const auto layout = CellLayout::create(rowType);
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  RecordingCellOutput output;
  CellSplitter splitter(layout, 2, allocator, output, *pool());
  const auto block = makeRowVector(
      {"b"}, {makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4, 5})});
  const std::vector<uint32_t> base{7, 0};
  const auto added =
      splitColumns(splitter, 2, 2, {1}, block, {0, 1, 1, 0, 1}, base.data());
  EXPECT_EQ(added, (std::vector<uint32_t>{2, 3}));
  EXPECT_FALSE(splitter.hasWindowRows());
  EXPECT_EQ(splitter.maxWindowRows(), 0);
  EXPECT_EQ(splitter.windowRowCounts()[0], 0);
  EXPECT_EQ(splitter.windowRowCounts()[1], 0);
  splitter.sealWindow();
  EXPECT_EQ(output.sealCalls, 0);
  EXPECT_EQ(splitter.sealCount(), 0);

  // The other column at the window's rows commits them.
  splitColumns(
      splitter,
      2,
      2,
      {0},
      makeRowVector({"a"}, {makeFlatVector<int64_t>({10, 11, 12})}),
      {1, 0, 1},
      nullptr);
  EXPECT_TRUE(splitter.hasWindowRows());
  EXPECT_EQ(splitter.maxWindowRows(), 2);
  EXPECT_EQ(splitter.windowRowCounts()[0], 1);
  EXPECT_EQ(splitter.windowRowCounts()[1], 2);
}

TEST_F(CellWriterTest, everySealCountsAndEmptiesItsPartitions) {
  // Window and partition seals alike advance the seal count and zero the
  // rows of the partitions they close; a call that closes nothing is no
  // seal.
  const auto layout = CellLayout::create(ROW({"s"}, {VARCHAR()}));
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  RecordingCellOutput output;
  CellSplitter splitter(layout, 3, allocator, output, *pool());
  const std::string longValue(4 << 10, 'x');
  const auto split = [&] {
    splitColumns(
        splitter,
        1,
        3,
        {0},
        makeRowVector(
            {"s"},
            {makeFlatVector<StringView>(
                {StringView(longValue), "a", "b", "c"})}),
        {0, 1, 2, 1},
        nullptr);
  };
  split();
  EXPECT_EQ(splitter.sealCount(), 0);
  EXPECT_EQ(splitter.sealPartitionsOver(1 << 10), 1);
  EXPECT_EQ(splitter.sealCount(), 1);
  EXPECT_EQ(splitter.windowRowCounts()[0], 0);
  EXPECT_EQ(splitter.windowRowCounts()[1], 2);
  EXPECT_EQ(splitter.windowRowCounts()[2], 1);
  EXPECT_EQ(splitter.sealPartitionsOver(1 << 10), 0);
  EXPECT_EQ(splitter.sealCount(), 1);
  splitter.sealWindow();
  EXPECT_EQ(splitter.sealCount(), 2);
  for (uint32_t pid = 0; pid < 3; ++pid) {
    EXPECT_EQ(splitter.windowRowCounts()[pid], 0);
  }
  splitter.sealWindow();
  EXPECT_EQ(splitter.sealCount(), 2);
  split();
  EXPECT_EQ(splitter.sealPartitionsOver(1 << 10), 1);
  EXPECT_EQ(splitter.sealCount(), 3);
  EXPECT_EQ(output.sealCalls, 3);
}

TEST_F(CellWriterTest, decodersOpenColumnGroupsAtTheirOwnPositions) {
  // Two column groups of one flat layout sit at positions of their own: as
  // many per row as the row's column na (group A) or nb (group B) says,
  // none when it is null. They go through the real outputs - local, with
  // and without runs spilled mid-window, and RSS - over partition and window
  // seals. One decoder reads each payload, and each group's decoder opens
  // the group's columns at the positions its rows' counts add up to, read
  // from the payload alone. Batches of every size span payloads, grow as
  // their rows' positions come in, and come out in step: a group's batch
  // holds exactly the positions of the rows' batch. Partition 1 never takes
  // a position of group A, nor partition 2 one of group B: their payloads
  // open those groups at zero positions. The same decoders read every
  // output, and only some writers encode a0 as a dictionary: a decoder
  // opens a column in either encoding after the other. Group A's batches
  // grow with an UNKNOWN column, which has no values to move.
  const auto rowType =
      ROW({"r", "na", "a0", "u", "a1", "nb", "b0", "a2", "b1", "b2"},
          {BIGINT(),
           INTEGER(),
           VARCHAR(),
           UNKNOWN(),
           BOOLEAN(),
           INTEGER(),
           TIMESTAMP(),
           INTEGER(),
           DOUBLE(),
           VARCHAR()});
  const auto layout = CellLayout::create(rowType);
  constexpr uint32_t kPartitions = 4;
  // The rows, group A and group B: their layout columns, and for A and B
  // the rows' column counting their positions, as a column of the rows'
  // decoder, and the partition taking none of them.
  const std::vector<std::vector<uint32_t>> kGroups{
      {0, 1, 5}, {2, 3, 4, 7}, {6, 8, 9}};
  const uint32_t kCountColumn[] = {0, 1, 5};
  const uint32_t kLocalCountColumn[] = {0, 1, 2};
  const uint32_t kSkipped[] = {kPartitions, 1, 2};

  // Per round, a letter per layout column: values only (V), some nulls
  // (S) or all null (N). A null na or nb takes no position.
  const char* const kFills[] = {
      "VSSNVVSNVS",
      "SVNNSSVSVV",
      "VNSNVVSVNS",
      "VSVNSSNSVN",
      "SSNNNVVVSS",
      "VVSNSNSSVV",
  };
  using Expected = std::vector<std::pair<RowVectorPtr, vector_size_t>>;
  struct Written {
    /// Per group and partition, the (block, row) at each position.
    std::vector<std::vector<Expected>> expected;
    /// Per partition, its payloads back to back.
    std::vector<std::string> bytes;
  };

  const auto write = [&](bool remote,
                         arrow::Compression::type compression,
                         bool spill,
                         bool dictionary) {
    Written written;
    written.expected.assign(3, std::vector<Expected>(kPartitions));
    auto client = std::make_shared<RecordingRssClient>();
    auto options = remote ? makeCelebornOptions(kPartitions, client)
                          : makeOptions(kPartitions);
    options.partitionWriterOptions.compressionType = compression;
    std::unique_ptr<RawPartitionOutput> raw;
    std::unique_ptr<CellOutput> output;
    if (remote) {
      raw = std::make_unique<RssRawOutput>(options.partitionWriterOptions);
      output = std::make_unique<RssCellOutput>(
          *raw,
          &layout,
          options.partitionWriterOptions,
          options.cellOptions,
          pool());
    } else {
      auto local =
          std::make_unique<LocalRawOutput>(options.partitionWriterOptions);
      output = std::make_unique<LocalCellOutput>(
          *local,
          &layout,
          options.cellOptions,
          options.partitionWriterOptions,
          pool());
      raw = std::move(local);
    }
    ChunkAllocator allocator(pool(), 16 << 10, 256);
    CellSplitter splitter(layout, kPartitions, allocator, *output, *pool());
    if (dictionary) {
      splitter.enableDictionary(2);
    }
    // The writer's position counts of groups A and B per partition.
    std::vector<std::vector<uint32_t>> counts(
        3, std::vector<uint32_t>(kPartitions, 0));
    // Wire columns r, na, a0, a1, nb, b0, a2, b1, b2.
    const auto* countsA = counts[1].data();
    const auto* countsB = counts[2].data();
    splitter.setColumnPositions(
        {nullptr,
         nullptr,
         countsA,
         countsA,
         nullptr,
         countsB,
         countsA,
         countsB,
         countsB});
    uint64_t seals = 0;
    std::mt19937 rng(23);
    const auto splitRound = [&](uint32_t round, uint32_t numRows, bool withP3) {
      for (const auto pid : emptiedBySeals(splitter, kPartitions, seals)) {
        counts[1][pid] = 0;
        counts[2][pid] = 0;
      }
      // The rows, then each group's positions: per row, its count of them
      // in its partition.
      std::vector<std::vector<uint32_t>> pids(3);
      std::vector<std::vector<std::optional<int32_t>>> lengths(3);
      for (uint32_t row = 0; row < numRows; ++row) {
        const uint32_t pid = rng() % (withP3 ? 4 : 3);
        pids[0].push_back(pid);
        for (uint32_t g = 1; g < 3; ++g) {
          const char fill = kFills[round][kCountColumn[g]];
          const int32_t length = pid == kSkipped[g] ? 0 : rng() % (g + 3);
          if (fill == 'N' || (fill == 'S' && (row + g) % 4 == 0)) {
            lengths[g].push_back(std::nullopt);
            continue;
          }
          lengths[g].push_back(length);
          pids[g].insert(pids[g].end(), length, pid);
        }
      }
      for (uint32_t g = 0; g < 3; ++g) {
        if (pids[g].empty()) {
          continue;
        }
        std::vector<std::string> names;
        std::vector<VectorPtr> children;
        for (const auto col : kGroups[g]) {
          names.push_back(rowType->nameOf(col));
          children.push_back(
              col == kCountColumn[1] || col == kCountColumn[2]
                  ? makeNullableFlatVector<int32_t>(
                        lengths[col == kCountColumn[1] ? 1 : 2])
                  : groupColumn(
                        rowType, round, col, kFills[round][col], pids[g]));
        }
        const auto block = makeRowVector(names, children);
        const auto added = splitColumns(
            splitter,
            layout.numColumns(),
            kPartitions,
            kGroups[g],
            block,
            pids[g],
            g == 0 ? nullptr : counts[g].data());
        for (uint32_t pid = 0; g != 0 && pid < kPartitions; ++pid) {
          counts[g][pid] += added[pid];
        }
        for (vector_size_t k = 0; k < block->size(); ++k) {
          written.expected[g][pids[g][k]].emplace_back(block, k);
        }
      }
    };

    splitRound(0, 300, false);
    if (spill) {
      splitter.spillRun();
    }
    splitRound(1, 150, false);
    EXPECT_EQ(splitter.sealPartitionsOver(32 << 10), 1);
    splitRound(2, 400, true);
    splitRound(3, 64, true);
    splitter.sealWindow();
    splitRound(4, 200, false);
    if (spill) {
      splitter.spillRun();
    }
    splitRound(5, 250, true);
    EXPECT_EQ(splitter.sealCount(), 2);
    ShuffleWriterMetrics metrics;
    splitter.finish(metrics);

    written.bytes.resize(kPartitions);
    if (remote) {
      for (uint32_t pid = 0; pid < kPartitions; ++pid) {
        for (const auto& blob : client->pushes[pid]) {
          written.bytes[pid] += blob;
        }
      }
    } else {
      const auto file = readFile();
      uint64_t offset = 0;
      for (uint32_t pid = 0; pid < kPartitions; ++pid) {
        written.bytes[pid] = file.substr(offset, metrics.partitionLengths[pid]);
        offset += metrics.partitionLengths[pid];
      }
      EXPECT_EQ(offset, file.size());
      ::unlink(dataFile_.c_str());
    }
    return written;
  };

  auto codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
  TestDecompressor decompressor(codec.get());
  CellPayloadDecoder reader(layout, &decompressor, pool());
  std::vector<std::unique_ptr<CellPayloadDecoder>> decoders;
  for (const auto& columns : kGroups) {
    decoders.push_back(std::make_unique<CellPayloadDecoder>(
        subsetLayout(rowType, columns), &decompressor, pool()));
  }
  auto& rows = *decoders[0];
  const auto readBack = [&](const Written& written, int32_t batchSize) {
    // Rows taken per append, as a byte budget might cut them.
    const uint32_t kPieces[] = {1, 7, 64, 1000};
    uint32_t piece = 0;
    for (uint32_t pid = 0; pid < kPartitions; ++pid) {
      SCOPED_TRACE("partition " + std::to_string(pid));
      const auto& bytes = written.bytes[pid];
      MemoryByteSource source(
          reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
      std::vector<size_t> next(3, 0);
      uint32_t payloads = 0;
      std::string error;
      const auto takeBatches = [&] {
        // Each group's batch holds the positions its rows' counts add up to.
        uint64_t positions[3] = {};
        for (uint32_t g = 0; g < 3; ++g) {
          const auto vector = decoders[g]->finishBatch();
          ASSERT_NE(vector, nullptr) << "group " << g;
          for (uint32_t t = 1; g == 0 && t < 3; ++t) {
            const auto* lengths = vector->childAt(kLocalCountColumn[t])
                                      ->as<SimpleVector<int32_t>>();
            for (vector_size_t row = 0; row < vector->size(); ++row) {
              positions[t] +=
                  lengths->isNullAt(row) ? 0 : lengths->valueAt(row);
            }
          }
          if (g != 0) {
            ASSERT_EQ(vector->size(), positions[g]) << "group " << g;
          }
          const auto& expected = written.expected[g][pid];
          ASSERT_LE(next[g] + vector->size(), expected.size());
          for (vector_size_t row = 0; row < vector->size(); ++row) {
            const auto& [block, sourceRow] = expected[next[g] + row];
            ASSERT_TRUE(block->equalValueAt(vector.get(), sourceRow, row))
                << "group " << g << " position " << next[g] + row
                << ": expected " << block->toString(sourceRow) << ", got "
                << vector->toString(row);
          }
          next[g] += vector->size();
        }
      };
      while (true) {
        if (rows.payloadRowsLeft() == 0) {
          if (source.atEnd()) {
            break;
          }
          uint8_t kind;
          ASSERT_TRUE(source.read(&kind, 1));
          ASSERT_EQ(kind, static_cast<uint8_t>(CellBlockKind::kPayload));
          uint32_t rowCount;
          ASSERT_TRUE(reader.readPayload(source, rowCount, error)) << error;
          EXPECT_EQ(reader.payloadRowsLeft(), 0);
          ++payloads;
          ASSERT_TRUE(
              rows.openPayload(reader, kGroups[0].data(), rowCount, error))
              << error;
          for (uint32_t g = 1; g < 3; ++g) {
            // The group's positions: its rows' counts, added up.
            const uint32_t col = kLocalCountColumn[g];
            const auto stream =
                rows.streamBytes(rows.layout().columnStream(col));
            StreamCursor<int32_t> cursor;
            cursor.reset(
                reinterpret_cast<const uint8_t*>(stream.data()),
                stream.size(),
                rows.nonNullCount(col));
            uint64_t positions = 0;
            int32_t lengths[1024];
            for (uint32_t left = rows.nonNullCount(col); left > 0;) {
              const uint32_t n = std::min<uint32_t>(left, 1024);
              ASSERT_TRUE(cursor.next(n, lengths));
              for (uint32_t i = 0; i < n; ++i) {
                ASSERT_GE(lengths[i], 0);
                positions += lengths[i];
              }
              left -= n;
            }
            ASSERT_TRUE(cursor.finished());
            ASSERT_TRUE(decoders[g]->openPayload(
                reader, kGroups[g].data(), positions, error))
                << error;
            if (pid == kSkipped[g]) {
              EXPECT_EQ(positions, 0);
              EXPECT_EQ(decoders[g]->payloadRowsLeft(), 0);
            }
          }
          continue;
        }
        const uint32_t n = std::min(
            {rows.payloadRowsLeft(),
             static_cast<uint32_t>(batchSize - rows.batchRows()),
             kPieces[piece++ % 4]});
        const vector_size_t at = rows.batchRows();
        ASSERT_TRUE(rows.appendRows(n, batchSize, error)) << error;
        for (uint32_t g = 1; g < 3; ++g) {
          // A null count reads 0.
          const auto* lengths =
              rows.batchValues(kLocalCountColumn[g])->as<int32_t>() + at;
          uint32_t positions = 0;
          for (uint32_t i = 0; i < n; ++i) {
            positions += lengths[i];
          }
          // Group A's batches open at one position and grow from there,
          // group B's at their first append's positions.
          ASSERT_TRUE(decoders[g]->appendRows(
              positions,
              g == 1 ? 1 : static_cast<vector_size_t>(std::max(positions, 1u)),
              error))
              << error;
        }
        if (rows.batchRows() == batchSize) {
          ASSERT_NO_FATAL_FAILURE(takeBatches());
        }
      }
      if (rows.batchRows() > 0) {
        ASSERT_NO_FATAL_FAILURE(takeBatches());
      }
      for (uint32_t g = 0; g < 3; ++g) {
        EXPECT_EQ(next[g], written.expected[g][pid].size()) << "group " << g;
        EXPECT_EQ(decoders[g]->payloadRowsLeft(), 0);
      }
      // Partition 0 has a payload per seal and the last window's.
      EXPECT_GE(payloads, pid == 0 ? 3 : 2);
    }
  };

  for (const auto& [remote, compression, spill, dictionary] :
       {std::tuple{false, arrow::Compression::LZ4_FRAME, true, true},
        std::tuple{false, arrow::Compression::UNCOMPRESSED, false, false},
        std::tuple{true, arrow::Compression::LZ4_FRAME, false, true}}) {
    SCOPED_TRACE(
        std::string(remote ? "rss" : "local") + (spill ? " spilled" : "") +
        (compression == arrow::Compression::UNCOMPRESSED ? " stored" : "") +
        (dictionary ? " dictionary" : ""));
    const auto written = write(remote, compression, spill, dictionary);
    for (uint32_t g = 1; g < 3; ++g) {
      EXPECT_TRUE(written.expected[g][kSkipped[g]].empty());
      EXPECT_FALSE(written.expected[g][0].empty());
    }
    for (const int32_t batchSize : {1, 63, 64, 65, 4096}) {
      SCOPED_TRACE("batch size " + std::to_string(batchSize));
      readBack(written, batchSize);
    }
  }
}

TEST_F(CellWriterTest, openedColumnsHoldExactlyTheirPositions) {
  // A group of columns x, y, s, u at 37 positions of its own beside 10
  // rows, through the real output; s is null at its last position, so its
  // records cover all 37. Opened at 37 the group decodes. At any other
  // count the payload is malformed for it - records past the positions, a
  // stream with values left over or missing, zero positions with records
  // or values - and so is a count past the position limit, or null bitmaps
  // past the byte bound: the open or the last append fails, and the decoder
  // then opens the right count cleanly. A batch grows within
  // maxDecodedBytes only: past it the append fails and drops the payload
  // and the batch; u, taking no bytes, grows past any bound. A byte past
  // the null body's records fails the whole payload.
  const auto rowType =
      ROW({"r", "x", "y", "s", "u"},
          {BIGINT(), INTEGER(), DOUBLE(), VARCHAR(), UNKNOWN()});
  const auto layout = CellLayout::create(rowType);
  constexpr uint32_t kRows = 10;
  constexpr uint32_t kPositions = 37;
  auto options = makeOptions(1);
  LocalRawOutput raw(options.partitionWriterOptions);
  LocalCellOutput output(
      raw,
      &layout,
      options.cellOptions,
      options.partitionWriterOptions,
      pool());
  ChunkAllocator allocator(pool(), 16 << 10, 256);
  CellSplitter splitter(layout, 1, allocator, output, *pool());
  std::vector<uint32_t> counts{0};
  splitter.setColumnPositions(
      {nullptr, counts.data(), counts.data(), counts.data()});
  splitColumns(
      splitter,
      layout.numColumns(),
      1,
      {0},
      makeRowVector(
          {"r"},
          {makeFlatVector<int64_t>(kRows, [](auto row) { return row * 3; })}),
      std::vector<uint32_t>(kRows, 0),
      nullptr);
  const auto group = makeRowVector(
      {"x", "y", "s", "u"},
      {makeFlatVector<int32_t>(
           kPositions, [](auto k) { return k * 1000003 % 99991; }),
       makeFlatVector<double>(kPositions, [](auto k) { return k * 0.5; }),
       makeFlatVector<StringView>(
           kPositions,
           [](auto k) { return StringView(k % 2 ? "odd" : "even"); },
           [](auto k) { return k == 5 || k == kPositions - 1; }),
       BaseVector::createNullConstant(UNKNOWN(), kPositions, pool())});
  counts[0] += splitColumns(
      splitter,
      layout.numColumns(),
      1,
      {1, 2, 3, 4},
      group,
      std::vector<uint32_t>(kPositions, 0),
      counts.data())[0];
  ShuffleWriterMetrics metrics;
  splitter.finish(metrics);
  const auto file = readFile();
  ASSERT_EQ(file[0], static_cast<char>(CellBlockKind::kPayload));

  auto codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
  TestDecompressor decompressor(codec.get());
  std::string error;
  {
    // The null body is stored as it is: a byte past its records.
    auto payload = file.substr(1);
    uint32_t nullSizes[2];
    ::memcpy(nullSizes, payload.data() + 16, sizeof(nullSizes));
    ASSERT_EQ(nullSizes[1], 0u);
    payload.insert(kPayloadFixedHeaderBytes + nullSizes[0], 1, '\0');
    ++nullSizes[0];
    ::memcpy(payload.data() + 16, nullSizes, sizeof(nullSizes[0]));
    CellPayloadDecoder decoder(layout, &decompressor, pool());
    MemoryByteSource bytes(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    RowVectorPtr vector;
    EXPECT_FALSE(decoder.decode(bytes, vector, error));
    EXPECT_EQ(error, "null body length mismatch");
  }
  CellPayloadDecoder reader(layout, &decompressor, pool());
  MemoryByteSource source(
      reinterpret_cast<const uint8_t*>(file.data()) + 1, file.size() - 1);
  uint32_t rowCount;
  ASSERT_TRUE(reader.readPayload(source, rowCount, error)) << error;
  ASSERT_TRUE(source.atEnd());
  ASSERT_EQ(rowCount, kRows);
  // Other decoders open what a decoder read, never the decoder itself.
  const uint32_t identity[] = {0, 1, 2, 3, 4};
  EXPECT_THROW(
      reader.openPayload(reader, identity, kRows, error), BoltException);

  // A decoder of the layout columns `columns`, of the group.
  const auto decoderOf = [&](const std::vector<uint32_t>& columns,
                             CellDecodeLimits limits = {}) {
    return std::make_unique<CellPayloadDecoder>(
        subsetLayout(rowType, columns), &decompressor, pool(), limits);
  };
  const auto expectGroup = [&](const RowVectorPtr& vector,
                               const std::vector<uint32_t>& columns) {
    ASSERT_NE(vector, nullptr);
    ASSERT_EQ(vector->size(), kPositions);
    for (uint32_t i = 0; i < columns.size(); ++i) {
      for (vector_size_t k = 0; k < kPositions; ++k) {
        ASSERT_TRUE(group->childAt(columns[i] - 1)
                        ->equalValueAt(vector->childAt(i).get(), k, k))
            << "column " << columns[i] << " position " << k;
      }
    }
  };
  // Opens `decoder` at the group's positions, all of which it reads.
  const auto expectOpens = [&](CellPayloadDecoder& decoder,
                               const std::vector<uint32_t>& columns) {
    ASSERT_TRUE(decoder.openPayload(reader, columns.data(), kPositions, error))
        << error;
    ASSERT_TRUE(decoder.appendRows(kPositions, kPositions, error)) << error;
    ASSERT_NO_FATAL_FAILURE(expectGroup(decoder.finishBatch(), columns));
  };
  const std::vector<uint32_t> all{1, 2, 3, 4};
  {
    auto decoder = decoderOf(all);
    EXPECT_EQ(decoder->rowBytes(), 4u + 8 + 16);
    ASSERT_NO_FATAL_FAILURE(expectOpens(*decoder, all));
  }
  {
    // Records past the positions, and zero positions with records.
    const std::vector<uint32_t> s{3};
    auto decoder = decoderOf(s);
    for (const uint32_t positions : {kPositions - 1, 0u}) {
      EXPECT_FALSE(decoder->openPayload(reader, s.data(), positions, error));
      EXPECT_EQ(error, "null records past the payload's rows");
      EXPECT_EQ(decoder->payloadRowsLeft(), 0);
    }
    ASSERT_NO_FATAL_FAILURE(expectOpens(*decoder, s));
  }
  {
    // A raw stream checks its length when opened, at zero positions too.
    const std::vector<uint32_t> y{2};
    auto decoder = decoderOf(y);
    for (const uint32_t positions : {kPositions - 1, kPositions + 1, 0u}) {
      EXPECT_FALSE(decoder->openPayload(reader, y.data(), positions, error));
      EXPECT_EQ(error, "raw stream length mismatch");
    }
    ASSERT_NO_FATAL_FAILURE(expectOpens(*decoder, y));
  }
  {
    // An encoded stream with values left over fails at the last position,
    // one with values missing when they run out, and one with values at
    // zero positions when opened.
    const std::vector<uint32_t> x{1};
    auto decoder = decoderOf(x);
    ASSERT_TRUE(decoder->openPayload(reader, x.data(), kPositions - 1, error))
        << error;
    ASSERT_TRUE(decoder->appendRows(20, 64, error)) << error;
    EXPECT_FALSE(decoder->appendRows(kPositions - 21, 64, error));
    EXPECT_EQ(error, "malformed value stream");
    EXPECT_EQ(decoder->payloadRowsLeft(), 0);
    EXPECT_EQ(decoder->finishBatch(), nullptr);
    ASSERT_TRUE(decoder->openPayload(reader, x.data(), kPositions + 1, error))
        << error;
    EXPECT_FALSE(decoder->appendRows(kPositions + 1, 64, error));
    EXPECT_EQ(error, "malformed value stream");
    EXPECT_FALSE(decoder->openPayload(reader, x.data(), 0, error));
    EXPECT_EQ(error, "malformed value stream");
    ASSERT_NO_FATAL_FAILURE(expectOpens(*decoder, x));
  }
  {
    // More positions than the limit allows, and more bitmap bytes than the
    // bound: s's bitmap takes 5.
    CellDecodeLimits limits;
    limits.maxRowCount = kPositions - 1;
    EXPECT_FALSE(decoderOf(all, limits)
                     ->openPayload(reader, all.data(), kPositions, error));
    EXPECT_EQ(error, "positions beyond the configured limit");
    limits.maxRowCount = kPositions;
    EXPECT_TRUE(decoderOf(all, limits)
                    ->openPayload(reader, all.data(), kPositions, error))
        << error;
    limits.maxDecodedBytes = 4;
    EXPECT_FALSE(decoderOf(all, limits)
                     ->openPayload(reader, all.data(), kPositions, error));
    EXPECT_EQ(error, "null bitmaps beyond sanity bound");
  }
  {
    // x takes 4 bytes a position: a bound of 30 positions caps the
    // doubling - 16 positions grow to 30, not 32 - and fails the 31st.
    const std::vector<uint32_t> x{1};
    CellDecodeLimits limits;
    limits.maxDecodedBytes = 4 * 30;
    auto decoder = decoderOf(x, limits);
    ASSERT_TRUE(decoder->openPayload(reader, x.data(), kPositions, error))
        << error;
    for (const uint32_t positions : {1, 15, 10, 4}) {
      ASSERT_TRUE(decoder->appendRows(positions, 1, error)) << error;
    }
    EXPECT_EQ(decoder->batchRows(), 30);
    EXPECT_FALSE(decoder->appendRows(1, 1, error));
    EXPECT_EQ(error, "batch beyond sanity bound");
    EXPECT_EQ(decoder->payloadRowsLeft(), 0);
    EXPECT_EQ(decoder->batchRows(), 0);
    EXPECT_EQ(decoder->finishBatch(), nullptr);
    // With room for every position x grows to them all, and u, taking no
    // bytes, grows past any bound.
    limits.maxDecodedBytes = 4 * kPositions;
    for (const auto& columns : {x, std::vector<uint32_t>{4}}) {
      auto bounded = decoderOf(columns, limits);
      ASSERT_TRUE(
          bounded->openPayload(reader, columns.data(), kPositions, error))
          << error;
      ASSERT_TRUE(bounded->appendRows(1, 1, error)) << error;
      ASSERT_TRUE(bounded->appendRows(kPositions - 1, 1, error)) << error;
      ASSERT_NO_FATAL_FAILURE(expectGroup(bounded->finishBatch(), columns));
    }
  }
}

TEST_F(CellWriterTest, fullPrimitiveTypesAndUnknownWireSlots) {
  constexpr int32_t n = 2057;
  std::vector<VectorPtr> children;
  for (int i = 0; i < 10; ++i) {
    children.push_back(BaseVector::createNullConstant(UNKNOWN(), n, pool()));
    children.push_back(makeFlatVector<bool>(
        n,
        [i](auto row) { return (row + i) % 2; },
        [](auto row) { return row % 5 == 0; }));
  }
  children.push_back(makeFlatVector<Timestamp>(
      n,
      [](auto row) {
        return Timestamp::fromMicros((int64_t(row) - 128) * 1000001);
      },
      [](auto row) { return row % 7 == 0; }));
  children.push_back(makeFlatVector<int128_t>(
      n,
      [](auto row) { return (int128_t(row) - 128) * (int128_t(1) << 90); },
      [](auto row) { return row % 9 == 0; },
      DECIMAL(38, 4)));
  children.push_back(BaseVector::createNullConstant(UNKNOWN(), n, pool()));
  children.push_back(makeFlatVector<StringView>(
      n, [](auto) { return StringView("dictionary"); }));
  auto data = makeRowVector(children);
  auto options = makeOptions(4);
  options.cellOptions.checkpointPartitionBytes = 1;
  std::vector<int32_t> pids(n);
  for (int i = 0; i < n; ++i) {
    pids[i] = i % 3;
  }
  const auto roundTripWithDictionary = [&](const RowVectorPtr& input) {
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    const auto [pidSlices, batches] = sliceBatches(input, pids, 1024);
    roundTrip(options, pidSlices, batches, &writer);
    EXPECT_GT(writer.metrics().dictionaryMatchedRows, 0);
  };
  roundTripWithDictionary(data);
  std::vector<VectorPtr> wrapped;
  auto indices = makeIndices(n, [](auto row) { return n - row - 1; });
  for (const auto& child : children) {
    wrapped.push_back(BaseVector::wrapInDictionary(nullptr, indices, n, child));
  }
  roundTripWithDictionary(makeRowVector(wrapped));
  wrapped.clear();
  for (const auto& child : children) {
    wrapped.push_back(wrapInLazyDictionary(child));
  }
  roundTripWithDictionary(makeRowVector(wrapped));
  wrapped.clear();
  for (const auto& child : children) {
    wrapped.push_back(BaseVector::wrapInConstant(n, 1, child));
  }
  roundTripWithDictionary(makeRowVector(wrapped));
}

TEST_F(CellWriterTest, unknownOnlyHeaderAndRowBoundaries) {
  auto data = makeRowVector(
      {BaseVector::createNullConstant(UNKNOWN(), 99, pool()),
       BaseVector::createNullConstant(UNKNOWN(), 99, pool())});
  auto options = makeOptions(3);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  roundTrip(options, {std::vector<int32_t>(99, 1)}, {data}, &writer);
  ASSERT_EQ(
      writer.metrics().partitionLengths, (std::vector<int64_t>{0, 25, 0}));
  const auto file = readFile();
  EXPECT_EQ(static_cast<uint8_t>(file[0]), 0); // the block kind byte
  const auto bytes = file.substr(1);
  EXPECT_EQ(static_cast<uint8_t>(bytes[0]), 99);
  EXPECT_EQ(bytes.substr(1), std::string(23, '\0'));

  auto layout = CellLayout::create(asRowType(data->type()));
  CellPayloadDecoder decoder(layout, nullptr, pool());
  for (const auto index : {4, 8, 16, 20}) {
    auto invalid = bytes;
    invalid[index] = 1;
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(invalid.data()), invalid.size());
    RowVectorPtr output;
    std::string error;
    EXPECT_FALSE(decoder.decode(source, output, error)) << index;
  }
}

TEST_F(CellWriterTest, groupedComplexAdapterAndCellRoundTrip) {
  auto arrays = makeArrayVector<int64_t>({{1, 2}, {}, {3}, {4, 5}});
  arrays->setNull(2, true);
  auto maps =
      makeMapVector<int64_t, int64_t>({{{1, 2}}, {}, {{3, 4}}, {{5, 6}}});
  maps->setNull(2, true);
  auto nested = makeRowVector({arrays, maps});
  nested->setNull(1, true);
  auto nativeBinary = makeFlatVector<StringView>(
      {StringView("ab"), StringView(""), StringView("cd"), StringView("ef")},
      VARBINARY());
  auto logical = makeRowVector(
      {arrays,
       nativeBinary,
       BaseVector::createNullConstant(UNKNOWN(), 4, pool()),
       maps,
       nested});
  CellShuffleTypeAdapter adapter(asRowType(logical->type()));
  ASSERT_EQ(adapter.physicalType()->size(), 3);
  EXPECT_EQ(adapter.physicalType()->childAt(0)->kind(), TypeKind::VARBINARY);
  EXPECT_EQ(adapter.physicalType()->childAt(1)->kind(), TypeKind::UNKNOWN);
  EXPECT_EQ(adapter.physicalType()->childAt(2)->kind(), TypeKind::VARBINARY);
  vector_size_t offset = 0;
  auto physical = adapter.encodeNext(logical, offset, pool());
  ASSERT_EQ(offset, 4);
  auto binary = physical->childAt(2)->as<FlatVector<StringView>>();
  for (int i = 0; i < 4; ++i) {
    EXPECT_FALSE(binary->isNullAt(i));
  }
  bolt::test::assertEqualVectors(logical, decodeComplex(adapter, physical));
  auto options = makeOptions(1);
  options.cellOptions.checkpointPartitionBytes = 1;
  const auto [pidSlices, batches] = sliceBatches(physical, {0, 0, 0, 0}, 2);
  roundTrip(options, pidSlices, batches);
  auto payloads = decodePartition(
      CellLayout::create(adapter.physicalType()),
      readFile(),
      0,
      readFile().size());
  vector_size_t at = 0;
  for (const auto& payload : payloads) {
    auto restored = decodeComplex(adapter, payload);
    bolt::test::assertEqualVectors(
        logical->slice(at, restored->size()), restored);
    at += restored->size();
  }
  ASSERT_EQ(at, 4);

  // CompactRow rejects a truncated binary.
  auto malformed =
      makeFlatVector<StringView>({StringView("\0", 1)}, VARBINARY());
  auto bad = std::make_shared<RowVector>(
      pool(),
      adapter.physicalType(),
      nullptr,
      1,
      std::vector<VectorPtr>{
          nativeBinary->slice(0, 1),
          BaseVector::createNullConstant(UNKNOWN(), 1, pool()),
          malformed});
  EXPECT_THROW(decodeComplex(adapter, bad), BoltException);
  malformed->setNull(0, true);
  EXPECT_THROW(decodeComplex(adapter, bad), BoltException);
}

TEST_F(CellWriterTest, complexAdapterSlicesAndSimpleFastPath) {
  auto simple = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  CellShuffleTypeAdapter identity(asRowType(simple->type()));
  vector_size_t offset = 0;
  EXPECT_EQ(identity.encodeNext(simple, offset, pool()).get(), simple.get());
  EXPECT_EQ(decodeComplex(identity, simple).get(), simple.get());
  auto arrays = makeArrayVector<int64_t>(
      2049, [](auto) { return 2; }, [](auto i) { return i; });
  auto input = makeRowVector({arrays});
  CellShuffleTypeAdapter adapter(asRowType(input->type()));
  offset = 0;
  int batches = 0;
  while (offset < input->size()) {
    const auto start = offset;
    auto physical = adapter.encodeNext(input, offset, pool());
    EXPECT_LE(physical->size(), 1024);
    bolt::test::assertEqualVectors(
        input->slice(start, offset - start), decodeComplex(adapter, physical));
    ++batches;
  }
  EXPECT_EQ(batches, 3);
}

TEST_F(CellWriterTest, nestedComplexEncodingsAndMalformedInput) {
  using Inner = std::vector<std::optional<int64_t>>;
  using Outer = std::vector<std::optional<Inner>>;
  auto arrays = makeNullableNestedArrayVector<int64_t>(
      {Outer{Inner{1, std::nullopt, 3}, std::nullopt, Inner{}},
       std::nullopt,
       Outer{Inner{4}, Inner{5, 6}}});
  auto nested = makeRowVector(
      {BaseVector::createNullConstant(UNKNOWN(), 3, pool()), arrays});
  auto logical = makeRowVector({arrays, nested});
  CellShuffleTypeAdapter adapter(asRowType(logical->type()));
  for (const bool lazy : {false, true}) {
    std::vector<VectorPtr> children;
    for (const auto& child : logical->children()) {
      children.push_back(lazy ? wrapInLazyDictionary(child) : child);
    }
    auto input = makeRowVector(children);
    vector_size_t offset = 0;
    auto physical = adapter.encodeNext(input, offset, pool());
    bolt::test::assertEqualVectors(logical, decodeComplex(adapter, physical));
    auto binary = physical->childAt(0)->as<FlatVector<StringView>>();
    auto value = binary->valueAt(0).str();
    // Match CompactRow's acceptance of trailing bytes.
    value.push_back('\0');
    binary->set(0, StringView(value));
    bolt::test::assertEqualVectors(logical, decodeComplex(adapter, physical));
    value.resize(value.size() - 2);
    binary->set(0, StringView(value));
    vector_size_t decodeOffset = 0;
    EXPECT_THROW(
        adapter.decodeNext(physical, decodeOffset, pool()), BoltException);
    EXPECT_EQ(decodeOffset, 0);
  }
  auto nullInput = makeRowVector(
      {BaseVector::createNullConstant(arrays->type(), 3, pool()),
       BaseVector::createNullConstant(nested->type(), 3, pool())});
  vector_size_t offset = 0;
  auto physical = adapter.encodeNext(nullInput, offset, pool());
  EXPECT_FALSE(physical->childAt(0)->isNullAt(0));
  bolt::test::assertEqualVectors(nullInput, decodeComplex(adapter, physical));
}

TEST_F(CellWriterTest, complexByteBounds) {
  std::string text(3 << 20, 'x');
  auto strings = makeFlatVector<StringView>(
      {StringView(text), StringView(text), StringView(text)});
  auto input = makeRowVector({makeRowVector({strings})});
  CellShuffleTypeAdapter adapter(asRowType(input->type()));
  vector_size_t offset = 0;
  auto physical = adapter.encodeNext(input, offset, pool());
  EXPECT_EQ(offset, 2);
  bolt::test::assertEqualVectors(
      input->slice(0, 2), decodeComplex(adapter, physical));
  physical = adapter.encodeNext(input, offset, pool());
  EXPECT_EQ(offset, 3);
  bolt::test::assertEqualVectors(
      input->slice(2, 1), decodeComplex(adapter, physical));

  auto repeated = std::make_shared<RowVector>(
      pool(),
      adapter.physicalType(),
      nullptr,
      3,
      std::vector<VectorPtr>{
          BaseVector::wrapInConstant(3, 0, physical->childAt(0))});
  vector_size_t decodeOffset = 0;
  auto first = adapter.decodeNext(repeated, decodeOffset, pool());
  EXPECT_EQ(decodeOffset, 2);
  bolt::test::assertEqualVectors(input->slice(0, 2), first);
  auto second = adapter.decodeNext(repeated, decodeOffset, pool());
  EXPECT_EQ(decodeOffset, 3);
  bolt::test::assertEqualVectors(input->slice(2, 1), second);

  // No per-row size cap: a row beyond the batch target forms its own batch.
  std::string oversized((64ULL << 20) + 1, 'z');
  auto large = makeRowVector({makeRowVector({makeFlatVector<StringView>(
      {StringView(oversized), StringView(text)})})});
  offset = 0;
  physical = adapter.encodeNext(large, offset, pool());
  EXPECT_EQ(offset, 1);
  bolt::test::assertEqualVectors(
      large->slice(0, 1), decodeComplex(adapter, physical));
  physical = adapter.encodeNext(large, offset, pool());
  EXPECT_EQ(offset, 2);
  bolt::test::assertEqualVectors(
      large->slice(1, 1), decodeComplex(adapter, physical));
}

TEST_F(CellWriterTest, rejectsInvalidBooleanAndMissingNullBody) {
  // One non-null BOOLEAN, one stored Run, one raw byte.
  std::string bytes(44, '\0');
  bytes[0] = 1;
  bytes[4] = 1;
  bytes[16] = 1;
  bytes[24] = 1;
  bytes[26] = 2;
  bytes[27] = 1;
  bytes[35] = 1;
  bytes[43] = 2;
  CellPayloadDecoder decoder(
      CellLayout::create(ROW({BOOLEAN()})), nullptr, pool());
  auto decode = [&](const std::string& payload) {
    MemoryByteSource source(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    RowVectorPtr output;
    std::string error;
    return decoder.decode(source, output, error);
  };
  EXPECT_FALSE(decode(bytes));
  bytes[43] = 1;
  EXPECT_TRUE(decode(bytes));
  bytes[16] = 0;
  EXPECT_FALSE(decode(bytes));
}

TEST_F(CellWriterTest, complexDecodeRespectsRowLimit) {
  constexpr vector_size_t elementsPerRow = 10000;
  auto offsets = allocateOffsets(1, pool());
  offsets->asMutable<vector_size_t>()[0] = 0;
  auto sizes = allocateSizes(1, pool());
  sizes->asMutable<vector_size_t>()[0] = elementsPerRow;
  auto array = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(UNKNOWN()),
      nullptr,
      1,
      offsets,
      sizes,
      BaseVector::createNullConstant(UNKNOWN(), elementsPerRow, pool()));
  auto input = makeRowVector({array});
  CellShuffleTypeAdapter adapter(asRowType(input->type()));
  vector_size_t offset = 0;
  auto encoded = adapter.encodeNext(input, offset, pool());
  // UNKNOWN elements remain a null constant in CompactRow. Batch by row count
  // without charging an artificial per-element workspace estimate.
  auto physical = std::make_shared<RowVector>(
      pool(),
      adapter.physicalType(),
      nullptr,
      2049,
      std::vector<VectorPtr>{
          BaseVector::wrapInConstant(2049, 0, encoded->childAt(0))});
  offset = 0;
  int batches = 0;
  while (offset < physical->size()) {
    const auto start = offset;
    auto part = adapter.decodeNext(physical, offset, pool());
    ASSERT_EQ(part->size(), std::min(1024, physical->size() - start));
    EXPECT_EQ(offset, start + part->size());
    const auto* restored = part->childAt(0)->as<ArrayVector>();
    EXPECT_EQ(restored->sizeAt(0), elementsPerRow);
    EXPECT_EQ(restored->sizeAt(part->size() - 1), elementsPerRow);
    EXPECT_TRUE(restored->elements()->isNullAt(0));
    ++batches;
  }
  EXPECT_EQ(batches, 3);
}

TEST_F(CellWriterTest, multiPartitionRoundTrip) {
  constexpr int32_t kPartitions = 8;
  std::mt19937 rng(7);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 5; ++batch) {
    const int n = 300 + batch * 57;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto bigints = makeFlatVector<int64_t>(
        n, [&](auto row) { return row * 1'000'003 + batch; });
    std::vector<std::optional<int16_t>> shorts(n);
    for (int i = 0; i < n; ++i) {
      shorts[i] = (i % 7 == 0)
          ? std::nullopt
          : std::optional<int16_t>(static_cast<int16_t>(i % 100 - 50));
    }
    auto smallints = makeNullableFlatVector<int16_t>(shorts);
    auto reals = makeFlatVector<float>(n, [](auto row) { return row * 0.5f; });
    auto strings = makeNullableStrings(n, rng);
    batches.push_back(makeRowVector(
        {"a", "b", "c", "d"}, {bigints, smallints, reals, strings}));
    pids.push_back(std::move(batchPids));
  }
  roundTrip(makeOptions(kPartitions), pids, batches);
}

TEST_F(CellWriterTest, spillsAndCheckpointsStillRoundTrip) {
  constexpr int32_t kPartitions = 4;
  auto options = makeOptions(kPartitions);
  // Force frequent physical spills and window closes.
  options.cellOptions.cellMemoryCapBytes = 8 << 20;
  options.cellOptions.checkpointPartitionBytes = 16 << 10;
  options.cellOptions.nullMemLimitBytes = 4 << 10;

  std::mt19937 rng(11);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 8; ++batch) {
    const int n = 1024;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto values = makeFlatVector<int64_t>(
        n, [&](auto /*row*/) { return static_cast<int64_t>(rng()); });
    auto strings = makeNullableStrings(n, rng);
    batches.push_back(makeRowVector({"v", "s"}, {values, strings}));
    pids.push_back(std::move(batchPids));
  }

  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  roundTrip(options, pids, batches, &writer);
  EXPECT_GT(writer.metrics().spillCount, 0) << "expected sealed windows";
  EXPECT_GT(writer.metrics().totalBytesEvicted, 0);
}

TEST_F(CellWriterTest, compressionCodecShrinksSpillsAndRoundTrips) {
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(29);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 6; ++batch) {
    const int n = 2048;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto values = makeFlatVector<int64_t>(
        n, [&](auto row) { return 5'000'000'000LL + row % 977; });
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      storage[i].assign(256, static_cast<char>('a' + i % 4));
      views[i] = StringView(storage[i]);
    }
    auto strings = makeNullableFlatVector<StringView>(views);
    batches.push_back(makeRowVector({"v", "s"}, {values, strings}));
    pids.push_back(std::move(batchPids));
  }

  const auto runOnce = [&](arrow::Compression::type compressionType) {
    auto options = makeOptions(kPartitions);
    options.cellOptions.cellMemoryCapBytes = 8 << 20;
    options.cellOptions.checkpointPartitionBytes = 64 << 10;
    options.partitionWriterOptions.compressionType = compressionType;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    EXPECT_GT(writer.metrics().spillCount, 0);
    return writer.metrics().totalBytesEvicted;
  };

  const auto compressedSpillBytes = runOnce(arrow::Compression::LZ4_FRAME);
  ::unlink(dataFile_.c_str());
  const auto rawSpillBytes = runOnce(arrow::Compression::UNCOMPRESSED);
  // The whole point of spill compression: fewer bytes hit the disk on the
  // spill pass (the compressible strings dominate this data set).
  EXPECT_LT(compressedSpillBytes * 2, rawSpillBytes)
      << "compressed spill " << compressedSpillBytes << " vs raw "
      << rawSpillBytes;
}

TEST_F(CellWriterTest, compressSpillToggleAcrossMergeShapes) {
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(31);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  // ~4MB per batch: with an 8MB chunk cap every window sees several
  // pressure spills. The last partition takes one short row per batch, so
  // its segments stay below the compression minimum - stored at spill time
  // even with compressSpill, and final as they are. The closing small
  // batch leaves resident cells beside the last window's spilled segments.
  for (int batch = 0; batch < 9; ++batch) {
    pids.emplace_back();
    batches.push_back(makeCompressibleBatch(
        batch < 8 ? 4096 : 256, kPartitions, rng, pids.back()));
  }
  const auto layout = CellLayout::create(asRowType(batches[0]->type()));
  // Sealed windows (the first payload of a partition spans several spill
  // runs), then one window only: the residual payload carries its spilled
  // segments and then the resident cells.
  for (const bool seal : {true, false}) {
    SCOPED_TRACE(seal ? "sealed windows" : "residual window");
    int64_t evicted[2] = {0, 0};
    int64_t written[2] = {0, 0};
    for (const bool compressSpill : {true, false}) {
      auto options = makeOptions(kPartitions);
      options.partitionWriterOptions.compressionType =
          arrow::Compression::LZ4_FRAME;
      options.cellOptions.compressSpill = compressSpill;
      options.cellOptions.cellMemoryCapBytes = 8 << 20;
      options.cellOptions.checkpointPartitionBytes = seal ? 4 << 20 : 1LL << 30;
      CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
      roundTrip(options, pids, batches, &writer);
      const auto& metrics = writer.metrics();
      EXPECT_GT(metrics.totalBytesEvicted, 0);
      EXPECT_EQ(metrics.spillCount > 0, seal); // sealed windows
      const auto file = readFile();
      const auto runCounts =
          payloadRunCounts(layout, file, 0, metrics.partitionLengths[0]);
      ASSERT_FALSE(runCounts.empty());
      if (seal) {
        EXPECT_GE(runCounts.size(), 2);
      } else {
        EXPECT_EQ(runCounts.size(), 1);
      }
      // Spilled segments, plus the resident cells when not sealed. A
      // pressure spill takes the largest partitions only, so not every one
      // is in every Run: some large partition's first payload spans
      // several.
      uint32_t most = 0;
      uint64_t offset = 0;
      for (int32_t pid = 0; pid + 1 < kPartitions; ++pid) {
        const auto counts = payloadRunCounts(
            layout, file, offset, metrics.partitionLengths[pid]);
        offset += metrics.partitionLengths[pid];
        if (!counts.empty()) {
          most = std::max(most, counts[0]);
        }
      }
      EXPECT_GE(most, 2);
      evicted[compressSpill] = metrics.totalBytesEvicted;
      written[compressSpill] = metrics.totalBytesWritten;
      ::unlink(dataFile_.c_str());
    }
    EXPECT_LT(evicted[1] * 2, evicted[0]);
    // Spill compression changes what hits the spill file, never the final
    // file: verbatim and re-merged runs carry the same bytes.
    EXPECT_EQ(written[1], written[0]);
  }
}

TEST_F(CellWriterTest, oneShotAndStreamingCompressionWriteTheSameRuns) {
  // The two compression modes differ only in how a frame is compressed:
  // the same runs, the same layouts, the same rows read back - through
  // pressure spills, seals and the merge.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(43);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 9; ++batch) {
    pids.emplace_back();
    batches.push_back(makeCompressibleBatch(
        batch < 8 ? 4096 : 256, kPartitions, rng, pids.back()));
  }
  const auto layout = CellLayout::create(asRowType(batches[0]->type()));
  std::vector<std::vector<uint32_t>> runCounts[2];
  int64_t written[2] = {0, 0};
  for (const bool streaming : {true, false}) {
    auto options = makeOptions(kPartitions);
    options.partitionWriterOptions.compressionType =
        arrow::Compression::LZ4_FRAME;
    options.cellOptions.streamCompression = streaming;
    options.cellOptions.cellMemoryCapBytes = 8 << 20;
    options.cellOptions.checkpointPartitionBytes = 4 << 20;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    const auto& metrics = writer.metrics();
    EXPECT_GT(metrics.totalBytesEvicted, 0);
    const auto file = readFile();
    uint64_t offset = 0;
    for (int32_t pid = 0; pid < kPartitions; ++pid) {
      runCounts[streaming].push_back(payloadRunCounts(
          layout, file, offset, metrics.partitionLengths[pid]));
      offset += metrics.partitionLengths[pid];
    }
    written[streaming] = metrics.totalBytesWritten;
    ::unlink(dataFile_.c_str());
  }
  EXPECT_EQ(runCounts[0], runCounts[1]);
  // Frames compressed whole or in independent blocks: close in size.
  EXPECT_LT(written[1], written[0] * 11 / 10);
  EXPECT_LT(written[0], written[1] * 11 / 10);
}

TEST_F(CellWriterTest, skewedPartitionSealsAlone) {
  // Most rows go to partition 0, which passes the payload bound several
  // times; the others stay far below it. Only partition 0's payloads close
  // and spill: the others keep one payload, written from memory.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(23);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 12; ++batch) {
    auto& batchPids = pids.emplace_back(4096);
    std::vector<std::string> storage(4096);
    std::vector<std::optional<StringView>> views(4096);
    for (int i = 0; i < 4096; ++i) {
      batchPids[i] = rng() % 16 == 0 ? 1 + rng() % (kPartitions - 1) : 0;
      storage[i].assign(256, static_cast<char>('a' + rng() % 26));
      views[i] = StringView(storage[i]);
    }
    batches.push_back(makeRowVector(
        {"v", "s"},
        {makeFlatVector<int64_t>(4096, [&](auto row) { return row * 7; }),
         makeNullableFlatVector<StringView>(views)}));
  }
  const auto layout = CellLayout::create(asRowType(batches[0]->type()));
  auto options = makeOptions(kPartitions);
  options.cellOptions.checkpointPartitionBytes = 2 << 20;
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  roundTrip(options, pids, batches, &writer);

  const auto& metrics = writer.metrics();
  const auto file = readFile();
  uint64_t offset = 0;
  std::vector<size_t> payloads;
  for (int32_t pid = 0; pid < kPartitions; ++pid) {
    payloads.push_back(
        payloadRunCounts(layout, file, offset, metrics.partitionLengths[pid])
            .size());
    offset += metrics.partitionLengths[pid];
  }
  EXPECT_GE(payloads[0], 4);
  for (int32_t pid = 1; pid < kPartitions; ++pid) {
    EXPECT_EQ(payloads[pid], 1) << "pid " << pid;
  }
  // Only partition 0's sealed payloads went through the spill file: well
  // under its share of the output, nothing of the others'.
  EXPECT_GT(metrics.totalBytesEvicted, 0);
  EXPECT_LT(metrics.totalBytesEvicted, metrics.partitionLengths[0] * 11 / 10);
}

TEST_F(CellWriterTest, pressureSpillCompressesWithoutWorkspace) {
  // A pressure spill under a pool with no room to spare still compresses:
  // runs stream from the cells through a fixed buffer, with no workspace
  // sized by the run to fund.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(37);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  int64_t rawBytes = 0;
  for (int batch = 0; batch < 8; ++batch) {
    pids.emplace_back();
    batches.push_back(
        makeCompressibleBatch(4096, kPartitions, rng, pids.back()));
    rawBytes += batches.back()->estimateFlatSize();
  }
  auto root = memory::memoryManager()->addRootPool(
      "unfundedSpillCompression", 24 << 20);
  auto leaf = root->addLeafChild("writer");
  const auto setCapacity = [&](int64_t bytes) {
    static_cast<memory::MemoryPoolImpl*>(root.get())->testingSetCapacity(bytes);
  };
  auto options = makeOptions(kPartitions);
  options.partitionWriterOptions.compressionType =
      arrow::Compression::LZ4_FRAME;
  options.cellOptions.checkpointPartitionBytes = 4 << 20;
  setCapacity(24 << 20);
  CellShuffleWriter writer(options, leaf.get(), arrow::default_memory_pool());
  roundTrip(
      options, pids, batches, &writer, [&](CellShuffleWriter&, uint32_t i) {
        if (i + 1 == batches.size()) {
          setCapacity(1LL << 30); // the merge is funded
        }
      });
  const auto& metrics = writer.metrics();
  EXPECT_GT(metrics.spillCount, 0);
  // The spill file holds compressed runs, and so does the data file.
  EXPECT_GT(metrics.totalBytesEvicted, 0);
  EXPECT_LT(metrics.totalBytesEvicted * 8, rawBytes);
  EXPECT_LT(metrics.totalBytesWritten * 8, rawBytes);
}

TEST_F(CellWriterTest, spilledRunCompressesPastAnIncompressibleStream) {
  // Each spilled run opens with a stream of random integers no codec
  // shrinks, followed by a stream of repetitive strings that makes most of
  // its bytes. Written stream by stream, the strings still compress: one
  // frame over the whole run would be given up within its first blocks.
  std::mt19937_64 rng(41);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 8; ++batch) {
    constexpr int kRows = 4096;
    pids.emplace_back(kRows, 0);
    std::vector<VectorPtr> columns;
    std::vector<std::string> names;
    for (int c = 0; c < 4; ++c) {
      columns.push_back(makeFlatVector<int64_t>(
          kRows, [&](auto) { return static_cast<int64_t>(rng()); }));
      names.push_back(fmt::format("v{}", c));
    }
    std::vector<std::string> storage(kRows);
    std::vector<std::optional<StringView>> views(kRows);
    for (int i = 0; i < kRows; ++i) {
      storage[i].assign(192, static_cast<char>('a' + i % 4));
      views[i] = StringView(storage[i]);
    }
    columns.push_back(makeNullableFlatVector<StringView>(views));
    names.push_back("s");
    batches.push_back(makeRowVector(names, columns));
  }

  const auto runOnce = [&](arrow::Compression::type compressionType) {
    auto options = makeOptions(1);
    // Payloads close every ~2MB, each as a run through the spill file.
    options.cellOptions.checkpointPartitionBytes = 2 << 20;
    options.partitionWriterOptions.compressionType = compressionType;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    EXPECT_GT(writer.metrics().totalBytesEvicted, 0);
    const auto result = std::make_pair(
        writer.metrics().totalBytesEvicted, writer.metrics().totalBytesWritten);
    ::unlink(dataFile_.c_str());
    return result;
  };

  const auto [compressedSpill, compressedOut] =
      runOnce(arrow::Compression::LZ4_FRAME);
  const auto [rawSpill, rawOut] = runOnce(arrow::Compression::UNCOMPRESSED);
  // The strings are 6/7 of the bytes: well under half remains of them.
  EXPECT_LT(compressedSpill * 2, rawSpill)
      << "compressed spill " << compressedSpill << " vs raw " << rawSpill;
  EXPECT_LT(compressedOut * 2, rawOut)
      << "compressed output " << compressedOut << " vs raw " << rawOut;
}

TEST_F(CellWriterTest, celebornPushesEveryWindowWhole) {
  // Under a cap far below the data, every pressure point turns into a seal
  // at the next batch boundary: many windows, each pushed as one whole
  // payload per partition, and no local file at all.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(41);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  int64_t rawBytes = 0;
  for (int batch = 0; batch < 8; ++batch) {
    pids.emplace_back();
    batches.push_back(
        makeCompressibleBatch(4096, kPartitions, rng, pids.back()));
    rawBytes += batches.back()->estimateFlatSize();
  }
  for (const bool compress : {true, false}) {
    SCOPED_TRACE(compress ? "lz4" : "uncompressed");
    auto client = std::make_shared<RecordingRssClient>();
    auto options = makeCelebornOptions(kPartitions, client);
    options.partitionWriterOptions.compressionType = compress
        ? arrow::Compression::LZ4_FRAME
        : arrow::Compression::UNCOMPRESSED;
    options.cellOptions.cellMemoryCapBytes = 8 << 20;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    const auto& metrics = writer.metrics();
    EXPECT_GE(metrics.spillCount, 3);
    // Every window spans whole batches, so it reaches every partition.
    EXPECT_EQ(client->numPushes % kPartitions, 0);
    EXPECT_GE(client->numPushes, metrics.spillCount * kPartitions);
    EXPECT_EQ(metrics.totalBytesEvicted, metrics.totalBytesWritten);
    if (compress) {
      EXPECT_LT(metrics.totalBytesWritten * 8, rawBytes);
    } else {
      EXPECT_GT(metrics.totalBytesWritten * 2, rawBytes);
    }
  }
}

TEST_F(CellWriterTest, celebornReclaimPushesTheWindowBetweenBatches) {
  // Between batches the window is whole: a reclaim pushes it at once and
  // returns its chunks.
  constexpr int32_t kPartitions = 4;
  std::mt19937 rng(43);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 6; ++batch) {
    pids.emplace_back();
    batches.push_back(
        makeCompressibleBatch(4096, kPartitions, rng, pids.back()));
  }
  auto client = std::make_shared<RecordingRssClient>();
  auto options = makeCelebornOptions(kPartitions, client);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  int64_t freed = 0;
  roundTrip(
      options, pids, batches, &writer, [&](CellShuffleWriter& w, uint32_t i) {
        if (i != 2) {
          return;
        }
        EXPECT_EQ(client->numPushes, 0);
        ASSERT_TRUE(w.reclaimFixedSize(1LL << 30, &freed).ok());
        EXPECT_EQ(client->numPushes, kPartitions);
      });
  EXPECT_GE(freed, 8 << 20);
  EXPECT_EQ(writer.metrics().spillCount, 1);
  EXPECT_EQ(client->numPushes, 2 * kPartitions);
}

TEST_F(CellWriterTest, celebornEmptyMapperStillEndsTheAttempt) {
  auto client = std::make_shared<RecordingRssClient>();
  const auto options = makeCelebornOptions(3, client);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  ASSERT_TRUE(writer.stop().ok());
  EXPECT_EQ(client->stops, 1);
  EXPECT_EQ(client->numPushes, 0);
  EXPECT_EQ(writer.metrics().partitionLengths, std::vector<int64_t>(3, 0));
  EXPECT_TRUE(readFile().empty());
}

TEST_F(CellWriterTest, compositeAndOrdinaryBatchesShareTheShuffle) {
  // Aggregation-state rows and ordinary rows alternate: every switch writes
  // the other side out first, and the reader hands back CompositeRowVectors
  // and ordinary batches, each with its rows intact and in order.
  constexpr int32_t kPartitions = 3;
  struct Batch {
    bool composite;
    std::vector<int32_t> pids;
    RowVectorPtr data;
  };
  std::vector<Batch> batches;
  const std::vector<bool> kinds = {true, true, false, true, false, false, true};
  int64_t base = 0;
  for (const bool composite : kinds) {
    const int n = 2500;
    Batch batch{composite, std::vector<int32_t>(n), nullptr};
    for (int i = 0; i < n; ++i) {
      batch.pids[i] = (base + i * 7) % kPartitions;
    }
    batch.data = makeRowVector(
        {"k", "s"},
        {makeFlatVector<int64_t>(n, [&](auto row) { return base + row; }),
         makeFlatVector<std::string>(n, [&](auto row) {
           return std::string((base + row) % 37, 'a' + row % 26);
         })});
    base += n;
    batches.push_back(std::move(batch));
  }
  const auto rowType = asRowType(batches[0].data->type());
  // Per partition and kind, the expected (batch, row) sequence.
  std::vector<std::vector<std::pair<size_t, vector_size_t>>> expected[2];
  expected[0].resize(kPartitions);
  expected[1].resize(kPartitions);
  for (size_t b = 0; b < batches.size(); ++b) {
    for (vector_size_t row = 0; row < batches[b].data->size(); ++row) {
      expected[batches[b].composite][batches[b].pids[row]].emplace_back(b, row);
    }
  }
  auto codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});

  for (const bool celeborn : {false, true}) {
    SCOPED_TRACE(celeborn ? "celeborn" : "local");
    auto client = std::make_shared<RecordingRssClient>();
    auto options = celeborn ? makeCelebornOptions(kPartitions, client)
                            : makeOptions(kPartitions);
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    for (const auto& batch : batches) {
      RowVectorPtr input = batch.composite
          ? makeComposite(batch.pids, batch.data)
          : withPid(batch.pids, batch.data);
      ASSERT_TRUE(writer.split(input, 0).ok());
    }
    ASSERT_TRUE(writer.stop().ok());
    const auto& metrics = writer.metrics();
    int64_t lengthSum = 0;
    for (const auto length : metrics.partitionLengths) {
      lengthSum += length;
    }
    EXPECT_EQ(lengthSum, metrics.totalBytesWritten);
    EXPECT_GE(metrics.spillCount, 1) << "an ordinary window was sealed";
    EXPECT_GT(metrics.convertTime, 0);

    std::vector<std::vector<char>> streams(kPartitions);
    if (celeborn) {
      EXPECT_EQ(client->stops, 1);
      EXPECT_TRUE(readFile().empty());
      for (int32_t pid = 0; pid < kPartitions; ++pid) {
        for (const auto& blob : client->pushes[pid]) {
          streams[pid].insert(streams[pid].end(), blob.begin(), blob.end());
        }
        EXPECT_EQ(
            metrics.partitionLengths[pid],
            static_cast<int64_t>(streams[pid].size()));
      }
    } else {
      const auto file = readFile();
      ASSERT_EQ(static_cast<int64_t>(file.size()), metrics.totalBytesWritten);
      uint64_t offset = 0;
      for (int32_t pid = 0; pid < kPartitions; ++pid) {
        const auto length = metrics.partitionLengths[pid];
        streams[pid].assign(
            file.begin() + offset, file.begin() + offset + length);
        offset += length;
      }
    }

    for (int32_t pid = 0; pid < kPartitions; ++pid) {
      CellShuffleReader reader(
          std::make_shared<test::MemoryReaderStreamIterator>(
              std::vector<std::vector<char>>{streams[pid]}),
          rowType,
          codec.get(),
          arrow::default_memory_pool(),
          pool(),
          /*batchSize=*/1000,
          1 << 20);
      size_t next[2] = {0, 0};
      while (auto batch = reader.next()) {
        const bool composite = RowVector::isComposite(batch);
        auto rows = composite ? decodeCompositeRows(rowType, batch) : batch;
        if (composite) {
          EXPECT_LE(batch->size(), 1000);
        }
        for (vector_size_t row = 0; row < rows->size(); ++row) {
          auto& want = expected[composite][pid];
          ASSERT_LT(next[composite], want.size());
          const auto [b, sourceRow] = want[next[composite]++];
          EXPECT_TRUE(batches[b].data->equalValueAt(rows.get(), sourceRow, row))
              << "partition " << pid << (composite ? " composite" : "")
              << " row " << next[composite] - 1;
        }
      }
      EXPECT_EQ(next[0], expected[0][pid].size()) << "partition " << pid;
      EXPECT_EQ(next[1], expected[1][pid].size()) << "partition " << pid;
    }
    ::unlink(dataFile_.c_str());
  }
}

TEST_F(CellWriterTest, compositeReclaimWritesTheBufferedRows) {
  // While composite rows are held, a reclaim writes them out as blocks and
  // gives their memory back; only-composite output still reaches the data
  // file (local) or the pushes (Celeborn), and the attempt ends once.
  constexpr int32_t kPartitions = 2;
  const auto data = makeRowVector(
      {"k", "s"},
      {makeFlatVector<int64_t>(4000, [](auto row) { return row; }),
       makeFlatVector<std::string>(
           4000, [](auto row) { return std::string(500 + row % 1000, 'z'); })});
  std::vector<int32_t> pids(data->size());
  for (vector_size_t row = 0; row < data->size(); ++row) {
    pids[row] = row % kPartitions;
  }
  for (const bool celeborn : {false, true}) {
    SCOPED_TRACE(celeborn ? "celeborn" : "local");
    auto client = std::make_shared<RecordingRssClient>();
    auto options = celeborn ? makeCelebornOptions(kPartitions, client)
                            : makeOptions(kPartitions);
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    ASSERT_TRUE(writer.split(makeComposite(pids, data), 0).ok());
    EXPECT_EQ(client->numPushes, 0);
    int64_t freed = 0;
    ASSERT_TRUE(writer.reclaimFixedSize(1LL << 30, &freed).ok());
    EXPECT_GT(freed, 0);
    if (celeborn) {
      EXPECT_EQ(client->numPushes, kPartitions);
    }
    ASSERT_TRUE(writer.split(makeComposite(pids, data), 0).ok());
    ASSERT_TRUE(writer.stop().ok());
    const auto& metrics = writer.metrics();
    EXPECT_GT(metrics.totalBytesWritten, 0);
    EXPECT_GT(metrics.totalBytesEvicted, 0);
    EXPECT_GT(metrics.dataSize, 2 * data->size() * 4);
    int64_t lengthSum = 0;
    for (const auto length : metrics.partitionLengths) {
      lengthSum += length;
    }
    EXPECT_EQ(lengthSum, metrics.totalBytesWritten);
    if (celeborn) {
      EXPECT_EQ(client->stops, 1);
      EXPECT_EQ(client->numPushes, 2 * kPartitions);
    } else {
      EXPECT_EQ(
          static_cast<int64_t>(readFile().size()), metrics.totalBytesWritten);
    }
    ::unlink(dataFile_.c_str());
  }
}

TEST_F(CellWriterTest, compositeReaderRejectsAnUnknownBlockKind) {
  const auto rowType = ROW({"k"}, {BIGINT()});
  CellShuffleReader reader(
      std::make_shared<test::MemoryReaderStreamIterator>(
          std::vector<std::vector<char>>{{7, 0, 0, 0}}),
      rowType,
      nullptr,
      arrow::default_memory_pool(),
      pool(),
      1000,
      1 << 20);
  EXPECT_THROW(reader.next(), BoltException);
}

TEST_F(CellWriterTest, rawOutputsPlaceOpaqueBlocks) {
  // The writer-neutral outputs on their own, as a row-based writer would
  // use them: each spillRaw() call is one unit over all partitions.
  constexpr uint32_t kPartitions = 3;
  const auto spill = [&](RawPartitionOutput& output,
                         std::vector<std::vector<std::string>> blocks) {
    output.spillRaw([&](uint32_t pid, BufferPtr& block) {
      if (blocks[pid].empty()) {
        return false;
      }
      const auto bytes = blocks[pid].front();
      blocks[pid].erase(blocks[pid].begin());
      block = AlignedBuffer::allocate<char>(bytes.size(), pool());
      ::memcpy(block->asMutable<char>(), bytes.data(), bytes.size());
      return true;
    });
  };
  {
    auto options = makeOptions(kPartitions);
    auto output = RawPartitionOutput::create(options.partitionWriterOptions);
    spill(*output, {{"a0"}, {}, {"c0", "c1"}});
    spill(*output, {{"a1"}, {"b1"}, {}});
    ShuffleWriterMetrics metrics;
    output->finish(metrics);
    EXPECT_EQ(readFile(), "a0a1b1c0c1");
    EXPECT_EQ(metrics.partitionLengths, (std::vector<int64_t>{4, 2, 4}));
    EXPECT_EQ(metrics.totalBytesWritten, 10);
    EXPECT_EQ(metrics.totalBytesEvicted, 10);
    EXPECT_FALSE(spillFilesExist());
  }
  {
    auto client = std::make_shared<RecordingRssClient>();
    auto options = makeCelebornOptions(kPartitions, client);
    ::unlink(dataFile_.c_str());
    auto output = RawPartitionOutput::create(options.partitionWriterOptions);
    spill(*output, {{"a0"}, {}, {"c0", "c1"}});
    EXPECT_EQ(client->numPushes, 3);
    spill(*output, {{"a1"}, {"b1"}, {}});
    ShuffleWriterMetrics metrics;
    output->finish(metrics);
    EXPECT_EQ(client->stops, 1);
    EXPECT_EQ(client->pushes[0], (std::vector<std::string>{"a0", "a1"}));
    EXPECT_EQ(client->pushes[1], (std::vector<std::string>{"b1"}));
    EXPECT_EQ(client->pushes[2], (std::vector<std::string>{"c0", "c1"}));
    EXPECT_EQ(metrics.partitionLengths, (std::vector<int64_t>{4, 2, 4}));
    EXPECT_EQ(metrics.totalBytesWritten, 10);
    EXPECT_FALSE(std::filesystem::exists(dataFile_));
  }
}

TEST_F(CellWriterTest, rawRunsFollowEachPartitionsPayloads) {
  // Each spillRaw() call is one unit over all partitions, like a Run; the
  // Cell output's merge places every partition's raw blocks after its
  // payloads, in call order - also when no window ever existed.
  constexpr uint32_t kPartitions = 3;
  auto options = makeOptions(kPartitions);
  const auto layout = CellLayout::create(ROW({"v"}, {BIGINT()}));
  LocalRawOutput raw(options.partitionWriterOptions);
  LocalCellOutput output(
      raw,
      &layout,
      options.cellOptions,
      options.partitionWriterOptions,
      pool());
  const auto spill = [&](std::vector<std::vector<std::string>> blocks) {
    raw.spillRaw([&](uint32_t pid, BufferPtr& block) {
      if (blocks[pid].empty()) {
        return false;
      }
      const auto bytes = blocks[pid].front();
      blocks[pid].erase(blocks[pid].begin());
      block = AlignedBuffer::allocate<char>(bytes.size(), pool());
      ::memcpy(block->asMutable<char>(), bytes.data(), bytes.size());
      return true;
    });
  };
  spill({{"a0"}, {}, {"c0", "c1"}});
  spill({{"a1"}, {"b1"}, {}});

  const std::vector<uint32_t> noRows(kPartitions, 0);
  const std::vector<uint64_t> noBytes(kPartitions, 0);
  CellWindowInput in{};
  in.layout = &layout;
  in.rowCounts = noRows.data();
  in.variableBytes = noBytes.data();
  in.numPartitions = kPartitions;
  ShuffleWriterMetrics metrics;
  output.finalize(in, /*windowHasData=*/false, metrics);

  EXPECT_EQ(readFile(), "a0a1b1c0c1");
  EXPECT_EQ(metrics.partitionLengths, (std::vector<int64_t>{4, 2, 4}));
  EXPECT_EQ(metrics.totalBytesWritten, 10);
  EXPECT_EQ(metrics.totalBytesEvicted, 10);
  EXPECT_FALSE(spillFilesExist());

  // The merge ran once and sealed both outputs behind it.
  EXPECT_THROW(spill({{"late"}, {}, {}}), BoltException);
  EXPECT_THROW(raw.finish(metrics), BoltException);
}

TEST_F(CellWriterTest, mergeToDataFileTakesEachSpillerOnce) {
  auto options = makeOptions(1);
  LocalRawOutput raw(options.partitionWriterOptions);
  ShuffleWriterMetrics metrics;
  EXPECT_THROW(
      PartialSpiller::mergeToDataFile(
          options.partitionWriterOptions, {&raw, &raw}, metrics),
      BoltException);
}

TEST_F(CellWriterTest, shuffleSpillFileRefusesAppendsOnceSealed) {
  auto options = makeOptions(1);
  ShuffleSpillFile spill(options.partitionWriterOptions, "raw");
  EXPECT_EQ(spill.end(), 0);
  EXPECT_EQ(spill.append("abc", 3), 0);
  EXPECT_EQ(spill.append("de", 2), 3);
  spill.flush();
  char out[5];
  spill.read(0, out, 5);
  EXPECT_EQ(std::string(out, 5), "abcde");
  spill.seal();
  EXPECT_THROW(spill.append("f", 1), BoltException);
  spill.read(3, out, 2);
  EXPECT_EQ(std::string(out, 2), "de");
  EXPECT_THROW(spill.read(4, out, 2), BoltException);
  EXPECT_TRUE(spillFilesExist());
  spill.remove();
  EXPECT_FALSE(spillFilesExist());
}

TEST_F(CellWriterTest, shrinkIsSafeWhileTheOutputReadsCells) {
  // Reclaim may shrink idle chunks while the output is reading the cells:
  // only chunks without a live cell may go, and a seal (which recycles
  // every cell first) leaves them all idle.
  const auto rows = makeRowVector(
      {makeFlatVector<int64_t>(4096, [](auto row) { return row * 31; })});
  const auto layout = CellLayout::create(asRowType(rows->type()));
  ChunkAllocator allocator(pool(), 16 << 10, 1 << 10, []() {});
  RecordingCellOutput output;
  CellSplitter splitter(layout, 1, allocator, output, *pool());
  std::string reference;
  std::string scanned;
  const auto scan = [&](const CellWindowInput& in, std::string& into) {
    in.cells->scan(0, 0, [&](const char* data, uint32_t bytes) {
      into.append(data, bytes);
    });
  };
  output.onSpill = [&](const CellWindowInput& in) {
    scan(in, reference);
    allocator.shrink(); // all chunks hold live cells: nothing may move
    scan(in, scanned);
  };
  int64_t releasedAtSeal = 0;
  output.onSeal = [&](const CellWindowInput&) {
    releasedAtSeal = allocator.shrink();
  };
  std::vector<uint32_t> row2Partition(rows->size(), 0);
  std::vector<uint32_t> partition2RowCount{static_cast<uint32_t>(rows->size())};
  std::vector<DecodedVector> decoded(1);
  decoded[0].decode(*rows->childAt(0));
  BatchNullClass nullClass = BatchNullClass::kNoNulls;
  SplitBatch batch;
  batch.decoded = decoded.data();
  batch.row2Partition = row2Partition.data();
  batch.partition2RowCount = partition2RowCount.data();
  batch.nullClass = &nullClass;
  batch.numRows = rows->size();
  batch.rowIndexInPid = nullptr;
  splitter.split(batch);
  splitter.sealWindow();
  EXPECT_FALSE(reference.empty());
  EXPECT_EQ(scanned, reference);
  EXPECT_GT(releasedAtSeal, 0);
}

TEST_F(CellWriterTest, reclaimAfterStopIsHarmless) {
  auto options = makeOptions(2);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(4096, [](auto row) { return row; })});
  ASSERT_TRUE(
      writer.split(withPid(std::vector<int32_t>(4096, 1), data), 0).ok());
  ASSERT_TRUE(writer.stop().ok());
  int64_t actual = -1;
  ASSERT_TRUE(writer.reclaimFixedSize(1 << 30, &actual).ok());
  EXPECT_GE(actual, 0);
}

TEST_F(CellWriterTest, complexColumnsRoundTripThroughWriterAndReader) {
  // Callers hand logical rows to the writer and get logical rows back from
  // the reader; the flattening stays inside the Cell classes.
  constexpr int32_t kPartitions = 3;
  constexpr vector_size_t kRows = 3000;
  auto data = makeRowVector(
      {"id", "tags", "attrs", "nested"},
      {makeFlatVector<int64_t>(kRows, [](auto row) { return row; }),
       makeArrayVector<int32_t>(
           kRows,
           [](auto row) { return row % 4; },
           [](auto row) { return row; },
           [](auto row) { return row % 7 == 0; }),
       makeMapVector<int32_t, int64_t>(
           kRows,
           [](auto row) { return row % 3; },
           [](auto row) { return row; },
           [](auto row) { return row * 2; }),
       makeRowVector(
           {makeFlatVector<std::string>(
                kRows, [](auto row) { return std::string(row % 20, 'n'); }),
            makeFlatVector<bool>(kRows, [](auto row) { return row % 2; })},
           [](auto row) { return row % 11 == 0; })});
  std::vector<int32_t> pids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    pids[row] = row % kPartitions;
  }
  auto options = makeOptions(kPartitions);
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  ASSERT_TRUE(writer.split(withPid(pids, data), 0).ok());
  ASSERT_TRUE(writer.stop().ok());
  EXPECT_GT(writer.metrics().flattenTime, 0);
  EXPECT_EQ(writer.metrics().convertTime, 0);

  const auto file = readFile();
  auto codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
  uint64_t offset = 0;
  for (int32_t pid = 0; pid < kPartitions; ++pid) {
    const auto length = writer.metrics().partitionLengths[pid];
    auto streams = std::make_shared<test::MemoryReaderStreamIterator>(
        std::vector<std::vector<char>>{std::vector<char>(
            file.begin() + offset, file.begin() + offset + length)});
    offset += length;
    CellShuffleReader reader(
        streams,
        asRowType(data->type()),
        codec.get(),
        arrow::default_memory_pool(),
        pool(),
        4096,
        1 << 20);
    vector_size_t next = pid;
    while (auto batch = reader.next()) {
      ASSERT_TRUE(batch->type()->equivalent(*data->type()));
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        ASSERT_LT(next, kRows);
        EXPECT_TRUE(data->equalValueAt(batch.get(), next, row))
            << "partition " << pid << " row " << next;
        next += kPartitions;
      }
    }
    EXPECT_GE(next, kRows);
  }
}

TEST_F(CellWriterTest, readerStreamsPayloadsInBatches) {
  // One large payload per partition comes out over many batches of
  // batchSize rows; payloads near a batch's size straddle batches, and
  // many small ones fill one batch across their boundaries. Every column kind,
  // with nulls, inline and out-of-line strings, dictionary strings and an
  // all-null column.
  constexpr int32_t kPartitions = 2;
  constexpr vector_size_t kRows = 20'000;
  std::mt19937 rng(5);
  const auto data = makeRowVector(
      {makeFlatVector<int64_t>(
           kRows,
           [](auto row) { return row * 7919; },
           [](auto row) { return row % 13 == 0; }),
       makeFlatVector<int16_t>(kRows, [](auto row) { return row % 300; }),
       makeFlatVector<bool>(
           kRows,
           [](auto row) { return row % 3 == 0; },
           [](auto row) { return row % 7 == 0; }),
       makeFlatVector<double>(
           kRows,
           [](auto row) { return row * 0.5; },
           [](auto row) { return row % 5 == 0; }),
       makeFlatVector<Timestamp>(
           kRows,
           [](auto row) { return Timestamp(row, row * 1000); },
           [](auto row) { return row % 9 == 0; }),
       makeNullableStrings(kRows, rng),
       makeFlatVector<StringView>(
           kRows,
           [](auto row) {
             return StringView(
                 row % 3 == 0 ? "a dictionary value past inline" : "short");
           }),
       BaseVector::createNullConstant(BIGINT(), kRows, pool())});
  std::vector<int32_t> pids(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    pids[row] = row % kPartitions;
  }
  auto codec = createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
  for (const vector_size_t windowRows : {0, 2048, 7}) {
    SCOPED_TRACE(windowRows);
    auto options = makeOptions(kPartitions);
    std::vector<std::vector<int32_t>> pidSlices{pids};
    std::vector<RowVectorPtr> batches{data};
    if (windowRows != 0) {
      options.cellOptions.checkpointPartitionBytes = 1;
      std::tie(pidSlices, batches) = sliceBatches(data, pids, windowRows);
    }
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    for (size_t i = 0; i < batches.size(); ++i) {
      ASSERT_TRUE(writer.split(withPid(pidSlices[i], batches[i]), 0).ok());
    }
    ASSERT_TRUE(writer.stop().ok());
    // The dictionary probe needs a first batch of 1024 rows.
    if (windowRows == 0 || windowRows >= 1024) {
      EXPECT_GT(writer.metrics().dictionaryMatchedRows, 0);
    }

    const auto file = readFile();
    for (const int32_t batchSize : {333, 1000}) {
      SCOPED_TRACE(batchSize);
      uint64_t offset = 0;
      for (int32_t pid = 0; pid < kPartitions; ++pid) {
        const auto length = writer.metrics().partitionLengths[pid];
        if (windowRows == 0) {
          EXPECT_EQ(
              decodePartition(
                  CellLayout::create(asRowType(data->type())),
                  file,
                  offset,
                  length)
                  .size(),
              1);
        }
        CellShuffleReader reader(
            std::make_shared<test::MemoryReaderStreamIterator>(
                std::vector<std::vector<char>>{std::vector<char>(
                    file.begin() + offset, file.begin() + offset + length)}),
            asRowType(data->type()),
            codec.get(),
            arrow::default_memory_pool(),
            pool(),
            batchSize,
            1 << 30);
        offset += length;
        vector_size_t next = pid;
        bool sawShort = false;
        while (auto batch = reader.next()) {
          // Only the last batch of a partition may be short.
          EXPECT_FALSE(sawShort);
          ASSERT_LE(batch->size(), batchSize);
          sawShort = batch->size() < batchSize;
          for (vector_size_t row = 0; row < batch->size(); ++row) {
            ASSERT_LT(next, kRows);
            ASSERT_TRUE(data->equalValueAt(batch.get(), next, row))
                << "partition " << pid << " row " << next;
            next += kPartitions;
          }
        }
        EXPECT_EQ(next, kRows + pid);
      }
    }
    // A byte target at or below zero still reads every row, one append per
    // batch: at most one window's rows when the windows are small.
    for (const int64_t batchByteSize : {int64_t{0}, int64_t{-1}}) {
      SCOPED_TRACE(batchByteSize);
      uint64_t offset = 0;
      for (int32_t pid = 0; pid < kPartitions; ++pid) {
        const auto length = writer.metrics().partitionLengths[pid];
        CellShuffleReader reader(
            std::make_shared<test::MemoryReaderStreamIterator>(
                std::vector<std::vector<char>>{std::vector<char>(
                    file.begin() + offset, file.begin() + offset + length)}),
            asRowType(data->type()),
            codec.get(),
            arrow::default_memory_pool(),
            pool(),
            1000,
            batchByteSize);
        offset += length;
        vector_size_t next = pid;
        while (auto batch = reader.next()) {
          ASSERT_GT(batch->size(), 0);
          if (windowRows != 0) {
            ASSERT_LE(batch->size(), windowRows);
          }
          for (vector_size_t row = 0; row < batch->size(); ++row) {
            ASSERT_LT(next, kRows);
            ASSERT_TRUE(data->equalValueAt(batch.get(), next, row))
                << "partition " << pid << " row " << next;
            next += kPartitions;
          }
        }
        EXPECT_EQ(next, kRows + pid);
      }
    }
    ::unlink(dataFile_.c_str());
  }
}

TEST_F(CellWriterTest, reclaimMidStreamReleasesMemory) {
  constexpr int32_t kPartitions = 4;
  auto options = makeOptions(kPartitions);
  std::mt19937 rng(13);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  // Enough volume that the writer holds several chunks by the trigger
  // point; a tiny writer legitimately declines to reclaim (anti-churn
  // guard) and would make this test meaningless.
  for (int batch = 0; batch < 6; ++batch) {
    const int n = 8192;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = rng() % kPartitions;
    }
    auto values = makeFlatVector<int64_t>(
        n, [&](auto /*row*/) { return static_cast<int64_t>(rng()); });
    std::vector<std::string> storage(n);
    std::vector<std::optional<StringView>> views(n);
    for (int i = 0; i < n; ++i) {
      storage[i].assign(1024, static_cast<char>('a' + rng() % 26));
      views[i] = StringView(storage[i]);
    }
    auto strings = makeNullableFlatVector<StringView>(views);
    batches.push_back(makeRowVector({"v", "s"}, {values, strings}));
    pids.push_back(std::move(batchPids));
  }

  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  bool reclaimed = false;
  roundTrip(
      options,
      pids,
      batches,
      &writer,
      [&](CellShuffleWriter& w, uint32_t batch) {
        if (batch == 3) {
          int64_t actual = 0;
          ASSERT_TRUE(w.reclaimFixedSize(1 << 30, &actual).ok());
          reclaimed = actual > 0;
        }
      });
  EXPECT_TRUE(reclaimed) << "reclaim should have released chunk memory";
}

TEST_F(CellWriterTest, emptyPartitionsHaveZeroLength) {
  constexpr int32_t kPartitions = 8;
  const int n = 200;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = (i % 2) == 0 ? 2 : 5; // only partitions 2 and 5
  }
  auto values = makeFlatVector<int64_t>(n, [](auto row) { return row; });
  auto data = makeRowVector({"v"}, {values});
  CellShuffleWriter writer(
      makeOptions(kPartitions), pool(), arrow::default_memory_pool());
  roundTrip(makeOptions(kPartitions), {batchPids}, {data}, &writer);
  for (int32_t pid = 0; pid < kPartitions; ++pid) {
    if (pid != 2 && pid != 5) {
      EXPECT_EQ(writer.metrics().partitionLengths[pid], 0);
    } else {
      EXPECT_GT(writer.metrics().partitionLengths[pid], 0);
    }
  }
}

TEST_F(CellWriterTest, dictionaryInputAndLongStrings) {
  constexpr int32_t kPartitions = 4;
  const int n = 500;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = i % kPartitions;
  }
  // A dictionary-wrapped bigint column: the split must see through it
  // without flattening.
  auto base = makeFlatVector<int64_t>({10, 20, 30, 40, 50});
  auto indices = allocateIndices(n, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (int i = 0; i < n; ++i) {
    rawIndices[i] = i % 5;
  }
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, n, base);
  // Strings crossing the 64-byte cache line exercise the direct-write path.
  std::vector<std::string> storage(n);
  std::vector<StringView> views(n);
  for (int i = 0; i < n; ++i) {
    storage[i].assign((i % 7) * 33 + 1, static_cast<char>('a' + i % 26));
    views[i] = StringView(storage[i]);
  }
  auto strings = makeFlatVector<StringView>(views);
  roundTrip(
      makeOptions(kPartitions),
      {batchPids},
      {makeRowVector({"d", "s"}, {dictionary, strings})});
}

TEST_F(CellWriterTest, staleNullsBufferAndFlatAllNull) {
  constexpr int32_t kPartitions = 4;
  const int n = 600;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = i % kPartitions;
  }
  // Column a: a defensively allocated, all-set nulls buffer - mayHaveNulls()
  // is stale and the batch scan must classify it as no-nulls.
  auto stale = makeFlatVector<int64_t>(n, [](auto row) { return row * 7; });
  auto allSet = AlignedBuffer::allocate<bool>(n, pool());
  ::memset(allSet->asMutable<uint8_t>(), 0xFF, (n + 7) / 8);
  stale->setNulls(allSet);
  ASSERT_TRUE(stale->mayHaveNulls());
  // Column b: a flat vector whose every row is null (values are garbage).
  auto flatNull = makeFlatVector<int32_t>(n, [](auto row) { return row; });
  auto allClear = AlignedBuffer::allocate<bool>(n, pool());
  ::memset(allClear->asMutable<uint8_t>(), 0, (n + 7) / 8);
  flatNull->setNulls(allClear);
  roundTrip(
      makeOptions(kPartitions),
      {batchPids},
      {makeRowVector({"a", "b"}, {stale, flatNull})});
}

TEST_F(CellWriterTest, nullBodiesCompressAndRoundTrip) {
  // Sparse nulls: long null bodies of mostly zero bytes, compressed whole -
  // payloads of the window in memory, sealed ones and pushed ones alike.
  constexpr int32_t kPartitions = 2;
  const int n = 20000;
  std::vector<std::vector<int32_t>> pids(2);
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 2; ++batch) {
    pids[batch].resize(n);
    for (int i = 0; i < n; ++i) {
      pids[batch][i] = i % kPartitions;
    }
    batches.push_back(makeRowVector(
        {"v"},
        {makeFlatVector<int64_t>(
            n,
            [&](auto row) { return row * 7 + batch; },
            [](auto row) { return row % 301 == 0; })}));
  }
  // The first payload: its kind byte, then the header.
  const auto expectCompressedNulls = [](const std::string& bytes) {
    ASSERT_GT(bytes.size(), 1 + kPayloadFixedHeaderBytes);
    uint32_t stored;
    uint32_t decoded;
    ::memcpy(&stored, bytes.data() + 1 + 16, sizeof(stored));
    ::memcpy(&decoded, bytes.data() + 1 + 20, sizeof(decoded));
    EXPECT_GT(decoded, stored);
  };
  // Pushed first: the pushing writer expects no local data file.
  auto client = std::make_shared<RecordingRssClient>();
  auto pushing = makeCelebornOptions(kPartitions, client);
  pushing.partitionWriterOptions.compressionType =
      arrow::Compression::LZ4_FRAME;
  roundTrip(pushing, pids, batches);
  ASSERT_FALSE(client->pushes[0].empty());
  expectCompressedNulls(client->pushes[0][0]);

  roundTrip(makeOptions(kPartitions), pids, batches);
  expectCompressedNulls(readFile());

  auto sealing = makeOptions(kPartitions);
  sealing.cellOptions.checkpointPartitionBytes = 1;
  CellShuffleWriter sealed(sealing, pool(), arrow::default_memory_pool());
  roundTrip(sealing, pids, batches, &sealed);
  EXPECT_GT(sealed.metrics().spillCount, 0);
  expectCompressedNulls(readFile());
}

TEST_F(CellWriterTest, constantNullColumnRoundTrip) {
  constexpr int32_t kPartitions = 4;
  const int n = 700;
  std::vector<int32_t> batchPids(n);
  for (int i = 0; i < n; ++i) {
    batchPids[i] = i % kPartitions;
  }
  auto values = makeFlatVector<int64_t>(n, [](auto row) { return row * 3; });
  auto allNull = BaseVector::createNullConstant(BIGINT(), n, pool());
  roundTrip(
      makeOptions(kPartitions),
      {batchPids},
      {makeRowVector({"v", "dead"}, {values, allNull})});
}

TEST_F(CellWriterTest, columnsChangingNullShapeMidWindow) {
  constexpr int32_t kPartitions = 5;
  // Payloads closing by partition, then whole windows closing on the null
  // bitmaps: columns change state over partitions part-written, released
  // and fresh, and start over in each window.
  auto bySize = makeOptions(kPartitions);
  bySize.cellOptions.checkpointPartitionBytes = 8 << 10;
  auto byNulls = makeOptions(kPartitions);
  byNulls.cellOptions.nullMemLimitBytes = 1;
  for (const auto& options : {bySize, byNulls}) {
    std::mt19937 rng(23);
    std::vector<std::vector<int32_t>> pids;
    std::vector<RowVectorPtr> batches;
    for (int batch = 0; batch < 8; ++batch) {
      const int n = 400 + batch * 31;
      std::vector<int32_t> batchPids(n);
      for (int i = 0; i < n; ++i) {
        // Half the rows go to partition 0; partition 4 has none before
        // batch 3.
        batchPids[i] = rng() % 2 == 0 ? 0 : 1 + rng() % (batch < 3 ? 3 : 4);
      }
      auto values = makeFlatVector<int64_t>(
          n, [&](auto row) { return row * 13 + batch; });
      // Null throughout, then mixed: constant null, flat all-null, flat.
      VectorPtr a = batch < 2
          ? BaseVector::createNullConstant(BIGINT(), n, pool())
          : batch < 3 ? makeAllNullFlatVector<int64_t>(n)
                      : makeFlatVector<int64_t>(
                            n,
                            [&](auto row) { return row + batch; },
                            [](auto row) { return row % 3 == 0; });
      // Non-null throughout, then null throughout, then non-null again.
      VectorPtr b = batch == 4 || batch == 5
          ? BaseVector::createNullConstant(INTEGER(), n, pool())
          : makeFlatVector<int32_t>(n, [&](auto row) { return row - batch; });
      // Never anything but null.
      VectorPtr c = batch % 2 == 0
          ? BaseVector::createNullConstant(SMALLINT(), n, pool())
          : makeAllNullFlatVector<int16_t>(n);
      // Strings null throughout, then mixed.
      VectorPtr d = batch < 4
          ? BaseVector::createNullConstant(VARCHAR(), n, pool())
          : makeNullableStrings(n, rng);
      batches.push_back(
          makeRowVector({"v", "a", "b", "c", "d"}, {values, a, b, c, d}));
      pids.push_back(std::move(batchPids));
    }
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    EXPECT_GT(writer.metrics().spillCount, 0) << "expected sealed payloads";
  }
}

TEST_F(CellWriterTest, stringDictionaryRoundTripAndShrinksBytes) {
  constexpr int32_t kPartitions = 8;
  constexpr int kRows = 2000;
  // 12 distinct values, 54 serialized bytes: fits one 64-byte dictionary,
  // so the probe turns the column on. The second string column is
  // high-cardinality and must stay raw.
  const std::vector<std::string> vocab = {
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
      "",
      "kk"};
  std::mt19937 rng(42);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 4; ++b) {
    std::vector<int32_t> batchPids(kRows);
    std::vector<std::optional<StringView>> lowCard(kRows);
    std::vector<std::string> storage(kRows);
    std::vector<std::optional<StringView>> highCard(kRows);
    std::vector<int64_t> ids(kRows);
    for (int i = 0; i < kRows; ++i) {
      batchPids[i] = rng() % kPartitions;
      ids[i] = static_cast<int64_t>(rng());
      if (rng() % 7 == 0) {
        lowCard[i] = std::nullopt;
      } else {
        lowCard[i] = StringView(vocab[rng() % vocab.size()]);
      }
      storage[i] = "value-" + std::to_string(b) + "-" + std::to_string(rng());
      highCard[i] = StringView(storage[i]);
    }
    pids.push_back(std::move(batchPids));
    batches.push_back(makeRowVector(
        {"low", "id", "high"},
        {makeNullableFlatVector<StringView>(lowCard),
         makeFlatVector<int64_t>(ids),
         makeNullableFlatVector<StringView>(highCard)}));
  }

  // Uncompressed merge so the wire sizes compare byte for byte.
  int64_t bytesWith = 0;
  int64_t bytesWithout = 0;
  for (const bool enable : {true, false}) {
    auto options = makeOptions(kPartitions);
    options.partitionWriterOptions.compressionType =
        arrow::Compression::UNCOMPRESSED;
    options.cellOptions.enableStringDictionary = enable;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    (enable ? bytesWith : bytesWithout) = writer.metrics().totalBytesWritten;
    if (enable) {
      EXPECT_GT(writer.metrics().dictionaryMatchedRows, 0);
    } else {
      EXPECT_EQ(writer.metrics().dictionaryMatchedRows, 0);
    }
  }
  EXPECT_LT(bytesWith, bytesWithout);
}

TEST_F(CellWriterTest, dictionarySegmentsChainDemoteAndResetAcrossWindows) {
  constexpr int32_t kPartitions = 4;
  constexpr int kRows = 1500;
  // Both vocabularies serialize to 48 bytes (8 entries of 5 chars).
  // "same1"/"same2" share length and 4-byte prefix: the scan-key collision
  // path (prefix hit, suffix mismatch, rescan) is exercised.
  const std::vector<std::string> vocabA = {
      "north", "south", "east!", "west!", "same1", "same2", "left-", "right"};
  const std::vector<std::string> vocabB = {
      "ocean", "river", "lake!", "pond!", "sea--", "bay--", "gulf-", "creek"};
  std::mt19937 rng(7);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  const auto addBatch =
      [&](const std::function<std::optional<std::string>(int)>& gen) {
        std::vector<int32_t> batchPids(kRows);
        std::vector<std::optional<StringView>> col(kRows);
        std::vector<std::string> storage(kRows);
        for (int i = 0; i < kRows; ++i) {
          batchPids[i] = rng() % kPartitions;
          auto value = gen(i);
          if (!value.has_value()) {
            col[i] = std::nullopt;
          } else {
            storage[i] = std::move(*value);
            col[i] = StringView(storage[i]);
          }
        }
        pids.push_back(std::move(batchPids));
        batches.push_back(
            makeRowVector({"s"}, {makeNullableFlatVector<StringView>(col)}));
      };

  // Probe batch: vocabulary A with nulls -> dictionary on.
  addBatch([&](int) -> std::optional<std::string> {
    return rng() % 9 == 0 ? std::nullopt
                          : std::make_optional(vocabA[rng() % vocabA.size()]);
  });
  // Mid-batch vocabulary shift: the A segment closes with a high hit rate
  // and a successor segment chains (0xFE framing).
  addBatch([&](int i) -> std::optional<std::string> {
    return i < kRows / 2 ? vocabA[rng() % vocabA.size()]
                         : vocabB[rng() % vocabB.size()];
  });
  // Values too long for any entry force the per-partition fallback tail.
  addBatch([&](int i) -> std::optional<std::string> {
    return i % 5 == 0 ? std::string(80, 'x') : vocabB[rng() % vocabB.size()];
  });
  // High cardinality after a window reset: segments fill with no reuse and
  // the hit-rate rule demotes.
  addBatch([&](int) -> std::optional<std::string> {
    return "uid-" + std::to_string(rng());
  });
  // Back to a friendly vocabulary in a later window: dictionary re-engages.
  addBatch([&](int) -> std::optional<std::string> {
    return vocabA[rng() % vocabA.size()];
  });

  // Tiny window bound: several checkpoints, so per-window framing and the
  // dictionary state reset are exercised - on Celeborn, each window pushed.
  for (const bool celeborn : {false, true}) {
    SCOPED_TRACE(celeborn ? "celeborn" : "local");
    auto options = celeborn
        ? makeCelebornOptions(
              kPartitions, std::make_shared<RecordingRssClient>())
        : makeOptions(kPartitions);
    options.cellOptions.checkpointPartitionBytes = 2 << 10;
    CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
    roundTrip(options, pids, batches, &writer);
    // The vocabulary batches index through dictionaries; the long values
    // and the high-cardinality batch demote partitions into the tail.
    EXPECT_GT(writer.metrics().dictionaryMatchedRows, 0);
    EXPECT_GT(writer.metrics().dictionaryFallbackRows, 0);
    EXPECT_GT(writer.metrics().spillCount, 1);
    ::unlink(dataFile_.c_str());
  }
}

TEST_F(CellWriterTest, manyRunPayloadsRoundTrip) {
  // Wide schema (many streams) plus a tiny memory cap: runs spill every
  // few batches, so payloads carry many runs, each with its own header
  // (1 + 8 + 8 x streams bytes) and compression context, read back in
  // order.
  constexpr int32_t kPartitions = 64;
  constexpr int kColumns = 12;
  std::mt19937 rng(11);
  std::vector<std::vector<int32_t>> pids;
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 24; ++batch) {
    const int n = 1024;
    std::vector<int32_t> batchPids(n);
    for (int i = 0; i < n; ++i) {
      batchPids[i] = static_cast<int32_t>(rng() % kPartitions);
    }
    std::vector<std::string> names;
    std::vector<VectorPtr> columns;
    for (int c = 0; c < kColumns; ++c) {
      names.push_back("c" + std::to_string(c));
      columns.push_back(makeFlatVector<int64_t>(
          n, [&](auto /*row*/) { return static_cast<int64_t>(rng()); }));
    }
    batches.push_back(makeRowVector(names, columns));
    pids.push_back(std::move(batchPids));
  }

  auto options = makeOptions(kPartitions);
  options.cellOptions.cellMemoryCapBytes = 8 << 20;
  CellShuffleWriter writer(options, pool(), arrow::default_memory_pool());
  roundTrip(options, pids, batches, &writer);
  const auto& metrics = writer.metrics();
  EXPECT_GT(metrics.totalBytesEvicted, 0) << "expected spills";
  const auto layout = CellLayout::create(asRowType(batches[0]->type()));
  const auto file = readFile();
  uint32_t most = 0;
  uint64_t offset = 0;
  for (int32_t pid = 0; pid < kPartitions; ++pid) {
    for (const auto runs : payloadRunCounts(
             layout, file, offset, metrics.partitionLengths[pid])) {
      most = std::max(most, runs);
    }
    offset += metrics.partitionLengths[pid];
  }
  EXPECT_GE(most, 2);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
