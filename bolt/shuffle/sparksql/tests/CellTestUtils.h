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

#pragma once

#include <gtest/gtest.h>

#include <unistd.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"
#include "bolt/shuffle/sparksql/tests/MemoryReaderStreamIterator.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

/// A Cell shuffle end to end: logical rows into CellShuffleWriter, each
/// partition's bytes - from the data file, or as pushed to an RSS client -
/// out through CellShuffleReader.
namespace bytedance::bolt::shuffle::sparksql::cell::test {

/// Keeps every push as its own blob, in push order.
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

/// The codec the reader decompresses `compression` with; null when
/// UNCOMPRESSED.
inline std::unique_ptr<Codec> codecOf(arrow::Compression::type compression) {
  if (compression == arrow::Compression::UNCOMPRESSED) {
    return nullptr;
  }
  return createCodec(
      compression,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
}

/// LZ4 frames, as the writer and the reader use them.
inline std::unique_ptr<Codec> lz4Codec() {
  return codecOf(arrow::Compression::LZ4_FRAME);
}

/// Removes the data file `dataFile` and the spill files beside it.
inline void removeShuffleFiles(const std::string& dataFile) {
  ::unlink(dataFile.c_str());
  ::unlink((dataFile + ".cell.spill").c_str());
  ::unlink((dataFile + ".raw.spill").c_str());
}

/// A hash shuffle of `numPartitions` partitions, compressed as
/// `compression`, into `dataFile` or, when `rss`, pushed to a
/// RecordingRssClient.
inline ShuffleWriterOptions shuffleOptions(
    const std::string& dataFile,
    int32_t numPartitions,
    arrow::Compression::type compression,
    bool rss) {
  ShuffleWriterOptions options;
  options.partitioning = Partitioning::kHash;
  auto& partitionWriter = options.partitionWriterOptions;
  partitionWriter.numPartitions = numPartitions;
  partitionWriter.dataFile = dataFile;
  partitionWriter.compressionType = compression;
  // No cell threshold: a test of it opts in, and the others see only the
  // spills of the memory they set.
  options.cellOptions.cellSpillBytesPerStream =
      std::numeric_limits<int64_t>::max();
  if (rss) {
    partitionWriter.partitionWriterType = PartitionWriterType::kCeleborn;
    partitionWriter.rssClient = std::make_shared<RecordingRssClient>();
  }
  return options;
}

/// The RecordingRssClient of options shuffleOptions() made with `rss`.
inline RecordingRssClient& rssClient(const ShuffleWriterOptions& options) {
  auto* client = dynamic_cast<RecordingRssClient*>(
      options.partitionWriterOptions.rssClient.get());
  BOLT_CHECK_NOT_NULL(client);
  return *client;
}

/// Each partition's bytes as its reader gets them: the ranges of the data
/// file that `partitionLengths` gives, or every push the options'
/// RecordingRssClient took, back to back; `pushes`, if given, gets those
/// pushes.
inline std::vector<std::string> partitionBytes(
    const ShuffleWriterOptions& options,
    const std::vector<int64_t>& partitionLengths,
    std::vector<std::vector<std::string>>* pushes = nullptr) {
  const auto& partitionWriter = options.partitionWriterOptions;
  std::vector<std::string> partitions(partitionWriter.numPartitions);
  if (partitionWriter.partitionWriterType == PartitionWriterType::kCeleborn) {
    const auto& client = rssClient(options);
    if (pushes != nullptr) {
      pushes->resize(partitions.size());
    }
    for (const auto& [pid, blobs] : client.pushes) {
      for (const auto& blob : blobs) {
        partitions[pid] += blob;
      }
      if (pushes != nullptr) {
        (*pushes)[pid] = blobs;
      }
    }
    return partitions;
  }
  std::ifstream in(partitionWriter.dataFile, std::ios::binary);
  const std::string file(
      (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  uint64_t offset = 0;
  for (size_t pid = 0; pid < partitions.size(); ++pid) {
    const auto length = static_cast<uint64_t>(partitionLengths[pid]);
    BOLT_CHECK_LE(offset + length, file.size());
    partitions[pid] = file.substr(offset, length);
    offset += length;
  }
  BOLT_CHECK_EQ(offset, file.size());
  return partitions;
}

/// A row of a vector that outlives the reference.
using RowRef = std::pair<const BaseVector*, vector_size_t>;

/// One shuffle's input: every batch's rows without the pid column, and
/// their partitions.
struct ShuffleInput {
  std::vector<RowVectorPtr> data;
  std::vector<std::vector<int32_t>> pids;

  /// Per partition, each row it receives, in order.
  std::vector<std::vector<RowRef>> rowsOf(int32_t numPartitions) const {
    std::vector<std::vector<RowRef>> rows(numPartitions);
    for (uint32_t batch = 0; batch < data.size(); ++batch) {
      for (vector_size_t row = 0; row < data[batch]->size(); ++row) {
        rows[pids[batch][row]].emplace_back(data[batch].get(), row);
      }
    }
    return rows;
  }
};

/// `batches`, row r of each going to partition r % `numPartitions`.
inline ShuffleInput roundRobin(
    std::vector<RowVectorPtr> batches,
    int32_t numPartitions) {
  ShuffleInput input;
  for (const auto& batch : batches) {
    auto& pids = input.pids.emplace_back(batch->size());
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      pids[row] = row % numPartitions;
    }
  }
  input.data = std::move(batches);
  return input;
}

/// Every row of `batches`, in order.
inline std::vector<RowRef> rowsIn(const std::vector<RowVectorPtr>& batches) {
  std::vector<RowRef> rows;
  for (const auto& batch : batches) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      rows.emplace_back(batch.get(), row);
    }
  }
  return rows;
}

/// What one writer lifecycle wrote: each partition's bytes as its reader
/// gets them and, from an RSS client, every push they arrived in.
struct WrittenShuffle {
  std::vector<std::string> partitions;
  std::vector<std::vector<std::string>> pushes;
  ShuffleWriterMetrics metrics;
  /// The time the split() calls took, together.
  int64_t splitWallNs{0};
};

/// `data` behind a pid column of `pids`.
inline RowVectorPtr withPid(
    const std::vector<int32_t>& pids,
    const RowVectorPtr& data,
    memory::MemoryPool* pool) {
  BOLT_CHECK_EQ(pids.size(), static_cast<size_t>(data->size()));
  auto pid =
      BaseVector::create<FlatVector<int32_t>>(INTEGER(), data->size(), pool);
  for (vector_size_t row = 0; row < data->size(); ++row) {
    pid->set(row, pids[row]);
  }
  std::vector<std::string> names{"pid"};
  std::vector<TypePtr> types{INTEGER()};
  std::vector<VectorPtr> children{std::move(pid)};
  const auto& rowType = data->type()->asRow();
  for (uint32_t i = 0; i < rowType.size(); ++i) {
    names.push_back(rowType.nameOf(i));
    types.push_back(rowType.childAt(i));
    children.push_back(data->childAt(i));
  }
  return std::make_shared<RowVector>(
      pool,
      ROW(std::move(names), std::move(types)),
      nullptr,
      data->size(),
      std::move(children));
}

/// Writes `input` through `writer`, made of `options`, and collects its
/// output: the data file's partitions, or the pushes the options'
/// RecordingRssClient took. Each split() takes `batchOf(batch)`, by default
/// the batch behind its pid column; `afterSplit(batch)` follows it.
inline WrittenShuffle writeShuffle(
    ShuffleWriter& writer,
    const ShuffleWriterOptions& options,
    const ShuffleInput& input,
    memory::MemoryPool* pool,
    const std::function<RowVectorPtr(size_t)>& batchOf = nullptr,
    const std::function<void(size_t)>& afterSplit = nullptr) {
  WrittenShuffle written;
  for (size_t batch = 0; batch < input.data.size(); ++batch) {
    const auto rv = batchOf
        ? batchOf(batch)
        : withPid(input.pids[batch], input.data[batch], pool);
    const auto start = std::chrono::steady_clock::now();
    const auto status = writer.split(rv, 0);
    written.splitWallNs += std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now() - start)
                               .count();
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    if (afterSplit) {
      afterSplit(batch);
    }
  }
  const auto status = writer.stop();
  BOLT_CHECK(status.ok(), "{}", status.ToString());
  written.metrics = writer.metrics();
  if (options.partitionWriterOptions.partitionWriterType ==
      PartitionWriterType::kCeleborn) {
    BOLT_CHECK_EQ(rssClient(options).stops, 1);
  }
  written.partitions = partitionBytes(
      options, written.metrics.partitionLengths, &written.pushes);
  for (size_t pid = 0; pid < written.partitions.size(); ++pid) {
    BOLT_CHECK_EQ(
        written.metrics.partitionLengths[pid],
        static_cast<int64_t>(written.partitions[pid].size()));
  }
  return written;
}

/// writeShuffle() through a CellShuffleWriter of `options` on `pool`.
inline WrittenShuffle writeShuffle(
    const ShuffleWriterOptions& options,
    const ShuffleInput& input,
    memory::MemoryPool* pool,
    const std::function<RowVectorPtr(size_t)>& batchOf = nullptr) {
  CellShuffleWriter writer(options, pool, arrow::default_memory_pool());
  return writeShuffle(writer, options, input, pool, batchOf);
}

/// Partition `pid` of `written` as its reader gets it: the data file's
/// range, or, from an RSS client, each push as a stream of its own - a
/// payload never spans two pushes.
inline std::vector<std::string> partitionStreams(
    const WrittenShuffle& written,
    int32_t pid) {
  if (written.pushes.empty()) {
    return {written.partitions[pid]};
  }
  return written.pushes[pid];
}

/// `data` as HashAggregation composite output behind a pid column of
/// `pids`: each row [size i32][CompactRow bytes], as serialized
/// aggregation state is.
inline std::shared_ptr<CompositeRowVector> makeComposite(
    const std::vector<int32_t>& pids,
    const RowVectorPtr& data,
    memory::MemoryPool* pool) {
  ShuffleColumnarToRowConverter converter(
      asRowType(data->type()), pool, row::RowFormat::COMPACT);
  auto stats =
      converter.getWithStats(data, std::numeric_limits<int64_t>::max());
  std::vector<std::vector<uint8_t*>> rows(1);
  std::vector<uint32_t> partitions(data->size(), 0);
  std::vector<int64_t> partitionBytes(1, 0);
  converter.convert(stats, partitions, rows, partitionBytes);
  const auto ordinary = withPid(pids, data, pool);
  auto children = ordinary->children();
  auto composite = std::make_shared<CompositeRowVector>(
      ordinary->type(),
      data->size(),
      pool,
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

/// The rows of a CompositeRowVector the reader returned, decoded as `type`.
inline RowVectorPtr compositeRows(
    const RowTypePtr& type,
    const RowVectorPtr& batch,
    memory::MemoryPool* pool) {
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
  ShuffleRowToColumnarConverter converter(type, pool, row::RowFormat::COMPACT);
  return converter.convert(rows);
}

/// A CellShuffleReader of one partition's `streams`, read in turn. `codec`
/// may be null when nothing was compressed; `nativeNestedTypes` is the
/// writer's option of that name.
inline std::unique_ptr<CellShuffleReader> makeReader(
    const std::vector<std::string>& streams,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool nativeNestedTypes = true) {
  std::vector<std::vector<char>> bytes;
  for (const auto& stream : streams) {
    bytes.emplace_back(stream.begin(), stream.end());
  }
  return std::make_unique<CellShuffleReader>(
      std::make_shared<sparksql::test::MemoryReaderStreamIterator>(
          std::move(bytes)),
      type,
      codec,
      arrow::default_memory_pool(),
      pool,
      batchSize,
      batchByteSize,
      nativeNestedTypes);
}

/// makeReader() of one stream, `bytes`.
inline std::unique_ptr<CellShuffleReader> makeReader(
    const std::string& bytes,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool nativeNestedTypes = true) {
  return makeReader(
      std::vector<std::string>{bytes},
      type,
      codec,
      pool,
      batchSize,
      batchByteSize,
      nativeNestedTypes);
}

/// Every batch CellShuffleReader decodes from one partition's `streams`,
/// each checked to be a well-formed vector; the arguments are
/// makeReader()'s.
inline std::vector<RowVectorPtr> readPartition(
    const std::vector<std::string>& streams,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool nativeNestedTypes = true) {
  const auto reader = makeReader(
      streams, type, codec, pool, batchSize, batchByteSize, nativeNestedTypes);
  std::vector<RowVectorPtr> batches;
  while (auto batch = reader->next()) {
    // What a consumer that validates, slices or resizes the batch relies
    // on, the values aside: buffers covering every row, offsets in range.
    batch->validate({});
    batches.push_back(std::move(batch));
  }
  return batches;
}

/// readPartition() of one stream, `bytes`.
inline std::vector<RowVectorPtr> readPartition(
    const std::string& bytes,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool nativeNestedTypes = true) {
  return readPartition(
      std::vector<std::string>{bytes},
      type,
      codec,
      pool,
      batchSize,
      batchByteSize,
      nativeNestedTypes);
}

/// The rows of every batch CellShuffleReader returns from one partition's
/// bytes; the arguments are makeReader()'s. With a byte target at or below
/// zero and a batch size above every payload's rows, a batch is a payload.
inline std::vector<vector_size_t> readerBatchSizes(
    const std::string& bytes,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize) {
  std::vector<vector_size_t> sizes;
  for (const auto& batch :
       readPartition(bytes, type, codec, pool, batchSize, batchByteSize)) {
    sizes.push_back(batch->size());
  }
  return sizes;
}

/// Whether row `leftRow` of `left` and row `rightRow` of `right` hold the
/// same value, whatever their encodings: arrays element by element and maps
/// key-value pair by pair, in their order, structs field by field. Unlike
/// equalValueAt, which compares maps as sets of pairs, a map whose pairs
/// changed order differs.
inline bool sameValue(
    const BaseVector& left,
    vector_size_t leftRow,
    const BaseVector& right,
    vector_size_t rightRow) {
  const bool isNull = left.isNullAt(leftRow);
  if (isNull != right.isNullAt(rightRow)) {
    return false;
  }
  if (isNull) {
    return true;
  }
  const auto* leftBase = left.wrappedVector();
  const auto* rightBase = right.wrappedVector();
  const auto leftIndex = left.wrappedIndex(leftRow);
  const auto rightIndex = right.wrappedIndex(rightRow);
  if (leftBase->typeKind() != rightBase->typeKind()) {
    return false;
  }
  // Entries [leftOffset, +size) of `leftValues` against those of
  // `rightValues` from `rightOffset`.
  const auto sameEntries = [](const BaseVector& leftValues,
                              vector_size_t leftOffset,
                              const BaseVector& rightValues,
                              vector_size_t rightOffset,
                              vector_size_t size) {
    for (vector_size_t i = 0; i < size; ++i) {
      if (!sameValue(
              leftValues, leftOffset + i, rightValues, rightOffset + i)) {
        return false;
      }
    }
    return true;
  };
  switch (leftBase->typeKind()) {
    case TypeKind::ARRAY: {
      const auto* l = leftBase->as<ArrayVector>();
      const auto* r = rightBase->as<ArrayVector>();
      BOLT_CHECK(l != nullptr && r != nullptr);
      const auto size = l->sizeAt(leftIndex);
      return size == r->sizeAt(rightIndex) &&
          sameEntries(
                 *l->elements(),
                 l->offsetAt(leftIndex),
                 *r->elements(),
                 r->offsetAt(rightIndex),
                 size);
    }
    case TypeKind::MAP: {
      const auto* l = leftBase->as<MapVector>();
      const auto* r = rightBase->as<MapVector>();
      BOLT_CHECK(l != nullptr && r != nullptr);
      const auto size = l->sizeAt(leftIndex);
      return size == r->sizeAt(rightIndex) &&
          sameEntries(
                 *l->mapKeys(),
                 l->offsetAt(leftIndex),
                 *r->mapKeys(),
                 r->offsetAt(rightIndex),
                 size) &&
          sameEntries(
                 *l->mapValues(),
                 l->offsetAt(leftIndex),
                 *r->mapValues(),
                 r->offsetAt(rightIndex),
                 size);
    }
    case TypeKind::ROW: {
      const auto* l = leftBase->as<RowVector>();
      const auto* r = rightBase->as<RowVector>();
      BOLT_CHECK(l != nullptr && r != nullptr);
      if (l->childrenSize() != r->childrenSize()) {
        return false;
      }
      for (column_index_t i = 0; i < l->childrenSize(); ++i) {
        if (!sameValue(*l->childAt(i), leftIndex, *r->childAt(i), rightIndex)) {
          return false;
        }
      }
      return true;
    }
    default:
      return leftBase->equalValueAt(rightBase, leftIndex, rightIndex);
  }
}

/// Row `row` of `vector` as text, cut short past 1000 characters: a row may
/// hold megabytes.
inline std::string rowText(const BaseVector& vector, vector_size_t row) {
  constexpr size_t kMaxChars = 1000;
  auto text = vector.toString(row);
  if (text.size() > kMaxChars) {
    text.resize(kMaxChars);
    text += "...";
  }
  return text;
}

/// Expects `batches`, read back from partition `pid`, to hold the rows
/// `expected`, in order, in batches of at most `batchSize` rows.
inline void expectPartitionRows(
    const std::vector<RowRef>& expected,
    const std::vector<RowVectorPtr>& batches,
    int32_t pid,
    int32_t batchSize) {
  size_t next = 0;
  for (const auto& batch : batches) {
    ASSERT_GT(batch->size(), 0) << "partition " << pid;
    ASSERT_LE(batch->size(), batchSize) << "partition " << pid;
    ASSERT_LT(next, expected.size()) << "partition " << pid;
    ASSERT_TRUE(batch->type()->equivalent(*expected[next].first->type()))
        << "partition " << pid << ": " << batch->type()->toString();
    for (vector_size_t row = 0; row < batch->size(); ++row, ++next) {
      ASSERT_LT(next, expected.size()) << "partition " << pid;
      const auto [source, sourceRow] = expected[next];
      ASSERT_TRUE(sameValue(*source, sourceRow, *batch, row))
          << "partition " << pid << " row " << next << ": expected "
          << rowText(*source, sourceRow) << ", got " << rowText(*batch, row);
    }
  }
  EXPECT_EQ(next, expected.size()) << "partition " << pid;
}

/// One way to read a partition back: rows and bytes per batch.
struct ReadShape {
  int32_t batchSize;
  int64_t batchByteSize;
};

/// Whole batches, and small ones that a byte budget cuts.
inline const std::vector<ReadShape> kReadShapes{{4096, 1 << 20}, {7, 100}};

/// Expects every partition `written` holds, compressed as `compression`, to
/// read back at `shape` the rows `input` sent it, in order.
inline void expectReadsBack(
    const ShuffleInput& input,
    const WrittenShuffle& written,
    arrow::Compression::type compression,
    memory::MemoryPool* pool,
    const ReadShape& shape = kReadShapes[0],
    bool nativeNestedTypes = true) {
  const auto type = asRowType(input.data[0]->type());
  const auto numPartitions = static_cast<int32_t>(written.partitions.size());
  const auto rows = input.rowsOf(numPartitions);
  const auto codec = codecOf(compression);
  for (int32_t pid = 0; pid < numPartitions; ++pid) {
    expectPartitionRows(
        rows[pid],
        readPartition(
            partitionStreams(written, pid),
            type,
            codec.get(),
            pool,
            shape.batchSize,
            shape.batchByteSize,
            nativeNestedTypes),
        pid,
        shape.batchSize);
    if (::testing::Test::HasFatalFailure()) {
      return;
    }
  }
}

/// Whether some column of `type` is an ARRAY, MAP or ROW.
inline bool hasComplexColumns(const RowTypePtr& type) {
  for (const auto& child : type->children()) {
    if (child->kind() == TypeKind::ARRAY || child->kind() == TypeKind::MAP ||
        child->kind() == TypeKind::ROW) {
      return true;
    }
  }
  return false;
}

/// Expects what a nested layout reads back below a null struct of `vector`:
/// every field null there, and every container among them empty.
inline void expectFieldsNullUnderNullStructs(const BaseVector& vector) {
  if (const auto* row = vector.as<RowVector>()) {
    for (const auto& child : row->children()) {
      const auto* containers = child->as<ArrayVectorBase>();
      for (vector_size_t i = 0; i < row->size(); ++i) {
        if (row->isNullAt(i)) {
          ASSERT_TRUE(child->isNullAt(i)) << "row " << i;
          ASSERT_TRUE(containers == nullptr || containers->sizeAt(i) == 0);
        }
      }
      expectFieldsNullUnderNullStructs(*child);
    }
  } else if (const auto* array = vector.as<ArrayVector>()) {
    expectFieldsNullUnderNullStructs(*array->elements());
  } else if (const auto* map = vector.as<MapVector>()) {
    expectFieldsNullUnderNullStructs(*map->mapKeys());
    expectFieldsNullUnderNullStructs(*map->mapValues());
  }
}

/// The name of `compression` in test traces.
inline const char* codecName(arrow::Compression::type compression) {
  switch (compression) {
    case arrow::Compression::UNCOMPRESSED:
      return "none";
    case arrow::Compression::LZ4_FRAME:
      return "lz4";
    case arrow::Compression::ZSTD:
      return "zstd";
    case arrow::Compression::GZIP:
      return "gzip";
    default:
      return "other";
  }
}

/// Where a round trip writes, and with which codec.
struct ShuffleOutput {
  bool rss;
  arrow::Compression::type compression;

  std::string toString() const {
    return fmt::format("{} {}", rss ? "rss" : "local", codecName(compression));
  }
};

/// Into the data file uncompressed, and pushed to an RSS client with LZ4.
inline const std::vector<ShuffleOutput> kShuffleOutputs{
    {false, arrow::Compression::UNCOMPRESSED},
    {true, arrow::Compression::LZ4_FRAME}};

/// Writes `input` once per output of `outputs` and reads every partition
/// back at every read shape (an RSS partition push by push), expecting the
/// input's rows in order. Also expects a schema that takes a nested layout
/// to report flattenTime, which holds the shredding, any other complex
/// schema to report convertTime, every field null below a null struct, and
/// an uncompressed output's raw lengths to be its lengths. `tune` adjusts
/// each writer, `check` sees what it wrote. Returns what each output
/// wrote.
inline std::vector<WrittenShuffle> roundTrip(
    const ShuffleInput& input,
    int32_t numPartitions,
    const std::string& dataFile,
    memory::MemoryPool* pool,
    const std::function<void(CellShuffleOptions&, bool rss)>& tune = nullptr,
    const std::function<void(const WrittenShuffle&, bool rss)>& check = nullptr,
    const std::vector<ReadShape>& readShapes = kReadShapes,
    const std::vector<ShuffleOutput>& outputs = kShuffleOutputs) {
  const auto rowType = asRowType(input.data[0]->type());
  const auto rows = input.rowsOf(numPartitions);
  // Every writer first, so that the first one sees the input's lazy
  // vectors unloaded.
  std::vector<WrittenShuffle> written;
  std::vector<bool> native;
  for (const auto& output : outputs) {
    SCOPED_TRACE(output.toString());
    removeShuffleFiles(dataFile);
    auto options =
        shuffleOptions(dataFile, numPartitions, output.compression, output.rss);
    if (tune) {
      tune(options.cellOptions, output.rss);
    }
    written.push_back(writeShuffle(options, input, pool));
    native.push_back(options.cellOptions.nativeNestedTypes);
    const auto& metrics = written.back().metrics;
    if (native.back() && CellNestedLayout::supports(rowType)) {
      EXPECT_GT(metrics.flattenTime, 0);
      EXPECT_EQ(metrics.convertTime, 0);
    } else if (hasComplexColumns(rowType)) {
      EXPECT_GT(metrics.convertTime, 0);
      EXPECT_EQ(metrics.flattenTime, 0);
    }
    if (output.compression == arrow::Compression::UNCOMPRESSED) {
      EXPECT_EQ(metrics.rawPartitionLengths, metrics.partitionLengths);
    }
    if (check) {
      check(written.back(), output.rss);
    }
  }
  for (size_t i = 0; i < outputs.size(); ++i) {
    SCOPED_TRACE(outputs[i].toString());
    const bool nested = native[i] && CellNestedLayout::supports(rowType);
    const auto codec = codecOf(outputs[i].compression);
    for (const auto& shape : readShapes) {
      SCOPED_TRACE(fmt::format(
          "batches of {} rows, {} bytes",
          shape.batchSize,
          shape.batchByteSize));
      for (int32_t pid = 0; pid < numPartitions; ++pid) {
        const auto batches = readPartition(
            partitionStreams(written[i], pid),
            rowType,
            codec.get(),
            pool,
            shape.batchSize,
            shape.batchByteSize,
            native[i]);
        expectPartitionRows(rows[pid], batches, pid, shape.batchSize);
        if (nested) {
          for (const auto& batch : batches) {
            expectFieldsNullUnderNullStructs(*batch);
          }
        }
        if (::testing::Test::HasFatalFailure()) {
          return written;
        }
      }
    }
  }
  return written;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell::test
