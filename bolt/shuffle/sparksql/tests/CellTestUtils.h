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
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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
    return static_cast<int32_t>(size);
  }

  void stop() override {
    ++stops;
  }

  std::map<int32_t, std::vector<std::string>> pushes;
  int32_t stops{0};
};

/// LZ4 frames, as the writer and the reader use them.
inline std::unique_ptr<Codec> lz4Codec() {
  return createCodec(
      arrow::Compression::LZ4_FRAME,
      CodecOptions{CodecBackend::NONE, kDefaultCompressionLevel, true});
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
  if (rss) {
    partitionWriter.partitionWriterType = PartitionWriterType::kCeleborn;
    partitionWriter.rssClient = std::make_shared<RecordingRssClient>();
  }
  return options;
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
    const auto client = std::dynamic_pointer_cast<RecordingRssClient>(
        partitionWriter.rssClient);
    BOLT_CHECK_NOT_NULL(client);
    if (pushes != nullptr) {
      pushes->resize(partitions.size());
    }
    for (const auto& [pid, blobs] : client->pushes) {
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

/// Writes `input` through one CellShuffleWriter on `pool` and collects its
/// output: the data file's partitions, or the pushes the options'
/// RecordingRssClient took. Each split() takes `batchOf(batch)`, by default
/// the batch behind its pid column.
inline WrittenShuffle writeShuffle(
    const ShuffleWriterOptions& options,
    const ShuffleInput& input,
    memory::MemoryPool* pool,
    const std::function<RowVectorPtr(size_t)>& batchOf = nullptr) {
  WrittenShuffle written;
  {
    CellShuffleWriter writer(options, pool, arrow::default_memory_pool());
    for (size_t batch = 0; batch < input.data.size(); ++batch) {
      const auto rv = batchOf
          ? batchOf(batch)
          : withPid(input.pids[batch], input.data[batch], pool);
      const auto start = std::chrono::steady_clock::now();
      const auto status = writer.split(rv, 0);
      written.splitWallNs +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - start)
              .count();
      BOLT_CHECK(status.ok(), "{}", status.ToString());
    }
    const auto status = writer.stop();
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    written.metrics = writer.metrics();
  }
  if (options.partitionWriterOptions.partitionWriterType ==
      PartitionWriterType::kCeleborn) {
    const auto client = std::dynamic_pointer_cast<RecordingRssClient>(
        options.partitionWriterOptions.rssClient);
    BOLT_CHECK_NOT_NULL(client);
    BOLT_CHECK_EQ(client->stops, 1);
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

/// A CellShuffleReader of one partition's bytes. `codec` may be null when
/// nothing was compressed; `nativeNestedTypes` is the writer's option of
/// that name.
inline std::unique_ptr<CellShuffleReader> makeReader(
    const std::string& bytes,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool nativeNestedTypes = true) {
  return std::make_unique<CellShuffleReader>(
      std::make_shared<sparksql::test::MemoryReaderStreamIterator>(
          std::vector<std::vector<char>>{
              std::vector<char>(bytes.begin(), bytes.end())}),
      type,
      codec,
      arrow::default_memory_pool(),
      pool,
      batchSize,
      batchByteSize,
      nativeNestedTypes);
}

/// Every batch CellShuffleReader decodes from one partition's bytes, each
/// checked to be a well-formed vector; the arguments are makeReader()'s.
inline std::vector<RowVectorPtr> readPartition(
    const std::string& bytes,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool nativeNestedTypes = true) {
  const auto reader = makeReader(
      bytes, type, codec, pool, batchSize, batchByteSize, nativeNestedTypes);
  std::vector<RowVectorPtr> batches;
  while (auto batch = reader->next()) {
    // What a consumer that validates, slices or resizes the batch relies
    // on, the values aside: buffers covering every row, offsets in range.
    batch->validate({});
    batches.push_back(std::move(batch));
  }
  return batches;
}

/// Whether row `leftRow` of `left` and row `rightRow` of `right` hold the
/// same value, whatever their encodings: arrays and maps entry by entry in
/// their order, structs field by field. Unlike equalValueAt, which compares
/// maps as sets of entries, a map whose entries changed order differs.
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
          << source->toString(sourceRow) << ", got " << batch->toString(row);
    }
  }
  EXPECT_EQ(next, expected.size()) << "partition " << pid;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell::test
