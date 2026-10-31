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

#include <fstream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleWriter.h"
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
/// RecordingRssClient took.
inline WrittenShuffle writeShuffle(
    const ShuffleWriterOptions& options,
    const ShuffleInput& input,
    memory::MemoryPool* pool) {
  const auto numPartitions = options.partitionWriterOptions.numPartitions;
  WrittenShuffle written;
  {
    CellShuffleWriter writer(options, pool, arrow::default_memory_pool());
    for (size_t batch = 0; batch < input.data.size(); ++batch) {
      const auto status =
          writer.split(withPid(input.pids[batch], input.data[batch], pool), 0);
      BOLT_CHECK(status.ok(), "{}", status.ToString());
    }
    const auto status = writer.stop();
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    written.metrics = writer.metrics();
  }
  written.partitions.resize(numPartitions);
  if (options.partitionWriterOptions.partitionWriterType ==
      PartitionWriterType::kCeleborn) {
    const auto client = std::dynamic_pointer_cast<RecordingRssClient>(
        options.partitionWriterOptions.rssClient);
    BOLT_CHECK_NOT_NULL(client);
    BOLT_CHECK_EQ(client->stops, 1);
    written.pushes.resize(numPartitions);
    for (const auto& [pid, blobs] : client->pushes) {
      for (const auto& blob : blobs) {
        written.partitions[pid] += blob;
      }
      written.pushes[pid] = blobs;
    }
  } else {
    std::ifstream in(options.partitionWriterOptions.dataFile, std::ios::binary);
    const std::string file(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    uint64_t offset = 0;
    for (int32_t pid = 0; pid < numPartitions; ++pid) {
      const auto length =
          static_cast<uint64_t>(written.metrics.partitionLengths[pid]);
      BOLT_CHECK_LE(offset + length, file.size());
      written.partitions[pid] = file.substr(offset, length);
      offset += length;
    }
    BOLT_CHECK_EQ(offset, file.size());
  }
  for (int32_t pid = 0; pid < numPartitions; ++pid) {
    BOLT_CHECK_EQ(
        written.metrics.partitionLengths[pid],
        static_cast<int64_t>(written.partitions[pid].size()));
  }
  return written;
}

/// Every batch CellShuffleReader decodes from one partition's bytes.
/// `codec` may be null when nothing was compressed.
inline std::vector<RowVectorPtr> readPartition(
    const std::string& bytes,
    const RowTypePtr& type,
    Codec* codec,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize) {
  CellShuffleReader reader(
      std::make_shared<sparksql::test::MemoryReaderStreamIterator>(
          std::vector<std::vector<char>>{
              std::vector<char>(bytes.begin(), bytes.end())}),
      type,
      codec,
      arrow::default_memory_pool(),
      pool,
      batchSize,
      batchByteSize);
  std::vector<RowVectorPtr> batches;
  while (auto batch = reader.next()) {
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
