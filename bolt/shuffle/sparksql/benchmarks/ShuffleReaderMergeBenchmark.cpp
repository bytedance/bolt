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

// Drives BoltColumnarBatchDeserializer::next() against a stream made of many
// small serialized BlockPayloads. The merge loop in BoltShuffleReader.cpp:540
// calls InMemoryPayload::merge() once per inbound payload; merge() resizes
// the source buffer with shrink_to_fit=true, so Arrow's PoolBuffer::Reserve
// reallocates exactly to the new size on each call (no geometric growth) and
// the result is one Reallocate per buffer per merge.

#include <arrow/io/memory.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/type/Type.h"

DEFINE_int32(num_columns, 4, "VARCHAR columns per payload");

using namespace bytedance::bolt::shuffle::sparksql;
using bytedance::bolt::ROW;

namespace {

constexpr int32_t kStringLength = 1024; // bytes per VARCHAR row
constexpr int32_t kBatchSize = 32 * 1024;
constexpr int64_t kShuffleBatchByteSize = 40LL << 20;
// Total rows processed per iteration is kept constant across variants so
// per-row cost is directly comparable. 524288 == 16 × kBatchSize, which
// divides 128 / 1024 / 8192 evenly.
constexpr int32_t kTotalRowsPerIter = kBatchSize;

bytedance::bolt::RowTypePtr makeRowType(int32_t numColumns) {
  std::vector<std::string> names;
  std::vector<bytedance::bolt::TypePtr> types;
  for (int32_t i = 0; i < numColumns; ++i) {
    names.push_back("c" + std::to_string(i));
    types.push_back(bytedance::bolt::VARCHAR());
  }
  return ROW(std::move(names), std::move(types));
}

std::shared_ptr<arrow::Schema> makeArrowSchema(int32_t numColumns) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (int32_t i = 0; i < numColumns; ++i) {
    fields.push_back(arrow::field(
        "c" + std::to_string(i), arrow::utf8(), /*nullable=*/true));
  }
  return arrow::schema(fields);
}

// Three buffers per VARCHAR column: validity bitmap, per-row lengths
// (uint32_t each — see BinaryArrayLengthBufferType), data.
std::vector<std::shared_ptr<arrow::Buffer>> makeColumnBuffers(
    uint32_t rowsPerPayload,
    int32_t numColumns,
    arrow::MemoryPool* pool,
    uint8_t fill) {
  const int64_t dataBytes =
      static_cast<int64_t>(rowsPerPayload) * kStringLength;
  const int64_t lengthsBytes =
      static_cast<int64_t>(rowsPerPayload) * sizeof(uint32_t);
  const int64_t validityBytes = arrow::bit_util::BytesForBits(rowsPerPayload);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  buffers.reserve(numColumns * 3);
  for (int32_t c = 0; c < numColumns; ++c) {
    auto v = arrow::AllocateResizableBuffer(validityBytes, pool).ValueOrDie();
    std::memset(v->mutable_data(), 0xFF, validityBytes);
    buffers.push_back(std::move(v));

    auto l = arrow::AllocateResizableBuffer(lengthsBytes, pool).ValueOrDie();
    auto* lengths = reinterpret_cast<uint32_t*>(l->mutable_data());
    for (uint32_t r = 0; r < rowsPerPayload; ++r) {
      lengths[r] = kStringLength;
    }
    buffers.push_back(std::move(l));

    auto d = arrow::AllocateResizableBuffer(dataBytes, pool).ValueOrDie();
    std::memset(d->mutable_data(), fill, dataBytes);
    buffers.push_back(std::move(d));
  }
  return buffers;
}

std::shared_ptr<arrow::Buffer> buildSerializedStream(
    uint32_t rowsPerPayload,
    int32_t numColumns,
    int32_t numPayloads,
    const std::vector<bool>* isValidityBuffer,
    arrow::MemoryPool* pool) {
  auto stream =
      arrow::io::BufferOutputStream::Create(1 << 20, pool).ValueOrDie();
  for (int32_t i = 0; i < numPayloads; ++i) {
    auto buffers = makeColumnBuffers(
        rowsPerPayload, numColumns, pool, static_cast<uint8_t>(i & 0xFF));
    auto payload = BlockPayload::fromBuffers(
                       Payload::Type::kUncompressed,
                       rowsPerPayload,
                       std::move(buffers),
                       isValidityBuffer,
                       pool,
                       /*codec=*/nullptr,
                       Payload::Mode::kBuffer,
                       /*hasComplexType=*/false)
                       .ValueOrDie();
    BOLT_CHECK(
        payload->serialize(stream.get()).ok(), "serialize failed");
  }
  return stream->Finish().ValueOrDie();
}

void runBenchmark(size_t iterations, uint32_t rowsPerPayload) {
  folly::BenchmarkSuspender suspender;
  BOLT_CHECK(
      kTotalRowsPerIter % rowsPerPayload == 0,
      "rows_per_payload must divide kTotalRowsPerIter");
  const int32_t numPayloads = kTotalRowsPerIter / rowsPerPayload;

  auto* arrowPool = arrow::default_memory_pool();
  auto boltPool = bytedance::bolt::memory::memoryManager()->addLeafPool();
  auto rowType = makeRowType(FLAGS_num_columns);
  auto schema = makeArrowSchema(FLAGS_num_columns);

  // Three buffers per VARCHAR column: validity bitmap, lengths, data.
  std::vector<bool> isValidityBuffer;
  for (int32_t c = 0; c < FLAGS_num_columns; ++c) {
    isValidityBuffer.push_back(true);
    isValidityBuffer.push_back(false);
    isValidityBuffer.push_back(false);
  }

  auto streamBuffer = buildSerializedStream(
      rowsPerPayload,
      FLAGS_num_columns,
      numPayloads,
      &isValidityBuffer,
      arrowPool);

  // Payloads are Type::kUncompressed, so the codec is never invoked during
  // deserialize; pass an empty shared_ptr.
  BoltColumnarBatchDeserializerFactory factory(
      schema,
      /*codec=*/nullptr,
      rowType,
      kBatchSize,
      kShuffleBatchByteSize,
      arrowPool,
      boltPool.get(),
      /*checksumEnabled=*/false);
  factory.setpartitioningShortName("single");

  const int64_t allocsBefore = arrowPool->num_allocations();

  suspender.dismiss();
  for (size_t iter = 0; iter < iterations; ++iter) {
    auto in = std::make_shared<arrow::io::BufferReader>(streamBuffer);
    auto deserializer = factory.createDeserializer(in);
    while (auto batch = deserializer->next()) {
      folly::doNotOptimizeAway(batch.get());
    }
  }
  suspender.rehire();

  const int64_t allocsDuringRun = arrowPool->num_allocations() - allocsBefore;
  const int64_t totalMergeOps =
      static_cast<int64_t>(iterations) * std::max(0, numPayloads - 1);

  LOG(INFO) << "ShuffleReaderMerge"
            << " rows_per_payload=" << rowsPerPayload
            << " num_payloads=" << numPayloads
            << " total_rows_per_iter=" << kTotalRowsPerIter
            << " num_columns=" << FLAGS_num_columns
            << " iterations=" << iterations
            << " arrow_allocations=" << allocsDuringRun
            << " allocations_per_merge_op="
            << (totalMergeOps > 0
                    ? static_cast<double>(allocsDuringRun) / totalMergeOps
                    : 0.0);
}

} // namespace

BENCHMARK(ShuffleReaderMerge_Small, n) {
  runBenchmark(n, 4);
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  bytedance::bolt::memory::MemoryManager::initialize({});

  std::cout << "total_rows_per_iter = " << kTotalRowsPerIter << std::endl;
  std::cout << "num_columns         = " << FLAGS_num_columns << std::endl;
  std::cout << "codec               = uncompressed" << std::endl;

  folly::runBenchmarks();
  return 0;
}
