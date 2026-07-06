/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <arrow/io/memory.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

constexpr int32_t kNumColumns = 2;

int64_t expectedValue(int64_t globalRow, int32_t col) {
  return globalRow * (col + 1);
}

// Two buffers per BIGINT column: validity bitmap (all-valid) + values seeded
// with the sentinel pattern from expectedValue().
std::vector<std::shared_ptr<arrow::Buffer>> makePayloadBuffers(
    int32_t payloadIdx,
    int32_t rowsPerPayload,
    arrow::MemoryPool* pool) {
  const int64_t valueBytes = rowsPerPayload * sizeof(int64_t);
  const int64_t validityBytes = arrow::bit_util::BytesForBits(rowsPerPayload);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int32_t c = 0; c < kNumColumns; ++c) {
    auto v = arrow::AllocateResizableBuffer(validityBytes, pool).ValueOrDie();
    std::memset(v->mutable_data(), 0xFF, validityBytes);
    buffers.push_back(std::move(v));

    auto d = arrow::AllocateResizableBuffer(valueBytes, pool).ValueOrDie();
    auto* values = reinterpret_cast<int64_t*>(d->mutable_data());
    for (int32_t r = 0; r < rowsPerPayload; ++r) {
      values[r] = expectedValue(payloadIdx * rowsPerPayload + r, c);
    }
    buffers.push_back(std::move(d));
  }
  return buffers;
}

std::shared_ptr<arrow::Buffer> buildStream(
    int32_t numPayloads,
    int32_t rowsPerPayload,
    const std::vector<bool>* isValidityBuffer,
    arrow::MemoryPool* pool) {
  auto stream =
      arrow::io::BufferOutputStream::Create(1 << 12, pool).ValueOrDie();
  for (int32_t p = 0; p < numPayloads; ++p) {
    auto payload = BlockPayload::fromBuffers(
                       Payload::Type::kUncompressed,
                       rowsPerPayload,
                       makePayloadBuffers(p, rowsPerPayload, pool),
                       isValidityBuffer,
                       pool,
                       /*codec=*/nullptr,
                       Payload::Mode::kBuffer,
                       /*hasComplexType=*/false)
                       .ValueOrDie();
    BOLT_CHECK(payload->serialize(stream.get()).ok(), "serialize failed");
  }
  return stream->Finish().ValueOrDie();
}

class BoltShuffleReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = arrow::default_memory_pool();
    boltPool_ = bytedance::bolt::memory::memoryManager()->addLeafPool();

    std::vector<std::string> names;
    std::vector<bytedance::bolt::TypePtr> types;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int32_t c = 0; c < kNumColumns; ++c) {
      names.push_back("c" + std::to_string(c));
      types.push_back(bytedance::bolt::BIGINT());
      fields.push_back(
          arrow::field("c" + std::to_string(c), arrow::int64(), true));
    }
    rowType_ = bytedance::bolt::ROW(std::move(names), std::move(types));
    schema_ = arrow::schema(fields);
    for (int32_t c = 0; c < kNumColumns; ++c) {
      isValidityBuffer_.push_back(true);
      isValidityBuffer_.push_back(false);
    }
  }

  std::unique_ptr<BoltColumnarBatchDeserializer> makeDeserializer(
      const std::shared_ptr<arrow::Buffer>& stream,
      int32_t batchSize,
      int64_t shuffleBatchByteSize) {
    factory_ = std::make_unique<BoltColumnarBatchDeserializerFactory>(
        schema_,
        /*codec=*/nullptr,
        rowType_,
        batchSize,
        shuffleBatchByteSize,
        pool_,
        boltPool_.get(),
        /*checksumEnabled=*/false);
    factory_->setpartitioningShortName("single");
    return factory_->createDeserializer(
        std::make_shared<arrow::io::BufferReader>(stream));
  }

  // Drain the deserializer, verifying every cell value and total row count.
  void drainAndVerify(BoltColumnarBatchDeserializer& d, int64_t expectedRows) {
    int64_t total = 0;
    while (auto batch = d.next()) {
      ASSERT_EQ(batch->childrenSize(), kNumColumns);
      for (int32_t c = 0; c < kNumColumns; ++c) {
        auto* col = batch->childAt(c)->asFlatVector<int64_t>();
        ASSERT_NE(col, nullptr);
        for (vector_size_t i = 0; i < batch->size(); ++i) {
          EXPECT_FALSE(col->isNullAt(i));
          EXPECT_EQ(col->valueAt(i), expectedValue(total + i, c));
        }
      }
      total += batch->size();
    }
    EXPECT_EQ(total, expectedRows);
  }

  arrow::MemoryPool* pool_{};
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltPool_;
  bytedance::bolt::RowTypePtr rowType_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<bool> isValidityBuffer_;
  std::unique_ptr<BoltColumnarBatchDeserializerFactory> factory_;
};

TEST_F(BoltShuffleReaderTest, SinglePayload) {
  constexpr int32_t kRows = 16;
  auto stream = buildStream(1, kRows, &isValidityBuffer_, pool_);
  auto d = makeDeserializer(stream, /*batchSize=*/1024, /*byteSize=*/1 << 20);
  drainAndVerify(*d, kRows);
}

TEST_F(BoltShuffleReaderTest, ManyPayloadsSingleBatch) {
  constexpr int32_t kPayloads = 200;
  constexpr int32_t kRows = 16;
  auto stream = buildStream(kPayloads, kRows, &isValidityBuffer_, pool_);
  auto d =
      makeDeserializer(stream, /*batchSize=*/100000, /*byteSize=*/1LL << 30);
  drainAndVerify(*d, kPayloads * kRows);
}

TEST_F(BoltShuffleReaderTest, ManyPayloadsMultipleBatches) {
  constexpr int32_t kPayloads = 100;
  constexpr int32_t kRows = 16;
  auto stream = buildStream(kPayloads, kRows, &isValidityBuffer_, pool_);
  auto d = makeDeserializer(stream, /*batchSize=*/160, /*byteSize=*/1LL << 30);
  drainAndVerify(*d, kPayloads * kRows);
}

TEST_F(BoltShuffleReaderTest, AllocationCountBounded) {
  constexpr int32_t kPayloads = 200;
  constexpr int32_t kRows = 16;
  constexpr int32_t kBuffersPerPayload = kNumColumns * 2;
  auto stream = buildStream(kPayloads, kRows, &isValidityBuffer_, pool_);
  auto d =
      makeDeserializer(stream, /*batchSize=*/100000, /*byteSize=*/1LL << 30);

  const int64_t before = pool_->num_allocations();
  drainAndVerify(*d, kPayloads * kRows);
  const int64_t allocs = pool_->num_allocations() - before;

  EXPECT_LT(allocs, kPayloads * kBuffersPerPayload * 1.2)
      << "saw " << allocs << " allocations; perf fix may have regressed";
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::test

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  bytedance::bolt::memory::MemoryManager::initialize({});
  return RUN_ALL_TESTS();
}
