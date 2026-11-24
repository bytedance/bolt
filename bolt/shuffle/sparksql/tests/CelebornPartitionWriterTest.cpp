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

#include <arrow/memory_pool.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "bolt/shuffle/sparksql/partition_writer/rss/CelebornPartitionWriter.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

class MockRssClient : public RssClient {
 public:
  int32_t pushPartitionData(int32_t, char*, int64_t size) override {
    pushedBytes_ += size;
    return static_cast<int32_t>(size);
  }

  void stop() override {}

  int64_t pushedBytes_ = 0;
};

// Regression test for the empty-partition core dump in
// BoltShuffleWriter::tryEvictComposite(). That path calls
// CelebornPartitionWriter::evict(rows, dummy, /*isCompositeVector=*/true) with
// an EMPTY `partitionBytes` vector. Before the fix the evict loop reset
// `partitionBytes[pid] = 0` unconditionally, which wrote out of bounds on the
// empty vector (null data()) and crashed with SIGSEGV at address 0.
TEST(CelebornPartitionWriterTest, EvictCompositeWithEmptyPartitionBytes) {
  PartitionWriterOptions options;
  auto client = std::make_shared<MockRssClient>();
  CelebornPartitionWriter writer(
      2, std::move(options), arrow::default_memory_pool(), client);

  // Two partitions, both with no buffered rows. The outer vector is what
  // `sortedRows_` looks like: sized to numPartitions_, inner vectors may be
  // empty for partitions with no rows.
  std::vector<std::vector<uint8_t*>> rows(2);
  std::vector<int64_t>
      partitionBytes; // mirrors the `dummy` in tryEvictComposite

  auto status = writer.evict(rows, partitionBytes, /*isCompositeVector=*/true);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // The composite path must not write to `partitionBytes`; it must stay empty.
  EXPECT_TRUE(partitionBytes.empty());
  // No rows were present, so nothing should have been pushed to the client.
  EXPECT_EQ(client->pushedBytes_, 0);
}

// Guards the non-composite path: the columnar evict still resets per-partition
// byte counters after writing, so the fix must not disable that reset.
TEST(CelebornPartitionWriterTest, EvictColumnarStillResetsPartitionBytes) {
  PartitionWriterOptions options;
  auto client = std::make_shared<MockRssClient>();
  CelebornPartitionWriter writer(
      2, std::move(options), arrow::default_memory_pool(), client);

  std::vector<std::vector<uint8_t*>> rows(2);
  std::vector<int64_t> partitionBytes = {100, 200};

  auto status = writer.evict(rows, partitionBytes, /*isCompositeVector=*/false);
  ASSERT_TRUE(status.ok()) << status.ToString();

  EXPECT_EQ(partitionBytes, (std::vector<int64_t>{0, 0}));
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::test
