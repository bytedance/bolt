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

#include <arrow/io/api.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "bolt/shuffle/sparksql/partition_writer/rss/CelebornPartitionWriter.h"
#include "bolt/shuffle/sparksql/tests/MockRssClient.h"

namespace bytedance::bolt::shuffle::sparksql::test {

class CelebornPartitionWriterTest : public testing::TestWithParam<bool> {};

TEST_P(CelebornPartitionWriterTest, evictRows) {
  const bool isComposite = GetParam();
  auto client = std::make_shared<MockRssClient>();
  PartitionWriterOptions options;
  CelebornPartitionWriter writer(
      3, options, arrow::default_memory_pool(), client);
  std::array<int32_t, 3> firstRow{8, 11, 22};
  std::array<int32_t, 3> secondRow{8, 33, 44};
  std::vector<std::vector<uint8_t*>> rows(3);
  rows[1] = {
      reinterpret_cast<uint8_t*>(firstRow.data()),
      reinterpret_cast<uint8_t*>(secondRow.data())};
  std::vector<int64_t> partitionBytes;
  if (!isComposite) {
    partitionBytes = {0, 24, 0};
  }

  ASSERT_TRUE(writer.evict(rows, partitionBytes, isComposite).ok());
  EXPECT_TRUE(rows[1].empty());
  if (isComposite) {
    EXPECT_TRUE(partitionBytes.empty());
  } else {
    EXPECT_EQ(partitionBytes, (std::vector<int64_t>{0, 0, 0}));
  }
  ShuffleWriterMetrics metrics;
  ASSERT_TRUE(writer.stop(&metrics).ok());
  EXPECT_EQ(metrics.rawPartitionLengths, (std::vector<int64_t>{0, 24, 0}));
  EXPECT_EQ(metrics.partitionLengths[0], 0);
  EXPECT_EQ(metrics.partitionLengths[2], 0);
  EXPECT_GT(metrics.partitionLengths[1], 0);

  const auto& data = client->getData().at(1);
  arrow::io::BufferReader input(arrow::Buffer::Wrap(data));
  AdaptiveParallelZstdCodec codec(
      options.compressionLevel, false, arrow::default_memory_pool(), false);
  std::array<uint8_t, 1024> output;
  std::vector<std::string_view> decodedRows;
  int32_t offset = 0;
  int32_t remaining = 0;
  bool eof = false;
  bool layoutEnd = false;
  RowVectorLayout layout = RowVectorLayout::kInvalid;
  uint64_t decompressTime = 0;
  ASSERT_TRUE(RowBlockPayload::deserialize(
                  &input,
                  output.data(),
                  output.size(),
                  offset,
                  &codec,
                  decodedRows,
                  remaining,
                  eof,
                  layoutEnd,
                  layout,
                  decompressTime)
                  .ok());
  ASSERT_EQ(decodedRows.size(), 2);
  EXPECT_EQ(
      decodedRows[0],
      std::string_view(reinterpret_cast<const char*>(firstRow.data() + 1), 8));
  EXPECT_EQ(
      decodedRows[1],
      std::string_view(reinterpret_cast<const char*>(secondRow.data() + 1), 8));
  EXPECT_EQ(
      layout,
      isComposite ? RowVectorLayout::kComposite : RowVectorLayout::kColumnar);
}

TEST_P(CelebornPartitionWriterTest, evictEmptyRows) {
  const bool isComposite = GetParam();
  auto client = std::make_shared<MockRssClient>();
  CelebornPartitionWriter writer(
      3, PartitionWriterOptions{}, arrow::default_memory_pool(), client);
  std::vector<std::vector<uint8_t*>> rows(3);
  std::vector<int64_t> partitionBytes;
  if (!isComposite) {
    partitionBytes.resize(3, 0);
  }

  ASSERT_TRUE(writer.evict(rows, partitionBytes, isComposite).ok());
  ShuffleWriterMetrics metrics;
  ASSERT_TRUE(writer.stop(&metrics).ok());
  EXPECT_EQ(metrics.totalBytesWritten, 0);
  EXPECT_EQ(metrics.rawPartitionLengths, (std::vector<int64_t>{0, 0, 0}));
}

INSTANTIATE_TEST_SUITE_P(
    RowLayouts,
    CelebornPartitionWriterTest,
    testing::Values(false, true));

} // namespace bytedance::bolt::shuffle::sparksql::test
