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

#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/memory_pool.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include <folly/Range.h>

#include "bolt/shuffle/sparksql/CompressionStream.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

std::vector<uint8_t> buildRow(const std::string& payload) {
  std::vector<uint8_t> row(sizeof(int32_t) + payload.size());
  auto payloadSize = static_cast<int32_t>(payload.size());
  std::memcpy(row.data(), &payloadSize, sizeof(int32_t));
  if (!payload.empty()) {
    std::memcpy(row.data() + sizeof(int32_t), payload.data(), payload.size());
  }
  return row;
}

std::vector<uint8_t> buildRow(size_t payloadSize, uint8_t seed) {
  std::vector<uint8_t> row(sizeof(int32_t) + payloadSize);
  auto payloadSize32 = static_cast<int32_t>(payloadSize);
  std::memcpy(row.data(), &payloadSize32, sizeof(int32_t));
  for (size_t i = 0; i < payloadSize; ++i) {
    row[sizeof(int32_t) + i] = static_cast<uint8_t>(seed + i);
  }
  return row;
}

void runRoundTrip(
    const std::vector<std::vector<uint8_t>>& rows,
    int64_t rawSize,
    RowVectorLayout layout) {
  std::vector<uint8_t*> rowPtrs;
  std::vector<uint8_t> expected;
  expected.reserve(static_cast<size_t>(rawSize));
  rowPtrs.reserve(rows.size());

  for (const auto& row : rows) {
    rowPtrs.push_back(const_cast<uint8_t*>(row.data()));
    expected.insert(expected.end(), row.begin(), row.end());
  }

  auto pool = arrow::default_memory_pool();
  auto outputStreamResult = arrow::io::BufferOutputStream::Create(1024, pool);
  ASSERT_TRUE(outputStreamResult.ok());
  auto outputStream = outputStreamResult.ValueOrDie();

  AdaptiveParallelZstdCodec encoder(1, true, pool, true);
  auto encodeStatus = encoder.CompressAndFlush(
      folly::Range<uint8_t**>(rowPtrs.data(), rowPtrs.size()),
      outputStream.get(),
      rawSize,
      layout);
  ASSERT_TRUE(encodeStatus.ok());

  auto bufferResult = outputStream->Finish();
  ASSERT_TRUE(bufferResult.ok());
  auto buffer = bufferResult.ValueOrDie();
  ASSERT_NE(buffer, nullptr);

  auto inputStream = std::make_shared<arrow::io::BufferReader>(buffer);

  AdaptiveParallelZstdCodec decoder(1, false, pool, true);
  std::vector<uint8_t> output(expected.size());
  int32_t totalOutput = 0;
  bool eof = false;
  bool layoutEnd = false;
  RowVectorLayout decodedLayout = RowVectorLayout::kInvalid;

  while (!eof && totalOutput < expected.size()) {
    int32_t outputLen = 0;
    auto decodeStatus = decoder.Decompress(
        inputStream.get(),
        output.data(),
        static_cast<int32_t>(output.size()),
        totalOutput,
        outputLen,
        eof,
        layoutEnd,
        decodedLayout);
    ASSERT_TRUE(decodeStatus.ok());
    EXPECT_FALSE(layoutEnd);
    if (!eof) {
      ASSERT_GT(outputLen, 0);
    }
    totalOutput += outputLen;
  }

  EXPECT_EQ(decodedLayout, layout);
  EXPECT_EQ(totalOutput, static_cast<int32_t>(expected.size()));
  EXPECT_EQ(0, std::memcmp(output.data(), expected.data(), expected.size()));
}

} // namespace

TEST(AdaptiveParallelZstdCodecTest, RoundTripSmallPayloads) {
  std::vector<std::vector<uint8_t>> rows;
  rows.emplace_back(buildRow("alpha"));
  rows.emplace_back(buildRow("bravo"));
  rows.emplace_back(buildRow("charlie"));

  int64_t rawSize = 0;
  for (const auto& row : rows) {
    rawSize += static_cast<int64_t>(row.size());
  }

  runRoundTrip(rows, rawSize, RowVectorLayout::kComposite);
}

TEST(AdaptiveParallelZstdCodecTest, RoundTripLargePayload) {
  constexpr size_t kLargePayloadSize = 2'300'000;
  std::vector<std::vector<uint8_t>> rows;
  rows.emplace_back(buildRow(kLargePayloadSize, 7));

  int64_t rawSize = 0;
  for (const auto& row : rows) {
    rawSize += static_cast<int64_t>(row.size());
  }

  runRoundTrip(rows, rawSize, RowVectorLayout::kComposite);
}

} // namespace bytedance::bolt::shuffle::sparksql::test
