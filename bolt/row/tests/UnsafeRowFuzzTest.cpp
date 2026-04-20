/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/init/Init.h>

#include "bolt/row/UnsafeRowDeserializers.h"
#include "bolt/row/UnsafeRowFast.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::row {
namespace {
using namespace bytedance::bolt::test;

class UnsafeRowFuzzTests : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  UnsafeRowFuzzTests() {
    clearBuffers();
  }

  void clearBuffers() {
    for (auto& buffer : buffers_) {
      std::memset(buffer, 0, kBufferSize);
    }
  }

  void doTest(
      const RowTypePtr& rowType,
      std::function<std::vector<std::optional<std::string_view>>(
          const RowVectorPtr& data)> serializeFunc) {
    VectorFuzzer::Options opts;
    opts.vectorSize = kNumBuffers;
    opts.nullRatio = 0.1;
    opts.dictionaryHasNulls = false;
    opts.stringVariableLength = true;
    opts.stringLength = 20;
    opts.containerVariableLength = true;
    opts.complexElementsMaxSize = 10'000;

    // Spark uses microseconds to store timestamp
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds,
    opts.containerLength = 10;

    VectorFuzzer fuzzer(opts, pool_.get());

    const auto iterations = 200;
    for (size_t i = 0; i < iterations; ++i) {
      clearBuffers();

      auto seed = folly::Random::rand32();

      LOG(INFO) << "seed: " << seed;
      SCOPED_TRACE(fmt::format("seed: {}", seed));

      fuzzer.reSeed(seed);
      const auto& inputVector = fuzzer.fuzzInputRow(rowType);

      // Serialize rowVector into bytes.
      auto serialized = serializeFunc(inputVector);

      // Deserialize previous bytes back to row vector
      VectorPtr outputVector =
          UnsafeRowDeserializer::deserialize(serialized, rowType, pool_.get());

      assertEqualVectors(inputVector, outputVector);
    }
  }

  static constexpr uint64_t kBufferSize = 70 << 10; // 70kb
  static constexpr uint64_t kNumBuffers = 100;

  std::array<char[kBufferSize], kNumBuffers> buffers_{};

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
};

TEST_F(UnsafeRowFuzzTests, fast) {
  auto rowType = ROW({
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      VARCHAR(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      UNKNOWN(),
      DECIMAL(20, 2),
      DECIMAL(12, 4),
      // Arrays.
      ARRAY(BOOLEAN()),
      ARRAY(TINYINT()),
      ARRAY(SMALLINT()),
      ARRAY(INTEGER()),
      ARRAY(BIGINT()),
      ARRAY(REAL()),
      ARRAY(DOUBLE()),
      ARRAY(VARCHAR()),
      ARRAY(VARBINARY()),
      ARRAY(UNKNOWN()),
      ARRAY(DECIMAL(20, 2)),
      ARRAY(DECIMAL(12, 4)),
      // Nested arrays.
      ARRAY(ARRAY(INTEGER())),
      ARRAY(ARRAY(BIGINT())),
      ARRAY(ARRAY(VARCHAR())),
      ARRAY(ARRAY(UNKNOWN())),
      // Maps.
      MAP(BIGINT(), REAL()),
      MAP(BIGINT(), BIGINT()),
      MAP(BIGINT(), VARCHAR()),
      MAP(BIGINT(), DECIMAL(20, 2)),
      MAP(BIGINT(), DECIMAL(12, 4)),
      MAP(INTEGER(), MAP(BIGINT(), DOUBLE())),
      MAP(VARCHAR(), BOOLEAN()),
      MAP(INTEGER(), MAP(BIGINT(), ARRAY(REAL()))),
      // Timestamp and date types.
      TIMESTAMP(),
      DATE(),
      ARRAY(TIMESTAMP()),
      ARRAY(DATE()),
      MAP(DATE(), ARRAY(TIMESTAMP())),
      // Structs.
      ROW(
          {BOOLEAN(),
           INTEGER(),
           TIMESTAMP(),
           DECIMAL(20, 2),
           VARCHAR(),
           ARRAY(BIGINT())}),
      ROW(
          {BOOLEAN(),
           ROW({INTEGER(), TIMESTAMP()}),
           VARCHAR(),
           ARRAY(BIGINT())}),
      ARRAY({ROW({BIGINT(), VARCHAR()})}),
      MAP(BIGINT(), ROW({BOOLEAN(), TINYINT(), REAL()})),
  });

  doTest(rowType, [&](const RowVectorPtr& data) {
    std::vector<std::optional<std::string_view>> serialized;
    serialized.reserve(data->size());

    UnsafeRowFast fast(data);
    for (auto i = 0; i < data->size(); ++i) {
      auto rowSize = fast.serialize(i, buffers_[i]);
      BOLT_CHECK_LE(rowSize, kBufferSize);

      EXPECT_EQ(rowSize, fast.rowSize(i)) << i << ", " << data->toString(i);

      serialized.push_back(std::string_view(buffers_[i], rowSize));
    }
    return serialized;
  });
}

TEST_F(UnsafeRowFuzzTests, constantVector) {
  clearBuffers();

  auto emptyStructType = ROW({});
  auto rowType = ROW({emptyStructType});
  auto data = std::make_shared<RowVector>(
      pool_.get(),
      rowType,
      BufferPtr(nullptr),
      1,
      std::vector<VectorPtr>{
          BaseVector::createNullConstant(emptyStructType, 1, pool_.get())});

  UnsafeRowFast fast(data);
  auto rowSize = fast.serialize(0, buffers_[0]);
  BOLT_CHECK_LE(rowSize, kBufferSize);

  std::vector<std::optional<std::string_view>> serialized;
  serialized.reserve(1);
  serialized.push_back(std::string_view(buffers_[0], rowSize));

  VectorPtr outputVector =
      UnsafeRowDeserializer::deserialize(serialized, rowType, pool_.get());

  assertEqualVectors(data, outputVector);
}

} // namespace
} // namespace bytedance::bolt::row
