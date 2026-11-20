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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/common/TypeWithId.h"
#include "bolt/dwio/dwrf/writer/ColumnWriter.h"
#include "bolt/dwio/dwrf/writer/WriterContext.h"
#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/vector/FlatVector.h"

using namespace ::testing;
namespace bytedance::bolt::dwrf {
class WriterContextTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(WriterContextTest, WideTableDictionaryMemory) {
  constexpr uint32_t kNumColumns = 80'000;
  constexpr int64_t kMiB = 1 << 20;
  auto root = memory::memoryManager()->addRootPool(
      "wide_table_dictionaries", 512 * kMiB);
  auto inputPool = memory::memoryManager()->addLeafPool();
  auto config = std::make_shared<Config>();
  config->set(Config::COMPRESSION, common::CompressionKind_NONE);
  config->set(Config::DISABLE_LOW_MEMORY_MODE, true);

  // Keep all 80,000 column dictionaries live. Alternate integer and string
  // columns to exercise both production resource-wiring paths. Two identical
  // rows per column exercise insertion and dictionary lookup.
  auto integers = BaseVector::create(BIGINT(), 2, inputPool.get());
  integers->asFlatVector<int64_t>()->set(0, 42);
  integers->asFlatVector<int64_t>()->set(1, 42);
  auto strings = BaseVector::create(VARCHAR(), 2, inputPool.get());
  strings->asFlatVector<StringView>()->set(0, StringView("shared"));
  strings->asFlatVector<StringView>()->set(1, StringView("shared"));
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  names.reserve(kNumColumns);
  types.reserve(kNumColumns);
  children.reserve(kNumColumns);
  for (uint32_t i = 0; i < kNumColumns; ++i) {
    names.push_back(fmt::format("c{}", i));
    auto child = i % 2 == 0 ? integers : strings;
    types.push_back(child->type());
    children.push_back(std::move(child));
  }
  const auto rowType = ROW(std::move(names), std::move(types));
  auto input = std::make_shared<RowVector>(
      inputPool.get(), rowType, nullptr, 2, std::move(children));
  auto typeWithId = dwio::common::TypeWithId::create(rowType);
  {
    WriterContext context{config, root};
    auto writer = BaseColumnWriter::create(context, *typeWithId);
    writer->write(input, common::Ranges::of(0, 2));

    // Per-column slab resources would charge about 80 GiB and exhaust the
    // 512 MiB pool before reaching this point. Only one reservation's worth
    // of unused slab space may be charged across the entire writer.
    const auto& resource = context.slabResource();
    EXPECT_GT(resource.usedBytes(), 0);
    EXPECT_LT(resource.freeBytes(), kMiB);
    // Each column writer also preallocates a 4 KiB row buffer (312.5 MiB
    // in total). Allow that real storage plus dictionary data and metadata.
    EXPECT_LT(
        context.getMemoryPool(MemoryUsageCategory::GENERAL).usedBytes(),
        384 * kMiB);
  }
  EXPECT_EQ(root->usedBytes(), 0);
}

TEST_F(WriterContextTest, GetIntDictionaryEncoder) {
  auto config = std::make_shared<Config>();
  WriterContext context{
      config, memory::memoryManager()->addRootPool("GetIntDictionaryEncoder")};

  EXPECT_EQ(0, context.dictEncoders_.size());
  auto& intEncoder_1_0 = context.getIntDictionaryEncoder<int32_t>(
      {1, 0}, *context.dictionaryPool_);
  EXPECT_EQ(1, context.dictEncoders_.size());
  ASSERT_EQ(1, intEncoder_1_0.refCount_);

  auto& duplicateCallResult_1_0 = context.getIntDictionaryEncoder<int32_t>(
      {1, 0}, *context.dictionaryPool_);
  EXPECT_EQ(1, context.dictEncoders_.size());
  EXPECT_EQ(&intEncoder_1_0, &duplicateCallResult_1_0);
  EXPECT_EQ(2, intEncoder_1_0.refCount_);

  for (size_t i = 0; i < 40; ++i) {
    context.getIntDictionaryEncoder<int32_t>({1, 0}, *context.dictionaryPool_);
  }
  EXPECT_EQ(42, intEncoder_1_0.refCount_);

  // Ignore the mismatched type request.
  context.getIntDictionaryEncoder<int64_t>({1, 0}, *context.dictionaryPool_);
  EXPECT_EQ(1, context.dictEncoders_.size());

  context.getIntDictionaryEncoder<int32_t>({2, 0}, *context.dictionaryPool_);
  EXPECT_EQ(2, context.dictEncoders_.size());
}

TEST_F(WriterContextTest, RemoveIntDictionaryEncoderForNode) {
  auto config = std::make_shared<Config>();
  config->set(Config::MAP_FLAT_DICT_SHARE, false);
  WriterContext context{
      config,
      memory::memoryManager()->addRootPool(
          "RemoveIntDictionaryEncoderForNode")};

  context.getIntDictionaryEncoder<int32_t>({1, 1}, *context.dictionaryPool_);
  context.getIntDictionaryEncoder<int32_t>({1, 2}, *context.dictionaryPool_);
  context.getIntDictionaryEncoder<int32_t>({1, 4}, *context.dictionaryPool_);
  context.getIntDictionaryEncoder<int32_t>({1, 5}, *context.dictionaryPool_);
  EXPECT_EQ(4, context.dictEncoders_.size());

  context.getIntDictionaryEncoder<int32_t>({2, 0}, *context.dictionaryPool_);
  context.getIntDictionaryEncoder<int32_t>({3, 1}, *context.dictionaryPool_);
  context.getIntDictionaryEncoder<int32_t>({3, 3}, *context.dictionaryPool_);
  EXPECT_EQ(7, context.dictEncoders_.size());

  context.removeAllIntDictionaryEncodersOnNode(
      [](uint32_t nodeId) { return nodeId == 1; });
  EXPECT_EQ(3, context.dictEncoders_.size());
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 1}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 2}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 4}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 5}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{2, 0}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{3, 1}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{3, 3}));

  context.removeAllIntDictionaryEncodersOnNode(
      [](uint32_t nodeId) { return nodeId == 3; });
  EXPECT_EQ(1, context.dictEncoders_.size());
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 1}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 2}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 4}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 5}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{2, 0}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{3, 1}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{3, 3}));

  context.removeAllIntDictionaryEncodersOnNode(
      [](uint32_t nodeId) { return nodeId == 2; });
  EXPECT_EQ(0, context.dictEncoders_.size());
}

TEST_F(WriterContextTest, BuildPhysicalSizeAggregators) {
  auto config = std::make_shared<Config>();
  WriterContext context{
      config,
      memory::memoryManager()->addRootPool("BuildPhysicalSizeAggregators")};
  auto type = ROW({
      {"array", ARRAY(REAL())},
      {"map", MAP(INTEGER(), DOUBLE())},
      {"row",
       ROW({
           {"a", REAL()},
           {"b", INTEGER()},
       })},
      {"nested",
       ARRAY(ROW({
           {"a", INTEGER()},
           {"b", MAP(REAL(), REAL())},
       }))},
  });
  auto typeWithId = bolt::dwio::common::TypeWithId::create(type);
  context.buildPhysicalSizeAggregators(*typeWithId);
  std::vector<uint32_t> mapNodes{3, 12};
  for (size_t i = 0; i < 14; ++i) {
    EXPECT_NO_THROW(context.getPhysicalSizeAggregator(i));
  }
  for (const auto nodeId : mapNodes) {
    EXPECT_NO_THROW(dynamic_cast<MapPhysicalSizeAggregator&>(
        context.getPhysicalSizeAggregator(nodeId)));
  }
}

TEST_F(WriterContextTest, memory) {
  auto writerRoot = memory::memoryManager()->addRootPool(
      "memory", 1L << 30, exec::MemoryReclaimer::create());
  WriterContext context{std::make_shared<Config>(), writerRoot};
  ASSERT_EQ(context.getTotalMemoryUsage(), 0);
  context.initBuffer();
  BOLT_ASSERT_THROW(context.initBuffer(), "");
  // The writer context has some initial memory allocation on construction.
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208);
  ASSERT_EQ(context.availableMemoryReservation(), 786368);

  auto& generalPool = context.getMemoryPool(MemoryUsageCategory::GENERAL);
  auto& dictPool = context.getMemoryPool(MemoryUsageCategory::DICTIONARY);
  auto& outputPool = context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM);
  ASSERT_TRUE(generalPool.reclaimer() == nullptr);
  ASSERT_TRUE(dictPool.reclaimer() == nullptr);
  ASSERT_TRUE(outputPool.reclaimer() == nullptr);
  const int bufferSize{1024};
  void* generalBuf = generalPool.allocate(bufferSize);
  void* dictBuf = dictPool.allocate(bufferSize);
  void* outputBuf = outputPool.allocate(bufferSize);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(context.availableMemoryReservation(), 2880448);

  ASSERT_EQ(generalPool.currentBytes(), 262208 + bufferSize);
  ASSERT_EQ(generalPool.reservedBytes(), 1048576);
  ASSERT_EQ(dictPool.currentBytes(), bufferSize);
  ASSERT_EQ(dictPool.reservedBytes(), 1048576);
  ASSERT_EQ(outputPool.currentBytes(), bufferSize);
  ASSERT_EQ(outputPool.reservedBytes(), 1048576);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(context.availableMemoryReservation(), 2880448);

  ASSERT_TRUE(generalPool.maybeReserve(4L << 20));
  ASSERT_TRUE(dictPool.maybeReserve(4L << 20));
  ASSERT_TRUE(outputPool.maybeReserve(4L << 20));
  ASSERT_EQ(generalPool.currentBytes(), 262208 + bufferSize);
  ASSERT_EQ(generalPool.reservedBytes(), 9437184);
  ASSERT_EQ(dictPool.currentBytes(), bufferSize);
  ASSERT_EQ(dictPool.reservedBytes(), 9437184);
  ASSERT_EQ(outputPool.currentBytes(), bufferSize);
  ASSERT_EQ(outputPool.reservedBytes(), 9437184);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(context.availableMemoryReservation(), 28046272);

  context.releaseMemoryReservation();
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(generalPool.currentBytes(), 262208 + bufferSize);
  ASSERT_EQ(generalPool.reservedBytes(), 1048576);
  ASSERT_EQ(dictPool.currentBytes(), bufferSize);
  ASSERT_EQ(dictPool.reservedBytes(), 1048576);
  ASSERT_EQ(outputPool.currentBytes(), bufferSize);
  ASSERT_EQ(outputPool.reservedBytes(), 1048576);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(context.availableMemoryReservation(), 2880448);

  generalPool.free(generalBuf, bufferSize);
  dictPool.free(dictBuf, bufferSize);
  outputPool.free(outputBuf, bufferSize);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208);
  ASSERT_EQ(generalPool.currentBytes(), 262208);
  ASSERT_EQ(generalPool.reservedBytes(), 1048576);
  ASSERT_EQ(dictPool.currentBytes(), 0);
  ASSERT_EQ(dictPool.reservedBytes(), 0);
  ASSERT_EQ(outputPool.currentBytes(), 0);
  ASSERT_EQ(outputPool.reservedBytes(), 0);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208);
  ASSERT_EQ(context.availableMemoryReservation(), 786368);
}

TEST_F(WriterContextTest, abort) {
  auto writerRoot = memory::memoryManager()->addRootPool(
      "abort", 1L << 30, exec::MemoryReclaimer::create());
  WriterContext context{std::make_shared<Config>(), writerRoot};
  ASSERT_EQ(context.getTotalMemoryUsage(), 0);
  context.initBuffer();
  // The writer context has some initial memory allocation on construction.
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208);
  ASSERT_EQ(context.availableMemoryReservation(), 786368);

  auto& generalPool = context.getMemoryPool(MemoryUsageCategory::GENERAL);
  auto& dictPool = context.getMemoryPool(MemoryUsageCategory::DICTIONARY);
  auto& outputPool = context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM);

  const int bufferSize{1024};
  void* generalBuf = generalPool.allocate(bufferSize);
  void* dictBuf = dictPool.allocate(bufferSize);
  void* outputBuf = outputPool.allocate(bufferSize);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(context.availableMemoryReservation(), 2880448);

  ASSERT_EQ(generalPool.currentBytes(), 262208 + bufferSize);
  ASSERT_EQ(generalPool.reservedBytes(), 1048576);
  ASSERT_EQ(dictPool.currentBytes(), bufferSize);
  ASSERT_EQ(dictPool.reservedBytes(), 1048576);
  ASSERT_EQ(outputPool.currentBytes(), bufferSize);
  ASSERT_EQ(outputPool.reservedBytes(), 1048576);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(context.availableMemoryReservation(), 2880448);

  ASSERT_TRUE(generalPool.maybeReserve(4L << 20));
  ASSERT_TRUE(dictPool.maybeReserve(4L << 20));
  ASSERT_TRUE(outputPool.maybeReserve(4L << 20));
  ASSERT_EQ(generalPool.currentBytes(), 262208 + bufferSize);
  ASSERT_EQ(generalPool.reservedBytes(), 9437184);
  ASSERT_EQ(dictPool.currentBytes(), bufferSize);
  ASSERT_EQ(dictPool.reservedBytes(), 9437184);
  ASSERT_EQ(outputPool.currentBytes(), bufferSize);
  ASSERT_EQ(outputPool.reservedBytes(), 9437184);
  ASSERT_EQ(context.getTotalMemoryUsage(), 262208 + bufferSize * 3);
  ASSERT_EQ(context.availableMemoryReservation(), 28046272);

  context.abort();

  ASSERT_EQ(context.getTotalMemoryUsage(), bufferSize * 3);
  ASSERT_EQ(generalPool.currentBytes(), bufferSize);
  ASSERT_EQ(generalPool.reservedBytes(), 1048576);
  ASSERT_EQ(dictPool.currentBytes(), bufferSize);
  ASSERT_EQ(dictPool.reservedBytes(), 1048576);
  ASSERT_EQ(outputPool.currentBytes(), bufferSize);
  ASSERT_EQ(outputPool.reservedBytes(), 1048576);
  ASSERT_EQ(context.availableMemoryReservation(), 3142656);

  generalPool.free(generalBuf, bufferSize);
  dictPool.free(dictBuf, bufferSize);
  outputPool.free(outputBuf, bufferSize);
  ASSERT_EQ(context.getTotalMemoryUsage(), 0);
  ASSERT_EQ(generalPool.currentBytes(), 0);
  ASSERT_EQ(generalPool.reservedBytes(), 0);
  ASSERT_EQ(dictPool.currentBytes(), 0);
  ASSERT_EQ(dictPool.reservedBytes(), 0);
  ASSERT_EQ(outputPool.currentBytes(), 0);
  ASSERT_EQ(outputPool.reservedBytes(), 0);
  ASSERT_EQ(context.getTotalMemoryUsage(), 0);
  ASSERT_EQ(context.availableMemoryReservation(), 0);
}
} // namespace bytedance::bolt::dwrf
