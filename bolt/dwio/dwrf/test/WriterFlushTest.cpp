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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <unordered_map>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryArbitrator.h"
#include "bolt/dwio/dwrf/writer/Writer.h"
#include "bolt/type/fbhive/HiveTypeParser.h"

using namespace ::testing;
using bytedance::bolt::dwrf::MemoryUsageCategory;
using bytedance::bolt::dwrf::WriterContext;
using bytedance::bolt::dwrf::WriterOptions;

namespace {
constexpr size_t kSizeKB = 1024;
constexpr size_t kSizeMB = 1024 * 1024;
constexpr size_t kReservationHeadroom = 512 * kSizeMB;
} // namespace
namespace bytedance::bolt::dwrf {
namespace {
struct SimulatedAllocation {
  void* data;
  int64_t size;
};

std::string nextPoolName(const std::string& prefix) {
  static std::atomic<uint64_t> nextId{0};
  return fmt::format("{}_{}", prefix, nextId++);
}

std::shared_ptr<memory::MemoryPool> createRootPool(
    const std::string& prefix,
    int64_t capacity = memory::kMaxMemory) {
  return memory::memoryManager()->addRootPool(nextPoolName(prefix), capacity);
}

std::shared_ptr<memory::MemoryPool> createSinkPool() {
  return memory::memoryManager()->addLeafPool(
      nextPoolName("writer_flush_sink"));
}

std::unordered_map<memory::MemoryPool*, std::vector<SimulatedAllocation>>&
simulatedAllocations() {
  static std::
      unordered_map<memory::MemoryPool*, std::vector<SimulatedAllocation>>
          allocations;
  return allocations;
}

void addSimulatedMemory(memory::MemoryPool& pool, int64_t bytes) {
  if (bytes == 0) {
    return;
  }
  BOLT_CHECK_GT(bytes, 0);
  simulatedAllocations()[&pool].push_back({pool.allocate(bytes), bytes});
}

void clearSimulatedMemory(memory::MemoryPool& pool) {
  auto& allocations = simulatedAllocations();
  auto it = allocations.find(&pool);
  if (it == allocations.end()) {
    return;
  }
  for (auto& allocation : it->second) {
    pool.free(allocation.data, allocation.size);
  }
  allocations.erase(it);
}

int64_t simulatedMemory(const memory::MemoryPool& pool) {
  auto& allocations = simulatedAllocations();
  auto it = allocations.find(const_cast<memory::MemoryPool*>(&pool));
  if (it == allocations.end()) {
    return 0;
  }
  int64_t bytes = 0;
  for (const auto& allocation : it->second) {
    bytes += allocation.size;
  }
  return bytes;
}

void setSimulatedMemory(memory::MemoryPool& pool, int64_t bytes) {
  clearSimulatedMemory(pool);
  addSimulatedMemory(pool, bytes);
  ASSERT_EQ(bytes, simulatedMemory(pool));
}

void clearSimulatedMemory(WriterContext& context) {
  clearSimulatedMemory(context.getMemoryPool(MemoryUsageCategory::DICTIONARY));
  clearSimulatedMemory(
      context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM));
  clearSimulatedMemory(context.getMemoryPool(MemoryUsageCategory::GENERAL));
}

int64_t baselineMemoryUsage(WriterContext& context) {
  return context.getTotalMemoryUsage() -
      simulatedMemory(context.getMemoryPool(MemoryUsageCategory::DICTIONARY)) -
      simulatedMemory(
             context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM)) -
      simulatedMemory(context.getMemoryPool(MemoryUsageCategory::GENERAL));
}
} // namespace

// For testing functionality of Writer we need to instantiate
// it.
class DummyWriter : public bolt::dwrf::Writer {
 public:
  DummyWriter(
      WriterOptions& options,
      std::unique_ptr<dwio::common::FileSink> sink,
      std::shared_ptr<memory::MemoryPool> pool)
      : Writer{std::move(sink), options, std::move(pool)} {}

  ~DummyWriter() override {
    clearSimulatedMemory(writerBase_->getContext());
  }

  MOCK_METHOD1(
      flushImpl,
      void(std::function<proto::ColumnEncoding&(uint32_t)>));
  MOCK_METHOD0(createIndexEntryImpl, void());
  MOCK_METHOD1(
      writeFileStatsImpl,
      void(std::function<proto::ColumnStatistics&(uint32_t)>));
  MOCK_METHOD0(abandonDictionariesImpl, void());
  MOCK_METHOD0(resetImpl, void());

  friend class WriterFlushTestHelper;
  BOLT_FRIEND_TEST(TestWriterFlush, CheckAgainstMemoryBudget);
};

// Big idea is to directly manipulate context states (num rows) + memory pool
// and just call writer.write() to trigger the flush?

// The most elegant solution would be to mock column writers, which then
// updates the memory pool stats. The point is to control the memory footprint
// while ideally just calling writer.write() and make sure it takes all
// these into account.

struct SimulatedWrite {
  SimulatedWrite(
      uint64_t numRows,
      uint64_t outputStreamMemoryUsage,
      uint64_t generalMemoryUsage)
      : numRows{numRows},
        outputStreamMemoryUsage{outputStreamMemoryUsage},
        generalMemoryUsage{generalMemoryUsage} {}

  void apply(WriterContext& context) const {
    context.incRowCount(numRows);
    // Not the most accurate semantically, but suffices for testing
    // purposes.
    addSimulatedMemory(
        context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM),
        outputStreamMemoryUsage);
    addSimulatedMemory(
        context.getMemoryPool(MemoryUsageCategory::GENERAL),
        generalMemoryUsage);
  }

  uint64_t numRows;
  uint64_t outputStreamMemoryUsage;
  uint64_t generalMemoryUsage;
};

struct SimulatedFlush {
  SimulatedFlush(
      uint64_t flushOverhead,
      uint64_t stripeRowCount,
      uint64_t stripeRawSize,
      uint64_t compressedSize,
      uint64_t dictMemoryUsage,
      uint64_t outputStreamMemoryUsage,
      uint64_t generalMemoryUsage)
      : flushOverhead{flushOverhead},
        stripeRowCount{stripeRowCount},
        stripeRawSize{stripeRawSize},
        compressedSize{compressedSize},
        dictMemoryUsage{dictMemoryUsage},
        outputStreamMemoryUsage{outputStreamMemoryUsage},
        generalMemoryUsage{generalMemoryUsage} {}

  void apply(WriterContext& context) const {
    context.setStripeRawSize(stripeRawSize);
    ASSERT_EQ(stripeRowCount, context.stripeRowCount());
    auto& dictPool = context.getMemoryPool(MemoryUsageCategory::DICTIONARY);
    auto& outputPool =
        context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM);
    auto& generalPool = context.getMemoryPool(MemoryUsageCategory::GENERAL);
    setSimulatedMemory(dictPool, dictMemoryUsage);
    ASSERT_EQ(outputStreamMemoryUsage, simulatedMemory(outputPool));
    addSimulatedMemory(outputPool, flushOverhead);
    setSimulatedMemory(generalPool, generalMemoryUsage);

    context.recordAverageRowSize();
    context.recordFlushOverhead(flushOverhead);
    context.recordCompressionRatio(compressedSize);

    context.testingIncStripeIndex();
    // Clear context
    context.setStripeRawSize(0);
    context.setStripeRowCount(0);
    // Simplified memory footprint modeling for testing.
    clearSimulatedMemory(context);
  }

  uint64_t flushOverhead;
  uint64_t stripeRowCount;
  uint64_t stripeRawSize;
  uint64_t compressedSize;
  // Memory footprint can change drastically at flush time
  // esp for dictionary encoding.
  uint64_t dictMemoryUsage;
  uint64_t outputStreamMemoryUsage;
  uint64_t generalMemoryUsage;
};

class WriterFlushTestHelper {
 public:
  static std::unique_ptr<DummyWriter> prepWriter(
      const std::shared_ptr<memory::MemoryPool>& sinkPool,
      int64_t writerMemoryBudget) {
    WriterOptions options;
    options.config = std::make_shared<Config>();
    options.schema = type::fbhive::HiveTypeParser().parse(
        "struct<int_val:int,string_val:string>");
    // A completely memory pressure based flush policy.
    options.flushPolicyFactory = []() {
      return std::make_unique<LambdaFlushPolicy>([]() { return false; });
    };
    auto makeWriter = [&](int64_t capacity) {
      return std::make_unique<DummyWriter>(
          options,
          // Unused sink.
          std::make_unique<dwio::common::MemorySink>(
              kSizeKB, dwio::common::FileSink::Options{.pool = sinkPool.get()}),
          createRootPool("writer_flush_root", capacity));
    };
    auto probeWriter = makeWriter(memory::kMaxMemory);
    auto baselineBytes =
        baselineMemoryUsage(probeWriter->writerBase_->getContext());
    probeWriter.reset();

    auto writer = makeWriter(
        writerMemoryBudget + baselineBytes +
        std::max<int64_t>(kReservationHeadroom, writerMemoryBudget / 4));
    auto& context = writer->writerBase_->getContext();
    zeroOutMemoryUsage(context);
    return writer;
  }

  static void testRandomSequence(
      std::unique_ptr<DummyWriter> writer,
      int64_t numStripes,
      uint32_t seed,
      uint32_t averageOutputStreamMemoryUsage,
      uint32_t generalMemoryUsageVariation) {
    constexpr size_t kSequenceLength = 1000;
    std::mt19937 gen{};
    gen.seed(seed);
    auto sequence = generateSimulatedWrites(
        gen,
        averageOutputStreamMemoryUsage,
        generalMemoryUsageVariation,
        kSequenceLength);
    testRandomSequence(std::move(writer), numStripes, sequence, gen);
  }

 private:
  static void zeroOutMemoryUsage(WriterContext& context) {
    clearSimulatedMemory(context);
  }

  static void testRandomSequence(
      std::unique_ptr<DummyWriter> writer,
      int64_t numStripes,
      const std::vector<SimulatedWrite>& writeSequence,
      std::mt19937& gen) {
    auto& context = writer->writerBase_->getContext();
    for (const auto& write : writeSequence) {
      if (writer->shouldFlush(context, write.numRows) ||
          needsFlushBeforeSimulatedAllocation(context, write)) {
        ASSERT_EQ(
            0,
            simulatedMemory(
                context.getMemoryPool(MemoryUsageCategory::DICTIONARY)));
        auto outputStreamMemoryUsage = simulatedMemory(
            context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM));
        auto generalMemoryUsage = simulatedMemory(
            context.getMemoryPool(MemoryUsageCategory::GENERAL));

        uint64_t flushOverhead =
            folly::Random::rand32(0, context.stripeRawSize(), gen);
        uint64_t compressedSize =
            folly::Random::rand32(0, context.stripeRawSize(), gen);
        uint64_t dictMemoryUsage =
            folly::Random::rand32(0, flushOverhead / 3, gen);
        SimulatedFlush{
            flushOverhead,
            context.stripeRowCount(),
            context.stripeRawSize(),
            compressedSize,
            folly::to<uint64_t>(dictMemoryUsage),
            // Flush overhead is the delta of output stream memory
            // usage before and after flush. Peak memory footprint
            // happens when we finished writing dictionary encoded to
            // streams and before we can clear the dictionary encoders.
            folly::to<uint64_t>(outputStreamMemoryUsage) + flushOverhead -
                dictMemoryUsage,
            // For simplicy, general pool usage is held constant.
            folly::to<uint64_t>(generalMemoryUsage)}
            .apply(context);
      }
      write.apply(context);
    }
    EXPECT_EQ(numStripes, context.stripeIndex());
  }

  static bool needsFlushBeforeSimulatedAllocation(
      WriterContext& context,
      const SimulatedWrite& write) {
    return context.getTotalMemoryUsage() + write.outputStreamMemoryUsage +
        write.generalMemoryUsage + kReservationHeadroom / 4 >
        context.getMemoryBudget();
  }

  static std::vector<SimulatedWrite> generateSimulatedWrites(
      std::mt19937& gen,
      uint32_t averageOutputStreamMemoryUsage,
      uint32_t generalMemoryUsageVariation,
      size_t sequenceLength) {
    std::vector<SimulatedWrite> sequence;
    for (size_t i = 0; i < sequenceLength; ++i) {
      sequence.emplace_back(
          1000,
          folly::Random::rand32(
              averageOutputStreamMemoryUsage / 2,
              averageOutputStreamMemoryUsage,
              gen),
          // For simplicity, general pool memory footprint is monotonically
          // increasing from 0. It's equivalent to removing the base
          // footprint from the budget anyway.
          folly::Random::rand32(0, generalMemoryUsageVariation, gen));
    }
    return sequence;
  }
};

class TestWriterFlush : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

// This test checks against constructed test cases.
TEST_F(TestWriterFlush, CheckAgainstMemoryBudget) {
  auto pool = createSinkPool();
  constexpr uint64_t kScale = kSizeMB;
  const auto budgetBytes = [](uint64_t value) { return value * kScale; };
  const auto bytes = [](uint64_t value) { return value * kScale * 3 / 2; };
  {
    auto writer = WriterFlushTestHelper::prepWriter(pool, budgetBytes(1024));
    auto& context = writer->writerBase_->getContext();

    SimulatedWrite simWrite{10, bytes(500), bytes(300)};
    simWrite.apply(context);
    // Writer has no data point in the first stripe and uses a static
    // (though configurable) flush overhead ratio to determine whether
    // we need to flush.
    EXPECT_FALSE(writer->shouldFlush(context, 10));
    EXPECT_FALSE(writer->shouldFlush(context, 20));
    EXPECT_FALSE(writer->shouldFlush(context, 200));
  }
  {
    auto writer = WriterFlushTestHelper::prepWriter(pool, budgetBytes(1024));
    auto& context = writer->writerBase_->getContext();

    SimulatedWrite simWrite{10, bytes(500), bytes(300)};
    simWrite.apply(context);
    // The flush produces 0 overhead for miraculous reasons.
    SimulatedFlush simFlush{
        bytes(0) /* flushOverhead */,
        10 /* stripeRowCount */,
        bytes(1000) /* stripeRawSize */,
        bytes(450) /* compressedSize */,
        bytes(0) /* dictMemoryUsage */,
        bytes(500) /* outputStreamMemoryUsage */,
        bytes(300) /* generalMemoryUsage */};

    simFlush.apply(context);
    // Aborting write based on whether the write would exceed budget.
    // Ideally logic should be added to further break up batches like bbio.
    EXPECT_FALSE(writer->shouldFlush(context, 10));
    EXPECT_FALSE(writer->shouldFlush(context, 20));
    EXPECT_TRUE(writer->shouldFlush(context, 25));
    EXPECT_TRUE(writer->shouldFlush(context, 200));
  }
  {
    auto writer = WriterFlushTestHelper::prepWriter(pool, budgetBytes(1024));
    auto& context = writer->writerBase_->getContext();

    SimulatedWrite{10, bytes(500), bytes(300)}.apply(context);
    SimulatedFlush simFlush{
        bytes(0) /* flushOverhead */,
        10 /* stripeRowCount */,
        bytes(1000) /* stripeRawSize */,
        bytes(450) /* compressedSize */,
        bytes(0) /* dictMemoryUsage */,
        bytes(500) /* outputStreamMemoryUsage */,
        bytes(300) /* generalMemoryUsage */};
    simFlush.apply(context);
    // Aborting write based on whether the write would exceed budget.
    SimulatedWrite{10, bytes(500), bytes(300)}.apply(context);

    EXPECT_FALSE(writer->shouldFlush(context, 4));
    EXPECT_TRUE(writer->shouldFlush(context, 5));
    EXPECT_TRUE(writer->shouldFlush(context, 15));
    EXPECT_TRUE(writer->shouldFlush(context, 200));
  }
  {
    auto writer = WriterFlushTestHelper::prepWriter(pool, budgetBytes(1024));
    auto& context = writer->writerBase_->getContext();

    // 0 overhead flush but with raw size per row variance.
    SimulatedWrite{10, bytes(500), bytes(300)}.apply(context);
    SimulatedFlush{
        bytes(0) /* flushOverhead */,
        10 /* stripeRowCount */,
        bytes(1000) /* stripeRawSize */,
        bytes(500) /* compressedSize */,
        bytes(0) /* dictMemoryUsage */,
        bytes(500) /* outputStreamMemoryUsage */,
        bytes(300) /* generalMemoryUsage */}
        .apply(context);
    SimulatedWrite{10, bytes(500), bytes(300)}.apply(context);
    SimulatedFlush{
        bytes(0) /* flushOverhead */,
        10 /* stripeRowCount */,
        bytes(600) /* stripeRawSize */,
        bytes(300) /* compressedSize */,
        bytes(0) /* dictMemoryUsage */,
        bytes(500) /* outputStreamMemoryUsage */,
        bytes(300) /* generalMemoryUsage */}
        .apply(context);

    EXPECT_FALSE(writer->shouldFlush(context, 10));
    EXPECT_FALSE(writer->shouldFlush(context, 25));
    EXPECT_TRUE(writer->shouldFlush(context, 26));
    EXPECT_TRUE(writer->shouldFlush(context, 200));
  }
  {
    auto writer = WriterFlushTestHelper::prepWriter(pool, budgetBytes(1024));
    auto& context = writer->writerBase_->getContext();

    // 0 overhead flush but with raw size per row variance.
    SimulatedWrite{10, bytes(500), bytes(300)}.apply(context);
    SimulatedFlush{
        bytes(200) /* flushOverhead */,
        10 /* stripeRowCount */,
        bytes(1000) /* stripeRawSize */,
        bytes(500) /* compressedSize */,
        bytes(0) /* dictMemoryUsage */,
        bytes(500) /* outputStreamMemoryUsage */,
        bytes(300) /* generalMemoryUsage */}
        .apply(context);

    SimulatedWrite{5, bytes(250), bytes(150)}.apply(context);

    EXPECT_FALSE(writer->shouldFlush(context, 5));
    EXPECT_FALSE(writer->shouldFlush(context, 6));
    EXPECT_TRUE(writer->shouldFlush(context, 10));
    EXPECT_TRUE(writer->shouldFlush(context, 200));
  }
  {
    auto writer = WriterFlushTestHelper::prepWriter(pool, budgetBytes(1024));
    auto& context = writer->writerBase_->getContext();

    // 0 overhead flush but with flush overhead variance.
    SimulatedWrite{10, bytes(500), bytes(300)}.apply(context);
    SimulatedFlush{
        bytes(200) /* flushOverhead */,
        10 /* stripeRowCount */,
        bytes(1000) /* stripeRawSize */,
        bytes(500) /* compressedSize */,
        bytes(0) /* dictMemoryUsage */,
        bytes(500) /* outputStreamMemoryUsage */,
        bytes(300) /* generalMemoryUsage */}
        .apply(context);
    SimulatedWrite{10, bytes(500), bytes(300)}.apply(context);
    SimulatedFlush{
        bytes(100) /* flushOverhead */,
        10 /* stripeRowCount */,
        bytes(1000) /* stripeRawSize */,
        bytes(500) /* compressedSize */,
        bytes(0) /* dictMemoryUsage */,
        bytes(500) /* outputStreamMemoryUsage */,
        bytes(300) /* generalMemoryUsage */}
        .apply(context);

    SimulatedWrite{5, bytes(250), bytes(150)}.apply(context);
    EXPECT_FALSE(writer->shouldFlush(context, 5));
    EXPECT_FALSE(writer->shouldFlush(context, 7));
    EXPECT_TRUE(writer->shouldFlush(context, 10));
    EXPECT_TRUE(writer->shouldFlush(context, 200));
  }
}

// Tests the number of stripes produced based on random results.
TEST_F(TestWriterFlush, MemoryBasedFlushRandom) {
  struct TestCase {
    TestCase(
        uint32_t seed,
        int64_t averageOutputStreamMemoryUsage,
        size_t numStripes)
        : seed{seed},
          averageOutputStreamMemoryUsage{averageOutputStreamMemoryUsage},
          numStripes{numStripes} {}

    uint32_t seed;
    int64_t averageOutputStreamMemoryUsage;
    size_t numStripes;
  };

  auto pool = createSinkPool();
  std::vector<TestCase> testCases{
      {10237629, 20 * kSizeMB, 17},
      // TODO: investigate why this fails on linux specifically.
      // {30227679, 20 * kSizeMB, 30},
      {10237629, 10 * kSizeMB, 8},
      {30227679, 10 * kSizeMB, 9},
      {10237629, 49 * kSizeMB, 42},
      {30227679, 70 * kSizeMB, 61}};

  for (auto& testCase : testCases) {
    WriterFlushTestHelper::testRandomSequence(
        WriterFlushTestHelper::prepWriter(pool, 512 * kSizeMB),
        testCase.numStripes,
        testCase.seed,
        testCase.averageOutputStreamMemoryUsage,
        kSizeMB);
  }
}
} // namespace bytedance::bolt::dwrf
