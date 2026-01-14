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

#include "bolt/exec/SortBuffer.h"
#include <gtest/gtest.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/Type.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;
namespace bytedance::bolt::functions::test {

class SortBufferTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      this->registerVectorSerde();
    }
    rng_.seed(123);
  }

  void TearDown() override {
    pool_.reset();
    rootPool_.reset();
    OperatorTestBase::TearDown();
  }

  common::SpillConfig getSpillConfig(const std::string& spillDir) const {
    return common::SpillConfig(
        [&]() -> const std::string& { return spillDir; },
        [&](uint64_t) {},
        "0.0.0",
        0,
        false,
        0,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        0,
        "none");
  }

  const RowTypePtr inputType_ = ROW(
      {{"c0", BIGINT()},
       {"c1", INTEGER()},
       {"c2", SMALLINT()},
       {"c3", REAL()},
       {"c4", DOUBLE()},
       {"c5", VARCHAR()}});
  // Specifies the sort columns ["c4", "c1"].
  std::vector<column_index_t> sortColumnIndices_{4, 1};
  std::vector<CompareFlags> sortCompareFlags_{
      {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
      {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}};

  const std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};

  tsan_atomic<bool> nonReclaimableSection_{false};
  folly::Random::DefaultGenerator rng_;
};

TEST_F(SortBufferTest, singleKey) {
  struct {
    std::vector<CompareFlags> sortCompareFlags;
    std::vector<int32_t> expectedResult;
    bool hybridSortEnabled;

    std::string debugString() const {
      const std::string expectedResultStr = folly::join(",", expectedResult);
      std::stringstream sortCompareFlagsStr;
      for (const auto sortCompareFlag : sortCompareFlags) {
        sortCompareFlagsStr << sortCompareFlag.toString();
      }
      return fmt::format(
          "sortCompareFlags:{}, expectedResult:{}, hybridSortEnabled:{}",
          sortCompareFlagsStr.str(),
          expectedResultStr,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {{{true,
         true,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Ascending
       {1, 2, 3, 4, 5},
       false},
      {{{true,
         true,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Ascending with
                                                         // hybrid
       {1, 2, 3, 4, 5},
       true},
      {{{true,
         false,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Descending
       {5, 4, 3, 2, 1},
       false},
      {{{true,
         false,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Descending with
                                                         // hybrid
       {5, 4, 3, 2, 1},
       true}};

  // Specifies the sort columns ["c1"].
  sortColumnIndices_ = {1};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        testData.sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_,
        nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);

    RowVectorPtr data = makeRowVector(
        {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted column
         makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
         makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
         makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}),
         makeFlatVector<std::string>(
             {"hello", "world", "today", "is", "great"})});

    sortBuffer->addInput(data);
    sortBuffer->noMoreInput();
    auto output = sortBuffer->getOutput(10000);
    ASSERT_EQ(output->size(), 5);
    int resultIndex = 0;
    for (int expectedValue : testData.expectedResult) {
      ASSERT_EQ(
          output->childAt(1)->asFlatVector<int32_t>()->valueAt(resultIndex++),
          expectedValue);
    }
  }
}

TEST_F(SortBufferTest, multipleKeys) {
  struct {
    bool hybridSortEnabled;

    std::string debugString() const {
      return fmt::format("hybridSortEnabled:{}", hybridSortEnabled);
    }
  } testSettings[] = {{false}, {true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);

    RowVectorPtr data = makeRowVector(
        {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted-2 column
         makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
         makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
         makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}), // sorted-1 column
         makeFlatVector<std::string>(
             {"hello", "world", "today", "is", "great"})});

    sortBuffer->addInput(data);
    sortBuffer->noMoreInput();
    auto output = sortBuffer->getOutput(10000);
    ASSERT_EQ(output->size(), 5);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(0), 5);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(1), 3);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(2), 4);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(3), 1);
    ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(4), 2);
  }
}

// TODO: enable it later with test utility to compare the sorted result.
TEST_F(SortBufferTest, DISABLED_randomData) {
  struct {
    RowTypePtr inputType;
    std::vector<column_index_t> sortColumnIndices;
    std::vector<CompareFlags> sortCompareFlags;
    bool hybridSortEnabled;

    std::string debugString() const {
      const std::string sortColumnIndicesStr =
          folly::join(",", sortColumnIndices);
      std::stringstream sortCompareFlagsStr;
      for (auto sortCompareFlag : sortCompareFlags) {
        sortCompareFlagsStr << sortCompareFlag.toString() << ";";
      }
      return fmt::format(
          "inputType:{}, sortColumnIndices:{}, sortCompareFlags:{}, hybridSortEnabled:{}",
          inputType,
          sortColumnIndicesStr,
          sortCompareFlagsStr.str(),
          hybridSortEnabled);
    }
  } testSettings[] = {
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {2},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       false},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {2},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       true},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       false},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       true},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {false, false, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       false},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {false, false, false, CompareFlags::NullHandlingMode::kNullAsValue}},
       true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        testData.inputType,
        testData.sortColumnIndices,
        testData.sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_,
        nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("VectorFuzzer");

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(3);
    for (size_t inputRows : {1000, 1000, 1000}) {
      VectorFuzzer fuzzer({.vectorSize = inputRows}, fuzzerPool.get());
      RowVectorPtr input = fuzzer.fuzzRow(inputType_);
      sortBuffer->addInput(input);
      inputVectors.push_back(input);
    }
    sortBuffer->noMoreInput();
    // todo: have a utility function buildExpectedSortResult and verify the
    // sorting result for random data.
  }
}

TEST_F(SortBufferTest, batchOutput) {
  struct {
    bool triggerSpill;
    std::vector<size_t> numInputRows;
    size_t maxOutputRows;
    std::vector<size_t> expectedOutputRowCount;
    bool hybridSortEnabled;

    std::string debugString() const {
      const std::string numInputRowsStr = folly::join(",", numInputRows);
      const std::string expectedOutputRowCountStr =
          folly::join(",", expectedOutputRowCount);
      return fmt::format(
          "triggerSpill:{}, numInputRows:{}, maxOutputRows:{}, expectedOutputRowCount:{}, hybridSortEnabled:{}",
          triggerSpill,
          numInputRowsStr,
          maxOutputRows,
          expectedOutputRowCountStr,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {false, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, false},
      {false, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, true},
      {true, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, false},
      {true, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}, true},
      {false, {2000, 2000}, 10000, {4000}, false},
      {false, {2000, 2000}, 10000, {4000}, true},
      {true, {2000, 2000}, 10000, {4000}, false},
      {true, {2000, 2000}, 10000, {4000}, true},
      {false, {2000, 2000}, 2000, {2000, 2000}, false},
      {false, {2000, 2000}, 2000, {2000, 2000}, true},
      {true, {2000, 2000}, 2000, {2000, 2000}, false},
      {true, {2000, 2000}, 2000, {2000, 2000}, true},
      {false, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, false},
      {false, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, true},
      {true, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, false},
      {true, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}, true}};

  TestScopedSpillInjection scopedSpillInjection(100);
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        false,
        0,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        100, //  testSpillPct
        "none");
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        testData.triggerSpill ? &spillConfig : nullptr,
        0,
        nullptr,
        testData.hybridSortEnabled);
    ASSERT_EQ(sortBuffer->canSpill(), testData.triggerSpill);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("VectorFuzzer");

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(testData.numInputRows.size());
    uint64_t totalNumInput = 0;
    for (size_t inputRows : testData.numInputRows) {
      VectorFuzzer fuzzer({.vectorSize = inputRows}, fuzzerPool.get());
      RowVectorPtr input = fuzzer.fuzzRow(inputType_);
      sortBuffer->addInput(input);
      inputVectors.push_back(input);
      totalNumInput += inputRows;
    }
    sortBuffer->noMoreInput();
    auto spillStats = sortBuffer->spilledStats();

    int expectedOutputBufferIndex = 0;
    RowVectorPtr output = sortBuffer->getOutput(testData.maxOutputRows);
    while (output != nullptr) {
      ASSERT_EQ(
          output->size(),
          testData.expectedOutputRowCount[expectedOutputBufferIndex++]);
      output = sortBuffer->getOutput(testData.maxOutputRows);
    }

    if (!testData.triggerSpill) {
      ASSERT_FALSE(spillStats.has_value());
    } else {
      ASSERT_TRUE(spillStats.has_value());
      ASSERT_GT(spillStats->spilledRows, 0);
      ASSERT_LE(spillStats->spilledRows, totalNumInput);
      ASSERT_GT(spillStats->spilledBytes, 0);
      ASSERT_EQ(spillStats->spilledPartitions, 1);
      ASSERT_GT(spillStats->spilledFiles, 0);
    }
  }
}

TEST_F(SortBufferTest, spill) {
  struct {
    bool spillEnabled;
    bool memoryReservationFailure;
    uint64_t spillMemoryThreshold;
    bool spillTriggered;
    bool hybridSortEnabled;

    std::string debugString() const {
      return fmt::format(
          "spillEnabled:{}, memoryReservationFailure:{}, spillMemoryThreshold:{}, spillTriggered:{}, hybridSortEnabled:{}",
          spillEnabled,
          memoryReservationFailure,
          spillMemoryThreshold,
          spillTriggered,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {false, true, 0, false, false}, // spilling is not enabled.
      {false, true, 0, false, true}, // spilling is not enabled, hybrid enabled.
      {true,
       true,
       0,
       false,
       false}, // memory reservation failure won't trigger spilling.
      {true, true, 0, false, true}, // memory reservation failure won't trigger
                                    // spilling, hybrid enabled.
      {true,
       false,
       1000,
       true,
       false}, // threshold is small, spilling is triggered.
      {true,
       false,
       1000,
       true,
       true}, // threshold is small, spilling is triggered, hybrid enabled.
      {true,
       false,
       1000000,
       false,
       false} // threshold is too large, not triggered
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    // memory pool limit is 20M
    // Set 'kSpillableReservationGrowthPct' to an extreme large value to trigger
    // memory reservation failure and thus trigger disk spilling.
    auto spillableReservationGrowthPct =
        testData.memoryReservationFailure ? 100000 : 100;
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        false,
        0,
        executor_.get(),
        100,
        spillableReservationGrowthPct,
        0,
        0,
        0,
        0,
        0,
        0,
        "none");
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        testData.spillEnabled ? &spillConfig : nullptr,
        testData.spillMemoryThreshold,
        nullptr,
        testData.hybridSortEnabled);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("spillSource");
    VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
    uint64_t totalNumInput = 0;

    ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
    const auto peakSpillMemoryUsage =
        memory::spillMemoryPool()->stats().peakBytes;

    for (int i = 0; i < 3; ++i) {
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
      totalNumInput += 1024;
    }
    sortBuffer->noMoreInput();
    const auto spillStats = sortBuffer->spilledStats();

    if (!testData.spillTriggered) {
      ASSERT_FALSE(spillStats.has_value());
      if (!testData.spillEnabled) {
        BOLT_ASSERT_THROW(sortBuffer->spill(), "spill config is null");
      }
    } else {
      ASSERT_TRUE(spillStats.has_value());
      ASSERT_GT(spillStats->spilledRows, 0);
      ASSERT_LE(spillStats->spilledRows, totalNumInput);
      ASSERT_GT(spillStats->spilledBytes, 0);
      ASSERT_EQ(spillStats->spilledPartitions, 1);
      // SortBuffer shall not respect maxFileSize. Total files should be num
      // addInput() calls minus one which is the first one that has nothing to
      // spill.
      ASSERT_EQ(spillStats->spilledFiles, 3);
      sortBuffer.reset();
      ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
      if (memory::spillMemoryPool()->trackUsage()) {
        ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
        ASSERT_GE(
            memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
      }
    }
  }
}

DEBUG_ONLY_TEST_F(SortBufferTest, reserveMemoryGetOutput) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto spillConfig = common::SpillConfig(
      [&]() -> const std::string& { return spillDirectory->getPath(); },
      [&](uint64_t) {},
      "0.0.0",
      1000,
      false,
      1 << 20,
      executor_.get(),
      100,
      100000,
      0,
      0,
      0,
      0,
      0,
      0,
      "none",
      "",
      "disabled",
      "",
      true);
  folly::Synchronized<common::SpillStats> spillStats;
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      &spillConfig);

  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("spillSource");
  VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());

  TestScopedSpillInjection scopedSpillInjection(0);
  for (int i = 0; i < 3; ++i) {
    sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
  }

  std::atomic_bool noMoreInput{false};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::SortBuffer::noMoreInput",
      std::function<void(SortBuffer*)>(
          ([&](SortBuffer* sortBuffer) { noMoreInput.store(true); })));

  std::atomic_int numInputs{0};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            if (noMoreInput) {
              ++numInputs;
            }
          })));

  sortBuffer->noMoreInput();
  sortBuffer->getOutput(10000);
  ASSERT_EQ(numInputs, 1);
}

TEST_F(SortBufferTest, emptySpill) {
  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("emptySpillSource");

  struct {
    bool hasPostSpillData;
    bool hybridSortEnabled;

    std::string debugString() const {
      return fmt::format(
          "hasPostSpillData:{}, hybridSortEnabled:{}",
          hasPostSpillData,
          hybridSortEnabled);
    }
  } testSettings[] = {
      {false, false}, {false, true}, {true, false}, {true, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = getSpillConfig(spillDirectory->path);
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        &spillConfig,
        0,
        nullptr,
        testData.hybridSortEnabled);

    sortBuffer->spill();
    if (testData.hasPostSpillData) {
      VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
    }
    sortBuffer->noMoreInput();
    ASSERT_FALSE(sortBuffer->spilledStats());
  }
}

TEST_F(SortBufferTest, rowBasedSpillMemory) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  // memory pool limit is 20M
  // Set 'kSpillableReservationGrowthPct' to an extreme large value to trigger
  // memory reservation failure and thus trigger disk spilling.
  auto spillableReservationGrowthPct = 100000;
  auto spillConfig = common::SpillConfig(
      [&]() -> const std::string& { return spillDirectory->path; },
      [&](uint64_t) {},
      "0.0.0",
      1000,
      false,
      0,
      executor_.get(),
      100,
      spillableReservationGrowthPct,
      0,
      0,
      0,
      0,
      0,
      0,
      "none",
      "",
      "raw");
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      &spillConfig,
      1000);

  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("spillSource");
  VectorFuzzer fuzzer(
      {.vectorSize = 1024, .stringLength = 1024}, fuzzerPool.get());
  uint64_t totalNumInput = 0;

  ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
  const auto peakSpillMemoryUsage =
      memory::spillMemoryPool()->stats().peakBytes;

  for (int i = 0; i < 5; ++i) {
    sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
    totalNumInput += 1024;
  }
  sortBuffer->noMoreInput();
  const auto spillStats = sortBuffer->spilledStats();

  ASSERT_TRUE(spillStats.has_value());
  ASSERT_GT(spillStats->spilledRows, 0);
  ASSERT_LE(spillStats->spilledRows, totalNumInput);
  ASSERT_GT(spillStats->spilledBytes, 0);
  ASSERT_EQ(spillStats->spilledPartitions, 1);
  // SortBuffer shall not respect maxFileSize. Total files should be num
  // addInput() calls minus one which is the first one that has nothing to
  // spill.
  ASSERT_EQ(spillStats->spilledFiles, 5);
  auto rowVector = sortBuffer->getOutput(1024);
  ASSERT_LT(sortBuffer->pool()->currentBytes(), 12 * 1024 * 1024);
  sortBuffer.reset();
  ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
  if (memory::spillMemoryPool()->trackUsage()) {
    ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
    ASSERT_GE(
        memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
  }
}

TEST_F(SortBufferTest, spillWithHybridModeValidateOutput) {
  // Test hybrid sort mode with spilling enabled and validate output
  // correctness.
  struct {
    bool hybridSortEnabled;
    std::string debugString() const {
      return fmt::format("hybridSortEnabled:{}", hybridSortEnabled);
    }
  } testSettings[] = {
      {false}, // Spill without hybrid mode
      {true} // Spill with hybrid mode
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        false,
        0,
        executor_.get(),
        5,
        100, // spillableReservationGrowthPct to trigger spilling
        0,
        0,
        0,
        0,
        0,
        100, // testSpillPct
        "none");

    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        &spillConfig,
        0,
        nullptr,
        testData.hybridSortEnabled);

    TestScopedSpillInjection scopedSpillInjection(100);

    // Create multiple batches to trigger spilling.
    for (int batch = 0; batch < 2; ++batch) {
      RowVectorPtr data = makeRowVector(
          {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
           makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted-2 column
           makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
           makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
           makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}), // sorted-1 column
           makeFlatVector<std::string>(
               {"hello", "world", "today", "is", "great"})});
      sortBuffer->addInput(data);
    }

    sortBuffer->noMoreInput();
    const auto spillStats = sortBuffer->spilledStats();

    // Validate spilling occurred
    ASSERT_TRUE(spillStats.has_value());
    ASSERT_GT(spillStats->spilledRows, 0);
    ASSERT_GT(spillStats->spilledBytes, 0);
    ASSERT_EQ(spillStats->spilledPartitions, 1);

    // // Log spill statistics for verification
    // std::cout << "\n=== Spill Statistics ===" << std::endl;
    // std::cout << "Hybrid Mode Enabled: " << testData.hybridSortEnabled <<
    // std::endl; std::cout << "Spilled Rows: " << spillStats->spilledRows <<
    // std::endl; std::cout << "Spilled Bytes: " << spillStats->spilledBytes <<
    // std::endl; std::cout << "Spilled Files: " << spillStats->spilledFiles <<
    // std::endl; std::cout << "========================\n" << std::endl;

    // Validate output correctness: check that all columns are correctly sorted
    std::vector<int64_t> expectedC0 = {
        1, 1, 3, 3, 2, 2, 5, 5, 4, 4}; // sorted by c4, then c1
    std::vector<int32_t> expectedC1 = {
        5, 5, 3, 3, 4, 4, 1, 1, 2, 2}; // sorted by c4, then c1
    std::vector<int16_t> expectedC2 = {1, 1, 3, 3, 2, 2, 5, 5, 4, 4};
    std::vector<float> expectedC3 = {
        1.1, 1.1, 3.3, 3.3, 2.2, 2.2, 5.5, 5.5, 4.4, 4.4};
    std::vector<double> expectedC4 = {
        1.1, 1.1, 2.2, 2.2, 2.2, 2.2, 5.5, 5.5, 5.5, 5.5};
    std::vector<std::string> expectedC5 = {
        "hello",
        "hello",
        "today",
        "today",
        "world",
        "world",
        "great",
        "great",
        "is",
        "is"};

    RowVectorPtr output;
    int totalRowsVerified = 0;
    while ((output = sortBuffer->getOutput(10000)) != nullptr) {
      auto c0Column = output->childAt(0)->asFlatVector<int64_t>();
      auto c1Column = output->childAt(1)->asFlatVector<int32_t>();
      auto c2Column = output->childAt(2)->asFlatVector<int16_t>();
      auto c3Column = output->childAt(3)->asFlatVector<float>();
      auto c4Column = output->childAt(4)->asFlatVector<double>();
      auto c5Column = output->childAt(5)->asFlatVector<StringView>();

      for (int i = 0; i < output->size(); ++i) {
        ASSERT_EQ(c0Column->valueAt(i), expectedC0[totalRowsVerified]);
        ASSERT_EQ(c1Column->valueAt(i), expectedC1[totalRowsVerified]);
        ASSERT_EQ(c2Column->valueAt(i), expectedC2[totalRowsVerified]);
        ASSERT_FLOAT_EQ(c3Column->valueAt(i), expectedC3[totalRowsVerified]);
        ASSERT_DOUBLE_EQ(c4Column->valueAt(i), expectedC4[totalRowsVerified]);
        ASSERT_EQ(c5Column->valueAt(i), expectedC5[totalRowsVerified]);
        totalRowsVerified++;
      }
    }

    // Verify we got all 10 rows (2 batches × 5 rows)
    ASSERT_EQ(totalRowsVerified, 10);
  }
}

} // namespace bytedance::bolt::functions::test
