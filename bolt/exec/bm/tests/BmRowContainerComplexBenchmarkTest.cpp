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

#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

class BmRowContainerComplexBenchmarkTest : public testing::Test,
                                           public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(BmRowContainerComplexBenchmarkTest, ComplexInputsRoundTripEqually) {
  for (const auto dataset :
       {DatasetKind::kArray, DatasetKind::kMap, DatasetKind::kRow}) {
    auto opts = options(dataset, 4096);
    opts.batchRows = 64;
    BenchmarkContext context("complex-round-trip", opts.dataBytes);
    auto input = makeInputBatch(pool(), opts, 0, opts.batchRows);
    OldStoredRows oldRows{
        makeOldKeyRowContainer(dataset, context.pool.get()), {}};
    BmStoredRows bmRows{
        makeBmKeyRowContainer(dataset, context.bufferManager), {}};
    OldStoredRows equalOldRows{
        makeOldKeyRowContainer(dataset, context.pool.get()), {}};
    BmStoredRows equalBmRows{
        makeBmKeyRowContainer(dataset, context.bufferManager), {}};
    // Each complex dataset is a single key column.
    ASSERT_EQ(1, columnTypes(dataset).size()) << datasetName(dataset);
    storeInputBatchOld(*oldRows.container, input, &oldRows.rows);
    storeInputBatchBm(*bmRows.container, input, &bmRows.rows);
    storeInputBatchOld(*equalOldRows.container, input, &equalOldRows.rows);
    storeInputBatchBm(*equalBmRows.container, input, &equalBmRows.rows);

    auto oldResult = BaseVector::create(
        columnTypes(dataset).front(), oldRows.rows.size(), pool());
    auto bmResult = BaseVector::create(
        columnTypes(dataset).front(), bmRows.rows.size(), pool());
    oldRows.container->extractColumn(
        oldRows.rows.data(), oldRows.rows.size(), 0, 0, oldResult);
    bmRows.container->extractColumnResident(
        bmRows.rows.data(), bmRows.rows.size(), 0, bmResult);

    ASSERT_EQ(oldRows.rows.size(), bmRows.rows.size());
    for (vector_size_t row = 0; row < oldResult->size(); ++row) {
      EXPECT_TRUE(oldResult->equalValueAt(bmResult.get(), row, row))
          << datasetName(dataset) << " row " << row;
    }
    EXPECT_EQ(
        0,
        oldRows.container->compare(oldRows.rows[0], equalOldRows.rows[0], 0));
    EXPECT_EQ(
        0, bmRows.container->compare(bmRows.rows[0], equalBmRows.rows[0], 0));
    EXPECT_EQ(
        (oldRows.container->compare(oldRows.rows[0], oldRows.rows[1], 0) > 0) -
            (oldRows.container->compare(oldRows.rows[0], oldRows.rows[1], 0) <
             0),
        (bmRows.container->compare(bmRows.rows[0], bmRows.rows[1], 0) > 0) -
            (bmRows.container->compare(bmRows.rows[0], bmRows.rows[1], 0) < 0))
        << datasetName(dataset);
  }
}

TEST_F(BmRowContainerComplexBenchmarkTest, ComplexInputsSupportBatchStore) {
  for (const auto dataset :
       {DatasetKind::kArray, DatasetKind::kMap, DatasetKind::kRow}) {
    auto opts = options(dataset, 4096);
    opts.batchRows = 64;
    BenchmarkContext context("complex-batch-store", opts.dataBytes);
    auto input = makeInputBatch(pool(), opts, 0, opts.batchRows);
    auto oldContainer = makeOldKeyRowContainer(dataset, context.pool.get());
    auto bmContainer = makeBmKeyRowContainer(dataset, context.bufferManager);
    std::vector<char*> bmRows;

    EXPECT_NO_THROW(storeInputBatchOldBatch(*oldContainer, input))
        << datasetName(dataset);
    EXPECT_NO_THROW(storeInputBatchBmBatch(*bmContainer, input, &bmRows))
        << datasetName(dataset);
    EXPECT_EQ(input->size(), oldContainer->numRows());
    EXPECT_EQ(input->size(), bmContainer->numRows());

    std::vector<char*> oldRows(input->size());
    RowContainerIterator iterator;
    ASSERT_EQ(
        input->size(),
        oldContainer->listRows(&iterator, input->size(), oldRows.data()));
    ASSERT_EQ(input->size(), bmRows.size());

    auto oldResult =
        BaseVector::create(columnTypes(dataset).front(), input->size(), pool());
    auto bmResult =
        BaseVector::create(columnTypes(dataset).front(), input->size(), pool());
    oldContainer->extractColumn(
        oldRows.data(), oldRows.size(), 0, 0, oldResult);
    bmContainer->extractColumnResident(
        bmRows.data(), bmRows.size(), 0, bmResult);
    for (vector_size_t row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(input->childAt(0)->equalValueAt(oldResult.get(), row, row))
          << datasetName(dataset) << " old row " << row;
      EXPECT_TRUE(input->childAt(0)->equalValueAt(bmResult.get(), row, row))
          << datasetName(dataset) << " bm row " << row;
    }

    std::vector<uint64_t> oldHashes(oldRows.size());
    oldContainer->hash(
        0,
        folly::Range<char**>(oldRows.data(), oldRows.size()),
        false,
        oldHashes.data());
    for (size_t row = 0; row < bmRows.size(); ++row) {
      EXPECT_EQ(oldHashes[row], bmContainer->hash(bmRows[row], 0))
          << datasetName(dataset) << " row " << row;
    }
  }
}

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
