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

#include <atomic>

#include <folly/synchronization/Baton.h>
#include <folly/synchronization/Latch.h>

#include "bolt/common/base/Fs.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/testutil/TempFilePath.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/dwio/common/tests/utils/DataSetBuilder.h"
#include "bolt/dwio/paimon/deletionvectors/DeletionFileReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/OutputBufferManager.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/TableScan.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/Type.h"
#include "bolt/type/tests/SubfieldFiltersBuilder.h"
#include "folly/experimental/EventCount.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::core;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::common::test;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::dwio::common;

class ParquetRowGroupFilterTest : public virtual HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
  }

 protected:
  template <typename K, typename V>
  void testSubfieldPruning(
      int32_t numBatches,
      int32_t vectorSize,
      const std::string& remainingFilter,
      std::vector<common::Subfield>&& bSubfields,
      int32_t expectedSize,
      const std::function<vector_size_t(vector_size_t)>& sizeAt,
      const std::function<K(vector_size_t, int32_t)>& keyAt,
      const std::function<V(vector_size_t, int32_t)>& valueAt,
      const bool nulls = false);
};

template <typename K, typename V>
void ParquetRowGroupFilterTest::testSubfieldPruning(
    int32_t numBatches,
    int32_t vectorSize,
    const std::string& remainingFilter,
    std::vector<common::Subfield>&& bSubfields,
    int32_t expectedSize,
    const std::function<vector_size_t(vector_size_t)>& sizeAt,
    const std::function<K(vector_size_t, int32_t)>& keyAt,
    const std::function<V(vector_size_t, int32_t)>& valueAt,
    const bool nulls) {
  std::vector<RowVectorPtr> batch;
  for (int j = 0; j < numBatches; ++j) {
    auto mapVector = this->makeMapVector<K, V>(
        vectorSize,
        [&](vector_size_t i) { return sizeAt(i); },
        [&](auto i) { return keyAt(i, j); },
        [&](auto i) { return valueAt(i, j); });
    if (nulls) {
      for (auto i = 0; i < vectorSize; i++) {
        if (i % 2 == 0) {
          mapVector->setNull(i, true);
        }
      }
    }

    auto vector = this->makeRowVector(
        {"a", "b"},
        {this->makeFlatVector<int64_t>(vectorSize, folly::identity),
         mapVector});
    batch.push_back(vector);
  }
  auto mapType = batch[0]->childAt(1)->type();
  auto rowType = asRowType(batch[0]->type());
  auto filePath = ::bytedance::bolt::test::TempFilePath::create();

  auto path = filePath->path;
  auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink = std::make_unique<WriteFileSink>(std::move(localWriteFile), path);

  bytedance::bolt::parquet::WriterOptions options;
  options.memoryPool = this->rootPool_.get();

  std::unique_ptr<bytedance::bolt::parquet::Writer> writer_ =
      std::make_unique<bytedance::bolt::parquet::Writer>(
          std::move(sink), options, rowType);

  for (auto& vector : batch) {
    writer_->write(vector);
    writer_->flush();
  }
  writer_->close();

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["a"] = std::make_shared<HiveColumnHandle>(
      "a", HiveColumnHandle::ColumnType::kRegular, BIGINT(), BIGINT());
  assignments["b"] = std::make_shared<HiveColumnHandle>(
      "b",
      HiveColumnHandle::ColumnType::kRegular,
      mapType,
      mapType,
      std::move(bSubfields));

  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .remainingFilter(remainingFilter)
                .isFilterPushdownEnabled(true)
                .dataColumns(rowType)
                .assignments(assignments)
                .endTableScan()
                .planNode();
  auto split = makeHiveConnectorSplits(
      filePath->path, 1, dwio::common::FileFormat::PARQUET)[0];
  auto result = AssertQueryBuilder(op).split(split).copyResults(this->pool());

  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->size(), expectedSize);
}

TEST_F(ParquetRowGroupFilterTest, uniqueKeyMatch) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[0]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[0] < 10",
      std::move(subfields),
      1,
      [](auto i) { return i; },
      [](auto i, auto j) { return i + j; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, uniqueKeyMatchOtherValue) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[0]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[0] < 5",
      std::move(subfields),
      1,
      [](auto i) { return i; },
      [](auto i, auto j) { return i + j; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, uniqueKeyMatchLargerVec) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[0]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      20,
      "b[0] < 15",
      std::move(subfields),
      1,
      [](auto i) { return i; },
      [](auto i, auto j) { return i + j; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, uniqueKeyNoMatch) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[0]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[0] > 15",
      std::move(subfields),
      0,
      [](auto i) { return i; },
      [](auto i, auto j) { return i + j; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, nonUniqueKeyNoMatch) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[1] > 15",
      std::move(subfields),
      0,
      [](auto i) { return i; },
      [](auto i, auto j) { return i + j; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, ConstantSizeAt) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[1] > 50",
      std::move(subfields),
      5,
      [](auto /*i*/) { return 5; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, ConstantKey) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[5]");
  this->testSubfieldPruning<int64_t, int64_t>(
      5,
      10,
      "b[5] > 30",
      std::move(subfields),
      40,
      [](auto /*i*/) { return 10; },
      [](auto i, auto /*j*/) { return 5; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, SequentialKeys) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[4]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[4] > 40",
      std::move(subfields),
      6,
      [](auto /*i*/) { return 10; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, ValueDependsOnBatch) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[1] > 50",
      std::move(subfields),
      5,
      [](auto /*i*/) { return 10; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, NoMatchingRows) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[1] > 1000",
      std::move(subfields),
      0,
      [](auto /*i*/) { return 10; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, FirstRowOfEachBatchMatches) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      5,
      5,
      "b[1] >= 0",
      std::move(subfields),
      5,
      [](auto /*i*/) { return 2; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, LargeVectorSize) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[50]");
  this->testSubfieldPruning<int64_t, int64_t>(
      2,
      100,
      "b[50] > 10",
      std::move(subfields),
      2,
      [](auto /*i*/) { return 100; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + j; });
}

TEST_F(ParquetRowGroupFilterTest, LargeNumBatches) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      100,
      5,
      "b[1] > 500",
      std::move(subfields),
      50,
      [](auto /*i*/) { return 2; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, ZeroSizeMap) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[0]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[0] > 0",
      std::move(subfields),
      0,
      [](auto /*i*/) { return 0; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, NonZeroKey) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[10]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[10] > 50",
      std::move(subfields),
      4,
      [](auto /*i*/) { return 20; },
      [](auto i, auto /*j*/) { return i + 10; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, FilterOnSecondKey) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[1] < 10",
      std::move(subfields),
      1,
      [](auto i) { return i; },
      [](auto i, auto j) { return i + j; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, StringKeyMatch) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b");
  std::vector<std::string> keys;
  this->testSubfieldPruning<StringView, int64_t>(
      10,
      10,
      "element_at(b,'key1') < 10",
      std::move(subfields),
      1,
      [](auto i) { return i; },
      [&keys](auto i, auto j) {
        keys.emplace_back(fmt::format("key{}", i + j));
        return StringView(keys.back());
      },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, NoMatchStringKey) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[\"key100\"]");
  std::vector<std::string> keys;
  this->testSubfieldPruning<StringView, int64_t>(
      10,
      10,
      "element_at(b, 'key100') > 15",
      std::move(subfields),
      0,
      [](auto i) { return i; },
      [&keys](auto i, auto j) {
        keys.emplace_back(fmt::format("key{}", i + j));
        return StringView(keys.back());
      },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, FilterOnMapValue) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "element_at(b, 1) > 50",
      std::move(subfields),
      5,
      [](auto /*i*/) { return 10; },
      [](auto i, auto /*j*/) { return i; },
      [](auto i, auto j) { return i + 10 * j; });
}

TEST_F(ParquetRowGroupFilterTest, NullMap) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[0]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[0] < 10",
      std::move(subfields),
      1,
      [](auto i) { return i % 2 == 0 ? 0 : i; },
      [](auto i, auto j) { return i + j; },
      [](auto i, auto j) { return i + 10 * j; },
      true);
}

TEST_F(ParquetRowGroupFilterTest, ComplexFilter) {
  std::vector<common::Subfield> subfields;
  subfields.emplace_back("b[0]");
  subfields.emplace_back("b[1]");
  this->testSubfieldPruning<int64_t, int64_t>(
      10,
      10,
      "b[0] > 5 AND b[1] < 10",
      std::move(subfields),
      1,
      [](auto /*i*/) { return 2; },
      [](auto i, auto /*j*/) { return i % 2; },
      [](auto i, auto j) {
        auto r = i / 2;
        bool is_the_one = (r == 3 && j == 5);
        if (i % 2 == 0) { // key 0
          return is_the_one ? 6 : 0;
        } else { // key 1
          return is_the_one ? 9 : 20;
        }
      });
}
