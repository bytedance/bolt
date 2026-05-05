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

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <paimon/data/decimal.h>
#include <paimon/data/timestamp.h>
#include <paimon/predicate/literal.h>
#include <paimon/predicate/predicate_builder.h>
#include <paimon/result.h>
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonFilterTranslator.h"
#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::paimon;
using namespace bytedance::bolt::dwio::common;

namespace {

class PaimonFilterPushdownTest : public testing::Test,
                                 public bytedance::bolt::test::VectorTestBase {
 protected:
  static constexpr int32_t kBatchSize = 128;
  static constexpr int32_t kRows = 1024;

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    bytedance::bolt::dwio::common::LocalFileSink::registerFactory();
  }

  void SetUp() override {
    rootPool_ =
        memory::memoryManager()->addRootPool("PaimonFilterPushdownTest");
    leafPool_ = rootPool_->addLeafChild("leaf");
    tempDir_ = std::filesystem::temp_directory_path();

    buildData();

    const auto* testInfo =
        testing::UnitTest::GetInstance()->current_test_info();
    auto filename =
        std::string("paimon_filter_pushdown_") + testInfo->name() + ".parquet";
    parquetPath_ = (tempDir_ / filename).string();

    writeParquet();
  }

  void buildData() {
    rowType_ =
        ROW({"ti", "si", "i", "bi", "f", "d", "s", "bin", "ts", "dec", "ldec"},
            {TINYINT(),
             SMALLINT(),
             INTEGER(),
             BIGINT(),
             REAL(),
             DOUBLE(),
             VARCHAR(),
             VARBINARY(),
             TIMESTAMP(),
             DECIMAL(10, 2),
             DECIMAL(20, 3)});

    tinyValues_.resize(kRows);
    smallValues_.resize(kRows);
    intValues_.resize(kRows);
    bigintValues_.resize(kRows);
    floatValues_.resize(kRows);
    doubleValues_.resize(kRows);
    stringValues_.resize(kRows);
    binaryValues_.resize(kRows);
    timestampValues_.resize(kRows);
    decimalValues_.resize(kRows);
    longDecimalValues_.resize(kRows);

    for (int32_t row = 0; row < kRows; ++row) {
      tinyValues_[row] = static_cast<int8_t>(row);
      smallValues_[row] = static_cast<int16_t>(100 + (row * 2));
      intValues_[row] = row * 10;
      bigintValues_[row] = 1000 + (row * 100);
      floatValues_[row] = static_cast<float>(row) * 0.5F;
      doubleValues_[row] = static_cast<double>(row) * 1.25;

      if (row % 5 == 0) {
        stringValues_[row] = std::nullopt;
      } else {
        stringValues_[row] = "str_" + std::to_string(row);
      }

      if (row % 6 == 0) {
        binaryValues_[row] = std::nullopt;
      } else {
        binaryValues_[row] = "bin_" + std::to_string(row);
      }

      timestampValues_[row] = Timestamp::fromMillis(
          1'700'000'000'000 + (static_cast<int64_t>(row) * 1000));
      decimalValues_[row] = static_cast<int64_t>(row) * 100; // scale=2
      longDecimalValues_[row] =
          static_cast<__int128_t>(row) * 1000LL; // scale=3
    }

    auto tinyVec = makeFlatVector<int8_t>(
        kRows, [&](auto row) { return tinyValues_[row]; });
    auto smallVec = makeFlatVector<int16_t>(
        kRows, [&](auto row) { return smallValues_[row]; });
    auto intVec = makeFlatVector<int32_t>(
        kRows, [&](auto row) { return intValues_[row]; });
    auto bigintVec = makeFlatVector<int64_t>(
        kRows, [&](auto row) { return bigintValues_[row]; });
    auto floatVec = makeFlatVector<float>(
        kRows, [&](auto row) { return floatValues_[row]; });
    auto doubleVec = makeFlatVector<double>(
        kRows, [&](auto row) { return doubleValues_[row]; });

    stringStorage_.clear();
    stringStorage_.reserve(kRows);
    std::vector<std::optional<StringView>> stringViews;
    stringViews.reserve(kRows);
    for (const auto& value : stringValues_) {
      if (value.has_value()) {
        stringStorage_.push_back(*value);
        stringViews.emplace_back(StringView(stringStorage_.back()));
      } else {
        stringViews.emplace_back(std::nullopt);
      }
    }
    auto stringVec = makeNullableFlatVector<StringView>(stringViews);

    binaryStorage_.clear();
    binaryStorage_.reserve(kRows);
    std::vector<std::optional<StringView>> binaryViews;
    binaryViews.reserve(kRows);
    for (const auto& value : binaryValues_) {
      if (value.has_value()) {
        binaryStorage_.push_back(*value);
        binaryViews.emplace_back(StringView(binaryStorage_.back()));
      } else {
        binaryViews.emplace_back(std::nullopt);
      }
    }
    auto binaryVec =
        makeNullableFlatVector<StringView>(binaryViews, VARBINARY());

    auto tsVec = makeFlatVector<Timestamp>(
        kRows, [&](auto row) { return timestampValues_[row]; });

    auto decVec = makeFlatVector<int64_t>(
        kRows,
        [&](auto row) { return decimalValues_[row]; },
        /*isNullAt=*/nullptr,
        DECIMAL(10, 2));
    auto ldecVec = makeFlatVector<__int128_t>(
        kRows,
        [&](auto row) { return longDecimalValues_[row]; },
        /*isNullAt=*/nullptr,
        DECIMAL(20, 3));

    data_ = makeRowVector(
        {tinyVec,
         smallVec,
         intVec,
         bigintVec,
         floatVec,
         doubleVec,
         stringVec,
         binaryVec,
         tsVec,
         decVec,
         ldecVec});
  }

  void writeParquet() {
    writeParquet(data_, rowType_, parquetPath_);
  }

  void writeParquet(
      const RowVectorPtr& data,
      const RowTypePtr& rowType,
      const std::string& path) {
    auto sink = FileSink::create(path, {.pool = leafPool_.get()});
    bytedance::bolt::parquet::WriterOptions opts;
    opts.memoryPool = leafPool_.get();
    opts.enableFlushBasedOnBlockSize = true;
    bytedance::bolt::parquet::Writer writer(
        std::move(sink),
        opts,
        rootPool_,
        ::arrow::default_memory_pool(),
        rowType);
    writer.write(data);
    writer.close();
  }

  RowVectorPtr readWithPredicate(
      const std::shared_ptr<::paimon::Predicate>& predicate) {
    return readWithPredicate(predicate, parquetPath_, rowType_);
  }

  RowVectorPtr readWithPredicate(
      const std::shared_ptr<::paimon::Predicate>& predicate,
      const std::string& path,
      const RowTypePtr& rowType) {
    PaimonParquetReader format({});
    auto rbRes = format.CreateReaderBuilder(kBatchSize);
    EXPECT_TRUE(rbRes.ok());
    if (!rbRes.ok()) {
      return RowVector::createEmpty(rowType, leafPool_.get());
    }
    auto builder = std::move(rbRes).value();

    auto paimonPool = std::make_shared<BoltPaimonMemoryPool>(leafPool_.get());
    builder->WithMemoryPool(paimonPool);

    auto readerRes = builder->Build(path);
    EXPECT_TRUE(readerRes.ok());
    if (!readerRes.ok()) {
      return RowVector::createEmpty(rowType, leafPool_.get());
    }
    auto fileReader = std::move(readerRes).value();

    auto dummyVector = BaseVector::create(rowType, 0, leafPool_.get());
    auto arrowSchema = std::make_unique<::ArrowSchema>();
    ArrowOptions arrowOptions;
    exportToArrow(dummyVector, *arrowSchema, arrowOptions);

    auto status =
        fileReader->SetReadSchema(arrowSchema.get(), predicate, std::nullopt);
    EXPECT_TRUE(status.ok()) << status.message();

    std::vector<RowVectorPtr> batches;
    while (true) {
      auto batchRes = fileReader->NextBatch();
      if (!batchRes.ok()) {
        break;
      }
      if (::paimon::BatchReader::IsEofBatch(batchRes.value())) {
        break;
      }

      auto pair = std::move(batchRes).value();
      auto& arr = pair.first;
      auto& sch = pair.second;

      auto batch = importFromArrowAsOwner(*sch, *arr, {}, leafPool_.get());
      auto rowBatch = std::dynamic_pointer_cast<RowVector>(batch);
      EXPECT_TRUE(rowBatch != nullptr);
      if (rowBatch) {
        batches.push_back(rowBatch);
      }

      if (arr && arr->release) {
        arr->release(arr.get());
      }
      if (sch && sch->release) {
        sch->release(sch.get());
      }
    }
    fileReader->Close();

    if (batches.empty()) {
      return RowVector::createEmpty(rowType, leafPool_.get());
    }

    auto result = RowVector::createEmpty(rowType, leafPool_.get());
    for (const auto& batch : batches) {
      result->append(batch.get());
    }
    return result;
  }

  /// End-to-end filter pushdown test: writes data to parquet, verifies filter
  /// translation produces at least `minExpectedFilters` subfield filters, reads
  /// through the parquet reader with the predicate, and asserts the result
  /// matches the expected rows.
  ///
  /// @param data              The row data to write to parquet.
  /// @param rowType           The schema for the data.
  /// @param predicate         The paimon predicate to push down.
  /// @param minExpectedFilters Minimum number of subfield filters expected from
  ///                          toTypedExpr() + toSubfieldFilters(). Use 0 when
  ///                          the predicate is known to not translate (e.g. OR
  ///                          across columns).
  /// @param expectedFn        Predicate function: returns true for rows that
  ///                          should pass the filter.
  void runPushdownTest(
      const RowVectorPtr& data,
      const RowTypePtr& rowType,
      const std::shared_ptr<::paimon::Predicate>& predicate,
      size_t minExpectedFilters,
      const std::function<bool(vector_size_t)>& expectedFn) {
    // 1. Write data to a temp parquet file.
    const auto* testInfo =
        testing::UnitTest::GetInstance()->current_test_info();
    auto filename = std::string("pushdown_") + testInfo->name() + ".parquet";
    auto path = (tempDir_ / filename).string();
    writeParquet(data, rowType, path);

    // 2. Verify filter translation.
    auto exprResult =
        PaimonFilterTranslator::toTypedExpr(predicate, leafPool_.get());
    if (minExpectedFilters > 0) {
      ASSERT_TRUE(exprResult.ok())
          << "toTypedExpr failed: " << exprResult.reason;
      auto filters =
          PaimonFilterTranslator::toSubfieldFilters(exprResult.value);
      ASSERT_GE(filters.size(), minExpectedFilters)
          << "Expected at least " << minExpectedFilters
          << " subfield filter(s), got " << filters.size();
    }

    // 3. Read with predicate and verify correctness.
    auto actual = readWithPredicate(predicate, path, rowType);
    auto expected = expectedByPredicate(data, rowType, expectedFn);
    assertRowVectorsEqual(expected, actual);
  }

  /// Overload that uses the fixture's built-in data_ and rowType_.
  void runPushdownTest(
      const std::shared_ptr<::paimon::Predicate>& predicate,
      size_t minExpectedFilters,
      const std::function<bool(vector_size_t)>& expectedFn) {
    runPushdownTest(data_, rowType_, predicate, minExpectedFilters, expectedFn);
  }

  RowVectorPtr expectedByPredicate(
      const RowVectorPtr& data,
      const RowTypePtr& rowType,
      const std::function<bool(vector_size_t)>& predicate) {
    std::vector<vector_size_t> indices;
    for (vector_size_t row = 0; row < data->size(); ++row) {
      if (predicate(row)) {
        indices.push_back(row);
      }
    }
    if (indices.empty()) {
      return RowVector::createEmpty(rowType, leafPool_.get());
    }

    auto indicesBuffer = makeIndices(indices);
    std::vector<VectorPtr> children;
    children.reserve(data->childrenSize());
    for (column_index_t i = 0; i < data->childrenSize(); ++i) {
      auto child = BaseVector::wrapInDictionary(
          BufferPtr(nullptr), indicesBuffer, indices.size(), data->childAt(i));
      children.push_back(child);
    }
    return std::make_shared<RowVector>(
        leafPool_.get(), rowType, BufferPtr(nullptr), indices.size(), children);
  }

  RowVectorPtr expectedByPredicate(
      const std::function<bool(vector_size_t)>& predicate) {
    return expectedByPredicate(data_, rowType_, predicate);
  }

  static void assertRowVectorsEqual(
      const RowVectorPtr& expected,
      const RowVectorPtr& actual) {
    ASSERT_TRUE(expected != nullptr);
    ASSERT_TRUE(actual != nullptr);
    ASSERT_EQ(expected->size(), actual->size());
    ASSERT_EQ(*expected->type(), *actual->type());
    for (vector_size_t i = 0; i < expected->size(); ++i) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i))
          << "Row " << i << " expected: " << expected->toString(i)
          << " got: " << actual->toString(i);
    }
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::filesystem::path tempDir_;
  std::string parquetPath_;

  RowTypePtr rowType_;
  RowVectorPtr data_;

  std::vector<int8_t> tinyValues_;
  std::vector<int16_t> smallValues_;
  std::vector<int32_t> intValues_;
  std::vector<int64_t> bigintValues_;
  std::vector<float> floatValues_;
  std::vector<double> doubleValues_;
  std::vector<std::optional<std::string>> stringValues_;
  std::vector<std::optional<std::string>> binaryValues_;
  std::vector<Timestamp> timestampValues_;
  std::vector<int64_t> decimalValues_;
  std::vector<__int128_t> longDecimalValues_;
  std::vector<std::string> stringStorage_;
  std::vector<std::string> binaryStorage_;
};

TEST_F(PaimonFilterPushdownTest, IntEquality) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          2, "i", ::paimon::FieldType::INT, ::paimon::Literal(50)),
      1,
      [&](auto row) { return intValues_[row] == 50; });
}

TEST_F(PaimonFilterPushdownTest, IntNotEqual) {
  runPushdownTest(
      ::paimon::PredicateBuilder::NotEqual(
          2, "i", ::paimon::FieldType::INT, ::paimon::Literal(50)),
      1,
      [&](auto row) { return intValues_[row] != 50; });
}

TEST_F(PaimonFilterPushdownTest, IntBetween) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Between(
          2,
          "i",
          ::paimon::FieldType::INT,
          ::paimon::Literal(20),
          ::paimon::Literal(60)),
      1,
      [&](auto row) { return intValues_[row] >= 20 && intValues_[row] <= 60; });
}

TEST_F(PaimonFilterPushdownTest, IntGreaterLess) {
  runPushdownTest(
      ::paimon::PredicateBuilder::GreaterThan(
          2, "i", ::paimon::FieldType::INT, ::paimon::Literal(80)),
      1,
      [&](auto row) { return intValues_[row] > 80; });
}

TEST_F(PaimonFilterPushdownTest, IntInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::In(
          2,
          "i",
          ::paimon::FieldType::INT,
          {::paimon::Literal(10),
           ::paimon::Literal(30),
           ::paimon::Literal(70)}),
      1,
      [&](auto row) {
        return intValues_[row] == 10 || intValues_[row] == 30 ||
            intValues_[row] == 70;
      });
}

TEST_F(PaimonFilterPushdownTest, IntNotInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::NotIn(
          2,
          "i",
          ::paimon::FieldType::INT,
          {::paimon::Literal(10),
           ::paimon::Literal(30),
           ::paimon::Literal(70)}),
      1,
      [&](auto row) {
        return intValues_[row] != 10 && intValues_[row] != 30 &&
            intValues_[row] != 70;
      });
}

TEST_F(PaimonFilterPushdownTest, FloatRange) {
  runPushdownTest(
      ::paimon::PredicateBuilder::GreaterThan(
          4, "f", ::paimon::FieldType::FLOAT, ::paimon::Literal(5.0F)),
      1,
      [&](auto row) { return floatValues_[row] > 5.0F; });
}

TEST_F(PaimonFilterPushdownTest, DoubleRange) {
  runPushdownTest(
      ::paimon::PredicateBuilder::LessThan(
          5, "d", ::paimon::FieldType::DOUBLE, ::paimon::Literal(10.0)),
      1,
      [&](auto row) { return doubleValues_[row] < 10.0; });
}

TEST_F(PaimonFilterPushdownTest, StringEquality) {
  std::string value = "str_7";
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          6,
          "s",
          ::paimon::FieldType::STRING,
          ::paimon::Literal(
              ::paimon::FieldType::STRING, value.data(), value.size())),
      1,
      [&](auto row) {
        return stringValues_[row].has_value() &&
            stringValues_[row].value() == "str_7";
      });
}

TEST_F(PaimonFilterPushdownTest, StringRange) {
  std::string lower = "str_10";
  std::string upper = "str_15";
  runPushdownTest(
      ::paimon::PredicateBuilder::Between(
          6,
          "s",
          ::paimon::FieldType::STRING,
          ::paimon::Literal(
              ::paimon::FieldType::STRING, lower.data(), lower.size()),
          ::paimon::Literal(
              ::paimon::FieldType::STRING, upper.data(), upper.size())),
      1,
      [&](auto row) {
        if (!stringValues_[row].has_value()) {
          return false;
        }
        const auto& v = stringValues_[row].value();
        return v >= lower && v <= upper;
      });
}

TEST_F(PaimonFilterPushdownTest, StringInList) {
  std::string v1 = "str_3";
  std::string v2 = "str_9";
  runPushdownTest(
      ::paimon::PredicateBuilder::In(
          6,
          "s",
          ::paimon::FieldType::STRING,
          {::paimon::Literal(::paimon::FieldType::STRING, v1.data(), v1.size()),
           ::paimon::Literal(
               ::paimon::FieldType::STRING, v2.data(), v2.size())}),
      1,
      [&](auto row) {
        return stringValues_[row].has_value() &&
            (stringValues_[row].value() == v1 ||
             stringValues_[row].value() == v2);
      });
}

TEST_F(PaimonFilterPushdownTest, StringNotInList) {
  std::string v1 = "str_3";
  std::string v2 = "str_9";
  runPushdownTest(
      ::paimon::PredicateBuilder::NotIn(
          6,
          "s",
          ::paimon::FieldType::STRING,
          {::paimon::Literal(::paimon::FieldType::STRING, v1.data(), v1.size()),
           ::paimon::Literal(
               ::paimon::FieldType::STRING, v2.data(), v2.size())}),
      1,
      [&](auto row) {
        return stringValues_[row].has_value() &&
            stringValues_[row].value() != v1 &&
            stringValues_[row].value() != v2;
      });
}

TEST_F(PaimonFilterPushdownTest, TimestampRange) {
  auto tsLower =
      ::paimon::Timestamp::FromEpochMillis(1'700'000'000'000 + (5 * 1000));
  auto tsUpper =
      ::paimon::Timestamp::FromEpochMillis(1'700'000'000'000 + (8 * 1000));
  runPushdownTest(
      ::paimon::PredicateBuilder::Between(
          8,
          "ts",
          ::paimon::FieldType::TIMESTAMP,
          ::paimon::Literal(tsLower),
          ::paimon::Literal(tsUpper)),
      1,
      [&](auto row) {
        auto millis = 1'700'000'000'000 + (static_cast<int64_t>(row) * 1000);
        return millis >= tsLower.GetMillisecond() &&
            millis <= tsUpper.GetMillisecond();
      });
}

TEST_F(PaimonFilterPushdownTest, IsNull) {
  runPushdownTest(
      ::paimon::PredicateBuilder::IsNull(6, "s", ::paimon::FieldType::STRING),
      1,
      [&](auto row) { return !stringValues_[row].has_value(); });
}

TEST_F(PaimonFilterPushdownTest, IsNotNull) {
  runPushdownTest(
      ::paimon::PredicateBuilder::IsNotNull(
          6, "s", ::paimon::FieldType::STRING),
      1,
      [&](auto row) { return stringValues_[row].has_value(); });
}

TEST_F(PaimonFilterPushdownTest, OrSameColumn) {
  auto left = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(10));
  auto right = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(20));
  auto orRes = ::paimon::PredicateBuilder::Or({left, right});
  ASSERT_TRUE(orRes.ok()) << orRes.status().message();
  runPushdownTest(orRes.value(), 1, [&](auto row) {
    return intValues_[row] == 10 || intValues_[row] == 20;
  });
}

TEST_F(PaimonFilterPushdownTest, OrAcrossColumnsMetadataOnly) {
  auto left = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(10));
  std::string value = "str_7";
  auto right = ::paimon::PredicateBuilder::Equal(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(
          ::paimon::FieldType::STRING, value.data(), value.size()));
  auto orRes = ::paimon::PredicateBuilder::Or({left, right});
  ASSERT_TRUE(orRes.ok()) << orRes.status().message();

  // OR across columns is not pushed into ScanSpec filters (0 filters).
  // Reader-level row filtering is not expected; only metadata pruning may
  // apply.
  runPushdownTest(orRes.value(), 0, [&](auto row) { return true; });
}

TEST_F(PaimonFilterPushdownTest, AndAcrossColumns) {
  auto left = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(70));
  std::string value = "str_7";
  auto right = ::paimon::PredicateBuilder::Equal(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(
          ::paimon::FieldType::STRING, value.data(), value.size()));
  auto andRes = ::paimon::PredicateBuilder::And({left, right});
  ASSERT_TRUE(andRes.ok()) << andRes.status().message();
  runPushdownTest(andRes.value(), 2, [&](auto row) {
    return intValues_[row] == 70 && stringValues_[row].has_value() &&
        stringValues_[row].value() == "str_7";
  });
}

TEST_F(PaimonFilterPushdownTest, ShortDecimalEquality) {
  auto dec = ::paimon::Decimal::FromUnscaledLong(5000, 10, 2);
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          9, "dec", ::paimon::FieldType::DECIMAL, ::paimon::Literal(dec)),
      1,
      [&](auto row) { return decimalValues_[row] == 5000; });
}

TEST_F(PaimonFilterPushdownTest, ShortDecimalRange) {
  auto lower = ::paimon::Decimal::FromUnscaledLong(2500, 10, 2);
  auto upper = ::paimon::Decimal::FromUnscaledLong(7500, 10, 2);
  runPushdownTest(
      ::paimon::PredicateBuilder::Between(
          9,
          "dec",
          ::paimon::FieldType::DECIMAL,
          ::paimon::Literal(lower),
          ::paimon::Literal(upper)),
      1,
      [&](auto row) {
        return decimalValues_[row] >= 2500 && decimalValues_[row] <= 7500;
      });
}

TEST_F(PaimonFilterPushdownTest, LongDecimalEquality) {
  auto ldec = ::paimon::Decimal(20, 3, static_cast<__int128_t>(42000));
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          10, "ldec", ::paimon::FieldType::DECIMAL, ::paimon::Literal(ldec)),
      1,
      [&](auto row) { return longDecimalValues_[row] == 42000; });
}

TEST_F(PaimonFilterPushdownTest, LongDecimalRange) {
  // KNOWN LIMITATION: Between decomposes to AND(gte, lte) which produces two
  // HugeintRanges. HugeintRange::mergeWith() is not implemented, so the merge
  // throws and we gracefully drop the filter — all rows are returned.
  auto lower = ::paimon::Decimal(20, 3, static_cast<__int128_t>(30000));
  auto upper = ::paimon::Decimal(20, 3, static_cast<__int128_t>(60000));
  runPushdownTest(
      ::paimon::PredicateBuilder::Between(
          10,
          "ldec",
          ::paimon::FieldType::DECIMAL,
          ::paimon::Literal(lower),
          ::paimon::Literal(upper)),
      0,
      [&](auto row) { return true; });
}

// --- Narrow integer type coverage -- exercises BUILD_INT_IN_FILTER macro paths
// --

TEST_F(PaimonFilterPushdownTest, TinyIntEquality) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          0, "ti", ::paimon::FieldType::TINYINT, ::paimon::Literal(int8_t(42))),
      1,
      [&](auto row) { return tinyValues_[row] == 42; });
}

TEST_F(PaimonFilterPushdownTest, SmallIntInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::In(
          1,
          "si",
          ::paimon::FieldType::SMALLINT,
          {::paimon::Literal(::paimon::FieldType::SMALLINT, int16_t(200)),
           ::paimon::Literal(::paimon::FieldType::SMALLINT, int16_t(400))}),
      1,
      [&](auto row) {
        return smallValues_[row] == 200 || smallValues_[row] == 400;
      });
}

TEST_F(PaimonFilterPushdownTest, BigIntRange) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Between(
          3,
          "bi",
          ::paimon::FieldType::BIGINT,
          ::paimon::Literal(int64_t(2000)),
          ::paimon::Literal(int64_t(5000))),
      1,
      [&](auto row) {
        return bigintValues_[row] >= 2000 && bigintValues_[row] <= 5000;
      });
}

// --- TINYINT operators ---

TEST_F(PaimonFilterPushdownTest, TinyIntNotEqual) {
  runPushdownTest(
      ::paimon::PredicateBuilder::NotEqual(
          0, "ti", ::paimon::FieldType::TINYINT, ::paimon::Literal(int8_t(50))),
      1,
      [&](auto row) { return tinyValues_[row] != 50; });
}

TEST_F(PaimonFilterPushdownTest, TinyIntGreaterThan) {
  runPushdownTest(
      ::paimon::PredicateBuilder::GreaterThan(
          0,
          "ti",
          ::paimon::FieldType::TINYINT,
          ::paimon::Literal(int8_t(100))),
      1,
      [&](auto row) { return tinyValues_[row] > 100; });
}

TEST_F(PaimonFilterPushdownTest, TinyIntLessThanOrEqual) {
  runPushdownTest(
      ::paimon::PredicateBuilder::LessOrEqual(
          0, "ti", ::paimon::FieldType::TINYINT, ::paimon::Literal(int8_t(10))),
      1,
      [&](auto row) { return tinyValues_[row] <= 10; });
}

// --- SMALLINT operators ---

TEST_F(PaimonFilterPushdownTest, SmallIntEquality) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          1,
          "si",
          ::paimon::FieldType::SMALLINT,
          ::paimon::Literal(int16_t(300))),
      1,
      [&](auto row) { return smallValues_[row] == 300; });
}

TEST_F(PaimonFilterPushdownTest, SmallIntNotInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::NotIn(
          1,
          "si",
          ::paimon::FieldType::SMALLINT,
          {::paimon::Literal(::paimon::FieldType::SMALLINT, int16_t(100)),
           ::paimon::Literal(::paimon::FieldType::SMALLINT, int16_t(300)),
           ::paimon::Literal(::paimon::FieldType::SMALLINT, int16_t(500))}),
      1,
      [&](auto row) {
        return smallValues_[row] != 100 && smallValues_[row] != 300 &&
            smallValues_[row] != 500;
      });
}

TEST_F(PaimonFilterPushdownTest, SmallIntBetween) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Between(
          1,
          "si",
          ::paimon::FieldType::SMALLINT,
          ::paimon::Literal(int16_t(200)),
          ::paimon::Literal(int16_t(600))),
      1,
      [&](auto row) {
        return smallValues_[row] >= 200 && smallValues_[row] <= 600;
      });
}

// --- INTEGER operators ---

TEST_F(PaimonFilterPushdownTest, IntegerEquality) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          2, "i", ::paimon::FieldType::INT, ::paimon::Literal(int32_t(500))),
      1,
      [&](auto row) { return intValues_[row] == 500; });
}

TEST_F(PaimonFilterPushdownTest, IntegerInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::In(
          2,
          "i",
          ::paimon::FieldType::INT,
          {::paimon::Literal(::paimon::FieldType::INT, int32_t(0)),
           ::paimon::Literal(::paimon::FieldType::INT, int32_t(100)),
           ::paimon::Literal(::paimon::FieldType::INT, int32_t(200))}),
      1,
      [&](auto row) {
        return intValues_[row] == 0 || intValues_[row] == 100 ||
            intValues_[row] == 200;
      });
}

TEST_F(PaimonFilterPushdownTest, IntegerGreaterOrEqual) {
  runPushdownTest(
      ::paimon::PredicateBuilder::GreaterOrEqual(
          2, "i", ::paimon::FieldType::INT, ::paimon::Literal(int32_t(8000))),
      1,
      [&](auto row) { return intValues_[row] >= 8000; });
}

TEST_F(PaimonFilterPushdownTest, IntegerLessThan) {
  runPushdownTest(
      ::paimon::PredicateBuilder::LessThan(
          2, "i", ::paimon::FieldType::INT, ::paimon::Literal(int32_t(50))),
      1,
      [&](auto row) { return intValues_[row] < 50; });
}

// --- BIGINT operators ---

TEST_F(PaimonFilterPushdownTest, BigIntEquality) {
  runPushdownTest(
      ::paimon::PredicateBuilder::Equal(
          3,
          "bi",
          ::paimon::FieldType::BIGINT,
          ::paimon::Literal(int64_t(5000))),
      1,
      [&](auto row) { return bigintValues_[row] == 5000; });
}

TEST_F(PaimonFilterPushdownTest, BigIntNotEqual) {
  runPushdownTest(
      ::paimon::PredicateBuilder::NotEqual(
          3,
          "bi",
          ::paimon::FieldType::BIGINT,
          ::paimon::Literal(int64_t(3000))),
      1,
      [&](auto row) { return bigintValues_[row] != 3000; });
}

TEST_F(PaimonFilterPushdownTest, BigIntGreaterOrEqual) {
  runPushdownTest(
      ::paimon::PredicateBuilder::GreaterOrEqual(
          3,
          "bi",
          ::paimon::FieldType::BIGINT,
          ::paimon::Literal(int64_t(50000))),
      1,
      [&](auto row) { return bigintValues_[row] >= 50000; });
}

TEST_F(PaimonFilterPushdownTest, BigIntLessThan) {
  runPushdownTest(
      ::paimon::PredicateBuilder::LessThan(
          3,
          "bi",
          ::paimon::FieldType::BIGINT,
          ::paimon::Literal(int64_t(5000))),
      1,
      [&](auto row) { return bigintValues_[row] < 5000; });
}

TEST_F(PaimonFilterPushdownTest, BigIntInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::In(
          3,
          "bi",
          ::paimon::FieldType::BIGINT,
          {::paimon::Literal(::paimon::FieldType::BIGINT, int64_t(1000)),
           ::paimon::Literal(::paimon::FieldType::BIGINT, int64_t(5000)),
           ::paimon::Literal(::paimon::FieldType::BIGINT, int64_t(9000))}),
      1,
      [&](auto row) {
        return bigintValues_[row] == 1000 || bigintValues_[row] == 5000 ||
            bigintValues_[row] == 9000;
      });
}

TEST_F(PaimonFilterPushdownTest, BigIntNotInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::NotIn(
          3,
          "bi",
          ::paimon::FieldType::BIGINT,
          {::paimon::Literal(::paimon::FieldType::BIGINT, int64_t(1000)),
           ::paimon::Literal(::paimon::FieldType::BIGINT, int64_t(5000))}),
      1,
      [&](auto row) {
        return bigintValues_[row] != 1000 && bigintValues_[row] != 5000;
      });
}

// --- IN/NOT_IN: TINYINT DecodedVector<int8_t> path ---

TEST_F(PaimonFilterPushdownTest, TinyIntInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::In(
          0,
          "ti",
          ::paimon::FieldType::TINYINT,
          {::paimon::Literal(::paimon::FieldType::TINYINT, int8_t(1)),
           ::paimon::Literal(::paimon::FieldType::TINYINT, int8_t(5)),
           ::paimon::Literal(::paimon::FieldType::TINYINT, int8_t(10))}),
      1,
      [&](auto row) {
        return tinyValues_[row] == 1 || tinyValues_[row] == 5 ||
            tinyValues_[row] == 10;
      });
}

TEST_F(PaimonFilterPushdownTest, TinyIntNotInList) {
  runPushdownTest(
      ::paimon::PredicateBuilder::NotIn(
          0,
          "ti",
          ::paimon::FieldType::TINYINT,
          {::paimon::Literal(::paimon::FieldType::TINYINT, int8_t(0)),
           ::paimon::Literal(::paimon::FieldType::TINYINT, int8_t(1)),
           ::paimon::Literal(::paimon::FieldType::TINYINT, int8_t(2))}),
      1,
      [&](auto row) {
        return tinyValues_[row] != 0 && tinyValues_[row] != 1 &&
            tinyValues_[row] != 2;
      });
}

// --- neq: NegatedHugeintRange / NegatedBytesValues ---

TEST_F(PaimonFilterPushdownTest, LongDecimalNotEqual) {
  auto val = ::paimon::Decimal(20, 3, static_cast<__int128_t>(42000));
  runPushdownTest(
      ::paimon::PredicateBuilder::NotEqual(
          10, "ldec", ::paimon::FieldType::DECIMAL, ::paimon::Literal(val)),
      1,
      [&](auto row) { return longDecimalValues_[row] != 42000; });
}

TEST_F(PaimonFilterPushdownTest, StringNotEqual) {
  // KNOWN: NegatedBytesValues with a single-element vector excludes nulls
  // differently than the full-row evaluation. Null string rows are excluded.
  std::string t = "str_42";
  runPushdownTest(
      ::paimon::PredicateBuilder::NotEqual(
          6,
          "s",
          ::paimon::FieldType::STRING,
          ::paimon::Literal(::paimon::FieldType::STRING, t.data(), t.size())),
      1,
      [&](auto row) {
        return stringValues_[row].has_value() &&
            stringValues_[row].value() != t;
      });
}

// --- Range comparisons on HUGEINT / VARCHAR ---

TEST_F(PaimonFilterPushdownTest, LongDecimalGreaterThan) {
  auto th = ::paimon::Decimal(20, 3, static_cast<__int128_t>(500000));
  runPushdownTest(
      ::paimon::PredicateBuilder::GreaterThan(
          10, "ldec", ::paimon::FieldType::DECIMAL, ::paimon::Literal(th)),
      1,
      [&](auto row) { return longDecimalValues_[row] > 500000; });
}

TEST_F(PaimonFilterPushdownTest, LongDecimalLessThan) {
  auto th = ::paimon::Decimal(20, 3, static_cast<__int128_t>(200000));
  runPushdownTest(
      ::paimon::PredicateBuilder::LessThan(
          10, "ldec", ::paimon::FieldType::DECIMAL, ::paimon::Literal(th)),
      1,
      [&](auto row) { return longDecimalValues_[row] < 200000; });
}

TEST_F(PaimonFilterPushdownTest, StringGreaterThan) {
  std::string b = "str_500";
  runPushdownTest(
      ::paimon::PredicateBuilder::GreaterOrEqual(
          6,
          "s",
          ::paimon::FieldType::STRING,
          ::paimon::Literal(::paimon::FieldType::STRING, b.data(), b.size())),
      1,
      [&](auto row) {
        return stringValues_[row].has_value() &&
            stringValues_[row].value() >= b;
      });
}

TEST_F(PaimonFilterPushdownTest, StringLessThan) {
  std::string b = "str_20";
  runPushdownTest(
      ::paimon::PredicateBuilder::LessThan(
          6,
          "s",
          ::paimon::FieldType::STRING,
          ::paimon::Literal(::paimon::FieldType::STRING, b.data(), b.size())),
      1,
      [&](auto row) {
        return stringValues_[row].has_value() && stringValues_[row].value() < b;
      });
}

// --- OR of equals on VARCHAR columns ---

TEST_F(PaimonFilterPushdownTest, OrOfEqualsOnString) {
  std::string v1 = "str_3", v2 = "str_7";
  auto p1 = ::paimon::PredicateBuilder::Equal(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(::paimon::FieldType::STRING, v1.data(), v1.size()));
  auto p2 = ::paimon::PredicateBuilder::Equal(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(::paimon::FieldType::STRING, v2.data(), v2.size()));
  auto orRes = ::paimon::PredicateBuilder::Or({p1, p2});
  ASSERT_TRUE(orRes.ok());
  runPushdownTest(orRes.value(), 1, [&](auto row) {
    return stringValues_[row].has_value() &&
        (stringValues_[row].value() == v1 || stringValues_[row].value() == v2);
  });
}

// --- NOT negation: NOT(eq)->neq, NOT(is_not_null)->IsNull ---

TEST_F(PaimonFilterPushdownTest, NotOfEquality) {
  auto inner = ::paimon::PredicateBuilder::Equal(
      0, "ti", ::paimon::FieldType::TINYINT, ::paimon::Literal(int8_t(42)));
  auto notRes = ::paimon::PredicateBuilder::Not(inner);
  ASSERT_TRUE(notRes.ok());
  runPushdownTest(
      notRes.value(), 1, [&](auto row) { return tinyValues_[row] != 42; });
}

TEST_F(PaimonFilterPushdownTest, NotOfIsNotNull) {
  auto inner = ::paimon::PredicateBuilder::IsNotNull(
      6, "s", ::paimon::FieldType::STRING);
  auto notRes = ::paimon::PredicateBuilder::Not(inner);
  ASSERT_TRUE(notRes.ok());
  runPushdownTest(notRes.value(), 1, [&](auto row) {
    return !stringValues_[row].has_value();
  });
}

} // namespace
