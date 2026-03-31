/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "paimon/data/timestamp.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"

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
    rootPool_ = memory::memoryManager()->addRootPool("PaimonFilterPushdownTest");
    leafPool_ = rootPool_->addLeafChild("leaf");
    tempDir_ = std::filesystem::temp_directory_path();

    buildData();
    writeParquet();
  }

  void buildData() {
    rowType_ = ROW(
        {"ti", "si", "i", "bi", "f", "d", "s", "bin", "ts"},
        {TINYINT(),
         SMALLINT(),
         INTEGER(),
         BIGINT(),
         REAL(),
         DOUBLE(),
         VARCHAR(),
         VARBINARY(),
         TIMESTAMP()});

    tinyValues_.resize(kRows);
    smallValues_.resize(kRows);
    intValues_.resize(kRows);
    bigintValues_.resize(kRows);
    floatValues_.resize(kRows);
    doubleValues_.resize(kRows);
    stringValues_.resize(kRows);
    binaryValues_.resize(kRows);
    timestampValues_.resize(kRows);

    for (int32_t row = 0; row < kRows; ++row) {
      tinyValues_[row] = static_cast<int8_t>(row);
      smallValues_[row] = static_cast<int16_t>(100 + row * 2);
      intValues_[row] = row * 10;
      bigintValues_[row] = 1000 + row * 100;
      floatValues_[row] = static_cast<float>(row) * 0.5f;
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

      timestampValues_[row] = Timestamp::fromMillis(1'700'000'000'000 +
                                                    static_cast<int64_t>(row) * 1000);
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
    auto binaryVec = makeNullableFlatVector<StringView>(binaryViews, VARBINARY());

    auto tsVec = makeFlatVector<Timestamp>(
        kRows, [&](auto row) { return timestampValues_[row]; });

    data_ = makeRowVector(
        {tinyVec,
         smallVec,
         intVec,
         bigintVec,
         floatVec,
         doubleVec,
         stringVec,
         binaryVec,
         tsVec});
  }

  void writeParquet() {
    auto testInfo = testing::UnitTest::GetInstance()->current_test_info();
    auto filename = std::string("paimon_filter_pushdown_") + testInfo->name() +
        ".parquet";
    parquetPath_ = (tempDir_ / filename).string();

    auto sink = FileSink::create(parquetPath_, {.pool = leafPool_.get()});
    bytedance::bolt::parquet::WriterOptions opts;
    opts.memoryPool = leafPool_.get();
    opts.enableFlushBasedOnBlockSize = true;
    bytedance::bolt::parquet::Writer writer(
        std::move(sink),
        opts,
        rootPool_,
        ::arrow::default_memory_pool(),
        rowType_);
    writer.write(data_);
    writer.close();
  }

  RowVectorPtr readWithPredicate(
      const std::shared_ptr<::paimon::Predicate>& predicate) {
    PaimonParquetReader format({});
    auto rbRes = format.CreateReaderBuilder(kBatchSize);
    EXPECT_TRUE(rbRes.ok());
    if (!rbRes.ok()) {
      return RowVector::createEmpty(rowType_, leafPool_.get());
    }
    auto builder = std::move(rbRes).value();

    auto readerRes = builder->Build(parquetPath_);
    EXPECT_TRUE(readerRes.ok());
    if (!readerRes.ok()) {
      return RowVector::createEmpty(rowType_, leafPool_.get());
    }
    auto fileReader = std::move(readerRes).value();

    auto dummyVector = BaseVector::create(rowType_, 0, leafPool_.get());
    auto arrowSchema = std::make_unique<::ArrowSchema>();
    ArrowOptions arrowOptions;
    exportToArrow(dummyVector, *arrowSchema, arrowOptions);

    auto status = fileReader->SetReadSchema(
        arrowSchema.get(), predicate, std::nullopt);
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

      auto batch = importFromArrowAsOwner(
          *sch, *arr, {}, leafPool_.get());
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
      return RowVector::createEmpty(rowType_, leafPool_.get());
    }

    auto result = RowVector::createEmpty(rowType_, leafPool_.get());
    for (const auto& batch : batches) {
      result->append(batch.get());
    }
    return result;
  }

  RowVectorPtr expectedByPredicate(
      std::function<bool(vector_size_t)> predicate) {
    std::vector<vector_size_t> indices;
    for (vector_size_t row = 0; row < data_->size(); ++row) {
      if (predicate(row)) {
        indices.push_back(row);
      }
    }
    if (indices.empty()) {
      return RowVector::createEmpty(rowType_, leafPool_.get());
    }

    auto indicesBuffer = makeIndices(indices);
    std::vector<VectorPtr> children;
    children.reserve(data_->childrenSize());
    for (column_index_t i = 0; i < data_->childrenSize(); ++i) {
      auto child = BaseVector::wrapInDictionary(
          BufferPtr(nullptr), indicesBuffer, indices.size(), data_->childAt(i));
      children.push_back(child);
    }
    return std::make_shared<RowVector>(
        leafPool_.get(), rowType_, BufferPtr(nullptr), indices.size(), children);
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
  std::vector<std::string> stringStorage_;
  std::vector<std::string> binaryStorage_;
};

TEST_F(PaimonFilterPushdownTest, IntEquality) {
  auto predicate = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(50));
  auto expected = expectedByPredicate(
      [&](auto row) { return intValues_[row] == 50; });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, IntNotEqual) {
  auto predicate = ::paimon::PredicateBuilder::NotEqual(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(50));
  auto expected = expectedByPredicate(
      [&](auto row) { return intValues_[row] != 50; });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, IntBetween) {
  auto predicate = ::paimon::PredicateBuilder::Between(
      2,
      "i",
      ::paimon::FieldType::INT,
      ::paimon::Literal(20),
      ::paimon::Literal(60));
  auto expected = expectedByPredicate(
      [&](auto row) { return intValues_[row] >= 20 && intValues_[row] <= 60; });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, IntGreaterLess) {
  auto predicate = ::paimon::PredicateBuilder::GreaterThan(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(80));
  auto expected = expectedByPredicate(
      [&](auto row) { return intValues_[row] > 80; });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, IntInList) {
  std::vector<::paimon::Literal> literals = {
      ::paimon::Literal(10), ::paimon::Literal(30), ::paimon::Literal(70)};
  auto predicate = ::paimon::PredicateBuilder::In(
      2, "i", ::paimon::FieldType::INT, literals);
  auto expected = expectedByPredicate([&](auto row) {
    return intValues_[row] == 10 || intValues_[row] == 30 ||
        intValues_[row] == 70;
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, IntNotInList) {
  std::vector<::paimon::Literal> literals = {
      ::paimon::Literal(10), ::paimon::Literal(30), ::paimon::Literal(70)};
  auto predicate = ::paimon::PredicateBuilder::NotIn(
      2, "i", ::paimon::FieldType::INT, literals);
  auto expected = expectedByPredicate([&](auto row) {
    return intValues_[row] != 10 && intValues_[row] != 30 &&
        intValues_[row] != 70;
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, FloatRange) {
  auto predicate = ::paimon::PredicateBuilder::GreaterThan(
      4, "f", ::paimon::FieldType::FLOAT, ::paimon::Literal(5.0f));
  auto expected = expectedByPredicate(
      [&](auto row) { return floatValues_[row] > 5.0f; });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, DoubleRange) {
  auto predicate = ::paimon::PredicateBuilder::LessThan(
      5, "d", ::paimon::FieldType::DOUBLE, ::paimon::Literal(10.0));
  auto expected = expectedByPredicate(
      [&](auto row) { return doubleValues_[row] < 10.0; });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, StringEquality) {
  std::string value = "str_7";
  auto predicate = ::paimon::PredicateBuilder::Equal(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(::paimon::FieldType::STRING, value.data(), value.size()));
  auto expected = expectedByPredicate([&](auto row) {
    return stringValues_[row].has_value() &&
        stringValues_[row].value() == "str_7";
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, StringRange) {
  std::string lower = "str_10";
  std::string upper = "str_15";
  auto predicate = ::paimon::PredicateBuilder::Between(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(::paimon::FieldType::STRING, lower.data(), lower.size()),
      ::paimon::Literal(::paimon::FieldType::STRING, upper.data(), upper.size()));
  auto expected = expectedByPredicate([&](auto row) {
    if (!stringValues_[row].has_value()) {
      return false;
    }
    const auto& value = stringValues_[row].value();
    return value >= lower && value <= upper;
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, StringInList) {
  std::string v1 = "str_3";
  std::string v2 = "str_9";
  std::vector<::paimon::Literal> literals = {
      ::paimon::Literal(::paimon::FieldType::STRING, v1.data(), v1.size()),
      ::paimon::Literal(::paimon::FieldType::STRING, v2.data(), v2.size())};
  auto predicate = ::paimon::PredicateBuilder::In(
      6, "s", ::paimon::FieldType::STRING, literals);
  auto expected = expectedByPredicate([&](auto row) {
    return stringValues_[row].has_value() &&
        (stringValues_[row].value() == v1 ||
         stringValues_[row].value() == v2);
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, StringNotInList) {
  std::string v1 = "str_3";
  std::string v2 = "str_9";
  std::vector<::paimon::Literal> literals = {
      ::paimon::Literal(::paimon::FieldType::STRING, v1.data(), v1.size()),
      ::paimon::Literal(::paimon::FieldType::STRING, v2.data(), v2.size())};
  auto predicate = ::paimon::PredicateBuilder::NotIn(
      6, "s", ::paimon::FieldType::STRING, literals);
  auto expected = expectedByPredicate([&](auto row) {
    return stringValues_[row].has_value() &&
        stringValues_[row].value() != v1 &&
        stringValues_[row].value() != v2;
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, TimestampRange) {
  auto tsLower = ::paimon::Timestamp::FromEpochMillis(1'700'000'000'000 + 5 * 1000);
  auto tsUpper = ::paimon::Timestamp::FromEpochMillis(1'700'000'000'000 + 8 * 1000);
  auto predicate = ::paimon::PredicateBuilder::Between(
      8,
      "ts",
      ::paimon::FieldType::TIMESTAMP,
      ::paimon::Literal(tsLower),
      ::paimon::Literal(tsUpper));
  auto expected = expectedByPredicate([&](auto row) {
    auto millis = 1'700'000'000'000 + static_cast<int64_t>(row) * 1000;
    return millis >= tsLower.GetMillisecond() &&
        millis <= tsUpper.GetMillisecond();
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, IsNull) {
  auto predicate = ::paimon::PredicateBuilder::IsNull(
      6, "s", ::paimon::FieldType::STRING);
  auto expected = expectedByPredicate(
      [&](auto row) { return !stringValues_[row].has_value(); });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, IsNotNull) {
  auto predicate = ::paimon::PredicateBuilder::IsNotNull(
      6, "s", ::paimon::FieldType::STRING);
  auto expected = expectedByPredicate(
      [&](auto row) { return stringValues_[row].has_value(); });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, OrSameColumn) {
  auto left = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(10));
  auto right = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(20));
  auto orRes = ::paimon::PredicateBuilder::Or({left, right});
  ASSERT_TRUE(orRes.ok()) << orRes.status().message();
  auto predicate = orRes.value();
  auto expected = expectedByPredicate(
      [&](auto row) { return intValues_[row] == 10 || intValues_[row] == 20; });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, OrAcrossColumnsMetadataOnly) {
  auto left = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(10));
  std::string value = "str_7";
  auto right = ::paimon::PredicateBuilder::Equal(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(::paimon::FieldType::STRING, value.data(), value.size()));
  auto orRes = ::paimon::PredicateBuilder::Or({left, right});
  ASSERT_TRUE(orRes.ok()) << orRes.status().message();
  auto predicate = orRes.value();

  // Hive-parity: OR across columns is not pushed into ScanSpec filters.
  // Reader-level row filtering is not expected here; only metadata pruning may apply.
  auto expected = readWithPredicate(nullptr);
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

TEST_F(PaimonFilterPushdownTest, AndAcrossColumns) {
  auto left = ::paimon::PredicateBuilder::Equal(
      2, "i", ::paimon::FieldType::INT, ::paimon::Literal(70));
  std::string value = "str_7";
  auto right = ::paimon::PredicateBuilder::Equal(
      6,
      "s",
      ::paimon::FieldType::STRING,
      ::paimon::Literal(::paimon::FieldType::STRING, value.data(), value.size()));
  auto andRes = ::paimon::PredicateBuilder::And({left, right});
  ASSERT_TRUE(andRes.ok()) << andRes.status().message();
  auto predicate = andRes.value();
  auto expected = expectedByPredicate([&](auto row) {
    return intValues_[row] == 70 &&
        stringValues_[row].has_value() &&
        stringValues_[row].value() == "str_7";
  });
  auto actual = readWithPredicate(predicate);
  assertRowVectorsEqual(expected, actual);
}

} // namespace
