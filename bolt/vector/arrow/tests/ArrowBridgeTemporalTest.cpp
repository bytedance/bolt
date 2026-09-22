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

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <folly/lang/Bits.h>
#include <array>

#include "bolt/common/base/tests/ArrowTestUtils.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::test {
namespace {

struct ArrowData {
  ArrowSchema schema{};
  ArrowArray array{};

  ~ArrowData() {
    if (array.release) {
      array.release(&array);
    }
    if (schema.release) {
      schema.release(&schema);
    }
  }
};

std::string timestampMetadata(
    std::string_view name = "bolt.timestamp",
    std::string_view descriptor = "1") {
  std::string result;
  auto appendSize = [&](int32_t size) {
    result.append(reinterpret_cast<const char*>(&size), sizeof(size));
  };
  appendSize(2);
  for (const auto& [key, value] :
       {std::pair<std::string_view, std::string_view>{
            "ARROW:extension:name", name},
        {"ARROW:extension:metadata", descriptor}}) {
    appendSize(key.size());
    result.append(key);
    appendSize(value.size());
    result.append(value);
  }
  return result;
}

class ArrowBridgeTemporalTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void exportVector(
      const VectorPtr& input,
      ArrowData& data,
      const ArrowOptions& options) {
    exportToArrow(input, data.schema, options, {}, pool());
    exportToArrow(input, data.array, pool(), options);
  }

  void roundTrip(const VectorPtr& input, const ArrowOptions& options) {
    ArrowData data;
    exportVector(input, data, options);
    EXPECT_EQ(*importFromArrow(data.schema, options), *input->type());
    {
      auto viewer =
          importFromArrowAsViewer(data.schema, data.array, options, pool());
      assertEqualVectors(input, viewer);
    }
    ASSERT_NE(data.schema.release, nullptr);
    ASSERT_NE(data.array.release, nullptr);
    auto owner =
        importFromArrowAsOwner(data.schema, data.array, options, pool());
    EXPECT_EQ(data.schema.release, nullptr);
    EXPECT_EQ(data.array.release, nullptr);
    assertEqualVectors(input, owner);
  }

  void arrowRoundTrip(
      const VectorPtr& input,
      const ArrowOptions& options,
      ReusableArrowBatchPool* batchPool = nullptr) {
    ArrowData data;
    if (batchPool) {
      batchPool->exportToArrow(
          input, pool(), options, &data.schema, &data.array);
    } else {
      exportVector(input, data, options);
    }
    EXPECT_OK_AND_ASSIGN(
        auto array, arrow::ImportArray(&data.array, &data.schema));
    ASSERT_OK(array->ValidateFull());
    ASSERT_OK(arrow::ExportArray(*array, &data.array, &data.schema));
    assertEqualVectors(
        input,
        importFromArrowAsOwner(data.schema, data.array, options, pool()));
  }

  void rawArray(
      ArrowData& data,
      const char* format,
      const void** buffers,
      int64_t length,
      int64_t nullCount = 0) {
    data.schema.format = format;
    data.schema.release = [](ArrowSchema* schema) {
      schema->release = nullptr;
    };
    data.array.length = length;
    data.array.null_count = nullCount;
    data.array.n_buffers = 2;
    data.array.buffers = buffers;
    data.array.release = [](ArrowArray* array) { array->release = nullptr; };
  }

  ArrowOptions wideOptions() {
    ArrowOptions options;
    options.timestampEncoding = TimestampEncoding::kSecondsNanos;
    return options;
  }
};

TEST_F(ArrowBridgeTemporalTest, standardTimestampBoundariesAndOverflow) {
  auto input = makeFlatVector<Timestamp>(
      {Timestamp::fromNanos(INT64_MIN),
       Timestamp::fromNanos(INT64_MAX),
       Timestamp(-1, 999'999'999)});
  for (bool ipc : {false, true}) {
    ArrowOptions options;
    options.exportToArrowIPC = ipc;
    roundTrip(input, options);
    for (auto value :
         {Timestamp(-9'223'372'037, 145'224'191),
          Timestamp(9'223'372'036, 854'775'808)}) {
      ArrowData data;
      BOLT_ASSERT_THROW(
          exportToArrow(
              makeFlatVector<Timestamp>({value}), data.array, pool(), options),
          "Could not convert Timestamp");
    }
  }
  for (const auto* format : {"ts", "tsn", "tsx:", "tsnX"}) {
    ArrowSchema schema{};
    schema.format = format;
    EXPECT_THROW(importFromArrow(schema), BoltUserError);
  }
}

TEST_F(ArrowBridgeTemporalTest, nullTimestampStorage) {
  auto mixed = makeNullableFlatVector<Timestamp>(
      {Timestamp(10, 0), std::nullopt, Timestamp(-1, 0), std::nullopt});
  // Null storage must not be converted, even if it exceeds the Arrow range.
  mixed->mutableRawValues()[1] = Timestamp::max();
  mixed->mutableRawValues()[3] = Timestamp::max();
  auto allNull = makeNullableFlatVector<Timestamp>(
      {std::nullopt, std::nullopt, std::nullopt});
  auto noValues = std::make_shared<FlatVector<Timestamp>>(
      pool(),
      TIMESTAMP(),
      allocateNulls(3, pool(), bits::kNull),
      3,
      nullptr,
      std::vector<BufferPtr>{});
  const std::vector<VectorPtr> inputs{
      mixed,
      allNull,
      noValues,
      BaseVector::wrapInDictionary(
          nullptr, makeIndices({3, 0, 1, 2}), 4, mixed),
      BaseVector::createNullConstant(TIMESTAMP(), 3, pool())};
  auto checkNullStorage = [](const std::shared_ptr<arrow::Array>& array) {
    const auto* values =
        reinterpret_cast<const int64_t*>(array->data()->buffers[1]->data());
    for (int64_t i = 0; i < array->length(); ++i) {
      if (array->IsNull(i)) {
        EXPECT_EQ(values[i], 0) << "row " << i;
      }
    }
  };
  for (auto unit :
       {TimestampUnit::kSecond,
        TimestampUnit::kMilli,
        TimestampUnit::kMicro,
        TimestampUnit::kNano}) {
    for (bool ipc : {false, true}) {
      ArrowOptions options;
      options.timestampUnit = unit;
      options.flattenDictionary = true;
      options.flattenConstant = true;
      options.exportToArrowIPC = ipc;
      for (const auto& input : inputs) {
        SCOPED_TRACE(input->toString());
        ArrowData data;
        exportVector(input, data, options);
        assertEqualVectors(
            input,
            importFromArrowAsViewer(data.schema, data.array, options, pool()));
        EXPECT_OK_AND_ASSIGN(
            auto array, arrow::ImportArray(&data.array, &data.schema));
        checkNullStorage(array);
        if (ipc) {
          auto batch = arrow::RecordBatch::Make(
              arrow::schema({arrow::field("ts", array->type())}),
              array->length(),
              {array});
          EXPECT_OK_AND_ASSIGN(
              auto sink, arrow::io::BufferOutputStream::Create());
          EXPECT_OK_AND_ASSIGN(
              auto writer, arrow::ipc::MakeStreamWriter(sink, batch->schema()));
          ASSERT_OK(writer->WriteRecordBatch(*batch));
          ASSERT_OK(writer->Close());
          EXPECT_OK_AND_ASSIGN(auto bytes, sink->Finish());
          auto source = std::make_shared<arrow::io::BufferReader>(bytes);
          EXPECT_OK_AND_ASSIGN(
              auto reader, arrow::ipc::RecordBatchStreamReader::Open(source));
          EXPECT_OK_AND_ASSIGN(auto restored, reader->Next());
          ASSERT_NE(restored, nullptr);
          EXPECT_TRUE(restored->Equals(*batch));
          checkNullStorage(restored->column(0));
        }
      }
    }
  }
}

TEST_F(ArrowBridgeTemporalTest, missingValuesAndUnknownNullCount) {
  uint64_t nulls = 0;
  const void* buffers[] = {&nulls, nullptr};
  const auto metadata = timestampMetadata();
  for (auto format : {"w:16", "tts", "ttm", "ttu", "ttn", "tsn:"}) {
    for (auto nullCount : {int64_t{3}, int64_t{-1}}) {
      ArrowData data;
      rawArray(data, format, buffers, 3, nullCount);
      data.schema.metadata =
          std::string_view(format) == "w:16" ? metadata.data() : nullptr;
      auto result =
          importFromArrowAsViewer(data.schema, data.array, {}, pool());
      for (vector_size_t i = 0; i < 3; ++i) {
        EXPECT_TRUE(result->isNullAt(i));
      }
      data.array.null_count = 0;
      BOLT_ASSERT_THROW(
          importFromArrowAsViewer(data.schema, data.array, {}, pool()),
          "Missing Arrow temporal values buffer");
    }
  }
}

TEST_F(ArrowBridgeTemporalTest, timestampsUnderNullRows) {
  auto timestamps =
      makeFlatVector<Timestamp>({Timestamp::max(), Timestamp(0, 1)});
  auto inner = makeRowVector({timestamps});
  inner->setNull(0, true);
  auto input = makeRowVector({inner});
  for (bool ipc : {false, true}) {
    ArrowOptions options;
    options.exportToArrowIPC = ipc;
    roundTrip(input, options);
    roundTrip(
        BaseVector::wrapInDictionary(nullptr, makeIndices({1, 0}), 2, input),
        options);
  }
  // Exporting must not change a child shared with another vector.
  EXPECT_FALSE(timestamps->isNullAt(0));
  EXPECT_EQ(timestamps->valueAt(0), Timestamp::max());
  roundTrip(input, wideOptions());
}

TEST_F(ArrowBridgeTemporalTest, nullParentPreservesConstantEncoding) {
  auto timestamps =
      makeFlatVector<Timestamp>({Timestamp(0, 1), Timestamp(0, 2)});
  auto constant = BaseVector::wrapInConstant(2, 1, timestamps);
  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, makeIndices({1, 0}), 2, timestamps);
  const std::vector<VectorPtr> payloads{
      makeRowVector({constant}),
      makeRowVector({dictionary}),
      makeArrayVector({0, 1}, dictionary),
      makeMapVector({0, 1}, makeFlatVector<int64_t>({1, 2}), dictionary)};
  for (bool ipc : {false, true}) {
    ArrowOptions options;
    options.exportToArrowIPC = ipc;
    for (const auto& payload : payloads) {
      auto child = BaseVector::wrapInConstant(3, 1, payload);
      auto input = makeRowVector({child});
      ReusableArrowBatchPool batchPool(1);
      for (bool allNull : {false, true, false, true}) {
        for (vector_size_t row = 0; row < input->size(); ++row) {
          input->setNull(row, allNull || row == 0);
        }
        SCOPED_TRACE(input->toString());
        arrowRoundTrip(input, options);
        arrowRoundTrip(input, options, &batchPool);
      }
      EXPECT_FALSE(child->isNullAt(0));
      EXPECT_FALSE(payload->isNullAt(1));
    }
  }
}

TEST_F(ArrowBridgeTemporalTest, dictionaryTimestampVisibility) {
  auto timestamps = makeFlatVector<Timestamp>(
      {Timestamp::max(), Timestamp(0, 1), Timestamp(0, 2), Timestamp::max()});
  auto dictionary = BaseVector::wrapInDictionary(
      nullptr, makeIndices({0, 1, 2}), 3, timestamps);
  auto unused = BaseVector::wrapInDictionary(
      nullptr, makeIndices({1, 2, 1}), 3, timestamps);
  auto nested = BaseVector::wrapInDictionary(
      nullptr, makeIndices({2, 0, 1}), 3, dictionary);
  for (bool ipc : {false, true}) {
    for (bool flatten : {false, true}) {
      ArrowOptions options;
      options.exportToArrowIPC = ipc;
      options.flattenDictionary = flatten;
      SCOPED_TRACE(fmt::format("ipc={}, flatten={}", ipc, flatten));
      arrowRoundTrip(unused, options);
      for (const auto& child : {dictionary, nested}) {
        SCOPED_TRACE(child == dictionary ? "dictionary" : "nested");
        auto input = makeRowVector({child});
        const vector_size_t hiddenRow = child == dictionary ? 0 : 1;
        input->setNull(hiddenRow, true);
        if (child == nested && !ipc && !flatten) {
          // The C Data path retains nested dictionaries. Validate with Arrow.
          ArrowData data;
          exportVector(input, data, options);
          EXPECT_OK_AND_ASSIGN(
              auto array, arrow::ImportArray(&data.array, &data.schema));
          ASSERT_OK(array->ValidateFull());
        } else {
          arrowRoundTrip(input, options);
        }
        input->setNull(hiddenRow, false);
        ArrowData data;
        BOLT_ASSERT_THROW(
            exportToArrow(input, data.array, pool(), options),
            "Could not convert Timestamp");
      }
      auto rows = makeRowVector({timestamps});
      auto complex =
          BaseVector::wrapInDictionary(nullptr, makeIndices({1, 2}), 2, rows);
      arrowRoundTrip(complex, options);
    }
  }
  EXPECT_FALSE(timestamps->isNullAt(0));
  EXPECT_FALSE(dictionary->isNullAt(0));
  EXPECT_EQ(timestamps->valueAt(0), Timestamp::max());
}

TEST_F(ArrowBridgeTemporalTest, nullParentRetainsConstantStorage) {
  auto integers =
      BaseVector::wrapInConstant(2, 0, makeFlatVector<int64_t>({123}));
  auto payload = makeRowVector(
      {integers,
       makeFlatVector<Timestamp>({Timestamp(0, 1), Timestamp(0, 2)})});
  auto input = makeRowVector({BaseVector::wrapInConstant(3, 1, payload)});
  input->setNulls(allocateNulls(input->size(), pool(), bits::kNull));
  for (bool ipc : {false, true}) {
    ArrowOptions options;
    options.exportToArrowIPC = ipc;
    ArrowData data;
    exportVector(input, data, options);
    const auto* values =
        data.array.children[0]->children[1]->children[0]->children[1];
    EXPECT_EQ(static_cast<const int64_t*>(values->buffers[1])[0], 123);
    EXPECT_OK_AND_ASSIGN(
        auto array, arrow::ImportArray(&data.array, &data.schema));
    ASSERT_OK(array->ValidateFull());
  }
}

TEST_F(ArrowBridgeTemporalTest, nullParentWithLongerTimestampChild) {
  auto timestamps = makeFlatVector<Timestamp>(65'536, [](auto row) {
    return row == 0 ? Timestamp::max() : Timestamp(0, 1);
  });
  timestamps->setNull(100, true);
  auto input = std::make_shared<RowVector>(
      pool(),
      ROW({{"ts", TIMESTAMP()}}),
      nullptr,
      2,
      std::vector<VectorPtr>{timestamps});
  input->setNull(0, true);
  arrowRoundTrip(input, {});
}

TEST_F(ArrowBridgeTemporalTest, dictionaryTimestampRetryReleasesSlot) {
  auto values = makeRowVector(
      {makeFlatVector<int64_t>({7, 8}),
       makeFlatVector<Timestamp>({Timestamp::max(), Timestamp(0, 1)})});
  auto input =
      BaseVector::wrapInDictionary(nullptr, makeIndices({0, 1}), 2, values);
  for (bool ipc : {false, true}) {
    ArrowOptions options;
    options.exportToArrowIPC = ipc;
    ReusableArrowBatchPool batchPool(1);
    ArrowArray* dictionary = nullptr;
    for (bool hidden : {true, true, false, true, true}) {
      input->setNull(0, hidden);
      ArrowData data;
      auto exportBatch = [&] {
        batchPool.exportToArrow(
            input, pool(), options, &data.schema, &data.array);
      };
      if (!hidden) {
        BOLT_ASSERT_THROW(exportBatch(), "Could not convert Timestamp");
        dictionary = nullptr;
        continue;
      }
      exportBatch();
      if (dictionary) {
        EXPECT_EQ(data.array.dictionary, dictionary);
      }
      dictionary = data.array.dictionary;
      EXPECT_OK_AND_ASSIGN(
          auto array, arrow::ImportArray(&data.array, &data.schema));
      ASSERT_OK(array->ValidateFull());
    }
  }
}

TEST_F(ArrowBridgeTemporalTest, dictionaryTimestampWithStringViews) {
  for (bool copyValues : {false, true}) {
    for (bool overflow : {false, true}) {
      auto strings = makeFlatVector<std::string>(
          {"unreferenced dictionary string",
           "first dictionary string",
           "second dictionary string"});
      auto values = makeRowVector(
          {strings,
           makeFlatVector<Timestamp>(
               {overflow ? Timestamp::max() : Timestamp(0, 0),
                Timestamp(0, 1),
                Timestamp(0, 2)})});
      auto input =
          BaseVector::wrapInDictionary(nullptr, makeIndices({1, 2}), 2, values);
      auto expected = makeRowVector(
          {makeFlatVector<std::string>(
               {"first dictionary string", "second dictionary string"}),
           makeFlatVector<Timestamp>({Timestamp(0, 1), Timestamp(0, 2)})});
      ArrowOptions options;
      options.exportToView = true;
      options.stringViewCopyValues = copyValues;
      ArrowData data;
      exportVector(input, data, options);
      EXPECT_EQ(
          data.array.dictionary->children[0]->buffers[2],
          strings->stringBuffers()[0]->as<void>());
      EXPECT_OK_AND_ASSIGN(
          auto array, arrow::ImportArray(&data.array, &data.schema));
      ASSERT_OK(array->ValidateFull());
      ASSERT_OK(arrow::ExportArray(*array, &data.array, &data.schema));
      assertEqualVectors(
          expected,
          importFromArrowAsOwner(data.schema, data.array, options, pool()));
    }
  }
}

TEST_F(ArrowBridgeTemporalTest, nullTimestampSelectionAllocation) {
  constexpr vector_size_t size = 65'536;
  auto timestamps = std::make_shared<FlatVector<Timestamp>>(
      pool(),
      TIMESTAMP(),
      allocateNulls(size, pool(), bits::kNull),
      size,
      nullptr,
      std::vector<BufferPtr>{});
  auto input =
      BaseVector::wrapInConstant(1, size - 1, makeRowVector({timestamps}));
  for (const auto& options : {ArrowOptions{}, wideOptions()}) {
    const auto before = pool()->usedBytes();
    ArrowData data;
    exportVector(input, data, options);
    // Selecting one row must not allocate storage proportional to the source.
    EXPECT_LT(pool()->usedBytes() - before, 4'096);
    const auto* values = data.array.children[1]->children[0];
    ASSERT_EQ(values->length, 1);
    EXPECT_EQ(values->null_count, 1);
    const auto* raw = static_cast<const uint64_t*>(values->buffers[1]);
    EXPECT_EQ(raw[0], 0);
    if (options.timestampEncoding == TimestampEncoding::kSecondsNanos) {
      EXPECT_EQ(raw[1], 0);
    }
    ArrowData fullData;
    exportVector(timestamps, fullData, options);
    const auto wordsPerValue =
        options.timestampEncoding == TimestampEncoding::kSecondsNanos ? 2 : 1;
    EXPECT_EQ(
        static_cast<const uint64_t*>(
            fullData.array.buffers[1])[wordsPerValue * size - 1],
        0);
  }
}

TEST_F(ArrowBridgeTemporalTest, unrelatedFieldsUnderNullRows) {
  auto integers = makeFlatVector<int64_t>({7, 8});
  auto arrays = makeArrayVector({0, 1}, integers);
  auto nested = makeRowVector({integers});
  auto timestamps =
      makeFlatVector<Timestamp>({Timestamp::max(), Timestamp(-1, 999'999'999)});
  auto input = makeRowVector({arrays, nested, timestamps});
  input->setNull(0, true);
  for (auto options : {ArrowOptions{}, wideOptions()}) {
    ArrowData data;
    exportVector(input, data, options);
    // Preserve the existing representation of fields without timestamps.
    EXPECT_EQ(data.array.children[0]->null_count, 0);
    EXPECT_EQ(data.array.children[1]->null_count, 0);
    EXPECT_EQ(
        data.array.children[1]->children[0]->buffers[1], integers->rawValues());
    assertEqualVectors(
        input,
        importFromArrowAsOwner(data.schema, data.array, options, pool()));
  }
}

TEST_F(ArrowBridgeTemporalTest, standardOwnerFailureReleasesOnce) {
  int schemaReleases = 0;
  int arrayReleases = 0;
  const int64_t value = 1;
  const void* buffers[] = {nullptr, &value};
  ArrowData data;
  rawArray(data, "tsn:", buffers, 1);
  data.array.offset = 1;
  data.schema.private_data = &schemaReleases;
  data.schema.release = [](ArrowSchema* schema) {
    ++*static_cast<int*>(schema->private_data);
    schema->release = nullptr;
  };
  data.array.private_data = &arrayReleases;
  data.array.release = [](ArrowArray* array) {
    ++*static_cast<int*>(array->private_data);
    array->release = nullptr;
  };
  EXPECT_THROW(
      importFromArrowAsViewer(data.schema, data.array, {}, pool()),
      BoltUserError);
  EXPECT_EQ(schemaReleases, 0);
  EXPECT_EQ(arrayReleases, 0);
  EXPECT_THROW(
      importFromArrowAsOwner(data.schema, data.array, {}, pool()),
      BoltUserError);
  EXPECT_EQ(data.schema.release, nullptr);
  EXPECT_EQ(data.array.release, nullptr);
  EXPECT_EQ(schemaReleases, 1);
  EXPECT_EQ(arrayReleases, 1);
}

TEST_F(ArrowBridgeTemporalTest, ownerKeepsRootForBorrowedChild) {
  for (bool fail : {false, true}) {
    int schemaReleases = 0;
    int arrayReleases = 0;
    const int64_t value = 42;
    const void* buffers[] = {nullptr, &value};
    ArrowData timestamp;
    rawArray(timestamp, "tsn:", buffers, 1);
    ArrowData integer;
    rawArray(integer, "l", buffers, 1);
    ArrowSchema* schemas[] = {&timestamp.schema, &integer.schema};
    ArrowArray* arrays[] = {&timestamp.array, &integer.array};
    ArrowData data;
    rawArray(data, "+s", buffers, 1);
    data.schema.n_children = 2;
    data.schema.children = schemas;
    data.schema.private_data = &schemaReleases;
    data.schema.release = [](ArrowSchema* schema) {
      ++*static_cast<int*>(schema->private_data);
      for (int64_t i = 0; i < schema->n_children; ++i) {
        if (schema->children[i]->release) {
          schema->children[i]->release(schema->children[i]);
        }
      }
      schema->release = nullptr;
    };
    data.array.n_buffers = 1;
    data.array.n_children = 2;
    data.array.children = arrays;
    data.array.private_data = &arrayReleases;
    data.array.release = [](ArrowArray* array) {
      ++*static_cast<int*>(array->private_data);
      for (int64_t i = 0; i < array->n_children; ++i) {
        if (array->children[i]->release) {
          array->children[i]->release(array->children[i]);
        }
      }
      array->release = nullptr;
    };
    if (fail) {
      integer.array.offset = 1;
      EXPECT_THROW(
          importFromArrowAsOwner(data.schema, data.array, {}, pool()),
          BoltUserError);
    } else {
      auto imported =
          importFromArrowAsOwner(data.schema, data.array, {}, pool());
      auto child = imported->as<RowVector>()->childAt(1);
      EXPECT_EQ(child->values()->as<int64_t>(), &value);
      imported.reset();
      EXPECT_EQ(schemaReleases, 0);
      EXPECT_EQ(arrayReleases, 0);
      EXPECT_EQ(child->as<FlatVector<int64_t>>()->valueAt(0), 42);
      child.reset();
    }
    EXPECT_EQ(data.schema.release, nullptr);
    EXPECT_EQ(data.array.release, nullptr);
    EXPECT_EQ(schemaReleases, 1);
    EXPECT_EQ(arrayReleases, 1);
  }
}

TEST_F(ArrowBridgeTemporalTest, reusableSchemaAfterOwnerImport) {
  auto input = makeRowVector({makeFlatVector<Timestamp>({Timestamp(0, 1)})});
  ReusableArrowBatchPool batchPool(1);
  for (int i = 0; i < 3; ++i) {
    ArrowData data;
    EXPECT_EQ(
        batchPool.exportToArrow(input, pool(), {}, &data.schema, &data.array),
        i == 0);
    assertEqualVectors(
        input, importFromArrowAsOwner(data.schema, data.array, {}, pool()));
  }
}

TEST_F(ArrowBridgeTemporalTest, fullRangeAndLayout) {
  auto input = makeNullableFlatVector<Timestamp>(
      {Timestamp::min(),
       Timestamp(-62'167'219'200, 123'456'789),
       Timestamp(-1, 999'999'999),
       Timestamp(0, 1),
       std::nullopt,
       Timestamp(253'402'300'799, 999'999'999),
       Timestamp::max()});
  auto options = wideOptions();
  roundTrip(input, options);

  ArrowData data;
  exportVector(input, data, options);
  ASSERT_STREQ(data.schema.format, "w:16");
  auto metadata = timestampMetadata();
  EXPECT_EQ(memcmp(data.schema.metadata, metadata.data(), metadata.size()), 0);
  const auto* values = static_cast<const char*>(data.array.buffers[1]);
  for (vector_size_t i = 0; i < input->size(); ++i) {
    if (!input->isNullAt(i)) {
      EXPECT_EQ(
          folly::Endian::little(folly::loadUnaligned<int64_t>(values + i * 16)),
          input->valueAt(i).getSeconds());
      EXPECT_EQ(
          folly::Endian::little(
              folly::loadUnaligned<uint64_t>(values + i * 16 + 8)),
          input->valueAt(i).getNanos());
    }
  }
  for (auto unit :
       {TimestampUnit::kSecond,
        TimestampUnit::kMilli,
        TimestampUnit::kMicro,
        TimestampUnit::kNano}) {
    options.timestampUnit = unit;
    options.timestampTimeZone = std::nullopt;
    roundTrip(input, options);
  }
}

TEST_F(ArrowBridgeTemporalTest, nestedAndEncoded) {
  auto input = makeNullableFlatVector<Timestamp>(
      {Timestamp(-1, 123'456'789), std::nullopt, Timestamp::max()});
  auto arrays = makeArrayVector({0, 2, 2}, input, {1});
  auto maps =
      makeMapVector({0, 1, 2}, makeFlatVector<int32_t>({1, 2, 3}), input);
  auto rows =
      makeRowVector({input, arrays, maps, makeFlatVector<int64_t>({7, 8, 9})});
  auto indices = makeIndices({2, 0, 1, 2});
  auto dict = BaseVector::wrapInDictionary(nullptr, indices, 4, rows);
  std::vector<VectorPtr> vectors{
      input,
      rows,
      dict,
      BaseVector::wrapInDictionary(nullptr, indices, 4, input),
      BaseVector::wrapInConstant(4, 2, input),
      BaseVector::wrapInConstant(4, 0, rows),
      BaseVector::createNullConstant(TIMESTAMP(), 4, pool()),
      makeFlatVector<Timestamp>({})};
  for (bool flatten : {false, true}) {
    auto options = wideOptions();
    options.flattenDictionary = flatten;
    options.flattenConstant = flatten;
    for (const auto& vector : vectors) {
      roundTrip(vector, options);
    }
  }
}

TEST_F(ArrowBridgeTemporalTest, extensionValidationAndUnalignedInput) {
  auto metadata = timestampMetadata();
  std::array<char, 17> values{};
  auto seconds = folly::Endian::little(int64_t{-1});
  auto nanos = folly::Endian::little(uint64_t{123'456'789});
  memcpy(values.data() + 1, &seconds, 8);
  memcpy(values.data() + 9, &nanos, 8);
  const void* buffers[] = {nullptr, values.data() + 1};
  ArrowData data;
  rawArray(data, "w:16", buffers, 1);
  data.schema.metadata = metadata.data();
  assertEqualVectors(
      makeFlatVector<Timestamp>({Timestamp(-1, 123'456'789)}),
      importFromArrowAsViewer(data.schema, data.array, {}, pool()));

  for (const auto& invalid :
       {timestampMetadata("another.type"),
        timestampMetadata("bolt.timestamp", "2"),
        timestampMetadata("bolt.timestamp", "invalid")}) {
    data.schema.metadata = invalid.data();
    EXPECT_THROW(importFromArrow(data.schema), BoltUserError);
  }
  data.schema.metadata = nullptr;
  EXPECT_THROW(importFromArrow(data.schema), BoltUserError);
  data.schema.metadata = metadata.data();
  EXPECT_EQ(*importFromArrow(data.schema), *TIMESTAMP());
  for (const auto* format : {"w:8", "l", "tsn:"}) {
    data.schema.format = format;
    BOLT_ASSERT_THROW(
        importFromArrow(data.schema), "requires FixedSizeBinary(16)");
  }
  data.schema.format = "w:16";
  nanos = folly::Endian::little(uint64_t{1'000'000'000});
  memcpy(values.data() + 9, &nanos, 8);
  BOLT_ASSERT_THROW(
      importFromArrowAsViewer(data.schema, data.array, {}, pool()),
      "Invalid timestamp nanos");

  nanos = 0;
  seconds = folly::Endian::little(INT64_MAX);
  memcpy(values.data() + 1, &seconds, 8);
  memcpy(values.data() + 9, &nanos, 8);
  EXPECT_THROW(
      importFromArrowAsViewer(data.schema, data.array, {}, pool()),
      BoltUserError);
  data.array.offset = 1;
  BOLT_ASSERT_THROW(
      importFromArrowAsViewer(data.schema, data.array, {}, pool()),
      "Offsets are not supported");
}

TEST_F(ArrowBridgeTemporalTest, allNullTimestampWithoutNativeValues) {
  auto input = std::make_shared<FlatVector<Timestamp>>(
      pool(),
      TIMESTAMP(),
      allocateNulls(3, pool(), bits::kNull),
      3,
      nullptr,
      std::vector<BufferPtr>{});
  ArrowData data;
  exportVector(input, data, wideOptions());
  const auto* values = static_cast<const uint64_t*>(data.array.buffers[1]);
  for (vector_size_t i = 0; i < input->size() * 2; ++i) {
    EXPECT_EQ(values[i], 0);
  }
  roundTrip(input, wideOptions());
}

TEST_F(ArrowBridgeTemporalTest, unalignedTime) {
  std::array<char, 17> bytes{};
  const int64_t values[] = {1'000, 86'399'999'000};
  memcpy(bytes.data() + 1, values, sizeof(values));
  const void* buffers[] = {nullptr, bytes.data() + 1};
  ArrowData data;
  rawArray(data, "ttu", buffers, 2);
  assertEqualVectors(
      makeFlatVector<int64_t>({values[0], values[1]}),
      importFromArrowAsViewer(data.schema, data.array, {}, pool()));
  ArrowOptions options;
  options.timeImportMode = TimeImportMode::kMillisOfDay;
  assertEqualVectors(
      makeFlatVector<int32_t>({1, 86'399'999}),
      importFromArrowAsOwner(data.schema, data.array, options, pool()));
}

TEST_F(ArrowBridgeTemporalTest, timeUnitsAndMillisConversion) {
  for (auto format : {"tts", "ttm", "ttu", "ttn"}) {
    const int64_t scale = format[2] == 's' ? 1
        : format[2] == 'm'                 ? 1'000
        : format[2] == 'u'                 ? 1'000'000
                                           : 1'000'000'000;
    const std::vector<int64_t> values{0, 1, 86'400 * scale - 1, -1};
    const std::vector<int32_t> values32(values.begin(), values.end());
    uint64_t nulls = 0b0111;
    const bool time32 = scale <= 1'000;
    const void* buffers[] = {
        &nulls,
        time32 ? static_cast<const void*>(values32.data()) : values.data()};
    ArrowData data;
    rawArray(data, format, buffers, values.size(), -1);
    auto result = importFromArrowAsViewer(data.schema, data.array, {}, pool());
    EXPECT_EQ(
        result->typeKind(), time32 ? TypeKind::INTEGER : TypeKind::BIGINT);
    EXPECT_EQ(result->values()->as<void>(), buffers[1]);
    if (time32) {
      assertEqualVectors(
          makeNullableFlatVector<int32_t>({0, 1, values32[2], std::nullopt}),
          result);
    } else {
      assertEqualVectors(
          makeNullableFlatVector<int64_t>({0, 1, values[2], std::nullopt}),
          result);
    }

    ArrowOptions options;
    options.timeImportMode = TimeImportMode::kMillisOfDay;
    EXPECT_EQ(*importFromArrow(data.schema, options), *INTEGER());
    if (time32) {
      assertEqualVectors(
          makeNullableFlatVector<int32_t>(
              {0,
               static_cast<int32_t>(1'000 / scale),
               static_cast<int32_t>(values[2] * 1'000 / scale),
               std::nullopt}),
          importFromArrowAsViewer(data.schema, data.array, options, pool()));
    } else {
      BOLT_ASSERT_THROW(
          importFromArrowAsViewer(data.schema, data.array, options, pool()),
          "sub-millisecond precision");
    }

    const int64_t millisValues[] = {
        0, 12'345 * std::max<int64_t>(1, scale / 1'000)};
    const int32_t millisValues32[] = {0, 12'345};
    buffers[1] =
        time32 ? static_cast<const void*>(millisValues32) : millisValues;
    data.array.length = 2;
    data.array.null_count = 0;
    assertEqualVectors(
        makeFlatVector<int32_t>(
            {0, time32 && scale == 1 ? 12'345'000 : 12'345}),
        importFromArrowAsViewer(data.schema, data.array, options, pool()));
    for (int64_t invalid : {int64_t{-1}, 86'400 * scale}) {
      const auto invalid32 = static_cast<int32_t>(invalid);
      buffers[1] = time32 ? static_cast<const void*>(&invalid32) : &invalid;
      data.array.length = 1;
      for (auto mode :
           {TimeImportMode::kPreserveUnits, TimeImportMode::kMillisOfDay}) {
        options.timeImportMode = mode;
        BOLT_ASSERT_THROW(
            importFromArrowAsViewer(data.schema, data.array, options, pool()),
            "time-of-day range");
      }
    }
  }
}

TEST_F(ArrowBridgeTemporalTest, timeInNestedDictionaryAndConstant) {
  auto values = makeNullableFlatVector<int64_t>(
      {1'234'000, std::nullopt, 86'399'999'000});
  auto expectedValues =
      makeNullableFlatVector<int32_t>({1'234, std::nullopt, 86'399'999});
  for (int encoding = 0; encoding < 3; ++encoding) {
    VectorPtr input = values;
    VectorPtr expected = expectedValues;
    if (encoding == 1) {
      const auto indices = makeIndices({2, 1, 0});
      input = BaseVector::wrapInDictionary(nullptr, indices, 3, input);
      expected = BaseVector::wrapInDictionary(nullptr, indices, 3, expected);
    } else if (encoding == 2) {
      input = BaseVector::wrapInConstant(3, 0, input);
      expected = BaseVector::wrapInConstant(3, 0, expected);
    }
    auto row = makeRowVector({makeArrayVector({0, 1, 2}, input)});
    ArrowData data;
    exportVector(row, data, {});
    auto* leaf = data.schema.children[0]->children[0];
    if (leaf->dictionary) {
      leaf = leaf->dictionary;
    } else if (std::string_view(leaf->format) == "+r") {
      leaf = leaf->children[1];
    }
    leaf->format = "ttu";
    ArrowOptions options;
    options.timeImportMode = TimeImportMode::kMillisOfDay;
    auto expectedRow = makeRowVector({makeArrayVector({0, 1, 2}, expected)});
    EXPECT_EQ(*importFromArrow(data.schema, options), *expectedRow->type());
    assertEqualVectors(
        expectedRow,
        importFromArrowAsOwner(data.schema, data.array, options, pool()));
  }
}

TEST_F(ArrowBridgeTemporalTest, ownerFailureReleasesOnce) {
  for (auto format : {"w:16", "ttn"}) {
    int schemaReleases = 0;
    int arrayReleases = 0;
    uint64_t values[] = {
        folly::Endian::little(uint64_t{86'400'000'000'000}),
        folly::Endian::little(uint64_t{1'000'000'000})};
    const void* buffers[] = {nullptr, values};
    const auto metadata = timestampMetadata();
    ArrowData data;
    rawArray(data, format, buffers, 1);
    data.schema.metadata =
        std::string_view(format) == "w:16" ? metadata.data() : nullptr;
    data.schema.private_data = &schemaReleases;
    data.schema.release = [](ArrowSchema* schema) {
      ++*static_cast<int*>(schema->private_data);
      schema->release = nullptr;
    };
    data.array.private_data = &arrayReleases;
    data.array.release = [](ArrowArray* array) {
      ++*static_cast<int*>(array->private_data);
      array->release = nullptr;
    };
    EXPECT_THROW(
        importFromArrowAsViewer(data.schema, data.array, {}, pool()),
        BoltUserError);
    EXPECT_EQ(schemaReleases, 0);
    EXPECT_EQ(arrayReleases, 0);
    EXPECT_THROW(
        importFromArrowAsOwner(data.schema, data.array, {}, pool()),
        BoltUserError);
    EXPECT_EQ(data.schema.release, nullptr);
    EXPECT_EQ(data.array.release, nullptr);
    EXPECT_EQ(schemaReleases, 1);
    EXPECT_EQ(arrayReleases, 1);
  }
}

TEST_F(ArrowBridgeTemporalTest, ownerKeepsUnconvertedBuffers) {
  auto timestamps =
      makeFlatVector<Timestamp>({Timestamp::max(), Timestamp(0, 1)});
  auto integers = makeFlatVector<int64_t>({42, 43});
  auto input = makeRowVector({makeArrayVector({0, 1}, timestamps), integers});
  auto valuesBuffer = integers->values();
  const auto* values = integers->rawValues();
  ArrowData data;
  exportVector(input, data, wideOptions());
  const auto* offsets = data.array.children[0]->buffers[1];
  input.reset();
  integers.reset();
  timestamps.reset();
  auto imported = importFromArrowAsOwner(data.schema, data.array, {}, pool());
  auto* row = imported->as<RowVector>();
  EXPECT_EQ(row->childAt(1)->values()->as<int64_t>(), values);
  EXPECT_EQ(row->childAt(0)->as<ArrayVector>()->rawOffsets(), offsets);
  EXPECT_EQ(row->childAt(1)->as<FlatVector<int64_t>>()->valueAt(1), 43);
  EXPECT_GT(valuesBuffer->refCount(), 1);
  auto child = row->childAt(1);
  imported.reset();
  EXPECT_GT(valuesBuffer->refCount(), 1);
  EXPECT_EQ(child->as<FlatVector<int64_t>>()->valueAt(1), 43);
  child.reset();
  EXPECT_EQ(valuesBuffer->refCount(), 1);
}

TEST_F(ArrowBridgeTemporalTest, nestedOwnerFailure) {
  const auto usedBytes = pool()->usedBytes();
  ArrowData data;
  {
    auto input = makeRowVector(
        {makeFlatVector<int64_t>({42}),
         makeArrayVector({0}, makeFlatVector<Timestamp>({Timestamp(0, 1)}))});
    exportVector(input, data, wideOptions());
  }
  const auto invalidNanos = folly::Endian::little(uint64_t{1'000'000'000});
  auto* values = const_cast<char*>(static_cast<const char*>(
      data.array.children[1]->children[0]->buffers[1]));
  memcpy(values + sizeof(int64_t), &invalidNanos, sizeof(invalidNanos));
  EXPECT_THROW(
      importFromArrowAsOwner(data.schema, data.array, {}, pool()),
      BoltUserError);
  EXPECT_EQ(data.schema.release, nullptr);
  EXPECT_EQ(data.array.release, nullptr);
  EXPECT_EQ(pool()->usedBytes(), usedBytes);
}

TEST_F(ArrowBridgeTemporalTest, reusableSchemaAndIpc) {
  auto input =
      makeRowVector({makeFlatVector<Timestamp>({Timestamp(-1, 123'456'789)})});
  ReusableArrowBatchPool batchPool(1);
  for (int i = 0; i < 6; ++i) {
    ArrowData data;
    const bool wide = i % 3 != 0;
    const auto options = wide ? wideOptions() : ArrowOptions{};
    EXPECT_EQ(
        batchPool.exportToArrow(
            input, pool(), options, &data.schema, &data.array),
        i % 3 != 2);
    EXPECT_STREQ(data.schema.children[0]->format, wide ? "w:16" : "tsn:UTC");
    assertEqualVectors(
        input, importFromArrowAsOwner(data.schema, data.array, {}, pool()));
  }

  auto fullRange = makeRowVector({makeNullableFlatVector<Timestamp>(
      {Timestamp::min(), Timestamp::max(), std::nullopt})});
  ArrowData data;
  auto options = wideOptions();
  options.exportToArrowIPC = true;
  exportVector(fullRange, data, options);
  EXPECT_OK_AND_ASSIGN(
      auto batch, arrow::ImportRecordBatch(&data.array, &data.schema));
  ASSERT_OK(batch->ValidateFull());
  EXPECT_OK_AND_ASSIGN(auto sink, arrow::io::BufferOutputStream::Create());
  EXPECT_OK_AND_ASSIGN(
      auto writer, arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  ASSERT_OK(writer->WriteRecordBatch(*batch));
  ASSERT_OK(writer->Close());
  EXPECT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  EXPECT_OK_AND_ASSIGN(
      auto reader,
      arrow::ipc::RecordBatchStreamReader::Open(
          std::make_shared<arrow::io::BufferReader>(buffer)));
  EXPECT_OK_AND_ASSIGN(auto restored, reader->Next());
  ASSERT_NE(restored, nullptr);
  ASSERT_TRUE(restored->column(0)->IsNull(2));
  const auto* values = reinterpret_cast<const uint64_t*>(
      restored->column(0)->data()->buffers[1]->data());
  EXPECT_EQ(values[4], 0);
  EXPECT_EQ(values[5], 0);
  ASSERT_OK(arrow::ExportRecordBatch(*restored, &data.array, &data.schema));
  assertEqualVectors(
      fullRange, importFromArrowAsOwner(data.schema, data.array, {}, pool()));
}

} // namespace
} // namespace bytedance::bolt::test
