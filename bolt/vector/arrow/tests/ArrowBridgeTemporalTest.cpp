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
    EXPECT_EQ(*importFromArrow(data.schema), *input->type());
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

TEST_F(ArrowBridgeTemporalTest, missingValuesAndUnknownNullCount) {
  uint64_t nulls = 0;
  const void* buffers[] = {&nulls, nullptr};
  for (auto format : {"tsn:"}) {
    for (auto nullCount : {int64_t{3}, int64_t{-1}}) {
      ArrowData data;
      rawArray(data, format, buffers, 3, nullCount);
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
}

TEST_F(ArrowBridgeTemporalTest, unrelatedFieldsUnderNullRows) {
  auto integers = makeFlatVector<int64_t>({7, 8});
  auto arrays = makeArrayVector({0, 1}, integers);
  auto nested = makeRowVector({integers});
  auto timestamps =
      makeFlatVector<Timestamp>({Timestamp::max(), Timestamp(-1, 999'999'999)});
  auto input = makeRowVector({arrays, nested, timestamps});
  input->setNull(0, true);
  for (auto options : {ArrowOptions{}}) {
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

} // namespace
} // namespace bytedance::bolt::test
