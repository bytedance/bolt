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

#include <limits>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/row/CompactRow.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt::test;
namespace bytedance::bolt::row {
namespace {

class CompactRowTest : public ::testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  /// TODO Replace with VectorFuzzer::fuzzInputRow once
  /// https://github.com/facebookincubator/velox/issues/6195 is fixed.
  RowVectorPtr fuzzInputRow(VectorFuzzer& fuzzer, const RowTypePtr& rowType) {
    const auto size = fuzzer.getOptions().vectorSize;
    std::vector<VectorPtr> children;
    for (auto i = 0; i < rowType->size(); ++i) {
      children.push_back(fuzzer.fuzz(rowType->childAt(i), size));
    }

    return std::make_shared<RowVector>(
        pool_.get(), rowType, nullptr, size, std::move(children));
  }

  void testRoundTrip(const RowVectorPtr& data) {
    SCOPED_TRACE(data->toString());

    auto rowType = asRowType(data->type());
    auto numRows = data->size();
    std::vector<size_t> rowSize(numRows);
    std::vector<size_t> offsets(numRows);

    CompactRow row(data);

    size_t totalSize = 0;
    if (auto fixedRowSize = CompactRow::fixedRowSize(rowType)) {
      totalSize = fixedRowSize.value() * numRows;
      for (auto i = 0; i < numRows; ++i) {
        rowSize[i] = fixedRowSize.value();
        offsets[i] = fixedRowSize.value() * i;
      }
    } else {
      for (auto i = 0; i < numRows; ++i) {
        rowSize[i] = row.rowSize(i);
        offsets[i] = totalSize;
        totalSize += rowSize[i];
      }
    }

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool(), 0);
    auto* rawBuffer = buffer->asMutable<char>();
    {
      // Test serialize row-by-row.
      size_t offset = 0;
      std::vector<std::string_view> serialized;
      for (auto i = 0; i < numRows; ++i) {
        auto size = row.serialize(i, rawBuffer + offset);
        serialized.push_back(std::string_view(rawBuffer + offset, size));
        offset += size;
        BOLT_CHECK_EQ(size, row.rowSize(i), "Row {}: {}", i, data->toString(i));
      }

      BOLT_CHECK_EQ(offset, totalSize);

      auto copy = CompactRow::deserialize(serialized, rowType, pool());
      assertEqualVectors(data, copy);
    }
    {
      // Test serialize by range.
      memset(rawBuffer, 0, totalSize);

      std::vector<std::string_view> serialized;
      vector_size_t offset = 0;
      vector_size_t rangeSize = 1;
      // Serialize with different range size.
      while (offset < numRows) {
        auto size = std::min<vector_size_t>(rangeSize, numRows - offset);
        row.serialize(offset, size, offsets.data() + offset, rawBuffer);
        offset += size;
        rangeSize = checkedMultiply<vector_size_t>(rangeSize, 2);
      }

      for (auto i = 0; i < numRows; ++i) {
        serialized.push_back(
            std::string_view(rawBuffer + offsets[i], rowSize[i]));
      }
      auto copy = CompactRow::deserialize(serialized, rowType, pool());
      assertEqualVectors(data, copy);
    }
  }
};

TEST_F(CompactRowTest, fixedRowSize) {
  ASSERT_EQ(1 + 1, CompactRow::fixedRowSize(ROW({BOOLEAN()})));
  ASSERT_EQ(1 + 8, CompactRow::fixedRowSize(ROW({BIGINT()})));
  ASSERT_EQ(1 + 4, CompactRow::fixedRowSize(ROW({INTEGER()})));
  ASSERT_EQ(1 + 2, CompactRow::fixedRowSize(ROW({SMALLINT()})));
  ASSERT_EQ(1 + 8, CompactRow::fixedRowSize(ROW({DOUBLE()})));
  ASSERT_EQ(std::nullopt, CompactRow::fixedRowSize(ROW({VARCHAR()})));
  ASSERT_EQ(std::nullopt, CompactRow::fixedRowSize(ROW({ARRAY(BIGINT())})));
  ASSERT_EQ(
      1 + 1 + 8 + 4 + 2 + 8,
      CompactRow::fixedRowSize(
          ROW({BOOLEAN(), BIGINT(), INTEGER(), SMALLINT(), DOUBLE()})));

  ASSERT_EQ(std::nullopt, CompactRow::fixedRowSize(ROW({BIGINT(), VARCHAR()})));
  ASSERT_EQ(
      std::nullopt,
      CompactRow::fixedRowSize(ROW({BIGINT(), ROW({VARCHAR()})})));

  ASSERT_EQ(1, CompactRow::fixedRowSize(ROW({UNKNOWN()})));
}

TEST_F(CompactRowTest, rowSizeString) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({"a", "abc", "Longer string", "d", ""}),
  });

  CompactRow row(data);

  // 1 byte for null flags. 4 bytes for string size. N bytes for the string
  // itself.
  ASSERT_EQ(1 + 4 + 1, row.rowSize(0));
  ASSERT_EQ(1 + 4 + 3, row.rowSize(1));
  ASSERT_EQ(1 + 4 + 13, row.rowSize(2));
  ASSERT_EQ(1 + 4 + 1, row.rowSize(3));
  ASSERT_EQ(1 + 4 + 0, row.rowSize(4));
}

TEST_F(CompactRowTest, rowSizeArrayOfBigint) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {},
          {6},
      }),
  });

  {
    CompactRow row(data);

    // 1 byte for null flags. 4 bytes for array
    // size. 1 byte for null flags for elements. N bytes for array elements.
    ASSERT_EQ(1 + 4 + 1 + 8 * 3, row.rowSize(0));
    ASSERT_EQ(1 + 4 + 1 + 8 * 2, row.rowSize(1));
    ASSERT_EQ(1 + 4, row.rowSize(2));
    ASSERT_EQ(1 + 4 + 1 + 8, row.rowSize(3));
  }

  data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {{1, 2, std::nullopt, 3}},
          {{4, 5}},
          emptyArray,
          std::nullopt,
          {{6}},
      }),
  });

  {
    CompactRow row(data);

    // 1 byte for null flags. 4 bytes for array
    // size. 1 byte for null flags for elements. N bytes for array elements.
    ASSERT_EQ(1 + 4 + 1 + 8 * 4, row.rowSize(0));
    ASSERT_EQ(1 + 4 + 1 + 8 * 2, row.rowSize(1));
    ASSERT_EQ(1 + 4, row.rowSize(2));
    ASSERT_EQ(1, row.rowSize(3));
    ASSERT_EQ(1 + 4 + 1 + 8, row.rowSize(4));
  }
}

TEST_F(CompactRowTest, rowSizeMixed) {
  auto data = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt}),
      makeNullableFlatVector<std::string>({"a", "abc", "", std::nullopt}),
  });

  CompactRow row(data);

  // 1 byte for null flags. 8 bytes for bigint field. 4 bytes for string size.
  // N bytes for the string itself.
  ASSERT_EQ(1 + 8 + (4 + 1), row.rowSize(0));
  ASSERT_EQ(1 + 8 + (4 + 3), row.rowSize(1));
  ASSERT_EQ(1 + 8 + (4 + 0), row.rowSize(2));
  ASSERT_EQ(1 + 8, row.rowSize(3));
}

TEST_F(CompactRowTest, rowSizeArrayOfStrings) {
  auto data = makeRowVector({
      makeArrayVector<std::string>({
          {"a", "Abc"},
          {},
          {"a", "Longer string", "abc"},
      }),
  });

  {
    CompactRow row(data);

    // 1 byte for null flags. 4 bytes for array
    // size. 1 byte for nulls flags for elements. 4 bytes for serialized size. 4
    // bytes per offset of an element. N bytes for elements. Each string element
    // is 4 bytes for size + string length.
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + (4 + 3), row.rowSize(0));
    ASSERT_EQ(1 + 4, row.rowSize(1));
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + (4 + 13) + (4 + 3), row.rowSize(2));
  }

  data = makeRowVector({
      makeNullableArrayVector<std::string>({
          {{"a", "Abc", std::nullopt}},
          emptyArray,
          std::nullopt,
          {{"a", std::nullopt, "Longer string", "abc"}},
      }),
  });

  {
    CompactRow row(data);

    // Null strings do not take space.
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + (4 + 3) + 0, row.rowSize(0));
    ASSERT_EQ(1 + 4, row.rowSize(1));
    ASSERT_EQ(1, row.rowSize(2));
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + 0 + (4 + 13) + (4 + 3), row.rowSize(3));
  }
}

TEST_F(CompactRowTest, boolean) {
  auto data = makeRowVector({
      makeFlatVector<bool>(
          {true, false, true, true, false, false, true, false}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableFlatVector<bool>({
          true,
          false,
          std::nullopt,
          true,
          std::nullopt,
          false,
          true,
          false,
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, bigint) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableFlatVector<int64_t>(
          {1, std::nullopt, 3, std::nullopt, 5, std::nullopt}),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, hugeint) {
  auto data = makeRowVector({
      makeFlatVector<int128_t>({1, 2, 3, 4, 5}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableFlatVector<int128_t>(
          {std::nullopt, 1, 2, std::nullopt, std::nullopt, 3, 4, 5}),
  });

  testRoundTrip(data);
}

Timestamp ts(int64_t micros) {
  return Timestamp::fromMicros(micros);
}

TEST_F(CompactRowTest, timestamp) {
  auto data = makeRowVector({
      makeFlatVector<Timestamp>({
          ts(0),
          ts(1),
          ts(2),
      }),
  });

  testRoundTrip(data);

  // Serialize null Timestamp values with null flags set over a large
  // non-serializable value (e.g. a value that triggers an exception in
  // Timestamp::toMicros()).
  data = makeRowVector({
      makeFlatVector<Timestamp>({
          ts(0),
          Timestamp::max(),
          ts(123'456),
          Timestamp::min(),
      }),
  });

  data->childAt(0)->setNull(1, true);
  data->childAt(0)->setNull(3, true);

  testRoundTrip(data);
}

TEST_F(CompactRowTest, string) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({"a", "Abc", "", "Longer test string"}),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, unknown) {
  auto data = makeRowVector({
      makeAllNullFlatVector<UnknownValue>(10),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeArrayVector({0, 3, 5, 9}, makeAllNullFlatVector<UnknownValue>(10)),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, arrayOfUnknownFollowedByFields) {
  auto data = makeRowVector({
      makeArrayVector({0, 1, 3, 3}, makeAllNullFlatVector<UnknownValue>(4)),
      makeArrayVector<int64_t>({{10}, {20, 30}, {}, {40}}),
      makeFlatVector<std::string>({"a", "bb", "ccc", "dddd"}),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, mix) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({"a", "Abc", "", "Longer test string"}),
      makeAllNullFlatVector<UnknownValue>(4),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, arrayOfBigint) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6},
          {},
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {{1, 2, std::nullopt, 3}},
          {{4, 5, std::nullopt}},
          {{std::nullopt, 6}},
          {{std::nullopt}},
          std::nullopt,
          emptyArray,
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, arrayOfTimestamp) {
  auto data = makeRowVector({
      makeArrayVector<Timestamp>({
          {ts(1), ts(2), ts(3)},
          {ts(4), ts(5)},
          {ts(6)},
          {},
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableArrayVector<Timestamp>({
          {{ts(1), ts(2), std::nullopt, ts(3)}},
          {{ts(4), ts(5), std::nullopt}},
          {{std::nullopt, ts(6)}},
          {{std::nullopt}},
          std::nullopt,
          emptyArray,
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, arrayOfString) {
  auto data = makeRowVector({
      makeArrayVector<std::string>({
          {"a", "abc", "Longer test string"},
          {"b", "Abc 12345 ...test", "foo"},
          {},
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableArrayVector<std::string>({
          {{"a", std::nullopt, "abc", "Longer test string"}},
          {{std::nullopt,
            "b",
            std::nullopt,
            "Abc 12345 ...test",
            std::nullopt,
            "foo"}},
          emptyArray,
          {{std::nullopt}},
          std::nullopt,
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, map) {
  auto data = makeRowVector({
      makeMapVector<int16_t, int64_t>(
          {{{1, 10}, {2, 20}, {3, 30}}, {{1, 11}, {2, 22}}, {{4, 444}}, {}}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeMapVector<std::string, std::string>({
          {{"a", "100"},
           {"b", "200"},
           {"Long string for testing", "Another long string"}},
          {{"abc", "300"}, {"d", "400"}},
          {},
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, row) {
  auto data = makeRowVector({
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeFlatVector<double>({1.05, 2.05, 3.05, 4.05, 5.05}),
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeFlatVector<std::string>(
              {"a", "Abc", "Long test string", "", "d"}),
          makeFlatVector<double>({1.05, 2.05, 3.05, 4.05, 5.05}),
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeRowVector(
          {
              makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
              makeNullableFlatVector<int64_t>({-1, 2, -3, std::nullopt, -5}),
              makeFlatVector<double>({1.05, 2.05, 3.05, 4.05, 5.05}),
              makeFlatVector<std::string>(
                  {"a", "Abc", "Long test string", "", "d"}),
          },
          nullEvery(2)),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, rejectsOversizedSerializedRow) {
  constexpr size_t kNumFields = 2'048;
  auto stringVector = makeFlatVector<std::string>({std::string(1 << 20, 'x')});
  auto data = makeRowVector(std::vector<VectorPtr>(kNumFields, stringVector));

  CompactRow row(data);
  BOLT_ASSERT_USER_THROW(
      row.rowSize(0), "serialized value exceeds the maximum supported size");
}

TEST_F(CompactRowTest, rejectsMalformedInput) {
  const auto appendInt32 = [](std::string& data, int32_t value) {
    data.append(reinterpret_cast<const char*>(&value), sizeof(value));
  };
  const auto expectInvalid = [&](const RowTypePtr& type,
                                 std::string data,
                                 std::string_view expectedMessage) {
    std::vector<std::string_view> rows{data};
    BOLT_ASSERT_RUNTIME_THROW(
        CompactRow::deserialize(rows, type, pool()), expectedMessage);
  };

  // Missing row null bitmap and truncated fixed-width field.
  expectInvalid(ROW({BIGINT()}), "", "row null bitmap");
  expectInvalid(ROW({BIGINT()}), std::string(1, '\0'), "fixed-width value");
  expectInvalid(
      ROW({BIGINT()}), std::string(1, '\1'), "null fixed-width row field");

  // Negative and truncated string lengths.
  std::string negativeString(1, '\0');
  appendInt32(negativeString, -1);
  expectInvalid(
      ROW({VARCHAR()}), std::move(negativeString), "negative string size");

  std::string truncatedString(1, '\0');
  appendInt32(truncatedString, 3);
  truncatedString.push_back('a');
  expectInvalid(ROW({VARCHAR()}), std::move(truncatedString), "string payload");

  // Negative array cardinality and a fixed-width array with no payload.
  std::string negativeArray(1, '\0');
  appendInt32(negativeArray, -1);
  expectInvalid(
      ROW({ARRAY(BIGINT())}), std::move(negativeArray), "negative array size");

  std::string truncatedArray(1, '\0');
  appendInt32(truncatedArray, 1);
  expectInvalid(
      ROW({ARRAY(BIGINT())}), std::move(truncatedArray), "array payload");

  // The maximum int32 cardinality must not overflow null-bitmap sizing.
  std::string hugeUnknownArray(1, '\0');
  appendInt32(hugeUnknownArray, std::numeric_limits<int32_t>::max());
  expectInvalid(
      ROW({ARRAY(UNKNOWN())}),
      std::move(hugeUnknownArray),
      "needs 268435456 bytes");

  // Complex arrays reject invalid serialized sizes and offsets.
  std::string negativeComplexSize(1, '\0');
  appendInt32(negativeComplexSize, 1);
  negativeComplexSize.push_back('\0');
  appendInt32(negativeComplexSize, -1);
  appendInt32(negativeComplexSize, 0);
  expectInvalid(
      ROW({ARRAY(ARRAY(BIGINT()))}),
      std::move(negativeComplexSize),
      "negative complex array serialized size");

  std::string undersizedOffsetTable(1, '\0');
  appendInt32(undersizedOffsetTable, 2);
  undersizedOffsetTable.push_back('\0');
  appendInt32(undersizedOffsetTable, sizeof(int32_t));
  undersizedOffsetTable.append(2 * sizeof(int32_t), '\0');
  expectInvalid(
      ROW({ARRAY(ARRAY(BIGINT()))}),
      std::move(undersizedOffsetTable),
      "smaller than its 8-byte offset table");

  std::string offsetInsideTable(1, '\0');
  appendInt32(offsetInsideTable, 1);
  offsetInsideTable.push_back('\0');
  appendInt32(offsetInsideTable, sizeof(int32_t));
  appendInt32(offsetInsideTable, 0);
  expectInvalid(
      ROW({ARRAY(ARRAY(BIGINT()))}),
      std::move(offsetInsideTable),
      "points into its offset table");

  std::string invalidComplexOffset(1, '\0');
  appendInt32(invalidComplexOffset, 1);
  invalidComplexOffset.push_back('\0');
  appendInt32(invalidComplexOffset, sizeof(int32_t));
  appendInt32(invalidComplexOffset, sizeof(int32_t) + 1);
  expectInvalid(
      ROW({ARRAY(ARRAY(BIGINT()))}),
      std::move(invalidComplexOffset),
      "exceeds payload size");

  std::string decreasingComplexOffsets(1, '\0');
  appendInt32(decreasingComplexOffsets, 2);
  decreasingComplexOffsets.push_back('\0');
  appendInt32(decreasingComplexOffsets, 12);
  appendInt32(decreasingComplexOffsets, 12);
  appendInt32(decreasingComplexOffsets, 8);
  decreasingComplexOffsets.append(sizeof(int32_t), '\0');
  expectInvalid(
      ROW({ARRAY(ARRAY(BIGINT()))}),
      std::move(decreasingComplexOffsets),
      "precedes offset 12");

  // Each complex element is bounded by the next element offset. The first
  // nested array below declares one BIGINT, but contains only its null byte;
  // it must not consume bytes belonging to the second nested array.
  std::string overlappingComplexElements(1, '\0');
  appendInt32(overlappingComplexElements, 2);
  overlappingComplexElements.push_back('\0');
  appendInt32(overlappingComplexElements, 26);
  appendInt32(overlappingComplexElements, 8);
  appendInt32(overlappingComplexElements, 13);
  appendInt32(overlappingComplexElements, 1);
  overlappingComplexElements.push_back('\0');
  appendInt32(overlappingComplexElements, 1);
  overlappingComplexElements.push_back('\0');
  overlappingComplexElements.append(sizeof(int64_t), '\0');
  expectInvalid(
      ROW({ARRAY(ARRAY(BIGINT()))}),
      std::move(overlappingComplexElements),
      "array payload");

  // Map key and value arrays must have matching cardinalities.
  std::string mismatchedMap(1, '\0');
  appendInt32(mismatchedMap, 0);
  appendInt32(mismatchedMap, 1);
  mismatchedMap.push_back('\0');
  mismatchedMap.append(sizeof(int64_t), '\0');
  expectInvalid(
      ROW({MAP(BIGINT(), BIGINT())}),
      std::move(mismatchedMap),
      "map has 0 keys but 1 values");
}

TEST_F(CompactRowTest, acceptsTrailingBytes) {
  std::string serialized(1, '\0');
  const int64_t value = 42;
  serialized.append(reinterpret_cast<const char*>(&value), sizeof(value));
  serialized.append("trailing bytes");

  std::vector<std::string_view> rows{serialized};
  auto actual = CompactRow::deserialize(rows, ROW({BIGINT()}), pool());
  auto expected = makeRowVector({makeFlatVector<int64_t>({value})});
  assertEqualVectors(expected, actual);
}

TEST_F(CompactRowTest, fuzz) {
  auto rowType = ROW({
      ROW({BIGINT(), VARCHAR(), DOUBLE()}),
      MAP(VARCHAR(), ROW({ARRAY(BIGINT()), ARRAY(VARCHAR()), REAL()})),
      ARRAY(ROW({BIGINT(), DOUBLE()})),
      ARRAY(MAP(BIGINT(), DOUBLE())),
      BIGINT(),
      ARRAY(MAP(BIGINT(), VARCHAR())),
      ARRAY(MAP(VARCHAR(), REAL())),
      MAP(BIGINT(), ARRAY(BIGINT())),
      BIGINT(),
      ARRAY(BIGINT()),
      DOUBLE(),
      MAP(VARCHAR(), VARCHAR()),
      VARCHAR(),
      ARRAY(ARRAY(BIGINT())),
      BIGINT(),
      ARRAY(ARRAY(VARCHAR())),
  });

  VectorFuzzer::Options opts;
  opts.vectorSize = 100;
  opts.containerLength = 5;
  opts.nullRatio = 0.1;
  opts.dictionaryHasNulls = false;
  opts.stringVariableLength = true;
  opts.stringLength = 20;
  opts.containerVariableLength = true;
  opts.complexElementsMaxSize = 1'000;

  // Spark uses microseconds to store timestamp
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;

  VectorFuzzer fuzzer(opts, pool_.get());

  const auto iterations = 200;
  for (size_t i = 0; i < iterations; ++i) {
    auto seed = folly::Random::rand32();

    LOG(INFO) << i << ": seed: " << seed;
    SCOPED_TRACE(fmt::format("seed: {}", seed));

    fuzzer.reSeed(seed);
    auto data = fuzzInputRow(fuzzer, rowType);

    testRoundTrip(data);

    if (Test::HasFailure()) {
      break;
    }
  }
}

} // namespace
} // namespace bytedance::bolt::row
