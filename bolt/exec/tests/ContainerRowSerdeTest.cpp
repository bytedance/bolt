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

#include "bolt/exec/ContainerRowSerde.h"

#include <gtest/gtest.h>

#include <span>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/BmContainerRowSerde.h"
#include "bolt/vector/VariantVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::exec {

namespace {

void appendSerializedInt32(std::string& bytes, int32_t value) {
  bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

class ContainerRowSerdeTest : public testing::Test,
                              public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  // Writes all rows together and returns a position at the start of this
  // combined write.
  HashStringAllocator::Position serialize(
      const VectorPtr& data,
      bool isKey = true) {
    ByteOutputStream out(&allocator_);
    auto position = allocator_.newWrite(out);
    const ContainerRowSerdeOptions options{.isKey = isKey};
    for (auto i = 0; i < data->size(); ++i) {
      ContainerRowSerde::serialize(*data, i, out, options);
    }
    allocator_.finishWrite(out, 0);
    return position;
  }

  // Writes each row individually and returns positions for individual rows.
  std::vector<HashStringAllocator::Position> serializeWithPositions(
      const VectorPtr& data,
      bool isKey = true) {
    std::vector<HashStringAllocator::Position> positions;
    auto size = data->size();
    positions.reserve(size);

    const ContainerRowSerdeOptions options{.isKey = isKey};
    for (auto i = 0; i < size; ++i) {
      ByteOutputStream out(&allocator_);
      auto position = allocator_.newWrite(out);
      ContainerRowSerde::serialize(*data, i, out, options);
      allocator_.finishWrite(out, 0);
      positions.emplace_back(position);
    }

    return positions;
  }

  VectorPtr deserialize(
      HashStringAllocator::Position position,
      const TypePtr& type,
      vector_size_t numRows) {
    auto data = BaseVector::create(type, numRows, pool());
    // Set all rows in data to NULL to verify that deserialize can clear nulls
    // correctly.
    for (auto i = 0; i < numRows; ++i) {
      data->setNull(i, true);
    }

    auto in = HashStringAllocator::prepareRead(position.header);
    for (auto i = 0; i < numRows; ++i) {
      ContainerRowSerde::deserialize(*in, i, data.get());
    }
    return data;
  }

  void testRoundTrip(const VectorPtr& data) {
    auto position = serialize(data);
    auto copy = deserialize(position, data->type(), data->size());
    test::assertEqualVectors(data, copy);

    allocator_.clear();
  }

  void assertExactSerialization(const VectorPtr& data, bool isKey = true) {
    const ContainerRowSerdeOptions options{.isKey = isKey};
    for (auto i = 0; i < data->size(); ++i) {
      ASSERT_FALSE(data->isNullAt(i));

      ByteOutputStream out(&allocator_);
      auto position = allocator_.newWrite(out);
      ContainerRowSerde::serialize(*data, i, out, options);
      const auto streamSize = out.size();
      allocator_.finishWrite(out, 0);

      const auto exactSize =
          BmContainerRowSerde::serializedSize(*data, i, options);
      ASSERT_EQ(streamSize, exactSize) << "at " << i;
      std::vector<char> expected(exactSize);
      auto in = HashStringAllocator::prepareRead(position.header);
      in->readBytes(
          reinterpret_cast<uint8_t*>(expected.data()), expected.size());

      std::vector<char> actual(exactSize);
      EXPECT_EQ(
          exactSize,
          BmContainerRowSerde::serializeInto(
              *data, i, std::span<char>(actual), options));
      EXPECT_EQ(expected, actual) << "at " << i;
      allocator_.clear();
    }
  }

  // Returns the exact serialized bytes of data[index] via serializeInto().
  std::vector<char> serializeIntoBytes(
      const VectorPtr& data,
      vector_size_t index,
      bool isKey = true) {
    const ContainerRowSerdeOptions options{.isKey = isKey};
    const auto size =
        BmContainerRowSerde::serializedSize(*data, index, options);
    std::vector<char> bytes(size);
    BmContainerRowSerde::serializeInto(
        *data, index, std::span<char>(bytes), options);
    return bytes;
  }

  void assertBatchSerializedSizes(const VectorPtr& data, bool isKey = true) {
    const ContainerRowSerdeOptions options{.isKey = isKey};
    DecodedVector decoded(*data);
    std::vector<uint64_t> sizes(data->size());
    BmContainerRowSerde::serializedSizes(
        decoded, 0, data->size(), options, std::span<uint64_t>(sizes));

    for (auto i = 0; i < data->size(); ++i) {
      ASSERT_FALSE(data->isNullAt(i));
      ByteOutputStream out(&allocator_);
      allocator_.newWrite(out);
      ContainerRowSerde::serialize(*data, i, out, options);
      EXPECT_EQ(out.size(), sizes[i]) << "at " << i;
      allocator_.finishWrite(out, 0);
      allocator_.clear();
    }
  }

  void assertNotEqualVectors(const VectorPtr& left, const VectorPtr& right) {
    ASSERT_NE(left->size(), 0);
    for (auto i = 0; i < left->size(); ++i) {
      bool equal = true;
      if (left->isNullAt(i) || right->isNullAt(i)) {
        equal = left->isNullAt(i) && right->isNullAt(i);
      } else {
        // For simplicity, compare as strings for test purposes
        equal = left->toString(i) == right->toString(i);
      }
      if (!equal) {
        return; // Found inequality
      }
    }
    FAIL() << "Vectors are unexpectedly equal";
  }

  // If the mode is NullAsIndeterminate with equalsOnly is false, and expected
  // is kIndeterminate, then the test ensures that an exception is thrown with
  // the message "Ordering nulls is not supported".
  void testCompareWithNulls(
      const DecodedVector& decodedVector,
      const std::vector<HashStringAllocator::Position>& positions,
      const std::vector<std::optional<int32_t>>& expected,
      bool equalsOnly,
      CompareFlags::NullHandlingMode mode) {
    CompareFlags compareFlags{
        true, // nullsFirst
        true, // ascending
        equalsOnly,
        mode};

    for (auto i = 0; i < expected.size(); ++i) {
      auto stream = HashStringAllocator::prepareRead(positions.at(i).header);
      if (expected.at(i) == kIndeterminate &&
          mode == CompareFlags::NullHandlingMode::kNullAsIndeterminate &&
          !equalsOnly) {
        BOLT_ASSERT_THROW(
            ContainerRowSerde::compareWithNulls(
                *stream, decodedVector, i, compareFlags),
            "Ordering nulls is not supported");
      } else {
        ASSERT_EQ(
            expected.at(i),
            ContainerRowSerde::compareWithNulls(
                *stream, decodedVector, i, compareFlags));
      }
    }
  }

  // If the mode is NullAsIndeterminate with equalsOnly is false, and expected
  // is kIndeterminate, then the test ensures that an exception is thrown with
  // the message "Ordering nulls is not supported".
  void testCompareByteStreamWithNulls(
      const std::vector<HashStringAllocator::Position>& leftPositions,
      const std::vector<HashStringAllocator::Position>& rightPositions,
      const std::vector<std::optional<int32_t>>& expected,
      const TypePtr& type,
      bool equalsOnly,
      CompareFlags::NullHandlingMode mode) {
    CompareFlags compareFlags{
        true, // nullsFirst
        true, // ascending
        equalsOnly,
        mode};

    for (auto i = 0; i < expected.size(); ++i) {
      auto leftStream =
          HashStringAllocator::prepareRead(leftPositions.at(i).header);
      auto rightStream =
          HashStringAllocator::prepareRead(rightPositions.at(i).header);
      if (expected.at(i) == kIndeterminate &&
          mode == CompareFlags::NullHandlingMode::kNullAsIndeterminate &&
          !equalsOnly) {
        BOLT_ASSERT_THROW(
            ContainerRowSerde::compareWithNulls(
                *leftStream, *rightStream, type.get(), compareFlags),
            "Ordering nulls is not supported");
      } else {
        ASSERT_EQ(
            expected.at(i),
            ContainerRowSerde::compareWithNulls(
                *leftStream, *rightStream, type.get(), compareFlags));
      }
    }
  }

  void testCompare(const VectorPtr& vector) {
    auto positions = serializeWithPositions(vector);

    CompareFlags compareFlags =
        CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

    DecodedVector decodedVector(*vector);

    for (auto i = 0; i < positions.size(); ++i) {
      auto stream = HashStringAllocator::prepareRead(positions.at(i).header);
      ASSERT_EQ(
          0,
          ContainerRowSerde::compare(*stream, decodedVector, i, compareFlags))
          << "at " << i << ": " << vector->toString(i);
    }
  }

  HashStringAllocator allocator_{pool()};
};

TEST_F(ContainerRowSerdeTest, variantCompare) {
  auto data = VariantVector::create(pool(), VARIANT(), 2);
  auto* values =
      data->valueChildVector()->asUnchecked<FlatVector<StringView>>();
  auto* metadata =
      data->metadataChildVector()->asUnchecked<FlatVector<StringView>>();
  const std::string longValue(48, 'v');
  const std::string longMetadata(36, 'm');

  values->set(0, StringView("short"));
  metadata->set(0, StringView("meta"));
  values->set(1, StringView(longValue));
  metadata->set(1, StringView(longMetadata));

  testCompare(data);
  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, variantHashWithSegmentedInput) {
  const std::string value(64, 'v');
  const std::string metadata(40, 'm');

  std::string serialized;
  appendSerializedInt32(serialized, value.size());
  serialized.append(value);
  appendSerializedInt32(serialized, metadata.size());
  serialized.append(metadata);

  const auto split = sizeof(int32_t) + 10;
  ASSERT_LT(split, serialized.size());

  ByteInputStream stream({
      ByteRange{
          reinterpret_cast<uint8_t*>(serialized.data()),
          static_cast<int32_t>(split),
          0,
      },
      ByteRange{
          reinterpret_cast<uint8_t*>(serialized.data() + split),
          static_cast<int32_t>(serialized.size() - split),
          0,
      },
  });

  EXPECT_EQ(
      std::hash<VariantValue>{}({StringView(value), StringView(metadata)}),
      ContainerRowSerde::hash(stream, VARIANT().get()));
}

TEST_F(ContainerRowSerdeTest, bigint) {
  auto data = makeFlatVector<int64_t>({1, 2, 3, 4, 5});

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, exactSerializationMatchesStreamEncoding) {
  assertExactSerialization(makeFlatVector<int64_t>({1, -2, 3}));
  auto arrays = makeNullableArrayVector<std::string>({
      {{{std::nullopt, "short", std::string(40, 'x')}}},
      {{std::vector<std::optional<std::string>>{}}},
      {{{"last"}}},
  });
  assertExactSerialization(arrays);
  assertExactSerialization(wrapInDictionary(makeIndices({2, 0, 1}), arrays));
  assertExactSerialization(BaseVector::wrapInConstant(3, 0, arrays));
  assertExactSerialization(wrapInLazyDictionary(arrays));
  assertExactSerialization(makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt}),
      makeNullableArrayVector<int64_t>(
          std::vector<std::vector<std::optional<int64_t>>>{
              {1, std::nullopt}, {}}),
  }));

  auto maps = makeMapVector<int64_t, int64_t>({
      {{2, 20}, {1, 10}},
      {},
  });
  assertExactSerialization(maps, true);
  assertExactSerialization(maps, false);

  auto largeArray = makeArrayVector<std::string>({{std::string(1 << 20, 'z')}});
  assertExactSerialization(largeArray);
}

// Exercises the flat fixed-width batched-run fast path in serializeDirectArray
// (used by serializeInto/trySerializeInto): full non-null runs, interior nulls
// splitting runs, int128 alignment, and the dictionary-element fallback. All
// must stay byte-identical to stream serialization.
TEST_F(ContainerRowSerdeTest, exactSerializationBatchedFixedWidthArrays) {
  // Contiguous non-null fixed-width elements (single batched run).
  assertExactSerialization(makeArrayVector<int64_t>({{1, 2, 3, 4, 5}, {6, 7}}));
  assertExactSerialization(makeArrayVector<int32_t>({{1, 2, 3}, {}, {4}}));
  assertExactSerialization(makeArrayVector<double>({{1.5, 2.5, 3.5}}));

  // Interior nulls split the copy into multiple runs.
  assertExactSerialization(makeNullableArrayVector<int64_t>(
      std::vector<std::vector<std::optional<int64_t>>>{
          {1, std::nullopt, 3, std::nullopt, 5},
          {std::nullopt, std::nullopt},
          {7, 8}}));

  // int128 goes through the int8 view to avoid a misaligned 16-byte load.
  assertExactSerialization(makeArrayVector<int128_t>({{1, 2, 3}, {4}}));
  assertExactSerialization(makeNullableArrayVector<int128_t>(
      std::vector<std::vector<std::optional<int128_t>>>{{1, std::nullopt, 3}}));

  // Dictionary-wrapped elements are not flat: must fall back to per-element.
  auto dictionaryElements = wrapInDictionary(
      makeIndices({2, 1, 0, 2}),
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3}));
  assertExactSerialization(makeArrayVector({0, 2}, dictionaryElements));
}

TEST_F(ContainerRowSerdeTest, BatchSerializedSizesMatchStreamEncoding) {
  auto variants = VariantVector::create(pool(), VARIANT(), 2);
  variants->valueChildVector()->asUnchecked<FlatVector<StringView>>()->set(
      0, StringView("value"));
  variants->metadataChildVector()->asUnchecked<FlatVector<StringView>>()->set(
      0, StringView("meta"));
  const std::string longValue(48, 'v');
  const std::string longMetadata(36, 'm');
  variants->valueChildVector()->asUnchecked<FlatVector<StringView>>()->set(
      1, StringView(longValue));
  variants->metadataChildVector()->asUnchecked<FlatVector<StringView>>()->set(
      1, StringView(longMetadata));
  assertExactSerialization(variants);
  assertBatchSerializedSizes(variants);

  assertBatchSerializedSizes(makeFlatVector<bool>({true, false}));
  assertBatchSerializedSizes(makeFlatVector<int8_t>({1, -2}));
  assertBatchSerializedSizes(makeFlatVector<int16_t>({1, -2}));
  assertBatchSerializedSizes(makeFlatVector<int32_t>({1, -2}));
  assertBatchSerializedSizes(makeFlatVector<int64_t>({1, -2}));
  assertBatchSerializedSizes(makeFlatVector<float>({1.5, -2.5}));
  assertBatchSerializedSizes(makeFlatVector<double>({1.5, -2.5}));
  assertBatchSerializedSizes(
      makeFlatVector<Timestamp>({Timestamp(1, 2), Timestamp(3, 4)}));
  assertBatchSerializedSizes(makeFlatVector<int128_t>({1, -2}));

  auto arrays = makeNullableArrayVector<std::string>({
      {{{std::nullopt, "short", std::string(40, 'x')}}},
      {{std::vector<std::optional<std::string>>{}}},
      {{{"last"}}},
  });
  assertBatchSerializedSizes(arrays);
  assertBatchSerializedSizes(wrapInDictionary(makeIndices({2, 0, 1}), arrays));
  assertBatchSerializedSizes(BaseVector::wrapInConstant(3, 0, arrays));
  assertBatchSerializedSizes(wrapInLazyDictionary(arrays));

  auto dictionaryElements = wrapInDictionary(
      makeIndices({2, 1, 0, 2}),
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3}));
  assertBatchSerializedSizes(makeArrayVector({0, 2}, dictionaryElements));

  auto maps = makeMapVector<int64_t, std::string>({
      {{2, "two"}, {1, std::string(32, 'a')}},
      {{4, "four"}},
      {},
  });
  assertBatchSerializedSizes(maps, true);
  assertBatchSerializedSizes(maps, false);
  assertBatchSerializedSizes(makeRowVector({maps, arrays}));

  using MapEntry = std::pair<int64_t, std::optional<std::string>>;
  const std::vector<MapEntry> firstMap{{2, "two"}, {1, "one"}};
  const std::vector<MapEntry> secondMap{{3, "three"}};
  const std::vector<std::vector<std::vector<MapEntry>>> arrayOfMaps{
      {firstMap, secondMap}, {secondMap}};
  assertBatchSerializedSizes(
      makeArrayOfMapVector<int64_t, std::string>(arrayOfMaps));

  auto rows = makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3}),
      arrays,
  });
  assertBatchSerializedSizes(rows);
}

TEST_F(
    ContainerRowSerdeTest,
    BatchSerializedSizesHandleNullsAndValidateOutput) {
  auto arrays = makeNullableArrayVector<int64_t>(
      {{{1, 2}}, std::nullopt, {{3, std::nullopt}}});
  DecodedVector decoded(*arrays);
  const ContainerRowSerdeOptions options;
  std::vector<uint64_t> sizes(arrays->size());

  BmContainerRowSerde::serializedSizes(
      decoded, 0, arrays->size(), options, std::span<uint64_t>(sizes));

  EXPECT_GT(sizes[0], 0);
  EXPECT_EQ(sizes[1], 0);
  EXPECT_GT(sizes[2], 0);

  std::vector<uint64_t> suffixSizes(2);
  BmContainerRowSerde::serializedSizes(
      decoded, 1, 2, options, std::span<uint64_t>(suffixSizes));
  EXPECT_EQ(suffixSizes[0], 0);
  EXPECT_EQ(suffixSizes[1], sizes[2]);

  const auto dictionaryNulls =
      makeNulls(3, [](vector_size_t row) { return row == 1; });
  auto nullableDictionary = BaseVector::wrapInDictionary(
      dictionaryNulls, makeIndices({0, 1, 2}), 3, arrays);
  DecodedVector nullableDecoded(*nullableDictionary);
  BmContainerRowSerde::serializedSizes(
      nullableDecoded, 0, 3, options, std::span<uint64_t>(sizes));
  EXPECT_GT(sizes[0], 0);
  EXPECT_EQ(sizes[1], 0);
  EXPECT_GT(sizes[2], 0);
  EXPECT_THROW(
      BmContainerRowSerde::serializedSizes(
          decoded,
          0,
          arrays->size(),
          options,
          std::span<uint64_t>(sizes.data(), sizes.size() - 1)),
      BoltException);
}

TEST_F(ContainerRowSerdeTest, serializeIntoRequiresExactCapacity) {
  auto data = makeFlatVector<int64_t>({123});
  const ContainerRowSerdeOptions options;
  const auto size = BmContainerRowSerde::serializedSize(*data, 0, options);
  ASSERT_EQ(sizeof(int64_t), size);

  std::vector<char> tooSmall(size - 1);
  EXPECT_THROW(
      BmContainerRowSerde::serializeInto(
          *data, 0, std::span<char>(tooSmall), options),
      BoltException);

  std::vector<char> tooLarge(size + 1);
  EXPECT_THROW(
      BmContainerRowSerde::serializeInto(
          *data, 0, std::span<char>(tooLarge), options),
      BoltException);

  auto nullable = makeNullableFlatVector<int64_t>({std::nullopt});
  EXPECT_THROW(
      BmContainerRowSerde::serializedSize(*nullable, 0, options),
      BoltException);
  EXPECT_THROW(
      BmContainerRowSerde::serializeInto(
          *nullable, 0, std::span<char>(tooLarge), options),
      BoltException);
}

// ---------------------------------------------------------------------------
// Speculative write tests for detail::trySerializeInto(source, index,
// available, options) -> {size, complete}. It always reports the exact
// serialized size, copies into 'available' only while the value fits, and
// signals capacity exhaustion via complete == false instead of throwing.
// Complex-type only (ARRAY / MAP / ROW); fixed-width and string paths are
// unchanged.
// ---------------------------------------------------------------------------

// Span exactly the serialized size: complete, exact size, byte-identical.
TEST_F(ContainerRowSerdeTest, TrySerializeExactFit) {
  auto arrays = makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}});
  const ContainerRowSerdeOptions options;
  for (auto i = 0; i < arrays->size(); ++i) {
    const auto expected = serializeIntoBytes(arrays, i);
    std::vector<char> actual(expected.size());
    const auto result =
        detail::trySerializeInto(*arrays, i, std::span<char>(actual), options);
    EXPECT_TRUE(result.complete) << "at " << i;
    EXPECT_EQ(result.size, expected.size()) << "at " << i;
    EXPECT_EQ(expected, actual) << "at " << i;
  }
}

// Span one byte short: incomplete, but size is still the exact full size.
TEST_F(ContainerRowSerdeTest, TrySerializeOneByteShort) {
  auto arrays = makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}});
  const ContainerRowSerdeOptions options;
  for (auto i = 0; i < arrays->size(); ++i) {
    const auto full = BmContainerRowSerde::serializedSize(*arrays, i, options);
    ASSERT_GT(full, 0);
    std::vector<char> actual(full - 1);
    const auto result =
        detail::trySerializeInto(*arrays, i, std::span<char>(actual), options);
    EXPECT_FALSE(result.complete) << "at " << i;
    EXPECT_EQ(result.size, full) << "at " << i;
  }
}

// Empty span (as when no heap block exists yet): incomplete, exact size.
TEST_F(ContainerRowSerdeTest, TrySerializeEmptySpan) {
  auto arrays = makeArrayVector<int64_t>({{1, 2, 3}});
  const ContainerRowSerdeOptions options;
  const auto full = BmContainerRowSerde::serializedSize(*arrays, 0, options);
  const auto result =
      detail::trySerializeInto(*arrays, 0, std::span<char>(), options);
  EXPECT_FALSE(result.complete);
  EXPECT_EQ(result.size, full);
}

// Inline-sized value (<= kInlineSize) into a large-enough span: complete.
// Empty ARRAY (4 bytes), empty MAP (8 bytes), all-null ROW (<= 12 bytes).
TEST_F(ContainerRowSerdeTest, TrySerializeInlineSizedFits) {
  const ContainerRowSerdeOptions options;

  auto emptyArray = makeArrayVector<int64_t>({{}});
  auto emptyMap = makeMapVector<int64_t, int64_t>({{}});
  auto allNullRow = makeRowVector({
      makeNullableFlatVector<int64_t>({std::nullopt}),
      makeNullableFlatVector<int64_t>({std::nullopt}),
  });

  for (const auto& data :
       std::vector<VectorPtr>{emptyArray, emptyMap, allNullRow}) {
    const auto expected = serializeIntoBytes(data, 0);
    EXPECT_LE(expected.size(), StringView::kInlineSize);
    std::vector<char> actual(expected.size());
    const auto result =
        detail::trySerializeInto(*data, 0, std::span<char>(actual), options);
    EXPECT_TRUE(result.complete);
    EXPECT_LE(result.size, StringView::kInlineSize);
    EXPECT_EQ(result.size, expected.size());
    EXPECT_EQ(expected, actual);
  }
}

// Inline-sized value with a span smaller than the value: incomplete, but the
// reported size is still exact and inline-sized.
TEST_F(ContainerRowSerdeTest, TrySerializeInlineSizedShortSpan) {
  const ContainerRowSerdeOptions options;
  auto emptyMap = makeMapVector<int64_t, int64_t>({{}});
  const auto full = BmContainerRowSerde::serializedSize(*emptyMap, 0, options);
  ASSERT_LE(full, StringView::kInlineSize);
  ASSERT_GT(full, 1);
  std::vector<char> actual(full - 1);
  const auto result =
      detail::trySerializeInto(*emptyMap, 0, std::span<char>(actual), options);
  EXPECT_FALSE(result.complete);
  EXPECT_EQ(result.size, full);
  EXPECT_LE(result.size, StringView::kInlineSize);
}

// Nested MAP and ROW<MAP, ARRAY>: exact fit is byte-identical, one byte short
// is incomplete with exact size.
TEST_F(ContainerRowSerdeTest, TrySerializeNestedComplex) {
  const ContainerRowSerdeOptions options;

  using MapEntry = std::pair<int64_t, std::optional<std::string>>;
  const std::vector<MapEntry> firstMap{{2, "two"}, {1, "one"}};
  const std::vector<MapEntry> secondMap{{3, "three"}};
  const std::vector<std::vector<std::vector<MapEntry>>> arrayOfMaps{
      {firstMap, secondMap}, {secondMap}};
  VectorPtr nestedArrayOfMaps =
      makeArrayOfMapVector<int64_t, std::string>(arrayOfMaps);

  auto maps = makeMapVector<int64_t, std::string>({
      {{2, "two"}, {1, std::string(32, 'a')}},
      {{4, "four"}},
  });
  auto arrays = makeArrayVector<int64_t>({{1, 2, 3}, {4}});
  VectorPtr rowOfMapArray = makeRowVector({maps, arrays});

  std::vector<VectorPtr> inputs{nestedArrayOfMaps, rowOfMapArray};
  for (const auto& data : inputs) {
    for (auto i = 0; i < data->size(); ++i) {
      const auto expected = serializeIntoBytes(data, i);
      std::vector<char> fit(expected.size());
      const auto fitResult =
          detail::trySerializeInto(*data, i, std::span<char>(fit), options);
      EXPECT_TRUE(fitResult.complete) << "at " << i;
      EXPECT_EQ(fitResult.size, expected.size()) << "at " << i;
      EXPECT_EQ(expected, fit) << "at " << i;

      std::vector<char> shortBuf(expected.size() - 1);
      const auto shortResult = detail::trySerializeInto(
          *data, i, std::span<char>(shortBuf), options);
      EXPECT_FALSE(shortResult.complete) << "at " << i;
      EXPECT_EQ(shortResult.size, expected.size()) << "at " << i;
    }
  }
}

// Unordered-key MAP stays canonical: output matches serializeInto(), and two
// logically-equal maps with different physical order serialize identically.
TEST_F(ContainerRowSerdeTest, TrySerializeMapCanonicalOrder) {
  const ContainerRowSerdeOptions options{.isKey = true};

  auto unordered =
      makeMapVector<int64_t, int64_t>({{{3, 30}, {1, 10}, {2, 20}}});
  auto ordered = makeMapVector<int64_t, int64_t>({{{1, 10}, {2, 20}, {3, 30}}});

  const auto expected = serializeIntoBytes(unordered, 0);
  std::vector<char> actual(expected.size());
  const auto result =
      detail::trySerializeInto(*unordered, 0, std::span<char>(actual), options);
  EXPECT_TRUE(result.complete);
  EXPECT_EQ(expected, actual);

  // Logically-equal maps with different physical order serialize identically.
  EXPECT_EQ(serializeIntoBytes(unordered, 0), serializeIntoBytes(ordered, 0));
}

// Dictionary / constant / lazy wrapped complex inputs behave like the flat
// cases: exact fit is byte-identical, one byte short is incomplete.
TEST_F(ContainerRowSerdeTest, TrySerializeWrappedEncodings) {
  const ContainerRowSerdeOptions options;
  auto arrays = makeNullableArrayVector<std::string>({
      {{{std::nullopt, "short", std::string(40, 'x')}}},
      {{{"last"}}},
  });

  std::vector<VectorPtr> wrapped{
      wrapInDictionary(makeIndices({1, 0}), arrays),
      BaseVector::wrapInConstant(2, 0, arrays),
      wrapInLazyDictionary(arrays),
  };
  for (const auto& data : wrapped) {
    for (auto i = 0; i < data->size(); ++i) {
      const auto expected = serializeIntoBytes(data, i);
      std::vector<char> fit(expected.size());
      const auto fitResult =
          detail::trySerializeInto(*data, i, std::span<char>(fit), options);
      EXPECT_TRUE(fitResult.complete) << "at " << i;
      EXPECT_EQ(expected, fit) << "at " << i;

      std::vector<char> shortBuf(expected.empty() ? 0 : expected.size() - 1);
      const auto shortResult = detail::trySerializeInto(
          *data, i, std::span<char>(shortBuf), options);
      EXPECT_EQ(shortResult.size, expected.size()) << "at " << i;
      if (!expected.empty()) {
        EXPECT_FALSE(shortResult.complete) << "at " << i;
      }
    }
  }
}

// The reported size equals serializedSize() for every input, whether the span
// fits or is empty.
TEST_F(ContainerRowSerdeTest, TrySerializeSizeParity) {
  const ContainerRowSerdeOptions options;
  VectorPtr maps = makeMapVector<int64_t, std::string>({
      {{2, "two"}, {1, std::string(32, 'a')}},
      {},
  });
  VectorPtr arrays = makeNullableArrayVector<int64_t>(
      std::vector<std::vector<std::optional<int64_t>>>{
          {1, std::nullopt, 3}, {}});
  VectorPtr rows = makeRowVector({maps, arrays});

  std::vector<VectorPtr> inputs{maps, arrays, rows};
  for (const auto& data : inputs) {
    for (auto i = 0; i < data->size(); ++i) {
      const auto expected =
          BmContainerRowSerde::serializedSize(*data, i, options);
      // Both a fitting and an empty span report the same exact size.
      std::vector<char> buf(expected);
      EXPECT_EQ(
          detail::trySerializeInto(*data, i, std::span<char>(buf), options)
              .size,
          expected)
          << "at " << i;
      EXPECT_EQ(
          detail::trySerializeInto(*data, i, std::span<char>(), options).size,
          expected)
          << "empty at " << i;
    }
  }
}

TEST_F(ContainerRowSerdeTest, map) {
  auto data = makeMapVector<int64_t, int64_t>({
      {{2, 20}, {3, 30}, {1, 10}},
      {{4, 40}},
  });
  testRoundTrip(data);

  // isKey=false: preserve order
  {
    auto position = serialize(data, false);
    auto deserialized = deserialize(position, data->type(), data->size());
    test::assertEqualVectors(
        data->mapKeys(), deserialized->as<MapVector>()->mapKeys());
  }
  // isKey=true: sorted order, thus different than input
  {
    auto position = serialize(data, true);
    auto deserialized = deserialize(position, data->type(), data->size());
    assertNotEqualVectors(
        data->mapKeys(), deserialized->as<MapVector>()->mapKeys());
  }
}

TEST_F(ContainerRowSerdeTest, string) {
  auto data =
      makeFlatVector<std::string>({"a", "Abc", "Long test sentence.", "", "d"});

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, arrayOfBigint) {
  auto data = makeArrayVector<int64_t>({
      {1, 2, 3},
      {4, 5},
      {6},
      {},
  });

  testRoundTrip(data);

  data = makeNullableArrayVector<int64_t>({
      {{{1, std::nullopt, 2, 3}}},
      {{{std::nullopt, 4, 5}}},
      {{{6, std::nullopt}}},
      {{std::vector<std::optional<int64_t>>({})}},
  });

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, arrayOfString) {
  auto data = makeArrayVector<std::string>({
      {"a", "b", "Longer string ...."},
      {"c", "Abc", "Mountains and rivers"},
      {},
      {"Oceans and skies"},
  });

  testRoundTrip(data);

  data = makeNullableArrayVector<std::string>({
      {{{std::nullopt,
         std::nullopt,
         "a",
         std::nullopt,
         "b",
         "Longer string ...."}}},
      {{{"c", "Abc", std::nullopt, "Mountains and rivers"}}},
      {{std::vector<std::optional<std::string>>({})}},
      {{{"Oceans and skies"}}},
  });

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, nested) {
  auto data = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<std::string>({"a", "", "Long test sentence ......"}),
       makeNullableArrayVector<std::string>({{"a", "b", "c"}, {}, {"d"}})});

  testRoundTrip(data);

  using OptString = std::optional<std::string>;
  using OptStringVec = std::vector<OptString>;
  using OptStringVecOpt = std::optional<OptStringVec>;
  using OptStringVecVecOpt = std::vector<OptStringVecOpt>;

  auto nestedArray = makeNullableNestedArrayVector<std::string>(
      {OptStringVecVecOpt{
           OptStringVecOpt{OptStringVec{OptString{"1"}, OptString{"2"}}},
           OptStringVecOpt{OptStringVec{OptString{"3"}, OptString{"4"}}}},
       OptStringVecVecOpt{},
       OptStringVecVecOpt{
           OptStringVecOpt{std::nullopt}, OptStringVecOpt{OptStringVec{}}}});
  testRoundTrip(nestedArray);

  std::vector<std::pair<std::string, std::optional<int64_t>>> map{
      {"a", {1}}, {"b", {2}}, {"c", {3}}, {"d", {4}}};
  nestedArray = makeArrayOfMapVector<std::string, int64_t>(
      {{map, std::nullopt}, {std::nullopt}});

  testRoundTrip(nestedArray);
}

TEST_F(ContainerRowSerdeTest, compareNullsInArrayVector) {
  auto data = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 5},
      {1, 3, 5},
      {1, 2, 3, 4},
      {1, 2, std::nullopt, 4},
      {1, std::nullopt, 5},
  });
  auto positions = serializeWithPositions(data);
  auto arrayVector = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 3},
      {1, 5},
      {std::nullopt, 1},
      {1, 2, std::nullopt, 4},
      {1, 5},
  });
  DecodedVector decodedVector(*arrayVector);

  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {1}, {-1}, std::nullopt, std::nullopt, std::nullopt},
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {1}, {1}, {1}, std::nullopt, {1}},
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {1}, {-1}, {1}, {0}, {-1}},
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInMapVector) {
  auto data = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {4, 30}, {2, 3}}},
      {{{2, 20}}},
      {{{3, 50}}},
      {{{4, std::nullopt}}},
  });
  auto positions = serializeWithPositions(data);
  auto mapVector = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {3, 20}}},
      {{{2, 20}}},
      {{{3, 40}}},
      {{{4, std::nullopt}}},
  });
  DecodedVector decodedVector(*mapVector);

  testCompareWithNulls(
      decodedVector,
      positions,
      {{-1}, {0}, {1}, std::nullopt},
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{1}, {0}, {1}, std::nullopt},
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{1}, {0}, {1}, {0}},
      true,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInRowVector) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto positions = serializeWithPositions(data);
  auto someNulls = makeNullableFlatVector<int32_t>({1, 3, 2, std::nullopt});
  auto rowVector = makeRowVector({someNulls});
  DecodedVector decodedVector(*rowVector);

  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {-1}, {1}, std::nullopt},
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {-1}, {1}, {1}},
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInArrayByteStream) {
  auto left = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 5},
      {1, 3, 5},
      {1, 2, 3, 4},
      {1, 2, std::nullopt, 4},
      {1, std::nullopt, 5},
  });
  auto leftPositions = serializeWithPositions(left);

  auto right = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 3},
      {1, 5},
      {std::nullopt, 1},
      {1, 2, std::nullopt, 4},
      {1, 5},
  });
  auto rightPositions = serializeWithPositions(right);

  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {1}, {-1}, std::nullopt, std::nullopt, std::nullopt},
      ARRAY(BIGINT()),
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {1}, {1}, {1}, std::nullopt, {1}},
      ARRAY(BIGINT()),
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {1}, {-1}, {1}, {0}, {-1}},
      ARRAY(BIGINT()),
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInRowByteStream) {
  auto left = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3, 4}),
       makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto leftPositions = serializeWithPositions(left);
  auto right = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 3, 2, std::nullopt}),
       makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto rightPositions = serializeWithPositions(right);

  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {-1}, {1}, std::nullopt},
      ROW({INTEGER(), INTEGER()}),
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {-1}, {1}, {1}},
      ROW({INTEGER(), INTEGER()}),
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInMapByteStream) {
  auto left = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {4, 30}, {2, 3}}},
      {{{2, 20}}},
      {{{3, 50}}},
      {{{4, std::nullopt}}},
  });
  auto leftPositions = serializeWithPositions(left);

  auto right = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {3, 20}}},
      {{{2, 20}}},
      {{{3, 40}}},
      {{{4, std::nullopt}}},
  });
  auto rightPositions = serializeWithPositions(right);

  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{-1}, {0}, {1}, std::nullopt},
      MAP(BIGINT(), BIGINT()),
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{1}, {0}, {1}, std::nullopt},
      MAP(BIGINT(), BIGINT()),
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{1}, {0}, {1}, {0}},
      MAP(BIGINT(), BIGINT()),
      true,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, fuzzCompare) {
  VectorFuzzer::Options opts;
  opts.vectorSize = 1'000;
  opts.nullRatio = 0.5;
  opts.dictionaryHasNulls = true;

  VectorFuzzer fuzzer(opts, pool_.get());

  std::vector<vector_size_t> offsets(100);
  for (auto i = 0; i < offsets.size(); ++i) {
    offsets[i] = i * 10;
  }

  for (auto i = 0; i < 1'000; ++i) {
    auto seed = folly::Random::rand32();

    LOG(INFO) << i << ": seed: " << seed;

    fuzzer.reSeed(seed);

    {
      SCOPED_TRACE(fmt::format("seed: {}, ARRAY", seed));
      auto elements = fuzzer.fuzz(BIGINT());
      auto arrayVector = makeArrayVector(offsets, elements);
      testCompare(arrayVector);
    }

    {
      SCOPED_TRACE(fmt::format("seed: {}, MAP", seed));
      auto keys = fuzzer.fuzz(BIGINT());
      auto values = fuzzer.fuzz(BIGINT());
      auto mapVector = makeMapVector(offsets, keys, values);
      testCompare(mapVector);
    }

    {
      SCOPED_TRACE(fmt::format("seed: {}, ROW", seed));
      std::vector<VectorPtr> children{
          fuzzer.fuzz(BIGINT()),
          fuzzer.fuzz(BIGINT()),
      };
      auto rowVector = makeRowVector(children);
      testCompare(rowVector);
    }
  }
}

} // namespace
} // namespace bytedance::bolt::exec
