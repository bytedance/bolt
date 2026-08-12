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

// Adapted from Apache Arrow.

#include "bolt/dwio/parquet/arrow/LevelConversion.h"

#include "bolt/dwio/parquet/arrow/LevelComparison.h"
#include "bolt/dwio/parquet/arrow/tests/TestUtil.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/ubsan.h"
namespace bytedance::bolt::parquet::arrow {
namespace internal {

using ::arrow::internal::Bitmap;
using ::testing::ElementsAreArray;

std::string BitmapToString(const uint8_t* bitmap, int64_t bit_count) {
  return ::arrow::internal::Bitmap(bitmap, /*offset*/ 0, /*length=*/bit_count)
      .ToString();
}

std::string BitmapToString(
    const std::vector<uint8_t>& bitmap,
    int64_t bit_count) {
  return BitmapToString(bitmap.data(), bit_count);
}

TEST(TestColumnReader, DefLevelsToBitmap) {
  // Bugs in this function were exposed in ARROW-3930
  std::vector<int16_t> def_levels = {3, 3, 3, 2, 3, 3, 3, 3, 3};

  std::vector<uint8_t> valid_bits(2, 0);

  LevelInfo level_info;
  level_info.def_level = 3;
  level_info.rep_level = 1;

  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = def_levels.size();
  io.values_read = -1;
  io.valid_bits = valid_bits.data();

  DefLevelsToBitmap(def_levels.data(), 9, level_info, &io);
  ASSERT_EQ(9, io.values_read);
  ASSERT_EQ(1, io.null_count);

  // Call again with 0 definition levels, make sure that valid_bits is
  // unmodified
  const uint8_t current_byte = valid_bits[1];
  io.null_count = 0;
  DefLevelsToBitmap(def_levels.data(), 0, level_info, &io);

  ASSERT_EQ(0, io.values_read);
  ASSERT_EQ(0, io.null_count);
  ASSERT_EQ(current_byte, valid_bits[1]);
}

TEST(TestColumnReader, DefLevelsToBitmapPowerOfTwo) {
  // PARQUET-1623: Invalid memory access when decoding a valid bits vector that
  // has a length equal to a power of two and also using a non-zero
  // valid_bits_offset.  This should not fail when run with ASAN or valgrind.
  std::vector<int16_t> def_levels = {3, 3, 3, 2, 3, 3, 3, 3};
  std::vector<uint8_t> valid_bits(1, 0);

  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 3;

  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = def_levels.size();
  io.values_read = -1;
  io.valid_bits = valid_bits.data();

  // Read the latter half of the validity bitmap
  DefLevelsToBitmap(def_levels.data() + 4, 4, level_info, &io);
  ASSERT_EQ(4, io.values_read);
  ASSERT_EQ(0, io.null_count);
}

#if defined(ARROW_LITTLE_ENDIAN)
TEST(GreaterThanBitmap, GeneratesExpectedBitmasks) {
  std::vector<int16_t> levels = {
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5,
      6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3,
      4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7};
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/0, /*rhs*/ 0), 0);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 8), 0);
  EXPECT_EQ(
      GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ -1),
      0xFFFFFFFFFFFFFFFF);
  // Should be zero padded.
  EXPECT_EQ(
      GreaterThanBitmap(levels.data(), /*num_levels=*/47, /*rhs*/ -1),
      0x7FFFFFFFFFFF);
  EXPECT_EQ(
      GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 6),
      0x8080808080808080);
}
#endif

TEST(DefLevelsToBitmap, WithRepetitionLevelFiltersOutEmptyListValues) {
  std::vector<uint8_t> validity_bitmap(/*count*/ 8, 0);

  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = 64;
  io.values_read = 1;
  io.null_count = 5;
  io.valid_bits = validity_bitmap.data();
  io.valid_bits_offset = 1;

  LevelInfo level_info;
  level_info.repeated_ancestor_def_level = 1;
  level_info.def_level = 2;
  level_info.rep_level = 1;
  // All zeros should be ignored, ones should be unset in the bitmp and 2 should
  // be set.
  std::vector<int16_t> def_levels = {0, 0, 0, 2, 2, 1, 0, 2};
  DefLevelsToBitmap(def_levels.data(), def_levels.size(), level_info, &io);

  EXPECT_EQ(BitmapToString(validity_bitmap, /*bit_count=*/8), "01101000");
  for (size_t x = 1; x < validity_bitmap.size(); x++) {
    EXPECT_EQ(validity_bitmap[x], 0) << "index: " << x;
  }
  EXPECT_EQ(io.null_count, /*5 + 1 =*/6);
  EXPECT_EQ(io.values_read, 4); // value should get overwritten.
}

struct MultiLevelTestData {
 public:
  std::vector<int16_t> def_levels;
  std::vector<int16_t> rep_levels;
};

MultiLevelTestData TriplyNestedList() {
  // Triply nested list values borrow from write_path
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  return MultiLevelTestData{
      /*def_levels=*/std::vector<int16_t>{
          2,
          7,
          6,
          7,
          5,
          3, // first row
          5,
          5,
          7,
          7,
          2,
          7, // second row
          0, // third row
          1},
      /*rep_levels=*/
      std::vector<int16_t>{
          0,
          1,
          3,
          3,
          2,
          1, // first row
          0,
          1,
          2,
          3,
          1,
          1, // second row
          0,
          0}};
}

template <typename ConverterType>
class NestedListTest : public testing::Test {
 public:
  void InitForLength(int length) {
    this->validity_bits_.clear();
    this->validity_bits_.insert(this->validity_bits_.end(), length, 0);
    validity_io_.valid_bits = validity_bits_.data();
    validity_io_.values_read_upper_bound = length;
    offsets_.clear();
    offsets_.insert(offsets_.end(), length + 1, 0);
  }

  typename ConverterType::OffsetsType* Run(
      const MultiLevelTestData& test_data,
      LevelInfo level_info) {
    return this->converter_.ComputeListInfo(
        test_data, level_info, &validity_io_, offsets_.data());
  }

  ConverterType converter_;
  ValidityBitmapInputOutput validity_io_;
  std::vector<uint8_t> validity_bits_;
  std::vector<typename ConverterType::OffsetsType> offsets_;
};

template <typename IndexType>
struct RepDefLevelConverter {
  using OffsetsType = IndexType;
  OffsetsType* ComputeListInfo(
      const MultiLevelTestData& test_data,
      LevelInfo level_info,
      ValidityBitmapInputOutput* output,
      IndexType* offsets) {
    DefRepLevelsToList(
        test_data.def_levels.data(),
        test_data.rep_levels.data(),
        test_data.def_levels.size(),
        level_info,
        output,
        offsets);
    return offsets + output->values_read;
  }
};

using ConverterTypes = ::testing::Types<
    RepDefLevelConverter</*list_length_type=*/int32_t>,
    RepDefLevelConverter</*list_length_type=*/int64_t>>;
TYPED_TEST_SUITE(NestedListTest, ConverterTypes);

TYPED_TEST(NestedListTest, OuterMostTest) {
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 4 outer most lists (len(3), len(4), null, len(0))
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;

  this->InitForLength(4);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 4);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 3, 7, 7, 7));

  EXPECT_EQ(this->validity_io_.values_read, 4);
  EXPECT_EQ(this->validity_io_.null_count, 1);
  EXPECT_EQ(
      BitmapToString(this->validity_io_.valid_bits, /*length=*/4), "1101");
}

TYPED_TEST(NestedListTest, MiddleListTest) {
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> middle lists (null, len(2), len(0),
  //                  len(1), len(2), null, len(1),
  //                  N/A,
  //                  N/A
  LevelInfo level_info;
  level_info.rep_level = 2;
  level_info.def_level = 4;
  level_info.repeated_ancestor_def_level = 2;

  this->InitForLength(7);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 7);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 0, 2, 2, 3, 5, 5, 6));

  EXPECT_EQ(this->validity_io_.values_read, 7);
  EXPECT_EQ(this->validity_io_.null_count, 2);
  EXPECT_EQ(
      BitmapToString(this->validity_io_.valid_bits, /*length=*/7), "0111101");
}

TYPED_TEST(NestedListTest, InnerMostListTest) {
  // [null, [[1, null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 6 inner lists (N/A, [len(3), len(0)], N/A
  //                        len(0), [len(0), len(2)], N/A, len(1),
  //                        N/A,
  //                        N/A
  LevelInfo level_info;
  level_info.rep_level = 3;
  level_info.def_level = 6;
  level_info.repeated_ancestor_def_level = 4;

  this->InitForLength(6);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 6);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 3, 3, 3, 3, 5, 6));

  EXPECT_EQ(this->validity_io_.values_read, 6);
  EXPECT_EQ(this->validity_io_.null_count, 0);
  EXPECT_EQ(
      BitmapToString(this->validity_io_.valid_bits, /*length=*/6), "111111");
}

TYPED_TEST(NestedListTest, SimpleLongList) {
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;
  level_info.repeated_ancestor_def_level = 0;

  MultiLevelTestData test_data;
  // No empty lists.
  test_data.def_levels = std::vector<int16_t>(65 * 9, 2);
  for (int x = 0; x < 65; x++) {
    test_data.rep_levels.push_back(0);
    test_data.rep_levels.insert(
        test_data.rep_levels.end(),
        8,
        /*rep_level=*/1);
  }

  std::vector<typename TypeParam::OffsetsType> expected_offsets(66, 0);
  for (size_t x = 1; x < expected_offsets.size(); x++) {
    expected_offsets[x] = static_cast<typename TypeParam::OffsetsType>(x) * 9;
  }
  this->InitForLength(65);
  typename TypeParam::OffsetsType* next_position =
      this->Run(test_data, level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 65);
  EXPECT_THAT(this->offsets_, testing::ElementsAreArray(expected_offsets));

  EXPECT_EQ(this->validity_io_.values_read, 65);
  EXPECT_EQ(this->validity_io_.null_count, 0);
  EXPECT_EQ(
      BitmapToString(this->validity_io_.valid_bits, /*length=*/65),
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "1");
}

TYPED_TEST(NestedListTest, TestOverflow) {
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;
  level_info.repeated_ancestor_def_level = 0;

  MultiLevelTestData test_data;
  test_data.def_levels = std::vector<int16_t>{2};
  test_data.rep_levels = std::vector<int16_t>{0};

  this->InitForLength(2);
  // Offsets is populated as the cumulative sum of all elements,
  // so populating the offsets[0] with max-value impacts the
  // other values populated.
  this->offsets_[0] =
      std::numeric_limits<typename TypeParam::OffsetsType>::max();
  this->offsets_[1] =
      std::numeric_limits<typename TypeParam::OffsetsType>::max();
  ASSERT_THROW(this->Run(test_data, level_info), ParquetException);

  ASSERT_THROW(this->Run(test_data, level_info), ParquetException);

  // Same thing should happen if the list already existed.
  test_data.rep_levels = std::vector<int16_t>{1};
  ASSERT_THROW(this->Run(test_data, level_info), ParquetException);

  // Should be OK because it shouldn't increment.
  test_data.def_levels = std::vector<int16_t>{0};
  test_data.rep_levels = std::vector<int16_t>{0};
  this->Run(test_data, level_info);
}

TEST(NestedListTest, DirectLengthsMatchesOffsets) {
  auto check = [](const MultiLevelTestData& test_data,
                  LevelInfo level_info,
                  int32_t expectedValuesRead) {
    std::vector<uint8_t> offsetsValidity(expectedValuesRead, 0);
    ValidityBitmapInputOutput offsetsIo;
    offsetsIo.valid_bits = offsetsValidity.data();
    offsetsIo.values_read_upper_bound = expectedValuesRead;
    std::vector<int32_t> offsets(expectedValuesRead + 1, 0);
    DefRepLevelsToList(
        test_data.def_levels.data(),
        test_data.rep_levels.data(),
        test_data.def_levels.size(),
        level_info,
        &offsetsIo,
        offsets.data());

    std::vector<uint8_t> lengthsValidity(expectedValuesRead, 0);
    ValidityBitmapInputOutput lengthsIo;
    lengthsIo.valid_bits = lengthsValidity.data();
    lengthsIo.values_read_upper_bound = expectedValuesRead;
    std::vector<int32_t> lengths(expectedValuesRead, -1);
    DefRepLevelsToListLengths(
        test_data.def_levels.data(),
        test_data.rep_levels.data(),
        test_data.def_levels.size(),
        level_info,
        &lengthsIo,
        lengths.data());

    ASSERT_EQ(lengthsIo.values_read, offsetsIo.values_read);
    ASSERT_EQ(lengthsIo.null_count, offsetsIo.null_count);
    ASSERT_EQ(
        BitmapToString(lengthsValidity, expectedValuesRead),
        BitmapToString(offsetsValidity, expectedValuesRead));
    for (auto i = 0; i < offsetsIo.values_read; ++i) {
      EXPECT_EQ(lengths[i], offsets[i + 1] - offsets[i]) << "index=" << i;
    }
  };

  LevelInfo outer;
  outer.rep_level = 1;
  outer.def_level = 2;
  check(TriplyNestedList(), outer, 4);

  LevelInfo middle;
  middle.rep_level = 2;
  middle.def_level = 4;
  middle.repeated_ancestor_def_level = 2;
  check(TriplyNestedList(), middle, 7);

  LevelInfo inner;
  inner.rep_level = 3;
  inner.def_level = 6;
  inner.repeated_ancestor_def_level = 4;
  check(TriplyNestedList(), inner, 6);

  MultiLevelTestData nullableElements;
  nullableElements.def_levels = std::vector<int16_t>{2, 1, 2, 0, 1, 1};
  nullableElements.rep_levels = std::vector<int16_t>{0, 1, 1, 0, 0, 1};
  check(nullableElements, outer, 3);
}

TEST(NestedListTest, DirectLengthsUpperBound) {
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;
  level_info.repeated_ancestor_def_level = 0;

  std::vector<int16_t> def_levels = {2, 2};
  std::vector<int16_t> rep_levels = {0, 0};
  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = 1;
  std::vector<int32_t> lengths(2, -1);

  ASSERT_THROW(
      DefRepLevelsToListLengths(
          def_levels.data(),
          rep_levels.data(),
          def_levels.size(),
          level_info,
          &io,
          lengths.data()),
      ParquetException);
  EXPECT_EQ(lengths[1], -1);
}

TEST(NestedListTest, DirectLengthsExactUpperBound) {
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;
  level_info.repeated_ancestor_def_level = 0;

  std::vector<int16_t> def_levels = {2, 2};
  std::vector<int16_t> rep_levels = {0, 0};
  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = 2;
  std::vector<int32_t> lengths(2, -1);

  DefRepLevelsToListLengths(
      def_levels.data(),
      rep_levels.data(),
      def_levels.size(),
      level_info,
      &io,
      lengths.data());

  EXPECT_EQ(io.values_read, 2);
  EXPECT_THAT(lengths, testing::ElementsAre(1, 1));
}

TEST(NestedListTest, FusedDirectStructListMatchesSeparateConversions) {
  MultiLevelTestData test_data;
  test_data.def_levels = std::vector<int16_t>{
      0, // null struct and therefore null list.
      1, // present struct, null list.
      2, // present struct, empty list.
      3, // present struct, list with null element.
      4, // present struct, list with first non-null element.
      4, // same list, second non-null element.
      2, // present struct, another empty list.
      0 // null struct.
  };
  test_data.rep_levels = std::vector<int16_t>{0, 0, 0, 0, 0, 1, 0, 0};

  LevelInfo structInfo;
  structInfo.rep_level = 0;
  structInfo.def_level = 1;
  structInfo.repeated_ancestor_def_level = 0;

  LevelInfo listInfo;
  listInfo.rep_level = 1;
  listInfo.def_level = 3;
  listInfo.repeated_ancestor_def_level = 0;

  constexpr int32_t expectedValuesRead = 7;

  std::vector<uint8_t> separateListValidity(expectedValuesRead, 0);
  ValidityBitmapInputOutput separateListIo;
  separateListIo.valid_bits = separateListValidity.data();
  separateListIo.values_read_upper_bound = expectedValuesRead;
  std::vector<int32_t> separateLengths(expectedValuesRead, -1);
  DefRepLevelsToListLengths(
      test_data.def_levels.data(),
      test_data.rep_levels.data(),
      test_data.def_levels.size(),
      listInfo,
      &separateListIo,
      separateLengths.data());

  std::vector<uint8_t> separateStructValidity(expectedValuesRead, 0);
  ValidityBitmapInputOutput separateStructIo;
  separateStructIo.valid_bits = separateStructValidity.data();
  separateStructIo.values_read_upper_bound = expectedValuesRead;
  DefRepLevelsToBitmap(
      test_data.def_levels.data(),
      test_data.rep_levels.data(),
      test_data.def_levels.size(),
      structInfo,
      &separateStructIo);

  std::vector<uint8_t> fusedListValidity(expectedValuesRead, 0);
  ValidityBitmapInputOutput fusedListIo;
  fusedListIo.valid_bits = fusedListValidity.data();
  fusedListIo.values_read_upper_bound = expectedValuesRead;
  std::vector<int32_t> fusedLengths(expectedValuesRead, -1);

  std::vector<uint8_t> fusedStructValidity(expectedValuesRead, 0);
  ValidityBitmapInputOutput fusedStructIo;
  fusedStructIo.valid_bits = fusedStructValidity.data();
  fusedStructIo.values_read_upper_bound = expectedValuesRead;

  ASSERT_TRUE(DefRepLevelsToListLengthsAndStructBitmap(
      test_data.def_levels.data(),
      test_data.rep_levels.data(),
      test_data.def_levels.size(),
      listInfo,
      structInfo,
      &fusedListIo,
      fusedLengths.data(),
      &fusedStructIo));

  EXPECT_EQ(fusedListIo.values_read, separateListIo.values_read);
  EXPECT_EQ(fusedListIo.null_count, separateListIo.null_count);
  EXPECT_EQ(fusedStructIo.values_read, separateStructIo.values_read);
  EXPECT_EQ(fusedStructIo.null_count, separateStructIo.null_count);
  EXPECT_THAT(fusedLengths, testing::ElementsAreArray(separateLengths));
  EXPECT_EQ(
      BitmapToString(fusedListValidity, expectedValuesRead),
      BitmapToString(separateListValidity, expectedValuesRead));
  EXPECT_EQ(
      BitmapToString(fusedStructValidity, expectedValuesRead),
      BitmapToString(separateStructValidity, expectedValuesRead));
}

TEST(NestedListTest, FusedDirectStructListMatchesSeparateConversionOptions) {
  MultiLevelTestData testData;
  testData.def_levels = std::vector<int16_t>{
      0, // Null repeated ancestor, ignored.
      1, // Empty repeated ancestor, ignored.
      2, // Null struct and therefore null list.
      3, // Present struct, null list.
      4, // Present struct, empty list.
      5, // Present struct, list with a null element.
      6, // Present struct, list with a non-null element.
      6, // Same list, second non-null element.
      6, // Deeper repeated descendant, ignored.
      2 // Null struct.
  };
  testData.rep_levels = std::vector<int16_t>{0, 0, 1, 1, 1, 1, 1, 2, 3, 1};

  LevelInfo structInfo;
  structInfo.rep_level = 1;
  structInfo.def_level = 3;
  structInfo.repeated_ancestor_def_level = 2;
  LevelInfo listInfo;
  listInfo.rep_level = 2;
  listInfo.def_level = 5;
  listInfo.repeated_ancestor_def_level = 2;

  constexpr int32_t kNumLists = 6;
  constexpr int32_t kBitmapOffset = 3;
  auto makeOutput = [](std::vector<uint8_t>* validity) {
    ValidityBitmapInputOutput output;
    output.valid_bits = validity ? validity->data() : nullptr;
    output.valid_bits_offset = kBitmapOffset;
    output.values_read_upper_bound = kNumLists;
    output.null_count = 4;
    return output;
  };

  std::vector<uint8_t> separateListValidity(2, 0xa5);
  auto separateListIo = makeOutput(&separateListValidity);
  std::vector<int32_t> separateLengths(kNumLists, -1);
  DefRepLevelsToListLengths(
      testData.def_levels.data(),
      testData.rep_levels.data(),
      testData.def_levels.size(),
      listInfo,
      &separateListIo,
      separateLengths.data());

  std::vector<uint8_t> separateStructValidity(2, 0xa5);
  auto separateStructIo = makeOutput(&separateStructValidity);
  DefRepLevelsToBitmap(
      testData.def_levels.data(),
      testData.rep_levels.data(),
      testData.def_levels.size(),
      structInfo,
      &separateStructIo);

  std::vector<uint8_t> fusedListValidity(2, 0xa5);
  auto fusedListIo = makeOutput(&fusedListValidity);
  std::vector<int32_t> fusedLengths(kNumLists, -1);
  std::vector<uint8_t> fusedStructValidity(2, 0xa5);
  auto fusedStructIo = makeOutput(&fusedStructValidity);

  ASSERT_TRUE(DefRepLevelsToListLengthsAndStructBitmap(
      testData.def_levels.data(),
      testData.rep_levels.data(),
      testData.def_levels.size(),
      listInfo,
      structInfo,
      &fusedListIo,
      fusedLengths.data(),
      &fusedStructIo));
  EXPECT_EQ(fusedListIo.values_read, separateListIo.values_read);
  EXPECT_EQ(fusedStructIo.values_read, separateStructIo.values_read);
  EXPECT_EQ(fusedListIo.null_count, separateListIo.null_count);
  EXPECT_EQ(fusedStructIo.null_count, separateStructIo.null_count);
  EXPECT_THAT(fusedLengths, testing::ElementsAreArray(separateLengths));
  EXPECT_EQ(fusedListValidity, separateListValidity);
  EXPECT_EQ(fusedStructValidity, separateStructValidity);

  auto separateLengthsOnlyIo = makeOutput(nullptr);
  std::vector<int32_t> separateLengthsOnly(kNumLists, -1);
  DefRepLevelsToListLengths(
      testData.def_levels.data(),
      testData.rep_levels.data(),
      testData.def_levels.size(),
      listInfo,
      &separateLengthsOnlyIo,
      separateLengthsOnly.data());

  auto fusedLengthsOnlyIo = makeOutput(nullptr);
  std::vector<int32_t> fusedLengthsOnly(kNumLists, -1);
  std::vector<uint8_t> fusedStructOnlyValidity(2, 0xa5);
  auto fusedStructOnlyIo = makeOutput(&fusedStructOnlyValidity);
  ASSERT_TRUE(DefRepLevelsToListLengthsAndStructBitmap(
      testData.def_levels.data(),
      testData.rep_levels.data(),
      testData.def_levels.size(),
      listInfo,
      structInfo,
      &fusedLengthsOnlyIo,
      fusedLengthsOnly.data(),
      &fusedStructOnlyIo));
  EXPECT_EQ(fusedLengthsOnlyIo.values_read, separateLengthsOnlyIo.values_read);
  EXPECT_EQ(fusedLengthsOnlyIo.null_count, separateLengthsOnlyIo.null_count);
  EXPECT_THAT(fusedLengthsOnly, testing::ElementsAreArray(separateLengthsOnly));
}

TEST(NestedListTest, FusedDirectStructListRandomizedMatchesSeparate) {
  constexpr uint32_t kSeed = 20260730;
  std::mt19937 rng(kSeed);
  std::uniform_int_distribution<int32_t> percent(0, 99);
  std::uniform_int_distribution<int32_t> mixedLength(0, 12);
  std::uniform_int_distribution<int32_t> longLength(32, 128);

  LevelInfo structInfo;
  structInfo.rep_level = 0;
  structInfo.def_level = 1;
  LevelInfo listInfo;
  listInfo.rep_level = 1;
  listInfo.def_level = 3;

  for (const int32_t numLists : {1, 17, 1024, 65536}) {
    for (const int32_t shape : {0, 1, 2}) {
      MultiLevelTestData testData;
      for (int32_t list = 0; list < numLists; ++list) {
        const auto state = percent(rng);
        if (state < 5) {
          testData.def_levels.push_back(0);
          testData.rep_levels.push_back(0);
          continue;
        }
        if (state < 10) {
          testData.def_levels.push_back(1);
          testData.rep_levels.push_back(0);
          continue;
        }
        if (state < 15) {
          testData.def_levels.push_back(2);
          testData.rep_levels.push_back(0);
          continue;
        }
        int32_t length = shape == 0 ? 1
            : shape == 1            ? mixedLength(rng)
                                    : longLength(rng);
        if (length == 0) {
          testData.def_levels.push_back(2);
          testData.rep_levels.push_back(0);
          continue;
        }
        for (int32_t index = 0; index < length; ++index) {
          testData.def_levels.push_back(percent(rng) < 10 ? 3 : 4);
          testData.rep_levels.push_back(index == 0 ? 0 : 1);
        }
      }

      std::vector<uint8_t> separateListValidity(numLists, 0);
      ValidityBitmapInputOutput separateListIo;
      separateListIo.valid_bits = separateListValidity.data();
      separateListIo.values_read_upper_bound = numLists;
      std::vector<int32_t> separateLengths(numLists, -1);
      DefRepLevelsToListLengths(
          testData.def_levels.data(),
          testData.rep_levels.data(),
          testData.def_levels.size(),
          listInfo,
          &separateListIo,
          separateLengths.data());

      std::vector<uint8_t> separateStructValidity(numLists, 0);
      ValidityBitmapInputOutput separateStructIo;
      separateStructIo.valid_bits = separateStructValidity.data();
      separateStructIo.values_read_upper_bound = numLists;
      DefRepLevelsToBitmap(
          testData.def_levels.data(),
          testData.rep_levels.data(),
          testData.def_levels.size(),
          structInfo,
          &separateStructIo);

      std::vector<uint8_t> fusedListValidity(numLists, 0);
      ValidityBitmapInputOutput fusedListIo;
      fusedListIo.valid_bits = fusedListValidity.data();
      fusedListIo.values_read_upper_bound = numLists;
      std::vector<int32_t> fusedLengths(numLists, -1);
      std::vector<uint8_t> fusedStructValidity(numLists, 0);
      ValidityBitmapInputOutput fusedStructIo;
      fusedStructIo.valid_bits = fusedStructValidity.data();
      fusedStructIo.values_read_upper_bound = numLists;

      ASSERT_TRUE(DefRepLevelsToListLengthsAndStructBitmap(
          testData.def_levels.data(),
          testData.rep_levels.data(),
          testData.def_levels.size(),
          listInfo,
          structInfo,
          &fusedListIo,
          fusedLengths.data(),
          &fusedStructIo));
      ASSERT_EQ(fusedListIo.values_read, separateListIo.values_read);
      ASSERT_EQ(fusedStructIo.values_read, separateStructIo.values_read);
      EXPECT_EQ(fusedListIo.null_count, separateListIo.null_count);
      EXPECT_EQ(fusedStructIo.null_count, separateStructIo.null_count);
      EXPECT_THAT(fusedLengths, testing::ElementsAreArray(separateLengths));
      EXPECT_EQ(
          BitmapToString(fusedListValidity, numLists),
          BitmapToString(separateListValidity, numLists));
      EXPECT_EQ(
          BitmapToString(fusedStructValidity, numLists),
          BitmapToString(separateStructValidity, numLists));
    }
  }
}

TEST(NestedListTest, FusedDirectStructListRejectsUnsupportedLevels) {
  LevelInfo structInfo;
  structInfo.rep_level = 0;
  structInfo.def_level = 1;
  LevelInfo listInfo;
  listInfo.rep_level = 2;
  listInfo.def_level = 3;
  std::vector<int16_t> definitions = {0};
  std::vector<int16_t> repetitions = {0};
  std::vector<int32_t> lengths(1);
  ValidityBitmapInputOutput listOutput;
  listOutput.values_read_upper_bound = 1;
  ValidityBitmapInputOutput structOutput;
  structOutput.values_read_upper_bound = 1;

  EXPECT_FALSE(DefRepLevelsToListLengthsAndStructBitmap(
      definitions.data(),
      repetitions.data(),
      definitions.size(),
      listInfo,
      structInfo,
      &listOutput,
      lengths.data(),
      &structOutput));
}

TEST(NestedListTest, FusedDirectStructListUpperBound) {
  LevelInfo structInfo;
  structInfo.rep_level = 0;
  structInfo.def_level = 1;
  LevelInfo listInfo;
  listInfo.rep_level = 1;
  listInfo.def_level = 3;
  std::vector<int16_t> definitions = {4, 4};
  std::vector<int16_t> repetitions = {0, 0};
  std::vector<int32_t> lengths(2, -1);
  ValidityBitmapInputOutput listOutput;
  listOutput.values_read_upper_bound = 1;
  ValidityBitmapInputOutput structOutput;
  structOutput.values_read_upper_bound = 1;

  EXPECT_THROW(
      DefRepLevelsToListLengthsAndStructBitmap(
          definitions.data(),
          repetitions.data(),
          definitions.size(),
          listInfo,
          structInfo,
          &listOutput,
          lengths.data(),
          &structOutput),
      ParquetException);
  EXPECT_EQ(lengths[1], -1);
}

TEST(LevelConversionTest, DefLevelsAreAllValid) {
  {
    std::vector<int16_t> def_levels = {3, 3, 3, 3, 3, 3, 3, 3, 3};
    LevelInfo level_info;
    level_info.def_level = 3;
    int64_t values_read = -1;
    EXPECT_TRUE(DefLevelsAreAllValid(
        def_levels.data(),
        def_levels.size(),
        level_info,
        def_levels.size(),
        &values_read));
    EXPECT_EQ(values_read, def_levels.size());
  }
  {
    std::vector<int16_t> def_levels = {3, 3, 3, 2, 3};
    LevelInfo level_info;
    level_info.def_level = 3;
    int64_t values_read = -1;
    EXPECT_FALSE(DefLevelsAreAllValid(
        def_levels.data(),
        def_levels.size(),
        level_info,
        def_levels.size(),
        &values_read));
    EXPECT_EQ(values_read, def_levels.size());
  }
  {
    std::vector<int16_t> def_levels = {0, 0, 2, 2, 2, 2, 2, 2, 0, 2};
    LevelInfo level_info;
    level_info.rep_level = 1;
    level_info.repeated_ancestor_def_level = 1;
    level_info.def_level = 2;
    int64_t values_read = -1;
    EXPECT_TRUE(DefLevelsAreAllValid(
        def_levels.data(), def_levels.size(), level_info, 7, &values_read));
    EXPECT_EQ(values_read, 7);
  }
  {
    std::vector<int16_t> def_levels = {0, 0, 2, 1, 2, 2, 2, 2};
    LevelInfo level_info;
    level_info.rep_level = 1;
    level_info.repeated_ancestor_def_level = 1;
    level_info.def_level = 2;
    int64_t values_read = -1;
    EXPECT_FALSE(DefLevelsAreAllValid(
        def_levels.data(), def_levels.size(), level_info, 6, &values_read));
    EXPECT_EQ(values_read, 6);
  }
  {
    std::vector<int16_t> def_levels = {3, 3, 3};
    LevelInfo level_info;
    level_info.def_level = 3;
    int64_t values_read = -1;
    EXPECT_THROW(
        DefLevelsAreAllValid(
            def_levels.data(), def_levels.size(), level_info, 2, &values_read),
        ParquetException);
  }
}

TEST(TestOnlyExtractBitsSoftware, BasicTest) {
  auto check =
      [](uint64_t bitmap, uint64_t selection, uint64_t expected) -> void {
    EXPECT_EQ(TestOnlyExtractBitsSoftware(bitmap, selection), expected);
  };
  check(0xFF, 0, 0);
  check(0xFF, ~uint64_t{0}, 0xFF);
  check(0xFF00FF, 0xAAAA, 0x000F);
  check(0xFF0AFF, 0xAFAA, 0x00AF);
  check(0xFFAAFF, 0xAFAA, 0x03AF);
  check(0xFECBDA9876543210ULL, 0xF00FF00FF00FF00FULL, 0xFBD87430ULL);
}

} // namespace internal
} // namespace bytedance::bolt::parquet::arrow
