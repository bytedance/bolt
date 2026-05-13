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

#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string_view>
#include <tuple>
#include <vector>

#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/exec/RowBasedCompare.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/RowBasedSerde.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/Type.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/SelectivityVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;
namespace bytedance::bolt::exec::test {
class RowBasedCompareTest : public OperatorTestBase {
 protected:
  // compare(a, b)
  // return -1 if a < b
  // return 0 if a == b
  // return 1 if a > b
  template <TypeKind Kind, typename T>
  void compare(
      T left,
      T right,
      int expect,
      TypePtr type,
      RowColumn leftCol,
      RowColumn rightCol,
      CompareFlags flag = CompareFlags()) {
    int ans = compareByRow<Kind>(
        reinterpret_cast<char*>(&left),
        reinterpret_cast<char*>(&right),
        leftCol,
        rightCol,
        flag,
        type.get());
    ASSERT_EQ(ans, expect);
  }

  template <TypeKind Kind, typename T>
  void compareNumber(TypePtr type) {
    RowColumn rc(0, RowColumn::kNotNullOffset);
    std::vector<std::tuple<T, T, int, bool>> data = {
        {1, 1, 0, true},
        {1, 1, 0, false},
        {1, 2, -1, true},
        {1, 2, 1, false},
        {2, 1, 1, true},
        {2, 1, -1, false}};
    for (const auto& row : data) {
      compare<Kind>(
          std::get<0>(row),
          std::get<1>(row),
          std::get<2>(row),
          type,
          rc,
          rc,
          {.ascending = std::get<3>(row)});
    }
  }

  void storeComplexType(
      const DecodedVector& decoded,
      vector_size_t index,
      char* row,
      int32_t offset = 0,
      int32_t nullByte = 0,
      uint8_t nullMask = 0) {
    if (decoded.isNullAt(index)) {
      BOLT_DCHECK(nullMask);
      row[nullByte] |= nullMask;
      return;
    }
    RowBasedSerializeStream stream(
        reinterpret_cast<uint8_t*>(row) + sizeof(std::string_view),
        kBufferSize);
    RowBasedSerde::serialize(*decoded.base(), decoded.index(index), stream);
    RowContainer::valueAt<std::string_view>(row, offset) = std::string_view(
        reinterpret_cast<char*>(stream.data()), stream.position());
  }

  void compareRow(
      int64_t left64,
      int64_t right64,
      StringView leftStr,
      StringView rightStr,
      double leftDouble,
      double rightDouble,
      CompareFlags flag,
      int expect) {
    auto left = makeRowVector(
        {makeNullableFlatVector<int64_t>({left64}, BIGINT()),
         makeNullableFlatVector<StringView>({leftStr}, VARCHAR()),
         makeNullableFlatVector<double>({leftDouble}, DOUBLE())});
    auto right = makeRowVector(
        {makeNullableFlatVector<int64_t>({right64}, BIGINT()),
         makeNullableFlatVector<StringView>({rightStr}, VARCHAR()),
         makeNullableFlatVector<double>({rightDouble}, DOUBLE())});
    auto type = left->type();

    DecodedVector leftDecode(*left, SelectivityVector(1, true));
    auto leftData = std::make_unique<char[]>(kBufferSize);
    storeComplexType(leftDecode, 0, leftData.get());
    DecodedVector rightDecode(*right, SelectivityVector(1, true));
    auto rightData = std::make_unique<char[]>(kBufferSize);
    storeComplexType(rightDecode, 0, rightData.get());

    int ans = compareByRow<TypeKind::ROW>(
        leftData.get(),
        rightData.get(),
        RowColumn(0, RowColumn::kNotNullOffset),
        RowColumn(0, RowColumn::kNotNullOffset),
        flag,
        type.get());
    ASSERT_EQ(ans, expect);
  }

  template <typename T>
  void compareArray(
      std::vector<std::vector<T>> left,
      std::vector<std::vector<T>> right,
      CompareFlags flag,
      TypePtr type,
      int expect) {
    auto leftArr = makeArrayVector<T>(left, type);
    auto rightArr = makeArrayVector<T>(right, type);

    DecodedVector leftDecode(*leftArr, SelectivityVector(1, true));
    auto leftData = std::make_unique<char[]>(kBufferSize);
    storeComplexType(leftDecode, 0, leftData.get());
    DecodedVector rightDecode(*rightArr, SelectivityVector(1, true));
    auto rightData = std::make_unique<char[]>(kBufferSize);
    storeComplexType(rightDecode, 0, rightData.get());

    int ans = compareByRow<TypeKind::ARRAY>(
        leftData.get(),
        rightData.get(),
        RowColumn(0, RowColumn::kNotNullOffset),
        RowColumn(0, RowColumn::kNotNullOffset),
        flag,
        ARRAY(type).get());
    ASSERT_EQ(ans, expect);
  }

 private:
  constexpr static int32_t kBufferSize = 10 * 1024 * 1024;
};

TEST_F(RowBasedCompareTest, number) {
  compareNumber<TypeKind::TINYINT, int8_t>(TINYINT());
  compareNumber<TypeKind::SMALLINT, int16_t>(SMALLINT());
  compareNumber<TypeKind::INTEGER, int32_t>(INTEGER());
  compareNumber<TypeKind::BIGINT, int64_t>(BIGINT());
  compareNumber<TypeKind::REAL, float>(REAL());
  compareNumber<TypeKind::DOUBLE, double>(DOUBLE());
}

TEST_F(RowBasedCompareTest, decimal) {
  compareNumber<TypeKind::BIGINT, int64_t>(DECIMAL(10, 2));
  compareNumber<TypeKind::HUGEINT, int128_t>(DECIMAL(38, 6));
}

TEST_F(RowBasedCompareTest, boolean) {
  RowColumn rc(0, RowColumn::kNotNullOffset);
  std::vector<std::tuple<bool, bool, int, bool>> data = {
      {true, false, 1, true},
      {true, false, -1, false},
      {false, true, -1, true},
      {false, true, 1, false},
      {true, true, 0, true},
      {false, false, 0, false}};
  for (const auto& row : data) {
    compare<TypeKind::BOOLEAN>(
        std::get<0>(row),
        std::get<1>(row),
        std::get<2>(row),
        BOOLEAN(),
        rc,
        rc,
        {.ascending = std::get<3>(row)});
  }
}

TEST_F(RowBasedCompareTest, timestamp) {
  RowColumn rc(0, RowColumn::kNotNullOffset);
  std::vector<std::tuple<Timestamp, Timestamp, int, bool>> data = {
      {{10000, 10000}, {100, 100}, 1, true},
      {{10000, 10000}, {100, 100}, -1, false},
      {{100, 100}, {10000, 10000}, -1, true},
      {{100, 100}, {10000, 10000}, 1, false},
      {{100, 100}, {100, 100}, 0, true},
      {{100, 100}, {100, 100}, 0, false}};
  for (const auto& row : data) {
    compare<TypeKind::TIMESTAMP>(
        std::get<0>(row),
        std::get<1>(row),
        std::get<2>(row),
        BOOLEAN(),
        rc,
        rc,
        {.ascending = std::get<3>(row)});
  }
}

TEST_F(RowBasedCompareTest, string) {
  RowColumn rc(0, RowColumn::kNotNullOffset);
  const std::string small = "row based spill test string",
                    big = "row based spill test string test test test test";
  StringView smallView(small.data(), small.size()),
      bigView(big.data(), big.size());
  std::vector<std::tuple<StringView, StringView, int, bool>> data = {
      {bigView, smallView, 1, true},
      {bigView, smallView, -1, false},
      {smallView, bigView, -1, true},
      {smallView, bigView, 1, false},
      {bigView, bigView, 0, true},
      {smallView, smallView, 0, false},
      {"a", "b", -1, true},
      {"a", "b", 1, false}};
  for (const auto& row : data) {
    compare<TypeKind::VARCHAR>(
        std::get<0>(row),
        std::get<1>(row),
        std::get<2>(row),
        BOOLEAN(),
        rc,
        rc,
        {.ascending = std::get<3>(row)});
    compare<TypeKind::VARBINARY>(
        std::get<0>(row),
        std::get<1>(row),
        std::get<2>(row),
        BOOLEAN(),
        rc,
        rc,
        {.ascending = std::get<3>(row)});
  }
}

TEST_F(RowBasedCompareTest, row) {
  compareRow(9, 9, "red", "red", 0.345, 0.345, {.ascending = true}, 0);
  compareRow(9, 9, "red", "red", 0.345, 0.345, {.ascending = false}, 0);

  compareRow(9, 10, "red", "red", 0.345, 0.345, {.ascending = true}, -1);
  compareRow(9, 10, "red", "red", 0.345, 0.345, {.ascending = false}, 1);

  compareRow(9, 9, "red", "red d", 0.345, 0.345, {.ascending = true}, -1);
  compareRow(9, 9, "red", "red d", 0.345, 0.345, {.ascending = false}, 1);

  compareRow(9, 9, "a", "b", 0.345, 0.345, {.ascending = true}, -1);
  compareRow(9, 9, "a", "b", 0.345, 0.345, {.ascending = false}, 1);

  compareRow(
      9,
      9,
      "aaaaaaaaaaaaaaaaaaaaaaaa",
      "aaaaaaaaaaaaaaaaaaaaaaab",
      0.345,
      0.345,
      {.ascending = true},
      -1);
  compareRow(
      9,
      9,
      "aaaaaaaaaaaaaaaaaaaaaaaa",
      "aaaaaaaaaaaaaaaaaaaaaaab",
      0.345,
      0.345,
      {.ascending = false},
      1);

  compareRow(9, 9, "red", "red", 0.345, 0.745, {.ascending = true}, -1);
  compareRow(9, 9, "red", "red", 0.345, 0.745, {.ascending = false}, 1);
}

TEST_F(RowBasedCompareTest, array) {
  compareArray<bool>(
      {{false, false, false}},
      {{false, false, false}},
      {.ascending = true},
      BOOLEAN(),
      0);

  compareArray<int32_t>(
      {{998, 618, 888}}, {{998, 618, 888}}, {.ascending = true}, INTEGER(), 0);
  compareArray<int64_t>(
      {{998, 618, 888}}, {{998, 618, 888}}, {.ascending = true}, BIGINT(), 0);

  compareArray<int64_t>(
      {{998, 618, 888}}, {{999, 618, 888}}, {.ascending = true}, BIGINT(), -1);
  compareArray<int64_t>(
      {{998, 618, 888}}, {{999, 618, 888}}, {.ascending = false}, BIGINT(), 1);
  compareArray<int64_t>(
      {{998, 618, 888}}, {{998, 618, 889}}, {.ascending = true}, BIGINT(), -1);
  compareArray<int64_t>(
      {{998, 618, 888}}, {{998, 618, 889}}, {.ascending = false}, BIGINT(), 1);

  compareArray<StringView>(
      {{"aaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaa"}},
      {{"aaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaab"}},
      {.ascending = true},
      VARCHAR(),
      -1);
  compareArray<StringView>(
      {{"aaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaa"}},
      {{"aaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaab"}},
      {.ascending = false},
      VARCHAR(),
      1);
}

} // namespace bytedance::bolt::exec::test
