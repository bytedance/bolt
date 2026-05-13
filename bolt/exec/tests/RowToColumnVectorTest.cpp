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

#include <folly/Range.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <tuple>
#include <vector>

#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/RowToColumnVector.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/RowBasedSerde.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;
namespace bytedance::bolt::exec::test {

class RowToColumnVectorTest : public OperatorTestBase {
 protected:
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

  constexpr static int32_t kBufferSize = 10 * 1024 * 1024;
};

TEST_F(RowToColumnVectorTest, basic) {
  VectorPtr ans = makeFlatVector<int32_t>(1, INTEGER());
  auto data = std::make_unique<char[]>(100);
  const int32_t x = 16;
  std::memcpy(data.get(), &x, sizeof(int32_t));
  auto ptr = data.get();
  std::vector<vector_size_t> rowNumbers{0};
  rowToColumnVector(
      &ptr,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      RowColumn(0, RowColumn::kNotNullOffset),
      0,
      ans);
  auto expect =
      makeFlatVector<int32_t>(1, [](auto row) -> int32_t { return x; });
  ASSERT_TRUE(ans->equalValueAt(expect.get(), 0, 0));
}

TEST_F(RowToColumnVectorTest, string) {
  VectorPtr ans = makeFlatVector<StringView>(1, VARCHAR());
  auto data = std::make_unique<char[]>(100);
  const std::string x = "AAAAAAAAAAAAAAAAAAAABBB";
  StringView xx(x.data(), x.size());
  std::memcpy(data.get(), &xx, sizeof(StringView));
  std::memcpy(data.get() + sizeof(StringView), xx.data(), xx.size());
  auto ptr = data.get();
  std::vector<vector_size_t> rowNumbers{0};
  rowToColumnVector(
      &ptr,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      RowColumn(0, RowColumn::kNotNullOffset),
      0,
      ans);
  auto expect = makeFlatVector<StringView>(1, [xx](auto row) { return xx; });
  ASSERT_TRUE(ans->equalValueAt(expect.get(), 0, 0));
}

TEST_F(RowToColumnVectorTest, row) {
  auto left = makeRowVector(
      {makeNullableFlatVector<int64_t>({78}, BIGINT()),
       makeNullableFlatVector<StringView>(
           {"aaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, VARCHAR()),
       makeNullableFlatVector<double>({9.8}, DOUBLE())});
  auto type = left->type();

  DecodedVector leftDecode(*left, SelectivityVector(1, true));
  auto leftData = std::make_unique<char[]>(kBufferSize);
  storeComplexType(leftDecode, 0, leftData.get());

  // pass random value when construct ans
  VectorPtr ans = makeRowVector(
      {makeNullableFlatVector<int64_t>({9999999999}, BIGINT()),
       makeNullableFlatVector<StringView>({"xx"}, VARCHAR()),
       makeNullableFlatVector<double>({0.5}, DOUBLE())});

  std::vector<vector_size_t> rowNumbers{0};
  auto ptr = leftData.get();
  rowToColumnVector(
      &ptr,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      RowColumn(0, RowColumn::kNotNullOffset),
      0,
      ans);

  auto expect = makeRowVector(
      {makeNullableFlatVector<int64_t>({78}, BIGINT()),
       makeNullableFlatVector<StringView>(
           {"aaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, VARCHAR()),
       makeNullableFlatVector<double>({9.8}, DOUBLE())});
  ans->equalValueAt(expect.get(), 0, 0);
}

} // namespace bytedance::bolt::exec::test
