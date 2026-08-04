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

#include <cstring>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/Bitmap.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace bytedance::bolt::functions::sparksql::test {
namespace {

using namespace bytedance::bolt::functions::sparksql;

class BitmapCountTest : public SparkFunctionBaseTest {
 protected:
  std::optional<int64_t> bitmapCount(const std::optional<std::string>& bitmap) {
    return evaluateOnce<int64_t, std::string>(
        "bitmap_count(cast(c0 as varbinary))", bitmap);
  }

  /// Evaluate bitmap_count for a binary value. Uses FunctionBaseTest::evaluate
  /// like other Spark tests (avoids manual ExprSet construction issues).
  int64_t evalBitmapCount(const std::string& serialized) {
    auto vec = makeConstant(StringView(serialized), 1, VARBINARY());
    auto result = evaluate<SimpleVector<int64_t>>(
        "bitmap_count(c0)", makeRowVector({vec}));
    return result->valueAt(0);
  }

  void assertNullResult() {
    auto vec = makeNullConstant(TypeKind::VARBINARY, 1);
    auto result = evaluate<SimpleVector<int64_t>>(
        "bitmap_count(c0)", makeRowVector({vec}));
    EXPECT_TRUE(result->isNullAt(0));
  }

  std::string makeAllZeroBitmap() {
    return std::string(kBitmapNumBytes, '\0');
  }

  std::string makeAllOnesBitmap() {
    return std::string(kBitmapNumBytes, '\xff');
  }

  std::string makeBitmapWithBit(int32_t bitPosition) {
    std::string bitmap(kBitmapNumBytes, '\0');
    int32_t byteIdx = bitPosition / 8;
    int32_t bitIdx = bitPosition % 8;
    bitmap[byteIdx] = static_cast<char>(1 << bitIdx);
    return bitmap;
  }
};

TEST_F(BitmapCountTest, nullInput) {
  auto result = bitmapCount(std::nullopt);
  EXPECT_EQ(result, std::nullopt);
}

TEST_F(BitmapCountTest, emptyBinary) {
  EXPECT_EQ(bitmapPopcount(nullptr, 0), 0);
  EXPECT_EQ(bitmapPopcount("", 0), 0);
}

TEST_F(BitmapCountTest, nullBinaryConstant) {
  assertNullResult();
}

TEST_F(BitmapCountTest, allZeroBitmap) {
  EXPECT_EQ(evalBitmapCount(makeAllZeroBitmap()), 0);
}

TEST_F(BitmapCountTest, allOnesBitmap) {
  EXPECT_EQ(evalBitmapCount(makeAllOnesBitmap()), 32768);
}

TEST_F(BitmapCountTest, singleBit) {
  EXPECT_EQ(evalBitmapCount(makeBitmapWithBit(0)), 1);
  EXPECT_EQ(evalBitmapCount(makeBitmapWithBit(32767)), 1);
}

TEST_F(BitmapCountTest, multipleBits) {
  auto bitmap = makeAllZeroBitmap();
  auto setBit = [&](int32_t pos) {
    bitmap[pos / 8] |= static_cast<char>(1 << (pos % 8));
  };
  setBit(0);
  setBit(100);
  setBit(1000);
  setBit(10000);
  setBit(32767);
  EXPECT_EQ(evalBitmapCount(bitmap), 5);
}

TEST_F(BitmapCountTest, mixedBits) {
  std::string bitmap(kBitmapNumBytes, static_cast<char>(0xAA));
  EXPECT_EQ(evalBitmapCount(bitmap), 16384);
}

TEST_F(BitmapCountTest, misalignedData) {
  constexpr int32_t kBufSize = kBitmapNumBytes + 1;
  auto padded = AlignedBuffer::allocate<char>(kBufSize, pool());
  std::memset(padded->asMutable<char>() + 1, 0xFF, kBitmapNumBytes);
  EXPECT_EQ(
      evalBitmapCount(std::string(padded->as<char>() + 1, kBitmapNumBytes)),
      32768);
}

TEST_F(BitmapCountTest, sparkHexFFFF) {
  std::string bytes(2, '\0');
  bytes[0] = static_cast<char>(0xFF);
  bytes[1] = static_cast<char>(0xFF);
  EXPECT_EQ(evalBitmapCount(bytes), 16);
}

TEST_F(BitmapCountTest, nonFullWord) {
  std::string bytes(3, '\0');
  bytes[0] = static_cast<char>(0xAA);
  bytes[1] = static_cast<char>(0x55);
  bytes[2] = static_cast<char>(0x0F);
  EXPECT_EQ(evalBitmapCount(bytes), 12);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
