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

#include <cstdint>
#include <limits>
#include <optional>

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class BitmapBucketNumberTest : public SparkFunctionBaseTest {
 protected:
  std::optional<int64_t> bucketNumber(std::optional<int64_t> value) {
    return evaluateOnce<int64_t>("bitmap_bucket_number(c0)", value);
  }

  std::optional<int64_t> bucketNumberFromInt(std::optional<int32_t> value) {
    return evaluateOnce<int64_t>(
        "bitmap_bucket_number(cast(c0 as bigint))", value);
  }
};

TEST_F(BitmapBucketNumberTest, positiveInputs) {
  EXPECT_EQ(bucketNumber(1), 1);
  EXPECT_EQ(bucketNumber(2), 1);
  EXPECT_EQ(bucketNumber(32768), 1);
  EXPECT_EQ(bucketNumber(32769), 2);
  EXPECT_EQ(bucketNumber(65536), 2);
  EXPECT_EQ(bucketNumber(65537), 3);
  EXPECT_EQ(bucketNumber(3232423), 99);
  EXPECT_EQ(
      bucketNumber(std::numeric_limits<int64_t>::max()), 281474976710656L);
}

TEST_F(BitmapBucketNumberTest, nonPositiveInputs) {
  EXPECT_EQ(bucketNumber(0), 0);
  EXPECT_EQ(bucketNumber(-1), 0);
  EXPECT_EQ(bucketNumber(-32767), 0);
  EXPECT_EQ(bucketNumber(-32768), -1);
  EXPECT_EQ(bucketNumber(-32769), -1);
  EXPECT_EQ(bucketNumber(-65535), -1);
  EXPECT_EQ(bucketNumber(-65536), -2);
  EXPECT_EQ(bucketNumber(-3843485), -117);
  EXPECT_EQ(
      bucketNumber(std::numeric_limits<int64_t>::min()), -281474976710656L);
  EXPECT_EQ(
      bucketNumber(std::numeric_limits<int64_t>::min() + 1), -281474976710655L);
}

TEST_F(BitmapBucketNumberTest, nullAndCastInputs) {
  EXPECT_EQ(bucketNumber(std::nullopt), std::nullopt);
  EXPECT_EQ(bucketNumberFromInt(65537), 3);
  EXPECT_EQ(bucketNumberFromInt(-65536), -2);
  EXPECT_EQ(bucketNumberFromInt(std::nullopt), std::nullopt);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
