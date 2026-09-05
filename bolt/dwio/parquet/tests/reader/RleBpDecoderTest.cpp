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

#include "bolt/dwio/common/BitPackDecoder.h"
#include "bolt/dwio/parquet/reader/RleBpDecoder.h"

#include <arrow/util/rle_encoding.h> // @manual
#include <gtest/gtest.h>

#include <random>
using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio::common;
using bytedance::bolt::parquet::RleBpDecoder;

TEST(RleBpDecoderDiagnosticTest, EmptyHeaderIncludesRoleAndBounds) {
  const char input[] = {0};
  RleBpDecoder decoder(
      input, input, 1, "definition-level", 7, 11, 13);
  uint64_t output = 0;

  try {
    decoder.readBits(1, &output);
    FAIL() << "Expected an empty RLE header to fail";
  } catch (const std::exception& error) {
    const std::string message = error.what();
    EXPECT_NE(message.find("role=definition-level"), std::string::npos);
    EXPECT_NE(message.find("row_group=7"), std::string::npos);
    EXPECT_NE(message.find("column=11"), std::string::npos);
    EXPECT_NE(message.find("page=13"), std::string::npos);
    EXPECT_NE(message.find("buffer_offset=0"), std::string::npos);
    EXPECT_NE(message.find("buffer_remaining=0"), std::string::npos);
    EXPECT_NE(message.find("requested_values=1"), std::string::npos);
    EXPECT_NE(
        message.find("Invalid varint value: too few bytes"),
        std::string::npos);
  }
}

TEST(RleBpDecoderDiagnosticTest, TruncatedHeaderIncludesDictionaryRole) {
  const char input[] = {static_cast<char>(0x80)};
  RleBpDecoder decoder(input, input + 1, 1, "dictionary-id", 2, 3, 5);
  uint8_t output = 0;
  auto* outputPtr = &output;

  try {
    decoder.next(outputPtr, 4);
    FAIL() << "Expected a truncated RLE header to fail";
  } catch (const std::exception& error) {
    const std::string message = error.what();
    EXPECT_NE(message.find("role=dictionary-id"), std::string::npos);
    EXPECT_NE(message.find("buffer_remaining=1"), std::string::npos);
    EXPECT_NE(message.find("requested_values=4"), std::string::npos);
  }
}

TEST(RleBpDecoderDiagnosticTest, ReportsTruncatedDefinitionLevelCount) {
  // Two RLE runs encode 8 + 803 zero definition levels. The page header
  // claims 827 values, so the final request exposes a 16-level shortfall.
  const char input[] = {
      0x10, 0x00, static_cast<char>(0xc6), 0x0c, 0x00};
  RleBpDecoder decoder(
      input, input + sizeof(input), 1, "definition-level", 0, 124, 1, 827);
  std::vector<uint64_t> output(bits::nwords(827));

  try {
    decoder.readBits(827, output.data());
    FAIL() << "Expected a short definition-level stream to fail";
  } catch (const std::exception& error) {
    const std::string message = error.what();
    EXPECT_NE(
        message.find("PARQUET_INVALID_LEVEL_COUNT"), std::string::npos);
    EXPECT_NE(message.find("role=definition-level"), std::string::npos);
    EXPECT_NE(message.find("column=124"), std::string::npos);
    EXPECT_NE(message.find("page=1"), std::string::npos);
    EXPECT_NE(message.find("encoded_values=811"), std::string::npos);
    EXPECT_NE(message.find("expected_values=827"), std::string::npos);
    EXPECT_NE(message.find("missing_values=16"), std::string::npos);
  }
}

template <typename T>
class RleBpDecoderTest {
 public:
  RleBpDecoderTest() {
    inputValues_.resize(numValues_, 0);
    outputValues_.resize(numValues_, 0);
    encodedValues_.resize(numValues_ * 4, 0);
  }

  RleBpDecoderTest(uint32_t numValues) : numValues_(numValues) {
    BOLT_CHECK(numValues % 8 == 0);

    inputValues_.resize(numValues_, 0);
    outputValues_.resize(numValues_, 0);
    encodedValues_.resize(numValues_ * 4, 0);
  }

  void testDecodeRandomData(uint8_t bitWidth) {
    bitWidth_ = bitWidth;

    populateInputValues();

    testDecode();
  }

  void testDecodeSuppliedData(std::vector<T> inputValues, uint8_t bitWidth) {
    numValues_ = inputValues.size();
    inputValues_ = inputValues;
    bitWidth_ = bitWidth;

    encodeInputValues();
    testDecode();
  }

 private:
  void testDecode() {
    const uint8_t* inputIter = encodedValues_.data();
    T* output = outputValues_.data();
    bytedance::bolt::dwio::common::unpack<T>(
        inputIter, bytes(bitWidth_), numValues_, bitWidth_, output);
    inputIter = encodedValues_.data();
    output = outputValues_.data();
    T* expectedOutput = inputValues_.data();
    for (int i = 0; i < numValues_; i++) {
      ASSERT_EQ(output[i], expectedOutput[i]);
    }
  }

  void populateInputValues() {
    auto maxValue = (1L << bitWidth_) - 1;

    for (auto j = 0; j < numValues_; j++) {
      inputValues_[j] = rand() % maxValue;
    }

    encodeInputValues();
  }

  void encodeInputValues() {
    arrow::util::RleEncoder arrowEncoder(
        reinterpret_cast<uint8_t*>(inputValues_.data()),
        bytes(bitWidth_),
        bitWidth_);

    for (auto i = 0; i < numValues_; i++) {
      arrowEncoder.Put(inputValues_[i]);
    }
    arrowEncoder.Flush();
  }

  uint32_t bytes(uint8_t bitWidth) {
    return (numValues_ * bitWidth + 7) / 8;
  }

  // multiple of 8
  uint32_t numValues_ = 1024;
  std::vector<T> inputValues_;
  std::vector<T> outputValues_;
  std::vector<uint8_t> encodedValues_;
  uint8_t bitWidth_;
};

TEST(RleBpDecoderTest, DISABLED_uint8) {
  RleBpDecoderTest<uint8_t> test(1024);

  test.testDecodeRandomData(1);
  test.testDecodeRandomData(2);
  test.testDecodeRandomData(3);
  test.testDecodeRandomData(4);
  test.testDecodeRandomData(5);
  test.testDecodeRandomData(6);
  test.testDecodeRandomData(7);
  test.testDecodeRandomData(8);
}

TEST(RleBpDecoderTest, DISABLED_uint16) {
  RleBpDecoderTest<uint16_t> test(1024);

  for (uint8_t i = 1; i <= 16; i++) {
    test.testDecodeRandomData(i);
  }
}

TEST(RleBpDecoderTest, DISABLED_uint32) {
  RleBpDecoderTest<uint32_t> test(1024);

  for (uint8_t i = 1; i <= 32; i++) {
    test.testDecodeRandomData(i);
  }
}

TEST(RleBpDecoderTest, DISABLED_allOnes) {
  std::vector<uint8_t> allOnesVector(1024, 1);
  RleBpDecoderTest<uint8_t> test;
  test.testDecodeSuppliedData(allOnesVector, 1);
}
