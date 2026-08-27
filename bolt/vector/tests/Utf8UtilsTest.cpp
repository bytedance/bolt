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

#include "bolt/vector/Utf8Utils.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "gtest/gtest.h"

namespace bytedance::bolt::test {
namespace {

class Utf8UtilsTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  static std::string decodedStringAt(
      const VectorPtr& vector,
      vector_size_t row) {
    DecodedVector decoded(*vector);
    return decoded.valueAt<StringView>(row).str();
  }

  static std::string replacementRun(int32_t count) {
    const std::string replacement{"\xEF\xBF\xBD", 3};
    std::string result;
    result.reserve(count * replacement.size());
    for (int32_t index = 0; index < count; ++index) {
      result.append(replacement);
    }
    return result;
  }
};

TEST_F(Utf8UtilsTest, replacesInvalidSequencesWithOpenJdkGrouping) {
  const std::string replacement{"\xEF\xBF\xBD", 3};
  const std::vector<std::pair<std::string, std::string>> cases = {
      {{"\xD5\xE8\xD6\xC6\xEC", 5},
       replacement + replacement + replacement + replacement + replacement},
      {{"\xE2\x28\xA1", 3}, replacement + "(" + replacement},
      {{"\xF0\x9F\x92", 3}, replacement},
      {{"\xED\xA0\x80", 3}, replacement},
      {{"\xC0\x80", 2}, replacement + replacement},
      {{"\xE0\x80\x80", 3}, replacement + replacement + replacement},
      {{"\xF5\x80\x80\x80", 4},
       replacement + replacement + replacement + replacement},
      {{"A\xE2\x28\xA1Z", 5}, "A" + replacement + "(" + replacement + "Z"},
      {{"\xE2\x82\x41", 3}, replacement + "A"},
      {{"\xF0\x9F\x41", 3}, replacement + "A"},
      {{"\xF0\x9F\x92\x41", 4}, replacement + "A"},
      {{"\xF4\x90\x80\x80", 4},
       replacement + replacement + replacement + replacement},
      {{"\x9C\xA9", 2}, replacement + replacement},
      {{"\x20\x02\x0F\x00\x01\x00\x00\x00\x9C\xA9\x06\x60\x00\x00"
        "\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00",
        28},
       std::string(
           "\x20\x02\x0F\x00\x01\x00\x00\x00\xEF\xBF\xBD\xEF\xBF"
           "\xBD\x06\x60\x00\x00\x00\x00\x02\x00\x00\x00\x05\x00"
           "\x00\x00\x01\x00\x00\x00",
           32)},
      {std::string(63, '\xD5') + std::string("\xD5\x80", 2),
       replacementRun(63) + std::string("\xD5\x80", 2)},
      {{"\xE4\xB8\xAD\xE4\xB8\xD5\xE4\xB8\xAD", 9},
       std::string("\xE4\xB8\xAD", 3) + replacement + replacement +
           std::string("\xE4\xB8\xAD", 3)},
  };

  std::vector<std::string> inputs;
  std::vector<std::string> expectedOutputs;
  for (const auto& [inputValue, expectedValue] : cases) {
    inputs.push_back(inputValue);
    expectedOutputs.push_back(expectedValue);
  }

  auto values = makeFlatVector<std::string>(inputs);
  auto input = makeRowVector({values});
  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  for (auto row = 0; row < cases.size(); ++row) {
    EXPECT_EQ(expectedOutputs[row], decodedStringAt(output->childAt(0), row));
    EXPECT_EQ(inputs[row], decodedStringAt(values, row));
  }
}

TEST_F(Utf8UtilsTest, leavesValidInputsByteIdentical) {
  std::string longThreeByte;
  for (int32_t index = 0; index < 20; ++index) {
    longThreeByte.append("\xE4\xB8\xAD", 3);
  }
  longThreeByte.append("tail");

  const std::vector<std::string> inputs = {
      "Spark ASCII",
      {"\xC2\xA2", 2},
      {"Spark \xE4\xB8\xAD \xF0\x9F\x98\x80", 14},
      {"\xE0\xA0\x80", 3},
      {"\xF4\x8F\xBF\xBF", 4},
      longThreeByte,
  };

  auto input = makeRowVector({makeFlatVector<std::string>(inputs)});
  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  EXPECT_EQ(input.get(), output.get());
}

TEST_F(Utf8UtilsTest, fallsBackAfterThreeByteFastValidation) {
  std::string valid;
  for (int32_t index = 0; index < 20; ++index) {
    valid.append("\xE4\xB8\xAD", 3);
  }
  std::vector<std::string> inputs(10, valid);
  inputs[8] = valid + std::string("\xD5", 1);
  inputs[9] = valid + std::string("\xF0\x9F\x98\x80", 4);

  auto input = makeRowVector({makeFlatVector<std::string>(inputs)});
  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  EXPECT_EQ(
      valid + std::string("\xEF\xBF\xBD", 3),
      decodedStringAt(output->childAt(0), 8));
  EXPECT_EQ(inputs[9], decodedStringAt(output->childAt(0), 9));
}

TEST_F(Utf8UtilsTest, fallsBackAfterDenseLeadFastScan) {
  constexpr int32_t kMalformedBytes = 64;
  std::vector<std::string> inputs(10, std::string(kMalformedBytes, '\xD5'));
  inputs[8] = std::string(63, '\xD5') + std::string("\xD5\x80", 2);

  auto input = makeRowVector({makeFlatVector<std::string>(inputs)});
  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  EXPECT_EQ(
      replacementRun(63) + std::string("\xD5\x80", 2),
      decodedStringAt(output->childAt(0), 8));
  EXPECT_EQ(
      replacementRun(kMalformedBytes), decodedStringAt(output->childAt(0), 9));
}

TEST_F(Utf8UtilsTest, denseMalformedInputUsesRowBoundedScratchMemory) {
  constexpr vector_size_t kNumRows = 10'000;
  constexpr int32_t kBytesPerRow = 64;
  constexpr int64_t kMaxAdditionalBytes = 6L << 20;

  auto densePool = rootPool_->addLeafChild("denseMalformedUtf8");
  VectorMaker maker(densePool.get());
  auto values = maker.flatVector<std::string>(kNumRows, [](vector_size_t) {
    return std::string(kBytesPerRow, '\xD5');
  });
  auto input = maker.rowVector({values});
  const auto bytesBefore = densePool->currentBytes();

  auto output =
      utf8::replaceInvalidUtf8InTopLevelVarchars(input, densePool.get());

  ASSERT_NE(input.get(), output.get());
  output->validate({});
  EXPECT_LT(densePool->peakBytes() - bytesBefore, kMaxAdditionalBytes);
}

TEST_F(Utf8UtilsTest, sharesUniformReplacementOnlyOutputBuffer) {
  constexpr vector_size_t kNumRows = 100;
  constexpr int32_t kBytesPerRow = 65;
  const auto expected = replacementRun(kBytesPerRow);

  auto input = makeRowVector({makeFlatVector<std::string>(
      std::vector<std::string>(kNumRows, std::string(kBytesPerRow, '\xD5')))});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, output->childAt(0)->encoding());
  auto* outputValues =
      output->childAt(0)->asUnchecked<FlatVector<StringView>>();
  EXPECT_EQ(outputValues->valueAt(0).data(), outputValues->valueAt(1).data());
  EXPECT_EQ(
      outputValues->valueAt(0).data(),
      outputValues->valueAt(kNumRows - 1).data());
  input.reset();
  output->validate({});
  EXPECT_EQ(expected, decodedStringAt(output->childAt(0), 0));
  EXPECT_EQ(expected, decodedStringAt(output->childAt(0), kNumRows - 1));
}

TEST_F(Utf8UtilsTest, sharesReplacementOnlyOutputBuffer) {
  auto values = makeFlatVector<std::string>(
      {std::string(64, '\xD5'),
       std::string(64, '\xF5'),
       std::string(63, '\xD5')});
  auto input = makeRowVector({values});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, output->childAt(0)->encoding());
  auto* outputValues =
      output->childAt(0)->asUnchecked<FlatVector<StringView>>();
  EXPECT_EQ(outputValues->valueAt(0).data(), outputValues->valueAt(1).data());
  EXPECT_EQ(outputValues->valueAt(0).data(), outputValues->valueAt(2).data());
  EXPECT_NE(outputValues->valueAt(0).size(), outputValues->valueAt(2).size());
  input.reset();
  values.reset();
  output->validate({});
  EXPECT_EQ(replacementRun(64), decodedStringAt(output->childAt(0), 0));
  EXPECT_EQ(replacementRun(64), decodedStringAt(output->childAt(0), 1));
  EXPECT_EQ(replacementRun(63), decodedStringAt(output->childAt(0), 2));
}

TEST_F(Utf8UtilsTest, keepsVaryingInlineReplacementOnlyOutputInline) {
  auto input = makeRowVector({makeFlatVector<std::string>(
      {std::string(1, '\xD5'),
       std::string(2, '\xD5'),
       std::string(3, '\xD5')})});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, output->childAt(0)->encoding());
  auto* outputValues =
      output->childAt(0)->asUnchecked<FlatVector<StringView>>();
  EXPECT_TRUE(outputValues->stringBuffers().empty());
  EXPECT_EQ(replacementRun(1), decodedStringAt(output->childAt(0), 0));
  EXPECT_EQ(replacementRun(2), decodedStringAt(output->childAt(0), 1));
  EXPECT_EQ(replacementRun(3), decodedStringAt(output->childAt(0), 2));
}

TEST_F(Utf8UtilsTest, trustsKnownAsciiMetadata) {
  const std::string invalid{"\xD5", 1};
  auto values = makeFlatVector<std::string>({invalid});
  values->as<SimpleVector<StringView>>()->setAllIsAscii(true);
  auto input = makeRowVector({values});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  EXPECT_EQ(input.get(), output.get());
  EXPECT_EQ(invalid, decodedStringAt(output->childAt(0), 0));
}

TEST_F(Utf8UtilsTest, ignoresUnreferencedInvalidDictionaryValues) {
  const std::string valid(64, 'v');
  const std::string invalid{"\xD5", 1};
  auto dictionary = wrapInDictionary(
      makeIndices({0, 0, 0}),
      makeFlatVector<std::string>({valid, invalid}, VARCHAR()));
  auto input = makeRowVector({dictionary});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  EXPECT_EQ(input.get(), output.get());
}

TEST_F(Utf8UtilsTest, preservesDictionaryEncodingWhenReplacing) {
  const std::string valid(64, 'v');
  const std::string invalid{"\xD5", 1};
  const std::string replacement{"\xEF\xBF\xBD", 3};
  auto base = makeFlatVector<std::string>({valid, invalid, invalid}, VARCHAR());
  auto indices = makeIndices({1, 0, 1, 0, 1, 0});
  auto dictionary = wrapInDictionary(indices, base);
  auto input = makeRowVector({dictionary});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  ASSERT_EQ(VectorEncoding::Simple::DICTIONARY, output->childAt(0)->encoding());
  auto* outputDictionary =
      output->childAt(0)->asUnchecked<DictionaryVector<StringView>>();
  EXPECT_EQ(indices.get(), outputDictionary->indices().get());
  EXPECT_EQ(dictionary->nulls().get(), outputDictionary->nulls().get());
  EXPECT_EQ(replacement, decodedStringAt(output->childAt(0), 0));
  EXPECT_EQ(valid, decodedStringAt(output->childAt(0), 1));
  EXPECT_EQ(invalid, decodedStringAt(outputDictionary->valueVector(), 2));
}

TEST_F(Utf8UtilsTest, sharesUnchangedFlatStringsWhenReplacementIsSparse) {
  constexpr vector_size_t kNumRows = 100;
  const std::string valid(64, 'v');
  std::string invalid = valid;
  invalid[32] = '\xD5';
  const std::string expected =
      valid.substr(0, 32) + std::string("\xEF\xBF\xBD", 3) + valid.substr(33);

  std::vector<std::string> inputs(kNumRows, valid);
  inputs[0] = invalid;
  auto values = makeFlatVector<std::string>(inputs);
  const auto unchangedValue =
      values->asUnchecked<FlatVector<StringView>>()->valueAt(1);
  const auto* unchangedData = unchangedValue.data();
  auto input = makeRowVector({values});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  auto* outputValues =
      output->childAt(0)->asUnchecked<FlatVector<StringView>>();
  EXPECT_EQ(unchangedData, outputValues->valueAt(1).data());
  input.reset();
  values.reset();
  output->validate({});
  EXPECT_EQ(expected, decodedStringAt(output->childAt(0), 0));
  EXPECT_EQ(valid, decodedStringAt(output->childAt(0), 1));
}

TEST_F(Utf8UtilsTest, handlesSequenceAndDictionaryNulls) {
  const std::string invalid{"\xD5", 1};
  const std::string valid(64, 'v');
  const std::string replacement{"\xEF\xBF\xBD", 3};
  auto sequence = vectorMaker_.sequenceVector<StringView>(
      {StringView(invalid), StringView(invalid), StringView(valid)});

  auto indices = makeIndices({0, 1, 0});
  auto nulls = makeNulls(3, [](vector_size_t row) { return row != 1; });
  auto nullMaskedDictionary = BaseVector::wrapInDictionary(
      nulls,
      indices,
      3,
      makeFlatVector<std::string>({invalid, valid}, VARCHAR()));
  auto input = makeRowVector({sequence, nullMaskedDictionary});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  EXPECT_EQ(VectorEncoding::Simple::FLAT, output->childAt(0)->encoding());
  EXPECT_EQ(replacement, decodedStringAt(output->childAt(0), 0));
  EXPECT_EQ(replacement, decodedStringAt(output->childAt(0), 1));
  EXPECT_EQ(valid, decodedStringAt(output->childAt(0), 2));
  EXPECT_EQ(nullMaskedDictionary.get(), output->childAt(1).get());
  EXPECT_TRUE(output->childAt(1)->isNullAt(0));
  EXPECT_EQ(valid, decodedStringAt(output->childAt(1), 1));
  EXPECT_TRUE(output->childAt(1)->isNullAt(2));
}

TEST_F(Utf8UtilsTest, skipsConstantVectors) {
  std::string invalid(64, 'v');
  invalid[32] = '\xD5';
  auto constant = makeConstant<StringView>(StringView(invalid), 3);
  auto input = makeRowVector({constant});

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  EXPECT_EQ(input.get(), output.get());
  EXPECT_EQ(constant.get(), output->childAt(0).get());
  EXPECT_EQ(invalid, decodedStringAt(constant, 0));
}

TEST_F(Utf8UtilsTest, replacesOnlyTopLevelVarcharsAcrossEncodings) {
  const std::string invalid{"\xD5", 1};
  const std::string valid(64, 'v');
  const std::string replacement{"\xEF\xBF\xBD", 3};

  auto flat = makeNullableFlatVector<std::string>(
      {invalid, valid, std::nullopt}, VARCHAR());
  auto constant = makeConstant<StringView>(StringView(invalid), 3);
  auto dictionary = wrapInDictionary(
      makeIndices({1, 0, 2}),
      makeNullableFlatVector<std::string>(
          {valid, invalid, std::nullopt}, VARCHAR()));
  auto binary =
      makeFlatVector<std::string>({invalid, invalid, invalid}, VARBINARY());
  auto nested = makeArrayVector<std::string>({{invalid}, {invalid}, {invalid}});
  auto unchangedVarchar = makeFlatVector<std::string>({valid, valid, valid});
  auto input = makeRowVector(
      {flat, constant, dictionary, unchangedVarchar, binary, nested},
      [](auto row) { return row == 2; });

  auto output = utf8::replaceInvalidUtf8InTopLevelVarchars(input, pool());

  ASSERT_NE(input.get(), output.get());
  output->validate({});
  EXPECT_EQ(replacement, decodedStringAt(output->childAt(0), 0));
  EXPECT_EQ(valid, decodedStringAt(output->childAt(0), 1));
  EXPECT_TRUE(output->childAt(0)->isNullAt(2));
  EXPECT_EQ(constant.get(), output->childAt(1).get());
  EXPECT_EQ(invalid, decodedStringAt(output->childAt(1), 0));
  EXPECT_EQ(replacement, decodedStringAt(output->childAt(2), 0));
  EXPECT_EQ(valid, decodedStringAt(output->childAt(2), 1));
  EXPECT_TRUE(output->childAt(2)->isNullAt(2));

  EXPECT_EQ(input->nulls().get(), output->nulls().get());
  EXPECT_TRUE(output->isNullAt(2));
  EXPECT_EQ(unchangedVarchar.get(), output->childAt(3).get());
  EXPECT_EQ(binary.get(), output->childAt(4).get());
  EXPECT_EQ(nested.get(), output->childAt(5).get());
  EXPECT_EQ(invalid, decodedStringAt(output->childAt(4), 0));
  EXPECT_EQ(
      invalid,
      output->childAt(5)
          ->as<ArrayVector>()
          ->elements()
          ->as<FlatVector<StringView>>()
          ->valueAt(0)
          .str());
  EXPECT_EQ(invalid, decodedStringAt(flat, 0));
  EXPECT_EQ(invalid, decodedStringAt(constant, 0));
  EXPECT_EQ(invalid, decodedStringAt(dictionary, 0));
}

} // namespace
} // namespace bytedance::bolt::test
