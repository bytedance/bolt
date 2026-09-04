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

#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/common/base/SparkCompatibility.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/core/CoreTypeSystem.h"
#include "bolt/functions/lib/string/JavaStyleSplit.h"
#include "bolt/type/StringView.h"

#include <gtest/gtest.h>
#include <memory>
#include <vector>
using namespace bytedance::bolt;
using namespace bytedance::bolt::functions::stringImpl;
using namespace bytedance::bolt::functions::stringCore;
using namespace bytedance::bolt::functions::javaStyle;

namespace {
template <typename T, std::size_t N>
inline std::string u8str(const T (&str)[N]) {
  static_assert(sizeof(T) == 1, "Type must be 1 byte (char or char8_t)");
  return std::string(reinterpret_cast<const char*>(str), N - 1);
}

template <typename T, std::size_t N>
inline std::string_view u8sv(const T (&str)[N]) {
  static_assert(sizeof(T) == 1, "Type must be 1 byte (char or char8_t)");
  return std::string_view(reinterpret_cast<const char*>(str), N - 1);
}

} // namespace

class StringImplTest : public testing::Test {
 public:
  std::vector<std::tuple<std::string, std::string>> getUpperAsciiTestData() {
    return {
        {"abcdefg", "ABCDEFG"},
        {"ABCDEFG", "ABCDEFG"},
        {"a B c D e F g", "A B C D E F G"},
    };
  }

  std::vector<std::tuple<std::string, std::string>> getUpperUnicodeTestData() {
    return {
        {"àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ", "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ"},
        {"αβγδεζηθικλμνξοπρςστυφχψ", "ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ"},
        {"абвгдежзийклмнопрстуфхцчшщъыьэюя",
         "АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"}};
  }

  std::vector<std::tuple<std::string, std::string>> getLowerAsciiTestData() {
    return {
        {"ABCDEFG", "abcdefg"},
        {"abcdefg", "abcdefg"},
        {"a B c D e F g", "a b c d e f g"},
    };
  }

  std::vector<std::tuple<std::string, std::string>> getLowerUnicodeTestData() {
    return {
        {"ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ", "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ"},
        {"ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ", "αβγδεζηθικλμνξοπρσστυφχψ"},
        {"АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ",
         "абвгдежзийклмнопрстуфхцчшщъыьэюя"}};
  }
};

TEST_F(StringImplTest, isAscii) {
  constexpr size_t kSimdWidth = xsimd::batch<uint8_t>::size;
  constexpr size_t kMaxLength = 2 * kSimdWidth + 1;
  std::string input(kMaxLength + 1, 'a');

  // Start at an unaligned address and cover lengths around the first two SIMD
  // boundaries. Deriving the range from the native batch width covers SSE,
  // AVX/AVX2, and AVX-512 builds.
  const char* data = input.data() + 1;
  for (size_t length = 0; length <= kMaxLength; ++length) {
    EXPECT_TRUE(isAscii(data, length));

    for (size_t nonAsciiPosition = 0; nonAsciiPosition < length;
         ++nonAsciiPosition) {
      input[nonAsciiPosition + 1] = static_cast<char>(0x80);
      EXPECT_FALSE(isAscii(data, length))
          << "length: " << length
          << ", non-ASCII position: " << nonAsciiPosition;
      input[nonAsciiPosition + 1] = 'a';
    }
  }
}

TEST_F(StringImplTest, upperAscii) {
  for (auto& testCase : getUpperAsciiTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedUpper = std::get<1>(testCase);

    std::string upperOutput;
    upper</*ascii*/ true>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerAscii) {
  for (auto& testCase : getLowerAsciiTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower</*ascii*/ true>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
}

TEST_F(StringImplTest, upperUnicode) {
  for (auto& testCase : getUpperUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedUpper = std::get<1>(testCase);

    std::string upperOutput;
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerUnicode) {
  for (auto& testCase : getLowerUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
}

TEST_F(StringImplTest, concatLazy) {
  core::StringWriter output;

  // concat(lower(in1), upper(in2));
  auto f1 = [&](core::StringWriter& out) {
    std::string input("AA");
    out.reserve(out.size() + input.size());
    lowerAscii(out.data() + out.size(), input.data(), input.size());
    out.resize(out.size() + input.size());
  };

  auto f2 = [&](core::StringWriter& out) {
    std::string input("bb");
    out.reserve(out.size() + input.size());
    upperAscii(out.data() + out.size(), input.data(), input.size());
    out.resize(out.size() + input.size());
  };

  concatLazy(output, f1, f2);
  ASSERT_EQ(StringView("aaBB"), output);
}

TEST_F(StringImplTest, length) {
  auto lengthUtf8Ref = [](const char* inputBuffer, size_t bufferLength) {
    size_t size = 0;
    for (size_t i = 0; i < bufferLength; i++) {
      if ((static_cast<const unsigned char>(inputBuffer[i]) & 0xC0) != 0x80) {
        size++;
      }
    }
    return size;
  };

  // Test ascii inputs
  for (const auto& test : getUpperAsciiTestData()) {
    auto& inputString = std::get<0>(test);

    ASSERT_EQ(length</*isAscii*/ true>(inputString), inputString.size());
    ASSERT_EQ(length</*isAscii*/ false>(inputString), inputString.size());
    ASSERT_EQ(length</*isAscii*/ false>(inputString), inputString.size());
  }

  // Test unicode inputs
  for (auto& test : getLowerUnicodeTestData()) {
    auto& inputString = std::get<0>(test);

    ASSERT_EQ(
        length</*isAscii*/ false>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
    ASSERT_EQ(
        length</*isAscii*/ false>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
  }
}

TEST_F(StringImplTest, cappedLength) {
  auto input = std::string("abcd");
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 1), 1);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 2), 2);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 3), 3);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 4), 4);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 5), 4);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 6), 4);

  input = std::string("你好a世界");
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 1), 1);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 2), 2);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 3), 3);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 4), 4);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 5), 5);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 6), 5);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 7), 5);
}

TEST_F(StringImplTest, cappedUnicodeBytes) {
  // Test functions use case for indexing
  // UTF strings.
  std::string stringInput = "\xF4\x90\x80\x80Hello";
  ASSERT_EQ('H', stringInput[cappedByteLength<false>(stringInput, 2) - 1]);
  ASSERT_EQ('e', stringInput[cappedByteLength<false>(stringInput, 3) - 1]);
  ASSERT_EQ('l', stringInput[cappedByteLength<false>(stringInput, 4) - 1]);
  ASSERT_EQ('l', stringInput[cappedByteLength<false>(stringInput, 5) - 1]);
  ASSERT_EQ('o', stringInput[cappedByteLength<false>(stringInput, 6) - 1]);
  ASSERT_EQ('o', stringInput[cappedByteLength<false>(stringInput, 7) - 1]);

  // Multi-byte chars
  stringInput = "♫¡Singing is fun!♫";
  auto sPos = cappedByteLength<false>(stringInput, 2);
  auto exPos = cappedByteLength<false>(stringInput, 17);
  ASSERT_EQ("Singing is fun!♫", stringInput.substr(sPos));
  ASSERT_EQ("♫¡Singing is fun!", stringInput.substr(0, exPos));
  ASSERT_EQ("Singing is fun!", stringInput.substr(sPos, exPos - sPos));

  stringInput = std::string("abcd");
  auto stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 1), 1);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 2), 2);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 3), 3);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 4), 4);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 5), 4);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 6), 4);

  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 1), 1);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 2), 2);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 3), 3);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 4), 4);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 5), 4);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 6), 4);

  stringInput = std::string("你好a世界");
  stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 3);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 2), 6);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 3), 7);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 4), 10);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 5), 13);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 6), 13);

  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 1), 3);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 2), 6);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 3), 7);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 4), 10);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 5), 13);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 6), 13);

  stringInput = std::string("\x80");
  stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 2), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 3), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 4), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 5), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 6), 1);

  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 1), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 2), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 3), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 4), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 5), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 6), 1);

  stringInput.resize(2);
  // Create corrupt data below.
  char16_t c = u'\u04FF';
  stringInput[0] = (char)c;
  stringInput[1] = (char)c;

  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 1);

  stringInput.resize(4);
  c = u'\u04F4';
  char16_t c2 = u'\u048F';
  char16_t c3 = u'\u04BF';
  stringInput[0] = (char)c;
  stringInput[1] = (char)c2;
  stringInput[2] = (char)c3;
  stringInput[3] = (char)c3;

  stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 4);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 2), 4);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 3), 4);

  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 1), 4);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 2), 4);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 3), 4);
}

TEST_F(StringImplTest, badUnicodeLength) {
  ASSERT_EQ(0, length</*isAscii*/ false>(std::string("")));
  ASSERT_EQ(2, length</*isAscii*/ false>(std::string("ab")));
  // Try a bunch of special case unicode chars
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\u04FF")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U000E002F")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U0001D437")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U00002799")));

  std::string str;
  str.resize(2);
  // Create corrupt data below.
  char16_t c = u'\u04FF';
  str[0] = (char)c;
  str[1] = (char)c;

  auto len = length</*isAscii*/ false>(str);
  ASSERT_EQ(2, len);
}

TEST_F(StringImplTest, codePointToString) {
  auto testValidInput = [](const int64_t codePoint,
                           const std::string& expectedString) {
    core::StringWriter output;
    codePointToString(output, codePoint);
    ASSERT_EQ(
        StringView(expectedString), StringView(output.data(), output.size()));
  };

  auto testInvalidCodePoint = [](const int64_t codePoint) {
    core::StringWriter output;
    EXPECT_THROW(codePointToString(output, codePoint), BoltUserError)
        << "codePoint " << codePoint;
  };

  testValidInput(65, "A");
  testValidInput(9731, "\u2603");
  testValidInput(0, std::string("\0", 1));

  testInvalidCodePoint(-1);
  testInvalidCodePoint(1234567);
  testInvalidCodePoint(8589934592);
}

TEST_F(StringImplTest, charToCodePoint) {
  auto testValidInput = [](const std::string& charString,
                           const int64_t expectedCodePoint) {
    ASSERT_EQ(charToCodePoint(StringView(charString)), expectedCodePoint);
  };

  auto testValidInputRoundTrip = [](const int64_t codePoint) {
    core::StringWriter string;
    codePointToString(string, codePoint);
    ASSERT_EQ(charToCodePoint(string), codePoint) << "codePoint " << codePoint;
  };

  auto testExpectDeath = [](const std::string& charString) {
    EXPECT_THROW(charToCodePoint(StringView(charString)), BoltUserError)
        << "charString " << charString;
  };

  testValidInput("x", 0x78);
  testValidInput("\u840C", 0x840C);

  testValidInputRoundTrip(128077);
  testValidInputRoundTrip(33804);

  testExpectDeath("hello");
  testExpectDeath("\u666E\u5217\u65AF\u6258");
  testExpectDeath("");
}

TEST_F(StringImplTest, stringToCodePoints) {
  auto testStringToCodePoints =
      [](const std::string& charString,
         const std::vector<int32_t>& expectedCodePoints) {
        std::vector<int32_t> codePoints = stringToCodePoints(charString);
        ASSERT_EQ(codePoints.size(), expectedCodePoints.size());
        for (int i = 0; i < codePoints.size(); i++) {
          ASSERT_EQ(codePoints.at(i), expectedCodePoints.at(i));
        }
      };

  testStringToCodePoints("", {});
  testStringToCodePoints("h", {0x0068});
  testStringToCodePoints("hello", {0x0068, 0x0065, 0x006C, 0x006C, 0x006F});

  testStringToCodePoints("hïllo", {0x0068, 0x00EF, 0x006C, 0x006C, 0x006F});
  testStringToCodePoints("hüóOO", {0x0068, 0x00FC, 0x00F3, 0x004F, 0x004F});
  testStringToCodePoints("\u840C", {0x840C});

  BOLT_ASSERT_THROW(
      testStringToCodePoints("\xA9", {}),
      "Invalid UTF-8 encoding in characters");
  BOLT_ASSERT_THROW(
      testStringToCodePoints("ü\xA9", {}),
      "Invalid UTF-8 encoding in characters");
  BOLT_ASSERT_THROW(
      testStringToCodePoints("ü\xA9hello wooooorld", {}),
      "Invalid UTF-8 encoding in characters");
  BOLT_ASSERT_THROW(
      testStringToCodePoints("ü\xA9hello wooooooooorrrrrld", {}),
      "Invalid UTF-8 encoding in characters");
}

TEST_F(StringImplTest, overlappedStringPosition) {
  auto testValidInputAsciiLpos = [](std::string_view string,
                                    std::string_view substr,
                                    const int64_t instance,
                                    const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ true, true>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };
  auto testValidInputAsciiRpos = [](std::string_view string,
                                    std::string_view substr,
                                    const int64_t instance,
                                    const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ true, false>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };

  auto testValidInputUnicodeLpos = [](std::string_view string,
                                      std::string_view substr,
                                      const int64_t instance,
                                      const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ false, true>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };

  auto testValidInputUnicodeRpos = [](std::string_view string,
                                      std::string_view substr,
                                      const int64_t instance,
                                      const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ false, false>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };

  testValidInputAsciiLpos("aaa", "aa", 2, 2L);
  testValidInputAsciiRpos("aaa", "aa", 2, 1L);

  testValidInputAsciiLpos("|||", "||", 2, 2L);
  testValidInputAsciiRpos("|||", "||", 2, 1L);

  testValidInputUnicodeLpos("😋😋😋", "😋😋", 2, 2L);
  testValidInputUnicodeRpos("😋😋😋", "😋😋", 2, 1L);

  testValidInputUnicodeLpos("你你你", "你你", 2, 2L);
  testValidInputUnicodeRpos("你你你", "你你", 2, 1L);
}

TEST_F(StringImplTest, stringPosition) {
  auto testValidInputAscii = [](std::string_view string,
                                std::string_view substr,
                                const int64_t instance,
                                const int64_t expectedPosition) {
    ASSERT_EQ(
        stringPosition</*isAscii*/ true>(string, substr, instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(string, substr, instance),
        expectedPosition);
  };

  auto testValidInputUnicode = [](std::string_view string,
                                  std::string_view substr,
                                  const int64_t instance,
                                  const int64_t expectedPosition) {
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(string, substr, instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(string, substr, instance),
        expectedPosition);
  };

  testValidInputAscii("high", "ig", 1, 2L);
  testValidInputAscii("high", "igx", 1, 0L);
  testValidInputAscii("Quadratically", "a", 1, 3L);
  testValidInputAscii("foobar", "foobar", 1, 1L);
  testValidInputAscii("foobar", "obar", 1, 3L);
  testValidInputAscii("zoo!", "!", 1, 4L);
  testValidInputAscii("x", "", 1, 1L);
  testValidInputAscii("", "", 1, 1L);
  testValidInputAscii("abc/xyz/foo/bar", "/", 3, 12L);

  testValidInputUnicode("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u7231", 1, 4L);
  testValidInputUnicode(
      "\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u5E0C\u671B", 1, 6L);
  testValidInputUnicode("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "nice", 1, 0L);

  testValidInputUnicode("abc/xyz/foo/bar", "/", 1, 4L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 2, 8L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 3, 12L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 4, 0L);

  EXPECT_THROW(
      stringPosition</*isAscii*/ false>("foobar", "foobar", 0), BoltUserError);
}

TEST_F(StringImplTest, replace) {
  auto runTest = [](const std::string& string,
                    const std::string& replaced,
                    const std::string& replacement,
                    const std::string& expectedResults) {
    // Test out of place
    core::StringWriter output;
    replace(
        output,
        StringView(string),
        StringView(replaced),
        StringView(replacement));

    ASSERT_EQ(
        StringView(output.data(), output.size()), StringView(expectedResults));

    // Test in place
    if (replacement.size() <= replaced.size()) {
      core::StringWriter inOutString;
      inOutString.resize(string.size());
      if (string.size()) {
        std::memcpy(inOutString.data(), string.data(), string.size());
      }

      replaceInPlace(
          inOutString, StringView(replaced), StringView(replacement));
      ASSERT_EQ(
          StringView(inOutString.data(), inOutString.size()),
          StringView(expectedResults));
    }
  };

  runTest("aaa", "a", "aa", "aaaaaa");
  runTest("abcdefabcdef", "cd", "XX", "abXXefabXXef");
  runTest("abcdefabcdef", "cd", "", "abefabef");
  runTest("123123tech", "123", "", "tech");
  runTest("123tech123", "123", "", "tech");
  runTest("222tech", "2", "3", "333tech");
  runTest("0000123", "0", "", "123");
  runTest("0000123", "0", " ", "    123");
  runTest("foo", "", "", "foo");
  runTest("foo", "foo", "", "");
  runTest("abc", "", "xx", "xxaxxbxxcxx");
  runTest("", "", "xx", "");
  runTest("", "", "", "");

  runTest(
      "\u4FE1\u5FF5,\u7231,\u5E0C\u671B",
      ",",
      "\u2014",
      "\u4FE1\u5FF5\u2014\u7231\u2014\u5E0C\u671B");
  runTest("\u00D6sterreich", "\u00D6", "Oe", "Oesterreich");
}

TEST_F(StringImplTest, getByteRange) {
  // Unicode string
  char* unicodeString = (char*)"\uFE3D\uFE4B\uFF05abc";

  // Number of characters
  int unicodeStringCharacters = 6;

  // Size of all its prefixes
  std::array<const char*, 7> unicodeStringPrefixes{
      "", // dummy
      "",
      "\uFE3D",
      "\uFE3D\uFE4B",
      "\uFE3D\uFE4B\uFF05",
      "\uFE3D\uFE4B\uFF05a",
      "\uFE3D\uFE4B\uFF05ab",
  };

  // Locations precomputed in bytes
  std::vector<int> locationInBytes(7);
  for (int i = 1; i <= unicodeStringCharacters; i++) {
    locationInBytes[i] = strlen(unicodeStringPrefixes[i]);
  }

  // Test getByteRange
  for (int i = 1; i <= unicodeStringCharacters; i++) {
    auto expectedStartByteIndex = locationInBytes[i];
    auto expectedEndByteIndex = strlen(unicodeString);

    // Find the byte range of unicodeString[i, end]
    auto range = getByteRange</*isAscii*/ false>(unicodeString, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);

    range = getByteRange</*isAscii*/ false>(unicodeString, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);
  }

  // Test bad unicode strings.

  // This exercises bad unicode byte in determining startByteIndex.
  std::string badUnicode = "aa\xff  ";
  auto range = getByteRange<false>(badUnicode.data(), 4, 3);
  EXPECT_EQ(range.first, 3);
  EXPECT_EQ(range.second, 6);

  // This exercises bad unicode byte in determining endByteIndex.
  badUnicode = "\xff aa";
  range = getByteRange<false>(badUnicode.data(), 1, 3);
  EXPECT_EQ(range.first, 0);
  EXPECT_EQ(range.second, 3);
}

TEST_F(StringImplTest, pad) {
  auto runTest = [](const std::string& string,
                    const int64_t size,
                    const std::string& padString,
                    const std::string& expectedLpadResult,
                    const std::string& expectedRpadResult) {
    core::StringWriter lpadOutput;
    core::StringWriter rpadOutput;

    bool stringIsAscii = isAscii(string.c_str(), string.size());
    bool padStringIsAscii = isAscii(padString.c_str(), padString.size());
    if (stringIsAscii && padStringIsAscii) {
      bytedance::bolt::functions::stringImpl::
          pad<true /*lpad*/, true /*isAscii*/>(
              lpadOutput, StringView(string), size, StringView(padString));
      bytedance::bolt::functions::stringImpl::
          pad<false /*lpad*/, true /*isAscii*/>(
              rpadOutput, StringView(string), size, StringView(padString));
    } else {
      // At least one of the string args is non-ASCII
      bytedance::bolt::functions::stringImpl::
          pad<true /*lpad*/, false /*IsAscii*/>(
              lpadOutput, StringView(string), size, StringView(padString));
      bytedance::bolt::functions::stringImpl::
          pad<false /*lpad*/, false /*IsAscii*/>(
              rpadOutput, StringView(string), size, StringView(padString));
    }

    ASSERT_EQ(
        StringView(lpadOutput.data(), lpadOutput.size()),
        StringView(expectedLpadResult));
    ASSERT_EQ(
        StringView(rpadOutput.data(), rpadOutput.size()),
        StringView(expectedRpadResult));
  };

  auto runTestUserError = [](const std::string& string,
                             const int64_t size,
                             const std::string& padString) {
    core::StringWriter output;

    EXPECT_THROW(
        (bytedance::bolt::functions::stringImpl::pad<true, true>(
            output, StringView(string), size, StringView(padString))),
        BoltUserError);
  };

  // ASCII string with various values for size and padString
  runTest("text", 5, "x", "xtext", "textx");
  runTest("text", 4, "x", "text", "text");
  runTest("text", 6, "xy", "xytext", "textxy");
  runTest("text", 7, "xy", "xyxtext", "textxyx");
  runTest("text", 9, "xyz", "xyzxytext", "textxyzxy");
  // Non-ASCII string with various values for size and padString
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      10,
      "\u671B",
      "\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      11,
      "\u671B",
      "\u671B\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B\u671B");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      12,
      "\u5E0C\u671B",
      "\u5E0C\u671B\u5E0C\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      13,
      "\u5E0C\u671B",
      "\u5E0C\u671B\u5E0C\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C\u671B");
  // Empty string
  runTest("", 3, "a", "aaa", "aaa");
  // Truncating string
  runTest("abc", 0, "e", "", "");
  runTest("text", 3, "xy", "tex", "tex");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      5,
      "\u671B",
      "\u4FE1\u5FF5 \u7231 ",
      "\u4FE1\u5FF5 \u7231 ");

  // Empty padString
  runTestUserError("text", 10, "");
  // size outside the allowed range
  runTestUserError("text", -1, "a");
  runTestUserError(
      "text", ((int64_t)std::numeric_limits<int32_t>::max()) + 1, "a");
  // Additional tests with bad unicode bytes.
  runTest("abcd\xff \xff ef", 6, "0", "abcd\xff ", "abcd\xff ");
  runTest(
      "abcd\xff \xff ef", 11, "0", "0abcd\xff \xff ef", "abcd\xff \xff ef0");
  runTest("abcd\xff ef", 6, "0", "abcd\xff ", "abcd\xff ");
}

// Make sure that utf8proc_codepoint returns invalid codepoint (-1) for
// incomplete character of length>1.
TEST_F(StringImplTest, utf8proc_codepoint) {
  int size;

  std::string twoBytesChar = "\xdd\x81";
  EXPECT_EQ(
      utf8proc_codepoint(twoBytesChar.data(), twoBytesChar.data() + 1, &size),
      -1);
  EXPECT_NE(
      utf8proc_codepoint(twoBytesChar.data(), twoBytesChar.data() + 2, &size),
      -1);
  EXPECT_EQ(size, 2);

  std::string threeBytesChar = "\xe0\xa4\x86";
  for (int i = 1; i <= 2; i++) {
    EXPECT_EQ(
        utf8proc_codepoint(
            threeBytesChar.data(), threeBytesChar.data() + i, &size),
        -1);
  }

  EXPECT_NE(
      utf8proc_codepoint(
          threeBytesChar.data(), threeBytesChar.data() + 3, &size),
      -1);
  EXPECT_EQ(size, 3);

  std::string fourBytesChar = "\xf0\x92\x80\x85";
  for (int i = 1; i <= 3; i++) {
    EXPECT_EQ(
        utf8proc_codepoint(
            fourBytesChar.data(), fourBytesChar.data() + i, &size),
        -1);
  }
  EXPECT_NE(
      utf8proc_codepoint(fourBytesChar.data(), fourBytesChar.data() + 4, &size),
      -1);
  EXPECT_EQ(size, 4);
}

TEST_F(StringImplTest, isUnicodeWhiteSpace) {
  EXPECT_FALSE(isUnicodeWhiteSpace(-1));
}

TEST_F(StringImplTest, findNthInstanceByteIndexFromStart) {
  std::string_view str = "This is a simple test string used for testing.";
  std::string_view subStr = "test";

  // Test that the first instance at position 10 is found
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str, subStr, 1), 17);

  // Test that the substring not found returns -1
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str, "not found", 1), -1);

  // Test that the third instance (which doesn't exist) returns -1
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str, subStr, 3), -1);

  // Test for overlapping substrings
  EXPECT_EQ(findNthInstanceByteIndexFromStart("aaaaaa", "aaa", 2), 1);

  // Test with startPosition set beyond the string size
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str, subStr, 1, str.size()), -1);

  std::string_view str1 = "abc abc abc";
  std::string_view subStr1 = "abc";

  // Test finding the second instance with startPosition
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str1, subStr1, 2, 2), 8);

  // Test finding the second instance with startPosition after the first
  // occurrence
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str1, subStr1, 2, 4), 8);

  // Test finding the second instance with startPosition after the second
  // occurrence
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str1, subStr1, 2, 5), -1);

  // Test with string starting with substring
  EXPECT_EQ(findNthInstanceByteIndexFromStart("test test test", "test", 2), 5);

  // Test with empty string and non-empty substring
  EXPECT_EQ(findNthInstanceByteIndexFromStart("", "test", 1), -1);

  // Test with non-empty string and empty substring
  EXPECT_EQ(findNthInstanceByteIndexFromStart("test", "", 1), 0);
  EXPECT_EQ(findNthInstanceByteIndexFromStart("abc", "", 1, 3), 0);

  // Test with both string and substring being empty
  EXPECT_EQ(findNthInstanceByteIndexFromStart("", "", 1), 0);

  // Test that an instance count of 0 asserts
  // The actual assertion test depends on the gtest configuration and might need
  // to be adjusted EXPECT_DEATH(findNthInstanceByteIndexFromStart(str, subStr,
  // 0), ".*");
}

TEST_F(StringImplTest, findNthInstanceCharIndexFromStart) {
  std::string_view str = "This is a simple test string used for testing.";
  std::string_view subStr = "test";

  // Test that the first instance at char position 10 is found
  EXPECT_EQ(findNthInstanceCharIndexFromStart(str, subStr, 1), 17);

  // Test that the substring not found returns -1
  EXPECT_EQ(findNthInstanceCharIndexFromStart(str, "not found", 1), -1);

  // Test that the third instance (which doesn't exist) returns -1
  EXPECT_EQ(findNthInstanceCharIndexFromStart(str, subStr, 3), -1);

  // Test for overlapping substrings
  EXPECT_EQ(findNthInstanceCharIndexFromStart("aaaaaa", "aaa", 2), 1);

  // Test with startPosition set beyond the string size
  EXPECT_EQ(findNthInstanceCharIndexFromStart(str, subStr, 1, str.size()), -1);

  // Test that an instance count of 0 asserts
  // EXPECT_DEATH(findNthInstanceCharIndexFromStart(str, subStr, 0), ".*");

  std::string_view str1 = "abc abc abc";
  std::string_view subStr1 = "abc";

  // Test finding the second instance with startPosition
  EXPECT_EQ(findNthInstanceCharIndexFromStart(str1, subStr1, 2, 2), 8);

  // Test finding the second instance with startPosition after the first
  // occurrence
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str1, subStr1, 2, 4), 8);

  // Test finding the second instance with startPosition after the second
  // occurrence
  EXPECT_EQ(findNthInstanceByteIndexFromStart(str1, subStr1, 2, 5), -1);

  // Test with string starting with substring
  EXPECT_EQ(findNthInstanceCharIndexFromStart("test test test", "test", 2), 5);

  // Test with empty string and non-empty substring
  EXPECT_EQ(findNthInstanceCharIndexFromStart("", "test", 1), -1);

  // Test with non-empty string and empty substring
  EXPECT_EQ(findNthInstanceCharIndexFromStart("test", "", 1), 0);

  // Test with both string and substring being empty
  EXPECT_EQ(findNthInstanceCharIndexFromStart("", "", 1), 0);

  // Test searching for a Unicode substring starting in the middle of a
  // multibyte character
  std::string_view strUnicodeMidChar = u8sv(u8"Test 字符串 with 中文");
  std::string_view subStrUnicodeMidChar = u8sv(u8"中文");
  EXPECT_EQ(
      findNthInstanceCharIndexFromStart(
          strUnicodeMidChar, subStrUnicodeMidChar, 1),
      14);

  std::string_view subStrUnicodeMidChar1 = u8sv(u8"字符串");
  EXPECT_EQ(
      findNthInstanceCharIndexFromStart(
          strUnicodeMidChar, subStrUnicodeMidChar1, 1),
      5);

  // Test with a longer Unicode substring
  std::string_view longSubStrUnicode = u8sv(u8"字符串 with 中文");
  EXPECT_EQ(
      findNthInstanceCharIndexFromStart(
          strUnicodeMidChar, longSubStrUnicode, 1),
      5);

  std::string_view longSubStrUnicode1 =
      u8sv(u8"Test 字符串 with 中文 中文 中文 中文");
  EXPECT_EQ(
      findNthInstanceCharIndexFromStart(
          strUnicodeMidChar, longSubStrUnicode1, 1),
      -1);

  // Test with a Unicode substring that is not found
  std::string_view nonExistingSubStrUnicode = u8sv(u8"不存在");
  EXPECT_EQ(
      findNthInstanceCharIndexFromStart(
          strUnicodeMidChar, nonExistingSubStrUnicode, 1),
      -1);

  // Test with Unicode string containing same character with different case
  std::string_view caseStrUnicode = u8sv(u8"Test 测试 TEST 测试");
  std::string_view caseSubStrUnicode = u8sv(u8"测试");
  EXPECT_EQ(
      findNthInstanceCharIndexFromStart(caseStrUnicode, caseSubStrUnicode, 2),
      13);
}

TEST_F(StringImplTest, EmptyInput) {
  std::string input = "";
  EXPECT_EQ(replaceNonBMPWithUnicodeSequence(input), "");
}

TEST_F(StringImplTest, AsciiOnlyInput) {
  std::string input = "Hello, world!";
  EXPECT_EQ(replaceNonBMPWithUnicodeSequence(input), "Hello, world!");
}

TEST_F(StringImplTest, BMPCharactersOnly) {
  std::string input =
      u8str(u8"Hello, 世界!"); // Assuming the file is encoded in UTF-8.
  EXPECT_EQ(replaceNonBMPWithUnicodeSequence(input), "Hello, 世界!");
}

TEST_F(StringImplTest, SingleNonBMPCharacter) {
  std::string input = u8str(u8"😊"); // U+1F60A
  EXPECT_EQ(replaceNonBMPWithUnicodeSequence(input), "\\uD83D\\uDE0A");
}

TEST_F(StringImplTest, MultipleNonBMPCharacters) {
  std::string input = u8str(u8"🌍🚀🌕"); // U+1F30D U+1F680 U+1F315
  EXPECT_EQ(
      replaceNonBMPWithUnicodeSequence(input),
      "\\uD83C\\uDF0D\\uD83D\\uDE80\\uD83C\\uDF15");
}

TEST_F(StringImplTest, NonBMPCharacterAtStart) {
  std::string input = u8str(u8"🎉Party!");
  EXPECT_EQ(replaceNonBMPWithUnicodeSequence(input), "\\uD83C\\uDF89Party!");
}

TEST_F(StringImplTest, NonBMPCharacterInMiddle) {
  std::string input = u8str(u8"Party🎉Time!");
  EXPECT_EQ(
      replaceNonBMPWithUnicodeSequence(input), "Party\\uD83C\\uDF89Time!");
}

TEST_F(StringImplTest, NonBMPCharacterAtEnd) {
  std::string input = u8str(u8"Party🎉");
  EXPECT_EQ(replaceNonBMPWithUnicodeSequence(input), "Party\\uD83C\\uDF89");
  std::string input2 = u8str(u8"玫瑰🌹用雪做的一支玫瑰花 𠕇");
  EXPECT_EQ(
      replaceNonBMPWithUnicodeSequence(input2),
      "玫瑰\\uD83C\\uDF39用雪做的一支玫瑰花 \\uD841\\uDD47");
}

TEST_F(StringImplTest, LargeInput) {
  std::string largeInput(10000, 'a'); // 10000个 'a' 字符
  largeInput += u8str(u8"😊"); // 在末尾追加一个非BMP字符
  std::string expectedOutput = largeInput.substr(0, 10000) + "\\uD83D\\uDE0A";
  EXPECT_EQ(replaceNonBMPWithUnicodeSequence(largeInput), expectedOutput);
}

TEST_F(StringImplTest, EmptyString) {
  EXPECT_EQ(javaStyleSplit("", ",", -1), std::vector<std::string_view>({""}));
  EXPECT_EQ(javaStyleSplit("", ",", 0), std::vector<std::string_view>({""}));
  EXPECT_EQ(javaStyleSplit("", ",", 1), std::vector<std::string_view>({""}));
}

TEST_F(StringImplTest, StringWithoutDelimiter) {
  EXPECT_EQ(
      javaStyleSplit("test", ",", -1), std::vector<std::string_view>({"test"}));
  EXPECT_EQ(
      javaStyleSplit("test", ",", 0), std::vector<std::string_view>({"test"}));
  EXPECT_EQ(
      javaStyleSplit("test", ",", 1), std::vector<std::string_view>({"test"}));
}

TEST_F(StringImplTest, StringWithDelimiter) {
  EXPECT_EQ(
      javaStyleSplit("a,b,c", ",", -1),
      std::vector<std::string_view>({"a", "b", "c"}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c", ",", 0),
      std::vector<std::string_view>({"a", "b", "c"}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c", ",", 2),
      std::vector<std::string_view>({"a", "b,c"}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c", ",", 4),
      std::vector<std::string_view>({"a", "b", "c"}));
}

TEST_F(StringImplTest, DelimiterAtStartOrEnd) {
  EXPECT_EQ(
      javaStyleSplit(",a,b,c,", ",", -1),
      std::vector<std::string_view>({"", "a", "b", "c", ""}));
  EXPECT_EQ(
      javaStyleSplit(",a,b,c,", ",", 0),
      std::vector<std::string_view>({"", "a", "b", "c"}));
  EXPECT_EQ(
      javaStyleSplit(",a,b,c,", ",", 6),
      std::vector<std::string_view>({"", "a", "b", "c", ""}));

  EXPECT_EQ(
      javaStyleSplit(",a,b,c,", ",", 1),
      std::vector<std::string_view>({",a,b,c,"}));
}

TEST_F(StringImplTest, SpecialCharactersAndEscapeSequences) {
  EXPECT_EQ(
      javaStyleSplit("a\\,b,c", "\\,", -1),
      std::vector<std::string_view>({"a\\", "b", "c"}));
  EXPECT_EQ(
      javaStyleSplit("word1 word2\tword3\nword4", "\\s+", -1),
      std::vector<std::string_view>({"word1", "word2", "word3", "word4"}));
}

TEST_F(StringImplTest, MultipleDelimitersAtEnd) {
  EXPECT_EQ(
      javaStyleSplit("a,b,c,,", ",", -1),
      std::vector<std::string_view>({"a", "b", "c", "", ""}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c,,", ",", 0),
      std::vector<std::string_view>({"a", "b", "c"}));
}

TEST_F(StringImplTest, EmptyDelimiter) {
  EXPECT_EQ(
      javaStyleSplit("a,b,c", "", -1),
      std::vector<std::string_view>({"a", ",", "b", ",", "c", ""}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c", "", 0),
      std::vector<std::string_view>({"a", ",", "b", ",", "c"}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c", "", 1), std::vector<std::string_view>({"a,b,c"}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c", "", 2),
      std::vector<std::string_view>({"a", ",b,c"}));
  EXPECT_EQ(
      javaStyleSplit("a,b,c", "", 10),
      std::vector<std::string_view>({"a", ",", "b", ",", "c", ""}));
}

// Different 'limit' parameter values
TEST_F(StringImplTest, StringWithLimitParameter) {
  EXPECT_EQ(
      javaStyleSplit("one,two,three", ",", 2),
      std::vector<std::string_view>({"one", "two,three"}));

  EXPECT_EQ(
      javaStyleSplit("one,two,three", ",", -1),
      std::vector<std::string_view>({"one", "two", "three"}));

  EXPECT_EQ(
      javaStyleSplit("one,two,three", ",", 0),
      std::vector<std::string_view>({"one", "two", "three"}));
}

// Special characters like dot (which needs escaping) and space
TEST_F(StringImplTest, StringWithSpecialCharacters) {
  EXPECT_EQ(
      javaStyleSplit("one.two three", "\\.", -1),
      std::vector<std::string_view>({"one", "two three"}));
  EXPECT_EQ(
      javaStyleSplit("one.two three", " ", -1),
      std::vector<std::string_view>({"one.two", "three"}));
}

// Treatment of regular expression metacharacters
TEST_F(StringImplTest, StringWithRegExMetacharacters) {
  EXPECT_EQ(
      javaStyleSplit("one*two+three?four", "\\*", -1),
      std::vector<std::string_view>({"one", "two+three?four"}));
}

// Mixed Chinese, English, and emoji characters
TEST_F(StringImplTest, StringWithMixedCharacters) {
  EXPECT_EQ(
      javaStyleSplit(u8sv(u8"hello,你好,😊"), ",", -1),
      std::vector<std::string_view>(
          {u8sv(u8"hello"), u8sv(u8"你好"), u8sv(u8"😊")}));
}

// Delimiter is a complex regex pattern
TEST_F(StringImplTest, StringWithComplexRegexPattern) {
  EXPECT_EQ(
      javaStyleSplit("one42two42three", "\\d+", -1),
      std::vector<std::string_view>({"one", "two", "three"}));
}

// String contains escape sequences
TEST_F(StringImplTest, StringWithEscapeSequences) {
  EXPECT_EQ(
      javaStyleSplit("one\ntwo\tthree", "\n", -1),
      std::vector<std::string_view>({"one", "two\tthree"}));
  EXPECT_EQ(
      javaStyleSplit("one\ntwo\tthree", "\t", -1),
      std::vector<std::string_view>({"one\ntwo", "three"}));
}

// UTF-8 encoded multibyte characters
TEST_F(StringImplTest, StringWithUTF8MultibyteCharacters) {
  EXPECT_EQ(
      javaStyleSplit(u8sv(u8"α,β,γ"), ",", -1),
      std::vector<std::string_view>({u8sv(u8"α"), u8sv(u8"β"), u8sv(u8"γ")}));
}

// Newline character as delimiter
TEST_F(StringImplTest, StringWithNewlineDelimiter) {
  EXPECT_EQ(
      javaStyleSplit("one\ntwo\nthree", "\n", -1),
      std::vector<std::string_view>({"one", "two", "three"}));
}

// String containing only whitespace characters
TEST_F(StringImplTest, StringWithWhitespacesOnly) {
  EXPECT_EQ(
      javaStyleSplit("   \t\n", "\\s", -1),
      std::vector<std::string_view>({"", "", "", "", "", ""}));
}

// String and delimiter are non-ASCII characters
TEST_F(StringImplTest, StringAndDelimiterNonASCII) {
  EXPECT_EQ(
      javaStyleSplit(u8sv(u8"你好✓世界✓"), u8sv(u8"✓"), -1),
      std::vector<std::string_view>({u8sv(u8"你好"), u8sv(u8"世界"), ""}));
}

// Delimiter is a string, not a single character
TEST_F(StringImplTest, StringWithDelimiterString) {
  EXPECT_EQ(
      javaStyleSplit("oneANDtwoANDthree", "AND", -1),
      std::vector<std::string_view>({"one", "two", "three"}));
}

// Delimiter string contains regex metacharacters
TEST_F(StringImplTest, StringWithRegexMetacharactersInDelimiter) {
  EXPECT_EQ(
      javaStyleSplit("one*+?two*+?three", "\\*\\+\\?", -1),
      std::vector<std::string_view>({"one", "two", "three"}));
}

// Long string and long delimiter pattern
TEST_F(StringImplTest, LongStringAndLongDelimiterPattern) {
  EXPECT_EQ(
      javaStyleSplit("longstringwithnodelimiter", "LONGDELIMITER", -1),
      std::vector<std::string_view>({"longstringwithnodelimiter"}));
}

// String contains regex boundary matchers
TEST_F(StringImplTest, StringWithRegexBoundaryMatchers) {
  EXPECT_EQ(
      javaStyleSplit("^one$^two$^three$", "\\$", -1),
      std::vector<std::string_view>({"^one", "^two", "^three", ""}));
}

TEST_F(StringImplTest, isAlpha) {
  EXPECT_FALSE(isAlpha(std::string_view("1")));
  EXPECT_FALSE(isAlpha(std::string_view("123")));
  EXPECT_FALSE(isAlpha(std::string_view("2")));
  EXPECT_FALSE(isAlpha(std::string_view("11.4445")));
  EXPECT_FALSE(isAlpha(std::string_view("3")));
  EXPECT_FALSE(isAlpha(std::string_view("abc123")));
  EXPECT_FALSE(isAlpha(std::string_view("ABC123")));
  EXPECT_FALSE(isAlpha(std::string_view("abc123xyz")));
  EXPECT_FALSE(isAlpha(std::string_view("")));
  EXPECT_FALSE(isAlpha(std::string_view(".")));
  EXPECT_FALSE(isAlpha(std::string_view("١٢٣٤٥"))); // Arabic numbers

  EXPECT_TRUE(isAlpha(std::string_view("a")));
  EXPECT_TRUE(isAlpha(std::string_view("A")));
  EXPECT_TRUE(isAlpha(std::string_view("z")));
  EXPECT_TRUE(isAlpha(std::string_view("Z")));
  EXPECT_TRUE(isAlpha(std::string_view("abc")));
  EXPECT_TRUE(isAlpha(std::string_view("ABC")));
  EXPECT_TRUE(isAlpha(std::string_view("xyz")));
  EXPECT_TRUE(isAlpha(std::string_view("XYZ")));
  EXPECT_TRUE(isAlpha(std::string_view("abcxyz")));
  EXPECT_TRUE(isAlpha(std::string_view("ABCXYZ")));
  EXPECT_TRUE(isAlpha(std::string_view("Привет"))); // Russian hello

  // test unicode
  EXPECT_TRUE(isAlpha(std::string_view("你好")));
  EXPECT_TRUE(isAlpha(std::string_view("您")));
  EXPECT_TRUE(isAlpha(std::string_view("您a")));
  EXPECT_TRUE(isAlpha(std::string_view("a您")));
  // test emoji
  EXPECT_FALSE(isAlpha(std::string_view("😊")));
  EXPECT_FALSE(isAlpha(std::string_view("😊a")));
  EXPECT_FALSE(isAlpha(std::string_view("a😊a")));
}

TEST_F(StringImplTest, isDigit) {
  EXPECT_TRUE(isDigit(std::string_view("1")));
  EXPECT_TRUE(isDigit(std::string_view("123")));
  EXPECT_TRUE(isDigit(std::string_view("2")));
  EXPECT_TRUE(isDigit(std::string_view("१२३")));
  EXPECT_TRUE(isDigit(std::string_view("١٢٣٤٥"))); // Arabic numbers

  EXPECT_FALSE(isDigit(std::string_view("11.4445")));
  EXPECT_FALSE(isDigit(std::string_view("a")));
  EXPECT_FALSE(isDigit(std::string_view("A")));
  EXPECT_FALSE(isDigit(std::string_view("abc123")));
  EXPECT_FALSE(isDigit(std::string_view("123abc")));
  EXPECT_FALSE(isDigit(std::string_view("a1a")));
  EXPECT_FALSE(isDigit(std::string_view("1a1")));
  EXPECT_FALSE(isDigit(std::string_view("")));
  EXPECT_FALSE(isDigit(std::string_view("Привет"))); // Russian hello
}

TEST_F(StringImplTest, isDecimal) {
  EXPECT_TRUE(isDecimal(std::string_view("1")));
  EXPECT_TRUE(isDecimal(std::string_view("123")));
  EXPECT_TRUE(isDecimal(std::string_view("2")));
  EXPECT_TRUE(isDecimal(std::string_view("11.4445")));
  EXPECT_FALSE(isDecimal(std::string_view("a")));
  EXPECT_FALSE(isDecimal(std::string_view("A")));
  EXPECT_FALSE(isDecimal(std::string_view("abc123")));
  EXPECT_FALSE(isDecimal(std::string_view("123abc")));
  EXPECT_FALSE(isDecimal(std::string_view("a1a")));
  EXPECT_FALSE(isDecimal(std::string_view("1a1")));
  EXPECT_FALSE(isDecimal(std::string_view("")));
}

TEST_F(StringImplTest, toLower) {
  EXPECT_EQ(toLower<true>(std::string_view("1")), "1");
  EXPECT_EQ(toLower<true>(std::string_view("11.4445")), "11.4445");
  EXPECT_EQ(toLower<true>(std::string_view("a")), "a");
  EXPECT_EQ(toLower<true>(std::string_view("A")), "a");
  EXPECT_EQ(toLower<true>(std::string_view("HELLO WORLD")), "hello world");

  EXPECT_EQ(toLower<true>(std::string_view("")), "");
  // Basic tests.
  EXPECT_EQ(toLower<true>(std::string_view("xyz")), "xyz");
  EXPECT_EQ(toLower<true>(std::string_view("abcd")), "abcd");
  // Advanced tests.
  EXPECT_EQ(toLower<true>(std::string_view("你好")), "你好");
  EXPECT_EQ(toLower<true>(std::string_view("Γειά")), "γειά");
  EXPECT_EQ(toLower<true>(std::string_view("Здраво")), "здраво");
  // Case variation.
  EXPECT_EQ(toLower<true>(std::string_view("xYz")), "xyz");
  EXPECT_EQ(toLower<true>(std::string_view("AbCd")), "abcd");
  // Accent variation.
  EXPECT_EQ(toLower<true>(std::string_view("äbć")), "äbć");
  EXPECT_EQ(toLower<true>(std::string_view("AbĆd")), "abćd");
  EXPECT_EQ(toLower<true>(std::string_view("aBcΔ")), "abcδ");
  // One-to-many case mapping (e.g. Turkish dotted I).
  EXPECT_EQ(toLower<true>(std::string_view("i\u0307")), "i\u0307");
  EXPECT_EQ(toLower<true>(std::string_view("I\u0307")), "i\u0307");
  EXPECT_EQ(toLower<true>(std::string_view("İ")), "i\u0307");
  EXPECT_EQ(toLower<true>(std::string_view("İİİ")), "i\u0307i\u0307i\u0307");
  EXPECT_EQ(toLower<true>(std::string_view("İiIi\u0307")), "i\u0307iii\u0307");
  EXPECT_EQ(toLower<true>(std::string_view("İoDiNe")), "i\u0307odine");
  EXPECT_EQ(toLower<true>(std::string_view("Abi\u0307o12")), "abi\u0307o12");
  // Conditional case mapping (e.g. Greek sigmas).
  EXPECT_EQ(toLower<true>(std::string_view("ς")), "ς");
  EXPECT_EQ(toLower<true>(std::string_view("σ")), "σ");
  EXPECT_EQ(toLower<true>(std::string_view("Σ")), "σ");
  EXPECT_EQ(toLower<true>(std::string_view("ςΑΛΑΤΑ")), "ςαλατα");
  EXPECT_EQ(toLower<true>(std::string_view("σΑΛΑΤΑ")), "σαλατα");
  EXPECT_EQ(toLower<true>(std::string_view("ΣΑΛΑΤΑ")), "σαλατα");
  EXPECT_EQ(toLower<true>(std::string_view("ΘΑΛΑΣΣΙΝΟς")), "θαλασσινος");
  EXPECT_EQ(toLower<true>(std::string_view("ΘΑΛΑΣΣΙΝΟσ")), "θαλασσινοσ");
  EXPECT_EQ(toLower<true>(std::string_view("ΘΑΛΑΣΣΙΝΟΣ")), "θαλασσινος");
  EXPECT_EQ(
      toLower<true>(std::string_view("ΑΕΡΑΣ, ΜΥΣΤΗΡΙΟ, ΩΡΑΙΟ")),
      "αερας, μυστηριο, ωραιο");
  EXPECT_EQ(
      toLower<true>(std::string_view("ΕΠΕΙΔΗ Η ΑΝΑΓΝΩΡΙΣΗ ΤΗΣ ΑΞΙΟΠΡΕΠΕΙΑΣ")),
      "επειδη η αναγνωριση της αξιοπρεπειας");
  if constexpr (kSparkCompatible) {
    // Exercise sparse and repeated Sigma adjustments, including more offsets
    // than fit in the inline small-vector buffer.
    EXPECT_EQ(
        (toLower<false, kSparkCompatible>(std::string_view("AΣ8B"))), "aσ8b");
    EXPECT_EQ(
        (toLower<false, kSparkCompatible>(std::string_view("AΣ AΣ AΣ AΣ AΣ"))),
        "aς aς aς aς aς");
    EXPECT_EQ((toLower<false, kSparkCompatible>(std::string_view("AΣ"))), "aς");
  }
  // Surrogate pairs.
  EXPECT_EQ(toLower<true>(std::string_view("a🙃b🙃c")), "a🙃b🙃c");
  EXPECT_EQ(toLower<true>(std::string_view("😀😆😃😄😄😆")), "😀😆😃😄😄😆");
  EXPECT_EQ(toLower<true>(std::string_view("𐐅")), "𐐭");
  EXPECT_EQ(toLower<true>(std::string_view("𝔸")), "𝔸");
}

TEST_F(StringImplTest, toTitle) {
  EXPECT_EQ(toTitle(std::string_view("ʻcAt! ʻeTc.")), "ʻCat! ʻEtc.");
  EXPECT_EQ(toTitle(std::string_view("aBc ABc")), "Abc Abc");
  EXPECT_EQ(toTitle(std::string_view("a")), "A");
  EXPECT_EQ(toTitle(std::string_view("")), "");
  EXPECT_EQ(toTitle(std::string_view("abcde")), "Abcde");
  EXPECT_EQ(toTitle(std::string_view("AbCdE")), "Abcde");
  EXPECT_EQ(toTitle(std::string_view("aBcDe")), "Abcde");
  EXPECT_EQ(toTitle(std::string_view("ABCDE")), "Abcde");
  EXPECT_EQ(toTitle(std::string_view("σ")), "Σ");
  EXPECT_EQ(toTitle(std::string_view("ς")), "Σ");
  EXPECT_EQ(toTitle(std::string_view("Σ")), "Σ");
  EXPECT_EQ(toTitle(std::string_view("ΣΑΛΑΤΑ")), "Σαλατα");
  EXPECT_EQ(toTitle(std::string_view("σαλατα")), "Σαλατα");
  EXPECT_EQ(toTitle(std::string_view("ςαλατα")), "Σαλατα");
  EXPECT_EQ(toTitle(std::string_view("ΘΑΛΑΣΣΙΝΟΣ")), "Θαλασσινος");
  EXPECT_EQ(toTitle(std::string_view("θαλασσινοσ")), "Θαλασσινοσ");
  EXPECT_EQ(toTitle(std::string_view("θαλασσινος")), "Θαλασσινος");

  // Advanced tests.
  EXPECT_EQ(toTitle(std::string_view("aBćDe")), "Abćde");
  EXPECT_EQ(toTitle(std::string_view("ab世De")), "Ab世De");
  EXPECT_EQ(toTitle(std::string_view("äbćδe")), "Äbćδe");
  EXPECT_EQ(toTitle(std::string_view("ÄBĆΔE")), "Äbćδe");
  // Case-variable character length
  EXPECT_EQ(toTitle(std::string_view("İo")), "İo");
  EXPECT_EQ(toTitle(std::string_view("i\u0307o")), "I\u0307o");
  // Different possible word boundaries
  EXPECT_EQ(toTitle(std::string_view("aB 世 de")), "Ab 世 De");
  // One-to-many case mapping (e.g. Turkish dotted I).
  EXPECT_EQ(toTitle(std::string_view("İ")), "İ");
  EXPECT_EQ(toTitle(std::string_view("I\u0307")), "I\u0307");
  EXPECT_EQ(toTitle(std::string_view("İonic")), "İonic");
  EXPECT_EQ(toTitle(std::string_view("i\u0307onic")), "I\u0307onic");
  EXPECT_EQ(toTitle(std::string_view("FIDELİO")), "Fideli\u0307o");
  // Surrogate pairs.
  EXPECT_EQ(toTitle(std::string_view("a🙃B🙃c")), "A🙃B🙃C");
  EXPECT_EQ(toTitle(std::string_view("😄 😆")), "😄 😆");
  EXPECT_EQ(toTitle(std::string_view("😀😆😃😄")), "😀😆😃😄");
  EXPECT_EQ(toTitle(std::string_view("𝔸")), "𝔸");
  EXPECT_EQ(toTitle(std::string_view("𐐅")), "𐐅");
  EXPECT_EQ(toTitle(std::string_view("𐐭")), "𐐅");
  EXPECT_EQ(toTitle(std::string_view("𐐭𝔸")), "𐐅𝔸");
  // Different possible word boundaries.
  EXPECT_EQ(toTitle(std::string_view("a.b,c")), "A.b,C");
  EXPECT_EQ(toTitle(std::string_view("a. b-c")), "A. B-C");
  EXPECT_EQ(toTitle(std::string_view("a?b世c")), "A?B世C");
  // Titlecase characters that are different from uppercase characters.
  EXPECT_EQ(toTitle(std::string_view("ǳǱǲ")), "ǲǳǳ");
  EXPECT_EQ(
      toTitle(std::string_view("ß ﬁ ﬃ ﬀ ﬆ ΣΗΜΕΡςΙΝΟΣ ΑΣΗΜΕΝΙΟΣ İOTA")),
      "Ss Fi Ffi Ff St Σημερςινος Ασημενιος İota");
}

TEST_F(StringImplTest, md5_radix) {
  std::string out;
  out.resize(64);
  bool ret = md5_radix<std::string, std::string>(out, std::string("a"), 16);
  EXPECT_TRUE(ret);
  EXPECT_EQ(out, "0cc175b9c0f1b6a831c399e269772661");
  ret = md5_radix<std::string, std::string>(out, std::string("a"), 10);
  EXPECT_TRUE(ret);
  std::string out10 = "16955237001963240173058271559858";
  // just for compatible with the previous code, we need to resize the output
  // string to 38 characters.
  out10.resize(38);
  EXPECT_EQ(out10, out10);
}
