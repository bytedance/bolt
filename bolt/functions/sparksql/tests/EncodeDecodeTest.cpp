/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <optional>
#include <string>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class EncodeDecodeTest : public SparkFunctionBaseTest {
 protected:
  // Builds the two-column input explicitly so the first column's type can be
  // VARCHAR for encode and VARBINARY for decode.
  std::optional<std::string> evalTwoArg(
      const std::string& expr,
      const std::optional<std::string>& value,
      const std::optional<std::string>& charset,
      const TypePtr& valueType) {
    auto valueVector = makeNullableFlatVector<std::string>({value}, valueType);
    auto charsetVector = makeNullableFlatVector<std::string>({charset});
    return evaluateOnce<std::string>(
        expr, makeRowVector({valueVector, charsetVector}));
  }

  std::optional<std::string> encode(
      const std::optional<std::string>& value,
      const std::optional<std::string>& charset) {
    return evalTwoArg("encode(c0, c1)", value, charset, VARCHAR());
  }

  std::optional<std::string> decode(
      const std::optional<std::string>& value,
      const std::optional<std::string>& charset) {
    return evalTwoArg("decode(c0, c1)", value, charset, VARBINARY());
  }

  // Round-trips a string through encode then decode; the result must equal
  // the input for every charset that can represent it.
  std::optional<std::string> roundTrip(
      const std::string& value,
      const std::string& charset) {
    auto encoded = encode(value, charset);
    if (!encoded.has_value()) {
      return std::nullopt;
    }
    return decode(encoded, charset);
  }
};

TEST_F(EncodeDecodeTest, utf8) {
  // For UTF-8 the encoded bytes are the input's own bytes.
  EXPECT_EQ(encode("abc", "UTF-8"), "abc");
  EXPECT_EQ(encode("", "UTF-8"), "");
  EXPECT_EQ(encode("\u00fcber", "UTF-8"), "\u00fcber");
  EXPECT_EQ(decode("abc", "UTF-8"), "abc");
  EXPECT_EQ(decode("\u00fcber", "UTF-8"), "\u00fcber");
}

TEST_F(EncodeDecodeTest, asciiRoundTrip) {
  EXPECT_EQ(roundTrip("abc", "US-ASCII"), "abc");
  EXPECT_EQ(roundTrip("hello world", "US-ASCII"), "hello world");
  EXPECT_EQ(roundTrip("", "US-ASCII"), "");
}

TEST_F(EncodeDecodeTest, latin1) {
  // 'é' is a single byte 0xE9 in ISO-8859-1 but two bytes in UTF-8.
  EXPECT_EQ(
      encode("\u00fcber", "ISO-8859-1"),
      std::string("\xFC"
                  "ber"));
  EXPECT_EQ(roundTrip("\u00fcber", "ISO-8859-1"), "\u00fcber");
}

TEST_F(EncodeDecodeTest, utf16Variants) {
  // UTF-16BE/LE differ in byte order and emit no BOM.
  EXPECT_EQ(
      encode("ab", "UTF-16BE"),
      std::string(
          "\x00"
          "a\x00"
          "b",
          4));
  EXPECT_EQ(
      encode("ab", "UTF-16LE"),
      std::string(
          "a\x00"
          "b\x00",
          4));
  // Plain UTF-16 emits a big-endian BOM followed by big-endian code units,
  // matching Java's Charset("UTF-16"). ICU's own "UTF-16" converter would emit
  // FF FE and little-endian units on a little-endian host, so the
  // implementation maps this name to ICU's "UnicodeBig" when encoding.
  EXPECT_EQ(
      encode("ab", "UTF-16"),
      std::string(
          "\xFE\xFF\x00"
          "a\x00"
          "b",
          6));

  EXPECT_EQ(roundTrip("ab", "UTF-16BE"), "ab");
  EXPECT_EQ(roundTrip("ab", "UTF-16LE"), "ab");
  EXPECT_EQ(roundTrip("ab", "UTF-16"), "ab");
}

TEST_F(EncodeDecodeTest, utf16DecodeHonoursEitherBom) {
  // Java's UTF-16 decoder accepts either BOM and assumes big-endian when none
  // is present. Decoding must therefore keep ICU's "UTF-16" converter: the
  // "UnicodeBig" used for encoding would mis-decode little-endian input.
  EXPECT_EQ(
      decode(
          std::string(
              "\xFE\xFF\x00"
              "a\x00"
              "b",
              6),
          "UTF-16"),
      "ab");
  EXPECT_EQ(
      decode(
          std::string(
              "\xFF\xFE"
              "a\x00"
              "b\x00",
              6),
          "UTF-16"),
      "ab");
  EXPECT_EQ(
      decode(
          std::string(
              "\x00"
              "a\x00"
              "b",
              4),
          "UTF-16"),
      "ab");
}

TEST_F(EncodeDecodeTest, multiByteRoundTrip) {
  for (const auto& charset : {"UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"}) {
    EXPECT_EQ(roundTrip("\u4f60\u597d", charset), "\u4f60\u597d")
        << "charset: " << charset;
  }
}

TEST_F(EncodeDecodeTest, unmappableCharacterBecomesQuestionMark) {
  // Java's String.getBytes substitutes '?' rather than throwing. ICU's own
  // default is 0x1A, so this asserts the Spark-compatible byte.
  EXPECT_EQ(encode("\u4f60", "US-ASCII"), "?");
  EXPECT_EQ(encode("\u00fcber", "US-ASCII"), "?ber");
  EXPECT_EQ(encode("a\u4f60b", "US-ASCII"), "a?b");
  EXPECT_EQ(encode("\u4f60", "ISO-8859-1"), "?");
}

TEST_F(EncodeDecodeTest, invalidBytesBecomeReplacementCharacter) {
  // Java maps undecodable input to U+FFFD (EF BF BD in UTF-8).
  EXPECT_EQ(decode(std::string("\xFF"), "UTF-8"), "\uFFFD");
  EXPECT_EQ(
      decode(
          std::string("a\xFF"
                      "b"),
          "UTF-8"),
      "a\uFFFDb");
}

TEST_F(EncodeDecodeTest, nulls) {
  EXPECT_EQ(encode(std::nullopt, "UTF-8"), std::nullopt);
  EXPECT_EQ(encode("abc", std::nullopt), std::nullopt);
  EXPECT_EQ(decode(std::nullopt, "UTF-8"), std::nullopt);
  EXPECT_EQ(decode("abc", std::nullopt), std::nullopt);
}

TEST_F(EncodeDecodeTest, charsetAliases) {
  // ICU canonicalises the alias spellings Java also accepts.
  for (const auto& alias : {"utf-8", "UTF8", "utf8", "UTF_8"}) {
    EXPECT_EQ(encode("abc", alias), "abc") << "alias: " << alias;
  }
  EXPECT_EQ(
      encode("\u00fcber", "latin1"),
      std::string("\xFC"
                  "ber"));
  EXPECT_EQ(encode("abc", "ascii"), "abc");
}

TEST_F(EncodeDecodeTest, unsupportedCharsetThrows) {
  // Spark raises for an unknown charset rather than returning NULL.
  BOLT_ASSERT_THROW(encode("abc", "NO-SUCH-CHARSET"), "Unsupported charset");
  BOLT_ASSERT_THROW(decode("abc", "NO-SUCH-CHARSET"), "Unsupported charset");
}

TEST_F(EncodeDecodeTest, nonConstantCharset) {
  // With a non-constant charset the converter is built per row; results must
  // still match the constant-charset path.
  auto input = makeFlatVector<std::string>({"abc", "\u00fcber", "abc"});
  auto charsets =
      makeFlatVector<std::string>({"US-ASCII", "ISO-8859-1", "UTF-8"});
  auto result = evaluate<SimpleVector<StringView>>(
      "encode(c0, c1)", makeRowVector({input, charsets}));

  EXPECT_EQ(result->valueAt(0).str(), "abc");
  EXPECT_EQ(
      result->valueAt(1).str(),
      std::string("\xFC"
                  "ber"));
  EXPECT_EQ(result->valueAt(2).str(), "abc");
}

TEST_F(EncodeDecodeTest, constantUtf8CharsetStillValidatesOnDecode) {
  // A charset written as a SQL literal reaches the factory as
  // inputArgs[1].constantValue, which selects the fast path where encode may
  // copy bytes unchanged. decode must NOT take that shortcut: undecodable
  // bytes still have to become U+FFFD rather than landing in a VARCHAR result
  // as invalid UTF-8.
  //
  // Passing the charset as a column instead would build a per-row converter
  // and would not cover this, so the literal here is load-bearing.
  auto input = makeFlatVector<std::string>(
      {std::string("\xFF"),
       std::string("a\xFF"
                   "b"),
       std::string("ok")},
      VARBINARY());
  auto result = evaluate<SimpleVector<StringView>>(
      "decode(c0, 'UTF-8')", makeRowVector({input}));

  EXPECT_EQ(result->valueAt(0).str(), "\uFFFD");
  EXPECT_EQ(result->valueAt(1).str(), "a\uFFFDb");
  EXPECT_EQ(result->valueAt(2).str(), "ok");

  // The encode direction may use the pass-through, and must be unaffected.
  auto text = makeFlatVector<std::string>({"abc", "\u00fcber"});
  auto encoded = evaluate<SimpleVector<StringView>>(
      "encode(c0, 'UTF-8')", makeRowVector({text}));
  EXPECT_EQ(encoded->valueAt(0).str(), "abc");
  EXPECT_EQ(encoded->valueAt(1).str(), "\u00fcber");
}

TEST_F(EncodeDecodeTest, unsupportedCharsetInOneRowOnly) {
  // With a per-row charset, an unsupported name must fail only its own row.
  // Without TRY the whole expression raises; inside TRY the bad row becomes
  // NULL and the valid rows still produce their values. This is what
  // applyToSelectedNoThrow buys, so it is asserted rather than assumed.
  auto text = makeFlatVector<std::string>({"abc", "def", "ghi"});
  auto charsets =
      makeFlatVector<std::string>({"UTF-8", "NO-SUCH-CHARSET", "US-ASCII"});
  auto data = makeRowVector({text, charsets});

  BOLT_ASSERT_THROW(
      evaluate<SimpleVector<StringView>>("encode(c0, c1)", data),
      "Unsupported charset");

  auto encoded =
      evaluate<SimpleVector<StringView>>("try(encode(c0, c1))", data);
  EXPECT_FALSE(encoded->isNullAt(0));
  EXPECT_EQ(encoded->valueAt(0).str(), "abc");
  EXPECT_TRUE(encoded->isNullAt(1));
  EXPECT_FALSE(encoded->isNullAt(2));
  EXPECT_EQ(encoded->valueAt(2).str(), "ghi");

  auto binary = makeFlatVector<std::string>({"abc", "def", "ghi"}, VARBINARY());
  auto binaryData = makeRowVector({binary, charsets});

  BOLT_ASSERT_THROW(
      evaluate<SimpleVector<StringView>>("decode(c0, c1)", binaryData),
      "Unsupported charset");

  auto decoded =
      evaluate<SimpleVector<StringView>>("try(decode(c0, c1))", binaryData);
  EXPECT_FALSE(decoded->isNullAt(0));
  EXPECT_EQ(decoded->valueAt(0).str(), "abc");
  EXPECT_TRUE(decoded->isNullAt(1));
  EXPECT_FALSE(decoded->isNullAt(2));
  EXPECT_EQ(decoded->valueAt(2).str(), "ghi");
}

TEST_F(EncodeDecodeTest, literalCharsetErrorIsCatchableByTry) {
  // A literal charset must be resolved during execution, not at plan time.
  // Building the converter in the constructor put the error outside
  // applyToSelectedNoThrow, so TRY() could not contain it.
  auto text = makeFlatVector<std::string>({"abc", "def"});
  auto data = makeRowVector({text});

  BOLT_ASSERT_THROW(
      evaluate<SimpleVector<StringView>>("encode(c0, 'BAD-CHARSET')", data),
      "Unsupported charset");
  auto encoded = evaluate<SimpleVector<StringView>>(
      "try(encode(c0, 'BAD-CHARSET'))", data);
  EXPECT_TRUE(encoded->isNullAt(0));
  EXPECT_TRUE(encoded->isNullAt(1));

  auto bin = makeFlatVector<std::string>({"abc", "def"}, VARBINARY());
  auto binData = makeRowVector({bin});
  BOLT_ASSERT_THROW(
      evaluate<SimpleVector<StringView>>("decode(c0, 'BAD-CHARSET')", binData),
      "Unsupported charset");
  auto decoded = evaluate<SimpleVector<StringView>>(
      "try(decode(c0, 'BAD-CHARSET'))", binData);
  EXPECT_TRUE(decoded->isNullAt(0));
  EXPECT_TRUE(decoded->isNullAt(1));
}

TEST_F(EncodeDecodeTest, illegalCharsetNameRejectedLikeJava) {
  // Java's Charset.forName rejects these outright; ICU would accept them, so
  // Bolt would otherwise offload queries Spark itself fails.
  for (const auto& bad : {" UTF-8", "UTF-8 ", "-UTF-8", "_UTF8", ""}) {
    BOLT_ASSERT_THROW(encode("abc", bad), "Illegal charset name");
  }
  // "UTF_8" and "ISO8859_1" are *legal* Java charset names (underscore is
  // permitted after the first character), so they are deliberately NOT
  // rejected by the name check. In Java they fail later, at alias lookup;
  // ICU happens to resolve them, which is a remaining gap recorded in the PR
  // discussion rather than something this check is meant to catch.
}

TEST_F(EncodeDecodeTest, malformedUtf8UsesMaximalSubpartReplacement) {
  // Java replaces each maximal ill-formed subpart with ONE U+FFFD
  // (Unicode 16.0 section 3.9); ICU's converter emits one per byte. The
  // surrogate X'EDA080' is a single 3-byte subpart, so Spark returns exactly
  // one replacement character, not three.
  EXPECT_EQ(decode(std::string("\xED\xA0\x80", 3), "UTF-8"), "\uFFFD");
  // A lone invalid byte is still one subpart, surrounded text is preserved.
  EXPECT_EQ(
      decode(
          std::string(
              "a\xFF"
              "b",
              3),
          "UTF-8"),
      "a\uFFFDb");
  // Truncated sequences: the lead byte alone is the maximal subpart.
  EXPECT_EQ(decode(std::string("\xE2\x82", 2), "UTF-8"), "\uFFFD");
  EXPECT_EQ(decode(std::string("\xF0\x9F\x92", 3), "UTF-8"), "\uFFFD");
  // Overlong forms have no valid initial subsequence, so each byte is its own
  // subpart and Java emits one U+FFFD per byte here.
  EXPECT_EQ(decode(std::string("\xC0\xAF", 2), "UTF-8"), "\uFFFD\uFFFD");
  EXPECT_EQ(
      decode(std::string("\xE0\x80\x80", 3), "UTF-8"), "\uFFFD\uFFFD\uFFFD");
  // Well-formed input, including multi-byte and supplementary characters, must
  // round-trip untouched through the same path.
  EXPECT_EQ(decode("abc", "UTF-8"), "abc");
  EXPECT_EQ(decode("\u20ac", "UTF-8"), "\u20ac");
  EXPECT_EQ(decode("\U0001F600", "UTF-8"), "\U0001F600");
  EXPECT_EQ(
      decode(
          std::string(
              "a\xED\xA0\x80"
              "z",
              5),
          "UTF-8"),
      "a\uFFFDz");
  // Aliases resolve to the same canonical charset and must behave identically.
  EXPECT_EQ(decode(std::string("\xED\xA0\x80", 3), "utf8"), "\uFFFD");
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
