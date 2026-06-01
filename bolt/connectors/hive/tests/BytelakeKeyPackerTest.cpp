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

#include "bolt/connectors/hive/bytelake/BytelakeKeyPacker.h"

#include <climits>

#include "gtest/gtest.h"

using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt;

// ---- appendBigint encoding ----

TEST(BytelakeKeyPackerTest, bigintProducesEightBytes) {
  std::string s;
  appendBigint(s, 0);
  EXPECT_EQ(s.size(), 8u);
}

TEST(BytelakeKeyPackerTest, bigintZeroEncoding) {
  // Sign-bit flip: 0 -> 0x80 in the top byte.
  std::string s;
  appendBigint(s, 0);
  EXPECT_EQ(static_cast<unsigned char>(s[0]), 0x80);
  for (int i = 1; i < 8; ++i) {
    EXPECT_EQ(static_cast<unsigned char>(s[i]), 0x00);
  }
}

TEST(BytelakeKeyPackerTest, bigintMinAndMax) {
  // After sign-flip:
  //   INT64_MIN (0x8000...0000) -> 0x0000...0000
  //   INT64_MAX (0x7FFF...FFFF) -> 0xFFFF...FFFF
  std::string minPack, maxPack;
  appendBigint(minPack, INT64_MIN);
  appendBigint(maxPack, INT64_MAX);
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(static_cast<unsigned char>(minPack[i]), 0x00)
        << "min[" << i << "]";
    EXPECT_EQ(static_cast<unsigned char>(maxPack[i]), 0xFF)
        << "max[" << i << "]";
  }
}

TEST(BytelakeKeyPackerTest, bigintMemcmpOrderingMatchesNumeric) {
  std::vector<int64_t> values = {
      INT64_MIN, -1000000, -1, 0, 1, 1000000, INT64_MAX};
  std::vector<std::string> packs;
  for (auto v : values) {
    std::string p;
    appendBigint(p, v);
    packs.push_back(std::move(p));
  }
  for (size_t i = 0; i + 1 < packs.size(); ++i) {
    EXPECT_LT(packs[i], packs[i + 1])
        << "values[" << i << "]=" << values[i] << " < " << values[i + 1];
  }
}

TEST(BytelakeKeyPackerTest, bigintEqualValuesEqualBytes) {
  std::string a, b;
  appendBigint(a, 42);
  appendBigint(b, 42);
  EXPECT_EQ(a, b);
}

// ---- appendVarchar encoding ----

TEST(BytelakeKeyPackerTest, varcharLengthPrefix) {
  std::string s;
  appendVarchar(s, StringView("hello"));
  ASSERT_EQ(s.size(), 4u + 5u);
  // Length prefix: 5 big-endian.
  EXPECT_EQ(static_cast<unsigned char>(s[0]), 0x00);
  EXPECT_EQ(static_cast<unsigned char>(s[1]), 0x00);
  EXPECT_EQ(static_cast<unsigned char>(s[2]), 0x00);
  EXPECT_EQ(static_cast<unsigned char>(s[3]), 0x05);
  EXPECT_EQ(s.substr(4), "hello");
}

TEST(BytelakeKeyPackerTest, varcharEmpty) {
  std::string s;
  appendVarchar(s, StringView(""));
  EXPECT_EQ(s.size(), 4u);
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(static_cast<unsigned char>(s[i]), 0);
  }
}

TEST(BytelakeKeyPackerTest, varcharSameLengthOrdering) {
  std::string a, b, c;
  appendVarchar(a, StringView("abc"));
  appendVarchar(b, StringView("abd"));
  appendVarchar(c, StringView("abe"));
  EXPECT_LT(a, b);
  EXPECT_LT(b, c);
}

TEST(BytelakeKeyPackerTest, varcharFixedLengthOrderingMatchesContent) {
  // Production scenario: all _hoodie_commit_time strings have the same
  // fixed length (e.g., "yyyyMMddHHmmss" = 14 chars). With identical length
  // prefixes, ordering falls through to lexicographic content compare.
  std::string a, b;
  appendVarchar(a, StringView("20260513000000"));
  appendVarchar(b, StringView("20260514000000"));
  EXPECT_LT(a, b);
}

TEST(BytelakeKeyPackerTest, varcharEqualValuesEqualBytes) {
  std::string a, b;
  appendVarchar(a, StringView("hello"));
  appendVarchar(b, StringView("hello"));
  EXPECT_EQ(a, b);
}

// ---- Multi-column safety ----

TEST(BytelakeKeyPackerTest, multiColumnDistinctEvenIfConcatLooksSame) {
  // Without the length prefix, ("abc","def") and ("abcdef","") would pack
  // identically. The 4-byte length prefix makes them distinct.
  std::string a, b;
  appendVarchar(a, StringView("abc"));
  appendVarchar(a, StringView("def"));
  appendVarchar(b, StringView("abcdef"));
  appendVarchar(b, StringView(""));
  EXPECT_NE(a, b);
}

TEST(BytelakeKeyPackerTest, multiColumnMixedBigintAndVarchar) {
  // (1, "a") < (1, "b") < (2, "a") under composite ordering.
  auto pack = [](int64_t b, const char* s) {
    std::string out;
    appendBigint(out, b);
    appendVarchar(out, StringView(s));
    return out;
  };
  auto a = pack(1, "a");
  auto b = pack(1, "b");
  auto c = pack(2, "a");
  EXPECT_LT(a, b);
  EXPECT_LT(b, c);
}

TEST(BytelakeKeyPackerTest, multiColumnEqualValues) {
  auto pack = [](int64_t b, const char* s) {
    std::string out;
    appendBigint(out, b);
    appendVarchar(out, StringView(s));
    return out;
  };
  EXPECT_EQ(pack(42, "foo"), pack(42, "foo"));
}

// ---- packCompositeKey via DecodedVector ----
// (Lower-level appendBigint / appendVarchar already cover encoding. Here
// we only verify packCompositeKey delegates correctly.)

namespace {
// Build a DecodedVector over a fresh FlatVector. Returns the vector and
// fills `decoded` (in/out) without touching ownership of out-vec.
template <typename T>
VectorPtr makeFlatAndDecode(
    memory::MemoryPool* pool,
    const std::vector<T>& values,
    DecodedVector& decoded) {
  auto rowType = CppToType<T>::create();
  auto vec = BaseVector::create(rowType, values.size(), pool);
  auto flat = vec->template asFlatVector<T>();
  for (size_t i = 0; i < values.size(); ++i) {
    flat->set(i, values[i]);
  }
  SelectivityVector rows(values.size());
  decoded.decode(*vec, rows);
  return vec;
}
} // namespace

TEST(BytelakeKeyPackerTest, packCompositeKeyUnsupportedTypeThrows) {
  // Construct a single-column setup with a non-supported TypeKind to verify
  // BOLT_FAIL fires. Use a stack DecodedVector default-constructed and pass
  // BOOLEAN to packCompositeKey.
  DecodedVector dv;
  std::vector<const DecodedVector*> decoders{&dv};
  std::vector<TypeKind> kinds{TypeKind::BOOLEAN};
  // We don't actually need decoded data to fire the type check.
  EXPECT_ANY_THROW(packCompositeKey(decoders, kinds, 0));
}
