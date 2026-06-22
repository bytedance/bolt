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

#include <gtest/gtest.h>

#include "bolt/row/dense/DenseRow.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::row {
namespace {

class DenseRowTest : public ::testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  // Serialize a RowVector into one contiguous buffer plus (N + 1) cumulative
  // offsets — the test-side equivalent of how shuffle lays out a partition
  // buffer from DenseRow::rowSizes() and then DenseRow::serialize()s into it.
  struct Bytes {
    std::vector<uint8_t> buffer;
    std::vector<size_t> offsets; // size N + 1

    std::string_view toView(size_t index) const {
      return std::string_view(
          reinterpret_cast<const char*>(buffer.data()) + offsets[index],
          offsets[index + 1] - offsets[index]);
    }

    std::string toHex(size_t index) const {
      auto view = toView(index);
      std::string out;
      out.reserve(view.size() * 2);
      static constexpr char kHex[] = "0123456789abcdef";
      for (unsigned char c : view) {
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0x0f]);
      }
      return out;
    }
  };

  static Bytes serializeToBytes(const RowVectorPtr& input) {
    DenseRow rows(input);
    const auto n = rows.numRows();
    Bytes out;
    out.offsets.resize(n + 1);
    size_t cum = 0;
    for (vector_size_t r = 0; r < n; ++r) {
      out.offsets[r] = cum;
      cum += rows.rowSizes()[r];
    }
    out.offsets[n] = cum;
    EXPECT_EQ(cum, rows.totalSize());
    out.buffer.resize(std::max<size_t>(cum, 1));
    rows.serialize(
        out.buffer.data(), folly::Range<const size_t*>(out.offsets.data(), n));
    return out;
  }

  // DenseRow is marker-less (no top-level null rows), so rebuild a null-free
  // RowVector from the fuzzed input's children, then serialize -> split ->
  // deserialize and compare.
  void roundTrip(const RowVectorPtr& fuzzed) {
    auto input = makeRowVector(fuzzed->children());
    const auto rowType =
        std::dynamic_pointer_cast<const RowType>(input->type());
    ASSERT_NE(rowType, nullptr);
    const auto n = input->size();

    auto bytes = serializeToBytes(input);
    std::vector<std::string_view> data(n);
    for (vector_size_t r = 0; r < n; ++r) {
      data[r] = bytes.toView(r);
    }
    auto out = DenseRow::deserialize(data, rowType, pool());
    assertEqualVectors(input, out);
  }

  VectorPtr
  fuzzVector(const TypePtr& type, vector_size_t size, uint32_t seed = 7) {
    VectorFuzzer::Options opts;
    opts.vectorSize = size;
    opts.nullRatio = 0.2;
    opts.dictionaryHasNulls = false;
    opts.stringVariableLength = true;
    opts.stringLength = 24;
    opts.containerVariableLength = true;
    opts.containerLength = 7;
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;

    VectorFuzzer fuzzer(opts, pool(), seed);
    return fuzzer.fuzzFlat(type, size);
  }
};

TEST_F(DenseRowTest, dictionaryEncodedInput) {
  // DenseRow decodes via DecodedVector (buildPlan), so dictionary-wrapped
  // inputs round-trip.
  auto base = makeFlatVector<int64_t>({100, 200, 300, 400});
  auto indices = makeIndicesInReverse(4);
  auto dict = BaseVector::wrapInDictionary(nullptr, indices, 4, base);
  roundTrip(makeRowVector({dict}));
}

TEST_F(DenseRowTest, constantEncodedInput) {
  // Constant-wrapped scalars decode via DecodedVector with isConstantMapping();
  // the scalar encoder sizes them once and splats. Cover a non-null constant,
  // a null constant, and a constant in a wide row alongside a flat column.
  roundTrip(makeRowVector({makeConstant<int64_t>(987654321, 16)}));
  roundTrip(makeRowVector({makeNullConstant(TypeKind::BIGINT, 16)}));
  roundTrip(makeRowVector({
      makeConstant<int32_t>(-7, 16),
      makeConstant<int16_t>(123, 16),
      makeNullConstant(TypeKind::TINYINT, 16),
      makeFlatVector<int64_t>(16, [](auto r) { return r * 3 - 5; }),
  }));
}

TEST_F(DenseRowTest, rowOfScalars) {
  roundTrip(std::dynamic_pointer_cast<RowVector>(fuzzVector(
      ROW({BIGINT(), INTEGER(), REAL(), DOUBLE(), VARCHAR()}), 128, 11)));
}

TEST_F(DenseRowTest, multiScalarWideRow) {
  // 10-column flat ROW covering every supported scalar leaf encoder.
  auto type = ROW({
      BIGINT(),
      INTEGER(),
      SMALLINT(),
      TINYINT(),
      BOOLEAN(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      TIMESTAMP(),
      BIGINT(),
  });
  roundTrip(std::dynamic_pointer_cast<RowVector>(fuzzVector(type, 256, 17)));
}

TEST_F(DenseRowTest, bigintEdges) {
  auto bigint = makeFlatVector<int64_t>({
      std::numeric_limits<int64_t>::min(),
      std::numeric_limits<int64_t>::max(),
      -1,
      0,
      1,
  });
  roundTrip(makeRowVector({bigint}));
}

// HUGEINT (128-bit, used by DECIMAL(precision > 18, *)). Cover null, zero,
// small, negative, and INT128 edges.
TEST_F(DenseRowTest, hugeintEdges) {
  using int128_t = __int128_t;
  const int128_t kMax = (int128_t{1} << 126) + ((int128_t{1} << 126) - 1);
  const int128_t kMin = -kMax - 1;
  auto values = makeNullableFlatVector<int128_t>(
      {kMin,
       kMax,
       int128_t{-1},
       int128_t{0},
       int128_t{1},
       std::nullopt,
       int128_t{1234567890123456789LL}});
  roundTrip(makeRowVector({values}));
}

// 16-column "Mix" schema from the production shuffle matrix tests: every
// supported scalar plus ARRAY/MAP/ROW.
TEST_F(DenseRowTest, mixWideRow) {
  auto type = ROW({
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      DECIMAL(10, 2),
      DECIMAL(38, 18),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      DATE(),
      TIMESTAMP(),
      ARRAY(INTEGER()),
      MAP(VARCHAR(), BIGINT()),
      ROW({INTEGER(), VARCHAR()}),
  });
  roundTrip(std::dynamic_pointer_cast<RowVector>(fuzzVector(type, 256, 41)));
}

// A top-level ROW whose nested-ROW child is dictionary-wrapped
TEST_F(DenseRowTest, dictionaryWrappedNestedRow) {
  auto innerInts = makeFlatVector<int32_t>({100, 200, 300, 400});
  auto innerStrs = makeFlatVector<StringView>({"aaa", "bbb", "ccc", "ddd"});
  auto baseNestedRow = makeRowVector({innerInts, innerStrs});

  const std::vector<vector_size_t> dictIndices = {3, 0, 2, 1, 0, 3};
  auto indicesBuf =
      AlignedBuffer::allocate<vector_size_t>(dictIndices.size(), pool());
  std::memcpy(
      indicesBuf->asMutable<vector_size_t>(),
      dictIndices.data(),
      dictIndices.size() * sizeof(vector_size_t));
  auto dictNestedRow = BaseVector::wrapInDictionary(
      nullptr,
      indicesBuf,
      static_cast<vector_size_t>(dictIndices.size()),
      baseNestedRow);

  auto bigintCol = makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60});
  roundTrip(makeRowVector({bigintCol, dictNestedRow}));
}

TEST_F(DenseRowTest, arrayOfBigint) {
  roundTrip(std::dynamic_pointer_cast<RowVector>(
      fuzzVector(ROW({ARRAY(BIGINT())}), 128, 12)));
}

TEST_F(DenseRowTest, arrayOfArrayOfBigint) {
  roundTrip(std::dynamic_pointer_cast<RowVector>(
      fuzzVector(ROW({ARRAY(ARRAY(BIGINT()))}), 256, 13)));
}

TEST_F(DenseRowTest, mapBigintReal) {
  roundTrip(std::dynamic_pointer_cast<RowVector>(
      fuzzVector(ROW({MAP(BIGINT(), REAL())}), 128, 14)));
}

TEST_F(DenseRowTest, nestedRowOfMixedFields) {
  auto type = ROW({
      BIGINT(),
      ARRAY(VARCHAR()),
      MAP(INTEGER(), ARRAY(BIGINT())),
      ROW({INTEGER(), VARCHAR()}),
  });
  roundTrip(std::dynamic_pointer_cast<RowVector>(fuzzVector(type, 128, 15)));
}

TEST_F(DenseRowTest, emptyContainers) {
  auto input = makeRowVector({
      makeArrayVector<int64_t>({{}, {}, {}}),
      makeMapVector<int32_t, StringView>({{}, {}, {}}),
      makeNestedArrayVectorFromJson<int64_t>({"[]", "[[]]", "[]"}),
  });
  roundTrip(input);
}

// Golden bytes pin the (marker-less) level-hoisted wire for
// ARRAY<ARRAY<BIGINT>>. Row 0: [[1,2,3],[4,5,6]]; row 1: [[7],[8,9]].
TEST_F(DenseRowTest, goldenBytesNestedArrays) {
  auto input = makeRowVector({
      makeNestedArrayVectorFromJson<int64_t>(
          {"[[1,2,3],[4,5,6]]", "[[7],[8,9]]"}),
  });
  auto bytes = serializeToBytes(input);
  // Row 0: 03 (outer=2+1) | 04 04 (inner=3+1,3+1) | 02 04 06 08 0a 0c (zz 1..6)
  EXPECT_EQ(bytes.toHex(0), "030404020406080a0c");
  // Row 1: 03 (outer) | 02 03 (inner=1+1,2+1) | 0e 10 12 (zz 7,8,9)
  EXPECT_EQ(bytes.toHex(1), "0302030e1012");
}

// Golden bytes for MAP<BIGINT, REAL> with hoisted key/value segments.
// Row 0: {1 -> 1.5, 2 -> 2.5}.
TEST_F(DenseRowTest, goldenBytesMapHoistedKV) {
  auto input = makeRowVector({
      makeMapVector<int64_t, float>({{{1, 1.5f}, {2, 2.5f}}}),
  });
  auto bytes = serializeToBytes(input);
  // 03 (card=2+1) | 02 04 (keys zz 1,2) | 0000c03f 00002040 (1.5f, 2.5f LE)
  EXPECT_EQ(
      bytes.toHex(0),
      "030204"
      "0000c03f"
      "00002040");
}

// Golden bytes for the top-level all-scalar ROW shape (the slot-free fast
// path). Per-row layout (marker-less):
// [bigint][int][varchar_len+1|payload][real].
TEST_F(DenseRowTest, goldenBytesScalarRow) {
  auto type = ROW({BIGINT(), INTEGER(), VARCHAR(), REAL()});
  auto bigint = makeFlatVector<int64_t>({1, -1});
  auto integer = makeNullableFlatVector<int32_t>({2, std::nullopt});
  auto varchar = makeFlatVector<StringView>({"ab", ""});
  auto real = makeNullableFlatVector<float>({1.5f, std::nullopt});
  auto input = makeRowVector({bigint, integer, varchar, real});

  auto bytes = serializeToBytes(input);
  // Row 0: bigint zz(1)=02, int zz(2)=04, varchar(len=2,"ab")=03 6162,
  // real 1.5f bits 0x3fc00000 LE = 0000c03f.
  EXPECT_EQ(
      bytes.toHex(0),
      "0204"
      "036162"
      "0000c03f");
  // Row 1: bigint zz(adjust(-1))=zz(-2)=3 -> 03, int null=00,
  // varchar(len=0)=01, real null = kNullFloatBits LE = 0000c07f.
  EXPECT_EQ(
      bytes.toHex(1),
      "03"
      "00"
      "01"
      "0000c07f");

  // Round-trip restores the original.
  std::vector<std::string_view> rows(2);
  for (vector_size_t r = 0; r < 2; ++r) {
    rows[r] = bytes.toView(r);
  }
  assertEqualVectors(input, DenseRow::deserialize(rows, type, pool()));
}

// Drive serialize() with reverse-order, gapped destination offsets to confirm
// each row's bytes land exactly where the offset table says and nowhere else.
TEST_F(DenseRowTest, serializeAtNonContiguousOffsets) {
  auto type = ROW({BIGINT(), VARCHAR(), ARRAY(INTEGER())});
  VectorFuzzer::Options opts;
  opts.vectorSize = 8;
  opts.nullRatio = 0.0;
  opts.stringLength = 12;
  opts.containerLength = 4;
  VectorFuzzer fuzzer(opts, pool(), 41);
  auto input = std::dynamic_pointer_cast<RowVector>(
      fuzzer.fuzzFlat(type, opts.vectorSize));
  const auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
  const auto n = input->size();

  DenseRow rows(input);
  const auto& sizes = rows.rowSizes();

  // Reverse buffer order, 7-byte gaps pre-filled with 0xCC.
  constexpr size_t kGap = 7;
  std::vector<size_t> offsets(n);
  size_t cum = 0;
  for (vector_size_t r = 0; r < n; ++r) {
    const auto srcRow = static_cast<vector_size_t>(n - 1 - r);
    offsets[srcRow] = cum;
    cum += sizes[srcRow] + kGap;
  }
  std::vector<uint8_t> buffer(cum, /*fill=*/0xCC);

  rows.serialize(
      buffer.data(),
      folly::Range<const size_t*>(offsets.data(), offsets.size()));

  std::vector<bool> claimed(cum, false);
  for (vector_size_t r = 0; r < n; ++r) {
    for (uint32_t i = offsets[r]; i < offsets[r] + sizes[r]; ++i) {
      claimed[i] = true;
    }
  }
  for (uint32_t i = 0; i < cum; ++i) {
    if (!claimed[i]) {
      EXPECT_EQ(buffer[i], 0xCC) << "gap byte at " << i << " was overwritten";
    }
  }

  std::vector<std::string_view> data(n);
  for (vector_size_t r = 0; r < n; ++r) {
    data[r] = std::string_view(
        reinterpret_cast<const char*>(buffer.data() + offsets[r]), sizes[r]);
  }
  assertEqualVectors(input, DenseRow::deserialize(data, rowType, pool()));
}

} // namespace
} // namespace bytedance::bolt::row
