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

#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/EffectiveSizeEstimator.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;

namespace bytedance::bolt::shuffle::sparksql {

// RowVector that overrides estimateFlatSize() to return a controlled value.
// This simulates batches whose flatSize exceeds the 32MB threshold so the
// estimator enters full-scan mode.
class FakeFlatSizeRowVector : public RowVector {
 public:
  FakeFlatSizeRowVector(
      memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      std::vector<VectorPtr> children,
      uint64_t fakeFlatSize)
      : RowVector(pool, type, std::move(nulls), length, std::move(children)),
        fakeFlatSize_(fakeFlatSize) {}

  uint64_t estimateFlatSize() const override {
    return fakeFlatSize_;
  }

 private:
  uint64_t fakeFlatSize_;
};

class EffectiveSizeEstimatorTest : public testing::Test,
                                   public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  // Create a FlatVector<StringView> where StringViews point into an
  // external buffer NOT registered in the vector's stringBuffers_.
  // This simulates the Parquet page-sharing scenario: estimateFlatSize()
  // sees only the StringView struct array (~16 bytes/row) while actual
  // string data is much larger.
  //
  // Caller must keep |externalBuf| alive for the lifetime of the vector.
  struct UntrackedStringResult {
    FlatVectorPtr<StringView> vector;
    BufferPtr externalBuf;
  };

  UntrackedStringResult makeUntrackedStringVector(
      vector_size_t numRows,
      int32_t stringLen) {
    auto totalBytes = static_cast<size_t>(numRows) * stringLen;
    auto buf = AlignedBuffer::allocate<char>(totalBytes, pool());
    auto* raw = buf->asMutable<char>();
    for (size_t i = 0; i < totalBytes; ++i) {
      raw[i] = 'a' + (i % 26);
    }

    auto values = AlignedBuffer::allocate<StringView>(numRows, pool());
    auto* views = values->asMutable<StringView>();
    for (vector_size_t i = 0; i < numRows; ++i) {
      views[i] =
          StringView(raw + static_cast<size_t>(i) * stringLen, stringLen);
    }

    auto vec = std::make_shared<FlatVector<StringView>>(
        pool(),
        VARCHAR(),
        /*nulls=*/nullptr,
        numRows,
        std::move(values),
        std::vector<BufferPtr>{} /* no string buffers → underestimate */);
    return {vec, buf};
  }

  // Wrap children in a FakeFlatSizeRowVector with a given fake flatSize.
  RowVectorPtr makeFakeRow(
      const RowTypePtr& type,
      std::vector<VectorPtr> children,
      uint64_t fakeFlatSize) {
    auto numRows = children.empty() ? 0 : children[0]->size();
    return std::make_shared<FakeFlatSizeRowVector>(
        pool(),
        type,
        /*nulls=*/nullptr,
        numRows,
        std::move(children),
        fakeFlatSize);
  }

  static constexpr uint64_t kFakeFlatSize = 40 << 20; // 40MB, above 32MB
};

// ---------- No binary columns ----------

TEST_F(EffectiveSizeEstimatorTest, noBinaryColumns) {
  auto intVec = BaseVector::create(INTEGER(), 1000, pool());
  auto rv = std::make_shared<RowVector>(
      pool(),
      ROW({"c0"}, {INTEGER()}),
      /*nulls=*/nullptr,
      1000,
      std::vector<VectorPtr>{intVec});

  EffectiveSizeEstimator estimator;
  EXPECT_EQ(estimator.estimate(rv), rv->estimateFlatSize());
}

// ---------- Flat VARCHAR with untracked buffer (high ratio) ----------

TEST_F(EffectiveSizeEstimatorTest, sharedBufferHighRatio) {
  auto numRows = 4096;
  auto stringLen = 1024;
  auto [vec, buf] = makeUntrackedStringVector(numRows, stringLen);

  auto rv = makeFakeRow(ROW({"c0"}, {VARCHAR()}), {vec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  // effectiveSize should reflect actual string data, much larger than
  // the fake 40MB flatSize.
  EXPECT_GT(effective, kFakeFlatSize);

  auto expectedStringData = static_cast<uint64_t>(numRows) * stringLen +
      static_cast<uint64_t>(numRows) * sizeof(StringView);
  EXPECT_GE(effective, expectedStringData * 0.8);
}

TEST_F(EffectiveSizeEstimatorTest, multipleStringColumns) {
  auto numRows = 4096;
  auto stringLen = 512;
  auto [vec1, buf1] = makeUntrackedStringVector(numRows, stringLen);
  auto [vec2, buf2] = makeUntrackedStringVector(numRows, stringLen);

  auto rv = makeFakeRow(
      ROW({"c0", "c1"}, {VARCHAR(), VARCHAR()}), {vec1, vec2}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto perColumn = static_cast<uint64_t>(numRows) * stringLen +
      static_cast<uint64_t>(numRows) * sizeof(StringView);
  EXPECT_GE(effective, perColumn * 2 * 0.8);
}

TEST_F(EffectiveSizeEstimatorTest, mixedStringAndNonStringColumns) {
  auto numRows = 4096;
  auto stringLen = 1024;
  auto intVec = BaseVector::create(INTEGER(), numRows, pool());
  auto [strVec, buf] = makeUntrackedStringVector(numRows, stringLen);

  auto rv = makeFakeRow(
      ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}),
      {intVec, strVec},
      kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto stringData = static_cast<uint64_t>(numRows) * stringLen +
      static_cast<uint64_t>(numRows) * sizeof(StringView);
  EXPECT_GE(effective, stringData * 0.8);
}

// ---------- ARRAY<VARCHAR> ----------

TEST_F(EffectiveSizeEstimatorTest, arrayOfStrings) {
  auto numRows = 1024;
  auto elementsPerRow = 8;
  auto totalElements = static_cast<vector_size_t>(numRows) * elementsPerRow;
  auto stringLen = 256;

  auto [elemVec, buf] = makeUntrackedStringVector(totalElements, stringLen);

  auto offsets = allocateOffsets(numRows, pool());
  auto sizes = allocateSizes(numRows, pool());
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * elementsPerRow;
    rawSizes[i] = elementsPerRow;
  }

  auto arrayVec = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(VARCHAR()),
      /*nulls=*/nullptr,
      numRows,
      std::move(offsets),
      std::move(sizes),
      elemVec);

  auto rv =
      makeFakeRow(ROW({"c0"}, {ARRAY(VARCHAR())}), {arrayVec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto expectedData = static_cast<uint64_t>(totalElements) * stringLen +
      static_cast<uint64_t>(totalElements) * sizeof(StringView);
  EXPECT_GE(effective, expectedData * 0.8);
}

// ---------- MAP<VARCHAR, VARCHAR> ----------

TEST_F(EffectiveSizeEstimatorTest, mapOfStrings) {
  auto numRows = 1024;
  auto entriesPerRow = 4;
  auto totalEntries = static_cast<vector_size_t>(numRows) * entriesPerRow;
  auto stringLen = 128;

  auto [keysVec, kBuf] = makeUntrackedStringVector(totalEntries, stringLen);
  auto [valsVec, vBuf] = makeUntrackedStringVector(totalEntries, stringLen);

  auto offsets = allocateOffsets(numRows, pool());
  auto sizes = allocateSizes(numRows, pool());
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapVec = std::make_shared<MapVector>(
      pool(),
      MAP(VARCHAR(), VARCHAR()),
      /*nulls=*/nullptr,
      numRows,
      std::move(offsets),
      std::move(sizes),
      keysVec,
      valsVec);

  auto rv = makeFakeRow(
      ROW({"c0"}, {MAP(VARCHAR(), VARCHAR())}), {mapVec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto perVec = static_cast<uint64_t>(totalEntries) * stringLen +
      static_cast<uint64_t>(totalEntries) * sizeof(StringView);
  EXPECT_GE(effective, perVec * 2 * 0.8);
}

// ---------- MAP<INTEGER, VARCHAR> ----------

TEST_F(EffectiveSizeEstimatorTest, mapIntKeyStringValue) {
  auto numRows = 1024;
  auto entriesPerRow = 4;
  auto totalEntries = static_cast<vector_size_t>(numRows) * entriesPerRow;
  auto stringLen = 256;

  auto keysVec =
      BaseVector::create<FlatVector<int32_t>>(INTEGER(), totalEntries, pool());
  for (vector_size_t i = 0; i < totalEntries; ++i) {
    keysVec->set(i, i);
  }
  auto [valsVec, vBuf] = makeUntrackedStringVector(totalEntries, stringLen);

  auto offsets = allocateOffsets(numRows, pool());
  auto sizes = allocateSizes(numRows, pool());
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOffsets[i] = i * entriesPerRow;
    rawSizes[i] = entriesPerRow;
  }

  auto mapVec = std::make_shared<MapVector>(
      pool(),
      MAP(INTEGER(), VARCHAR()),
      /*nulls=*/nullptr,
      numRows,
      std::move(offsets),
      std::move(sizes),
      keysVec,
      valsVec);

  auto rv = makeFakeRow(
      ROW({"c0"}, {MAP(INTEGER(), VARCHAR())}), {mapVec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto valData = static_cast<uint64_t>(totalEntries) * stringLen +
      static_cast<uint64_t>(totalEntries) * sizeof(StringView);
  EXPECT_GE(effective, valData * 0.8);
}

// ---------- Nested ROW ----------

TEST_F(EffectiveSizeEstimatorTest, nestedRowWithStrings) {
  auto numRows = 4096;
  auto stringLen = 512;
  auto intVec = BaseVector::create(INTEGER(), numRows, pool());
  auto [strVec, buf] = makeUntrackedStringVector(numRows, stringLen);

  auto innerRow = std::make_shared<RowVector>(
      pool(),
      ROW({"s0"}, {VARCHAR()}),
      /*nulls=*/nullptr,
      numRows,
      std::vector<VectorPtr>{strVec});

  auto rv = makeFakeRow(
      ROW({"c0", "c1"}, {INTEGER(), ROW({"s0"}, {VARCHAR()})}),
      {intVec, innerRow},
      kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto stringData = static_cast<uint64_t>(numRows) * stringLen +
      static_cast<uint64_t>(numRows) * sizeof(StringView);
  EXPECT_GE(effective, stringData * 0.8);
}

// ---------- VARBINARY ----------

TEST_F(EffectiveSizeEstimatorTest, varbinaryColumn) {
  auto numRows = 4096;
  auto dataLen = 512;

  auto totalBytes = static_cast<size_t>(numRows) * dataLen;
  auto buf = AlignedBuffer::allocate<char>(totalBytes, pool());
  auto* raw = buf->asMutable<char>();
  memset(raw, 0xAB, totalBytes);

  auto values = AlignedBuffer::allocate<StringView>(numRows, pool());
  auto* views = values->asMutable<StringView>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    views[i] = StringView(raw + static_cast<size_t>(i) * dataLen, dataLen);
  }

  auto vec = std::make_shared<FlatVector<StringView>>(
      pool(),
      VARBINARY(),
      /*nulls=*/nullptr,
      numRows,
      std::move(values),
      std::vector<BufferPtr>{} /* untracked */);

  auto rv = makeFakeRow(ROW({"c0"}, {VARBINARY()}), {vec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto expectedData = static_cast<uint64_t>(numRows) * dataLen +
      static_cast<uint64_t>(numRows) * sizeof(StringView);
  EXPECT_GE(effective, expectedData * 0.8);
}

// ---------- Nulls ----------

TEST_F(EffectiveSizeEstimatorTest, stringWithNulls) {
  auto numRows = 4096;
  auto stringLen = 512;

  auto totalBytes = static_cast<size_t>(numRows) * stringLen;
  auto buf = AlignedBuffer::allocate<char>(totalBytes, pool());
  auto* raw = buf->asMutable<char>();
  memset(raw, 'a', totalBytes);

  auto nulls = allocateNulls(numRows, pool());
  auto* rawNulls = nulls->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    bits::setNull(rawNulls, i, i % 2 == 0);
  }

  auto values = AlignedBuffer::allocate<StringView>(numRows, pool());
  auto* views = values->asMutable<StringView>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    if (i % 2 != 0) {
      views[i] =
          StringView(raw + static_cast<size_t>(i) * stringLen, stringLen);
    }
  }

  auto vec = std::make_shared<FlatVector<StringView>>(
      pool(),
      VARCHAR(),
      std::move(nulls),
      numRows,
      std::move(values),
      std::vector<BufferPtr>{} /* untracked */);

  auto rv = makeFakeRow(ROW({"c0"}, {VARCHAR()}), {vec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  auto nonNullCount = numRows / 2;
  auto expectedData = static_cast<uint64_t>(numRows) * sizeof(StringView) +
      static_cast<uint64_t>(nonNullCount) * stringLen;
  EXPECT_GE(effective, expectedData * 0.7);
}

// ---------- Fast path (flatSize <= 32MB) ----------

TEST_F(EffectiveSizeEstimatorTest, fastPathSmallBatch) {
  auto numRows = 100;
  auto stringLen = 64;
  auto [vec, buf] = makeUntrackedStringVector(numRows, stringLen);

  auto rv = std::make_shared<RowVector>(
      pool(),
      ROW({"c0"}, {VARCHAR()}),
      /*nulls=*/nullptr,
      numRows,
      std::vector<VectorPtr>{vec});

  EffectiveSizeEstimator estimator;
  auto flatSize = rv->estimateFlatSize();
  ASSERT_LE(flatSize, EffectiveSizeEstimator::kFullScanSizeThreshold);

  // Fast path: returns flatSize without scanning.
  EXPECT_EQ(estimator.estimate(rv), flatSize);
}

// ---------- alwaysFullScan trigger ----------

TEST_F(EffectiveSizeEstimatorTest, alwaysFullScanTriggered) {
  // Use large enough data so that effectiveSize > flatSize * 2.0.
  // With fakeFlatSize = 34MB (just above 32MB threshold) and ~100MB
  // of actual string data, the ratio will be ~3x.
  auto numRows = 16384;
  auto stringLen = 8192;
  auto [vec, buf] = makeUntrackedStringVector(numRows, stringLen);

  uint64_t smallFake = 34 << 20; // 34MB, just above threshold
  auto rv = makeFakeRow(ROW({"c0"}, {VARCHAR()}), {vec}, smallFake);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);
  // Effective should include ~128MB of actual string data.
  EXPECT_GT(effective, smallFake * 2);

  // Now alwaysFullScan should be set. A subsequent batch with
  // flatSize < 32MB should still be scanned instead of fast-pathed.
  auto rv2 = makeFakeRow(
      ROW({"c0"}, {VARCHAR()}), {vec}, 20 << 20 /* 20MB, below threshold */);
  auto effective2 = estimator.estimate(rv2);
  auto flatSize2 = rv2->estimateFlatSize();
  EXPECT_NE(effective2, flatSize2);
}

TEST_F(EffectiveSizeEstimatorTest, lowRatioDoesNotTriggerFullScan) {
  auto numRows = 4096;
  auto stringLen = 64;
  // Use tracked buffers (normal set()) → ratio ~1.0.
  auto vec =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), numRows, pool());
  std::string s(stringLen, 'x');
  for (vector_size_t i = 0; i < numRows; ++i) {
    vec->set(i, StringView(s));
  }

  auto rv = makeFakeRow(ROW({"c0"}, {VARCHAR()}), {vec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  estimator.estimate(rv);

  // With low ratio, alwaysFullScan should NOT be set.
  // A subsequent small-flatSize batch should take the fast path.
  auto rv2 = makeFakeRow(
      ROW({"c0"}, {VARCHAR()}), {vec}, 20 << 20 /* 20MB, below threshold */);
  auto effective2 = estimator.estimate(rv2);
  EXPECT_EQ(effective2, rv2->estimateFlatSize());
}

// ---------- Empty RowVector ----------

TEST_F(EffectiveSizeEstimatorTest, emptyRowVector) {
  auto vec = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 0, pool());
  auto rv = std::make_shared<RowVector>(
      pool(),
      ROW({"c0"}, {VARCHAR()}),
      /*nulls=*/nullptr,
      0,
      std::vector<VectorPtr>{vec});

  EffectiveSizeEstimator estimator;
  EXPECT_EQ(estimator.estimate(rv), 0);
}

// ---------- Reuse across multiple batches ----------

TEST_F(EffectiveSizeEstimatorTest, reusedAcrossBatches) {
  auto numRows = 4096;
  EffectiveSizeEstimator estimator;

  // Batch 1: untracked strings → high effective size.
  BufferPtr buf1;
  {
    auto [vec, buf] = makeUntrackedStringVector(numRows, 1024);
    buf1 = buf;
    auto rv = makeFakeRow(ROW({"c0"}, {VARCHAR()}), {vec}, kFakeFlatSize);
    auto e = estimator.estimate(rv);
    EXPECT_GT(e, kFakeFlatSize);
  }

  // Batch 2: tracked strings → lower effective size.
  {
    auto vec =
        BaseVector::create<FlatVector<StringView>>(VARCHAR(), numRows, pool());
    std::string s(64, 'x');
    for (vector_size_t i = 0; i < numRows; ++i) {
      vec->set(i, StringView(s));
    }
    auto rv = makeFakeRow(ROW({"c0"}, {VARCHAR()}), {vec}, kFakeFlatSize);
    auto e = estimator.estimate(rv);
    EXPECT_GT(e, 0);
    // With tracked buffers, effective should be close to flatSize.
    EXPECT_LE(e, kFakeFlatSize * 2);
  }
}

// ---------- Normal string (buffers tracked, accurate) ----------

TEST_F(EffectiveSizeEstimatorTest, normalStringAccurate) {
  auto numRows = 4096;
  auto stringLen = 200;
  auto vec =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), numRows, pool());
  std::string s(stringLen, 'x');
  for (vector_size_t i = 0; i < numRows; ++i) {
    vec->set(i, StringView(s));
  }

  auto rv = makeFakeRow(ROW({"c0"}, {VARCHAR()}), {vec}, kFakeFlatSize);

  EffectiveSizeEstimator estimator;
  auto effective = estimator.estimate(rv);

  // When buffers are tracked, effectiveSize should be close to flatSize.
  EXPECT_LE(effective, kFakeFlatSize * 2);
}

} // namespace bytedance::bolt::shuffle::sparksql
