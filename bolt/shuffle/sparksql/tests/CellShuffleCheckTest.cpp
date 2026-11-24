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

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleCheck.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

constexpr uint32_t kPartitions = 3;

/// Handwritten cache contents keep the observed values independent of the
/// input mapping and permit corruption that the production writer never emits.
struct Cache {
  alignas(
      kCacheLineBytes) std::array<char, kPartitions * kCacheLineBytes> bytes{};
  std::array<uint8_t, kPartitions> cursors{};

  char* line(uint32_t pid) {
    return bytes.data() + pid * kCacheLineBytes;
  }

  template <typename T>
  void set(uint32_t pid, std::initializer_list<T> values) {
    const auto size = values.size() * sizeof(T);
    BOLT_CHECK_LE(size, kCacheLineBytes);
    std::memcpy(line(pid), values.begin(), size);
    cursors[pid] = static_cast<uint8_t>(size);
  }
};

struct SizedCache {
  struct alignas(kCacheLineBytes) Line {
    std::array<char, kCacheLineBytes> bytes{};
  };

  explicit SizedCache(uint32_t partitions)
      : lines(partitions), cursors(partitions, 0) {}

  char* data() {
    return lines.front().bytes.data();
  }

  void reset() {
    std::fill(cursors.begin(), cursors.end(), 0);
    for (auto& line : lines) {
      line.bytes.fill(static_cast<char>(0xa5));
    }
  }

  template <typename T>
  void append(CellShuffleCheck& check, uint32_t col, uint32_t pid, T value) {
    auto* line = lines[pid].bytes.data();
    std::memcpy(line + cursors[pid], &value, sizeof(T));
    cursors[pid] += sizeof(T);
    if (cursors[pid] == kCacheLineBytes) {
      check.beforeFullFlush<T>(col, pid, line, kCacheLineBytes);
      cursors[pid] = 0;
      lines[pid].bytes.fill(static_cast<char>(0xa5));
    }
  }

  std::vector<Line> lines;
  std::vector<uint8_t> cursors;
};

void finishCaches(CellShuffleCheck& check, Cache& cache, uint32_t col = 0) {
  for (uint32_t pid = 0; pid < kPartitions; ++pid) {
    if (cache.cursors[pid] != 0) {
      check.beforeStreamFlush(col, pid, cache.line(pid), cache.cursors[pid]);
      cache.cursors[pid] = 0;
    }
  }
}

class CountingOutput final : public CellOutput {
 public:
  void spillCells(const CellWindowInput&) override {
    ++calls;
  }
  void sealPayloads(const CellWindowInput&) override {
    ++calls;
  }
  void finalize(const CellWindowInput&, ShuffleWriterMetrics&) override {
    ++calls;
  }
  int64_t bytesEvicted() const override {
    return 0;
  }
  uint32_t calls{0};
};

class CellShuffleCheckTest : public testing::Test,
                             public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  SplitBatch prepare(
      std::vector<VectorPtr> columns,
      std::vector<uint32_t> partitions,
      uint32_t numPartitions = kPartitions) {
    input_ = makeRowVector(columns);
    partitions_ = std::move(partitions);
    counts_.assign(numPartitions, 0);
    rowIndices_.resize(partitions_.size());
    for (uint32_t row = 0; row < partitions_.size(); ++row) {
      rowIndices_[row] = counts_[partitions_[row]]++;
    }
    decoded_.clear();
    for (const auto& column : input_->children()) {
      decoded_.emplace_back(*column);
    }
    nullClasses_.assign(columns.size(), NullClass::kSomeNulls);
    SplitBatch batch;
    batch.decoded = decoded_;
    batch.row2Partition = partitions_;
    batch.partition2RowCount = counts_;
    batch.nullClass = nullClasses_;
    batch.rowIndexInPid = rowIndices_;
    return batch;
  }

  std::vector<VectorPtr> fixedColumns(uint32_t rows, bool nullable) {
    const auto value = [](auto row) { return row % 101 - 50; };
    std::function<bool(vector_size_t)> isNull;
    if (nullable) {
      isNull = [](auto row) { return row % 7 == 0; };
    }
    return {
        makeFlatVector<int8_t>(rows, value, isNull),
        makeFlatVector<int16_t>(rows, value, isNull),
        makeFlatVector<int32_t>(rows, value, isNull),
        makeFlatVector<int64_t>(rows, value, isNull),
        makeFlatVector<float>(rows, value, isNull),
        makeFlatVector<double>(rows, value, isNull)};
  }

  RowVectorPtr input_;
  std::vector<DecodedVector> decoded_;
  std::vector<uint32_t> partitions_;
  std::vector<uint32_t> counts_;
  std::vector<uint32_t> rowIndices_;
  std::vector<NullClass> nullClasses_;
};

TEST_F(CellShuffleCheckTest, streamingAcrossBatchesDetectsCorruption) {
  const auto run = [&]<typename T>() {
    for (const int failure : {0, 1, 2, 3}) {
      SCOPED_TRACE(fmt::format("width={} failure={}", sizeof(T), failure));
      auto batch = prepare({makeFlatVector<T>({T{1}})}, {0});
      auto check = CellShuffleCheck::create(
          input_->type()->asRow(), kPartitions, 1, 1, pool(), 71);
      ASSERT_NE(check, nullptr);
      SizedCache cache(kPartitions);
      check->setColumnCache(0, cache.data());
      uint32_t ordinal = 0;
      for (const uint32_t rows : {3, 67, 5}) {
        auto values = makeFlatVector<T>(rows, [ordinal](auto row) {
          return static_cast<T>((ordinal + row) % 97);
        });
        batch = prepare({values}, std::vector<uint32_t>(rows, 0));
        check->observeBatch(batch);
        for (uint32_t row = 0; row < rows; ++row, ++ordinal) {
          T value = static_cast<T>(ordinal % 97);
          if ((failure == 1 && ordinal == 10) ||
              (failure == 2 && ordinal == 74)) {
            value = static_cast<T>(value + 1);
          }
          cache.append<T>(*check, 0, 0, value);
        }
      }

      if (failure != 3) {
        check->beforeStreamFlush(0, 0, cache.data(), cache.cursors[0]);
        cache.reset(); // Digest must survive actual cache overwrite.
      }
      if (failure == 0) {
        EXPECT_NO_THROW(check->finishStream());
      } else if (failure == 3) {
        BOLT_ASSERT_THROW(check->finishStream(), "count mismatch");
      } else {
        BOLT_ASSERT_THROW(check->finishStream(), "CRC mismatch");
      }
    }
  };
  run.template operator()<int8_t>();
  run.template operator()<int16_t>();
  run.template operator()<int32_t>();
  run.template operator()<int64_t>();
  run.template operator()<float>();
  run.template operator()<double>();
}

TEST_F(CellShuffleCheckTest, streamingSplitterSealsAndMappedNulls) {
  constexpr uint32_t kRows = 65;
  const auto dictionary = BaseVector::wrapInDictionary(
      makeNulls(kRows, [](auto row) { return row % 5 == 0; }),
      makeIndices(kRows, [](auto row) { return (3 * row + 1) % 4; }),
      kRows,
      makeFlatVector<int64_t>({-31, 42, 19, 77}));
  const auto constant = BaseVector::wrapInConstant(
      kRows, 1, makeFlatVector<double>({1.0, 19.5}), false);
  std::vector<uint32_t> pids(kRows);
  for (uint32_t row = 0; row < kRows; ++row) {
    pids[row] = row % kPartitions;
  }
  auto batch = prepare({dictionary, constant}, pids);
  const auto layout = CellLayout::create(asRowType(input_->type()));
  CountingOutput output;
  ChunkAllocator allocator(pool(), 1 << 20, 256);
  CellSplitter splitter(layout, kPartitions, allocator, output, *pool(), true);
  auto check = CellShuffleCheck::create(
      *layout.rowType(), kPartitions, 1, 2, pool(), 72);
  ASSERT_NE(check, nullptr);
  splitter.setStreamCheck(check.get());
  for (uint32_t i = 0; i < 4; ++i) {
    batch = prepare({dictionary, constant}, pids);
    nullClasses_[1] = NullClass::kNoNulls;
    check->observeBatch(batch);
    splitter.split(batch);
    if (i == 0) {
      splitter.sealWindow({});
    } else if (i == 1) {
      EXPECT_GT(splitter.sealPartitionsOver(1, {}), 0);
    } else if (i == 2) {
      splitter.spillAll(); // Spill leaves pending cache bytes untouched.
    }
  }
  ShuffleWriterMetrics metrics;
  EXPECT_NO_THROW(splitter.finish(metrics, {}));

  EXPECT_GT(output.calls, 0);
}

TEST_F(CellShuffleCheckTest, streamingFloatingSemanticsAndMissingAllNull) {
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  auto batch = prepare({makeFlatVector<double>({nan, 0.0, -0.0})}, {0, 0, 0});
  auto check = CellShuffleCheck::create(
      input_->type()->asRow(), kPartitions, 1, 1, pool(), 1);
  ASSERT_NE(check, nullptr);
  Cache cache;
  check->setColumnCache(0, cache.bytes.data());
  check->observeBatch(batch);
  cache.set<double>(0, {-nan, -0.0, 0.0});
  check->beforeStreamFlush(0, 0, cache.line(0), cache.cursors[0]);
  EXPECT_NO_THROW(check->finishStream());

  batch = prepare({makeFlatVector<double>({7.0})}, {0});
  nullClasses_[0] = NullClass::kAllNull;
  check->observeBatch(batch);
  BOLT_ASSERT_THROW(check->finishStream(), "(3 vs. 4)");
}

TEST_F(CellShuffleCheckTest, nullHiddenIndicesAcrossMaskBoundaries) {
  const auto base = makeFlatVector<int64_t>({11, -23, 47, -91});
  constexpr std::array<int64_t, 4> kExpected{-91, 11, -23, 47};
  for (const vector_size_t rows : {63, 64, 65, 127, 128, 129}) {
    for (uint32_t pattern = 0; pattern < 4; ++pattern) {
      SCOPED_TRACE(testing::Message() << rows << " rows, pattern " << pattern);
      const auto nonNull = [&](vector_size_t row) {
        switch (pattern) {
          case 0:
            return row == 62 || row == 63 || row == 64 || row == 126 ||
                row == 127 || row == 128;
          case 1:
            return row == rows - 1;
          case 2:
            return false;
          default:
            return row < 64 || row == 128;
        }
      };
      const auto dictionary = BaseVector::wrapInDictionary(
          makeNulls(rows, [&](auto row) { return !nonNull(row); }),
          makeIndices(
              rows,
              [&](auto row) {
                return nonNull(row) ? (row * 5 + 3) % 4
                                    : std::numeric_limits<vector_size_t>::max();
              }),
          rows,
          base);
      std::vector<uint32_t> pids(rows);
      for (vector_size_t row = 0; row < rows; ++row) {
        pids[row] = row % kPartitions;
      }
      auto batch = prepare({dictionary}, pids);
      auto check = CellShuffleCheck::create(
          input_->type()->asRow(), kPartitions, 1, 1, pool());
      ASSERT_NE(check, nullptr);
      Cache cache;
      check->setColumnCache(0, cache.bytes.data());
      check->observeBatch(batch);

      for (vector_size_t row = 0; row < rows; ++row) {
        if (!nonNull(row)) {
          continue;
        }
        const auto pid = pids[row];
        const auto value = kExpected[row % kExpected.size()];
        std::memcpy(
            cache.line(pid) + cache.cursors[pid], &value, sizeof(value));
        cache.cursors[pid] += sizeof(value);

        if (cache.cursors[pid] == kCacheLineBytes) {
          check->beforeFullFlush<int64_t>(
              0, pid, cache.line(pid), kCacheLineBytes);
          cache.cursors[pid] = 0;
          std::memset(cache.line(pid), 0xda, kCacheLineBytes);
        }
      }

      finishCaches(*check, cache);
      EXPECT_NO_THROW(check->finishStream());
    }
  }
}

TEST_F(CellShuffleCheckTest, flatNullWordsRespectSliceBoundsAndDirtyPadding) {
  const auto run = [&]<typename T>() {
    for (const vector_size_t rows : {1, 63, 64, 65, 127, 128, 129}) {
      for (const vector_size_t offset : {1, 7, 8, 63, 64, 65}) {
        for (uint32_t pattern = 0; pattern < 5; ++pattern) {
          SCOPED_TRACE(
              testing::Message()
              << sizeof(T) << " bytes, rows=" << rows << ", offset=" << offset
              << ", pattern=" << pattern);
          const auto nonNull = [&](vector_size_t row) {
            const bool boundary = row == 0 || row == rows - 1 || row == 62 ||
                row == 63 || row == 64 || row == 126 || row == 127 ||
                row == 128;
            switch (pattern) {
              case 0:
                return boundary;
              case 1:
                return !boundary;
              case 2:
                return row % 7 != 0;
              case 3:
                return false;
              default:
                return true;
            }
          };
          const auto value = [](vector_size_t parentRow) {
            return static_cast<T>(17 * parentRow - 101);
          };
          const vector_size_t parentRows = offset + rows + 64;
          auto parent = pattern == 3
              ? std::make_shared<FlatVector<T>>(
                    pool(),
                    CppToType<T>::create(),
                    makeNulls(parentRows, [](auto) { return true; }),
                    parentRows,
                    nullptr,
                    std::vector<BufferPtr>{})
              : makeFlatVector<T>(parentRows, value, [&](auto row) {
                  return row >= offset && row < offset + rows &&
                      !nonNull(row - offset);
                });
          const VectorPtr slice = parent->slice(offset, rows);
          if (slice->rawNulls() == nullptr) {
            slice->mutableRawNulls(); // Preserve an all-valid logical mask.
          }
          auto& nulls = slice->mutableNulls(rows + 64);
          bits::fillBits(nulls->asMutable<uint64_t>(), rows, rows + 64, true);
          const auto words = bits::nwords(rows);
          const std::vector<uint64_t> originalNulls(
              slice->rawNulls(), slice->rawNulls() + words);
          std::vector<uint32_t> pids(rows);
          for (vector_size_t row = 0; row < rows; ++row) {
            pids[row] = row % kPartitions;
          }
          auto batch = prepare({slice}, pids);
          ASSERT_TRUE(decoded_[0].isIdentityMapping());
          ASSERT_TRUE(decoded_[0].mayHaveNulls());
          auto check = CellShuffleCheck::create(
              input_->type()->asRow(), kPartitions, 1, 1, pool());
          ASSERT_NE(check, nullptr);
          Cache cache;
          check->setColumnCache(0, cache.bytes.data());
          check->observeBatch(batch);

          for (vector_size_t row = 0; row < rows; ++row) {
            if (!nonNull(row)) {
              continue;
            }
            const auto pid = pids[row];
            const T actual = value(offset + row);
            std::memcpy(
                cache.line(pid) + cache.cursors[pid], &actual, sizeof(T));
            cache.cursors[pid] += sizeof(T);

            if (cache.cursors[pid] == kCacheLineBytes) {
              check->beforeFullFlush<T>(
                  0, pid, cache.line(pid), kCacheLineBytes);
              cache.cursors[pid] = 0;
              std::memset(cache.line(pid), 0xda, kCacheLineBytes);
            }
          }
          const auto cursors = cache.cursors;
          EXPECT_EQ(cache.cursors, cursors);
          finishCaches(*check, cache);
          EXPECT_NO_THROW(check->finishStream());
          EXPECT_EQ(
              std::vector<uint64_t>(
                  slice->rawNulls(), slice->rawNulls() + words),
              originalNulls);
        }
      }
    }
  };
  run.template operator()<int32_t>();
  run.template operator()<int64_t>();
}

TEST_F(CellShuffleCheckTest, selectsSupportedLogicalColumnsInSchemaOrder) {
  const auto type =
      ROW({"bool",
           "shortDecimal",
           "longDecimal",
           "string",
           "tiny",
           "small",
           "integer",
           "big",
           "real",
           "double",
           "date",
           "timestamp"},
          {BOOLEAN(),
           DECIMAL(10, 2),
           DECIMAL(30, 2),
           VARCHAR(),
           TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           DATE(),
           TIMESTAMP()});
  auto limited = CellShuffleCheck::create(*type, kPartitions, 1, 2, pool());
  auto all = CellShuffleCheck::create(*type, kPartitions, 1, 100, pool());
  ASSERT_NE(limited, nullptr);
  ASSERT_NE(all, nullptr);
  for (uint32_t col = 0; col < type->size(); ++col) {
    SCOPED_TRACE(col);
    EXPECT_EQ(limited->checksColumn(col), col == 4 || col == 5);
    EXPECT_EQ(all->checksColumn(col), col >= 4 && col <= 10);
  }
}

TEST_F(CellShuffleCheckTest, taskSamplingIsStableAndIndependentOfPartitions) {
  const auto type = ROW({"a", "b"}, {INTEGER(), DOUBLE()});
  const std::vector<std::pair<int64_t, bool>> cases{
      {0, true},
      {24, true},
      {25, false},
      {99, false},
      {100, true},
      {124, true},
      {125, false},
      {-1, true},
      {std::numeric_limits<int64_t>::min(), true},
      {(int64_t{1} << 40) + 7, false},
      {std::numeric_limits<int64_t>::max(), true}};
  for (const auto& [taskId, expected] : cases) {
    for (const uint32_t partitions : {1, 16, 1024}) {
      auto check =
          CellShuffleCheck::create(*type, partitions, 0.25, 2, pool(), taskId);
      auto same =
          CellShuffleCheck::create(*type, partitions, 0.25, 2, pool(), taskId);
      EXPECT_EQ(check != nullptr, expected);
      EXPECT_EQ(same != nullptr, expected);
      if (expected) {
        EXPECT_TRUE(check->checksColumn(0));
        EXPECT_TRUE(check->checksColumn(1));
      }
    }
  }
}

TEST_F(CellShuffleCheckTest, taskSamplingIncreasingRatios) {
  const auto type = ROW({"a"}, {INTEGER()});
  for (const int64_t taskId : {0, 24, 25, 74, 75, 99}) {
    auto task = CellShuffleCheck::create(*type, 32, 0.25, 1, pool(), taskId);
    auto more = CellShuffleCheck::create(*type, 32, 0.75, 1, pool(), taskId);
    if (task != nullptr) {
      EXPECT_NE(more, nullptr);
    }
  }
}

TEST_F(CellShuffleCheckTest, fractionalPercentUsesWholeRemainderBuckets) {
  const auto type = ROW({"a"}, {INTEGER()});
  for (const auto& [ratio, expected] :
       std::vector<std::pair<double, uint32_t>>{{0.005, 1}, {0.255, 26}}) {
    uint32_t selected = 0;
    for (int64_t taskId = 0; taskId < 100; ++taskId) {
      selected += CellShuffleCheck::create(
                      *type, 1, ratio, 1, pool(), taskId) != nullptr;
    }
    EXPECT_EQ(selected, expected);
  }
}

TEST_F(CellShuffleCheckTest, factoryOmitsUnselectedAndUnsupportedWriters) {
  const auto type = ROW({"a"}, {INTEGER()});
  ShuffleWriterOptions options;
  options.shuffleCheckRatio = 0.1;
  options.taskAttemptId = 10;
  EXPECT_EQ(
      CellShuffleCheck::create(*type, 16, options, true, pool()), nullptr);
  options.taskAttemptId = 3;
  ASSERT_NE(
      CellShuffleCheck::create(*type, 16, options, true, pool()), nullptr);
  EXPECT_EQ(
      CellShuffleCheck::create(*type, 16, options, false, pool()), nullptr);
  const auto unsupported = ROW({"s"}, {VARCHAR()});
  EXPECT_EQ(
      CellShuffleCheck::create(*unsupported, 16, options, true, pool()),
      nullptr);
  options.shuffleCheckRatio = 0;
  EXPECT_EQ(
      CellShuffleCheck::create(*type, 16, options, true, pool()), nullptr);
  options.shuffleCheckRatio = 1;
  options.shuffleCheckMaxColumns = 0;
  EXPECT_EQ(
      CellShuffleCheck::create(*type, 16, options, true, pool()), nullptr);
}

TEST_F(CellShuffleCheckTest, singlePartitionCanBeSelectedOrSkipped) {
  auto batch = prepare({makeFlatVector<int64_t>({42})}, {0}, 1);
  uint32_t selected = 0;
  for (int64_t taskId = 0; taskId < 10000; ++taskId) {
    selected +=
        CellShuffleCheck::create(
            input_->type()->asRow(), 1, 0.1, 1, pool(), taskId) != nullptr;
  }
  EXPECT_EQ(selected, 1000);
  const auto before = pool()->usedBytes();
  EXPECT_EQ(
      CellShuffleCheck::create(input_->type()->asRow(), 1, 0.1, 1, pool(), 10),
      nullptr);
  EXPECT_EQ(pool()->usedBytes(), before); // No digest-state allocation.
  auto check =
      CellShuffleCheck::create(input_->type()->asRow(), 1, 0.1, 1, pool(), 3);
  ASSERT_NE(check, nullptr);
  Cache cache;
  check->setColumnCache(0, cache.bytes.data());
  check->observeBatch(batch);
  cache.set<int64_t>(0, {42});
  check->beforeStreamFlush(0, 0, cache.line(0), 8);
  EXPECT_NO_THROW(check->finishStream());
}

TEST_F(CellShuffleCheckTest, oneColumnAndDisabledSampling) {
  auto batch = prepare({makeFlatVector<int64_t>({42})}, {0});
  for (const double ratio :
       {0.0, -1.0, std::numeric_limits<double>::quiet_NaN()}) {
    EXPECT_EQ(
        CellShuffleCheck::create(
            input_->type()->asRow(), kPartitions, ratio, 1, pool()),
        nullptr);
  }
  for (const int32_t cap : {0, -1}) {
    EXPECT_EQ(
        CellShuffleCheck::create(
            input_->type()->asRow(), kPartitions, 1, cap, pool()),
        nullptr);
  }
  for (const double ratio : {1.0, 2.0}) {
    auto check = CellShuffleCheck::create(
        input_->type()->asRow(), kPartitions, ratio, 1, pool(), 7);
    ASSERT_NE(check, nullptr);
    EXPECT_TRUE(check->checksColumn(0));
    Cache cache;
    check->setColumnCache(0, cache.bytes.data());
    check->observeBatch(batch);
    cache.set<int64_t>(0, {42});
    finishCaches(*check, cache);
    EXPECT_NO_THROW(check->finishStream());
  }
  const auto unsupported = ROW({"string"}, {VARCHAR()});
  EXPECT_EQ(
      CellShuffleCheck::create(*unsupported, kPartitions, 0.1, 4, pool(), 1),
      nullptr);
}

TEST_F(CellShuffleCheckTest, sampledTaskDetectsCorruptionInEveryPartition) {
  for (const int corruptPid : {-1, 0, 1}) {
    auto batch = prepare(
        {makeFlatVector<int64_t>({11, 22}),
         makeFlatVector<int64_t>({33, 44}),
         makeFlatVector<int64_t>({55, 66}),
         makeFlatVector<int64_t>({77, 88})},
        {0, 1});
    auto check = CellShuffleCheck::create(
        input_->type()->asRow(), kPartitions, 0.5, 4, pool(), 3);
    ASSERT_NE(check, nullptr);
    std::array<Cache, 4> caches;
    for (uint32_t col = 0; col < 4; ++col) {
      check->setColumnCache(col, caches[col].bytes.data());
    }
    for (uint32_t b = 0; b < 3; ++b) {
      check->observeBatch(batch);
      for (uint32_t col = 0; col < 4; ++col) {
        for (uint32_t pid = 0; pid < 2; ++pid) {
          const int64_t value = (col * 2 + pid + 1) * 11 +
              ((static_cast<int>(pid) == corruptPid && b == 1) ? 1 : 0);
          std::memcpy(
              caches[col].line(pid) + caches[col].cursors[pid],
              &value,
              sizeof(value));
          caches[col].cursors[pid] += sizeof(value);
        }
      }
    }
    for (uint32_t col = 0; col < 4; ++col) {
      finishCaches(*check, caches[col], col);
    }

    if (corruptPid >= 0) {
      BOLT_ASSERT_THROW(check->finishStream(), "CRC mismatch");
    } else {
      EXPECT_NO_THROW(check->finishStream());
    }
  }
}

TEST_F(CellShuffleCheckTest, everyTailLengthAcrossBatchesAndFlushes) {
  const auto run = [&]<typename T>() {
    const uint32_t capacity = kCacheLineBytes / sizeof(T);
    for (uint32_t tail = 0; tail < capacity; ++tail) {
      for (const bool corrupt : {false, true}) {
        SCOPED_TRACE(fmt::format(
            "width={} tail={} corrupt={}", sizeof(T), tail, corrupt));
        auto batch = prepare({makeFlatVector<T>({T{1}})}, {0});
        auto check = CellShuffleCheck::create(
            input_->type()->asRow(), kPartitions, 1, 1, pool(), 1);
        ASSERT_NE(check, nullptr);
        SizedCache cache(kPartitions);
        check->setColumnCache(0, cache.data());
        uint32_t ordinal = 0;
        for (const uint32_t rows : {capacity + 1, capacity - 1 + tail}) {
          batch = prepare(
              {makeFlatVector<T>(
                  rows,
                  [ordinal](auto row) {
                    return static_cast<T>((ordinal + row) % 79 + 1);
                  })},
              std::vector<uint32_t>(rows, 0));
          check->observeBatch(batch);
          for (uint32_t row = 0; row < rows; ++row, ++ordinal) {
            T value = static_cast<T>(ordinal % 79 + 1);
            if (corrupt && ordinal == 2 * capacity + tail - 1) {
              value = static_cast<T>(value + 1);
            }
            cache.append<T>(*check, 0, 0, value);
          }
        }
        if (cache.cursors[0] != 0) {
          check->beforeStreamFlush(0, 0, cache.data(), cache.cursors[0]);
        }
        if (corrupt) {
          BOLT_ASSERT_THROW(check->finishStream(), "CRC mismatch");
        } else {
          EXPECT_NO_THROW(check->finishStream());
        }
      }
    }
  };
  run.template operator()<int8_t>();
  run.template operator()<int16_t>();
  run.template operator()<int32_t>();
  run.template operator()<int64_t>();
  run.template operator()<float>();
  run.template operator()<double>();
}

TEST_F(CellShuffleCheckTest, rejectsMissingExtraReorderedAndWrongPartition) {
  for (const int failure : {0, 1, 2, 3}) {
    auto batch =
        prepare({makeFlatVector<int32_t>({11, 22, 33, 44})}, {0, 0, 1, 1});
    auto check = CellShuffleCheck::create(
        input_->type()->asRow(), kPartitions, 1, 1, pool());
    ASSERT_NE(check, nullptr);
    Cache cache;
    check->setColumnCache(0, cache.bytes.data());
    check->observeBatch(batch);
    cache.set<int32_t>(0, {11, 22});
    cache.set<int32_t>(1, {33, 44});
    if (failure == 0) {
      cache.set<int32_t>(0, {11});
    } else if (failure == 1) {
      cache.set<int32_t>(0, {11, 22, 55});
    } else if (failure == 2) {
      cache.set<int32_t>(0, {22, 11});
    } else {
      cache.set<int32_t>(0, {33, 44});
      cache.set<int32_t>(1, {11, 22});
    }

    if (failure == 1) {
      BOLT_ASSERT_THROW(finishCaches(*check, cache), "(3 vs. 2)");
    } else {
      finishCaches(*check, cache);
      BOLT_ASSERT_THROW(
          check->finishStream(), failure == 0 ? "(1 vs. 2)" : "CRC mismatch");
    }
  }
}

TEST_F(CellShuffleCheckTest, validatesMappingCacheAndObserver) {
  auto batch = prepare({makeFlatVector<int64_t>({11, 22})}, {0, 1});
  auto check = CellShuffleCheck::create(
      input_->type()->asRow(), kPartitions, 1, 1, pool());
  ASSERT_NE(check, nullptr);
  counts_[0] = 0;
  BOLT_ASSERT_THROW(check->observeBatch(batch), "invalid partition counts");
  counts_[0] = 1;
  partitions_[0] = kPartitions;
  BOLT_ASSERT_THROW(check->observeBatch(batch), "invalid partition");
  partitions_[0] = 0;
  Cache cache;
  check->setColumnCache(0, cache.bytes.data());
  check->observeBatch(batch);
  EXPECT_THROW(
      check->beforeFullFlush<int32_t>(0, 0, cache.line(0), kCacheLineBytes),
      BoltException);
  EXPECT_THROW(check->beforeStreamFlush(0, 0, cache.line(1), 8), BoltException);
  EXPECT_THROW(check->beforeStreamFlush(0, 0, cache.line(0), 7), BoltException);
  EXPECT_THROW(
      check->beforeStreamFlush(0, kPartitions, cache.line(0), 8),
      BoltException);
  cache.set<int64_t>(0, {11});
  cache.set<int64_t>(1, {22});

  finishCaches(*check, cache);
  EXPECT_NO_THROW(check->finishStream());
}

TEST_F(CellShuffleCheckTest, floatingSpecialValuesAcrossFullCachesAndTails) {
  const auto run = [&]<typename T, typename U>(const std::vector<U>& bits) {
    constexpr uint32_t kRows = 137;
    const auto input = makeFlatVector<T>(kRows, [&](auto row) {
      return std::bit_cast<T>(bits[row % bits.size()]);
    });
    for (const bool corrupt : {false, true}) {
      auto batch = prepare({input}, std::vector<uint32_t>(kRows, 0));
      auto check = CellShuffleCheck::create(
          input_->type()->asRow(), kPartitions, 1, 1, pool());
      ASSERT_NE(check, nullptr);
      SizedCache cache(kPartitions);
      check->setColumnCache(0, cache.data());
      check->observeBatch(batch);
      for (uint32_t row = 0; row < kRows; ++row) {
        const U raw = bits[row % bits.size()];
        T actual = std::bit_cast<T>(raw);
        if (std::isnan(actual)) {
          actual = -std::numeric_limits<T>::quiet_NaN();
        } else if (actual == 0) {
          actual = -actual;
        }
        if (corrupt && row == kRows - 1) {
          actual = T{123};
        }
        cache.append<T>(*check, 0, 0, actual);
      }

      check->beforeStreamFlush(0, 0, cache.data(), cache.cursors[0]);
      if (corrupt) {
        BOLT_ASSERT_THROW(check->finishStream(), "CRC mismatch");
      } else {
        EXPECT_NO_THROW(check->finishStream());
      }
    }
  };
  run.template operator()<float, uint32_t>(
      {0,
       0x80000000,
       0x7f800000,
       0xff800000,
       0x7fc00001,
       0x7f800001,
       0xffc12345,
       0x3f800000,
       1});
  run.template operator()<double, uint64_t>(
      {0,
       0x8000000000000000ULL,
       0x7ff0000000000000ULL,
       0xfff0000000000000ULL,
       0x7ff8000000000001ULL,
       0x7ff0000000000001ULL,
       0xfff8123456789000ULL,
       0x3ff0000000000000ULL,
       1});
}

TEST_F(CellShuffleCheckTest, splitterChecksEncodedAndRawCaches) {
  auto batch = prepare(fixedColumns(1, false), {0});
  nullClasses_.assign(6, NullClass::kNoNulls);
  const auto layout = CellLayout::create(asRowType(input_->type()));
  CountingOutput output;
  ChunkAllocator allocator(pool(), 1 << 20, 256);
  CellSplitter splitter(layout, kPartitions, allocator, output, *pool(), true);
  auto check =
      CellShuffleCheck::create(*layout.rowType(), kPartitions, 1, 6, pool());
  ASSERT_NE(check, nullptr);
  splitter.setStreamCheck(check.get());
  check->observeBatch(batch);
  splitter.split(batch);
  EXPECT_EQ(splitter.cellDataBytes(), 0);

  batch = prepare(fixedColumns(0, false), {});
  check->observeBatch(batch);
  EXPECT_NO_THROW(splitter.split(batch));

  std::vector<uint32_t> pids(257);
  for (uint32_t row = 0; row < pids.size(); ++row) {
    pids[row] = row % kPartitions;
  }
  batch = prepare(fixedColumns(pids.size(), true), pids);
  check->observeBatch(batch);
  splitter.split(batch);

  EXPECT_GT(splitter.cellDataBytes(), 0); // Both kinds of kernel flushed.

  std::vector<VectorPtr> nulls;
  for (const auto& type : layout.rowType()->children()) {
    nulls.push_back(BaseVector::createNullConstant(type, 3, pool()));
  }
  batch = prepare(nulls, {0, 1, 2});
  nullClasses_.assign(6, NullClass::kAllNull);
  check->observeBatch(batch);
  splitter.split(batch);

  const auto bytesBefore = splitter.cellDataBytes();
  batch = prepare(fixedColumns(1, false), {1});
  nullClasses_.assign(6, NullClass::kNoNulls);
  check->observeBatch(batch);
  splitter.split(batch);

  EXPECT_EQ(splitter.cellDataBytes(), bytesBefore); // Tail-only checks.
  EXPECT_EQ(output.calls, 0);
  ShuffleWriterMetrics metrics;
  EXPECT_NO_THROW(splitter.finish(metrics, {}));
}

TEST_F(CellShuffleCheckTest, splitterChecksMappedInputsAndAllNullBranch) {
  constexpr uint32_t kRows = 257;
  const auto dictionary = BaseVector::wrapInDictionary(
      makeNulls(kRows, [](auto row) { return row % 5 == 0; }),
      makeIndices(kRows, [](auto row) { return (3 * row + 1) % 4; }),
      kRows,
      makeFlatVector<int64_t>({-31, 42, 19, 77}));
  const auto constant = BaseVector::wrapInConstant(
      kRows, 2, makeFlatVector<double>({1.0, 2.0, 19.5}), false);
  std::vector<uint32_t> pids(kRows);
  for (uint32_t row = 0; row < kRows; ++row) {
    pids[row] = row % kPartitions;
  }
  auto batch = prepare({dictionary, constant}, pids);
  nullClasses_[1] = NullClass::kNoNulls;
  const auto layout = CellLayout::create(asRowType(input_->type()));
  CountingOutput output;
  ChunkAllocator allocator(pool(), 1 << 20, 256);
  CellSplitter splitter(layout, kPartitions, allocator, output, *pool(), true);
  auto check =
      CellShuffleCheck::create(*layout.rowType(), kPartitions, 1, 2, pool());
  ASSERT_NE(check, nullptr);
  splitter.setStreamCheck(check.get());
  check->observeBatch(batch);
  splitter.split(batch);

  batch = prepare(
      {makeFlatVector<int64_t>({1}), makeFlatVector<double>({2.0})}, {0});
  nullClasses_.assign(2, NullClass::kAllNull);
  check->observeBatch(batch);
  EXPECT_NO_THROW(splitter.split(batch));
  EXPECT_EQ(output.calls, 0);
  ShuffleWriterMetrics metrics;
  BOLT_ASSERT_THROW(splitter.finish(metrics, {}), "count mismatch");
}

TEST_F(CellShuffleCheckTest, sampledSplitterAcrossMappingsAndSeals) {
  constexpr uint32_t kRows = 257;
  std::vector<uint32_t> pids(kRows);
  for (uint32_t row = 0; row < kRows; ++row) {
    pids[row] = row % kPartitions;
  }
  for (const bool streaming : {false, true}) {
    SCOPED_TRACE(streaming);
    for (const int64_t taskId : {0, 49, 50}) {
      auto batch = prepare(fixedColumns(kRows, true), pids);
      const auto layout = CellLayout::create(asRowType(input_->type()));
      CountingOutput output;
      ChunkAllocator allocator(pool(), 1 << 20, 256);
      CellSplitter splitter(
          layout, kPartitions, allocator, output, *pool(), streaming);
      auto check = CellShuffleCheck::create(
          *layout.rowType(), kPartitions, 0.5, 6, pool(), taskId);
      ASSERT_EQ(check != nullptr, taskId != 50);
      splitter.setStreamCheck(check.get());
      for (uint32_t b = 0; b < 5; ++b) {
        auto columns = fixedColumns(kRows, b % 2 != 0);
        for (auto& column : columns) {
          if (b == 2) {
            column = BaseVector::wrapInDictionary(
                makeNulls(kRows, [](auto row) { return row % 5 == 0; }),
                makeIndices(
                    kRows, [](auto row) { return (3 * row + 7) % kRows; }),
                kRows,
                column);
          } else if (b == 3) {
            column = BaseVector::wrapInConstant(kRows, 3, column, false);
          } else if (b == 4) {
            column =
                BaseVector::createNullConstant(column->type(), kRows, pool());
          }
        }
        batch = prepare(columns, pids);
        if (b == 4) {
          nullClasses_.assign(6, NullClass::kAllNull);
        }
        if (check != nullptr) {
          check->observeBatch(batch);
        }
        EXPECT_NO_THROW(splitter.split(batch));
        if (b == 0) {
          splitter.sealWindow({});
        } else if (b == 1) {
          splitter.sealPartitionsOver(1, {});
        } else if (b == 2) {
          splitter.spillAll();
        }
      }
      ShuffleWriterMetrics metrics;
      EXPECT_NO_THROW(splitter.finish(metrics, {}));
    }
  }
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
