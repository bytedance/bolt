#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/exec/bm/BmRowContainer.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::exec {
namespace {

constexpr std::size_t kLargeBlockBytes = 4 * 1024 * 1024;

bool isIoUringUnavailable(const std::exception& e) {
  return std::string(e.what()).find("io_uring_queue_init failed") !=
      std::string::npos;
}

class BmRowContainerTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = memoryManager_.addRootPool(
        fmt::format(
            "bm-row-container-root-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        256 << 20,
        memory::MemoryReclaimer::create());
    leaf_ = root_->addLeafChild("bm-row-container-vector");
  }

  std::shared_ptr<memory::bm::BufferManager> makeBufferManager(
      const std::string& name,
      memory::MemoryPool* parent = nullptr) {
    const auto directory =
        memory::bm::test::UniqueTempDir(fmt::format("bm-row-container-{}", name));
    std::filesystem::remove_all(directory);

    memory::bm::BufferManagerConfig config;
    config.poolName = fmt::format("bm-row-container-{}", name);
    config.spillStoreConfig.fileAllocatorConfig =
        memory::bm::test::ValidConfigWithDirectory(directory);
    return memory::bm::BufferManager::Create(
        parent == nullptr ? *root_ : *parent, std::move(config));
  }

  memory::MemoryManager memoryManager_;
  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> leaf_;
};

template <typename T>
VectorPtr makeFlatVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    const std::vector<std::optional<T>>& values) {
  auto vector = BaseVector::create(type, values.size(), pool);
  auto* flat = vector->template asFlatVector<T>();
  auto* rawValues = flat->mutableRawValues();
  for (auto i = 0; i < values.size(); ++i) {
    if (values[i].has_value()) {
      rawValues[i] = values[i].value();
    } else {
      vector->setNull(i, true);
    }
  }
  return vector;
}

VectorPtr makeBigintVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<int64_t>> values) {
  return makeFlatVector<int64_t>(pool, BIGINT(), values);
}

VectorPtr makeIntegerVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<int32_t>> values) {
  return makeFlatVector<int32_t>(pool, INTEGER(), values);
}

VectorPtr makeDoubleVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<double>> values) {
  return makeFlatVector<double>(pool, DOUBLE(), values);
}

VectorPtr makeVarcharVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<StringView>> values) {
  return makeFlatVector<StringView>(pool, VARCHAR(), values);
}

RowVectorPtr makeRowVector(
    memory::MemoryPool* pool,
    std::vector<std::string> names,
    std::vector<VectorPtr> children) {
  std::vector<TypePtr> types;
  types.reserve(children.size());
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  return std::make_shared<RowVector>(
      pool,
      ROW(std::move(names), std::move(types)),
      nullptr,
      children.front()->size(),
      std::move(children));
}

TEST_F(BmRowContainerTest, RequiresBufferManager) {
  EXPECT_THROW(
      BmRowContainer({BIGINT()}, {}, nullptr), std::invalid_argument);
}

TEST_F(BmRowContainerTest, StoresAndExtractsFixedWidthColumnByRowRef) {
  auto bufferManager = makeBufferManager("fixed-width");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {11, 22, 33, 44});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  rows.reserve(input->size());
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto result = BaseVector::create(BIGINT(), input->size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, result);

  auto* actual = result->asFlatVector<int64_t>();
  ASSERT_EQ(input->size(), result->size());
  for (auto i = 0; i < input->size(); ++i) {
    EXPECT_FALSE(result->isNullAt(i));
    EXPECT_EQ(11 * (i + 1), actual->valueAt(i));
  }
}

TEST_F(BmRowContainerTest, ExposesRequiredAccessorsAndClear) {
  auto bufferManager = makeBufferManager("accessors");
  BmRowContainer container({BIGINT()}, {INTEGER()}, bufferManager);

  EXPECT_EQ(0, container.numRows());
  EXPECT_GT(container.fixedRowSize(), 0);
  EXPECT_EQ(0, container.allocatedBytes());
  EXPECT_EQ(0, container.usedBytes());
  EXPECT_EQ(std::nullopt, container.estimateRowSize());
  EXPECT_EQ(2, container.columns().size());
  EXPECT_EQ(2, container.columnTypes().size());
  EXPECT_EQ(1, container.keyTypes().size());

  auto input = makeBigintVector(leaf_.get(), {11});
  DecodedVector decoded(*input);
  auto row = container.newRow();
  container.store(decoded, 0, row, 0);

  EXPECT_EQ(1, container.numRows());
  ASSERT_TRUE(container.estimateRowSize().has_value());
  EXPECT_GT(container.estimateRowSize().value(), 0);

  container.clear();
  EXPECT_EQ(0, container.numRows());
  EXPECT_EQ(0, container.allocatedBytes());
  EXPECT_EQ(0, container.usedBytes());
  EXPECT_EQ(std::nullopt, container.estimateRowSize());
}

TEST_F(BmRowContainerTest, PreservesNullsInFixedWidthColumn) {
  auto bufferManager = makeBufferManager("nulls");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {11, std::nullopt, 33});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  rows.reserve(input->size());
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto result = BaseVector::create(BIGINT(), input->size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, result);

  auto* actual = result->asFlatVector<int64_t>();
  EXPECT_FALSE(result->isNullAt(0));
  EXPECT_EQ(11, actual->valueAt(0));
  EXPECT_TRUE(result->isNullAt(1));
  EXPECT_FALSE(result->isNullAt(2));
  EXPECT_EQ(33, actual->valueAt(2));
}

TEST_F(BmRowContainerTest, ExtractsNulls) {
  auto bufferManager = makeBufferManager("extract-nulls");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {11, std::nullopt, 33});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto nulls = allocateNulls(rows.size(), leaf_.get());
  container.extractNulls(rows.data(), rows.size(), 0, nulls);
  const auto* rawNulls = nulls->as<uint64_t>();
  EXPECT_FALSE(bits::isBitSet(rawNulls, 0));
  EXPECT_TRUE(bits::isBitSet(rawNulls, 1));
  EXPECT_FALSE(bits::isBitSet(rawNulls, 2));
}

TEST_F(BmRowContainerTest, ExtractsColumnAtResultOffset) {
  auto bufferManager = makeBufferManager("extract-offset");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {11, 22});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto result = BaseVector::create(BIGINT(), 4, leaf_.get());
  container.extractColumn(
      folly::Range<const RowId*>(rows.data(), rows.size()),
      0,
      1,
      result);

  auto* actual = result->asFlatVector<int64_t>();
  EXPECT_EQ(11, actual->valueAt(1));
  EXPECT_EQ(22, actual->valueAt(2));
}

TEST_F(BmRowContainerTest, ComparesFixedWidthColumnsAndRows) {
  auto bufferManager = makeBufferManager("fixed-compare");
  BmRowContainer container({BIGINT(), INTEGER()}, {}, bufferManager);

  auto bigints = makeBigintVector(leaf_.get(), {10, 20, 20, std::nullopt});
  auto integers = makeIntegerVector(leaf_.get(), {1, 1, 2, 1});
  DecodedVector decodedBigints(*bigints);
  DecodedVector decodedIntegers(*integers);

  std::vector<RowId> rows;
  for (auto i = 0; i < bigints->size(); ++i) {
    auto row = container.newRow();
    container.store(decodedBigints, i, row, 0);
    container.store(decodedIntegers, i, row, 1);
    rows.push_back(row);
  }

  EXPECT_LT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_GT(container.compare(rows[1], rows[0], 0), 0);
  EXPECT_GT(
      container.compare(
          rows[0], rows[1], 0, CompareFlags{.ascending = false}),
      0);
  EXPECT_LT(container.compareRows(rows[1], rows[2]), 0);
  EXPECT_EQ(container.compareRows(rows[1], rows[1]), 0);
  EXPECT_LT(container.compare(rows[3], rows[0], 0), 0);
  EXPECT_GT(
      container.compare(
          rows[3], rows[0], 0, CompareFlags{.nullsFirst = false}),
      0);
}

TEST_F(BmRowContainerTest, StoresAndExtractsMultipleKeyAndDependentColumns) {
  auto bufferManager = makeBufferManager("multi-column");
  BmRowContainer container({BIGINT(), INTEGER()}, {DOUBLE()}, bufferManager);

  auto bigints = makeBigintVector(leaf_.get(), {100, 200, 300});
  auto integers = makeIntegerVector(leaf_.get(), {10, std::nullopt, 30});
  auto doubles = makeDoubleVector(leaf_.get(), {1.5, 2.5, std::nullopt});
  DecodedVector decodedBigints(*bigints);
  DecodedVector decodedIntegers(*integers);
  DecodedVector decodedDoubles(*doubles);

  std::vector<RowId> rows;
  rows.reserve(bigints->size());
  for (auto i = 0; i < bigints->size(); ++i) {
    auto row = container.newRow();
    container.store(decodedBigints, i, row, 0);
    container.store(decodedIntegers, i, row, 1);
    container.store(decodedDoubles, i, row, 2);
    rows.push_back(row);
  }

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), leaf_.get());
  auto integerResult = BaseVector::create(INTEGER(), rows.size(), leaf_.get());
  auto doubleResult = BaseVector::create(DOUBLE(), rows.size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, bigintResult);
  container.extractColumn(rows.data(), rows.size(), 1, integerResult);
  container.extractColumn(rows.data(), rows.size(), 2, doubleResult);

  auto* actualBigints = bigintResult->asFlatVector<int64_t>();
  auto* actualIntegers = integerResult->asFlatVector<int32_t>();
  auto* actualDoubles = doubleResult->asFlatVector<double>();

  EXPECT_EQ(100, actualBigints->valueAt(0));
  EXPECT_EQ(200, actualBigints->valueAt(1));
  EXPECT_EQ(300, actualBigints->valueAt(2));

  EXPECT_EQ(10, actualIntegers->valueAt(0));
  EXPECT_TRUE(integerResult->isNullAt(1));
  EXPECT_EQ(30, actualIntegers->valueAt(2));

  EXPECT_DOUBLE_EQ(1.5, actualDoubles->valueAt(0));
  EXPECT_DOUBLE_EQ(2.5, actualDoubles->valueAt(1));
  EXPECT_TRUE(doubleResult->isNullAt(2));
}

TEST_F(BmRowContainerTest, StoresRowVectorBatch) {
  auto bufferManager = makeBufferManager("row-vector-store");
  BmRowContainer container({BIGINT()}, {VARCHAR()}, bufferManager);

  auto bigints = makeBigintVector(leaf_.get(), {10, 20, 30});
  auto strings = makeVarcharVector(
      leaf_.get(), {StringView("aa"), std::nullopt, StringView("cccc")});
  auto input =
      makeRowVector(leaf_.get(), {"c0", "c1"}, {bigints, strings});

  auto rows = container.store(input);
  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(3, container.numRows());

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), leaf_.get());
  auto stringResult = BaseVector::create(VARCHAR(), rows.size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, bigintResult);
  container.extractColumn(rows.data(), rows.size(), 1, stringResult);

  auto* actualBigints = bigintResult->asFlatVector<int64_t>();
  auto* actualStrings = stringResult->asFlatVector<StringView>();
  EXPECT_EQ(10, actualBigints->valueAt(0));
  EXPECT_EQ("aa", actualStrings->valueAt(0).getString());
  EXPECT_TRUE(stringResult->isNullAt(1));
  EXPECT_EQ("cccc", actualStrings->valueAt(2).getString());
}

TEST_F(BmRowContainerTest, TryStoreChecksNextRowBlockReservation) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-try-store-root",
      3 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("try-store", limitedRoot.get());
  BmRowContainer container({BIGINT()}, {}, bufferManager);
  auto input = makeRowVector(
      leaf_.get(),
      {"c0"},
      {makeBigintVector(leaf_.get(), {1})});

  EXPECT_FALSE(container.tryStore(input));
  EXPECT_EQ(0, container.allocatedBytes());
}

TEST_F(BmRowContainerTest, TryStoreChecksWholeBatchRowBlockReservation) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-try-store-batch-root",
      12 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("try-store-batch", limitedRoot.get());
  constexpr uint32_t kRowBlockSize = 8 * 1024 * 1024;
  BmRowContainer container(
      {BIGINT()},
      {},
      bufferManager,
      memory::bm::MemoryTag::kWindow,
      kRowBlockSize);

  const auto numRows = kRowBlockSize / container.fixedRowSize() + 1;
  auto input = makeRowVector(
      leaf_.get(),
      {"c0"},
      {makeBigintVector(
          leaf_.get(),
          std::vector<std::optional<int64_t>>(numRows, 1))});

  EXPECT_FALSE(container.tryStore(input));
  EXPECT_EQ(0, container.allocatedBytes());
}

TEST_F(BmRowContainerTest, TryStoreChecksVariableWidthInput) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-try-store-varchar-root",
      8 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("try-store-varchar", limitedRoot.get());
  constexpr uint32_t kHeapBlockSize = 4096;
  BmRowContainer container(
      {VARCHAR()},
      {},
      bufferManager,
      memory::bm::MemoryTag::kWindow,
      4096,
      kHeapBlockSize);

  const std::string payload(kHeapBlockSize + 1, 'x');
  auto input = makeRowVector(
      leaf_.get(),
      {"c0"},
      {makeVarcharVector(leaf_.get(), {StringView(payload)})});

  EXPECT_FALSE(container.tryStore(input));
  EXPECT_EQ(0, container.allocatedBytes());
}

TEST_F(BmRowContainerTest, ExtractsRowsAcrossMultipleBlocks) {
  auto bufferManager = makeBufferManager("multi-block");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {7});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  while (container.allocatedBytes() < 2 * kLargeBlockBytes) {
    auto row = container.newRow();
    container.store(decoded, 0, row, 0);
    if (sampledRows.empty() || row.blockId != sampledRows.back().blockId) {
      sampledRows.push_back(row);
    }
  }

  ASSERT_GE(sampledRows.size(), 2);
  auto result = BaseVector::create(BIGINT(), sampledRows.size(), leaf_.get());
  container.extractColumn(sampledRows.data(), sampledRows.size(), 0, result);

  auto* actual = result->asFlatVector<int64_t>();
  for (auto i = 0; i < sampledRows.size(); ++i) {
    EXPECT_FALSE(result->isNullAt(i));
    EXPECT_EQ(7, actual->valueAt(i));
  }
}

TEST_F(BmRowContainerTest, SpillsColdBlocksAndReadsThemBack) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-spill-root",
      20 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("spill", limitedRoot.get());
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {42});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  try {
    while (container.allocatedBytes() < 6 * kLargeBlockBytes) {
      auto row = container.newRow();
      container.store(decoded, 0, row, 0);
      if (sampledRows.empty() ||
          row.blockId != sampledRows.back().blockId) {
        sampledRows.push_back(row);
      }
    }
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  ASSERT_GE(sampledRows.size(), 6);
  const auto statsAfterAppend = bufferManager->stats();
  EXPECT_GE(statsAfterAppend.spillWriteCount, 1);
  EXPECT_GE(statsAfterAppend.spillWriteBytes, kLargeBlockBytes);

  for (const auto row : {sampledRows.front(), sampledRows.back()}) {
    auto result = BaseVector::create(BIGINT(), 1, leaf_.get());
    try {
      container.extractColumn(&row, 1, 0, result);
    } catch (const std::exception& e) {
      if (isIoUringUnavailable(e)) {
        GTEST_SKIP() << e.what();
      }
      throw;
    }

    auto* actual = result->asFlatVector<int64_t>();
    EXPECT_FALSE(result->isNullAt(0));
    EXPECT_EQ(42, actual->valueAt(0));
  }
  EXPECT_GE(bufferManager->stats().spillReadCount, 1);
}

TEST_F(BmRowContainerTest, PreloadBatchPinsSpilledBlocksForLaterAccess) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-preload-root",
      64 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("preload", limitedRoot.get());
  BmRowContainer container(
      {BIGINT()}, {}, bufferManager, memory::bm::MemoryTag::kWindow, 4096);

  auto input = makeBigintVector(leaf_.get(), {99});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  while (container.allocatedBytes() < 3 * 4096) {
    auto row = container.newRow();
    container.store(decoded, 0, row, 0);
    if (sampledRows.empty() || row.blockId != sampledRows.back().blockId) {
      sampledRows.push_back(row);
    }
  }
  ASSERT_GE(sampledRows.size(), 3);

  try {
    container.spillAllBlocksForBenchmark();
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  ASSERT_GE(bufferManager->stats().spillWriteCount, 1);

  std::vector<BlockId> blockIds{sampledRows.front().blockId};
  const auto statsBeforePreload = bufferManager->stats();
  try {
    container.preload(blockIds);
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  const auto statsAfterPreload = bufferManager->stats();
  EXPECT_EQ(
      statsBeforePreload.batchPinCount + 1,
      statsAfterPreload.batchPinCount);
  EXPECT_GE(
      statsAfterPreload.spillReadCount,
      statsBeforePreload.spillReadCount + 1);

  auto result = BaseVector::create(BIGINT(), 1, leaf_.get());
  container.extractColumn(&sampledRows.front(), 1, 0, result);
  EXPECT_EQ(
      statsAfterPreload.spillReadCount,
      bufferManager->stats().spillReadCount);
  EXPECT_EQ(99, result->asFlatVector<int64_t>()->valueAt(0));
}

TEST_F(BmRowContainerTest, StoresExtractsAndComparesVariableWidthColumns) {
  auto bufferManager = makeBufferManager("varchar");
  BmRowContainer container({VARCHAR()}, {}, bufferManager);

  const std::string largeValue(128, 'x');
  auto input = makeVarcharVector(
      leaf_.get(),
      {StringView("abc"),
       StringView("abcd"),
       std::nullopt,
       StringView(largeValue)});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto result = BaseVector::create(VARCHAR(), rows.size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, result);

  auto* actual = result->asFlatVector<StringView>();
  EXPECT_EQ("abc", actual->valueAt(0).getString());
  EXPECT_EQ("abcd", actual->valueAt(1).getString());
  EXPECT_TRUE(result->isNullAt(2));
  EXPECT_EQ(largeValue, actual->valueAt(3).getString());

  EXPECT_LT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_GT(container.compare(rows[1], rows[0], 0), 0);
  EXPECT_LT(container.compare(rows[2], rows[0], 0), 0);
}

TEST_F(BmRowContainerTest, StoresVariableWidthDataAcrossMultipleHeapBlocks) {
  auto bufferManager = makeBufferManager("multi-heap-block");
  BmRowContainer container({VARCHAR()}, {}, bufferManager);

  const std::string payload(1024, 'z');
  auto input = makeVarcharVector(leaf_.get(), {StringView(payload)});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  while (container.heapAllocatedBytes() < 2 * kLargeBlockBytes) {
    auto row = container.newRow();
    container.store(decoded, 0, row, 0);
    if (sampledRows.empty() ||
        row.blockId != sampledRows.back().blockId) {
      sampledRows.push_back(row);
    }
  }

  ASSERT_GE(container.heapAllocatedBytes(), 2 * kLargeBlockBytes);
  ASSERT_FALSE(sampledRows.empty());
  auto result = BaseVector::create(VARCHAR(), sampledRows.size(), leaf_.get());
  container.extractColumn(sampledRows.data(), sampledRows.size(), 0, result);

  auto* actual = result->asFlatVector<StringView>();
  for (auto i = 0; i < sampledRows.size(); ++i) {
    EXPECT_EQ(payload, actual->valueAt(i).getString());
  }
}

TEST_F(BmRowContainerTest, ExtractsVariableWidthWithExactSizeAtResultOffset) {
  auto bufferManager = makeBufferManager("varchar-exact-size-offset");
  BmRowContainer container({BIGINT()}, {VARCHAR()}, bufferManager);

  auto bigints = makeBigintVector(leaf_.get(), {10, 20, 30, 40});
  const std::string largeValue(256, 'e');
  auto strings = makeVarcharVector(
      leaf_.get(),
      {StringView("aa"),
       StringView(""),
       std::nullopt,
       StringView(largeValue)});
  DecodedVector decodedBigints(*bigints);
  DecodedVector decodedStrings(*strings);

  std::vector<RowId> rows;
  for (auto i = 0; i < bigints->size(); ++i) {
    auto row = container.newRow();
    container.store(decodedBigints, i, row, 0);
    container.store(decodedStrings, i, row, 1);
    rows.push_back(row);
  }

  auto result = BaseVector::create(VARCHAR(), rows.size() + 2, leaf_.get());
  container.extractColumn(
      folly::Range<const RowId*>(rows.data(), rows.size()),
      1,
      1,
      result,
      true);

  auto* actual = result->asFlatVector<StringView>();
  EXPECT_EQ("aa", actual->valueAt(1).getString());
  EXPECT_EQ("", actual->valueAt(2).getString());
  EXPECT_TRUE(result->isNullAt(3));
  EXPECT_EQ(largeValue, actual->valueAt(4).getString());
}

TEST_F(BmRowContainerTest, SpillsVariableWidthHeapBlocksAndReadsThemBack) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-heap-spill-root",
      24 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("heap-spill", limitedRoot.get());
  BmRowContainer container({VARCHAR()}, {}, bufferManager);

  const std::string payload(1024, 'h');
  auto input = makeVarcharVector(leaf_.get(), {StringView(payload)});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  try {
    while (container.heapAllocatedBytes() < 6 * kLargeBlockBytes) {
      auto row = container.newRow();
      container.store(decoded, 0, row, 0);
      if (rows.empty() || row.blockId != rows.back().blockId) {
        rows.push_back(row);
      }
    }
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }

  EXPECT_GE(bufferManager->stats().spillWriteCount, 1);
  auto result = BaseVector::create(VARCHAR(), 1, leaf_.get());
  try {
    container.extractColumn(&rows.front(), 1, 0, result);
  } catch (const std::exception& e) {
    if (isIoUringUnavailable(e)) {
      GTEST_SKIP() << e.what();
    }
    throw;
  }
  EXPECT_EQ(payload, result->asFlatVector<StringView>()->valueAt(0).getString());
  EXPECT_GE(bufferManager->stats().spillReadCount, 1);
}

TEST_F(BmRowContainerTest, FailsWhenSecondReserveCannotAllocateNewBlock) {
  auto limitedRoot = memoryManager_.addRootPool(
      "bm-row-container-reserve-fail-root",
      3 * 1024 * 1024,
      memory::MemoryReclaimer::create());
  auto bufferManager = makeBufferManager("reserve-fail", limitedRoot.get());
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  EXPECT_THROW(container.newRow(), std::exception);
}

} // namespace
} // namespace bytedance::bolt::exec
