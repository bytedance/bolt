#include "bolt/exec/bm/tests/BmRowContainerTestUtil.h"

namespace bytedance::bolt::exec {
namespace {

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


} // namespace
} // namespace bytedance::bolt::exec
