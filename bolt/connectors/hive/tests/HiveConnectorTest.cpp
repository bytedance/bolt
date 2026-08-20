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

#include <gtest/gtest.h>
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"

#include <algorithm>
#include <numeric>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveConnectorUtil.h"
#include "bolt/connectors/hive/HiveDataSource.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/vector/LazyVector.h"
namespace bytedance::bolt::connector::hive {
namespace {
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::exec::test;

class TestingHiveDataSource;

class HiveConnectorTest : public exec::test::HiveConnectorTestBase {
 protected:
  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();

  RowTypePtr readerOutputReuseRowType() const {
    return ROW(
        {"c0", "c1", "c2", "c3", "c4"},
        {BIGINT(),
         ROW({"n0"}, {BIGINT()}),
         ARRAY(BIGINT()),
         MAP(BIGINT(), BIGINT()),
         VARCHAR()});
  }

  std::unique_ptr<TestingHiveDataSource> makeTestingDataSource(
      const RowTypePtr& rowType,
      std::unordered_map<std::string, std::string> queryConfig = {});
};

class FailingVectorLoader : public VectorLoader {
 public:
  void loadInternal(
      RowSet /*rows*/,
      ValueHook* /*hook*/,
      vector_size_t /*resultSize*/,
      VectorPtr* /*result*/) override {
    ADD_FAILURE() << "Preparing reader output must not load LazyVector";
  }
};

VectorPtr takeIfUnique(VectorPtr& vector) {
  if (vector && vector.use_count() == 1) {
    return std::move(vector);
  }
  return nullptr;
}

class TrackingSplitReader : public HiveSplitReaderBase {
 public:
  TrackingSplitReader(
      RowTypePtr rowType,
      memory::MemoryPool* pool,
      std::vector<vector_size_t> outputSizes = {1, 1},
      bool expectComplexShellReuse = false,
      bool outputLazyLeaf = false)
      : rowType_(std::move(rowType)),
        pool_(pool),
        outputSizes_(std::move(outputSizes)),
        expectComplexShellReuse_(expectComplexShellReuse),
        outputLazyLeaf_(outputLazyLeaf) {}

  uint64_t next(int64_t /*size*/, VectorPtr& output) override {
    auto rowOutput = std::dynamic_pointer_cast<RowVector>(output);
    EXPECT_NE(rowOutput, nullptr);
    if (rowOutput == nullptr) {
      return 0;
    }
    EXPECT_EQ(rowOutput->children().size(), rowType_->size());

    if (rowType_->size() == 0) {
      if (calls_ == 0) {
        backingOutput_ = rowOutput.get();
      }
      if (calls_ >= outputSizes_.size()) {
        return 0;
      }
      const auto outputSize = outputSizes_[calls_];
      rowOutput->unsafeResize(outputSize);
      ++calls_;
      return outputSize;
    }

    if (calls_ > 0) {
      EXPECT_EQ(rowOutput.get(), backingOutput_);
      EXPECT_EQ(rowOutput->size(), 0);
      EXPECT_NE(rowOutput->childAt(0), nullptr);
      if (rowOutput->childAt(0) == nullptr) {
        return 0;
      }
      EXPECT_FALSE(rowOutput->childAt(0)->isLazy());
      EXPECT_EQ(rowOutput->childAt(0)->size(), 0);
      expectFlatBuffersReleased(
          rowOutput->childAt(0),
          firstChildValues_.get(),
          firstChildNulls_.get());
      auto nestedRow =
          std::dynamic_pointer_cast<RowVector>(rowOutput->childAt(1));
      EXPECT_NE(nestedRow, nullptr);
      if (nestedRow == nullptr) {
        return 0;
      }
      EXPECT_EQ(nestedRow->size(), 0);
      EXPECT_NE(nestedRow->childAt(0), nullptr);
      if (nestedRow->childAt(0) == nullptr) {
        return 0;
      }
      EXPECT_EQ(nestedRow->childAt(0)->size(), 0);
      expectFlatBuffersReleased(
          nestedRow->childAt(0),
          firstNestedChildValues_.get(),
          firstNestedChildNulls_.get());
      if (expectComplexShellReuse_) {
        EXPECT_EQ(rowOutput->childAt(2).get(), firstArray_.lock().get());
        EXPECT_EQ(rowOutput->childAt(3).get(), firstMap_.lock().get());
        auto* array = rowOutput->childAt(2)->as<ArrayVector>();
        auto* map = rowOutput->childAt(3)->as<MapVector>();
        EXPECT_NE(array, nullptr);
        EXPECT_NE(map, nullptr);
        if (array == nullptr || map == nullptr) {
          return 0;
        }
        EXPECT_EQ(array->size(), 0);
        EXPECT_EQ(array->elements()->size(), 0);
        expectFlatBuffersReleased(
            array->elements(), firstArrayChildValues_.get(), nullptr);
        EXPECT_EQ(map->size(), 0);
        EXPECT_EQ(map->mapKeys()->size(), 0);
        EXPECT_EQ(map->mapValues()->size(), 0);
        expectFlatBuffersReleased(
            map->mapKeys(), firstMapKeyValues_.get(), nullptr);
        expectFlatBuffersReleased(
            map->mapValues(), firstMapValueValues_.get(), nullptr);
        expectStringBuffersReleased(
            rowOutput->childAt(4),
            firstStringValues_.get(),
            firstStringBuffer_.get());
        if (calls_ > 1) {
          EXPECT_EQ(rowOutput->childAt(1).get(), nestedShell_);
          EXPECT_EQ(rowOutput->childAt(2).get(), arrayShell_);
          EXPECT_EQ(rowOutput->childAt(3).get(), mapShell_);
        }
      } else {
        EXPECT_NE(rowOutput->childAt(2), nullptr);
        EXPECT_NE(rowOutput->childAt(3), nullptr);
        auto firstArray = firstArray_.lock();
        auto firstMap = firstMap_.lock();
        EXPECT_NE(firstArray, nullptr);
        EXPECT_NE(firstMap, nullptr);
        if (firstArray) {
          EXPECT_NE(rowOutput->childAt(2).get(), firstArray.get());
          EXPECT_EQ(firstArray.use_count(), 2);
        }
        if (firstMap) {
          EXPECT_NE(rowOutput->childAt(3).get(), firstMap.get());
          EXPECT_EQ(firstMap.use_count(), 2);
        }
      }

      if (!expectComplexShellReuse_) {
        auto firstChild = firstChild_.lock();
        EXPECT_NE(firstChild, nullptr);
        if (firstChild == nullptr) {
          return 0;
        }
        EXPECT_EQ(firstChild.use_count(), 2);
      }
    } else {
      backingOutput_ = rowOutput.get();
    }
    if (calls_ >= outputSizes_.size()) {
      return 0;
    }
    const auto outputSize = outputSizes_[calls_];

    auto makeChild = [&](int64_t value) {
      auto values = AlignedBuffer::allocate<int64_t>(outputSize, pool_);
      auto nulls = allocateNulls(outputSize, pool_, bits::kNull);
      for (auto i = 0; i < outputSize; ++i) {
        values->asMutable<int64_t>()[i] = value + i;
        bits::setNull(nulls->asMutable<uint64_t>(), i, false);
      }
      values->setSize(outputSize * sizeof(int64_t));
      return std::make_shared<FlatVector<int64_t>>(
          pool_, BIGINT(), nulls, outputSize, values, std::vector<BufferPtr>{});
    };
    auto makeStringChild = [&]() {
      auto values = AlignedBuffer::allocate<StringView>(outputSize, pool_);
      auto stringBuffer = AlignedBuffer::allocate<char>(outputSize * 8, pool_);
      auto* rawStringBuffer = stringBuffer->asMutable<char>();
      vector_size_t stringOffset = 0;
      for (auto i = 0; i < outputSize; ++i) {
        const auto value =
            "value-" + std::to_string(calls_) + "-" + std::to_string(i);
        std::copy(value.begin(), value.end(), rawStringBuffer + stringOffset);
        values->asMutable<StringView>()[i] =
            StringView(rawStringBuffer + stringOffset, value.size());
        stringOffset += value.size();
      }
      values->setSize(outputSize * sizeof(StringView));
      stringBuffer->setSize(stringOffset);
      return std::make_shared<FlatVector<StringView>>(
          pool_,
          VARCHAR(),
          nullptr,
          outputSize,
          values,
          std::vector<BufferPtr>{stringBuffer});
    };
    auto child = makeChild(calls_);
    auto nestedChild = makeChild(calls_ + 10);
    auto arrayChild = makeChild(calls_ + 20);
    auto mapKeyChild = makeChild(calls_ + 30);
    auto mapValueChild = makeChild(calls_ + 40);
    auto stringChild = makeStringChild();

    rowOutput->unsafeResize(outputSize);
    if (outputLazyLeaf_) {
      std::weak_ptr<BaseVector> emptyLeaf = rowOutput->childAt(0);
      rowOutput->childAt(0) = std::make_shared<LazyVector>(
          pool_,
          rowType_->childAt(0),
          outputSize,
          std::make_unique<FailingVectorLoader>(),
          takeIfUnique(rowOutput->childAt(0)));
      if (auto empty = emptyLeaf.lock()) {
        EXPECT_EQ(empty.use_count(), 2);
      }
    } else {
      rowOutput->childAt(0) = child;
    }
    rowOutput->childAt(4) = stringChild;
    auto& nestedResult = rowOutput->childAt(1);
    auto* nestedRow =
        nestedResult && nestedResult->encoding() == VectorEncoding::Simple::ROW
        ? nestedResult->as<RowVector>()
        : nullptr;
    if (!nestedRow || rowOutput->childAt(1).use_count() != 1) {
      ++createdRowShells_;
      rowOutput->childAt(1) = std::make_shared<RowVector>(
          pool_,
          rowType_->childAt(1),
          nullptr,
          outputSize,
          std::vector<VectorPtr>{nestedChild});
    } else {
      nestedRow->unsafeResize(outputSize);
      nestedRow->childAt(0) = nestedChild;
    }
    auto offsets = AlignedBuffer::allocate<vector_size_t>(outputSize, pool_);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    std::iota(rawOffsets, rawOffsets + outputSize, 0);
    auto sizes = AlignedBuffer::allocate<vector_size_t>(outputSize, pool_, 1);
    auto& arrayResult = rowOutput->childAt(2);
    auto* array =
        arrayResult && arrayResult->encoding() == VectorEncoding::Simple::ARRAY
        ? arrayResult->as<ArrayVector>()
        : nullptr;
    if (!array || rowOutput->childAt(2).use_count() != 1) {
      ++createdArrayShells_;
      rowOutput->childAt(2) = std::make_shared<ArrayVector>(
          pool_,
          rowType_->childAt(2),
          nullptr,
          outputSize,
          offsets,
          sizes,
          arrayChild);
    } else {
      array->resize(outputSize);
      auto* rawOffsets =
          array->mutableOffsets(outputSize)->asMutable<vector_size_t>();
      auto* rawSizes =
          array->mutableSizes(outputSize)->asMutable<vector_size_t>();
      std::iota(rawOffsets, rawOffsets + outputSize, 0);
      std::fill(rawSizes, rawSizes + outputSize, 1);
      array->elements() = arrayChild;
    }
    auto& mapResult = rowOutput->childAt(3);
    auto* map =
        mapResult && mapResult->encoding() == VectorEncoding::Simple::MAP
        ? mapResult->as<MapVector>()
        : nullptr;
    if (!map || rowOutput->childAt(3).use_count() != 1) {
      ++createdMapShells_;
      rowOutput->childAt(3) = std::make_shared<MapVector>(
          pool_,
          rowType_->childAt(3),
          nullptr,
          outputSize,
          offsets,
          sizes,
          mapKeyChild,
          mapValueChild);
    } else {
      map->resize(outputSize);
      auto* rawOffsets =
          map->mutableOffsets(outputSize)->asMutable<vector_size_t>();
      auto* rawSizes =
          map->mutableSizes(outputSize)->asMutable<vector_size_t>();
      std::iota(rawOffsets, rawOffsets + outputSize, 0);
      std::fill(rawSizes, rawSizes + outputSize, 1);
      map->mapKeys() = mapKeyChild;
      map->mapValues() = mapValueChild;
    }
    if (calls_ == 0) {
      firstChild_ = child;
      firstChildValues_ = child->as<FlatVector<int64_t>>()->values();
      firstChildNulls_ = child->nulls();
      firstNestedChildValues_ =
          nestedChild->as<FlatVector<int64_t>>()->values();
      firstNestedChildNulls_ = nestedChild->nulls();
      firstArrayChildValues_ = arrayChild->as<FlatVector<int64_t>>()->values();
      firstMapKeyValues_ = mapKeyChild->as<FlatVector<int64_t>>()->values();
      firstMapValueValues_ = mapValueChild->as<FlatVector<int64_t>>()->values();
      firstStringValues_ = stringChild->as<FlatVector<StringView>>()->values();
      firstStringBuffer_ =
          stringChild->as<FlatVector<StringView>>()->stringBuffers().front();
      firstArray_ = rowOutput->childAt(2);
      firstMap_ = rowOutput->childAt(3);
      nestedShell_ = rowOutput->childAt(1).get();
      arrayShell_ = rowOutput->childAt(2).get();
      mapShell_ = rowOutput->childAt(3).get();
    }
    rowOutput->updateContainsLazyNotLoaded();
    ++calls_;
    return outputSize;
  }

  bool allPrefetchIssued() const override {
    return true;
  }

  bool emptySplit() const override {
    return false;
  }

  void resetFilterCaches() override {}

  int64_t estimatedRowSize() const override {
    return sizeof(int64_t);
  }

  void updateRuntimeStats(dwio::common::RuntimeStatistics&) const override {}

  void resetSplit() override {}

  int32_t createdComplexShells() const {
    return createdRowShells_ + createdArrayShells_ + createdMapShells_;
  }

 private:
  void expectFlatBuffersReleased(
      const VectorPtr& vector,
      const Buffer* previousValues,
      const Buffer* previousNulls) const {
    auto* flat = vector->as<FlatVector<int64_t>>();
    EXPECT_NE(flat, nullptr);
    if (flat != nullptr) {
      EXPECT_NE(flat->values().get(), previousValues);
      if (previousNulls != nullptr) {
        EXPECT_NE(vector->nulls().get(), previousNulls);
      }
    }
  }

  void expectStringBuffersReleased(
      const VectorPtr& vector,
      const Buffer* previousValues,
      const Buffer* previousStringBuffer) const {
    auto* flat = vector->as<FlatVector<StringView>>();
    EXPECT_NE(flat, nullptr);
    if (flat != nullptr) {
      EXPECT_NE(flat->values().get(), previousValues);
      for (const auto& buffer : flat->stringBuffers()) {
        EXPECT_NE(buffer.get(), previousStringBuffer);
      }
    }
  }

  RowTypePtr rowType_;
  memory::MemoryPool* pool_;
  std::vector<vector_size_t> outputSizes_;
  bool expectComplexShellReuse_;
  bool outputLazyLeaf_;
  int32_t calls_{0};
  RowVector* backingOutput_{nullptr};
  BaseVector* nestedShell_{nullptr};
  BaseVector* arrayShell_{nullptr};
  BaseVector* mapShell_{nullptr};
  std::weak_ptr<BaseVector> firstChild_;
  std::weak_ptr<BaseVector> firstArray_;
  std::weak_ptr<BaseVector> firstMap_;
  BufferPtr firstChildValues_;
  BufferPtr firstChildNulls_;
  BufferPtr firstNestedChildValues_;
  BufferPtr firstNestedChildNulls_;
  BufferPtr firstArrayChildValues_;
  BufferPtr firstMapKeyValues_;
  BufferPtr firstMapValueValues_;
  BufferPtr firstStringValues_;
  BufferPtr firstStringBuffer_;
  int32_t createdRowShells_{0};
  int32_t createdArrayShells_{0};
  int32_t createdMapShells_{0};
};

class SequentialSplitReader : public HiveSplitReaderBase {
 public:
  SequentialSplitReader(RowTypePtr rowType, memory::MemoryPool* pool)
      : rowType_(std::move(rowType)), pool_(pool) {}

  uint64_t next(int64_t /*size*/, VectorPtr& output) override {
    if (calls_ >= 2) {
      return 0;
    }

    auto rowOutput = std::dynamic_pointer_cast<RowVector>(output);
    EXPECT_NE(rowOutput, nullptr);
    if (rowOutput == nullptr) {
      return 0;
    }
    readerOutputs_.push_back(output.get());
    rowOutput->unsafeResize(1);

    auto values = AlignedBuffer::allocate<int64_t>(1, pool_);
    values->asMutable<int64_t>()[0] = calls_ + 1;
    values->setSize(sizeof(int64_t));
    rowOutput->childAt(0) = std::make_shared<FlatVector<int64_t>>(
        pool_, BIGINT(), nullptr, 1, values, std::vector<BufferPtr>{});

    ++calls_;
    return 1;
  }

  bool allPrefetchIssued() const override {
    return true;
  }

  bool emptySplit() const override {
    return false;
  }

  void resetFilterCaches() override {}

  int64_t estimatedRowSize() const override {
    return sizeof(int64_t);
  }

  void updateRuntimeStats(dwio::common::RuntimeStatistics&) const override {}

  void resetSplit() override {}

  const std::vector<BaseVector*>& readerOutputs() const {
    return readerOutputs_;
  }

 private:
  RowTypePtr rowType_;
  memory::MemoryPool* pool_;
  int32_t calls_{0};
  std::vector<BaseVector*> readerOutputs_;
};

class TestingHiveDataSource : public HiveDataSource {
 public:
  TestingHiveDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* fileHandleFactory,
      const core::QueryConfig& queryConfig,
      folly::Executor* executor,
      const std::shared_ptr<ConnectorQueryCtx>& connectorQueryCtx,
      const std::shared_ptr<HiveConfig>& hiveConfig)
      : HiveDataSource(
            outputType,
            tableHandle,
            columnHandles,
            fileHandleFactory,
            queryConfig,
            executor,
            connectorQueryCtx,
            hiveConfig) {}

  void setSplitReader(std::unique_ptr<HiveSplitReaderBase> splitReader) {
    splitReader_ = std::move(splitReader);
  }

  void setSplit(std::shared_ptr<ConnectorSplit> split) {
    split_ = std::move(split);
  }
};

std::unique_ptr<TestingHiveDataSource> HiveConnectorTest::makeTestingDataSource(
    const RowTypePtr& rowType,
    std::unordered_map<std::string, std::string> queryConfig) {
  auto tableHandle = makeTableHandle({}, nullptr, "test_table", rowType);
  ColumnHandleMap assignments;
  for (const auto& name : rowType->names()) {
    assignments[name] = regularColumn(name, rowType->findChild(name));
  }
  auto hiveConfig =
      std::make_shared<HiveConfig>(std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{}));
  auto connectorQueryCtx = std::make_shared<ConnectorQueryCtx>(
      pool_.get(),
      pool_.get(),
      hiveConfig->config().get(),
      nullptr,
      nullptr,
      nullptr,
      nullptr,
      "query.HiveConnectorTest",
      "task.HiveConnectorTest",
      "planNodeId.HiveConnectorTest",
      0);

  auto dataSource = std::make_unique<TestingHiveDataSource>(
      rowType,
      tableHandle,
      assignments,
      nullptr,
      core::QueryConfig(std::move(queryConfig)),
      nullptr,
      connectorQueryCtx,
      hiveConfig);
  dataSource->setSplit(HiveConnectorSplitBuilder("unused")
                           .connectorId(kHiveConnectorId)
                           .build());
  return dataSource;
}

void validateNullConstant(const ScanSpec& spec, const Type& type) {
  ASSERT_TRUE(spec.isConstant());
  auto constant = spec.constantValue();
  ASSERT_TRUE(constant->isConstantEncoding());
  ASSERT_EQ(*constant->type(), type);
  ASSERT_TRUE(constant->isNullAt(0));
}

std::vector<Subfield> makeSubfields(const std::vector<std::string>& paths) {
  std::vector<Subfield> subfields;
  for (auto& path : paths) {
    subfields.emplace_back(path);
  }
  return subfields;
}

folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
groupSubfields(const std::vector<Subfield>& subfields) {
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>> grouped;
  for (auto& subfield : subfields) {
    auto& name =
        static_cast<const common::Subfield::NestedField&>(*subfield.path()[0])
            .name();
    grouped[name].push_back(&subfield);
  }
  return grouped;
}

TEST_F(HiveConnectorTest, hiveConfig) {
  ASSERT_EQ(
      HiveConfig::insertExistingPartitionsBehaviorString(
          HiveConfig::InsertExistingPartitionsBehavior::kError),
      "ERROR");
  ASSERT_EQ(
      HiveConfig::insertExistingPartitionsBehaviorString(
          HiveConfig::InsertExistingPartitionsBehavior::kOverwrite),
      "OVERWRITE");
  ASSERT_EQ(
      HiveConfig::insertExistingPartitionsBehaviorString(
          static_cast<HiveConfig::InsertExistingPartitionsBehavior>(100)),
      "UNKNOWN BEHAVIOR 100");
}

TEST_F(
    HiveConnectorTest,
    prepareReaderOutputReplacesSharedComplexVectorsBeforeNextRead) {
  auto rowType = readerOutputReuseRowType();
  auto dataSource = makeTestingDataSource(rowType);
  auto splitReader =
      std::make_unique<TrackingSplitReader>(rowType, pool_.get());
  auto* rawSplitReader = splitReader.get();
  dataSource->setSplitReader(std::move(splitReader));

  ContinueFuture future;
  auto first = dataSource->next(1, future);
  ASSERT_TRUE(first.has_value());
  ASSERT_NE(first.value(), nullptr);

  auto second = dataSource->next(1, future);
  ASSERT_TRUE(second.has_value());
  ASSERT_NE(second.value(), nullptr);
  EXPECT_EQ(rawSplitReader->createdComplexShells(), 0);
}

TEST_F(HiveConnectorTest, prepareReaderOutputReusesUniqueComplexVectors) {
  auto rowType = readerOutputReuseRowType();
  auto dataSource = makeTestingDataSource(rowType);
  auto splitReader = std::make_unique<TrackingSplitReader>(
      rowType, pool_.get(), std::vector<vector_size_t>{1, 1, 1}, true);
  auto* rawSplitReader = splitReader.get();
  dataSource->setSplitReader(std::move(splitReader));

  ContinueFuture future;
  auto first = dataSource->next(1, future);
  ASSERT_TRUE(first.has_value());
  ASSERT_NE(first.value(), nullptr);
  EXPECT_EQ(rawSplitReader->createdComplexShells(), 0);
  first.reset();

  auto second = dataSource->next(1, future);
  ASSERT_TRUE(second.has_value());
  ASSERT_NE(second.value(), nullptr);
  EXPECT_EQ(rawSplitReader->createdComplexShells(), 0);
  second.reset();

  auto third = dataSource->next(1, future);
  ASSERT_TRUE(third.has_value());
  ASSERT_NE(third.value(), nullptr);
  EXPECT_EQ(rawSplitReader->createdComplexShells(), 0);
}

TEST_F(HiveConnectorTest, prepareReaderOutputClearsLazyLeafToEmptyLeaf) {
  auto rowType = readerOutputReuseRowType();
  auto dataSource = makeTestingDataSource(rowType);
  dataSource->setSplitReader(std::make_unique<TrackingSplitReader>(
      rowType, pool_.get(), std::vector<vector_size_t>{1, 1}, true, true));

  ContinueFuture future;
  auto first = dataSource->next(1, future);
  ASSERT_TRUE(first.has_value());
  ASSERT_NE(first.value(), nullptr);
  first.reset();

  auto second = dataSource->next(1, future);
  ASSERT_TRUE(second.has_value());
  ASSERT_NE(second.value(), nullptr);
}

TEST_F(HiveConnectorTest, emptyOutputDoesNotExposeReaderOutput) {
  auto rowType = ROW({}, {});
  auto dataSource = makeTestingDataSource(rowType);
  dataSource->setSplitReader(std::make_unique<TrackingSplitReader>(
      rowType, pool_.get(), std::vector<vector_size_t>{2, 3}));

  ContinueFuture future;
  auto first = dataSource->next(1, future);
  ASSERT_TRUE(first.has_value());
  ASSERT_NE(first.value(), nullptr);
  EXPECT_EQ(first.value()->childrenSize(), 0);
  EXPECT_EQ(first.value()->size(), 2);

  auto second = dataSource->next(1, future);
  ASSERT_TRUE(second.has_value());
  ASSERT_NE(second.value(), nullptr);
  EXPECT_EQ(second.value()->childrenSize(), 0);
  EXPECT_EQ(second.value()->size(), 3);
  EXPECT_EQ(first.value()->size(), 2);
}

TEST_F(HiveConnectorTest, tableScanReusableOutputDoesNotOverwriteHeldChild) {
  auto rowType = ROW({"c0"}, {BIGINT()});
  auto dataSource = makeTestingDataSource(
      rowType, {{core::QueryConfig::kTableScanReusableOutputCount, "1"}});
  dataSource->setSplitReader(
      std::make_unique<SequentialSplitReader>(rowType, pool_.get()));

  ContinueFuture future;
  auto first = dataSource->next(1, future);
  ASSERT_TRUE(first.has_value());
  ASSERT_NE(first.value(), nullptr);
  ASSERT_EQ(first.value()->childrenSize(), 1);
  auto heldChild = first.value()->childAt(0);
  ASSERT_NE(heldChild, nullptr);
  auto* heldFlat = heldChild->as<FlatVector<int64_t>>();
  ASSERT_NE(heldFlat, nullptr);
  EXPECT_EQ(heldFlat->valueAt(0), 1);

  auto second = dataSource->next(1, future);
  ASSERT_TRUE(second.has_value());
  ASSERT_NE(second.value(), nullptr);
  ASSERT_EQ(second.value()->childrenSize(), 1);
  auto* secondFlat = second.value()->childAt(0)->as<FlatVector<int64_t>>();
  ASSERT_NE(secondFlat, nullptr);
  EXPECT_EQ(secondFlat->valueAt(0), 2);
  EXPECT_EQ(heldFlat->valueAt(0), 1);
}

TEST_F(HiveConnectorTest, tableScanReusableOutputReusesReleasedRoot) {
  auto rowType = ROW({"c0"}, {BIGINT()});
  auto dataSource = makeTestingDataSource(
      rowType, {{core::QueryConfig::kTableScanReusableOutputCount, "1"}});
  auto splitReader =
      std::make_unique<SequentialSplitReader>(rowType, pool_.get());
  auto* rawSplitReader = splitReader.get();
  dataSource->setSplitReader(std::move(splitReader));

  ContinueFuture future;
  {
    auto first = dataSource->next(1, future);
    ASSERT_TRUE(first.has_value());
    ASSERT_NE(first.value(), nullptr);
  }
  ASSERT_EQ(rawSplitReader->readerOutputs().size(), 1);
  auto* firstReaderOutput = rawSplitReader->readerOutputs()[0];

  auto second = dataSource->next(1, future);
  ASSERT_TRUE(second.has_value());
  ASSERT_NE(second.value(), nullptr);
  ASSERT_EQ(rawSplitReader->readerOutputs().size(), 2);
  EXPECT_EQ(rawSplitReader->readerOutputs()[1], firstReaderOutput);
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_multilevel) {
  auto columnType = ROW(
      {{"c0c0", BIGINT()},
       {"c0c1",
        ARRAY(MAP(
            VARCHAR(), ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})))}});
  auto rowType = ROW({{"c0", columnType}});
  auto subfields = makeSubfields({"c0.c0c1[3][\"foo\"].c0c1c0"});
  auto scanSpec = makeScanSpec(
      rowType, groupSubfields(subfields), {}, nullptr, {}, {}, pool_.get());
  auto* c0c0 = scanSpec->childByName("c0")->childByName("c0c0");
  validateNullConstant(*c0c0, *BIGINT());
  auto* c0c1 = scanSpec->childByName("c0")->childByName("c0c1");
  ASSERT_EQ(c0c1->maxArrayElementsCount(), 3);
  auto* elements = c0c1->childByName(ScanSpec::kArrayElementsFieldName);
  auto* keysFilter =
      elements->childByName(ScanSpec::kMapKeysFieldName)->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_TRUE(applyFilter(*keysFilter, "foo"_sv));
  ASSERT_FALSE(applyFilter(*keysFilter, "bar"_sv));
  ASSERT_FALSE(keysFilter->testNull());
  auto* values = elements->childByName(ScanSpec::kMapValuesFieldName);
  auto* c0c1c0 = values->childByName("c0c1c0");
  ASSERT_FALSE(c0c1c0->isConstant());
  ASSERT_FALSE(c0c1c0->filter());
  validateNullConstant(*values->childByName("c0c1c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeFields) {
  auto columnType = ROW(
      {{"c0c0",
        ROW(
            {{"c0c0c0", BIGINT()},
             {"c0c0c1", BIGINT()},
             {"c0c0c2", BIGINT()}})},
       {"c0c1", ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})}});
  auto rowType = ROW({{"c0", columnType}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0.c0c0.c0c0c0", "c0.c0c0.c0c0c2", "c0.c0c1", "c0.c0c1.c0c1c0"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0c0 = scanSpec->childByName("c0")->childByName("c0c0");
  ASSERT_FALSE(c0c0->childByName("c0c0c0")->isConstant());
  ASSERT_FALSE(c0c0->childByName("c0c0c2")->isConstant());
  validateNullConstant(*c0c0->childByName("c0c0c1"), *BIGINT());
  auto* c0c1 = scanSpec->childByName("c0")->childByName("c0c1");
  ASSERT_FALSE(c0c1->isConstant());
  ASSERT_FALSE(c0c1->hasFilter());
  ASSERT_FALSE(c0c1->childByName("c0c1c0")->isConstant());
  ASSERT_FALSE(c0c1->childByName("c0c1c1")->isConstant());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeArray) {
  auto columnType =
      ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto rowType = ROW({{"c0", columnType}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[1].c0c0", "c0[2].c0c2"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_EQ(c0->maxArrayElementsCount(), 2);
  auto* elements = c0->childByName(ScanSpec::kArrayElementsFieldName);
  ASSERT_FALSE(elements->childByName("c0c0")->isConstant());
  ASSERT_FALSE(elements->childByName("c0c2")->isConstant());
  validateNullConstant(*elements->childByName("c0c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeArrayNegative) {
  auto columnType =
      ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto rowType = ROW({{"c0", columnType}});
  auto subfields = makeSubfields({"c0[1].c0c0", "c0[-1].c0c2"});
  auto groupedSubfields = groupSubfields(subfields);
  BOLT_ASSERT_USER_THROW(
      makeScanSpec(rowType, groupedSubfields, {}, nullptr, {}, {}, pool_.get()),
      "Non-positive array subscript cannot be push down");
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeMap) {
  auto columnType =
      MAP(BIGINT(),
          ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto rowType = ROW({{"c0", columnType}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[10].c0c0", "c0[20].c0c2"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  auto* keysFilter = c0->childByName(ScanSpec::kMapKeysFieldName)->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_TRUE(applyFilter(*keysFilter, 10));
  ASSERT_TRUE(applyFilter(*keysFilter, 20));
  ASSERT_FALSE(applyFilter(*keysFilter, 15));
  auto* values = c0->childByName(ScanSpec::kMapValuesFieldName);
  ASSERT_FALSE(values->childByName("c0c0")->isConstant());
  ASSERT_FALSE(values->childByName("c0c2")->isConstant());
  validateNullConstant(*values->childByName("c0c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_allSubscripts) {
  auto columnType =
      MAP(BIGINT(), ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}})));
  auto rowType = ROW({{"c0", columnType}});
  for (auto* path : {"c0", "c0[*]", "c0[*][*]"}) {
    SCOPED_TRACE(path);
    auto scanSpec = makeScanSpec(
        rowType,
        groupSubfields(makeSubfields({path})),
        {},
        nullptr,
        {},
        {},
        pool_.get());
    auto* c0 = scanSpec->childByName("c0");
    ASSERT_FALSE(c0->childByName(ScanSpec::kMapKeysFieldName)->filter());
    auto* values = c0->childByName(ScanSpec::kMapValuesFieldName);
    ASSERT_EQ(
        values->maxArrayElementsCount(),
        std::numeric_limits<vector_size_t>::max());
    auto* elements = values->childByName(ScanSpec::kArrayElementsFieldName);
    ASSERT_FALSE(elements->hasFilter());
    ASSERT_FALSE(elements->childByName("c0c0")->isConstant());
    ASSERT_FALSE(elements->childByName("c0c1")->isConstant());
  }
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[*][*].c0c0"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_FALSE(c0->childByName(ScanSpec::kMapKeysFieldName)->filter());
  auto* values = c0->childByName(ScanSpec::kMapValuesFieldName);
  ASSERT_EQ(
      values->maxArrayElementsCount(),
      std::numeric_limits<vector_size_t>::max());
  auto* elements = values->childByName(ScanSpec::kArrayElementsFieldName);
  ASSERT_FALSE(elements->hasFilter());
  ASSERT_FALSE(elements->childByName("c0c0")->isConstant());
  validateNullConstant(*elements->childByName("c0c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_doubleMapKey) {
  auto rowType =
      ROW({{"c0", MAP(REAL(), BIGINT())}, {"c1", MAP(DOUBLE(), BIGINT())}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[0]", "c1[-1]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* keysFilter = scanSpec->childByName("c0")
                         ->childByName(ScanSpec::kMapKeysFieldName)
                         ->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_TRUE(applyFilter(*keysFilter, 0.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, 0.99f));
  ASSERT_FALSE(applyFilter(*keysFilter, 1.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, -0.99f));
  ASSERT_FALSE(applyFilter(*keysFilter, -1.0f));
  keysFilter = scanSpec->childByName("c1")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_FALSE(applyFilter(*keysFilter, 0.0));
  ASSERT_TRUE(applyFilter(*keysFilter, -1.0));
  ASSERT_TRUE(applyFilter(*keysFilter, -1.99));
  ASSERT_FALSE(applyFilter(*keysFilter, -2.0));

  // Integer min and max means infinities.
  scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0[-9223372036854775808]", "c1[9223372036854775807]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  keysFilter = scanSpec->childByName("c0")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, -1e30f));
  ASSERT_FALSE(applyFilter(*keysFilter, -9223370000000000000.0f));
  keysFilter = scanSpec->childByName("c1")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, 1e100));
  ASSERT_FALSE(applyFilter(*keysFilter, 9223372036854700000.0));
  scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0[9223372036854775807]", "c0[-9223372036854775808]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  keysFilter = scanSpec->childByName("c0")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, -1e30f));
  ASSERT_FALSE(applyFilter(*keysFilter, 0.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, 1e30f));

  // Unrepresentable values.
  scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[-100000000]", "c0[100000000]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  keysFilter = scanSpec->childByName("c0")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, -100000000.0f));
  ASSERT_FALSE(applyFilter(*keysFilter, -100000008.0f));
  ASSERT_FALSE(applyFilter(*keysFilter, 0.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, 100000000.0f));
  ASSERT_FALSE(applyFilter(*keysFilter, 100000008.0f));
}

TEST_F(HiveConnectorTest, makeScanSpec_filtersNotInRequiredSubfields) {
  auto c0Type = ROW({
      {"c0c0", BIGINT()},
      {"c0c1", VARCHAR()},
      {"c0c2", ROW({{"c0c2c0", BIGINT()}})},
      {"c0c3", ROW({{"c0c3c0", BIGINT()}})},
      {"c0c4", BIGINT()},
  });
  auto c1c1Type = ROW({{"c1c1c0", BIGINT()}, {"c1c1c1", BIGINT()}});
  auto c1Type = ROW({
      {"c1c0", ROW({{"c1c0c0", BIGINT()}, {"c1c0c1", BIGINT()}})},
      {"c1c1", c1c1Type},
  });
  SubfieldFilters filters;
  filters.emplace(Subfield("c0.c0c0"), exec::equal(42));
  filters.emplace(Subfield("c0.c0c2"), exec::isNotNull());
  filters.emplace(Subfield("c0.c0c3"), exec::isNotNull());
  filters.emplace(Subfield("c1.c1c0.c1c0c0"), exec::equal(43));
  auto scanSpec = makeScanSpec(
      ROW({{"c0", c0Type}}),
      groupSubfields(makeSubfields({"c0.c0c1", "c0.c0c3"})),
      filters,
      ROW({{"c0", c0Type}, {"c1", c1Type}}),
      {},
      {},
      pool_.get());
  auto c0 = scanSpec->childByName("c0");
  ASSERT_FALSE(c0->isConstant());
  ASSERT_TRUE(c0->projectOut());
  // Filter only.
  auto* c0c0 = scanSpec->childByName("c0")->childByName("c0c0");
  ASSERT_FALSE(c0c0->isConstant());
  ASSERT_TRUE(c0c0->filter());
  // Project output.
  auto* c0c1 = scanSpec->childByName("c0")->childByName("c0c1");
  ASSERT_FALSE(c0c1->isConstant());
  ASSERT_FALSE(c0c1->filter());
  // Filter on struct, no children.
  auto* c0c2 = scanSpec->childByName("c0")->childByName("c0c2");
  ASSERT_FALSE(c0c2->isConstant());
  ASSERT_TRUE(c0c2->filter());
  validateNullConstant(*c0c2->childByName("c0c2c0"), *BIGINT());
  // Filtered and project out.
  auto* c0c3 = scanSpec->childByName("c0")->childByName("c0c3");
  ASSERT_FALSE(c0c3->isConstant());
  ASSERT_TRUE(c0c3->filter());
  ASSERT_FALSE(c0c3->childByName("c0c3c0")->isConstant());
  // Filter only, column not projected out.
  auto* c1 = scanSpec->childByName("c1");
  ASSERT_FALSE(c1->isConstant());
  ASSERT_FALSE(c1->projectOut());
  auto* c1c0 = c1->childByName("c1c0");
  ASSERT_FALSE(c1c0->childByName("c1c0c0")->isConstant());
  ASSERT_TRUE(c1c0->childByName("c1c0c0"));
  validateNullConstant(*c1c0->childByName("c1c0c1"), *BIGINT());
  validateNullConstant(*c1->childByName("c1c1"), *c1c1Type);
}

TEST_F(HiveConnectorTest, makeScanSpec_duplicateSubfields) {
  auto c0Type = MAP(BIGINT(), MAP(BIGINT(), BIGINT()));
  auto c1Type = MAP(VARCHAR(), MAP(BIGINT(), BIGINT()));
  auto rowType = ROW({{"c0", c0Type}, {"c1", c1Type}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0[10][1]", "c0[10][2]", "c1[\"foo\"][1]", "c1[\"foo\"][2]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_EQ(c0->children().size(), 2);
  auto* c1 = scanSpec->childByName("c1");
  ASSERT_EQ(c1->children().size(), 2);
}

// For TEXTFILE, partition key is not included in data columns.
TEST_F(HiveConnectorTest, makeScanSpec_filterPartitionKey) {
  auto rowType = ROW({{"c0", BIGINT()}});
  SubfieldFilters filters;
  filters.emplace(Subfield("ds"), exec::equal("2023-10-13"));
  auto scanSpec = makeScanSpec(
      rowType, {}, filters, rowType, {{"ds", nullptr}}, {}, pool_.get());
  ASSERT_TRUE(scanSpec->childByName("c0")->projectOut());
  ASSERT_FALSE(scanSpec->childByName("ds")->projectOut());
}

TEST_F(HiveConnectorTest, extractFiltersFromRemainingFilter) {
  auto queryCtx = core::QueryCtx::create();
  exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), pool_.get());
  auto rowType = ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), DECIMAL(20, 0)});

  auto expr = parseExpr("not (c0 > 0 or c1 > 0)", rowType);
  SubfieldFilters filters;
  auto remaining = extractFiltersFromRemainingFilter(expr, &evaluator, filters);
  ASSERT_FALSE(remaining);
  ASSERT_EQ(filters.size(), 2);
  ASSERT_GT(filters.count(Subfield("c0")), 0);
  ASSERT_GT(filters.count(Subfield("c1")), 0);

  expr = parseExpr("not (c0 > 0 or c1 > c0)", rowType);
  filters.clear();
  remaining = extractFiltersFromRemainingFilter(expr, &evaluator, filters);
  ASSERT_EQ(filters.size(), 1);
  ASSERT_GT(filters.count(Subfield("c0")), 0);
  ASSERT_TRUE(remaining);
  ASSERT_EQ(remaining->toString(), "not(gt(ROW[\"c1\"],ROW[\"c0\"]))");

  expr = parseExpr(
      "not (c2 > 1::decimal(20, 0) or c2 < 0::decimal(20, 0))", rowType);
  filters.clear();
  remaining = extractFiltersFromRemainingFilter(expr, &evaluator, filters);
  ASSERT_GT(filters.count(Subfield("c2")), 0);
  // Change these once HUGEINT filter merge is fixed.
  ASSERT_TRUE(remaining);
  ASSERT_EQ(
      remaining->toString(), "not(lt(ROW[\"c2\"],cast 0 as DECIMAL(20, 0)))");
}

} // namespace
} // namespace bytedance::bolt::connector::hive
