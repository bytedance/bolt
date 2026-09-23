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

#include "bolt/exec/HashTable.h"
#include "bolt/common/base/SelectivityInfo.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "folly/experimental/EventCount.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <array>
#include <map>
#include <memory>
#include <optional>
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::test;
namespace bytedance::bolt::exec::test {

template <bool ignoreNullKeys>
class HashTableTestHelper {
 public:
  static HashTableTestHelper create(HashTable<ignoreNullKeys>* table) {
    return HashTableTestHelper(table);
  }

  int64_t nextBucketOffset(int64_t offset) const {
    return table_->nextBucketOffset(offset);
  }

  uint64_t bucketSize() const {
    return HashTable<ignoreNullKeys>::kBucketSize;
  }

  void allocateTables(uint64_t size) {
    table_->allocateTables(size);
  }

  int32_t simdBuildScratchSize() const {
    return table_->simdBuildScratch_.activeRows.size();
  }

  size_t tableSlotSize() const {
    return table_->tableSlotSize();
  }

  void insertForJoin(
      char** groups,
      uint64_t* hashes,
      int32_t numGroups,
      TableInsertPartitionInfo* partitionInfo) {
    table_->insertForJoin(groups, hashes, numGroups, partitionInfo);
  }

  void setHashMode(BaseHashTable::HashMode mode, int32_t numNew) {
    table_->setHashMode(mode, numNew);
  }

 private:
  explicit HashTableTestHelper(HashTable<ignoreNullKeys>* table)
      : table_(table) {
    BOLT_CHECK_NOT_NULL(table_);
  }

  HashTable<ignoreNullKeys>* const table_;
};

struct HashTableTestParam {
  bool enableRunParallel{false};
  bool jitRowEqVectors{false};
};

// Test framework for join hash tables. Generates probe keys, of which
// some percent are inserted in a hashTable. The placement of the
// payload is shuffled so as not to correlate with the probe
// order. Tests the presence/correctness of the hit for each key and
// measures the time for computing hashes/value ids vs the time spent
// probing the table. Covers kArray, kNormalizedKey and kHash hash
// modes.
class HashTableTest : public testing::TestWithParam<HashTableTestParam>,
                      public VectorTestBase {
 public:
  static std::vector<HashTableTestParam> getTestParams() {
    return std::vector<HashTableTestParam>({
        HashTableTestParam{false, false},
        HashTableTestParam{true, false},
        HashTableTestParam{false, true},
        HashTableTestParam{true, true},
    });
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    BOLT_TEST_VALUE_ENABLE();
    if (GetParam().enableRunParallel) {
      executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(16);
    }
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void testCycle(
      BaseHashTable::HashMode mode,
      int32_t size,
      int32_t numWays,
      TypePtr buildType,
      int32_t numKeys) {
    std::vector<TypePtr> dependentTypes;
    int32_t sequence = 0;
    isInTable_.resize(
        bits::nwords(numWays * size),
        static_cast<const std::vector<
            unsigned long,
            std::allocator<unsigned long>>::value_type>(-1));
    if (insertPct_ != 100) {
      // If we probe with all keys but only mean to insert part, we deselect.
      folly::Random::DefaultGenerator rng;
      rng.seed(1);
      for (auto i = 0; i < size * numWays; ++i) {
        if (folly::Random::rand32(rng) % 100 > insertPct_) {
          bits::clearBit(isInTable_.data(), i);
        }
      }
    }
    int32_t startOffset = 0;
    std::vector<std::unique_ptr<BaseHashTable>> otherTables;
    uint64_t numRows{0};
    for (auto way = 0; way < numWays; ++way) {
      std::vector<RowVectorPtr> batches;
      std::vector<std::unique_ptr<VectorHasher>> keyHashers;
      for (auto channel = 0; channel < numKeys; ++channel) {
        keyHashers.emplace_back(std::make_unique<VectorHasher>(
            buildType->childAt(channel), channel));
      }
      auto table = HashTable<true>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          true,
          false,
          BaseHashTable::HashMode::kArray,
          1'000,
          pool(),
          GetParam().jitRowEqVectors);
      table->setSimdEnabled(false);

      makeRows(size, 1, sequence, buildType, batches);
      copyVectorsToTable(batches, startOffset, table.get());
      sequence += size;
      if (topTable_ == nullptr) {
        topTable_ = std::move(table);
        numRows += topTable_->rows()->numRows();
      } else {
        numRows += table->rows()->numRows();
        otherTables.push_back(std::move(table));
      }
      batches_.insert(batches_.end(), batches.begin(), batches.end());
      startOffset += size;
    }

    const uint64_t estimatedTableSize =
        topTable_->estimateHashTableSize(numRows);
    const uint64_t usedMemoryBytes = topTable_->rows()->pool()->currentBytes();
    topTable_->prepareJoinTable(std::move(otherTables), executor_.get());
    ASSERT_GE(
        estimatedTableSize,
        topTable_->rows()->pool()->currentBytes() - usedMemoryBytes);
    ASSERT_EQ(topTable_->hashMode(), mode);
    ASSERT_EQ(topTable_->allRows().size(), numWays);
    uint64_t rowCount{0};
    for (auto* rowContainer : topTable_->allRows()) {
      rowCount += rowContainer->numRows();
    }
    ASSERT_EQ(rowCount, numRows);

    LOG(INFO) << "Made table " << describeTable();
    testProbe();
    testEraseEveryN(3);
    testProbe();
    testEraseEveryN(4);
    testProbe();
    testGroupBySpill(size, buildType, numKeys);
  }

  // Inserts and deletes rows in a HashTable, similarly to a group by
  // that periodically spills a fraction of the groups.
  void testGroupBySpill(
      int32_t size,
      TypePtr tableType,
      int32_t numKeys,
      int32_t batchSize = 1000,
      int32_t eraseSize = 500) {
    int32_t sequence = 0;
    std::vector<RowVectorPtr> batches;
    auto table = createHashTableForAggregation(
        tableType, numKeys, GetParam().jitRowEqVectors);
    auto lookup = std::make_unique<HashLookup>(
        table->hashers(), GetParam().jitRowEqVectors);
    std::vector<char*> allInserted;
    int32_t numErased = 0;
    // We insert 1000 and delete 500.
    for (auto round = 0; round < size; round += batchSize) {
      makeRows(batchSize, 1, sequence, tableType, batches);
      sequence += batchSize;
      lookup->reset(batchSize);
      insertGroups(*batches.back(), *lookup, *table);
      allInserted.insert(
          allInserted.end(), lookup->hits.begin(), lookup->hits.end());

      table->erase(folly::Range<char**>(&allInserted[numErased], eraseSize));
      numErased += eraseSize;
    }
    int32_t batchStart = 0;
    // We loop over the keys one more time. The first half will be all
    // new rows, the second half will be hits of existing ones.
    int32_t row = 0;
    for (auto i = 0; i < batches.size(); ++i) {
      insertGroups(*batches[0], *lookup, *table);
      for (; row < batchStart + batchSize; ++row) {
        if (row >= numErased) {
          ASSERT_EQ(lookup->hits[row - batchStart], allInserted[row]);
        }
      }
    }
    table->checkConsistency();
  }

  std::unique_ptr<HashTable<false>> createHashTableForAggregation(
      const TypePtr& tableType,
      int numKeys,
      bool jitRowEqVectors) {
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    for (auto channel = 0; channel < numKeys; ++channel) {
      keyHashers.emplace_back(
          std::make_unique<VectorHasher>(tableType->childAt(channel), channel));
    }

    auto _simdOffTable = HashTable<false>::createForAggregation(
        std::move(keyHashers),
        std::vector<Accumulator>{},
        pool(),
        nullptr,
        jitRowEqVectors);
    _simdOffTable->setSimdEnabled(false);
    return _simdOffTable;
  }

  void insertGroups(
      const RowVector& input,
      HashLookup& lookup,
      HashTable<false>& table) {
    const SelectivityVector rows(input.size());
    insertGroups(input, rows, lookup, table);
  }

  void insertGroups(
      const RowVector& input,
      const SelectivityVector& rows,
      HashLookup& lookup,
      HashTable<false>& table) {
    lookup.reset(rows.end());
    lookup.rows.clear();
    rows.applyToSelected([&](auto row) { lookup.rows.push_back(row); });

    auto& hashers = table.hashers();
    auto mode = table.hashMode();
    bool rehash = false;
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input.childAt(hashers[i]->channel());
      hashers[i]->decode(*key, rows);
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(rows, lookup.hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(rows, i > 0, lookup.hashes);
      }
    }

    if (rehash) {
      if (table.hashMode() != BaseHashTable::HashMode::kHash) {
        table.decideHashMode(input.size());
      }
      insertGroups(input, rows, lookup, table);
      return;
    }
    table.groupProbe(lookup);
  }

  std::string describeTable() {
    std::stringstream out;
    auto mode = topTable_->hashMode();
    if (mode == BaseHashTable::HashMode::kHash) {
      out << "Multipart key ";
    } else {
      out
          << (mode == BaseHashTable::HashMode::kArray ? "Array "
                                                      : "Normalized key ");
      out << "(";
      for (auto& hasher : topTable_->hashers()) {
        out << (hasher->isRange() ? "range " : "valueIds ");
      }
      out << ") ";
    }
    out << topTable_->numDistinct() << " entries";
    return out.str();
  }

  void copyVectorsToTable(
      const std::vector<RowVectorPtr>& batches,
      int32_t tableOffset,
      BaseHashTable* table) {
    const int32_t batchSize = batches[0]->size();
    raw_vector<uint64_t> dummy(batchSize);
    int32_t batchOffset = 0;
    rowOfKey_.resize(tableOffset + batchSize * batches.size());
    auto rowContainer = table->rows();
    auto& hashers = table->hashers();
    auto numKeys = hashers.size();
    // We init a DecodedVector for each member of the RowVectors in 'batches'.
    std::vector<std::vector<DecodedVector>> decoded;
    SelectivityVector rows(batchSize);
    SelectivityVector insertedRows(batchSize);
    for (auto& batch : batches) {
      // If we are only inserting a fraction of the rows, we set insertedRows to
      // that fraction so that the VectorHashers only see keys that will
      // actually be inserted.
      if (insertPct_ < 100) {
        bits::copyBits(
            isInTable_.data(),
            tableOffset + batchOffset,
            insertedRows.asMutableRange().bits(),
            0,
            batchSize);
        insertedRows.updateBounds();
      }
      decoded.emplace_back(batch->childrenSize());
      BOLT_CHECK_EQ(batch->size(), batchSize);
      auto& decoders = decoded.back();
      for (auto i = 0; i < batch->childrenSize(); ++i) {
        decoders[i].decode(*batch->childAt(i), rows);
        if (i < numKeys) {
          auto hasher = table->hashers()[i].get();
          hasher->decode(*batch->childAt(i), insertedRows);
          if (table->hashMode() != BaseHashTable::HashMode::kHash &&
              hasher->mayUseValueIds()) {
            hasher->computeValueIds(insertedRows, dummy);
          }
        }
      }
      batchOffset += batchSize;
    }

    const auto size = batchSize * batches.size();
    const auto powerOfTwo = bits::nextPowerOfTwo(size);
    const int32_t mask = powerOfTwo - 1;
    int32_t position = 0;
    int32_t delta = 1;
    const auto nextOffset = rowContainer->nextOffset();

    // We insert values in a geometric skip order. 1, 2, 4, 7,
    // 11,... where the skip increments by one. We wrap around at the
    // power of two boundary. This sequence hits every place in the
    // power of two range once. Like this, when we probe the data for
    // consecutive keys the hits will have no cache locality.
    for (auto count = 0; count < powerOfTwo; ++count) {
      if (position < size &&
          (insertPct_ == 100 ||
           bits::isBitSet(isInTable_.data(), tableOffset + position))) {
        char* newRow = rowContainer->newRow();
        rowOfKey_[tableOffset + position] = newRow;
        const auto batchIndex = position / batchSize;
        const auto rowIndex = position % batchSize;
        if (nextOffset > 0) {
          *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
        }
        for (auto i = 0; i < batches[batchIndex]->type()->size(); ++i) {
          rowContainer->store(decoded[batchIndex][i], rowIndex, newRow, i);
        }
      }
      position = (position + delta) & mask;
      ++delta;
    }
  }

  // Makes a vector of 'type' with 'size' unique elements, initialized
  // based on 'sequence'. If 'sequence' is incremented by 'size'
  // between the next call will not overlap with the results of the
  // previous one.
  VectorPtr makeVector(TypePtr type, int32_t size, int32_t sequence) {
    switch (type->kind()) {
      case TypeKind::BIGINT:
        return makeFlatVector<int64_t>(
            size,
            [&](vector_size_t row) { return keySpacing_ * (sequence + row); },
            nullptr);

      case TypeKind::VARCHAR: {
        auto strings =
            BaseVector::create<FlatVector<StringView>>(VARCHAR(), size, pool());
        for (auto row = 0; row < size; ++row) {
          auto string = fmt::format("{}", keySpacing_ * (sequence + row));
          // Make strings that overflow the inline limit for 1/10 of
          // the values after 10K,000. Datasets with only
          // range-encodable small strings can be made within the
          // first 10K values.
          if (row > 10000 && row % 10 == 0) {
            string += "----" + string + "----" + string;
          }
          strings->set(row, StringView(string));
        }
        return strings;
      }

      case TypeKind::ROW: {
        std::vector<VectorPtr> children;
        for (auto i = 0; i < type->size(); ++i) {
          children.push_back(makeVector(type->childAt(i), size, sequence));
        }
        return makeRowVector(children);
      }
      default:
        BOLT_FAIL("Unsupported kind for makeVector {}", type->kind());
    }
  }

  void makeRows(
      int32_t batchSize,
      int32_t numBatches,
      int32_t sequence,
      TypePtr buildType,
      std::vector<RowVectorPtr>& batches) {
    for (auto i = 0; i < numBatches; ++i) {
      batches.push_back(std::static_pointer_cast<RowVector>(
          makeVector(buildType, batchSize, sequence)));
      sequence += batchSize;
    }
  }

  void store(RowContainer& rowContainer, const RowVectorPtr& data) {
    std::vector<DecodedVector> decodedVectors;
    for (auto& vector : data->children()) {
      decodedVectors.emplace_back(*vector);
    }

    std::vector<char*> rows;
    for (auto i = 0; i < data->size(); ++i) {
      auto* row = rowContainer.newRow();

      for (auto j = 0; j < decodedVectors.size(); ++j) {
        rowContainer.store(decodedVectors[j], i, row, j);
      }
    }
  }

  void testProbe() {
    auto lookup = std::make_unique<HashLookup>(
        topTable_->hashers(), GetParam().jitRowEqVectors);
    const auto batchSize = batches_[0]->size();
    SelectivityVector rows(batchSize);
    const auto mode = topTable_->hashMode();
    SelectivityInfo hashTime;
    SelectivityInfo probeTime;
    int32_t numHashed = 0;
    int32_t numProbed = 0;
    int32_t numHit = 0;
    auto& hashers = topTable_->hashers();
    VectorHasher::ScratchMemory scratchMemory;
    for (auto batchIndex = 0; batchIndex < batches_.size(); ++batchIndex) {
      const auto& batch = batches_[batchIndex];
      lookup->reset(batch->size());
      rows.setAll();
      numHashed += batch->size();
      {
        SelectivityTimer timer(hashTime, 0);
        for (auto i = 0; i < hashers.size(); ++i) {
          auto& key = batch->childAt(i);
          if (mode != BaseHashTable::HashMode::kHash) {
            hashers[i]->lookupValueIds(
                *key, rows, scratchMemory, lookup->hashes);
          } else {
            hashers[i]->decode(*key, rows);
            hashers[i]->hash(rows, i > 0, lookup->hashes);
          }
        }
      }

      lookup->rows.clear();
      if (rows.isAllSelected()) {
        lookup->rows.resize(rows.size());
        std::iota(lookup->rows.begin(), lookup->rows.end(), 0);
      } else {
        constexpr int32_t kPadding = simd::kPadding / sizeof(int32_t);
        lookup->rows.resize(bits::roundUp(rows.size() + kPadding, kPadding));
        const auto numRows = simd::indicesOfSetBits(
            rows.asRange().bits(), 0, batch->size(), lookup->rows.data());
        lookup->rows.resize(numRows);
      }

      const auto startOffset = batchIndex * batchSize;
      if (lookup->rows.empty()) {
        // the keys disqualify all entries. The table is not consulted.
        for (auto i = startOffset; i < startOffset + batch->size(); ++i) {
          ASSERT_EQ(nullptr, rowOfKey_[i]);
        }
      } else {
        {
          numProbed += lookup->rows.size();
          SelectivityTimer timer(probeTime, 0);
          topTable_->joinProbe(*lookup);
        }
        for (auto i = 0; i < lookup->rows.size(); ++i) {
          const auto key = lookup->rows[i];
          numHit += lookup->hits[key] != nullptr;
          ASSERT_EQ(rowOfKey_[startOffset + key], lookup->hits[key]);
        }
      }
    }
  }

  // Erases every strideth non-erased item in the hash table.
  void testEraseEveryN(int32_t stride) {
    std::vector<char*> toErase;
    int32_t counter = 0;
    for (auto i = 0; i < rowOfKey_.size(); ++i) {
      if (rowOfKey_[i] && ++counter % stride == 0) {
        toErase.push_back(rowOfKey_[i]);
        rowOfKey_[i] = nullptr;
      }
    }
    topTable_->erase(folly::Range<char**>(toErase.data(), toErase.size()));
  }

  void testListNullKeyRows(
      const VectorPtr& keys,
      BaseHashTable::HashMode mode) {
    folly::F14FastSet<int> nullValues;
    for (int i = 0; i < keys->size(); ++i) {
      if (i % 97 == 0) {
        keys->setNull(i, true);
        nullValues.insert(i);
      }
    }
    auto batch = makeRowVector(
        {keys, makeFlatVector<int64_t>(keys->size(), folly::identity)});
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(std::make_unique<VectorHasher>(keys->type(), 0));
    auto table = HashTable<false>::createForJoin(
        std::move(hashers),
        {BIGINT()},
        true,
        false,
        BaseHashTable::HashMode::kArray,
        1'000,
        pool(),
        GetParam().jitRowEqVectors);
    table->setSimdEnabled(false);
    copyVectorsToTable({batch}, 0, table.get());
    table->prepareJoinTable({}, executor_.get());
    ASSERT_EQ(table->hashMode(), mode);
    std::vector<char*> rows(nullValues.size());
    BaseHashTable::NullKeyRowsIterator iter;
    auto numRows = table->listNullKeyRows(&iter, rows.size(), rows.data());
    ASSERT_EQ(numRows, nullValues.size());
    auto actual =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), numRows, pool());
    table->rows()->extractColumn(rows.data(), numRows, 1, actual);
    for (int i = 0; i < actual->size(); ++i) {
      auto it = nullValues.find(actual->valueAt(i));
      ASSERT_TRUE(it != nullValues.end());
      nullValues.erase(it);
    }
    ASSERT_TRUE(nullValues.empty());
    ASSERT_EQ(0, table->listNullKeyRows(&iter, rows.size(), rows.data()));
  }

  // Bitmap of positions in batches_ that end up in the table.
  std::vector<uint64_t> isInTable_;
  // Test payload, keys first.
  std::vector<RowVectorPtr> batches_;

  // Corresponds 1:1 to data in 'batches_'. nullptr if the key is not
  // inserted, otherwise pointer into the RowContainer.
  std::vector<char*> rowOfKey_;
  std::unique_ptr<HashTable<true>> topTable_;
  // Percentage of keys inserted into the table. This is for measuring
  // joins that miss the table part of the time. Used in initializing
  // 'isInTable_'.
  int32_t insertPct_ = 100;
  // Spacing between consecutive generated keys. Affects whether
  // Vectorhashers make ranges or ids of distinct values.
  int64_t keySpacing_ = 1;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

TEST_P(HashTableTest, int2DenseArray) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 2);
}

TEST_P(HashTableTest, string1DenseArray) {
  auto type = ROW({"k1"}, {VARCHAR()});
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 1);
}

TEST_P(HashTableTest, string2Normalized) {
  auto type = ROW({"k1", "k2"}, {VARCHAR(), VARCHAR()});
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 5000, 19, type, 2);
}

TEST_P(HashTableTest, int2SparseArray) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 2);
}

TEST_P(HashTableTest, int2SparseNormalized) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 10000, 2, type, 2);
}

TEST_P(HashTableTest, int2SparseNormalizedMostMiss) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  insertPct_ = 10;
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 100000, 2, type, 2);
}

TEST_P(HashTableTest, structKey) {
  auto type =
      ROW({"key"}, {ROW({"k1", "k2", "k3"}, {BIGINT(), VARCHAR(), BIGINT()})});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kHash, 100000, 2, type, 1);
}

TEST_P(HashTableTest, mixed6Sparse) {
  auto type =
      ROW({"k1", "k2", "k3", "k4", "k5", "k6"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), VARCHAR()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kHash, 100000, 9, type, 6);
}

// It should be safe to call clear() before we insert any data into HashTable
TEST_P(HashTableTest, clear) {
  std::vector<std::unique_ptr<VectorHasher>> keyHashers;
  keyHashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0 /*channel*/));
  core::QueryConfig config({});
  auto aggregate = Aggregate::create(
      "sum",
      core::AggregationNode::Step::kPartial,
      std::vector<TypePtr>{BIGINT()},
      BIGINT(),
      config);

  auto table = HashTable<true>::createForAggregation(
      std::move(keyHashers),
      {Accumulator{aggregate.get(), nullptr}},
      pool(),
      nullptr,
      GetParam().jitRowEqVectors);
  table->setSimdEnabled(false);
  ASSERT_NO_THROW(table->clear());
}

// Test a specific code path in HashTable::decodeHashMode where
// rangesWithReserve overflows, distinctsWithReserve fits and bestWithReserve =
// rangesWithReserve.
TEST_P(HashTableTest, bestWithReserveOverflow) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  const auto numKeys = 4;
  auto table = createHashTableForAggregation(
      rowType, numKeys, GetParam().jitRowEqVectors);
  auto lookup = std::make_unique<HashLookup>(
      table->hashers(), GetParam().jitRowEqVectors);

  // Make sure rangesWithReserve overflows.
  //  Ranges for keys are: 200K, 200K, 200K, 100K.
  //  With 50% reserve at both ends: 400K, 400K, 400K, 200K.
  //  Combined ranges with reserve: 400K * 400K * 400K * 200K =
  //  12,800,000,000,000,000,000,000.
  // Also, make sure that distinctsWithReserve fits.
  //  Number of distinct values (ndv) are: 20K, 20K, 20K, 10K.
  //  With 50% reserve: 30K, 30K, 30K, 15K.
  //  Combined ndvs with reserve: 30K * 30K * 30K * 15K =
  //  405,000,000,000,000,000.
  // Also, make sure bestWithReserve == rangesWithReserve and therefore
  // overflows as well.
  //  Range is considered 'best' if range < 20 * ndv.
  //
  // Finally, make sure last key has some duplicate values. The original bug
  // this test is reproducing was when HashTable failed to set multiplier for
  // the VectorHasher, which caused the combined value IDs to be computed using
  // only the last VectorHasher. Hence, all values where last key was the same
  // were assigned the same value IDs.
  auto data = makeRowVector({
      makeFlatVector<int64_t>(20'000, [](auto row) { return row * 10; }),
      makeFlatVector<int64_t>(20'000, [](auto row) { return 1 + row * 10; }),
      makeFlatVector<int64_t>(20'000, [](auto row) { return 2 + row * 10; }),
      makeFlatVector<int64_t>(
          20'000, [](auto row) { return 3 + (row / 2) * 10; }),
  });

  lookup->reset(data->size());
  insertGroups(*data, *lookup, *table);

  // Expect 'normalized key' hash mode using distinct values, not ranges.
  ASSERT_EQ(table->hashMode(), BaseHashTable::HashMode::kNormalizedKey);
  ASSERT_EQ(table->numDistinct(), data->size());

  for (auto i = 0; i < numKeys; ++i) {
    ASSERT_FALSE(table->hashers()[i]->isRange());
    ASSERT_TRUE(table->hashers()[i]->mayUseValueIds());
  }

  // Compute value IDs and verify all are unique.
  SelectivityVector rows(data->size());
  raw_vector<uint64_t> valueIds(data->size());

  for (int32_t i = 0; i < numKeys; ++i) {
    bool ok = table->hashers()[i]->computeValueIds(rows, valueIds);
    ASSERT_TRUE(ok);
  }

  std::unordered_set<uint64_t> uniqueValueIds;
  for (auto id : valueIds) {
    ASSERT_TRUE(uniqueValueIds.insert(id).second) << id;
  }
}

/// Test edge case that used to trigger a rounding error in
/// HashTable::enableRangeWhereCan.
TEST_P(HashTableTest, enableRangeWhereCan) {
  auto rowType = ROW({"a", "b", "c"}, {BIGINT(), VARCHAR(), VARCHAR()});
  auto table =
      createHashTableForAggregation(rowType, 3, GetParam().jitRowEqVectors);
  auto lookup = std::make_unique<HashLookup>(
      table->hashers(), GetParam().jitRowEqVectors);

  // Generate 3 keys with the following ranges and number of distinct values
  // (ndv):
  //  0: range=4409503440398, ndv=25
  //  1: range=18446744073709551615, ndv=748
  //  2: range=18446744073709551615, ndv=1678

  std::vector<int64_t> a;
  for (int i = 1; i < 25; i++) {
    a.push_back(i);
  }
  a.back() = 4409503440398;

  std::vector<std::string> b;
  for (int i = 1; i < 748; i++) {
    b.push_back(std::string(15, '.') + std::to_string(i));
  }

  std::vector<std::string> c;
  for (int i = 1; i < 1678; i++) {
    c.push_back(std::string(15, '.') + std::to_string(i));
  }

  auto data = makeRowVector({
      makeFlatVector<int64_t>(
          2'000, [&](auto row) { return a[row % a.size()]; }),
      makeFlatVector<StringView>(
          2'000, [&](auto row) { return StringView(b[row % b.size()]); }),
      makeFlatVector<StringView>(
          2'000, [&](auto row) { return StringView(c[row % c.size()]); }),
  });

  lookup->reset(data->size());
  insertGroups(*data, *lookup, *table);
}

TEST_P(HashTableTest, arrayProbeNormalizedKey) {
  auto table = createHashTableForAggregation(
      ROW({"a"}, {BIGINT()}), 1, GetParam().jitRowEqVectors);
  auto lookup = std::make_unique<HashLookup>(
      table->hashers(), GetParam().jitRowEqVectors);

  for (auto i = 0; i < 200; ++i) {
    auto data = makeRowVector({
        makeFlatVector<int64_t>(
            10'000, [&](auto row) { return i * 10'000 + row; }),
    });

    SelectivityVector rows(5'000);
    insertGroups(*data, rows, *lookup, *table);

    rows.resize(10'000);
    rows.clearAll();
    rows.setValidRange(5'000, 10'000, true);
    rows.updateBounds();
    insertGroups(*data, rows, *lookup, *table);
    EXPECT_LE(table->stats().numDistinct, table->rehashSize());
  }

  ASSERT_EQ(table->hashMode(), BaseHashTable::HashMode::kNormalizedKey);
}

TEST_P(HashTableTest, regularHashingTableSize) {
  keySpacing_ = 1000;
  auto checkTableSize = [&](BaseHashTable::HashMode mode,
                            const RowTypePtr& type) {
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    for (auto channel = 0; channel < type->size(); ++channel) {
      keyHashers.emplace_back(
          std::make_unique<VectorHasher>(type->childAt(channel), channel));
    }
    auto table = HashTable<true>::createForJoin(
        std::move(keyHashers),
        {},
        true,
        false,
        BaseHashTable::HashMode::kArray,
        1'000,
        pool(),
        GetParam().jitRowEqVectors);
    table->setSimdEnabled(false);
    std::vector<RowVectorPtr> batches;
    makeRows(1 << 12, 1, 0, type, batches);
    copyVectorsToTable(batches, 0, table.get());
    table->prepareJoinTable({}, executor_.get());
    ASSERT_EQ(table->hashMode(), mode);
    EXPECT_GE(table->rehashSize(), table->numDistinct());
  };
  {
    auto type = ROW({"key"}, {ROW({"k1"}, {BIGINT()})});
    checkTableSize(BaseHashTable::HashMode::kHash, type);
  }
  {
    auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
    checkTableSize(BaseHashTable::HashMode::kNormalizedKey, type);
  }
}

TEST_P(HashTableTest, simdDisabledByDefault) {
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(VectorHasher::create(BIGINT(), 0));
  auto table = HashTable<true>::createForJoin(
      std::move(hashers),
      {},
      false,
      false,
      BaseHashTable::HashMode::kHash,
      1'000,
      pool(),
      GetParam().jitRowEqVectors);
  EXPECT_FALSE(table->simdEnabled());
  EXPECT_FALSE(table->simdActive());
}

TEST_P(HashTableTest, simdDisabledForHybridJoin) {
  const auto makeTable = [&](bool hybridMode) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(VectorHasher::create(BIGINT(), 0));
    return HashTable<true>::createForJoin(
        std::move(hashers),
        {BIGINT()},
        false,
        false,
        BaseHashTable::HashMode::kHash,
        1'000,
        pool(),
        GetParam().jitRowEqVectors,
        hybridMode,
        true);
  };

  auto table = makeTable(false);
  EXPECT_EQ(table->simdActive(), process::hasAvx2());
  EXPECT_EQ(table->simdHashLayoutActive(), process::hasAvx2());

  auto hybridTable = makeTable(true);
  ASSERT_TRUE(hybridTable->simdEnabled());
  ASSERT_NE(hybridTable->hybridData(), nullptr);
  EXPECT_FALSE(hybridTable->simdActive());
  EXPECT_FALSE(hybridTable->simdHashLayoutActive());
}

DEBUG_ONLY_TEST_P(HashTableTest, simdConsistencyWithDuplicateJoinKeys) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }

  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(std::make_unique<VectorHasher>(VARCHAR(), 0));
  auto table = HashTable<true>::createForJoin(
      std::move(hashers),
      {},
      true,
      false,
      BaseHashTable::HashMode::kHash,
      1'000,
      pool(),
      GetParam().jitRowEqVectors);
  table->setSimdEnabled(true);

  constexpr int32_t kNumRows = 1'000;
  auto batch = makeRowVector({makeFlatVector<std::string>(
      kNumRows, [](auto row) { return fmt::format("key_{}", row % 10); })});
  copyVectorsToTable({batch}, 0, table.get());
  table->prepareJoinTable({}, executor_.get());

  ASSERT_EQ(table->hashMode(), BaseHashTable::HashMode::kHash);
  ASSERT_TRUE(table->simdActive());
  ASSERT_TRUE(table->hasDuplicateKeys());
  ASSERT_EQ(table->numDistinct(), kNumRows);
  ASSERT_NO_THROW(table->checkConsistency());
}

TEST_P(HashTableTest, simdKeyColumnsUseNullMask) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }

  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(VectorHasher::create(BIGINT(), 0));
  HashLookup lookup(hashers);
  auto input = makeFlatVector<int64_t>({1, 2});
  SelectivityVector selected(input->size());
  hashers[0]->decode(*input, selected);
  for (bool nullable : {false, true}) {
    RowContainer rows(
        {BIGINT()},
        nullable,
        {},
        {},
        false,
        false,
        false,
        false,
        false,
        pool());
    std::vector<hash_table_simd::KeyColumn> columns;
    hash_table_simd::buildKeyColumns<false>(lookup, columns, &rows, false, {});
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].hasNulls, nullable);
    // A stored NULL from an earlier batch must still compare with null
    // semantics.
    auto* stored = rows.newRow();
    if (nullable) {
      auto nullInput = makeNullableFlatVector<int64_t>({std::nullopt});
      DecodedVector decoded(*nullInput);
      rows.store(decoded, 0, stored, 0);
    } else {
      rows.store(hashers[0]->decodedVector(), 0, stored, 0);
    }
    vector_size_t candidate = 0;
    vector_size_t mismatch = -1;
    uint64_t candidateSlot = 17;
    uint64_t mismatchSlot = 0;
    int32_t numMismatches = 0;
    uint8_t matchMask = 0;
    uint8_t nullMask = 0;
    const auto matches = hash_table_simd::compareProbeColumns<false>(
        columns,
        &candidate,
        &stored,
        &candidateSlot,
        1,
        &mismatch,
        &mismatchSlot,
        numMismatches,
        &rows,
        31,
        &matchMask,
        &nullMask);
    EXPECT_EQ(matches, nullable ? 0 : 1);
    EXPECT_EQ(numMismatches, nullable ? 1 : 0);
    EXPECT_EQ(mismatchSlot, nullable ? 18 : 0);
  }
}

TEST_P(HashTableTest, simdProbeCandidateCompaction) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }

  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(VectorHasher::create(BIGINT(), 0));
  hashers.push_back(VectorHasher::create(BIGINT(), 1));
  HashLookup lookup(hashers);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, 20, 30, std::nullopt, 50}),
      makeNullableFlatVector<int64_t>({100, 200, 300, 400, std::nullopt}),
  });
  auto storedValues = makeRowVector({
      makeNullableFlatVector<int64_t>({10, 21, 30, std::nullopt, 50}),
      makeNullableFlatVector<int64_t>({100, 200, 301, 400, std::nullopt}),
  });
  SelectivityVector selected(input->size());
  for (int32_t column = 0; column < hashers.size(); ++column) {
    hashers[column]->decode(*input->childAt(column), selected);
  }

  RowContainer rows(
      {BIGINT(), BIGINT()},
      true,
      {},
      {},
      false,
      false,
      false,
      false,
      false,
      pool());
  std::vector<DecodedVector> storedColumns;
  storedColumns.reserve(2);
  for (int32_t column = 0; column < 2; ++column) {
    storedColumns.emplace_back(*storedValues->childAt(column), selected);
  }
  std::array<char*, 5> storedRows;
  for (int32_t row = 0; row < storedRows.size(); ++row) {
    storedRows[row] = rows.newRow();
    for (int32_t column = 0; column < storedColumns.size(); ++column) {
      rows.store(storedColumns[column], row, storedRows[row], column);
    }
  }

  std::vector<hash_table_simd::KeyColumn> columns;
  hash_table_simd::buildKeyColumns<false>(
      lookup, columns, &rows, true, {true, true});
  std::array<vector_size_t, 5> candidateRows{0, 1, 2, 3, 4};
  std::array<char*, 5> candidateGroups = storedRows;
  std::array<uint64_t, 5> candidateSlots{4, 9, 14, 19, 24};
  std::array<vector_size_t, 5> mismatchRows{};
  std::array<uint64_t, 5> mismatchSlots{};
  std::array<uint8_t, 5> matchMask{};
  std::array<uint8_t, 5> bothNullMask{};
  int32_t numMismatches = 0;

  const auto numMatches = hash_table_simd::compareProbeColumns<false>(
      columns,
      candidateRows.data(),
      candidateGroups.data(),
      candidateSlots.data(),
      candidateRows.size(),
      mismatchRows.data(),
      mismatchSlots.data(),
      numMismatches,
      &rows,
      31,
      matchMask.data(),
      bothNullMask.data());

  EXPECT_EQ(numMatches, 3);
  EXPECT_EQ(
      (std::array<vector_size_t, 3>{
          candidateRows[0], candidateRows[1], candidateRows[2]}),
      (std::array<vector_size_t, 3>{0, 3, 4}));
  EXPECT_EQ(numMismatches, 2);
  EXPECT_EQ(
      (std::array<vector_size_t, 2>{mismatchRows[0], mismatchRows[1]}),
      (std::array<vector_size_t, 2>{1, 2}));
  EXPECT_EQ(
      (std::array<uint64_t, 2>{mismatchSlots[0], mismatchSlots[1]}),
      (std::array<uint64_t, 2>{10, 15}));
}

TEST_P(HashTableTest, simdAggregationCollisionAndRehash) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }

  using Key = std::pair<std::optional<int64_t>, std::optional<int64_t>>;

  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(VectorHasher::create(BIGINT(), 0));
  hashers.push_back(VectorHasher::create(BIGINT(), 1));
  auto table = HashTable<false>::createForAggregation(
      std::move(hashers),
      {},
      pool(),
      nullptr,
      GetParam().jitRowEqVectors,
      true);
  auto helper = HashTableTestHelper<false>::create(table.get());
  helper.setHashMode(BaseHashTable::HashMode::kHash, 1);
  ASSERT_TRUE(table->simdHashLayoutActive());

  const auto mask = table->capacity() - 1;
  std::unordered_map<uint64_t, int64_t> keyBySignature;
  std::array<int64_t, 2> collidingKeys{};
  for (int64_t value = 1; value < 100'000; ++value) {
    const auto hash = bits::hashMix(
        folly::hasher<int64_t>()(7), folly::hasher<int64_t>()(value));
    const auto signature = (hash & hash_table_simd::kTagMask) |
        hash_table_simd::bucketStart(hash & mask);
    const auto [it, inserted] = keyBySignature.emplace(signature, value);
    if (!inserted) {
      collidingKeys = {it->second, value};
      break;
    }
  }
  ASSERT_NE(collidingKeys[1], 0);

  std::map<Key, char*> groupsByKey;
  HashLookup lookup(table->hashers(), GetParam().jitRowEqVectors);
  auto runBatch = [&](const std::vector<Key>& keys) {
    std::vector<std::optional<int64_t>> firstKeys;
    std::vector<std::optional<int64_t>> secondKeys;
    firstKeys.reserve(keys.size());
    secondKeys.reserve(keys.size());
    for (const auto& [first, second] : keys) {
      firstKeys.push_back(first);
      secondKeys.push_back(second);
    }
    auto input = makeRowVector({
        makeNullableFlatVector<int64_t>(firstKeys),
        makeNullableFlatVector<int64_t>(secondKeys),
    });
    insertGroups(*input, lookup, *table);
    const auto firstColumn = table->rows()->columnAt(0);
    const auto secondColumn = table->rows()->columnAt(1);
    for (int32_t row = 0; row < keys.size(); ++row) {
      auto* group = lookup.hits[row];
      ASSERT_NE(group, nullptr);
      const auto& [first, second] = keys[row];
      EXPECT_EQ(RowContainer::isNullAt(group, firstColumn), !first.has_value());
      EXPECT_EQ(
          RowContainer::isNullAt(group, secondColumn), !second.has_value());
      if (first) {
        EXPECT_EQ(
            folly::loadUnaligned<int64_t>(group + firstColumn.offset()),
            *first);
      }
      if (second) {
        EXPECT_EQ(
            folly::loadUnaligned<int64_t>(group + secondColumn.offset()),
            *second);
      }
      const auto [it, inserted] = groupsByKey.emplace(keys[row], group);
      if (!inserted) {
        EXPECT_EQ(group, it->second);
      }
    }
    EXPECT_EQ(table->numDistinct(), groupsByKey.size());
    ASSERT_NO_THROW(table->checkConsistency());
  };

  std::vector<Key> collisionBatch;
  collisionBatch.reserve(257);
  for (int32_t row = 0; row < 257; ++row) {
    switch (row % 4) {
      case 0:
        collisionBatch.emplace_back(7, collidingKeys[0]);
        break;
      case 1:
        collisionBatch.emplace_back(7, collidingKeys[1]);
        break;
      case 2:
        collisionBatch.emplace_back(std::nullopt, 31);
        break;
      default:
        collisionBatch.emplace_back(10'000 + row, 20'000 + row);
        break;
    }
  }
  runBatch(collisionBatch);

  std::vector<Key> rehashBatch;
  rehashBatch.reserve(1'800);
  for (int32_t row = 0; row < 1'800; ++row) {
    if (row % 127 == 0) {
      rehashBatch.emplace_back(std::nullopt, 31);
    } else if (row % 31 == 0) {
      rehashBatch.emplace_back(
          7, row % 62 == 0 ? collidingKeys[0] : collidingKeys[1]);
    } else {
      rehashBatch.emplace_back(100'000 + row, 200'000 + row * 13);
    }
  }
  const auto initialCapacity = table->capacity();
  runBatch(rehashBatch);
  EXPECT_GT(table->capacity(), initialCapacity);
  runBatch(collisionBatch);
}

TEST_P(HashTableTest, simdSkewedParallelBuildOverflow) {
  if (!process::hasAvx2()) {
    GTEST_SKIP() << "SIMD requires AVX2";
  }
  constexpr int32_t kRows = 4096;
  std::vector<int64_t> keys;
  for (int64_t key = 0; keys.size() < kRows; ++key) {
    if ((folly::hasher<int64_t>()(key) & 6144) == 0) {
      keys.push_back(key);
    }
  }
  auto input = makeRowVector({makeFlatVector<int64_t>(keys)});
  SelectivityVector selected(kRows);
  DecodedVector decoded(*input->childAt(0), selected);
  std::vector<std::unique_ptr<BaseHashTable>> tables;
  for (int way = 0; way < 4; ++way) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(VectorHasher::create(BIGINT(), 0));
    auto table = HashTable<true>::createForJoin(
        std::move(hashers),
        {},
        true,
        false,
        BaseHashTable::HashMode::kHash,
        1000,
        pool(),
        GetParam().jitRowEqVectors,
        false,
        true);
    for (int i = way; i < kRows; i += 4) {
      auto* row = table->rows()->newRow();
      *reinterpret_cast<char**>(row + table->rows()->nextOffset()) = nullptr;
      table->rows()->store(decoded, i, row, 0);
    }
    tables.push_back(std::move(table));
  }
  auto table = std::move(tables.back());
  tables.pop_back();
  folly::CPUThreadPoolExecutor executor(4);
  table->prepareJoinTable(std::move(tables), &executor);
  ASSERT_EQ(table->capacity(), 8192);
  auto* typed = dynamic_cast<HashTable<true>*>(table.get());
  ASSERT_NE(typed, nullptr);
  ASSERT_TRUE(typed->simdHashLayoutActive());
  const auto helper = HashTableTestHelper<true>::create(typed);
  EXPECT_GT(helper.simdBuildScratchSize(), 0);
  EXPECT_LE(helper.simdBuildScratchSize(), 1024);
  HashLookup lookup(table->hashers(), GetParam().jitRowEqVectors);
  table->prepareForJoinProbe(lookup, input, selected, true);
  table->joinProbe(lookup);
  for (int row = 0; row < kRows; ++row) {
    ASSERT_NE(lookup.hits[row], nullptr);
    EXPECT_EQ(
        folly::loadUnaligned<int64_t>(
            lookup.hits[row] + table->rows()->columnAt(0).offset()),
        keys[row]);
  }
  EXPECT_FALSE(table->hasDuplicateKeys());
}

TEST_P(HashTableTest, groupBySpill) {
  auto type = ROW({"k1"}, {BIGINT()});
  testGroupBySpill(5'000'000, type, 1, 1000, 1000);
}

TEST_P(HashTableTest, checkSizeValidation) {
  auto rowType = ROW({"a"}, {BIGINT()});
  auto table =
      createHashTableForAggregation(rowType, 1, GetParam().jitRowEqVectors);
  auto lookup = std::make_unique<HashLookup>(
      table->hashers(), GetParam().jitRowEqVectors);
  auto testHelper = HashTableTestHelper<false>::create(table.get());

  // The initial set hash mode with table size of 256K entries.
  testHelper.setHashMode(BaseHashTable::HashMode::kHash, 131'072);
  ASSERT_EQ(table->capacity(), 256 << 10);

  auto vector1 = makeRowVector(
      {makeFlatVector<int64_t>(131'072, [&](auto row) { return row; })});
  // The first insertion of 128KB distinct entries.
  insertGroups(*vector1, *lookup, *table);
  ASSERT_EQ(table->capacity(), 256 << 10);

  auto vector2 = makeRowVector({makeFlatVector<int64_t>(
      131'072, [&](auto row) { return 131'072 + row; })});
  // The second insertion of 128KB distinct entries triggers the table resizing.
  // And we expect the table size bumps up to 512KB.
  insertGroups(*vector2, *lookup, *table);
  ASSERT_EQ(table->capacity(), 512 << 10);

  auto vector3 = makeRowVector(
      {makeFlatVector<int64_t>(1, [&](auto row) { return row; })});
  // The last insertion triggers the check size which see the table size matches
  // the number of distinct entries that it stores.
  insertGroups(*vector3, *lookup, *table);
  ASSERT_EQ(table->capacity(), 512 << 10);
}

TEST_P(HashTableTest, listNullKeyRows) {
  VectorPtr keys = makeFlatVector<int64_t>(500, folly::identity);
  testListNullKeyRows(keys, BaseHashTable::HashMode::kArray);
  {
    auto flat =
        makeFlatVector<int64_t>(10'000, [](auto i) { return i * 1000; });
    keys = makeRowVector({flat, flat});
  }
  testListNullKeyRows(keys, BaseHashTable::HashMode::kHash);
}

TEST(HashTableTest, modeString) {
  ASSERT_EQ("HASH", BaseHashTable::modeString(BaseHashTable::HashMode::kHash));
  ASSERT_EQ(
      "NORMALIZED_KEY",
      BaseHashTable::modeString(BaseHashTable::HashMode::kNormalizedKey));
  ASSERT_EQ(
      "ARRAY", BaseHashTable::modeString(BaseHashTable::HashMode::kArray));
  ASSERT_EQ(
      "Unknown HashTable mode:100",
      BaseHashTable::modeString(static_cast<BaseHashTable::HashMode>(100)));
}

DEBUG_ONLY_TEST_P(HashTableTest, nextBucketOffset) {
  auto runTest = [&](BaseHashTable::HashMode mode, const RowTypePtr& type) {
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    for (auto channel = 0; channel < type->size(); ++channel) {
      keyHashers.emplace_back(
          std::make_unique<VectorHasher>(type->childAt(channel), channel));
    }
    auto table = HashTable<true>::createForJoin(
        std::move(keyHashers),
        {},
        true,
        false,
        BaseHashTable::HashMode::kArray,
        1'000,
        pool(),
        GetParam().jitRowEqVectors);
    table->setSimdEnabled(false);
    auto testHelper = HashTableTestHelper<true>::create(table.get());
    const uint64_t numDistincts = bits::nextPowerOfTwo(
        2UL * std::numeric_limits<int32_t>::max() / testHelper.tableSlotSize());
    const uint64_t totalSize = numDistincts * testHelper.tableSlotSize();
    testHelper.allocateTables(numDistincts);
    const auto bucketSize = testHelper.bucketSize();

    struct {
      uint64_t offset;
      bool expectedNextOffsetError;
      uint64_t expectedNextOffset;

      std::string debugString() const {
        return fmt::format(
            "offset {}, expectedNextOffsetError {}, expectedNextOffset {}",
            succinctBytes(offset),
            expectedNextOffsetError,
            succinctBytes(expectedNextOffset));
      }
    } testSettings[] = {
        {1, true, 0},
        {bucketSize - 1, true, 0},
        {bucketSize + 1, true, 0},
        {0, false, bucketSize},
        {bucketSize, false, bucketSize + bucketSize},
        {bits::nextPowerOfTwo(std::numeric_limits<int32_t>::max()),
         false,
         bits::nextPowerOfTwo(std::numeric_limits<int32_t>::max()) +
             bucketSize},
        {bits::nextPowerOfTwo(std::numeric_limits<int32_t>::max()) + 1,
         true,
         0},
        {bits::nextPowerOfTwo(std::numeric_limits<int32_t>::max()) + bucketSize,
         false,
         bits::nextPowerOfTwo(std::numeric_limits<int32_t>::max()) +
             2 * bucketSize},
        {totalSize, true, 0},
        {totalSize + bucketSize, true, 0},
        {totalSize + 1, true, 0},
        {totalSize - bucketSize, false, 0}};

    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());

      if (testData.expectedNextOffsetError) {
        BOLT_ASSERT_THROW(testHelper.nextBucketOffset(testData.offset), "");
      } else {
        ASSERT_EQ(
            testHelper.nextBucketOffset(testData.offset),
            testData.expectedNextOffset);
      }
    }
  };

  const auto type = ROW({"key"}, {ROW({"k1"}, {BIGINT()})});
  runTest(BaseHashTable::HashMode::kHash, type);
  runTest(BaseHashTable::HashMode::kNormalizedKey, type);
}

BOLT_INSTANTIATE_TEST_SUITE_P(
    HashTableTests,
    HashTableTest,
    testing::ValuesIn(HashTableTest::getTestParams()));

/// This tests an issue only seen when the number of unique entries
/// in the HashTable, crosses over int32 limit. The HashTable::loadTag()
/// offset argument was int32 and for positions greater than int32 max,
/// it would seg fault.
TEST_P(HashTableTest, offsetOverflowLoadTags) {
  GTEST_SKIP() << "Skipping as it takes long time to converge,"
                  " re-enable to reproduce the issue";
  if (GetParam().enableRunParallel == true) {
    return;
  }
  auto rowType = ROW({"a"}, {BIGINT()});
  auto table = createHashTableForAggregation(
      rowType, rowType->size(), GetParam().jitRowEqVectors);
  table->hashMode();
  auto lookup = std::make_unique<HashLookup>(
      table->hashers(), GetParam().jitRowEqVectors);
  auto batchSize = 1 << 25;
  for (auto i = 0; i < 64; ++i) {
    std::vector<RowVectorPtr> batches;
    makeRows(batchSize, 1, i * batchSize, rowType, batches);
    insertGroups(*batches.back(), *lookup, *table);
  }
}

DEBUG_ONLY_TEST_P(HashTableTest, failureInCreateRowPartitions) {
  // This tests an issue in parallelJoinBuild where an exception in
  // createRowPartitions could lead to concurrency issues in async table
  // partitioning threads.

  // It is only relevant when the parallel join build is enabled.
  if (!GetParam().enableRunParallel) {
    return;
  }

  // Create a table, and 3 "other" tables.
  std::unique_ptr<HashTable<false>> topTable;
  std::vector<std::unique_ptr<BaseHashTable>> otherTables;
  for (int i = 0; i < 4; i++) {
    auto batch = makeRowVector({makeFlatVector<int64_t>(10, folly::identity)});
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));
    // Set minTableSizeForParallelJoinBuild to be really small so we can trigger
    // a parallel join build without needing a lot of data.
    auto table = HashTable<false>::createForJoin(
        std::move(hashers),
        {BIGINT()},
        true,
        false,
        BaseHashTable::HashMode::kArray,
        1,
        pool(),
        GetParam().jitRowEqVectors);
    table->setSimdEnabled(false);
    copyVectorsToTable({batch}, 0, table.get());

    if (topTable == nullptr) {
      topTable = std::move(table);
    } else {
      otherTables.emplace_back(std::move(table));
    }
  }

  topTable->prepareJoinTable(std::move(otherTables), executor_.get());
  auto topTabletestHelper = HashTableTestHelper<false>::create(topTable.get());

  const std::string expectedFailureMessage =
      "Triggering expected failure in allocation";

  // Fail when allocating memory for the third table.  So we know 2
  // RowPartitions have been created.
  std::atomic_int allocateCount{0};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::common::memory::MemoryPoolImpl::allocateNonContiguous",
      std::function<void(void*)>(([&](void*) {
        if (++allocateCount >= 3) {
          BOLT_FAIL(expectedFailureMessage);
        }
      })));

  std::atomic_bool moveReady{false};
  folly::EventCount moveWait;
  std::atomic_bool prepareReady{false};
  folly::EventCount prepareWait;
  std::atomic_int moveCount{0};
  // Wait until prepare hash been called at least once to call move.  This way
  // we know at least one async thread is running.
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::AsyncSource::move",
      std::function<void(void*)>(([&](void*) {
        // We only need to do this the first time it's called.
        if (++moveCount == 1) {
          prepareWait.await([&]() { return prepareReady.load(); });
          moveReady.store(true);
          moveWait.notifyAll();
        }
      })));

  // Make any async table partitioning threads wait until move is
  // called. Since move blocks until the threads complete, this is as
  // long as the threads can possibly wait to begin processing. (I.e. we've
  // given as much time as we can for something to go wrong.)
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::AsyncSource::prepare",
      std::function<void(void*)>(([&](void*) {
        prepareReady.store(true);
        prepareWait.notifyAll();
        moveWait.await([&]() { return moveReady.load(); });
      })));

  // Set a flag so we know if something in the future causes the test to miss
  // the parallelJoinBuild function which is what we're targeting.
  std::atomic<bool> isParallelBuild{false};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::HashTable::parallelJoinBuild",
      std::function<void(void*)>([&](void*) { isParallelBuild = true; }));

  // We expect this to trigger the exception from the TestValue we set for
  // allocateNonContiguous.
  // Set hash mode to HASH and numNew to something much larger than the
  // capacity to trigger a rehash.
  BOLT_ASSERT_THROW(
      topTabletestHelper.setHashMode(
          BaseHashTable::HashMode::kHash, topTable->capacity() * 2),
      expectedFailureMessage);

  // Double check that the parallelJoinBuild function was called.
  ASSERT_TRUE(isParallelBuild.load());

  // Any outstanding async work should be finish cleanly despite the exception.
  executor_->join();
}

TEST_P(HashTableTest, toStringSingleKey) {
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));

  auto table = HashTable<false>::createForJoin(
      std::move(hashers),
      {}, /*dependentTypes*/
      true /*allowDuplicates*/,
      false /*hasProbedFlag*/,
      BaseHashTable::HashMode::kArray,
      1 /*minTableSizeForParallelJoinBuild*/,
      pool(),
      GetParam().jitRowEqVectors);
  table->setSimdEnabled(false);

  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row / 2; }),
  });

  store(*table->rows(), data);

  table->prepareJoinTable({});

  ASSERT_NO_THROW(table->toString());
  ASSERT_NO_THROW(table->toString(0));
  ASSERT_NO_THROW(table->toString(10));
  ASSERT_NO_THROW(table->toString(1000));
  ASSERT_NO_THROW(table->toString(31, 5));
}

TEST_P(HashTableTest, toStringMultipleKeys) {
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));
  hashers.push_back(std::make_unique<VectorHasher>(VARCHAR(), 1));

  auto table = HashTable<false>::createForJoin(
      std::move(hashers),
      {}, /*dependentTypes*/
      true /*allowDuplicates*/,
      false /*hasProbedFlag*/,
      BaseHashTable::HashMode::kArray,
      1 /*minTableSizeForParallelJoinBuild*/,
      pool(),
      GetParam().jitRowEqVectors);
  table->setSimdEnabled(false);

  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row / 2; }),
      makeFlatVector<std::string>(
          size, [](auto row) { return std::string(row, 'x'); }),
  });

  store(*table->rows(), data);

  table->prepareJoinTable({});

  ASSERT_NO_THROW(table->toString());
}

TEST(HashTableTest, tableInsertPartitionInfo) {
  std::vector<char*> overflows;
  const auto testFn = [&](PartitionBoundIndexType start,
                          PartitionBoundIndexType end) {
    TableInsertPartitionInfo info{start, end, overflows};
  };
  struct {
    PartitionBoundIndexType start;
    PartitionBoundIndexType end;

    std::string debugString() const {
      return fmt::format("start {}, end {}", start, end);
    }
  } badSettings[] = {
      {0, 0}, {-2, -1}, {-1, -1}, {-1, 0}, {-1, 1}, {32, 1}, {32, 32}};
  for (const auto& badData : badSettings) {
    SCOPED_TRACE(badData.debugString());
    BOLT_ASSERT_THROW(testFn(badData.start, badData.end), "");
  }
  ASSERT_TRUE(overflows.empty());

  TableInsertPartitionInfo info{1, 1000, overflows};
  ASSERT_TRUE(info.inRange(1));
  ASSERT_FALSE(info.inRange(0));
  ASSERT_FALSE(info.inRange(-1));
  ASSERT_TRUE(info.inRange(999));
  ASSERT_FALSE(info.inRange(1'000));
  ASSERT_FALSE(info.inRange(12'000));
  ASSERT_TRUE(overflows.empty());

  const std::vector<uint64_t> insertBuffers{100, 200, 300, 500};
  for (const auto insertBuffer : insertBuffers) {
    info.addOverflow(reinterpret_cast<char*>(insertBuffer));
  }
  ASSERT_EQ(overflows.size(), insertBuffers.size());
  for (int i = 0; i < insertBuffers.size(); ++i) {
    ASSERT_EQ(insertBuffers[i], reinterpret_cast<uint64_t>(info.overflows[i]));
  }
  for (int i = 0; i < overflows.size(); ++i) {
    ASSERT_EQ(overflows[i], info.overflows[i]);
  }
}
} // namespace bytedance::bolt::exec::test
