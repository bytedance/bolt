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

#include "bolt/common/base/SpillConfig.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/type/Type.h"

using namespace ::testing;
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;

class WriterOptionsSerDeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    WriterOptions::registerSerDe();
  }
};

TEST(OptionsTests, defaultRowNumberColumnInfoTest) {
  RowReaderOptions rowReaderOptions;
  ASSERT_EQ(std::nullopt, rowReaderOptions.getRowNumberColumnInfo());
}

TEST(OptionsTests, setRowNumberColumnInfoTest) {
  RowReaderOptions rowReaderOptions;
  RowNumberColumnInfo rowNumberColumnInfo;
  rowNumberColumnInfo.insertPosition = 0;
  rowNumberColumnInfo.name = "test";
  rowReaderOptions.setRowNumberColumnInfo(rowNumberColumnInfo);
  auto readBack = rowReaderOptions.getRowNumberColumnInfo();
  ASSERT_TRUE(readBack.has_value());
  ASSERT_EQ(rowNumberColumnInfo.insertPosition, readBack->insertPosition);
  ASSERT_EQ(rowNumberColumnInfo.name, readBack->name);
}

TEST(OptionsTests, testRowNumberColumnInfoInCopy) {
  RowReaderOptions rowReaderOptions;
  RowReaderOptions rowReaderOptionsCopy{rowReaderOptions};
  ASSERT_EQ(std::nullopt, rowReaderOptionsCopy.getRowNumberColumnInfo());

  RowNumberColumnInfo rowNumberColumnInfo;
  rowNumberColumnInfo.insertPosition = 0;
  rowNumberColumnInfo.name = "test";
  rowReaderOptions.setRowNumberColumnInfo(rowNumberColumnInfo);
  RowReaderOptions rowReaderOptionsSecondCopy{rowReaderOptions};
  auto readBack = rowReaderOptionsSecondCopy.getRowNumberColumnInfo();
  ASSERT_TRUE(readBack.has_value());
  ASSERT_EQ(rowNumberColumnInfo.insertPosition, readBack->insertPosition);
  ASSERT_EQ(rowNumberColumnInfo.name, readBack->name);
}

TEST_F(WriterOptionsSerDeTest, writerOptionsSerializeDeserializeRoundTrip) {
  WriterOptions opts;

  // 1) schema (use a simple type so serialization is stable)
  opts.schema = ROW({"c1", "c2"}, {INTEGER(), VARCHAR()});

  // 2) compression kind
  opts.compressionKind = common::CompressionKind_ZSTD;

  // 3) serde parameters
  opts.serdeParameters["k1"] = "v1";
  opts.serdeParameters["k2"] = "v2";

  // 4) maxStripeSize
  opts.maxStripeSize = 128UL * 1024 * 1024;

  // 5) arrowBridgeTimestampUnit
  opts.arrowBridgeTimestampUnit = 3;

  // 6) zlibCompressionLevel
  opts.zlibCompressionLevel = 6;

  // ---- serialize ----
  folly::dynamic dyn = opts.serialize();

  // ---- deserialize ----
  auto roundTrip = WriterOptions::create(dyn);

  // ---- verify ----

  ASSERT_TRUE(roundTrip->schema != nullptr);
  ASSERT_EQ(opts.schema->toString(), roundTrip->schema->toString());

  ASSERT_TRUE(roundTrip->compressionKind.has_value());
  ASSERT_EQ(opts.compressionKind.value(), roundTrip->compressionKind.value());

  ASSERT_EQ(opts.serdeParameters.size(), roundTrip->serdeParameters.size());
  ASSERT_EQ(opts.serdeParameters.at("k1"), roundTrip->serdeParameters.at("k1"));
  ASSERT_EQ(opts.serdeParameters.at("k2"), roundTrip->serdeParameters.at("k2"));

  ASSERT_TRUE(roundTrip->maxStripeSize.has_value());
  ASSERT_EQ(opts.maxStripeSize.value(), roundTrip->maxStripeSize.value());

  ASSERT_TRUE(roundTrip->arrowBridgeTimestampUnit.has_value());
  ASSERT_EQ(
      opts.arrowBridgeTimestampUnit.value(),
      roundTrip->arrowBridgeTimestampUnit.value());

  ASSERT_TRUE(roundTrip->zlibCompressionLevel.has_value());
  ASSERT_EQ(
      opts.zlibCompressionLevel.value(),
      roundTrip->zlibCompressionLevel.value());
}

TEST_F(WriterOptionsSerDeTest, writerOptionsDefaultsRoundTrip) {
  WriterOptions opts;

  folly::dynamic dyn = opts.serialize();
  auto roundTrip = WriterOptions::create(dyn);

  ASSERT_EQ(nullptr, roundTrip->schema);
  ASSERT_FALSE(roundTrip->compressionKind.has_value());
  ASSERT_TRUE(roundTrip->serdeParameters.empty());
  ASSERT_FALSE(roundTrip->maxStripeSize.has_value());
  ASSERT_FALSE(roundTrip->arrowBridgeTimestampUnit.has_value());
  ASSERT_FALSE(roundTrip->zlibCompressionLevel.has_value());
  ASSERT_EQ(nullptr, roundTrip->spillConfig);
  ASSERT_EQ(nullptr, roundTrip->ownedSpillConfig.get());
}

TEST_F(WriterOptionsSerDeTest, writerOptionsWithSpillConfigRoundTrip) {
  SpillConfig cfg;
  cfg.fileNamePrefix = "writer_spill";
  cfg.maxFileSize = 256UL * 1024 * 1024;
  cfg.spillUringEnabled = true;
  cfg.writeBufferSize = 8UL * 1024 * 1024;
  cfg.minSpillableReservationPct = 5;
  cfg.spillableReservationGrowthPct = 15;
  cfg.startPartitionBit = 29;
  cfg.joinPartitionBits = 4;
  cfg.joinRepartitionBits = 4;
  cfg.maxSpillLevel = -1;
  cfg.maxSpillRunRows = 0;
  cfg.writerFlushThresholdSize = 32UL * 1024 * 1024;
  cfg.testSpillPct = 0;
  cfg.compressionKind = CompressionKind_NONE;
  cfg.rowBasedSpillMode = RowBasedSpillMode::RAW;
  cfg.jitEnabled = false;

  WriterOptions opts;
  opts.spillConfig = &cfg;

  auto rt = WriterOptions::create(opts.serialize());

  ASSERT_NE(nullptr, rt->spillConfig);
  ASSERT_NE(nullptr, rt->ownedSpillConfig.get());
  ASSERT_EQ(rt->spillConfig, rt->ownedSpillConfig.get());
  ASSERT_EQ(cfg.fileNamePrefix, rt->spillConfig->fileNamePrefix);
  ASSERT_EQ(cfg.maxFileSize, rt->spillConfig->maxFileSize);
  ASSERT_EQ(cfg.spillUringEnabled, rt->spillConfig->spillUringEnabled);
  ASSERT_EQ(cfg.rowBasedSpillMode, rt->spillConfig->rowBasedSpillMode);
  ASSERT_EQ(cfg.jitEnabled, rt->spillConfig->jitEnabled);
}

TEST_F(WriterOptionsSerDeTest, writerOptionsNoSpillConfigRoundTrip) {
  WriterOptions opts;
  ASSERT_EQ(nullptr, opts.spillConfig);

  auto rt = WriterOptions::create(opts.serialize());

  ASSERT_EQ(nullptr, rt->spillConfig);
  ASSERT_EQ(nullptr, rt->ownedSpillConfig.get());
}

TEST_F(WriterOptionsSerDeTest, writerOptionsNoSchemaRoundTrip) {
  WriterOptions opts;
  opts.compressionKind = common::CompressionKind_ZLIB;
  opts.serdeParameters["key"] = "value";

  folly::dynamic dyn = opts.serialize();
  auto roundTrip = WriterOptions::create(dyn);

  ASSERT_EQ(nullptr, roundTrip->schema);
  ASSERT_TRUE(roundTrip->compressionKind.has_value());
  ASSERT_EQ(common::CompressionKind_ZLIB, roundTrip->compressionKind.value());
  ASSERT_EQ(1u, roundTrip->serdeParameters.size());
  ASSERT_EQ("value", roundTrip->serdeParameters.at("key"));
}

class ReaderOptionsSerDeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    bytedance::bolt::memory::MemoryManager::testingSetInstance(
        bytedance::bolt::memory::MemoryManager::Options{});
    bytedance::bolt::dwio::common::ReaderOptions::registerSerDe();
  }
};

TEST_F(ReaderOptionsSerDeTest, readerOptionsSerializeDeserializeRoundTrip) {
  auto rootPool = bytedance::bolt::memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("test_pool");
  bytedance::bolt::dwio::common::ReaderOptions opts(pool.get());

  // Set various options
  opts.setTailLocation(12345);
  opts.setFileFormat(FileFormat::PARQUET);
  opts.setFileSchema(ROW({"c1", "c2"}, {INTEGER(), VARCHAR()}));

  SerDeOptions serdeOpts('a', 'b', 'c', 'd', true);
  serdeOpts.nullString = "NULL_VAL";
  serdeOpts.lastColumnTakesRest = true;
  opts.setSerDeOptions(serdeOpts);

  opts.setFooterEstimatedSize(1024 * 1024);
  opts.setFilePreloadThreshold(8 * 1024 * 1024);
  opts.setFileColumnNamesReadAsLowerCase(true);
  opts.setUseColumnNamesForColumnMapping(false);
  opts.setUseNestedColumnNamesForColumnMapping(true);

  opts.setAutoPreloadLength(1000);
  opts.setPrefetchMode(bytedance::bolt::io::PrefetchMode::PREFETCH);
  opts.setLoadQuantum(2000);
  opts.setMaxCoalesceDistance(3000);
  opts.setMaxCoalesceBytes(4000);
  opts.setPrefetchRowGroups(5);
  opts.setPrefetchMemoryPercent(50);

  // ---- serialize ----
  folly::dynamic dyn = opts.serialize();

  // ---- deserialize ----
  auto roundTrip =
      bytedance::bolt::dwio::common::ReaderOptions::create(dyn, pool.get());

  // ---- verify ----
  ASSERT_EQ(opts.getTailLocation(), roundTrip.getTailLocation());
  ASSERT_EQ(opts.getFileFormat(), roundTrip.getFileFormat());
  ASSERT_EQ(
      opts.getFileSchema()->toString(), roundTrip.getFileSchema()->toString());

  auto& rtSerDe = roundTrip.getSerDeOptions();
  ASSERT_EQ(serdeOpts.separators[0], rtSerDe.separators[0]);
  ASSERT_EQ(serdeOpts.separators[1], rtSerDe.separators[1]);
  ASSERT_EQ(serdeOpts.separators[2], rtSerDe.separators[2]);
  ASSERT_EQ(serdeOpts.separators[3], rtSerDe.separators[3]);
  ASSERT_EQ(serdeOpts.nullString, rtSerDe.nullString);
  ASSERT_EQ(serdeOpts.lastColumnTakesRest, rtSerDe.lastColumnTakesRest);
  ASSERT_EQ(serdeOpts.escapeChar, rtSerDe.escapeChar);
  ASSERT_EQ(serdeOpts.isEscaped, rtSerDe.isEscaped);

  ASSERT_EQ(opts.getFooterEstimatedSize(), roundTrip.getFooterEstimatedSize());
  ASSERT_EQ(
      opts.getFilePreloadThreshold(), roundTrip.getFilePreloadThreshold());
  ASSERT_EQ(
      opts.isFileColumnNamesReadAsLowerCase(),
      roundTrip.isFileColumnNamesReadAsLowerCase());
  ASSERT_EQ(
      opts.isUseColumnNamesForColumnMapping(),
      roundTrip.isUseColumnNamesForColumnMapping());
  ASSERT_EQ(
      opts.useNestedColumnNamesForColumnMapping(),
      roundTrip.useNestedColumnNamesForColumnMapping());

  ASSERT_EQ(opts.getAutoPreloadLength(), roundTrip.getAutoPreloadLength());
  ASSERT_EQ(opts.getPrefetchMode(), roundTrip.getPrefetchMode());
  ASSERT_EQ(opts.loadQuantum(), roundTrip.loadQuantum());
  ASSERT_EQ(opts.maxCoalesceDistance(), roundTrip.maxCoalesceDistance());
  ASSERT_EQ(opts.maxCoalesceBytes(), roundTrip.maxCoalesceBytes());
  ASSERT_EQ(opts.prefetchRowGroups(), roundTrip.prefetchRowGroups());
  ASSERT_EQ(opts.prefetchMemoryPercent(), roundTrip.prefetchMemoryPercent());
}

TEST_F(ReaderOptionsSerDeTest, readerOptionsDefaultsRoundTrip) {
  auto rootPool = bytedance::bolt::memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("test_pool");
  bytedance::bolt::dwio::common::ReaderOptions opts(pool.get());

  folly::dynamic dyn = opts.serialize();
  auto roundTrip =
      bytedance::bolt::dwio::common::ReaderOptions::create(dyn, pool.get());

  ASSERT_EQ(opts.getTailLocation(), roundTrip.getTailLocation());
  ASSERT_EQ(opts.getFileFormat(), roundTrip.getFileFormat());
  ASSERT_EQ(nullptr, roundTrip.getFileSchema());

  auto& rtSerDe = roundTrip.getSerDeOptions();
  // Default values check
  ASSERT_EQ('\1', rtSerDe.separators[0]);
  ASSERT_EQ("\\N", rtSerDe.nullString);
  ASSERT_FALSE(rtSerDe.lastColumnTakesRest);

  ASSERT_EQ(
      bytedance::bolt::dwio::common::ReaderOptions::kDefaultFooterEstimatedSize,
      roundTrip.getFooterEstimatedSize());
  ASSERT_EQ(
      bytedance::bolt::dwio::common::ReaderOptions::
          kDefaultFilePreloadThreshold,
      roundTrip.getFilePreloadThreshold());
  ASSERT_FALSE(roundTrip.isFileColumnNamesReadAsLowerCase());
  ASSERT_FALSE(roundTrip.isUseColumnNamesForColumnMapping());
  // Default for useNestedColumnNamesForColumnMapping_ is false
  // But accessor useNestedColumnNamesForColumnMapping() has a check that might
  // fail if useColumnNamesForColumnMapping is set to true. Since both are false
  // by default, it should be safe.
}
