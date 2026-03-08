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

TEST(OptionsTests, defaultAppendRowNumberColumnTest) {
  // appendRowNumberColumn flag should be false by default
  RowReaderOptions rowReaderOptions;
  ASSERT_EQ(false, rowReaderOptions.getAppendRowNumberColumn());
}

TEST(OptionsTests, setAppendRowNumberColumnToTrueTest) {
  RowReaderOptions rowReaderOptions;
  rowReaderOptions.setAppendRowNumberColumn(true);
  ASSERT_EQ(true, rowReaderOptions.getAppendRowNumberColumn());
}

TEST(OptionsTests, testAppendRowNumberColumnInCopy) {
  RowReaderOptions rowReaderOptions;
  RowReaderOptions rowReaderOptionsCopy{rowReaderOptions};
  ASSERT_EQ(false, rowReaderOptionsCopy.getAppendRowNumberColumn());

  rowReaderOptions.setAppendRowNumberColumn(true);
  RowReaderOptions rowReaderOptionsSecondCopy{rowReaderOptions};
  ASSERT_EQ(true, rowReaderOptionsSecondCopy.getAppendRowNumberColumn());
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
  auto roundTrip = WriterOptions::deserialize(dyn);

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
  auto roundTrip = WriterOptions::deserialize(dyn);

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

  auto rt = WriterOptions::deserialize(opts.serialize());

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

  auto rt = WriterOptions::deserialize(opts.serialize());

  ASSERT_EQ(nullptr, rt->spillConfig);
  ASSERT_EQ(nullptr, rt->ownedSpillConfig.get());
}

TEST_F(WriterOptionsSerDeTest, writerOptionsNoSchemaRoundTrip) {
  WriterOptions opts;
  opts.compressionKind = common::CompressionKind_ZLIB;
  opts.serdeParameters["key"] = "value";

  folly::dynamic dyn = opts.serialize();
  auto roundTrip = WriterOptions::deserialize(dyn);

  ASSERT_EQ(nullptr, roundTrip->schema);
  ASSERT_TRUE(roundTrip->compressionKind.has_value());
  ASSERT_EQ(common::CompressionKind_ZLIB, roundTrip->compressionKind.value());
  ASSERT_EQ(1u, roundTrip->serdeParameters.size());
  ASSERT_EQ("value", roundTrip->serdeParameters.at("key"));
}
