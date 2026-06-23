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

#include "bolt/common/config/Config.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonParquetReader.h"

namespace bytedance::bolt::connector::paimon {

class PaimonConfigTest : public ::testing::Test {
 protected:
  // Helper: create a ConfigBase with the given key-value pairs.
  static std::shared_ptr<config::ConfigBase> makeConfig(
      std::unordered_map<std::string, std::string> values) {
    return std::make_shared<config::ConfigBase>(std::move(values));
  }

  // Empty config — all defaults should apply.
  std::shared_ptr<config::ConfigBase> empty_ =
      makeConfig(std::unordered_map<std::string, std::string>{});
};

// ===========================================================================
// Default values
// ===========================================================================

TEST_F(PaimonConfigTest, TestConfigDefaults) {
  PaimonConfig cfg(empty_);

  // Batch / Row Reading
  EXPECT_EQ(cfg.readBatchSize(), 16384);
  EXPECT_FALSE(cfg.multiThreadRowToBatch());
  EXPECT_EQ(cfg.rowToBatchThreadNumber(), 1);

  // Prefetch & I/O
  EXPECT_FALSE(cfg.prefetchEnabled());
  EXPECT_EQ(cfg.prefetchBatchCount(), 600);
  EXPECT_EQ(cfg.prefetchMaxParallel(), 3);

  // Filter Pushdown
  EXPECT_TRUE(cfg.predicateFilterEnabled());

  // I/O Tuning
  EXPECT_EQ(cfg.naturalReadSize(), 32ULL << 20); // 32 MB
  EXPECT_TRUE(cfg.coalesceReads());
}

// ===========================================================================
// Multi-thread row-to-batch config
// ===========================================================================

TEST_F(PaimonConfigTest, TestMultiThreadRowToBatchConfig) {
  // Enable with default thread count.
  {
    auto cfg = makeConfig({{PaimonConfig::kMultiThreadRowToBatch, "true"}});
    PaimonConfig paimonCfg(cfg);
    EXPECT_TRUE(paimonCfg.multiThreadRowToBatch());
    EXPECT_EQ(paimonCfg.rowToBatchThreadNumber(), 1);
  }

  // Enable with custom thread count and custom batch size.
  {
    auto cfg = makeConfig({
        {PaimonConfig::kMultiThreadRowToBatch, "true"},
        {PaimonConfig::kRowToBatchThreadNum, "4"},
        {PaimonConfig::kReadBatchSize, "4096"},
    });
    PaimonConfig paimonCfg(cfg);
    EXPECT_TRUE(paimonCfg.multiThreadRowToBatch());
    EXPECT_EQ(paimonCfg.rowToBatchThreadNumber(), 4);
    EXPECT_EQ(paimonCfg.readBatchSize(), 4096);
  }
}

// ===========================================================================
// Prefetch config
// ===========================================================================

TEST_F(PaimonConfigTest, TestPrefetchConfig) {
  // Enable with default tuning params.
  {
    auto cfg = makeConfig({{PaimonConfig::kPrefetchEnabled, "true"}});
    PaimonConfig paimonCfg(cfg);
    EXPECT_TRUE(paimonCfg.prefetchEnabled());
    EXPECT_EQ(paimonCfg.prefetchBatchCount(), 600);
    EXPECT_EQ(paimonCfg.prefetchMaxParallel(), 3);
  }

  // Enable with custom tuning params.
  {
    auto cfg = makeConfig({
        {PaimonConfig::kPrefetchEnabled, "true"},
        {PaimonConfig::kPrefetchBatchCount, "1200"},
        {PaimonConfig::kPrefetchMaxParallel, "6"},
    });
    PaimonConfig paimonCfg(cfg);
    EXPECT_TRUE(paimonCfg.prefetchEnabled());
    EXPECT_EQ(paimonCfg.prefetchBatchCount(), 1200);
    EXPECT_EQ(paimonCfg.prefetchMaxParallel(), 6);
  }
}

// ===========================================================================
// Predicate filter config
// ===========================================================================

TEST_F(PaimonConfigTest, TestPredicateFilterConfig) {
  // Default is enabled.
  {
    PaimonConfig cfg(empty_);
    EXPECT_TRUE(cfg.predicateFilterEnabled());
  }

  // Can be disabled.
  {
    auto cfg = makeConfig({{PaimonConfig::kPredicateFilterEnabled, "false"}});
    PaimonConfig paimonCfg(cfg);
    EXPECT_FALSE(paimonCfg.predicateFilterEnabled());
  }
}

// ===========================================================================
// Parquet reader config
// ===========================================================================

TEST_F(PaimonConfigTest, TestParquetReaderConfig) {
  // Custom natural read size.
  {
    auto cfg =
        makeConfig({{PaimonConfig::kNaturalReadSize, "52428800"}}); // 50 MB
    PaimonConfig paimonCfg(cfg);
    EXPECT_EQ(paimonCfg.naturalReadSize(), 52428800ULL);
  }

  // Disable coalescing.
  {
    auto cfg = makeConfig({{PaimonConfig::kCoalesceReads, "false"}});
    PaimonConfig paimonCfg(cfg);
    EXPECT_FALSE(paimonCfg.coalesceReads());
  }
}

// ===========================================================================
// I/O options flow from config map → PaimonParquetReader → PaimonReadFile
// ===========================================================================

TEST_F(PaimonConfigTest, TestIoTuningConfig) {
  // PaimonIoOptions struct has correct defaults.
  {
    PaimonIoOptions opts;
    EXPECT_EQ(opts.naturalReadSize, 32ULL << 20); // 32 MB
    EXPECT_TRUE(opts.coalesceReads);
  }

  // PaimonReadFile::getNaturalReadSize() and shouldCoalesce() return the
  // values wired through PaimonIoOptions.  We pass a null InputStream because
  // those two methods only read from io_ and never dereference input_.
  {
    PaimonIoOptions customOpts;
    customOpts.naturalReadSize = 52428800ULL; // 50 MB
    customOpts.coalesceReads = false;
    PaimonReadFile rf(nullptr, customOpts);
    EXPECT_EQ(rf.getNaturalReadSize(), 52428800ULL);
    EXPECT_FALSE(rf.shouldCoalesce());
  }

  // Default options produce default return values.
  {
    PaimonIoOptions defaultOpts;
    PaimonReadFile rf(nullptr, defaultOpts);
    EXPECT_EQ(rf.getNaturalReadSize(), 32ULL << 20); // 32 MB
    EXPECT_TRUE(rf.shouldCoalesce());
  }

  // PaimonParquetReader parses options from the map into ioOptions_, which
  // then flows to PaimonReadFile via CreateReaderBuilder → Build().
  {
    auto opts = std::map<std::string, std::string>{
        {PaimonConfig::kNaturalReadSize, "10485760"}, // 10 MB
        {PaimonConfig::kCoalesceReads, "false"},
    };
    PaimonParquetReader reader(opts);
    auto result = reader.CreateReaderBuilder(4096);
    ASSERT_TRUE(result.ok())
        << "CreateReaderBuilder should succeed with valid options";
  }

  // Missing options keep defaults.
  {
    auto opts = std::map<std::string, std::string>{};
    PaimonParquetReader reader(opts);
    auto result = reader.CreateReaderBuilder(4096);
    ASSERT_TRUE(result.ok())
        << "CreateReaderBuilder should succeed with empty options";
  }
}

} // namespace bytedance::bolt::connector::paimon
