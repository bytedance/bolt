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
 */
/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "bolt/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

using namespace bytedance::bolt::exec::test;

namespace bytedance::bolt::filesystems {
namespace {

class S3ReadTest : public S3Test, public ::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    S3Test::SetUp();
    filesystems::registerS3FileSystem();
    connector::hive::HiveConnectorFactory factory;
    auto hiveConnector =
        factory.newConnector(kHiveConnectorId, minioServer_->hiveConfig());
    connector::registerConnector(hiveConnector);
    parquet::registerParquetReaderFactory();
  }

  void TearDown() override {
    parquet::unregisterParquetReaderFactory();
    filesystems::finalizeS3FileSystem();
    connector::unregisterConnector(kHiveConnectorId);
    S3Test::TearDown();
  }
};
} // namespace

TEST_F(S3ReadTest, s3ReadTest) {
  const auto sourceFile = test::getDataFilePath(
      "bolt/connectors/hive/storage_adapters/s3fs/tests",
      "../../../../../dwio/parquet/tests/examples/int.parquet");
  const char* bucketName = "data";
  const auto destinationFile = S3Test::localPath(bucketName) + "/int.parquet";
  minioServer_->addBucket(bucketName);
  std::ifstream src(sourceFile, std::ios::binary);
  std::ofstream dest(destinationFile, std::ios::binary);
  // Copy source file to destination bucket.
  dest << src.rdbuf();
  ASSERT_GT(dest.tellp(), 0) << "Unable to copy from source " << sourceFile;
  dest.close();

  // Read the parquet file via the S3 bucket.
  auto rowType = ROW({"int", "bigint"}, {INTEGER(), BIGINT()});
  auto plan = PlanBuilder().tableScan(rowType).planNode();
  auto split = HiveConnectorSplitBuilder(s3URI(bucketName, "int.parquet"))
                   .fileFormat(dwio::common::FileFormat::PARQUET)
                   .build();
  auto copy = AssertQueryBuilder(plan).split(split).copyResults(pool());

  // expectedResults is the data in int.parquet file.
  const int64_t kExpectedRows = 10;
  auto expectedResults = makeRowVector(
      {makeFlatVector<int32_t>(
           kExpectedRows, [](auto row) { return row + 100; }),
       makeFlatVector<int64_t>(
           kExpectedRows, [](auto row) { return row + 1000; })});
  assertEqualResults({expectedResults}, {copy});
}
} // namespace bytedance::bolt::filesystems

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
