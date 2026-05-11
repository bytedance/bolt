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

#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsUtil.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/file/FileSystems.h"

#include "gtest/gtest.h"
using namespace bytedance::bolt::filesystems;

TEST(HdfsUtilTest, getHdfsPath) {
  const std::string& kScheme = "hdfs://";
  std::string path1 =
      getHdfsPath("hdfs://hdfsCluster/user/hive/a.txt", kScheme);
  EXPECT_EQ("/user/hive/a.txt", path1);

  std::string path2 =
      getHdfsPath("hdfs://localhost:9000/user/hive/a.txt", kScheme);
  EXPECT_EQ("/user/hive/a.txt", path2);

  std::string path3 = getHdfsPath("hdfs:///user/hive/a.txt", kScheme);
  EXPECT_EQ("/user/hive/a.txt", path3);
}

TEST(HdfsUtilTest, hdfsOpenFileOptionKeys) {
  EXPECT_STREQ(HdfsOpenFileOptions::kBufferSize, "bolt.io.file.buffer.size");
  EXPECT_STREQ(HdfsOpenFileOptions::kReplication, "bolt.dfs.replication");
  EXPECT_STREQ(HdfsOpenFileOptions::kBlockSize, "bolt.dfs.blocksize");
}

TEST(HdfsUtilTest, getHdfsOpenFileOptions) {
  FileOptions defaultOptions;
  const auto defaultHdfsOptions = getHdfsOpenFileOptions(defaultOptions);
  EXPECT_EQ(defaultHdfsOptions.bufferSize, 0);
  EXPECT_EQ(defaultHdfsOptions.replication, 0);
  EXPECT_EQ(defaultHdfsOptions.blockSize, 0);

  FileOptions options;
  options.values[HdfsOpenFileOptions::kBufferSize] = "4096";
  options.values[HdfsOpenFileOptions::kReplication] = "2";
  options.values[HdfsOpenFileOptions::kBlockSize] = "134217728";

  const auto hdfsOptions = getHdfsOpenFileOptions(options);
  EXPECT_EQ(hdfsOptions.bufferSize, 4096);
  EXPECT_EQ(hdfsOptions.replication, 2);
  EXPECT_EQ(hdfsOptions.blockSize, 134217728);
}

TEST(HdfsUtilTest, getHdfsOpenFileOptionsRejectsInvalidValues) {
  FileOptions negativeReplication;
  negativeReplication.values[HdfsOpenFileOptions::kReplication] = "-1";
  EXPECT_THROW(
      getHdfsOpenFileOptions(negativeReplication),
      bytedance::bolt::BoltException);

  FileOptions largeReplication;
  largeReplication.values[HdfsOpenFileOptions::kReplication] = "32768";
  EXPECT_THROW(
      getHdfsOpenFileOptions(largeReplication), bytedance::bolt::BoltException);

  FileOptions invalidBufferSize;
  invalidBufferSize.values[HdfsOpenFileOptions::kBufferSize] = "not-a-number";
  EXPECT_THROW(
      getHdfsOpenFileOptions(invalidBufferSize),
      bytedance::bolt::BoltException);
}

TEST(HdfsUtilTest, setHdfsOpenFileOptionsFromConfig) {
  auto config = std::make_shared<const bytedance::bolt::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {HdfsOpenFileOptions::kBufferSize, "4096"},
          {HdfsOpenFileOptions::kReplication, "2"},
          {HdfsOpenFileOptions::kBlockSize, "134217728"}});

  FileOptions fileOptions;
  setHdfsOpenFileOptionsFromConfig(config.get(), fileOptions);

  EXPECT_EQ(fileOptions.values.at(HdfsOpenFileOptions::kBufferSize), "4096");
  EXPECT_EQ(fileOptions.values.at(HdfsOpenFileOptions::kReplication), "2");
  EXPECT_EQ(
      fileOptions.values.at(HdfsOpenFileOptions::kBlockSize), "134217728");
}
