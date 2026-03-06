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

#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/common/config/Config.h"
#include "gtest/gtest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using bytedance::bolt::connector::hive::HiveConfig;

TEST(HiveConfigTest, defaultConfig) {
  HiveConfig hiveConfig(std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>()));
  const auto emptySession = std::make_unique<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(emptySession.get()),
      bytedance::bolt::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kError);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 100);
  ASSERT_EQ(hiveConfig.immutablePartitions(), false);
  ASSERT_EQ(hiveConfig.s3UseVirtualAddressing(), true);
  ASSERT_EQ(hiveConfig.s3GetLogLevel(), "FATAL");
  ASSERT_EQ(hiveConfig.s3UseSSL(), true);
  ASSERT_EQ(hiveConfig.s3UseInstanceCredentials(), false);
  ASSERT_EQ(hiveConfig.s3Endpoint(), "");
  ASSERT_EQ(hiveConfig.s3AccessKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3SecretKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRole(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRoleSessionName(), "bolt-session");
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "");
  ASSERT_EQ(hiveConfig.gcsScheme(), "https");
  ASSERT_EQ(hiveConfig.gcsCredentials(), "");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(emptySession.get()), false);
  ASSERT_EQ(hiveConfig.isParquetUseColumnNames(emptySession.get()), false);
  ASSERT_EQ(
      hiveConfig.isFileColumnNamesReadAsLowerCase(emptySession.get()), false);

  ASSERT_EQ(hiveConfig.maxCoalescedBytes(emptySession.get()), 128 << 20);
  ASSERT_EQ(
      hiveConfig.maxCoalescedDistanceBytes(emptySession.get()), 512 << 10);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 20'000);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), true);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(emptySession.get()),
      64L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(emptySession.get()),
      16L * 1024L * 1024L);

  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(emptySession.get()), 1024);
  ASSERT_EQ(
      hiveConfig.sortWriterMaxOutputBytes(emptySession.get()), 10UL << 20);
  ASSERT_EQ(hiveConfig.isPartitionPathAsLowerCase(emptySession.get()), true);
}

TEST(HiveConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> configFromFile = {
      {HiveConfig::kInsertExistingPartitionsBehavior, "OVERWRITE"},
      {HiveConfig::kMaxPartitionsPerWriters, "120"},
      {HiveConfig::kImmutablePartitions, "true"},
      {HiveConfig::kS3PathStyleAccess, "true"},
      {HiveConfig::kS3LogLevel, "Warning"},
      {HiveConfig::kS3SSLEnabled, "false"},
      {HiveConfig::kS3UseInstanceCredentials, "true"},
      {HiveConfig::kS3Endpoint, "hey"},
      {HiveConfig::kS3AwsAccessKey, "hello"},
      {HiveConfig::kS3AwsSecretKey, "hello"},
      {HiveConfig::kS3IamRole, "hello"},
      {HiveConfig::kS3IamRoleSessionName, "bolt"},
      {HiveConfig::kGcsEndpoint, "hey"},
      {HiveConfig::kGcsScheme, "http"},
      {HiveConfig::kGcsCredentials, "hey"},
      {HiveConfig::kOrcUseColumnNames, "true"},
      {HiveConfig::kFileColumnNamesReadAsLowerCase, "true"},
      {HiveConfig::kParquetUseColumnNames, "true"},
      {HiveConfig::kMaxCoalescedBytes, "100"},
      {HiveConfig::kMaxCoalescedDistanceBytes, "100B"},
      {HiveConfig::kNumCacheFileHandles, "100"},
      {HiveConfig::kEnableFileHandleCache, "false"},
      {HiveConfig::kOrcWriterMaxStripeSize, "100MB"},
      {HiveConfig::kOrcWriterMaxDictionaryMemory, "100MB"},
      {HiveConfig::kSortWriterMaxOutputRows, "100"},
      {HiveConfig::kSortWriterMaxOutputBytes, "100MB"}};
  HiveConfig hiveConfig(
      std::make_shared<config::ConfigBase>(std::move(configFromFile)));
  auto emptySession = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(emptySession.get()),
      bytedance::bolt::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kOverwrite);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 120);
  ASSERT_EQ(hiveConfig.immutablePartitions(), true);
  ASSERT_EQ(hiveConfig.s3UseVirtualAddressing(), false);
  ASSERT_EQ(hiveConfig.s3GetLogLevel(), "Warning");
  ASSERT_EQ(hiveConfig.s3UseSSL(), false);
  ASSERT_EQ(hiveConfig.s3UseInstanceCredentials(), true);
  ASSERT_EQ(hiveConfig.s3Endpoint(), "hey");
  ASSERT_EQ(hiveConfig.s3AccessKey(), std::optional("hello"));
  ASSERT_EQ(hiveConfig.s3SecretKey(), std::optional("hello"));
  ASSERT_EQ(hiveConfig.s3IAMRole(), std::optional("hello"));
  ASSERT_EQ(hiveConfig.s3IAMRoleSessionName(), "bolt");
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "hey");
  ASSERT_EQ(hiveConfig.gcsScheme(), "http");
  ASSERT_EQ(hiveConfig.gcsCredentials(), "hey");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(emptySession.get()), true);
  ASSERT_EQ(hiveConfig.isParquetUseColumnNames(emptySession.get()), true);
  ASSERT_EQ(
      hiveConfig.isFileColumnNamesReadAsLowerCase(emptySession.get()), true);
  ASSERT_EQ(hiveConfig.maxCoalescedBytes(emptySession.get()), 100);
  ASSERT_EQ(hiveConfig.maxCoalescedDistanceBytes(emptySession.get()), 100);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 100);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), false);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(emptySession.get()),
      100L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(emptySession.get()),
      100L * 1024L * 1024L);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(emptySession.get()), 100);
  ASSERT_EQ(
      hiveConfig.sortWriterMaxOutputBytes(emptySession.get()), 100UL << 20);
}

TEST(HiveConfigTest, overrideSession) {
  HiveConfig hiveConfig(std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>()));
  std::unordered_map<std::string, std::string> sessionOverride = {
      {HiveConfig::kInsertExistingPartitionsBehaviorSession, "OVERWRITE"},
      {HiveConfig::kOrcUseColumnNamesSession, "true"},
      {HiveConfig::kFileColumnNamesReadAsLowerCaseSession, "true"},
      {HiveConfig::kParquetUseColumnNamesSession, "true"},
      {HiveConfig::kOrcWriterMaxStripeSizeSession, "22MB"},
      {HiveConfig::kOrcWriterMaxDictionaryMemorySession, "22MB"},
      {HiveConfig::kSortWriterMaxOutputRowsSession, "20"},
      {HiveConfig::kSortWriterMaxOutputBytesSession, "20MB"},
      {HiveConfig::kPartitionPathAsLowerCaseSession, "false"},
      {HiveConfig::kIgnoreMissingFilesSession, "true"}};
  const auto session =
      std::make_unique<config::ConfigBase>(std::move(sessionOverride));
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(session.get()),
      bytedance::bolt::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kOverwrite);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(session.get()), 100);
  ASSERT_EQ(hiveConfig.immutablePartitions(), false);
  ASSERT_EQ(hiveConfig.s3UseVirtualAddressing(), true);
  ASSERT_EQ(hiveConfig.s3GetLogLevel(), "FATAL");
  ASSERT_EQ(hiveConfig.s3UseSSL(), true);
  ASSERT_EQ(hiveConfig.s3UseInstanceCredentials(), false);
  ASSERT_EQ(hiveConfig.s3Endpoint(), "");
  ASSERT_EQ(hiveConfig.s3AccessKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3SecretKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRole(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRoleSessionName(), "bolt-session");
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "");
  ASSERT_EQ(hiveConfig.gcsScheme(), "https");
  ASSERT_EQ(hiveConfig.gcsCredentials(), "");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(session.get()), true);
  ASSERT_EQ(hiveConfig.isParquetUseColumnNames(session.get()), true);
  ASSERT_EQ(hiveConfig.isFileColumnNamesReadAsLowerCase(session.get()), true);

  ASSERT_EQ(hiveConfig.maxCoalescedBytes(session.get()), 128 << 20);
  ASSERT_EQ(hiveConfig.maxCoalescedDistanceBytes(session.get()), 512 << 10);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 20'000);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), true);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(session.get()), 22L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(session.get()),
      22L * 1024L * 1024L);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(session.get()), 20);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputBytes(session.get()), 20UL << 20);
  ASSERT_EQ(hiveConfig.isPartitionPathAsLowerCase(session.get()), false);
  ASSERT_EQ(hiveConfig.ignoreMissingFiles(session.get()), true);
}
