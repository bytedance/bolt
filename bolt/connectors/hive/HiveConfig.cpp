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
#include "bolt/core/QueryConfig.h"

#include <boost/algorithm/string.hpp>
namespace bytedance::bolt::connector::hive {

namespace {

HiveConfig::InsertExistingPartitionsBehavior
stringToInsertExistingPartitionsBehavior(const std::string& strValue) {
  auto upperValue = boost::algorithm::to_upper_copy(strValue);
  if (upperValue == "ERROR") {
    return HiveConfig::InsertExistingPartitionsBehavior::kError;
  }
  if (upperValue == "OVERWRITE") {
    return HiveConfig::InsertExistingPartitionsBehavior::kOverwrite;
  }
  BOLT_UNSUPPORTED(
      "Unsupported insert existing partitions behavior: {}.", strValue);
}

} // namespace

const std::set<std::string> HiveConfig::hms_session_key = {
    // cloud session config
    "hms_client_region",
    "hms_client_service",
    "hms_client_ak",
    "hms_client_sk",
    "hms_client_session_token",
    "hms_client_filesystem",
    "identity_type",
    "identity_id",
    "account_id",
    // tos session config
    "fs.tos.access.key",
    "fs.tos.secret.key",
    "fs.tos.session.token",
    "las_tos_external_table_credentials",
    // general session config
    "user"};

// static
std::string HiveConfig::insertExistingPartitionsBehaviorString(
    InsertExistingPartitionsBehavior behavior) {
  switch (behavior) {
    case InsertExistingPartitionsBehavior::kError:
      return "ERROR";
    case InsertExistingPartitionsBehavior::kOverwrite:
      return "OVERWRITE";
    default:
      return fmt::format("UNKNOWN BEHAVIOR {}", static_cast<int>(behavior));
  }
}

HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::insertExistingPartitionsBehavior(
    const config::ConfigBase* session) const {
  if (session->valueExists(kInsertExistingPartitionsBehaviorSession)) {
    return stringToInsertExistingPartitionsBehavior(
        session->get<std::string>(kInsertExistingPartitionsBehaviorSession)
            .value());
  }
  const auto behavior =
      config_->get<std::string>(kInsertExistingPartitionsBehavior);
  return behavior.has_value()
      ? stringToInsertExistingPartitionsBehavior(behavior.value())
      : InsertExistingPartitionsBehavior::kError;
}

uint32_t HiveConfig::maxPartitionsPerWriters(
    const config::ConfigBase* session) const {
  if (session->valueExists(kMaxPartitionsPerWritersSession)) {
    return session->get<uint32_t>(kMaxPartitionsPerWritersSession).value();
  }
  return config_->get<uint32_t>(kMaxPartitionsPerWriters, 100);
}

bool HiveConfig::immutablePartitions() const {
  return config_->get<bool>(kImmutablePartitions, false);
}

bool HiveConfig::isOrcUseNestedColumnNames(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kOrcUseNestedColumnNamesSession,
      config_->get<bool>(kOrcUseNestedColumnNames, false));
}

bool HiveConfig::s3UseVirtualAddressing() const {
  return !config_->get(kS3PathStyleAccess, false);
}

std::string HiveConfig::s3GetLogLevel() const {
  return config_->get(kS3LogLevel, std::string("FATAL"));
}

bool HiveConfig::s3UseSSL() const {
  return config_->get(kS3SSLEnabled, true);
}

bool HiveConfig::s3UseInstanceCredentials() const {
  return config_->get(kS3UseInstanceCredentials, false);
}

std::string HiveConfig::s3Endpoint() const {
  return config_->get(kS3Endpoint, std::string(""));
}

std::optional<std::string> HiveConfig::s3AccessKey() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kS3AwsAccessKey));
}

std::optional<std::string> HiveConfig::s3SecretKey() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kS3AwsSecretKey));
}

std::optional<std::string> HiveConfig::s3IAMRole() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kS3IamRole));
}

std::string HiveConfig::s3IAMRoleSessionName() const {
  return config_->get(kS3IamRoleSessionName, std::string("bolt-session"));
}

std::string HiveConfig::gcsEndpoint() const {
  return config_->get<std::string>(kGcsEndpoint, std::string(""));
}

std::string HiveConfig::gcsScheme() const {
  return config_->get<std::string>(kGcsScheme, std::string("https"));
}

std::string HiveConfig::gcsCredentials() const {
  return config_->get<std::string>(kGcsCredentials, std::string(""));
}

std::optional<int> HiveConfig::gcsMaxRetryCount() const {
  return static_cast<std::optional<int>>(config_->get<int>(kGcsMaxRetryCount));
}

std::optional<std::string> HiveConfig::gcsMaxRetryTime() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kGcsMaxRetryTime));
}

bool HiveConfig::isOrcUseColumnNames(const config::ConfigBase* session) const {
  return session->get<bool>(
      kOrcUseColumnNamesSession, config_->get<bool>(kOrcUseColumnNames, false));
}

bool HiveConfig::isParquetUseColumnNames(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kParquetUseColumnNamesSession,
      config_->get<bool>(kParquetUseColumnNames, false));
}

bool HiveConfig::isFileColumnNamesReadAsLowerCase(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kFileColumnNamesReadAsLowerCaseSession,
      config_->get<bool>(kFileColumnNamesReadAsLowerCase, false));
}

bool HiveConfig::isPartitionPathAsLowerCase(
    const config::ConfigBase* session) const {
  return session->get<bool>(kPartitionPathAsLowerCaseSession, true);
}

int64_t HiveConfig::maxCoalescedBytes(const config::ConfigBase* session) const {
  return session->get<int64_t>(
      kMaxCoalescedBytesSession,
      config_->get<int64_t>(kMaxCoalescedBytes, 128 << 20)); // 128MB
}

bool HiveConfig::ignoreMissingFiles(const config::ConfigBase* session) const {
  return session->get<bool>(kIgnoreMissingFilesSession, false);
}

int32_t HiveConfig::maxCoalescedDistanceBytes(
    const config::ConfigBase* session) const {
  const auto distance = config::toCapacity(
      session->get<std::string>(
          kMaxCoalescedDistanceSession,
          config_->get<std::string>(kMaxCoalescedDistanceBytes, "512kB")),
      config::CapacityUnit::BYTE);
  BOLT_USER_CHECK_LE(
      distance,
      std::numeric_limits<int32_t>::max(),
      "The max merge distance to combine read requests must be less than 2GB."
      " Got {} bytes.",
      distance);
  return int32_t(distance);
}

int32_t HiveConfig::prefetchRowGroups() const {
  return config_->get<int32_t>(kPrefetchRowGroups, 1);
}

int32_t HiveConfig::loadQuantum() const {
  return config_->get<int32_t>(kLoadQuantum, 8 << 20);
}

int32_t HiveConfig::prefetchMemoryPercent() const {
  return config_->get<int32_t>(kPrefetchMemoryPercent, 30);
}

int32_t HiveConfig::numCacheFileHandles() const {
  return config_->get<int32_t>(kNumCacheFileHandles, 20'000);
}

bool HiveConfig::isFileHandleCacheEnabled() const {
  return config_->get<bool>(kEnableFileHandleCache, true);
}

uint64_t HiveConfig::orcWriterMaxStripeSize(
    const config::ConfigBase* session) const {
  return config::toCapacity(
      session->get<std::string>(
          kOrcWriterMaxStripeSizeSession,
          config_->get<std::string>(kOrcWriterMaxStripeSize, "64MB")),
      config::CapacityUnit::BYTE);
}

uint64_t HiveConfig::orcWriterMaxDictionaryMemory(
    const config::ConfigBase* session) const {
  return config::toCapacity(
      session->get<std::string>(
          kOrcWriterMaxDictionaryMemorySession,
          config_->get<std::string>(kOrcWriterMaxDictionaryMemory, "16MB")),
      config::CapacityUnit::BYTE);
}

std::string HiveConfig::writeFileCreateConfig() const {
  return config_->get<std::string>(kWriteFileCreateConfig, "");
}

uint32_t HiveConfig::sortWriterMaxOutputRows(
    const config::ConfigBase* session) const {
  if (session->valueExists(kSortWriterMaxOutputRowsSession)) {
    return session->get<uint32_t>(kSortWriterMaxOutputRowsSession).value();
  }
  return config_->get<int32_t>(kSortWriterMaxOutputRows, 1024);
}

uint64_t HiveConfig::sortWriterMaxOutputBytes(
    const config::ConfigBase* session) const {
  return config::toCapacity(
      session->get<std::string>(
          kSortWriterMaxOutputBytesSession,
          config_->get<std::string>(kSortWriterMaxOutputBytes, "10MB")),
      config::CapacityUnit::BYTE);
}

uint64_t HiveConfig::footerEstimatedSize() const {
  return config_->get<uint64_t>(kFooterEstimatedSize, 1UL << 20);
}

uint64_t HiveConfig::filePreloadThreshold() const {
  return config_->get<uint64_t>(kFilePreloadThreshold, 8UL << 20);
}

// static.
uint8_t HiveConfig::readTimestampUnit(const config::ConfigBase* session) const {
  const auto unit = session->get<uint8_t>(
      kReadTimestampUnitSession,
      config_->get<uint8_t>(kReadTimestampUnit, 3 /*milli*/));
  BOLT_CHECK(
      unit == 3 || unit == 6 /*micro*/ || unit == 9 /*nano*/,
      "Invalid timestamp unit.");
  return unit;
}

uint8_t HiveConfig::arrowBridgeTimestampUnit(
    const config::ConfigBase* session) const {
  return session->get<uint8_t>(kArrowBridgeTimestampUnit, 9 /* nano */);
}

// The ParquetDictionaryFilter reads DictionaryPage during query
// execution, which leads to read amplification in TPC-DS benchmark scenarios.
// It is disabled by default, and is only recommended to be enabled manually
// when the dictionary can provide a high filtering rate (i.e., the filter can
// significantly reduce the amount of data that needs to be processed
// subsequently)
bool HiveConfig::isDictionaryFilterEnabled() const {
  return config_->get<bool>(kParquetDictionaryFilterEnabled, false);
}

int32_t HiveConfig::decodeRepDefPageCount() const {
  auto value = config_->get<int32_t>(kParquetDecodeRepDefPageCount, 10);
  return std::max(value, 1);
}

} // namespace bytedance::bolt::connector::hive
