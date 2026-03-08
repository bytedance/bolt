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

#pragma once

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include "bolt/common/config/Config.h"
namespace bytedance::bolt::config {
class ConfigBase;
}
namespace bytedance::bolt::connector::hive {

/// Hive connector configs.
class HiveConfig {
 public:
  enum class InsertExistingPartitionsBehavior {
    kError,
    kOverwrite,
  };

  static std::string insertExistingPartitionsBehaviorString(
      InsertExistingPartitionsBehavior behavior);

  /// Behavior on insert into existing partitions.
  static constexpr const char* kInsertExistingPartitionsBehaviorSession =
      "insert_existing_partitions_behavior";
  static constexpr const char* kInsertExistingPartitionsBehavior =
      "insert-existing-partitions-behavior";

  /// Maximum number of (bucketed) partitions per a single table writer
  /// instance.
  // TODO: remove hive_orc_use_column_names since it doesn't exist in presto,
  // right now this is only used for testing.
  static constexpr const char* kMaxPartitionsPerWriters =
      "max-partitions-per-writers";
  static constexpr const char* kMaxPartitionsPerWritersSession =
      "max_partitions_per_writers";

  /// Whether new data can be inserted into an unpartition table.
  /// Bolt currently does not support appending data to existing partitions.
  static constexpr const char* kImmutablePartitions =
      "hive.immutable-partitions";

  /// Virtual addressing is used for AWS S3 and is the default
  /// (path-style-access is false). Path access style is used for some on-prem
  /// systems like Minio.
  static constexpr const char* kS3PathStyleAccess = "hive.s3.path-style-access";

  /// Log granularity of AWS C++ SDK.
  static constexpr const char* kS3LogLevel = "hive.s3.log-level";

  /// Use HTTPS to communicate with the S3 API.
  static constexpr const char* kS3SSLEnabled = "hive.s3.ssl.enabled";

  /// Use the EC2 metadata service to retrieve API credentials.
  static constexpr const char* kS3UseInstanceCredentials =
      "hive.s3.use-instance-credentials";

  /// The S3 storage endpoint server. This can be used to connect to an
  /// S3-compatible storage system instead of AWS.
  static constexpr const char* kS3Endpoint = "hive.s3.endpoint";

  /// Default AWS access key to use.
  static constexpr const char* kS3AwsAccessKey = "hive.s3.aws-access-key";

  /// Default AWS secret key to use.
  static constexpr const char* kS3AwsSecretKey = "hive.s3.aws-secret-key";

  /// IAM role to assume.
  static constexpr const char* kS3IamRole = "hive.s3.iam-role";

  /// Session name associated with the IAM role.
  static constexpr const char* kS3IamRoleSessionName =
      "hive.s3.iam-role-session-name";

  /// The GCS storage endpoint server.
  static constexpr const char* kGcsEndpoint = "hive.gcs.endpoint";

  /// The GCS storage scheme, https for default credentials.
  static constexpr const char* kGcsScheme = "hive.gcs.scheme";

  /// The GCS service account configuration as json string
  static constexpr const char* kGcsCredentials = "hive.gcs.credentials";

  /// The GCS maximum retry counter of transient errors.
  static constexpr const char* kGcsMaxRetryCount = "hive.gcs.max-retry-count";

  /// The GCS maximum time allowed to retry transient errors.
  static constexpr const char* kGcsMaxRetryTime = "hive.gcs.max-retry-time";

  /// Maps table field names to file field names using names, not indices.
  // TODO: remove hive_orc_use_column_names since it doesn't exist in presto,
  // right now this is only used for testing.
  static constexpr const char* kOrcUseColumnNames = "hive.orc.use-column-names";
  static constexpr const char* kOrcUseColumnNamesSession =
      "hive_orc_use_column_names";

  static constexpr const char* kParquetUseColumnNames =
      "hive.parquet.use-column-names";
  static constexpr const char* kParquetUseColumnNamesSession =
      "hive_parquet_use_column_names";

  /// Reads the source file column name as lower case.
  static constexpr const char* kFileColumnNamesReadAsLowerCase =
      "file-column-names-read-as-lower-case";
  static constexpr const char* kFileColumnNamesReadAsLowerCaseSession =
      "file_column_names_read_as_lower_case";

  /// Maps nested field using names. And map top-level field using indices.
  static constexpr const char* kOrcUseNestedColumnNames =
      "hive.orc.use-nested-column-names";
  static constexpr const char* kOrcUseNestedColumnNamesSession =
      "hive.orc.use-nested-column-names";

  static constexpr const char* kPartitionPathAsLowerCaseSession =
      "partition_path_as_lower_case";

  static constexpr const char* kIgnoreMissingFilesSession =
      "ignore_missing_files";

  /// The max coalesce bytes for a request.
  static constexpr const char* kMaxCoalescedBytes = "max-coalesced-bytes";
  static constexpr const char* kMaxCoalescedBytesSession =
      "max-coalesced-bytes";

  /// The max merge distance to combine read requests.
  /// Note: The session property name differs from the constant name for
  /// backward compatibility with Presto.
  static constexpr const char* kMaxCoalescedDistanceBytes =
      "max-coalesced-distance-bytes";
  static constexpr const char* kMaxCoalescedDistance = "max-coalesced-distance";
  static constexpr const char* kMaxCoalescedDistanceSession =
      "orc_max_merge_distance";

  /// The number of prefetch rowgroups
  static constexpr const char* kPrefetchRowGroups = "prefetch-rowgroups";

  /// The preload memory percent for a request.
  static constexpr const char* kPrefetchMemoryPercent =
      "prefetch-memory-percent";

  /// The total size in bytes for a direct coalesce request.
  static constexpr const char* kLoadQuantum = "load-quantum";

  /// Maximum number of entries in the file handle cache.
  static constexpr const char* kNumCacheFileHandles = "num_cached_file_handles";

  /// Enable file handle cache.
  static constexpr const char* kEnableFileHandleCache =
      "file-handle-cache-enabled";

  /// The size in bytes to be fetched with Meta data together, used when the
  /// data after meta data will be used later. Optimization to decrease small IO
  /// request
  static constexpr const char* kFooterEstimatedSize = "footer-estimated-size";

  /// The threshold of file size in bytes when the whole file is fetched with
  /// meta data together. Optimization to decrease the small IO requests
  static constexpr const char* kFilePreloadThreshold = "file-preload-threshold";

  /// Maximum stripe size in orc writer.
  static constexpr const char* kOrcWriterMaxStripeSize =
      "hive.orc.writer.stripe-max-size";
  static constexpr const char* kOrcWriterMaxStripeSizeSession =
      "orc_optimized_writer_max_stripe_size";

  /// Maximum dictionary memory that can be used in orc writer.
  static constexpr const char* kOrcWriterMaxDictionaryMemory =
      "hive.orc.writer.dictionary-max-memory";
  static constexpr const char* kOrcWriterMaxDictionaryMemorySession =
      "orc_optimized_writer_max_dictionary_memory";

  /// Config used to create write files. This config is provided to underlying
  /// file system through hive connector and data sink. The config is free form.
  /// The form should be defined by the underlying file system.
  static constexpr const char* kWriteFileCreateConfig =
      "hive.write_file_create_config";

  /// Maximum number of rows for sort writer in one batch of output.
  static constexpr const char* kSortWriterMaxOutputRows =
      "sort-writer-max-output-rows";
  static constexpr const char* kSortWriterMaxOutputRowsSession =
      "sort_writer_max_output_rows";

  /// Maximum bytes for sort writer in one batch of output.
  static constexpr const char* kSortWriterMaxOutputBytes =
      "sort-writer-max-output-bytes";
  static constexpr const char* kSortWriterMaxOutputBytesSession =
      "sort_writer_max_output_bytes";

  // Timestamp unit used during Bolt-Arrow conversion.
  static constexpr const char* kArrowBridgeTimestampUnit =
      "arrow_bridge_timestamp_unit";

  /// Configuration flag to enable or disable Parquet dictionary filtering.
  /// When enabled (default), the reader will use dictionary filtering to skip
  /// row groups that don't match filter conditions, which can improve query
  /// performance. When disabled, dictionary filtering will be bypassed.
  static constexpr const char* kParquetDictionaryFilterEnabled =
      "parquet.dictionary_filter.enabled";

  static constexpr const char* kParquetDecodeRepDefPageCount =
      "parquet_decode_repdef_page_count";

  static const std::set<std::string> hms_session_key;

  InsertExistingPartitionsBehavior insertExistingPartitionsBehavior(
      const config::ConfigBase* session) const;

  uint32_t maxPartitionsPerWriters(const config::ConfigBase* session) const;

  bool immutablePartitions() const;

  bool s3UseVirtualAddressing() const;

  std::string s3GetLogLevel() const;

  bool s3UseSSL() const;

  bool s3UseInstanceCredentials() const;

  std::string s3Endpoint() const;

  std::optional<std::string> s3AccessKey() const;

  std::optional<std::string> s3SecretKey() const;

  std::optional<std::string> s3IAMRole() const;

  std::string s3IAMRoleSessionName() const;

  std::string gcsEndpoint() const;

  std::string gcsScheme() const;

  std::string gcsCredentials() const;

  std::optional<int> gcsMaxRetryCount() const;

  std::optional<std::string> gcsMaxRetryTime() const;

  bool isOrcUseColumnNames(const config::ConfigBase* session) const;
  bool isParquetUseColumnNames(const config::ConfigBase* session) const;

  bool isOrcUseNestedColumnNames(const config::ConfigBase* session) const;

  bool isFileColumnNamesReadAsLowerCase(
      const config::ConfigBase* session) const;

  bool isPartitionPathAsLowerCase(const config::ConfigBase* session) const;
  int64_t maxCoalescedBytes(const config::ConfigBase* session) const;

  bool ignoreMissingFiles(const config::ConfigBase* session) const;

  int32_t maxCoalescedDistanceBytes(const config::ConfigBase* session) const;

  int32_t prefetchRowGroups() const;

  int32_t loadQuantum() const;

  int32_t prefetchMemoryPercent() const;

  int32_t numCacheFileHandles() const;

  bool isFileHandleCacheEnabled() const;

  uint64_t fileWriterFlushThresholdBytes() const;

  uint64_t orcWriterMaxStripeSize(const config::ConfigBase* session) const;

  uint64_t orcWriterMaxDictionaryMemory(
      const config::ConfigBase* session) const;

  std::string writeFileCreateConfig() const;

  uint32_t sortWriterMaxOutputRows(const config::ConfigBase* session) const;

  uint64_t sortWriterMaxOutputBytes(const config::ConfigBase* session) const;

  /// Returns whether dictionary filtering is enabled for the given session.
  /// If not explicitly set in the session, falls back to the global config.
  /// @param session The configuration object to check
  /// @return true if dictionary filtering should be used, false otherwise
  bool isDictionaryFilterEnabled() const;

  // For array/map types, if all decoded reps/defs levels of a single column
  // chunk cannot fit in memory, they need to be decoded in every N pages This
  // config determines the number of pages decoded for the first time. Setting
  // it to a large value to disable the batch decoding function.
  int32_t decodeRepDefPageCount() const;

  // The unit for reading timestamps from files.
  static constexpr const char* kReadTimestampUnit =
      "hive.reader.timestamp-unit";
  static constexpr const char* kReadTimestampUnitSession =
      "hive.reader.timestamp_unit";

  uint64_t footerEstimatedSize() const;

  uint64_t filePreloadThreshold() const;

  // Returns the timestamp unit used when reading timestamps from files.
  uint8_t readTimestampUnit(const config::ConfigBase* session) const;

  /// Returns the timestamp unit used in Bolt-Arrow conversion.
  /// 0: second, 3: milli, 6: micro, 9: nano.
  uint8_t arrowBridgeTimestampUnit(const config::ConfigBase* session) const;

  HiveConfig(std::shared_ptr<const config::ConfigBase> config) {
    BOLT_CHECK_NOT_NULL(config, "Config is null for HiveConfig initialization");
    config_ = std::move(config);
    // TODO: add sanity check
  }

  const std::shared_ptr<const config::ConfigBase>& config() const {
    return config_;
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};

} // namespace bytedance::bolt::connector::hive
