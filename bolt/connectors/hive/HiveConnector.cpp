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

#include "bolt/connectors/hive/HiveConnector.h"

#include "bolt/common/base/Fs.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveDataSink.h"
#include "bolt/connectors/hive/HiveDataSource.h"
#include "bolt/connectors/hive/HivePartitionFunction.h"
// Meta's buck build system needs this check.
#ifdef BOLT_ENABLE_GCS
#include "bolt/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h" // @manual
#endif
#ifdef BOLT_ENABLE_HDFS
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h" // @manual
#endif
#ifdef BOLT_ENABLE_S3
#include "bolt/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h" // @manual
#endif
#ifdef BOLT_ENABLE_ABFS
#include "bolt/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h" // @manual
#endif
#include "bolt/dwio/dwrf/reader/DwrfReader.h"
#include "bolt/dwio/dwrf/writer/Writer.h"
// Meta's buck build system needs this check.
#ifdef BOLT_ENABLE_PARQUET
#include "bolt/dwio/parquet/RegisterParquetReader.h" // @manual
#include "bolt/dwio/parquet/RegisterParquetWriter.h" // @manual
#endif
#ifdef BOLT_ENABLE_ORC
#include "bolt/dwio/orc/reader/RegisterOrcReader.h" // @manual
#include "bolt/dwio/orc/writer/RegisterOrcWriter.h" // @manual
#endif
#ifdef BOLT_ENABLE_TXT
#include "bolt/dwio/txt/reader/RegisterTxtReader.h"
#include "bolt/dwio/txt/writer/RegisterTxtWriter.h"
#endif
#include "bolt/expression/FieldReference.h"

#include <boost/lexical_cast.hpp>
#include <memory>
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::dwrf;
namespace bytedance::bolt::connector::hive {

HiveConnector::HiveConnector(
    const std::string& id,
    std::shared_ptr<const config::ConfigBase> config,
    folly::Executor* executor)
    : Connector(id),
      hiveConfig_(std::make_shared<HiveConfig>(config)),
      fileHandleFactory_(
          hiveConfig_->isFileHandleCacheEnabled()
              ? std::make_unique<
                    SimpleLRUCache<std::string, std::shared_ptr<FileHandle>>>(
                    hiveConfig_->numCacheFileHandles())
              : nullptr,
          std::make_unique<FileHandleGenerator>(config)),
      executor_(executor) {
  if (hiveConfig_->isFileHandleCacheEnabled()) {
    LOG(INFO) << "Hive connector " << connectorId()
              << " created with maximum of "
              << hiveConfig_->numCacheFileHandles() << " cached file handles.";
  } else {
    LOG(INFO) << "Hive connector " << connectorId()
              << " created with file handle cache disabled"
              << (executor_ == nullptr ? " with nullptr executor" : "");
  }
}

std::unique_ptr<DataSource> HiveConnector::createDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
    const core::QueryConfig& queryConfig) {
  return std::make_unique<HiveDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      &fileHandleFactory_,
      queryConfig,
      executor_,
      connectorQueryCtx,
      hiveConfig_);
}

std::unique_ptr<DataSink> HiveConnector::createDataSink(
    RowTypePtr inputType,
    std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const core::QueryConfig& queryConfig) {
  auto hiveInsertHandle = std::dynamic_pointer_cast<HiveInsertTableHandle>(
      connectorInsertTableHandle);
  BOLT_CHECK_NOT_NULL(
      hiveInsertHandle, "Hive connector expecting hive write handle!");
  return std::make_unique<HiveDataSink>(
      inputType,
      hiveInsertHandle,
      connectorQueryCtx,
      commitStrategy,
      hiveConfig_,
      queryConfig);
}

std::unique_ptr<core::PartitionFunction> HivePartitionFunctionSpec::create(
    int numPartitions) const {
  std::vector<int> bucketToPartitions;
  if (bucketToPartition_.empty()) {
    // NOTE: if hive partition function spec doesn't specify bucket to partition
    // mapping, then we do round-robin mapping based on the actual number of
    // partitions.
    bucketToPartitions.resize(numBuckets_);
    for (int bucket = 0; bucket < numBuckets_; ++bucket) {
      bucketToPartitions[bucket] = bucket % numPartitions;
    }
  }
  return std::make_unique<bolt::connector::hive::HivePartitionFunction>(
      numBuckets_,
      bucketToPartition_.empty() ? std::move(bucketToPartitions)
                                 : bucketToPartition_,
      channels_,
      constValues_);
}

void HiveConnectorFactory::initialize() {
  static bool once = []() {
    dwio::common::registerFileSinks();
    dwrf::registerDwrfReaderFactory();
    dwrf::registerDwrfWriterFactory();
// Meta's buck build system needs this check.
#ifdef BOLT_ENABLE_PARQUET
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
#endif
#ifdef BOLT_ENABLE_ORC
    orc::registerOrcReaderFactory();
    orc::registerOrcWriterFactory();
#endif
#ifdef BOLT_ENABLE_TXT
    txt::registerTxtReaderFactory();
    txt::registerTxtWriterFactory();
#endif
// Meta's buck build system needs this check.
#ifdef BOLT_ENABLE_S3
    filesystems::registerS3FileSystem();
#endif
#ifdef BOLT_ENABLE_HDFS
    filesystems::registerHdfsFileSystem();
#endif
#ifdef BOLT_ENABLE_GCS
    filesystems::registerGcsFileSystem();
#endif
#ifdef BOLT_ENABLE_ABFS
    filesystems::registerAbfsFileSystem();
#endif
    return true;
  }();
}

std::string HivePartitionFunctionSpec::toString() const {
  std::ostringstream keys;
  size_t constIndex = 0;
  for (auto i = 0; i < channels_.size(); ++i) {
    if (i > 0) {
      keys << ", ";
    }
    auto channel = channels_[i];
    if (channel == kConstantChannel) {
      keys << "\"" << constValues_[constIndex++]->toString(0) << "\"";
    } else {
      keys << channel;
    }
  }

  return fmt::format("HIVE(({}) buckets: {})", keys.str(), numBuckets_);
}

folly::dynamic HivePartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HivePartitionFunctionSpec";
  obj["numBuckets"] = ISerializable::serialize(numBuckets_);
  obj["bucketToPartition"] = ISerializable::serialize(bucketToPartition_);
  obj["keys"] = ISerializable::serialize(channels_);
  std::vector<bolt::core::ConstantTypedExpr> constValueExprs;
  constValueExprs.reserve(constValues_.size());
  for (const auto& value : constValues_) {
    constValueExprs.emplace_back(value);
  }
  obj["constants"] = ISerializable::serialize(constValueExprs);
  return obj;
}

// static
core::PartitionFunctionSpecPtr HivePartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* context) {
  std::vector<column_index_t> channels =
      ISerializable::deserialize<std::vector<column_index_t>>(
          obj["keys"], context);
  const auto constTypedValues =
      ISerializable::deserialize<std::vector<bolt::core::ConstantTypedExpr>>(
          obj["constants"], context);
  std::vector<VectorPtr> constValues;
  constValues.reserve(constTypedValues.size());
  auto* pool = static_cast<memory::MemoryPool*>(context);
  for (const auto& value : constTypedValues) {
    constValues.emplace_back(value->toConstantVector(pool));
  }
  return std::make_shared<HivePartitionFunctionSpec>(
      ISerializable::deserialize<int>(obj["numBuckets"], context),
      ISerializable::deserialize<std::vector<int>>(
          obj["bucketToPartition"], context),
      std::move(channels),
      std::move(constValues));
}

void registerHivePartitionFunctionSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register(
      "HivePartitionFunctionSpec", HivePartitionFunctionSpec::deserialize);
}

namespace {
static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) =
    CheckHiveConnectorFactoryInit<HiveConnectorFactory>();

static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory2) =
    CheckHiveConnectorFactoryInit<HiveHadoop2ConnectorFactory>();

static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory4) =
    CheckHiveConnectorFactoryInit<TosConnectorFactory>();
} // namespace

} // namespace bytedance::bolt::connector::hive
