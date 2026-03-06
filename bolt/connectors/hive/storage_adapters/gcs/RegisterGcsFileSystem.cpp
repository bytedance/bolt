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

#include "bolt/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h"

#ifdef BOLT_ENABLE_GCS
#include "bolt/common/config/Config.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/storage_adapters/gcs/GcsFileSystem.h" // @manual
#include "bolt/connectors/hive/storage_adapters/gcs/GcsUtil.h" // @manual
#include "bolt/dwio/common/FileSink.h"
#endif

namespace bytedance::bolt::filesystems {

#ifdef BOLT_ENABLE_GCS

using FileSystemMap = folly::Synchronized<
    std::unordered_map<std::string, std::shared_ptr<FileSystem>>>;

/// Multiple GCS filesystems are supported.
FileSystemMap& gcsFileSystems() {
  static FileSystemMap instances;
  return instances;
}

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
gcsFileSystemGenerator() {
  static auto filesystemGenerator =
      [](std::shared_ptr<const config::ConfigBase> properties,
         std::string_view filePath) {
        const auto file = gcsPath(filePath);
        std::string bucket;
        std::string object;
        setBucketAndKeyFromGcsPath(file, bucket, object);
        auto cacheKey = fmt::format(
            "{}-{}",
            properties->get<std::string>(
                connector::hive::HiveConfig::kGcsEndpoint,
                kGcsDefaultCacheKeyPrefix),
            bucket);

        // Check if an instance exists with a read lock (shared).
        auto fs = gcsFileSystems().withRLock(
            [&](auto& instanceMap) -> std::shared_ptr<FileSystem> {
              auto iterator = instanceMap.find(cacheKey);
              if (iterator != instanceMap.end()) {
                return iterator->second;
              }
              return nullptr;
            });
        if (fs != nullptr) {
          return fs;
        }

        return gcsFileSystems().withWLock(
            [&](auto& instanceMap) -> std::shared_ptr<FileSystem> {
              // Repeat the checks with a write lock.
              auto iterator = instanceMap.find(cacheKey);
              if (iterator != instanceMap.end()) {
                return iterator->second;
              }

              std::shared_ptr<GcsFileSystem> fs;
              if (properties != nullptr) {
                fs = std::make_shared<GcsFileSystem>(bucket, properties);
              } else {
                fs = std::make_shared<GcsFileSystem>(
                    bucket,
                    std::make_shared<config::ConfigBase>(
                        std::unordered_map<std::string, std::string>()));
              }
              fs->initializeClient();

              instanceMap.insert({cacheKey, fs});
              return fs;
            });
      };
  return filesystemGenerator;
}

std::unique_ptr<bolt::dwio::common::FileSink> gcsWriteFileSinkGenerator(
    const std::string& fileURI,
    const bolt::dwio::common::FileSink::Options& options) {
  if (isGcsFile(fileURI)) {
    auto fileSystem =
        filesystems::getFileSystem(fileURI, options.connectorProperties);
    return std::make_unique<dwio::common::WriteFileSink>(
        fileSystem->openFileForWrite(
            fileURI, filesystems::FileOptions{.pool = options.pool}),
        fileURI,
        options.metricLogger,
        options.stats);
  }
  return nullptr;
}
#endif

void registerGcsFileSystem() {
#ifdef BOLT_ENABLE_GCS
  registerFileSystem(isGcsFile, gcsFileSystemGenerator());
  dwio::common::FileSink::registerFactory(
      std::function(gcsWriteFileSinkGenerator));
#endif
}

void registerGcsOAuthCredentialsProvider(
    const std::string& providerName,
    const GcsOAuthCredentialsProviderFactory& factory) {
#ifdef BOLT_ENABLE_GCS
  registerOAuthCredentialsProvider(providerName, factory);
#endif
}

} // namespace bytedance::bolt::filesystems
