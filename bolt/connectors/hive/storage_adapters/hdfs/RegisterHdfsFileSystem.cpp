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

#ifdef BOLT_ENABLE_HDFS
#include "folly/concurrency/ConcurrentHashMap.h"

#include "bolt/common/config/Config.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h" // @manual
#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsUtil.h" // @manual
#include "bolt/dwio/common/FileSink.h"
#endif
namespace bytedance::bolt::filesystems {

#ifdef BOLT_ENABLE_HDFS

folly::ConcurrentHashMap<std::string, std::shared_ptr<HdfsFileSystem>>
    registeredFilesystems;

namespace {
std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
hdfsFileSystemGenerator() {
  static auto filesystemGenerator =
      [](const std::shared_ptr<const config::ConfigBase>& properties,
         std::string_view filePath) {
        HdfsServiceEndpoint endpoint =
            HdfsFileSystem::getServiceEndpoint(filePath, properties.get());
        std::string hdfsIdentity = endpoint.identity();

        static std::mutex mtx;
        {
          std::unique_lock<std::mutex> lk(mtx);
          if (registeredFilesystems.find(hdfsIdentity) ==
              registeredFilesystems.end()) {
            auto fileSystem =
                std::make_shared<HdfsFileSystem>(properties, endpoint);
            registeredFilesystems.insert(hdfsIdentity, fileSystem);
          }
          return registeredFilesystems[hdfsIdentity];
        }
      };
  return filesystemGenerator;
}
} // namespace

std::function<std::unique_ptr<bolt::dwio::common::FileSink>(
    const std::string&,
    const bolt::dwio::common::FileSink::Options& options)>
hdfsWriteFileSinkGenerator() {
  static auto hdfsWriteFileSink =
      [](const std::string& fileURI,
         const bolt::dwio::common::FileSink::Options& options) {
        if (HdfsFileSystem::isHdfsFile(fileURI)) {
          std::string pathSuffix =
              getHdfsPath(fileURI, HdfsFileSystem::kScheme);
          auto fileSystem =
              filesystems::getFileSystem(fileURI, options.connectorProperties);
          return std::make_unique<dwio::common::WriteFileSink>(
              fileSystem->openFileForWrite(pathSuffix),
              fileURI,
              options.metricLogger,
              options.stats);
        }
        return static_cast<std::unique_ptr<dwio::common::WriteFileSink>>(
            nullptr);
      };

  return hdfsWriteFileSink;
}
#endif

void registerHdfsFileSystem() {
#ifdef BOLT_ENABLE_HDFS
  registerFileSystem(HdfsFileSystem::isHdfsFile, hdfsFileSystemGenerator());
  dwio::common::FileSink::registerFactory(hdfsWriteFileSinkGenerator());
#endif
}

} // namespace bytedance::bolt::filesystems
