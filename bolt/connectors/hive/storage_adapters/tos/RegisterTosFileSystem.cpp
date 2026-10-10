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
 * 2026-09-20.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/connectors/hive/storage_adapters/tos/RegisterTosFileSystem.h"

#ifdef BOLT_ENABLE_TOS
#include <mutex>
#include "bolt/connectors/hive/storage_adapters/tos/TosFileSystem.h"
#include "bolt/core/Config.h"
#include "bolt/dwio/common/FileSink.h"

namespace bytedance::bolt::filesystems {
namespace {

TosFileSystemCache& tosFileSystems() {
  static TosFileSystemCache instances(
      folly::in_place, kTosFileSystemCacheCapacity);
  return instances;
}

std::shared_ptr<FileSystem> getOrCreateTosFileSystem(
    std::shared_ptr<const config::ConfigBase> properties,
    std::string_view filePath,
    const FileOptions& fileOptions) {
  auto [bucket, object] = TosFileSystem::parsePath(filePath);
  return getOrCreateCachedTosFileSystem(
      tosFileSystems(),
      std::move(properties),
      bucket,
      fileOptions,
      [](std::shared_ptr<const config::ConfigBase> resolvedProperties) {
        return std::make_shared<TosFileSystem>(std::move(resolvedProperties));
      });
}
} // namespace

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
tosFileSystemGenerator() {
  static auto filesystemGenerator =
      [](std::shared_ptr<const config::ConfigBase> properties,
         std::string_view filePath) {
        return getOrCreateTosFileSystem(
            std::move(properties), filePath, FileOptions{});
      };
  return filesystemGenerator;
}

std::function<std::unique_ptr<bolt::dwio::common::FileSink>(
    const std::string&,
    const bolt::dwio::common::FileSink::Options& options)>
tosWriteFileSinkGenerator() {
  static auto tosWriteFileSink =
      [](const std::string& fileURI,
         const bolt::dwio::common::FileSink::Options& options) {
        if (TosFileSystem::isTosFile(fileURI)) {
          auto fileOptions = options.fileOptions ? *options.fileOptions
                                                 : filesystems::FileOptions{};
          auto fileSystem = getOrCreateTosFileSystem(
              options.connectorProperties, fileURI, fileOptions);
          return std::make_unique<dwio::common::WriteFileSink>(
              fileSystem->openFileForWrite(fileURI, fileOptions),
              fileURI,
              options.metricLogger,
              options.stats);
        }
        return static_cast<std::unique_ptr<dwio::common::WriteFileSink>>(
            nullptr);
      };

  return tosWriteFileSink;
}

} // namespace bytedance::bolt::filesystems
#endif

namespace bytedance::bolt::filesystems {

void registerTosFileSystem() {
#ifdef BOLT_ENABLE_TOS
  static std::once_flag registered;
  std::call_once(registered, [] {
    registerFileSystem(TosFileSystem::isTosFile, tosFileSystemGenerator());
    dwio::common::FileSink::registerFactory(tosWriteFileSinkGenerator());
  });
#endif
}

} // namespace bytedance::bolt::filesystems
