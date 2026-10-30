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

#pragma once

#include <optional>
#include <string>
#include <string_view>

#include <folly/Synchronized.h>

#include "bolt/common/caching/SimpleLRUCache.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosCredentialsFile.h"

namespace bytedance::bolt::filesystems {

struct _TosSessionConfig__isset {
  _TosSessionConfig__isset()
      : endpoint(false),
        region(false),
        bucket(false),
        ak(false),
        sk(false),
        st(false) {}
  bool endpoint : 1;
  bool region : 1;
  bool bucket : 1;
  bool ak : 1;
  bool sk : 1;
  bool st : 1;
};

struct TosSessionConfig {
  std::string endpoint;
  std::string region;
  std::string bucket;
  std::string ak; // access key
  std::string sk; // secret key
  std::string st; // session token
  _TosSessionConfig__isset __isset;

  void __set_endpoint(const std::string& val);
  void __set_region(const std::string_view& val);
  void __set_bucket(const std::string_view& val);
  void __set_ak(const std::string& val);
  void __set_sk(const std::string& val);
  void __set_st(const std::string& val);
  std::string toString() const;
};

struct TosCredentialsFileSelection {
  std::optional<TosCredentials> credentials;
};

constexpr size_t kTosFileSystemCacheCapacity = 100;

using TosFileSystemCache = folly::Synchronized<
    SimpleLRUCache<std::string, std::shared_ptr<FileSystem>>>;

using TosFileSystemFactory = std::function<std::shared_ptr<FileSystem>(
    std::shared_ptr<const config::ConfigBase>)>;

TosCredentialsFileSelection resolveTosCredentialsFromFile(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config);

std::string getTosFileSystemCacheKey(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config);

std::shared_ptr<FileSystem> getOrCreateCachedTosFileSystem(
    TosFileSystemCache& fileSystems,
    std::shared_ptr<const config::ConfigBase> properties,
    std::string_view bucket,
    const FileOptions& fileOptions,
    const TosFileSystemFactory& factory);

TosSessionConfig getTosSessionConfig(
    const std::string_view& bucket,
    const std::string& endpoint,
    const FileOptions& fileOptions,
    const config::ConfigBase* config);

} // namespace bytedance::bolt::filesystems
