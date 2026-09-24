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

#include "bolt/connectors/hive/storage_adapters/tos/TosConfig.h"

#include <array>
#include <cstring>
#include <optional>
#include <sstream>
#include <unordered_map>

#include "bolt/common/config/Config.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosCredentialsFile.h"

namespace bytedance::bolt::filesystems {
namespace {
constexpr const char* kTosPrefix = "tos.";
constexpr const char* kTosBucketPrefix = "tos.bucket.";
constexpr const char* kFsTosPrefix = "fs.tos.";
constexpr const char* kFsTosBucketPrefix = "fs.tos.bucket.";
constexpr const char* kBoltTosEndpoint = "bolt.tos.endpoint";
constexpr const char* kHiveTosEndpoint = "hive.tos.endpoint";

enum class TosConfigKey {
  kEndpoint,
  kRegion,
  kCredentialsFilePath,
  kAccessKey,
  kSecretKey,
  kSessionToken,
};

const std::unordered_map<TosConfigKey, std::string_view>& tosConfigTraits() {
  static const std::unordered_map<TosConfigKey, std::string_view> config = {
      {TosConfigKey::kEndpoint, "endpoint"},
      {TosConfigKey::kRegion, "region"},
      {TosConfigKey::kCredentialsFilePath, "credentials.file.path"},
      {TosConfigKey::kAccessKey, "access.key"},
      {TosConfigKey::kSecretKey, "secret.key"},
      {TosConfigKey::kSessionToken, "session.token"},
  };
  return config;
}

std::string tosBaseConfigKey(TosConfigKey key, std::string_view prefix) {
  std::stringstream buffer;
  buffer << prefix << tosConfigTraits().find(key)->second;
  return buffer.str();
}

std::string tosBucketConfigKey(
    TosConfigKey key,
    std::string_view bucket,
    std::string_view bucketPrefix) {
  std::stringstream buffer;
  buffer << bucketPrefix << bucket << "."
         << tosConfigTraits().find(key)->second;
  return buffer.str();
}

std::optional<std::string> getFileOption(
    const FileOptions& fileOptions,
    const std::string& key) {
  auto it = fileOptions.values.find(key);
  if (it != fileOptions.values.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<std::string> getConfigValue(
    const config::ConfigBase* config,
    const std::string& key) {
  if (config == nullptr) {
    return std::nullopt;
  }
  auto value = config->get<std::string>(key);
  if (value.has_value()) {
    return value.value();
  }
  return std::nullopt;
}

std::shared_ptr<const config::ConfigBase> ensureProperties(
    std::shared_ptr<const config::ConfigBase> properties) {
  if (properties != nullptr) {
    return properties;
  }
  static const auto emptyProperties = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{}, false);
  return emptyProperties;
}

std::optional<std::string> getTosFileOptionValue(
    std::string_view bucket,
    const FileOptions& fileOptions,
    TosConfigKey key) {
  const std::array<std::string, 2> bucketKeys = {
      tosBucketConfigKey(key, bucket, kTosBucketPrefix),
      tosBucketConfigKey(key, bucket, kFsTosBucketPrefix)};
  const std::array<std::string, 2> baseKeys = {
      tosBaseConfigKey(key, kTosPrefix), tosBaseConfigKey(key, kFsTosPrefix)};

  for (const auto& key : bucketKeys) {
    if (auto value = getFileOption(fileOptions, key)) {
      return value;
    }
  }
  for (const auto& key : baseKeys) {
    if (auto value = getFileOption(fileOptions, key)) {
      return value;
    }
  }
  return std::nullopt;
}

std::optional<std::string> getTosConfigValue(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config,
    TosConfigKey key) {
  if (auto value = getTosFileOptionValue(bucket, fileOptions, key)) {
    return value;
  }

  const std::array<std::string, 2> bucketKeys = {
      tosBucketConfigKey(key, bucket, kTosBucketPrefix),
      tosBucketConfigKey(key, bucket, kFsTosBucketPrefix)};
  const std::array<std::string, 2> baseKeys = {
      tosBaseConfigKey(key, kTosPrefix), tosBaseConfigKey(key, kFsTosPrefix)};

  for (const auto& key : bucketKeys) {
    if (auto value = getConfigValue(config, key)) {
      return value;
    }
  }
  for (const auto& key : baseKeys) {
    if (auto value = getConfigValue(config, key)) {
      return value;
    }
  }
  return std::nullopt;
}

std::optional<std::string> getTosEndpointValue(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config) {
  if (auto value = getTosConfigValue(
          bucket, fileOptions, config, TosConfigKey::kEndpoint)) {
    return value;
  }
  if (auto value = getFileOption(fileOptions, kBoltTosEndpoint)) {
    return value;
  }
  if (auto value = getConfigValue(config, kBoltTosEndpoint)) {
    return value;
  }
  return getConfigValue(config, kHiveTosEndpoint);
}

void setCredentialsFromTosCredentials(
    const TosCredentials& credentials,
    TosSessionConfig& sessionConfig) {
  if (credentials.accessKey.has_value()) {
    sessionConfig.__set_ak(credentials.accessKey.value());
  }
  if (credentials.secretKey.has_value()) {
    sessionConfig.__set_sk(credentials.secretKey.value());
  }
  if (credentials.sessionToken.has_value()) {
    sessionConfig.__set_st(credentials.sessionToken.value());
  }
}

bool trySetCredentialsFromFile(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config,
    TosSessionConfig& sessionConfig) {
  auto credentialsPath = getTosConfigValue(
      bucket, fileOptions, config, TosConfigKey::kCredentialsFilePath);
  if (!credentialsPath.has_value()) {
    return false;
  }

  auto credentials = readTosCredentialsFile(credentialsPath.value(), bucket);
  BOLT_CHECK(
      credentials.has_value(),
      "Can not found TOS credentials from tos.credentials.file.path {}",
      credentialsPath.value());

  setCredentialsFromTosCredentials(credentials.value(), sessionConfig);
  return true;
}

bool trySetCredentialsFromFileOptions(
    std::string_view bucket,
    const FileOptions& fileOptions,
    TosSessionConfig& sessionConfig) {
  auto credentialsPath = getTosFileOptionValue(
      bucket, fileOptions, TosConfigKey::kCredentialsFilePath);
  if (!credentialsPath.has_value()) {
    return false;
  }

  auto credentials = readTosCredentialsFile(credentialsPath.value(), bucket);
  BOLT_CHECK(
      credentials.has_value(),
      "Can not found TOS credentials from tos.credentials.file.path {}",
      credentialsPath.value());

  setCredentialsFromTosCredentials(credentials.value(), sessionConfig);
  return true;
}

bool trySetSecretKeyCredentials(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config,
    TosSessionConfig& sessionConfig) {
  auto accessKey =
      getTosConfigValue(bucket, fileOptions, config, TosConfigKey::kAccessKey);
  auto secretKey =
      getTosConfigValue(bucket, fileOptions, config, TosConfigKey::kSecretKey);
  auto sessionToken = getTosConfigValue(
      bucket, fileOptions, config, TosConfigKey::kSessionToken);
  if (!accessKey && !secretKey && !sessionToken) {
    return false;
  }
  BOLT_USER_CHECK(
      accessKey && !accessKey->empty() && secretKey && !secretKey->empty(),
      "Invalid TOS configuration: both tos.access.key and tos.secret.key must be non-empty");
  sessionConfig.__set_ak(*accessKey);
  sessionConfig.__set_sk(*secretKey);
  if (sessionToken) {
    sessionConfig.__set_st(*sessionToken);
  }

  return true;
}

void appendCacheKeyPart(
    std::stringstream& buffer,
    std::string_view key,
    const std::optional<std::string>& value) {
  buffer << key << "=";
  if (value.has_value()) {
    buffer << value.value();
  }
  buffer << ";";
}
} // namespace

void TosSessionConfig::__set_endpoint(const std::string& val) {
  static const char* header = "tos-";
  static const int32_t headerSize = strlen(header);
  endpoint = val;
  __isset.endpoint = true;
  auto pos1 = val.find(header);
  auto pos2 = val.find('.');
  if (pos1 != std::string_view::npos && pos2 != std::string_view::npos &&
      pos2 > pos1 + headerSize) {
    __set_region(val.substr(pos1 + headerSize, pos2 - pos1 - headerSize));
  }
}

void TosSessionConfig::__set_region(const std::string_view& val) {
  region = val;
  __isset.region = true;
}

void TosSessionConfig::__set_bucket(const std::string_view& val) {
  bucket = val;
  __isset.bucket = true;
}

void TosSessionConfig::__set_ak(const std::string& val) {
  ak = val;
  __isset.ak = true;
}

void TosSessionConfig::__set_sk(const std::string& val) {
  sk = val;
  __isset.sk = true;
}

void TosSessionConfig::__set_st(const std::string& val) {
  st = val;
  __isset.st = true;
}

std::string TosSessionConfig::toString() const {
  std::stringstream ss;
  if (__isset.endpoint)
    ss << "endpoint=" << endpoint;
  if (__isset.region)
    ss << ";region=" << region;
  if (__isset.bucket)
    ss << ";bucket=" << bucket;
  if (__isset.ak)
    ss << ";ak=xxxxxx";
  if (__isset.sk)
    ss << ";sk=xxxxxx";
  if (__isset.st)
    ss << ";st=xxxxxx";
  return ss.str();
}

TosCredentialsFileSelection resolveTosCredentialsFromFile(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config) {
  auto credentialsPath = getTosConfigValue(
      bucket, fileOptions, config, TosConfigKey::kCredentialsFilePath);
  if (!credentialsPath.has_value()) {
    return {std::nullopt};
  }

  auto credentials = readTosCredentialsFile(credentialsPath.value(), bucket);
  BOLT_CHECK(
      credentials.has_value(),
      "Can not found TOS credentials from tos.credentials.file.path {}",
      credentialsPath.value());

  return {std::move(credentials)};
}

std::string getTosFileSystemCacheKey(
    std::string_view bucket,
    const FileOptions& fileOptions,
    const config::ConfigBase* config) {
  std::stringstream cacheKey;
  cacheKey << "config=" << config << ";bucket=" << bucket << ";";
  appendCacheKeyPart(
      cacheKey, "endpoint", getTosEndpointValue(bucket, fileOptions, config));
  return cacheKey.str();
}

std::shared_ptr<FileSystem> getOrCreateCachedTosFileSystem(
    TosFileSystemCache& fileSystems,
    std::shared_ptr<const config::ConfigBase> properties,
    std::string_view bucket,
    const FileOptions& fileOptions,
    const TosFileSystemFactory& factory) {
  auto resolvedProperties = ensureProperties(std::move(properties));
  auto cacheKey =
      getTosFileSystemCacheKey(bucket, fileOptions, resolvedProperties.get());

  return fileSystems.withWLock(
      [&](auto& instances) -> std::shared_ptr<FileSystem> {
        if (auto cached = instances.get(cacheKey)) {
          return cached.value();
        }

        auto fs = factory(std::move(resolvedProperties));
        instances.add(cacheKey, fs);
        return fs;
      });
}

TosSessionConfig getTosSessionConfig(
    const std::string_view& bucket,
    const std::string& endpoint,
    const FileOptions& fileOptions,
    const config::ConfigBase* config) {
  TosSessionConfig sessionConfig;

  sessionConfig.__set_bucket(bucket);

  if (!endpoint.empty()) {
    sessionConfig.__set_endpoint(endpoint);
  } else {
    auto endpointValue = getTosEndpointValue(bucket, fileOptions, config);
    if (endpointValue.has_value()) {
      sessionConfig.__set_endpoint(endpointValue.value());
    }
  }

  if (auto region = getTosConfigValue(
          bucket, fileOptions, config, TosConfigKey::kRegion)) {
    sessionConfig.__set_region(*region);
  }
  if (!trySetCredentialsFromFileOptions(bucket, fileOptions, sessionConfig) &&
      !trySetCredentialsFromFile(bucket, fileOptions, config, sessionConfig)) {
    trySetSecretKeyCredentials(bucket, fileOptions, config, sessionConfig);
  }

  return sessionConfig;
}

} // namespace bytedance::bolt::filesystems
