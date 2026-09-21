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

#include "bolt/connectors/hive/storage_adapters/tos/TosFileSystem.h"
#include <folly/Uri.h>
#include <model/bucket/CreateBucketV2Input.h>
#include <model/bucket/HeadBucketV2Input.h>
#include <utils/BaseUtils.h>
#include <mutex>
#include <utility>
#include "bolt/common/config/Config.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosFileSystemExtension.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosReadFile.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosWriteFile.h"

namespace bytedance::bolt::filesystems {
namespace {
constexpr std::string_view kScheme = "tos://";
constexpr size_t kBucketTypeCacheCapacity = 1024;

// The SDK initialization flag and libcurl runtime are process-wide. Keep them
// initialized: another component may still own a client when our last file is
// destroyed, so a per-file CloseClient() would shut down somebody else's SDK.
void initializeTosClient() {
  static std::once_flag initialized;
  std::call_once(initialized, [] { VolcengineTos::InitializeClient(); });
}

} // namespace

std::pair<std::string_view, std::string_view> TosFileSystem::parsePath(
    std::string_view path) {
  BOLT_USER_CHECK(
      isTosFile(path), "Invalid TOS URI: expected tos://bucket/object");
  path.remove_prefix(kScheme.size());
  const auto slash = path.find('/');
  BOLT_USER_CHECK(
      slash != std::string_view::npos && slash > 0 && slash + 1 < path.size(),
      "Invalid TOS URI: expected a non-empty bucket and object");
  const auto bucket = path.substr(0, slash);
  BOLT_USER_CHECK(
      bucket.find_first_of("@:#?\\") == std::string_view::npos,
      "Invalid TOS bucket in URI");
  return {bucket, path.substr(slash + 1)};
}

std::shared_ptr<VolcengineTos::TosClientV2> connectToTos(
    const TosSessionConfig& configInfo) {
  BOLT_USER_CHECK(
      !configInfo.endpoint.empty(), "TOS endpoint must be configured");
  BOLT_USER_CHECK(
      !configInfo.region.empty(),
      "TOS region must be configured or inferred from the endpoint");
  VolcengineTos::ClientConfig conf;
  conf.endPoint = configInfo.endpoint;
  customizeTosClientConfig(conf);
  const folly::Uri endpoint(
      conf.endPoint.find("://") == std::string::npos
          ? "https://" + conf.endPoint
          : conf.endPoint);
  BOLT_USER_CHECK(
      (endpoint.scheme() == "http" || endpoint.scheme() == "https") &&
          !endpoint.host().empty() && endpoint.path().empty() &&
          endpoint.query().empty() && endpoint.fragment().empty() &&
          endpoint.username().empty() && endpoint.password().empty(),
      "TOS endpoint must be an HTTP or HTTPS service hostname");
  // Match the SDK's endpoint checks before headObject: 2.6.28 dereferences a
  // null response when roundTrip rejects an IP, port or S3 endpoint.
  const auto schemeEnd = conf.endPoint.find("://");
  const auto endpointHost = schemeEnd == std::string::npos
      ? conf.endPoint
      : conf.endPoint.substr(schemeEnd + 3);
  BOLT_USER_CHECK(
      VolcengineTos::NetUtils::isNotIP(endpointHost) &&
          !VolcengineTos::NetUtils::isS3Endpoint(endpointHost),
      "TOS SDK requires a native service hostname without an IP address, port or S3 endpoint");
  initializeTosClient();
  return std::make_shared<VolcengineTos::TosClientV2>(
      configInfo.region, configInfo.ak, configInfo.sk, configInfo.st, conf);
}

TosFileSystem::TosFileSystem(std::shared_ptr<const config::ConfigBase> config)
    : FileSystem(
          config ? std::move(config)
                 : std::make_shared<config::ConfigBase>(
                       std::unordered_map<std::string, std::string>{})),
      bucketTypeCache_(folly::in_place, kBucketTypeCacheCapacity) {}

std::string TosFileSystem::name() const {
  return "Tos";
}

std::unique_ptr<ReadFile> TosFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& fileOptions) {
  auto [bucket, objectName] = parsePath(path);

  auto sessionConfig =
      getTosSessionConfig(bucket, "", fileOptions, config_.get());
  notifyTosBucketAccess(sessionConfig, *config_);
  LOG(INFO) << "connect to Tos for read with config "
            << sessionConfig.toString();
  auto tosClient = connectToTos(sessionConfig);
  return std::make_unique<TosReadFile>(
      std::move(tosClient), bucket, objectName);
}

std::unique_ptr<WriteFile> TosFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& fileOptions) {
  auto [bucket, objectName] = parsePath(path);
  auto sessionConfig =
      getTosSessionConfig(bucket, "", fileOptions, config_.get());
  notifyTosBucketAccess(sessionConfig, *config_);
  LOG(INFO) << "connect to Tos for write with config "
            << sessionConfig.toString();
  auto tosClient = connectToTos(sessionConfig);
  const std::string bucketName(bucket);
  std::string bucketTypeCacheKey(sessionConfig.endpoint);
  bucketTypeCacheKey.push_back('\0');
  bucketTypeCacheKey.append(bucket);
  auto bucketType = bucketTypeCache_.withWLock([&](auto& bucketTypes) {
    if (auto cachedType = bucketTypes.get(bucketTypeCacheKey)) {
      return cachedType.value();
    }

    VolcengineTos::HeadBucketV2Input headBucketInput(bucketName);
    auto headBucketResult = tosClient->headBucket(headBucketInput);
    BucketType loadedType;
    if (headBucketResult.isSuccess()) {
      loadedType = headBucketResult.result().getBucketType();
    } else {
      auto& error = headBucketResult.error();
      if (error.isClientError() || error.getStatusCode() != 404) {
        BOLT_FAIL(
            "Failed to get tos bucket type, msg: {}, objectName: {}, bucketName: {}.",
            error.String(),
            objectName,
            bucketName);
      }
      BOLT_CHECK(
          fileOptions.shouldCreateParentDirectories,
          "Tos bucket does not exist and bucket creation is disabled, msg: {}, objectName: {}, bucketName: {}.",
          error.String(),
          objectName,
          bucketName);

      VolcengineTos::CreateBucketV2Input createBucketInput(bucketName);
      auto createBucketResult = tosClient->createBucket(createBucketInput);
      BOLT_CHECK(
          createBucketResult.isSuccess(),
          "Failed to create tos bucket, msg: {}, objectName: {}, bucketName: {}.",
          createBucketResult.error().String(),
          objectName,
          bucketName);
      LOG(INFO) << fmt::format(
          "tos bucket: {} created successfully", bucketName);
      loadedType = BucketType::FNS;
    }
    bucketTypes.add(bucketTypeCacheKey, loadedType);
    return loadedType;
  });
  return std::make_unique<TosWriteFile>(
      std::move(tosClient),
      bucket,
      objectName,
      bucketType,
      fileOptions.shouldThrowOnFileAlreadyExists);
}

bool TosFileSystem::isTosFile(const std::string_view filename) {
  return filename.find(kScheme) == 0;
}

} // namespace bytedance::bolt::filesystems
