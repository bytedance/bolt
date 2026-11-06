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
#include "bolt/common/config/Config.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

namespace bytedance::bolt::filesystems {
namespace {

class FakeFileSystem : public FileSystem {
 public:
  explicit FakeFileSystem(std::shared_ptr<const config::ConfigBase> config)
      : FileSystem(std::move(config)) {}

  std::string name() const override {
    return "fake";
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view /*path*/,
      const FileOptions& /*options*/ = {}) override {
    BOLT_UNSUPPORTED("not used");
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view /*path*/,
      const FileOptions& /*options*/ = {}) override {
    BOLT_UNSUPPORTED("not used");
  }

  void remove(std::string_view /*path*/) override {
    BOLT_UNSUPPORTED("not used");
  }

  void rename(
      std::string_view /*oldPath*/,
      std::string_view /*newPath*/,
      bool /*overwrite*/ = false) override {
    BOLT_UNSUPPORTED("not used");
  }

  bool exists(std::string_view /*path*/) override {
    BOLT_UNSUPPORTED("not used");
  }

  std::vector<std::string> list(std::string_view /*path*/) override {
    BOLT_UNSUPPORTED("not used");
  }

  void mkdir(std::string_view /*path*/) override {
    BOLT_UNSUPPORTED("not used");
  }

  void rmdir(std::string_view /*path*/) override {
    BOLT_UNSUPPORTED("not used");
  }
};

std::shared_ptr<config::ConfigBase> makeConfig(
    std::unordered_map<std::string, std::string> values) {
  return std::make_shared<config::ConfigBase>(std::move(values));
}

std::string writeCredentialsFile(
    const std::string& name,
    const std::string& content) {
  static const auto directory = exec::test::TempDirectoryPath::create();
  const auto path =
      std::filesystem::path(directory->getPath()) / (name + ".xml");
  std::ofstream output(path);
  output << content;
  output.close();
  return path.string();
}

TEST(TosConfigTest, credentialsFileOverridesSecretKey) {
  auto credentialsPath = writeCredentialsFile("config", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-file-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-file-sk</value>
  </property>
  <property>
    <name>fs.tos.session-token</name>
    <value>base-file-token</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.access-key-id</name>
    <value>file-ak</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.secret-access-key</name>
    <value>file-sk</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"tos.credentials.file.path", credentialsPath},
      {"tos.access.key", "config-ak"},
      {"tos.secret.key", "config-sk"},
      {"tos.session.token", "config-token"},
  });
  auto fileOptions = FileOptions{};

  const auto sessionConfig =
      getTosSessionConfig("bucket", "", fileOptions, config.get());

  EXPECT_EQ(sessionConfig.ak, "file-ak");
  EXPECT_EQ(sessionConfig.sk, "file-sk");
  EXPECT_FALSE(sessionConfig.__isset.st);
}

TEST(TosConfigTest, CredentialsFileSelectionUsesBucketCredentialsWhenMatched) {
  auto credentialsPath = writeCredentialsFile("bucket_cache_key", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-file-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-file-sk</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.access-key-id</name>
    <value>bucket-file-ak</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.secret-access-key</name>
    <value>bucket-file-sk</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"tos.credentials.file.path", credentialsPath},
  });

  const auto selection =
      resolveTosCredentialsFromFile("bucket", FileOptions{}, config.get());

  ASSERT_TRUE(selection.credentials.has_value());
  EXPECT_EQ(selection.credentials->accessKey, "bucket-file-ak");
  EXPECT_EQ(selection.credentials->secretKey, "bucket-file-sk");
}

TEST(TosConfigTest, CredentialsFileSelectionUsesBaseCredentialsWhenUnmatched) {
  auto credentialsPath = writeCredentialsFile("base_cache_key", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-file-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-file-sk</value>
  </property>
  <property>
    <name>fs.tos.bucket.other.access-key-id</name>
    <value>other-file-ak</value>
  </property>
  <property>
    <name>fs.tos.bucket.other.secret-access-key</name>
    <value>other-file-sk</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"tos.credentials.file.path", credentialsPath},
  });

  const auto selection =
      resolveTosCredentialsFromFile("bucket", FileOptions{}, config.get());

  ASSERT_TRUE(selection.credentials.has_value());
  EXPECT_EQ(selection.credentials->accessKey, "base-file-ak");
  EXPECT_EQ(selection.credentials->secretKey, "base-file-sk");
}

TEST(TosConfigTest, CredentialsFileRejectsBucketPartialCredentials) {
  auto credentialsPath = writeCredentialsFile("bucket_partial_credentials", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-file-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-file-sk</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.access-key-id</name>
    <value>bucket-file-ak</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"tos.credentials.file.path", credentialsPath},
  });

  EXPECT_THROW(
      resolveTosCredentialsFromFile("bucket", FileOptions{}, config.get()),
      BoltRuntimeError);
  EXPECT_THROW(
      getTosSessionConfig("bucket", "", FileOptions{}, config.get()),
      BoltRuntimeError);
}

TEST(TosConfigTest, GeneratorCacheKeySeparatesConfigInstances) {
  auto configA = makeConfig({
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });
  auto configB = makeConfig({
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });

  EXPECT_NE(
      getTosFileSystemCacheKey("bucket", FileOptions{}, configA.get()),
      getTosFileSystemCacheKey("bucket", FileOptions{}, configB.get()));
}

TEST(TosConfigTest, GeneratorCacheReusesNullConfig) {
  TosFileSystemCache cache(folly::in_place, kTosFileSystemCacheCapacity);
  size_t createdFileSystems{0};
  auto factory = [&](std::shared_ptr<const config::ConfigBase> config)
      -> std::shared_ptr<FileSystem> {
    ++createdFileSystems;
    return std::make_shared<FakeFileSystem>(std::move(config));
  };

  auto firstFileSystem = getOrCreateCachedTosFileSystem(
      cache, nullptr, "bucket", FileOptions{}, factory);
  auto secondFileSystem = getOrCreateCachedTosFileSystem(
      cache, nullptr, "bucket", FileOptions{}, factory);

  EXPECT_EQ(firstFileSystem, secondFileSystem);
  EXPECT_EQ(createdFileSystems, 1);
}

TEST(TosConfigTest, GeneratorCacheKeyIgnoresRequestCredentialFilePath) {
  auto config = makeConfig({
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });
  FileOptions optionsA;
  optionsA.values["tos.credentials.file.path"] = "/credentials/first.xml";
  FileOptions optionsB;
  optionsB.values["tos.credentials.file.path"] = "/credentials/second.xml";

  EXPECT_EQ(
      getTosFileSystemCacheKey("bucket", optionsA, config.get()),
      getTosFileSystemCacheKey("bucket", optionsB, config.get()));
}

TEST(TosConfigTest, GeneratorCacheKeySeparatesBuckets) {
  auto config = makeConfig({});

  EXPECT_NE(
      getTosFileSystemCacheKey("bucket", FileOptions{}, config.get()),
      getTosFileSystemCacheKey("other", FileOptions{}, config.get()));
}

TEST(TosConfigTest, GeneratorCacheSeparatesConfigsAndBuckets) {
  auto configA = makeConfig({
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });
  auto configB = makeConfig({
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });
  TosFileSystemCache cache(folly::in_place, kTosFileSystemCacheCapacity);
  size_t createdFileSystems{0};
  auto factory = [&](std::shared_ptr<const config::ConfigBase> config)
      -> std::shared_ptr<FileSystem> {
    ++createdFileSystems;
    return std::make_shared<FakeFileSystem>(std::move(config));
  };

  auto bucketFileSystem = getOrCreateCachedTosFileSystem(
      cache, configA, "bucket", FileOptions{}, factory);
  auto sameBucketFileSystem = getOrCreateCachedTosFileSystem(
      cache, configB, "bucket", FileOptions{}, factory);
  auto fallbackFileSystem = getOrCreateCachedTosFileSystem(
      cache, configA, "other", FileOptions{}, factory);

  EXPECT_NE(bucketFileSystem, sameBucketFileSystem);
  EXPECT_NE(bucketFileSystem, fallbackFileSystem);
  EXPECT_EQ(createdFileSystems, 3);
}

TEST(TosConfigTest, GeneratorCacheIgnoresRotatingRequestCredentials) {
  auto config = makeConfig({
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });
  FileOptions firstOptions;
  firstOptions.values["tos.access.key"] = "first-ak";
  firstOptions.values["tos.secret.key"] = "first-sk";
  firstOptions.values["tos.session.token"] = "first-token";
  FileOptions secondOptions;
  secondOptions.values["tos.access.key"] = "second-ak";
  secondOptions.values["tos.secret.key"] = "second-sk";
  secondOptions.values["tos.session.token"] = "second-token";

  TosFileSystemCache cache(folly::in_place, kTosFileSystemCacheCapacity);
  size_t createdFileSystems{0};
  auto factory = [&](std::shared_ptr<const config::ConfigBase> properties)
      -> std::shared_ptr<FileSystem> {
    ++createdFileSystems;
    return std::make_shared<FakeFileSystem>(std::move(properties));
  };

  auto firstFileSystem = getOrCreateCachedTosFileSystem(
      cache, config, "bucket", firstOptions, factory);
  auto secondFileSystem = getOrCreateCachedTosFileSystem(
      cache, config, "bucket", secondOptions, factory);

  EXPECT_EQ(firstFileSystem, secondFileSystem);
  EXPECT_EQ(createdFileSystems, 1);

  const auto cacheKey =
      getTosFileSystemCacheKey("bucket", firstOptions, config.get());
  EXPECT_EQ(cacheKey.find("first-ak"), std::string::npos);
  EXPECT_EQ(cacheKey.find("first-sk"), std::string::npos);
  EXPECT_EQ(cacheKey.find("first-token"), std::string::npos);
}

TEST(TosConfigTest, GeneratorCacheEvictsLeastRecentlyUsedFileSystem) {
  auto config = makeConfig({
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });
  TosFileSystemCache cache(folly::in_place, 2);
  size_t createdFileSystems{0};
  auto factory = [&](std::shared_ptr<const config::ConfigBase> properties)
      -> std::shared_ptr<FileSystem> {
    ++createdFileSystems;
    return std::make_shared<FakeFileSystem>(std::move(properties));
  };

  getOrCreateCachedTosFileSystem(
      cache, config, "bucket-a", FileOptions{}, factory);
  getOrCreateCachedTosFileSystem(
      cache, config, "bucket-b", FileOptions{}, factory);
  getOrCreateCachedTosFileSystem(
      cache, config, "bucket-a", FileOptions{}, factory);
  getOrCreateCachedTosFileSystem(
      cache, config, "bucket-c", FileOptions{}, factory);
  getOrCreateCachedTosFileSystem(
      cache, config, "bucket-b", FileOptions{}, factory);

  EXPECT_EQ(createdFileSystems, 4);
  cache.withRLock(
      [](const auto& instances) { EXPECT_EQ(instances.currentSize(), 2); });
}

TEST(TosConfigTest, FsTosConfigUsesBaseCredentialsAndEndpoint) {
  auto credentialsPath = writeCredentialsFile("fs_tos_config", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-file-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-file-sk</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.access-key-id</name>
    <value>bucket-file-ak</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.secret-access-key</name>
    <value>bucket-file-sk</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"fs.tos.credentials.file.path", credentialsPath},
      {"fs.tos.endpoint", "tos-cn-beijing.volces.com"},
  });
  TosFileSystemCache cache(folly::in_place, kTosFileSystemCacheCapacity);
  size_t createdFileSystems{0};
  auto factory = [&](std::shared_ptr<const config::ConfigBase> config)
      -> std::shared_ptr<FileSystem> {
    ++createdFileSystems;
    return std::make_shared<FakeFileSystem>(std::move(config));
  };

  auto firstFallbackFileSystem = getOrCreateCachedTosFileSystem(
      cache, config, "fallback", FileOptions{}, factory);
  auto secondFallbackFileSystem = getOrCreateCachedTosFileSystem(
      cache, config, "fallback", FileOptions{}, factory);
  const auto sessionConfig =
      getTosSessionConfig("fallback-a", "", FileOptions{}, config.get());

  EXPECT_EQ(firstFallbackFileSystem, secondFallbackFileSystem);
  EXPECT_EQ(createdFileSystems, 1);
  EXPECT_EQ(sessionConfig.endpoint, "tos-cn-beijing.volces.com");
  EXPECT_EQ(sessionConfig.ak, "base-file-ak");
  EXPECT_EQ(sessionConfig.sk, "base-file-sk");
}

TEST(TosConfigTest, TosSessionConfigToStringMasksCredentials) {
  auto config = makeConfig({
      {"tos.access.key", "config-ak"},
      {"tos.secret.key", "config-sk"},
      {"tos.session.token", "config-token"},
      {"hive.tos.endpoint", "tos-cn-beijing.volces.com"},
  });

  const auto sessionConfig =
      getTosSessionConfig("bucket", "", FileOptions{}, config.get());
  const auto serialized = sessionConfig.toString();

  EXPECT_EQ(serialized.find("config-ak"), std::string::npos);
  EXPECT_EQ(serialized.find("config-sk"), std::string::npos);
  EXPECT_EQ(serialized.find("config-token"), std::string::npos);
  EXPECT_NE(serialized.find(";ak=xxxxxx"), std::string::npos);
  EXPECT_NE(serialized.find(";sk=xxxxxx"), std::string::npos);
  EXPECT_NE(serialized.find(";st=xxxxxx"), std::string::npos);
}

TEST(TosConfigTest, CredentialsFilePathFromFileOptionsOverridesConfig) {
  auto configCredentialsPath = writeCredentialsFile("config_loser", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>config-file-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>config-file-sk</value>
  </property>
</configuration>)");
  auto fileOptionsCredentialsPath = writeCredentialsFile("file_options", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>file-options-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>file-options-sk</value>
  </property>
  <property>
    <name>fs.tos.session-token</name>
    <value>file-options-token</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"tos.credentials.file.path", configCredentialsPath},
  });
  FileOptions fileOptions;
  fileOptions.values["tos.credentials.file.path"] = fileOptionsCredentialsPath;

  const auto sessionConfig =
      getTosSessionConfig("bucket", "", fileOptions, config.get());

  EXPECT_EQ(sessionConfig.ak, "file-options-ak");
  EXPECT_EQ(sessionConfig.sk, "file-options-sk");
  EXPECT_EQ(sessionConfig.st, "file-options-token");
}

TEST(TosConfigTest, CredentialsFileIsResolvedForEachRequest) {
  auto credentialsPath = writeCredentialsFile("rotated_credentials", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>first-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>first-sk</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"tos.credentials.file.path", credentialsPath},
  });

  const auto firstSessionConfig =
      getTosSessionConfig("bucket", "", FileOptions{}, config.get());
  EXPECT_EQ(firstSessionConfig.ak, "first-ak");
  EXPECT_EQ(firstSessionConfig.sk, "first-sk");

  writeCredentialsFile("rotated_credentials", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>second-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>second-sk</value>
  </property>
</configuration>)");

  const auto secondSessionConfig =
      getTosSessionConfig("bucket", "", FileOptions{}, config.get());
  EXPECT_EQ(secondSessionConfig.ak, "second-ak");
  EXPECT_EQ(secondSessionConfig.sk, "second-sk");
}

TEST(TosConfigTest, BucketCredentialsFilePathOverridesBasePath) {
  auto baseCredentialsPath = writeCredentialsFile("base_path", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-path-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-path-sk</value>
  </property>
</configuration>)");
  auto bucketCredentialsPath = writeCredentialsFile("bucket_path", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>bucket-path-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>bucket-path-sk</value>
  </property>
</configuration>)");
  auto config = makeConfig({
      {"tos.credentials.file.path", baseCredentialsPath},
      {"tos.bucket.bucket.credentials.file.path", bucketCredentialsPath},
  });

  const auto sessionConfig =
      getTosSessionConfig("bucket", "", FileOptions{}, config.get());

  EXPECT_EQ(sessionConfig.ak, "bucket-path-ak");
  EXPECT_EQ(sessionConfig.sk, "bucket-path-sk");
}

TEST(TosConfigTest, explicitRegionOverridesEndpointInference) {
  auto config = makeConfig(
      {{"tos.endpoint", "https://tos-cn-beijing.volces.com"},
       {"tos.region", "configured-region"}});
  EXPECT_EQ(
      getTosSessionConfig("bucket", "", {}, config.get()).region,
      "configured-region");
  FileOptions options;
  options.values["fs.tos.bucket.bucket.region"] = "request-region";
  EXPECT_EQ(
      getTosSessionConfig("bucket", "", options, config.get()).region,
      "request-region");
}

TEST(TosConfigTest, nullConfigAndPartialCredentials) {
  EXPECT_NO_THROW(getTosSessionConfig("bucket", "", {}, nullptr));
  for (const auto& key :
       {"tos.access.key", "tos.secret.key", "tos.session.token"}) {
    auto config = makeConfig({{key, "partial"}});
    EXPECT_ANY_THROW(getTosSessionConfig("bucket", "", {}, config.get()));
  }
}

} // namespace
} // namespace bytedance::bolt::filesystems
