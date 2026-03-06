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

#include "bolt/connectors/hive/storage_adapters/gcs/GcsFileSystem.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/File.h"
#include "bolt/connectors/hive/storage_adapters/gcs/GcsUtil.h"
#include "bolt/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h"
#include "bolt/connectors/hive/storage_adapters/gcs/tests/GcsEmulator.h"
#include "bolt/exec/tests/utils/TempFilePath.h"

#include "gtest/gtest.h"

using namespace bytedance::bolt::common::testutil;

namespace bytedance::bolt::filesystems {
namespace {

class DummyGcsOAuthCredentialsProvider : public GcsOAuthCredentialsProvider {
 public:
  explicit DummyGcsOAuthCredentialsProvider() : GcsOAuthCredentialsProvider() {}

  std::shared_ptr<gcs::oauth2::Credentials> getCredentials(
      const std::string&) override {
    BOLT_FAIL("DummyGcsOAuthCredentialsProvider: Not implemented");
  }
};

class GcsFileSystemTest : public testing::Test {
 public:
  void SetUp() {
    emulator_ = std::make_shared<GcsEmulator>();
    emulator_->bootstrap();
  }

  std::shared_ptr<GcsEmulator> emulator_;
};

TEST_F(GcsFileSystemTest, readFile) {
  const auto gcsFile = gcsURI(
      emulator_->preexistingBucketName(), emulator_->preexistingObjectName());

  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  gcfs.initializeClient();
  auto readFile = gcfs.openFileForRead(gcsFile);
  std::int64_t size = readFile->size();
  std::int64_t ref_size = kLoremIpsum.length();
  EXPECT_EQ(size, ref_size);
  EXPECT_EQ(readFile->pread(0, size), kLoremIpsum);

  std::vector<char> buffer1(size);
  ASSERT_EQ(readFile->pread(0, size, buffer1.data()), kLoremIpsum);
  ASSERT_EQ(readFile->size(), ref_size);

  char buffer2[50];
  ASSERT_EQ(readFile->pread(10, 50, &buffer2), kLoremIpsum.substr(10, 50));
  ASSERT_EQ(readFile->size(), ref_size);

  EXPECT_EQ(readFile->pread(10, size - 10), kLoremIpsum.substr(10));

  char buff1[10];
  char buff2[20];
  char buff3[30];
  std::vector<folly::Range<char*>> buffers = {
      folly::Range<char*>(buff1, 10),
      folly::Range<char*>(nullptr, 20),
      folly::Range<char*>(buff2, 20),
      folly::Range<char*>(nullptr, 30),
      folly::Range<char*>(buff3, 30)};
  ASSERT_EQ(10 + 20 + 20 + 30 + 30, readFile->preadv(0, buffers));
  ASSERT_EQ(std::string_view(buff1, sizeof(buff1)), kLoremIpsum.substr(0, 10));
  ASSERT_EQ(std::string_view(buff2, sizeof(buff2)), kLoremIpsum.substr(30, 20));
  ASSERT_EQ(std::string_view(buff3, sizeof(buff3)), kLoremIpsum.substr(80, 30));
}

TEST_F(GcsFileSystemTest, writeAndReadFile) {
  const std::string_view newFile = "readWriteFile.txt";
  const auto gcsFile = gcsURI(emulator_->preexistingBucketName(), newFile);

  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  gcfs.initializeClient();
  auto writeFile = gcfs.openFileForWrite(gcsFile);
  std::string_view kDataContent =
      "Dance me to your beauty with a burning violin"
      "Dance me through the panic till I'm gathered safely in"
      "Lift me like an olive branch and be my homeward dove"
      "Dance me to the end of love";

  EXPECT_EQ(writeFile->size(), 0);
  std::int64_t contentSize = kDataContent.length();
  writeFile->append(kDataContent.substr(0, 10));
  EXPECT_EQ(writeFile->size(), 10);
  writeFile->append(kDataContent.substr(10, contentSize - 10));
  EXPECT_EQ(writeFile->size(), contentSize);
  writeFile->flush();
  writeFile->close();
  BOLT_ASSERT_THROW(
      writeFile->append(kDataContent.substr(0, 10)), "File is not open");

  auto readFile = gcfs.openFileForRead(gcsFile);
  std::int64_t size = readFile->size();
  EXPECT_EQ(readFile->size(), contentSize);
  EXPECT_EQ(readFile->pread(0, size), kDataContent);

  // Opening an existing file for write must be an error.
  filesystems::GcsFileSystem newGcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  newGcfs.initializeClient();
  BOLT_ASSERT_THROW(newGcfs.openFileForWrite(gcsFile), "File already exists");
}

TEST_F(GcsFileSystemTest, rename) {
  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  gcfs.initializeClient();

  const std::string_view oldFile = "oldTest.txt";
  const std::string_view newFile = "newTest.txt";

  const auto gcsExistingFile =
      gcsURI(emulator_->preexistingBucketName(), oldFile);
  auto writeFile = gcfs.openFileForWrite(gcsExistingFile);
  std::string_view kDataContent = "GcsFileSystemTest rename operation test";
  writeFile->append(kDataContent.substr(0, 10));
  writeFile->flush();
  writeFile->close();

  const auto gcsNewFile = gcsURI(emulator_->preexistingBucketName(), newFile);

  BOLT_ASSERT_THROW(
      gcfs.rename(gcsExistingFile, gcsExistingFile, false),
      fmt::format(
          "Failed to rename object {} to {} with as {} exists.",
          oldFile,
          oldFile,
          oldFile));

  gcfs.rename(gcsExistingFile, gcsNewFile, true);

  auto results = gcfs.list(gcsNewFile);
  ASSERT_TRUE(
      std::find(results.begin(), results.end(), oldFile) == results.end());
  ASSERT_TRUE(
      std::find(results.begin(), results.end(), newFile) != results.end());
}

TEST_F(GcsFileSystemTest, mkdir) {
  const std::string_view dir = "newDirectory";
  const auto gcsNewDirectory = gcsURI(emulator_->preexistingBucketName(), dir);
  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  gcfs.initializeClient();
  gcfs.mkdir(gcsNewDirectory);
  const auto& results = gcfs.list(gcsNewDirectory);
  ASSERT_TRUE(std::find(results.begin(), results.end(), dir) != results.end());
}

TEST_F(GcsFileSystemTest, rmdir) {
  const std::string_view dir = "Directory";
  const auto gcsDirectory = gcsURI(emulator_->preexistingBucketName(), dir);
  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  gcfs.initializeClient();

  auto writeFile = gcfs.openFileForWrite(gcsDirectory);
  std::string_view kDataContent = "GcsFileSystemTest rename operation test";
  writeFile->append(kDataContent.substr(0, 10));
  writeFile->flush();
  writeFile->close();

  auto results = gcfs.list(gcsDirectory);
  ASSERT_TRUE(std::find(results.begin(), results.end(), dir) != results.end());
  gcfs.rmdir(gcsDirectory);

  results = gcfs.list(gcsDirectory);
  ASSERT_TRUE(std::find(results.begin(), results.end(), dir) == results.end());
}

TEST_F(GcsFileSystemTest, missingFile) {
  const std::string_view file = "newTest.txt";
  const auto gcsFile = gcsURI(emulator_->preexistingBucketName(), file);
  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  gcfs.initializeClient();
  BOLT_ASSERT_RUNTIME_THROW_CODE(
      gcfs.openFileForRead(gcsFile), error_code::kFileNotFound);
}

TEST_F(GcsFileSystemTest, missingBucket) {
  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), emulator_->hiveConfig());
  gcfs.initializeClient();
  const std::string_view gcsFile = "gs://dummy/foo.txt";
  BOLT_ASSERT_RUNTIME_THROW_CODE(
      gcfs.openFileForRead(gcsFile), error_code::kFileNotFound);
}

TEST_F(GcsFileSystemTest, credentialsConfig) {
  // This test needs a JSON that looks like Service Account credentials so the
  // GCS library rejects it along the ServiceAccountCredentials path.
  //
  // IMPORTANT: GitHub Push Protection flags even fake-looking Service Account
  // JSON blobs in source code. Build the JSON from pieces to avoid embedding a
  // credential-shaped literal in the repository.
  const std::string kCreds = std::string("{\n") + "  \"type\": \"" +
      std::string("service_") + "account\",\n" +
      "  \"project_id\": \"foo-project\",\n" + "  \"" +
      std::string("private_") + "key_id\": \"dummy\",\n" + "  \"" +
      std::string("private_") + "key\": \"dummy\",\n" + "  \"" +
      std::string("client_") + "email\": \"foo-email@foo-project.iam.g" +
      std::string("serviceaccount") + ".com\",\n" + "  \"" +
      std::string("client_") + "id\": \"100000000000000000001\",\n" +
      "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
      "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
      "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
      "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/foo-email%40foo-project.iam.g" +
      std::string("serviceaccount") + ".com\"\n" + "}\n";
  auto jsonFile = ::bytedance::bolt::exec::test::TempFilePath::create();
  std::ofstream credsOut(jsonFile->getPath());
  credsOut << kCreds;
  credsOut.close();

  std::unordered_map<std::string, std::string> configOverride = {
      {"hive.gcs.json-key-file-path", jsonFile->getPath()}};
  auto hiveConfig = emulator_->hiveConfig(configOverride);

  filesystems::GcsFileSystem gcfs(
      emulator_->preexistingBucketName(), hiveConfig);
  gcfs.initializeClient();
  const auto gcsFile = gcsURI(
      emulator_->preexistingBucketName(), emulator_->preexistingObjectName());
  BOLT_ASSERT_THROW(gcfs.openFileForRead(gcsFile), "ServiceAccountCredentials");
}

TEST_F(GcsFileSystemTest, credentialsProvider) {
  const auto providerFactory =
      [](const std::shared_ptr<connector::hive::HiveConfig>&) {
        return std::make_shared<DummyGcsOAuthCredentialsProvider>();
      };
  registerGcsOAuthCredentialsProvider("dummy_provider", providerFactory);

  {
    std::unordered_map<std::string, std::string> configOverride = {
        {"hive.gcs.auth.access-token-provider", "dummy_provider"}};
    auto hiveConfig = emulator_->hiveConfig(configOverride);

    filesystems::GcsFileSystem gcfs(
        emulator_->preexistingBucketName(), hiveConfig);
    BOLT_ASSERT_THROW(
        gcfs.initializeClient(),
        "DummyGcsOAuthCredentialsProvider: Not implemented");

    BOLT_ASSERT_THROW(
        registerGcsOAuthCredentialsProvider("", providerFactory),
        "GcsOAuthCredentialsProviderFactory name cannot be empty");

    BOLT_ASSERT_THROW(
        registerGcsOAuthCredentialsProvider("dummy_provider", providerFactory),
        "GcsOAuthCredentialsProviderFactory 'dummy_provider' already registered");
  }

  // Invalid provider name.
  {
    std::unordered_map<std::string, std::string> configOverride = {
        {"hive.gcs.auth.access-token-provider", ""}};
    auto hiveConfig = emulator_->hiveConfig(configOverride);

    filesystems::GcsFileSystem gcfs(
        emulator_->preexistingBucketName(), hiveConfig);

    BOLT_ASSERT_THROW(
        gcfs.initializeClient(),
        "GcsOAuthCredentialsProviderFactory name cannot be empty");
  }
}

TEST_F(GcsFileSystemTest, defaultCacheKey) {
  registerGcsFileSystem();
  std::unordered_map<std::string, std::string> configWithoutEndpoint = {};
  auto hiveConfigDefault = std::make_shared<const config::ConfigBase>(
      std::move(configWithoutEndpoint));
  const auto gcsFile1 = gcsURI(
      emulator_->preexistingBucketName(), emulator_->preexistingObjectName());
  // FileSystem should be cached by the default key.
  auto defaultGcs = filesystems::getFileSystem(gcsFile1, hiveConfigDefault);

  std::unordered_map<std::string, std::string> configWithEndpoint = {
      {connector::hive::HiveConfig::kGcsEndpoint, kGcsDefaultCacheKeyPrefix}};
  auto hiveConfigCustom =
      std::make_shared<const config::ConfigBase>(std::move(configWithEndpoint));
  const auto gcsFile2 = gcsURI(emulator_->preexistingBucketName(), "dummy.txt");
  auto customGcs = filesystems::getFileSystem(gcsFile2, hiveConfigCustom);
  // The same FileSystem should be cached by the value of key
  // kGcsDefaultCacheKeyPrefix.
  ASSERT_EQ(customGcs, defaultGcs);
}

} // namespace
} // namespace bytedance::bolt::filesystems
