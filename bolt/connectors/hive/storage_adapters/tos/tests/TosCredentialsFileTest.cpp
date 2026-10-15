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

#include "bolt/connectors/hive/storage_adapters/tos/TosCredentialsFile.h"

#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/tests/GTestUtils.h"

namespace bytedance::bolt::filesystems {
namespace {

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

TEST(
    TosCredentialsFileTest,
    BaseCredentialsAreUsedWhenBucketCredentialsAbsent) {
  const auto path = writeCredentialsFile("base", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-sk</value>
  </property>
  <property>
    <name>fs.tos.session-token</name>
    <value>base-token</value>
  </property>
</configuration>)");

  const auto credentials = readTosCredentialsFile(path, "bucket");

  ASSERT_TRUE(credentials.has_value());
  EXPECT_EQ(credentials->accessKey, "base-ak");
  EXPECT_EQ(credentials->secretKey, "base-sk");
  EXPECT_EQ(credentials->sessionToken, "base-token");
}

TEST(TosCredentialsFileTest, BucketCredentialsOverrideBaseCredentials) {
  const auto path = writeCredentialsFile("bucket_override", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-sk</value>
  </property>
  <property>
    <name>fs.tos.session-token</name>
    <value>base-token</value>
  </property>
  <property>
    <name>fs.tos.bucket.test-bucket.access-key-id</name>
    <value>bucket-ak</value>
  </property>
  <property>
    <name>fs.tos.bucket.test-bucket.secret-access-key</name>
    <value>bucket-sk</value>
  </property>
</configuration>)");

  const auto credentials = readTosCredentialsFile(path, "test-bucket");

  ASSERT_TRUE(credentials.has_value());
  EXPECT_EQ(credentials->accessKey, "bucket-ak");
  EXPECT_EQ(credentials->secretKey, "bucket-sk");
  EXPECT_EQ(credentials->sessionToken, std::nullopt);
}

TEST(TosCredentialsFileTest, BucketAccessKeyWithoutSecretKeyFails) {
  const auto path = writeCredentialsFile("bucket_access_key_only", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-sk</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.access-key-id</name>
    <value>bucket-ak</value>
  </property>
</configuration>)");

  BOLT_ASSERT_THROW(
      readTosCredentialsFile(path, "bucket"),
      "bucket 'bucket' credentials must include non-empty access-key-id and secret-access-key");
}

TEST(TosCredentialsFileTest, BucketSecretKeyWithoutAccessKeyFails) {
  const auto path = writeCredentialsFile("bucket_secret_key_only", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-sk</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.secret-access-key</name>
    <value>bucket-sk</value>
  </property>
</configuration>)");

  BOLT_ASSERT_THROW(
      readTosCredentialsFile(path, "bucket"),
      "bucket 'bucket' credentials must include non-empty access-key-id and secret-access-key");
}

TEST(TosCredentialsFileTest, BucketSessionTokenWithoutKeysFails) {
  const auto path = writeCredentialsFile("bucket_token", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-ak</value>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-sk</value>
  </property>
  <property>
    <name>fs.tos.session-token</name>
    <value>base-token</value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.session-token</name>
    <value>bucket-token</value>
  </property>
</configuration>)");

  BOLT_ASSERT_THROW(
      readTosCredentialsFile(path, "bucket"),
      "bucket 'bucket' credentials must include non-empty access-key-id and secret-access-key");
}

TEST(TosCredentialsFileTest, BaseAccessKeyWithoutSecretKeyFails) {
  const auto path = writeCredentialsFile("base_access_key_only", R"(
<configuration>
  <property>
    <name>fs.tos.access-key-id</name>
    <value>base-ak</value>
  </property>
</configuration>)");

  BOLT_ASSERT_THROW(
      readTosCredentialsFile(path, "bucket"),
      "base credentials must include non-empty fs.tos.access-key-id and fs.tos.secret-access-key");
}

TEST(TosCredentialsFileTest, BaseSecretKeyWithoutAccessKeyFails) {
  const auto path = writeCredentialsFile("base_secret_key_only", R"(
<configuration>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value>base-sk</value>
  </property>
</configuration>)");

  BOLT_ASSERT_THROW(
      readTosCredentialsFile(path, "bucket"),
      "base credentials must include non-empty fs.tos.access-key-id and fs.tos.secret-access-key");
}

TEST(TosCredentialsFileTest, BaseSessionTokenWithoutKeysFails) {
  const auto path = writeCredentialsFile("base_token_only", R"(
<configuration>
  <property>
    <name>fs.tos.session-token</name>
    <value>base-token</value>
  </property>
</configuration>)");

  BOLT_ASSERT_THROW(
      readTosCredentialsFile(path, "bucket"),
      "base credentials must include non-empty fs.tos.access-key-id and fs.tos.secret-access-key");
}

TEST(TosCredentialsFileTest, EmptyBucketAccessKeyFails) {
  const auto path = writeCredentialsFile("empty_bucket_access_key", R"(
<configuration>
  <property>
    <name>fs.tos.bucket.bucket.access-key-id</name>
    <value>  </value>
  </property>
  <property>
    <name>fs.tos.bucket.bucket.secret-access-key</name>
    <value>bucket-sk</value>
  </property>
</configuration>)");

  BOLT_ASSERT_THROW(
      readTosCredentialsFile(path, "bucket"),
      "bucket 'bucket' credentials must include non-empty access-key-id and secret-access-key");
}

TEST(TosCredentialsFileTest, MissingCredentialsReturnNullopt) {
  const auto path = writeCredentialsFile("missing", R"(
<configuration>
  <property>
    <name>unrelated.key</name>
    <value>unrelated-value</value>
  </property>
</configuration>)");

  EXPECT_EQ(readTosCredentialsFile(path, "bucket"), std::nullopt);
}

} // namespace
} // namespace bytedance::bolt::filesystems
