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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <paimon/defs.h>

#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/paimon/PaimonBoltFileSystem.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonDataSource.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include "paimon/fs/file_system_factory.h"

namespace bytedance::bolt::connector::paimon {
namespace {

class FakeFileSystem final : public filesystems::FileSystem {
 public:
  FakeFileSystem(
      std::shared_ptr<const config::ConfigBase> config,
      std::string name,
      int* renameCalls)
      : FileSystem(std::move(config)),
        name_(std::move(name)),
        renameCalls_(renameCalls) {}

  std::string name() const override {
    return name_;
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view,
      const filesystems::FileOptions&) override {
    return std::make_unique<InMemoryReadFile>(std::string_view{"fake"});
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view,
      const filesystems::FileOptions&) override {
    return nullptr;
  }

  void remove(std::string_view) override {}

  void rename(std::string_view, std::string_view, bool) override {
    ++*renameCalls_;
  }

  bool exists(std::string_view) override {
    return true;
  }

  bool isDirectory(std::string_view) const override {
    return false;
  }

  filesystems::FileInfo fileInfo(std::string_view) override {
    return {.isDirectory = false, .size = 4, .modificationTimeMs = 1'234'000};
  }

  std::vector<std::string> list(std::string_view) override {
    return {};
  }

  void mkdir(std::string_view) override {}

  void rmdir(std::string_view) override {}

 private:
  std::string name_;
  int* renameCalls_;
};

class PaimonFileSystemTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    filesystems::registerLocalFileSystem();
    EnsurePaimonBoltFileSystemRegistered();

    filesystems::registerFileSystem(
        [](std::string_view path) { return path.rfind("first://", 0) == 0; },
        [](std::shared_ptr<const config::ConfigBase> config, std::string_view) {
          firstConfig_ = config->rawConfigsCopy();
          return std::make_shared<FakeFileSystem>(
              std::move(config), "first", &firstRenameCalls_);
        });
    filesystems::registerFileSystem(
        [](std::string_view path) { return path.rfind("second://", 0) == 0; },
        [](std::shared_ptr<const config::ConfigBase> config, std::string_view) {
          return std::make_shared<FakeFileSystem>(
              std::move(config), "second", &secondRenameCalls_);
        });
  }

  static inline int firstRenameCalls_{0};
  static inline int secondRenameCalls_{0};
  static inline std::unordered_map<std::string, std::string> firstConfig_;
};

TEST_F(PaimonFileSystemTest, LocalRoundTripUsesBoltFileSystem) {
  auto temp = exec::test::TempDirectoryPath::create();
  const std::string dir = "file:" + temp->getPath() + "/dir";
  const std::string file = dir + "/data";
  const std::string renamed = dir + "/renamed";

  auto result = ::paimon::FileSystemFactory::Get("bolt", file, {});
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto fs = std::move(result).value();
  ASSERT_TRUE(fs->Mkdirs(dir).ok());
  auto output = fs->Create(file, true).value();
  ASSERT_TRUE(output->Write("bolt", 4).ok());
  ASSERT_TRUE(output->Close().ok());
  const auto status = fs->GetFileStatus(file);
  ASSERT_TRUE(status.ok()) << status.status().ToString();
  EXPECT_EQ(status.value()->GetLen(), 4);
  EXPECT_GT(status.value()->GetModificationTime(), 0);
  ASSERT_TRUE(fs->Rename(file, renamed).ok());
  ASSERT_TRUE(fs->Exists(renamed).value());
  ASSERT_TRUE(fs->Delete(dir, true).ok());
}

TEST_F(PaimonFileSystemTest, FileStatusUsesGenericFileInfo) {
  auto result = ::paimon::FileSystemFactory::Get("bolt", "first://file", {});
  ASSERT_TRUE(result.ok()) << result.status().ToString();

  auto status = result.value()->GetFileStatus("first://file");
  ASSERT_TRUE(status.ok()) << status.status().ToString();
  EXPECT_EQ(status.value()->GetLen(), 4);
  EXPECT_EQ(status.value()->GetModificationTime(), 1'234'000);

  std::vector<std::unique_ptr<::paimon::FileStatus>> statuses;
  ASSERT_TRUE(result.value()->ListFileStatus("first://file", &statuses).ok());
  ASSERT_EQ(statuses.size(), 1);
  EXPECT_EQ(statuses.front()->GetModificationTime(), 1'234'000);
}

TEST_F(PaimonFileSystemTest, ConnectorOptionsReachRegisteredFileSystem) {
  firstConfig_.clear();
  const auto connectorConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {"test.fs.endpoint", "session-endpoint"},
          {"test.fs.credential", "session-credential"},
          {::paimon::Options::FILE_SYSTEM, "not-bolt"}});
  const PaimonConfig paimonConfig(connectorConfig);
  const auto options = resolvePaimonDataSourceOptions(
      {{"test.fs.endpoint", "table-endpoint"}},
      core::QueryConfig({}),
      paimonConfig);

  auto result =
      ::paimon::FileSystemFactory::Get("bolt", "first://file", options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_TRUE(result.value()->Exists("first://file").value());

  EXPECT_EQ(firstConfig_.at("test.fs.endpoint"), "table-endpoint");
  EXPECT_EQ(firstConfig_.at("test.fs.credential"), "session-credential");
  EXPECT_EQ(firstConfig_.at(::paimon::Options::FILE_SYSTEM), "bolt");
}

TEST_F(PaimonFileSystemTest, RenameRejectsDifferentBoltFilesystems) {
  auto result = ::paimon::FileSystemFactory::Get("bolt", "first://a", {});
  ASSERT_TRUE(result.ok()) << result.status().ToString();

  const auto status = result.value()->Rename("first://a", "second://b");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), ::paimon::StatusCode::Invalid);
  EXPECT_EQ(firstRenameCalls_, 0);
  EXPECT_EQ(secondRenameCalls_, 0);
}

TEST_F(PaimonFileSystemTest, RenameRejectsDifferentHdfsAuthorities) {
  PaimonBoltFileSystem fs({});

  const auto status =
      fs.Rename("hdfs://cluster-a/path", "hdfs://cluster-b/path");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), ::paimon::StatusCode::Invalid);
}

TEST_F(PaimonFileSystemTest, UnregisteredSchemeReturnsIoErrorWithPath) {
  auto result =
      ::paimon::FileSystemFactory::Get("bolt", "unregistered://missing", {});
  ASSERT_TRUE(result.ok()) << result.status().ToString();

  const auto exists = result.value()->Exists("unregistered://missing");
  ASSERT_FALSE(exists.ok());
  EXPECT_EQ(exists.status().code(), ::paimon::StatusCode::IOError);
  EXPECT_NE(
      exists.status().ToString().find("unregistered://missing"),
      std::string::npos);
  EXPECT_NE(
      exists.status().ToString().find("No registered file system"),
      std::string::npos);
}

} // namespace
} // namespace bytedance::bolt::connector::paimon
