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

#ifdef BOLT_ENABLE_GCS
#include "bolt/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h"
#include "bolt/connectors/hive/storage_adapters/gcs/tests/GcsEmulator.h"
#endif

namespace bytedance::bolt::connector::paimon {
namespace {

class FakeFileSystem : public filesystems::FileSystem {
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
      const filesystems::FileOptions& options) override {
    readOptions = options;
    return std::make_unique<InMemoryReadFile>(std::string_view{"fake"});
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view,
      const filesystems::FileOptions& options) override {
    writeOptions = options;
    return std::make_unique<InMemoryWriteFile>(&output);
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

  static inline filesystems::FileOptions readOptions;
  static inline filesystems::FileOptions writeOptions;
  static inline std::string output;

 private:
  std::string name_;
  int* renameCalls_;
};

class NonListingFileSystem final : public FakeFileSystem {
 public:
  using FakeFileSystem::FakeFileSystem;

  filesystems::FileInfo fileInfo(std::string_view) override {
    return {.isDirectory = true};
  }

  std::vector<std::string> list(std::string_view) override {
    BOLT_FAIL("Directory listing is unavailable");
  }

  void remove(std::string_view path) override {
    const auto localPath =
        path.substr(std::string_view{"non-listing://"}.size());
    filesystems::getFileSystem(localPath, nullptr)->remove(localPath);
  }

  void rmdir(std::string_view) override {
    BOLT_FAIL("Recursive deletion must not be used");
  }
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
    filesystems::registerFileSystem(
        [](std::string_view path) {
          return path.rfind("non-listing://", 0) == 0;
        },
        [](std::shared_ptr<const config::ConfigBase> config, std::string_view) {
          return std::make_shared<NonListingFileSystem>(
              std::move(config), "non-listing", &firstRenameCalls_);
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

TEST_F(PaimonFileSystemTest, ReaderOptionsRespectTableAndQueryPrecedence) {
  const PaimonConfig config(std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {PaimonConfig::kNaturalReadSize, "1024"},
          {PaimonConfig::kCoalesceReads, "true"},
          {PaimonConfig::kReadTimestampUnit, "3"}}));
  const std::unordered_map<std::string, std::string> tableProperties{
      {PaimonConfig::kNaturalReadSize, "2048"},
      {PaimonConfig::kCoalesceReads, "false"},
      {PaimonConfig::kReadTimestampUnit, "6"}};
  auto options = resolvePaimonDataSourceOptions(
      tableProperties, core::QueryConfig({}), config);
  EXPECT_EQ(options.at(PaimonConfig::kNaturalReadSize), "2048");
  EXPECT_EQ(options.at(PaimonConfig::kCoalesceReads), "false");
  EXPECT_EQ(options.at(PaimonConfig::kReadTimestampUnit), "6");

  options = resolvePaimonDataSourceOptions(
      tableProperties,
      core::QueryConfig(std::unordered_map<std::string, std::string>{
          {PaimonConfig::kNaturalReadSize, "4096"}}),
      config);
  EXPECT_EQ(options.at(PaimonConfig::kNaturalReadSize), "4096");
  EXPECT_EQ(options.at(PaimonConfig::kCoalesceReads), "false");
  EXPECT_EQ(options.at(PaimonConfig::kReadTimestampUnit), "6");

  options = resolvePaimonDataSourceOptions({}, core::QueryConfig({}), config);
  EXPECT_EQ(options.at(PaimonConfig::kNaturalReadSize), "1024");
  EXPECT_EQ(options.at(PaimonConfig::kCoalesceReads), "true");
  EXPECT_EQ(options.at(PaimonConfig::kReadTimestampUnit), "3");
}

TEST_F(PaimonFileSystemTest, OpenOptionsReachReadAndWriteFiles) {
  const std::map<std::string, std::string> options{
      {"bolt.io.file.buffer.size", "4096"},
      {"bolt.dfs.replication", "2"},
      {"bolt.dfs.blocksize", "1048576"}};
  PaimonBoltFileSystem fs(options);
  FakeFileSystem::readOptions = {};
  FakeFileSystem::writeOptions = {};
  auto input = fs.Open("first://file");
  ASSERT_TRUE(input.ok()) << input.status().ToString();
  auto output = fs.Create("first://file", true);
  ASSERT_TRUE(output.ok()) << output.status().ToString();
  ASSERT_TRUE(output.value()->Close().ok());
  for (const auto& [key, value] : options) {
    EXPECT_EQ(FakeFileSystem::readOptions.values[key], value);
    EXPECT_EQ(FakeFileSystem::writeOptions.values[key], value);
  }
  EXPECT_TRUE(FakeFileSystem::writeOptions.shouldCreateParentDirectories);
  EXPECT_FALSE(FakeFileSystem::writeOptions.shouldThrowOnFileAlreadyExists);
}

TEST_F(PaimonFileSystemTest, QueryOverridesMalformedTableReaderOptions) {
  const PaimonConfig config(std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{}));
  const auto options = resolvePaimonDataSourceOptions(
      {{PaimonConfig::kNaturalReadSize, "invalid"},
       {PaimonConfig::kCoalesceReads, "invalid"},
       {PaimonConfig::kReadTimestampUnit, "invalid"}},
      core::QueryConfig(
          {{PaimonConfig::kNaturalReadSize, "4096"},
           {PaimonConfig::kCoalesceReads, "false"},
           {PaimonConfig::kReadTimestampUnit, "6"}}),
      config);
  EXPECT_EQ(options.at(PaimonConfig::kNaturalReadSize), "4096");
  EXPECT_EQ(options.at(PaimonConfig::kCoalesceReads), "false");
  EXPECT_EQ(options.at(PaimonConfig::kReadTimestampUnit), "6");
}

TEST_F(PaimonFileSystemTest, TableOverridesMalformedConnectorReaderOptions) {
  const PaimonConfig config(std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {PaimonConfig::kNaturalReadSize, "invalid"},
          {PaimonConfig::kCoalesceReads, "invalid"},
          {PaimonConfig::kReadTimestampUnit, "invalid"}}));
  const auto options = resolvePaimonDataSourceOptions(
      {{PaimonConfig::kNaturalReadSize, "4096"},
       {PaimonConfig::kCoalesceReads, "false"},
       {PaimonConfig::kReadTimestampUnit, "6"}},
      core::QueryConfig({}),
      config);
  EXPECT_EQ(options.at(PaimonConfig::kNaturalReadSize), "4096");
  EXPECT_EQ(options.at(PaimonConfig::kCoalesceReads), "false");
  EXPECT_EQ(options.at(PaimonConfig::kReadTimestampUnit), "6");
}

TEST_F(PaimonFileSystemTest, NonRecursiveDeleteAllowsOnlyEmptyDirectories) {
  auto temp = exec::test::TempDirectoryPath::create();
  const auto dir = temp->getPath() + "/dir";
  const auto file = dir + "/data";
  PaimonBoltFileSystem fs({});
  ASSERT_TRUE(fs.Mkdirs(dir).ok());
  ASSERT_TRUE(fs.Delete(dir, false).ok());
  EXPECT_FALSE(fs.Exists(dir).value());

  ASSERT_TRUE(fs.Mkdirs(dir).ok());
  auto output = fs.Create(file, false);
  ASSERT_TRUE(output.ok());
  ASSERT_TRUE(output.value()->Close().ok());
  EXPECT_FALSE(fs.Delete(dir, false).ok());
  EXPECT_TRUE(fs.Exists(file).value());
  ASSERT_TRUE(fs.Delete(file, false).ok());
  ASSERT_TRUE(fs.Delete(dir, false).ok());
}

TEST_F(PaimonFileSystemTest, NonRecursiveDeleteDoesNotRequireListing) {
  auto temp = exec::test::TempDirectoryPath::create();
  PaimonBoltFileSystem fs({});
  EXPECT_TRUE(fs.Delete("non-listing://" + temp->getPath(), false).ok());
  EXPECT_FALSE(filesystems::getFileSystem(temp->getPath(), nullptr)
                   ->exists(temp->getPath()));
}

#ifdef BOLT_ENABLE_GCS
void checkGcsDirectoryDeletionIsRejected(bool recursive) {
  filesystems::GcsEmulator emulator;
  emulator.bootstrap();
  filesystems::registerGcsFileSystem();
  const auto config = emulator.hiveConfig()->rawConfigsCopy();
  PaimonBoltFileSystem fs(
      std::map<std::string, std::string>(config.begin(), config.end()));
  const auto root = gcsURI(emulator.preexistingBucketName(), "");
  auto backend = filesystems::getFileSystem(root, emulator.hiveConfig());
  ASSERT_TRUE(fs.Mkdirs(root + "dir/").ok());
  for (const auto* key : {"dir/data", "virtual/data", "sibling"}) {
    auto output = backend->openFileForWrite(root + key);
    output->append("bolt");
    output->close();
  }

  for (const auto* key : {"dir/", "dir", "virtual", "virtual/", ""}) {
    SCOPED_TRACE(key);
    const auto status = fs.Delete(root + key, recursive);
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
    for (const auto* retained :
         {"dir/", "dir/data", "virtual/data", "sibling"}) {
      const auto info = fs.GetFileStatus(root + retained);
      EXPECT_TRUE(info.ok()) << info.status().ToString();
    }
    // fileInfo can synthesize a directory from its children even if its
    // marker was deleted, so also check that the exact marker still exists.
    EXPECT_NO_THROW(backend->openFileForRead(root + "dir/"));
  }

  // Rejecting directory deletion must not prevent ordinary file deletion.
  ASSERT_TRUE(fs.Delete(root + "dir/data", recursive).ok());
  EXPECT_FALSE(fs.GetFileStatus(root + "dir/data").ok());
  EXPECT_TRUE(fs.GetFileStatus(root + "sibling").ok());
}

TEST_F(PaimonFileSystemTest, GcsRecursiveDirectoryDeletePreservesBucket) {
  checkGcsDirectoryDeletionIsRejected(true);
}

TEST_F(PaimonFileSystemTest, GcsNonRecursiveDirectoryDeletePreservesChildren) {
  checkGcsDirectoryDeletionIsRejected(false);
}
#endif

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
