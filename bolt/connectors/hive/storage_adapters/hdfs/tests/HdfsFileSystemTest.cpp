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

#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include <boost/format.hpp>
#include <common/memory/Allocation.h>
#include <common/memory/Memory.h>
#include <gmock/gmock-matchers.h>
#include <hdfs/hdfs.h>
#include <atomic>
#include <random>
#include "HdfsMiniCluster.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsReadFile.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/tests/utils/TempFilePath.h"
#include "gtest/gtest.h"
using namespace bytedance::bolt;

constexpr int kOneMB = 1 << 20;
static const std::string destinationPath = "/test_file.txt";
static const std::string hdfsPort = "7878";
static const std::string localhost = "localhost";
static const std::string fullDestinationPath =
    "hdfs://" + localhost + ":" + hdfsPort + destinationPath;
static const std::string simpleDestinationPath = "hdfs:///" + destinationPath;
static const std::unordered_map<std::string, std::string> configurationValues(
    {{"hive.hdfs.host", localhost}, {"hive.hdfs.port", hdfsPort}});
static const std::map<std::string, std::string> config;
static const filesystems::HdfsServiceEndpoint endpoint(localhost, hdfsPort);

class HdfsFileSystemTest : public testing::Test {
 public:
  static const size_t BUFFER_SIZE = 8 * 1024 * 1024;
  static void SetUpTestSuite() {
    if (miniCluster == nullptr) {
      miniCluster = std::make_shared<filesystems::test::HdfsMiniCluster>();
      miniCluster->start();
      auto tempFile = createFile();
      miniCluster->addFile(tempFile->path, destinationPath);
    }
  }

  void SetUp() override {
    if (!miniCluster->isRunning()) {
      miniCluster->start();
    }

    pool_ = memory::MemoryManager().addLeafPool();
  }

  static void TearDownTestSuite() {
    for (const auto& [_, filesystem] :
         bytedance::bolt::filesystems::registeredFilesystems) {
      filesystem->close();
    }

    miniCluster->stop();
  }
  static std::atomic<bool> startThreads;
  static std::shared_ptr<filesystems::test::HdfsMiniCluster> miniCluster;

 private:
  static std::shared_ptr<::exec::test::TempFilePath> createFile() {
    auto tempFile = ::exec::test::TempFilePath::create();
    tempFile->append("aaaaa");
    tempFile->append("bbbbb");
    tempFile->append(std::string(kOneMB, 'c'));
    tempFile->append("ddddd");
    return tempFile;
  }

 protected:
  std::shared_ptr<memory::MemoryPool> pool_;
};

std::shared_ptr<filesystems::test::HdfsMiniCluster>
    HdfsFileSystemTest::miniCluster = nullptr;
std::atomic<bool> HdfsFileSystemTest::startThreads = false;

void readData(ReadFile* readFile) {
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer1[5];
  ASSERT_EQ(readFile->pread(10 + kOneMB, 5, &buffer1), "ddddd");
  char buffer2[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer2), "aaaaabbbbb");
  auto buffer3 = new char[kOneMB];
  ASSERT_EQ(readFile->pread(10, kOneMB, buffer3), std::string(kOneMB, 'c'));
  delete[] buffer3;
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer4[10];
  const std::string_view arf = readFile->pread(5, 10, &buffer4);
  const std::string zarf = readFile->pread(kOneMB, 15);
  auto buf = std::make_unique<char[]>(8);
  const std::string_view warf = readFile->pread(4, 8, buf.get());
  const std::string_view warfFromBuf(buf.get(), 8);
  ASSERT_EQ(arf, "bbbbbccccc");
  ASSERT_EQ(zarf, "ccccccccccddddd");
  ASSERT_EQ(warf, "abbbbbcc");
  ASSERT_EQ(warfFromBuf, "abbbbbcc");
}

std::unique_ptr<WriteFile> openFileForWrite(std::string_view path) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  std::string hdfsFilePath =
      "hdfs://" + localhost + ":" + hdfsPort + std::string(path);
  auto hdfsFileSystem = filesystems::getFileSystem(hdfsFilePath, config);
  return hdfsFileSystem->openFileForWrite(path);
}

void checkReadErrorMessages(
    ReadFile* readFile,
    std::string errorMessage,
    int endpoint) {
  try {
    readFile->pread(10 + kOneMB, endpoint);
    FAIL() << "expected BoltException";
  } catch (BoltException const& error) {
    EXPECT_THAT(error.message(), testing::HasSubstr(errorMessage));
  }
  try {
    auto buf = std::make_unique<char[]>(8);
    readFile->pread(10 + kOneMB, endpoint, buf.get());
    FAIL() << "expected BoltException";
  } catch (BoltException const& error) {
    EXPECT_THAT(error.message(), testing::HasSubstr(errorMessage));
  }
}

TEST_F(HdfsFileSystemTest, DISABLED_viaFileSystem) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);
  auto readFile = hdfsFileSystem->openFileForRead(fullDestinationPath);
  readData(readFile.get());
}

TEST_F(HdfsFileSystemTest, DISABLED_initializeFsWithEndpointInfoInFilePath) {
  // Without host/port configured.
  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);
  auto readFile = hdfsFileSystem->openFileForRead(fullDestinationPath);
  readData(readFile.get());

  // Wrong endpoint info specified in hdfs file path.
  const std::string wrongFullDestinationPath =
      "hdfs://not_exist_host:" + hdfsPort + destinationPath;
  BOLT_ASSERT_THROW(
      filesystems::getFileSystem(wrongFullDestinationPath, config),
      "Unable to connect to HDFS");
}

TEST_F(HdfsFileSystemTest, DISABLED_fallbackToUseConfig) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem =
      filesystems::getFileSystem(simpleDestinationPath, config);
  auto readFile = hdfsFileSystem->openFileForRead(simpleDestinationPath);
  readData(readFile.get());
}

TEST_F(HdfsFileSystemTest, DISABLED_oneFsInstanceForOneEndpoint) {
  auto hdfsFileSystem1 =
      filesystems::getFileSystem(fullDestinationPath, nullptr);
  auto hdfsFileSystem2 =
      filesystems::getFileSystem(fullDestinationPath, nullptr);
  ASSERT_TRUE(hdfsFileSystem1 == hdfsFileSystem2);
}

TEST_F(HdfsFileSystemTest, DISABLED_missingFileViaFileSystem) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);
  BOLT_ASSERT_RUNTIME_THROW(
      hdfsFileSystem->openFileForRead(
          "hdfs://localhost:7777/path/that/does/not/exist"),
      "Unable to get file path info for file: /path/that/does/not/exist. got error: FileNotFoundException: Path /path/that/does/not/exist does not exist.");
}

TEST_F(HdfsFileSystemTest, DISABLED_missingHost) {
  try {
    std::unordered_map<std::string, std::string> missingHostConfiguration(
        {{"hive.hdfs.port", hdfsPort}});
    auto config = std::make_shared<const config::ConfigBase>(
        std::move(missingHostConfiguration));
    filesystems::HdfsFileSystem hdfsFileSystem(
        config,
        filesystems::HdfsFileSystem::getServiceEndpoint(
            simpleDestinationPath, config.get()));
    FAIL() << "expected BoltException";
  } catch (BoltException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "hdfsHost is empty, configuration missing for hdfs host"));
  }
}

TEST_F(HdfsFileSystemTest, DISABLED_missingPort) {
  try {
    std::unordered_map<std::string, std::string> missingPortConfiguration(
        {{"hive.hdfs.host", localhost}});
    auto config = std::make_shared<const config::ConfigBase>(
        std::move(missingPortConfiguration));
    filesystems::HdfsFileSystem hdfsFileSystem(
        config,
        filesystems::HdfsFileSystem::getServiceEndpoint(
            simpleDestinationPath, config.get()));
    FAIL() << "expected BoltException";
  } catch (BoltException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "hdfsPort is empty, configuration missing for hdfs port"));
  }
}

TEST_F(HdfsFileSystemTest, DISABLED_schemeMatching) {
  try {
    auto fs = std::dynamic_pointer_cast<filesystems::HdfsFileSystem>(
        filesystems::getFileSystem("/", nullptr));
    FAIL() << "expected BoltException";
  } catch (BoltException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "No registered file system matched with file path '/'"));
  }
  auto fs = std::dynamic_pointer_cast<filesystems::HdfsFileSystem>(
      filesystems::getFileSystem(fullDestinationPath, nullptr));
  ASSERT_TRUE(fs->isHdfsFile(fullDestinationPath));
}

TEST_F(HdfsFileSystemTest, DISABLED_writeNotSupported) {
  try {
    auto config = std::make_shared<const config::ConfigBase>(
        std::unordered_map<std::string, std::string>(configurationValues));
    auto hdfsFileSystem =
        filesystems::getFileSystem(fullDestinationPath, config);
    hdfsFileSystem->openFileForWrite("/path");
  } catch (BoltException const& error) {
    EXPECT_EQ(error.message(), "Write to HDFS is unsupported");
  }
}

TEST_F(HdfsFileSystemTest, DISABLED_removeNotSupported) {
  try {
    auto config = std::make_shared<const config::ConfigBase>(
        std::unordered_map<std::string, std::string>(configurationValues));
    auto hdfsFileSystem =
        filesystems::getFileSystem(fullDestinationPath, config);
    hdfsFileSystem->remove("/path");
  } catch (BoltException const& error) {
    EXPECT_EQ(error.message(), "Does not support removing files from hdfs");
  }
}

TEST_F(HdfsFileSystemTest, DISABLED_multipleThreadsWithFileSystem) {
  startThreads = false;
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);

  std::vector<std::thread> threads;
  std::mt19937 generator(std::random_device{}());
  std::vector<int> sleepTimesInMicroseconds = {0, 500, 50000};
  std::uniform_int_distribution<std::size_t> distribution(
      0, sleepTimesInMicroseconds.size() - 1);
  for (int i = 0; i < 25; i++) {
    auto thread = std::thread([&hdfsFileSystem,
                               &distribution,
                               &generator,
                               &sleepTimesInMicroseconds] {
      int index = distribution(generator);
      while (!HdfsFileSystemTest::startThreads) {
        std::this_thread::yield();
      }
      std::this_thread::sleep_for(
          std::chrono::microseconds(sleepTimesInMicroseconds[index]));
      auto readFile = hdfsFileSystem->openFileForRead(fullDestinationPath);
      readData(readFile.get());
    });
    threads.emplace_back(std::move(thread));
  }
  startThreads = true;
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(HdfsFileSystemTest, DISABLED_write) {
  std::string path = "/a.txt";
  auto writeFile = openFileForWrite(path);
  std::string data = "abcdefghijk";
  writeFile->append(data);
  writeFile->flush();
  ASSERT_EQ(writeFile->size(), 0);
  writeFile->append(data);
  writeFile->append(data);
  writeFile->flush();
  writeFile->close();
  ASSERT_EQ(writeFile->size(), data.size() * 3);
}

TEST_F(HdfsFileSystemTest, DISABLED_missingFileForWrite) {
  const std::string filePath = "hdfs://localhost:7777/path/that/does/not/exist";
  const std::string errorMsg =
      "Failed to open hdfs file: hdfs://localhost:7777/path/that/does/not/exist";
  BOLT_ASSERT_THROW(openFileForWrite(filePath), errorMsg);
}

TEST_F(HdfsFileSystemTest, DISABLED_writeDataFailures) {
  auto writeFile = openFileForWrite("/a.txt");
  writeFile->close();
  BOLT_ASSERT_THROW(
      writeFile->append("abcde"),
      "Cannot append to HDFS file because file handle is null, file path: /a.txt");
}

TEST_F(HdfsFileSystemTest, DISABLED_writeFlushFailures) {
  auto writeFile = openFileForWrite("/a.txt");
  writeFile->close();
  BOLT_ASSERT_THROW(
      writeFile->flush(),
      "Cannot flush HDFS file because file handle is null, file path: /a.txt");
}

TEST_F(HdfsFileSystemTest, DISABLED_writeWithParentDirNotExist) {
  std::string path = "/parent/directory/that/does/not/exist/a.txt";
  auto writeFile = openFileForWrite(path);
  std::string data = "abcdefghijk";
  writeFile->append(data);
  writeFile->flush();
  ASSERT_EQ(writeFile->size(), 0);
  writeFile->append(data);
  writeFile->append(data);
  writeFile->flush();
  writeFile->close();
  ASSERT_EQ(writeFile->size(), data.size() * 3);
}
