/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <fcntl.h>

#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/common/testutil/TempFilePath.h"

#include "gtest/gtest.h"
using namespace bytedance::bolt;
using bytedance::bolt::common::Region;

constexpr int kOneMB = 1 << 20;

void writeDataAsync(WriteFile* writeFile) {
  if (writeFile->uringEnabled()) {
    std::vector<std::unique_ptr<folly::IOBuf>> buffers;
    buffers.push_back(folly::IOBuf::copyBuffer("aaaaa"));
    buffers.push_back(folly::IOBuf::copyBuffer("bbbbb"));
    buffers.push_back(folly::IOBuf::copyBuffer(std::string(kOneMB, 'c')));
    buffers.push_back(folly::IOBuf::copyBuffer("ddddd"));
    for (int i = 0; i < buffers.size(); i++)
      writeFile->submitWrite(buffers[i].get(), i);
    writeFile->waitForCompleteAll();
  } else {
    std::unique_ptr<folly::IOBuf> buf = folly::IOBuf::copyBuffer("aaaaa");
    buf->appendToChain(folly::IOBuf::copyBuffer("bbbbb"));
    buf->appendToChain(folly::IOBuf::copyBuffer(std::string(kOneMB, 'c')));
    buf->appendToChain(folly::IOBuf::copyBuffer("ddddd"));
    writeFile->append(std::move(buf));
  }
  ASSERT_EQ(writeFile->size(), 15 + kOneMB);
}

void readDataAsync(ReadFile* readFile, bool checkFileSize = true) {
  if (checkFileSize) {
    ASSERT_EQ(readFile->size(), 15 + kOneMB);
  }
  if (readFile->uringEnabled()) {
    char buffer1[5];
    readFile->submitRead(reinterpret_cast<char*>(&buffer1), 10 + kOneMB, 5);
    readFile->waitForComplete();
    ASSERT_EQ(std::string_view(buffer1, 5), "ddddd");
    char buffer2[10];
    readFile->submitRead(reinterpret_cast<char*>(&buffer2), 0, 10);
    readFile->waitForComplete();
    ASSERT_EQ(std::string_view(buffer2, 10), "aaaaabbbbb");
    char buffer3[kOneMB];
    readFile->submitRead(reinterpret_cast<char*>(&buffer3), 10, kOneMB);
    readFile->waitForComplete();
    ASSERT_EQ(std::string_view(buffer3, kOneMB), std::string(kOneMB, 'c'));
  } else {
    char buffer1[5];
    ASSERT_EQ(readFile->pread(10 + kOneMB, 5, &buffer1), "ddddd");
    char buffer2[10];
    ASSERT_EQ(readFile->pread(0, 10, &buffer2), "aaaaabbbbb");
    char buffer3[kOneMB];
    ASSERT_EQ(readFile->pread(10, kOneMB, &buffer3), std::string(kOneMB, 'c'));
  }
  if (checkFileSize) {
    ASSERT_EQ(readFile->size(), 15 + kOneMB);
  }
}

void readDataAsyncForWirteBuffers(
    ReadFile* readFile,
    bool checkFileSize = true) {
  if (checkFileSize) {
    ASSERT_EQ(readFile->size(), 150);
  }
  char buffer[150];
  readFile->submitRead(reinterpret_cast<char*>(&buffer), 0, 150);
  readFile->waitForComplete();
  std::string a;
  for (int i = 0; i < 150; i++)
    a += 'a' + i % 26;
  ASSERT_EQ(std::string_view(buffer, 150), a);
  if (checkFileSize) {
    ASSERT_EQ(readFile->size(), 150);
  }
}

TEST(AsyncLocalFile, writeAndRead) {
  auto tempFile = ::bytedance::bolt::test::TempFilePath::create();
  const auto& filename = tempFile->path.c_str();
  remove(filename);
  {
    AsyncLocalWriteFile writeFile(filename, false, true);
    writeDataAsync(&writeFile);
  }
  AsyncLocalReadFile readFile(filename);
  readDataAsync(&readFile);
}

TEST(AsyncLocalFile, viaRegistry) {
  filesystems::registerLocalFileSystem();
  auto tempFile = ::bytedance::bolt::test::TempFilePath::create();
  const auto& filename = tempFile->path.c_str();
  remove(filename);
  auto lfs = filesystems::getFileSystem(filename, nullptr);
  {
    auto writeFile = lfs->openAsyncFileForWrite(filename);
    writeDataAsync(writeFile.get());
  }
  auto readFile = lfs->openAsyncFileForRead(filename);
  readDataAsync(readFile.get());
  lfs->remove(filename);
}

TEST(AsyncLocalFileWrite, viaWriteBuffer) {
  filesystems::registerLocalFileSystem();
  auto tempFile = ::bytedance::bolt::test::TempFilePath::create();
  const auto& filename = tempFile->path.c_str();
  remove(filename);
  auto lfs = filesystems::getFileSystem(filename, nullptr);
  auto writeFile = lfs->openAsyncFileForWrite(filename);
  // test only when uring inits successfully
  if (writeFile->uringEnabled()) {
    WriteBuffers writeBuffers{64};
    std::vector<std::unique_ptr<folly::IOBuf>> buffers;
    for (int i = 0; i < 150; i++) {
      std::string content(1, i % 26 + 'a');
      std::unique_ptr<folly::IOBuf> buf = folly::IOBuf::copyBuffer(content);
      auto writeBuff = writeBuffers.add(buf, i, writeFile);
      writeFile->submitWrite(writeBuff, i);
    }
    writeBuffers.clear(writeFile);
    auto readFile = lfs->openAsyncFileForRead(filename);
    readDataAsyncForWirteBuffers(readFile.get());
  }
  lfs->remove(filename);
}
