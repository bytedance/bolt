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

#include "bolt/dwio/common/DirectBufferedInput.h"
#include <folly/Random.h>
#include <folly/container/F14Map.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "bolt/common/io/IoStatistics.h"
#include "bolt/common/memory/MmapAllocator.h"
#include "bolt/connectors/Connector.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/dwrf/common/Common.h"
#include "bolt/dwio/dwrf/test/TestReadFile.h"

#include <gtest/gtest.h>
#include <atomic>
#include <cstddef>
#include <limits>
#include <stdexcept>
using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::cache;

using bytedance::bolt::common::Region;

using memory::MemoryAllocator;
using IoStatisticsPtr = std::shared_ptr<IoStatistics>;

struct TestRegion {
  int32_t offset;
  int32_t length;
  bool read = true;
};

namespace {

class FailOnceReadFile final : public TestReadFile {
 public:
  enum class FailureMode {
    kThrow,
    kShortRead,
  };

  FailOnceReadFile(
      uint64_t seed,
      uint64_t length,
      std::shared_ptr<IoStatistics> ioStats,
      FailureMode failureMode)
      : TestReadFile(seed, length, std::move(ioStats)),
        failureMode_(failureMode) {}

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    if (readAttempts_++ > 0) {
      return TestReadFile::preadv(offset, buffers);
    }

    if (failureMode_ == FailureMode::kThrow) {
      throw std::runtime_error("injected read failure");
    }

    auto shortBuffers = buffers;
    BOLT_CHECK(!shortBuffers.empty());
    auto& last = shortBuffers.back();
    BOLT_CHECK_GT(last.size(), 0);
    last = {last.data(), last.size() - 1};
    return TestReadFile::preadv(offset, shortBuffers);
  }

  int32_t readAttempts() const {
    return readAttempts_;
  }

 private:
  const FailureMode failureMode_;
  mutable std::atomic<int32_t> readAttempts_{0};
};

} // namespace

class DirectBufferedInputTest : public testing::Test {
 protected:
  static constexpr int32_t kLoadQuantum = 8 << 20;
  static constexpr int32_t kCoalesceDistance = 512 << 10; // 512K
  static constexpr int32_t kPrefetchRowGroups = 1;

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    executor_ = std::make_unique<folly::IOThreadPoolExecutor>(10, 10);
    ioStats_ = std::make_shared<IoStatistics>();
    fileIoStats_ = std::make_shared<IoStatistics>();
    tracker_ = std::make_shared<cache::ScanTracker>("", nullptr, kLoadQuantum);
    file_ = std::make_shared<TestReadFile>(11, 100ULL << 20, fileIoStats_);
    opts_ = std::make_unique<dwio::common::ReaderOptions>(pool_.get());
    opts_->setPrefetchRowGroups(kPrefetchRowGroups);
    opts_->setMaxCoalesceDistance(kCoalesceDistance);
    opts_->setLoadQuantum(kLoadQuantum);
    asyncCtxPtr_ =
        std::make_shared<connector::AsyncThreadCtx>(1ULL << 30, true);
  }

  void TearDown() override {
    executor_->join();
  }

  std::unique_ptr<DirectBufferedInput> makeInput() {
    return std::make_unique<DirectBufferedInput>(
        file_,
        dwio::common::MetricsLog::voidLog(),
        1,
        tracker_,
        2,
        ioStats_,
        executor_.get(),
        *opts_,
        asyncCtxPtr_);
  }

  // Reads and checks the result of reading ''regions' and checks that this
  // causes 'numIos' accesses to the file.
  void testLoads(std::vector<TestRegion> regions, int32_t numIos) {
    auto previous = file_->numIos();
    auto input = makeInput();
    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    for (auto i = 0; i < regions.size(); ++i) {
      if (regions[i].length > 0) {
        Region region;
        region.offset = regions[i].offset;
        region.length = regions[i].length;
        StreamIdentifier si(i);
        streams.push_back(input->enqueue(region, &si));
      }
    }
    input->load(LogType::FILE);
    for (auto i = 0; i < regions.size(); ++i) {
      if (regions[i].read && regions[i].length > 0) {
        checkRead(streams[i].get(), regions[i]);
      }
    }
    EXPECT_EQ(numIos, file_->numIos() - previous);
  }

  // Marks the numStreams first streams as densely read. A large number of
  // references that all end in a read.
  void makeDense(int32_t numStreams) {
    for (auto i = 0; i < numStreams; ++i) {
      StreamIdentifier si(i);
      auto trackId = TrackingId(si.getId());
      for (auto counter = 0; counter < 100; ++counter) {
        tracker_->recordReference(trackId, 1000000, 1, 1);
        tracker_->recordRead(trackId, 1000000, 1, 1);
      }
    }
  }

  void checkRead(SeekableInputStream* stream, TestRegion region) {
    int32_t size;
    int32_t totalRead = 0;
    const void* buffer;
    while (stream->Next(&buffer, &size)) {
      file_->checkData(buffer, region.offset + totalRead, size);
      totalRead += size;
    }
    EXPECT_EQ(region.length, totalRead);
  }

  std::unique_ptr<dwio::common::ReaderOptions> opts_;
  std::shared_ptr<TestReadFile> file_;
  std::shared_ptr<cache::ScanTracker> tracker_;
  std::shared_ptr<IoStatistics> ioStats_;
  std::shared_ptr<IoStatistics> fileIoStats_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  std::shared_ptr<connector::AsyncThreadCtx> asyncCtxPtr_;
};

TEST_F(DirectBufferedInputTest, basic) {
  // The small leading parts coalesce with the the 7M.  The 2M goes standalone.
  // the last is read in 2 parts. This is because these are not yet densely
  // accessed and thus coalescing only works to load quantum of 8MB.
  testLoads(
      {{100, 100},
       {300, 100},
       {1000, 7000000},
       {7004000, 2000000},
       {20000000, 10000000}},
      4);

  // All but the last coalesce into one , the last is read in 2 parts. The
  // columns are now dense and coalesce goes up to 128MB if gaps are small
  // enough.
  testLoads(
      {{100, 100},
       {300, 100},
       {1000, 7000000},
       {7004000, 2000000},
       {20000000, 10000000}},
      3);

  // Mark the first 4 ranges as densely accessed.
  makeDense(4);

  // The first and first part of second coalesce.
  testLoads({{100, 100}, {1000, 10000000}}, 2);

  // The first is read in two parts, the tail of the first does not coalesce
  // with the second.
  testLoads({{1000, 10000000}, {10001000, 1000}}, 3);

  // One large standalone read in 2 parts.
  testLoads({{1000, 10000000}}, 2);

  // Small standalone read in 1 part.
  testLoads({{100, 100}}, 1);

  // Two small far apart
  testLoads({{100, 100}, {1000000, 100}}, 2);
  // The two coalesce because the first fits within load quantum + max coalesce
  // distance.
  testLoads({{1000, 8500000}, {8510000, 1000000}}, 1);

  // The two coalesce because the first fits within load quantum + max coalesce
  // distance. The tail of the second does not coalesce.
  testLoads({{1000, 8500000}, {8510000, 8400000}}, 2);

  // The first reads in 2 parts and does not coalesce to the second, which reads
  // in one part.
  testLoads({{1000, 9000000}, {9010000, 1000000}}, 3);
}

TEST_F(DirectBufferedInputTest, noRedownloadCoalescedPrefetch) {
  testLoads({{100, 100}, {201, 1, false}, {202, 100}}, 1);
  testLoads({{100, 100}, {201, 1, true}, {202, 100}}, 1);
}

TEST_F(DirectBufferedInputTest, enqueuePairSharesLockstepReads) {
  const int32_t quantum = 64 << 10;
  const int32_t length = 3 * quantum + 123;
  opts_->setLoadQuantum(quantum);
  const Region region{100, static_cast<uint64_t>(length)};
  auto previous = file_->numIos();
  auto input = makeInput();
  StreamIdentifier streamId(0);
  auto [first, second] = input->enqueuePair(region, &streamId);
  input->load(LogType::FILE);

  int32_t offset = 0;
  while (offset < length) {
    const void* firstData;
    const void* secondData;
    int32_t firstSize;
    int32_t secondSize;
    ASSERT_TRUE(first->Next(&firstData, &firstSize));
    ASSERT_TRUE(second->Next(&secondData, &secondSize));
    EXPECT_EQ(firstSize, secondSize);
    file_->checkData(firstData, region.offset + offset, firstSize);
    file_->checkData(secondData, region.offset + offset, secondSize);
    offset += firstSize;
  }

  EXPECT_EQ(4, file_->numIos() - previous);
}

TEST_F(DirectBufferedInputTest, enqueuePairSharesReadFailure) {
  constexpr int32_t kQuantum = 4 << 10;
  constexpr int32_t kLength = 2 * kQuantum;
  opts_->setLoadQuantum(kQuantum);

  for (const auto failureMode :
       {FailOnceReadFile::FailureMode::kThrow,
        FailOnceReadFile::FailureMode::kShortRead}) {
    SCOPED_TRACE(static_cast<int32_t>(failureMode));
    auto failOnceFile = std::make_shared<FailOnceReadFile>(
        11, kLength, fileIoStats_, failureMode);
    file_ = failOnceFile;
    auto input = makeInput();
    StreamIdentifier streamId(0);
    auto [first, second] = input->enqueuePair({0, kLength}, &streamId);

    const void* data;
    int32_t size;
    EXPECT_ANY_THROW(first->Next(&data, &size));
    EXPECT_ANY_THROW(second->Next(&data, &size));
    EXPECT_EQ(failOnceFile->readAttempts(), 1);
  }
}

TEST_F(DirectBufferedInputTest, enqueuePairBoundsMemory) {
  const int32_t quantum = 64 << 10;
  const int32_t length = 32 * quantum;
  opts_->setLoadQuantum(quantum);
  file_ = std::make_shared<TestReadFile>(11, length, fileIoStats_);
  auto pairPool = pool_;
  const auto baseline = pairPool->currentBytes();
  const auto chunkBytes =
      pairPool->preferredSize(quantum + AlignedBuffer::kPaddedSize);
  auto input = makeInput();
  StreamIdentifier streamId(0);
  auto [first, second] = input->enqueuePair({0, length}, &streamId);

  const void* data;
  int32_t size;
  int32_t offset = 0;
  while (first->Next(&data, &size)) {
    file_->checkData(data, offset, size);
    offset += size;
    EXPECT_LE(pairPool->currentBytes() - baseline, 3 * chunkBytes);
  }

  checkRead(second.get(), {0, length});
  EXPECT_LE(pairPool->currentBytes() - baseline, 3 * chunkBytes);
  EXPECT_LE(pairPool->peakBytes() - baseline, 4 * chunkBytes);

  first.reset();
  second.reset();
  input.reset();
  EXPECT_EQ(baseline, pairPool->currentBytes());
}

TEST_F(DirectBufferedInputTest, enqueuePairInterleavesIndependentCursors) {
  const int32_t quantum = 64 << 10;
  const int32_t length = 5 * quantum;
  opts_->setLoadQuantum(quantum);
  file_ = std::make_shared<TestReadFile>(11, length, fileIoStats_);
  auto input = makeInput();
  StreamIdentifier streamId(0);
  auto [first, second] = input->enqueuePair({0, length}, &streamId);

  const void* firstData;
  int32_t firstSize;
  ASSERT_TRUE(first->Next(&firstData, &firstSize));
  ASSERT_EQ(firstSize, quantum);
  const auto* retained = static_cast<const char*>(firstData);

  EXPECT_TRUE(second->SkipInt64(2 * quantum + 17));
  const void* secondData;
  int32_t secondSize;
  ASSERT_TRUE(second->Next(&secondData, &secondSize));
  file_->checkData(secondData, 2 * quantum + 17, secondSize);
  EXPECT_EQ(second->ByteCount(), 3 * quantum);

  EXPECT_TRUE(second->SkipInt64(quantum));
  ASSERT_TRUE(second->Next(&secondData, &secondSize));
  file_->checkData(secondData, 4 * quantum, secondSize);
  file_->checkData(retained, 0, firstSize);

  first->BackUp(23);
  EXPECT_EQ(first->ByteCount(), quantum - 23);
  ASSERT_TRUE(first->Next(&firstData, &firstSize));
  file_->checkData(firstData, quantum - 23, firstSize);

  const std::vector<uint64_t> positions{quantum + 11};
  PositionProvider position(positions);
  first->seekToPosition(position);
  ASSERT_TRUE(first->Next(&firstData, &firstSize));
  file_->checkData(firstData, quantum + 11, firstSize);
  EXPECT_EQ(first->positionSize(), 1);
  EXPECT_EQ(first->ByteCount(), 2 * quantum);
  EXPECT_EQ(file_->numIos(), 5);
}

TEST_F(DirectBufferedInputTest, enqueuePairSupportsLargeRegion) {
  constexpr uint64_t kLength =
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) + 1;
  file_ = std::make_shared<TestReadFile>(11, kLength, fileIoStats_);
  opts_->setLoadQuantum(64 << 10);
  auto input = makeInput();
  StreamIdentifier streamId(0);

  auto [first, second] = input->enqueuePair({0, kLength}, &streamId);

  EXPECT_TRUE(first->SkipInt64(kLength - 1));
  const void* data;
  int32_t size;
  ASSERT_TRUE(first->Next(&data, &size));
  ASSERT_EQ(size, 1);
  file_->checkData(data, kLength - 1, size);
  EXPECT_EQ(first->ByteCount(), kLength);
  EXPECT_EQ(second->ByteCount(), 0);
}

TEST_F(DirectBufferedInputTest, coalesedPrefetchOverlap) {
  testLoads({{100, 100}, {201, 1, false}, {201, 2, false}, {203, 100}}, 2);
  testLoads({{100, 100}, {201, 1, true}, {201, 2, true}, {203, 100}}, 2);
}

TEST_F(DirectBufferedInputTest, preload) {
  struct TestParam {
    uint64_t fileSize;
    int32_t offset;
    int32_t length;
  };
  std::vector<TestParam> testSettings = {
      {DirectBufferedInput::kTinySize - 100, 10, 100},
      {DirectBufferedInput::kTinySize + 1000, 1000, 1200},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.fileSize);
    file_ = std::make_shared<TestReadFile>(11, testData.fileSize, fileIoStats_);
    auto input = makeInput();

    EXPECT_FALSE(input->preloaded());
    EXPECT_FALSE(input->isBuffered(testData.offset, testData.length));
    const auto iosBeforePreload = file_->numIos();
    const auto rawBytesBeforePreload = ioStats_->rawBytesRead();
    const auto readBytesBeforePreload = ioStats_->read().sum();

    input->preload();

    EXPECT_TRUE(input->preloaded());
    EXPECT_TRUE(input->isBuffered(testData.offset, testData.length));
    EXPECT_EQ(file_->numIos() - iosBeforePreload, 1);
    EXPECT_EQ(
        ioStats_->rawBytesRead() - rawBytesBeforePreload, testData.fileSize);
    EXPECT_EQ(
        ioStats_->read().sum() - readBytesBeforePreload, testData.fileSize);

    auto fullFileStream = input->read(0, testData.fileSize, LogType::FILE);
    checkRead(
        fullFileStream.get(), {0, static_cast<int32_t>(testData.fileSize)});
    EXPECT_EQ(file_->numIos() - iosBeforePreload, 1);

    auto stream = input->enqueue(
        {static_cast<uint64_t>(testData.offset),
         static_cast<uint64_t>(testData.length)},
        nullptr);
    checkRead(stream.get(), {testData.offset, testData.length});
    EXPECT_EQ(file_->numIos() - iosBeforePreload, 1);

    auto [first, second] = input->enqueuePair(
        {static_cast<uint64_t>(testData.offset),
         static_cast<uint64_t>(testData.length)},
        nullptr);
    checkRead(first.get(), {testData.offset, testData.length});
    checkRead(second.get(), {testData.offset, testData.length});
    EXPECT_EQ(file_->numIos() - iosBeforePreload, 1);
  }
}

TEST_F(DirectBufferedInputTest, preloadCalledTwice) {
  file_ = std::make_shared<TestReadFile>(11, 1024, fileIoStats_);
  auto input = makeInput();

  input->preload();
  ASSERT_TRUE(input->preloaded());
  EXPECT_THROW(input->preload(), BoltException);
}

TEST_F(DirectBufferedInputTest, preloadAfterEnqueue) {
  file_ = std::make_shared<TestReadFile>(11, 1024, fileIoStats_);
  auto input = makeInput();

  input->enqueue({0, 100}, nullptr);
  EXPECT_THROW(input->preload(), BoltException);
}
