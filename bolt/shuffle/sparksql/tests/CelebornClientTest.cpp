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

#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/memory_pool.h>
#include <gtest/gtest.h>

#include <celeborn/client/ShuffleClient.h>
#include <celeborn/client/reader/CelebornInputStream.h>
#include <celeborn/conf/CelebornConf.h>
#include <celeborn/memory/ByteBuffer.h>
#include <celeborn/network/TransportClient.h>
#include <celeborn/proto/TransportMessagesCpp.pb.h>
#include <celeborn/protocol/PartitionLocation.h>
#include <celeborn/protocol/TransportMessage.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/CelebornReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/NativeCelebornClient.h"
#include "bolt/shuffle/sparksql/tests/CelebornTestUtils.h"

namespace bytedance::bolt::shuffle::sparksql::test {
std::shared_ptr<const celeborn::protocol::PartitionLocation> makeLocation(
    int partitionId) {
  auto location = std::make_shared<celeborn::protocol::PartitionLocation>();
  location->id = partitionId;
  location->epoch = 0;
  location->host = "127.0.0.1";
  location->fetchPort = 1;
  location->rpcPort = 1;
  location->pushPort = 1;
  location->replicatePort = 1;
  location->mode = celeborn::protocol::PartitionLocation::PRIMARY;
  auto storage = std::make_unique<celeborn::protocol::StorageInfo>();
  storage->type = celeborn::protocol::StorageInfo::HDD;
  location->storageInfo = std::move(storage);
  return location;
}

std::unique_ptr<celeborn::memory::ReadOnlyByteBuffer> buildChunkPayload(
    const std::vector<uint8_t>& payload) {
  auto buffer = celeborn::memory::ByteBuffer::createWriteOnly(
      sizeof(int32_t) * 4 + payload.size(), false);
  buffer->write<int32_t>(0);
  buffer->write<int32_t>(0);
  buffer->write<int32_t>(0);
  buffer->write<int32_t>(static_cast<int32_t>(payload.size()));
  if (!payload.empty()) {
    buffer->writeFromBuffer(payload.data(), 0, payload.size());
  }
  std::unique_ptr<celeborn::memory::ByteBuffer> owned = std::move(buffer);
  return celeborn::memory::ByteBuffer::toReadOnly(std::move(owned));
}

// Identifies a single map output for a shuffle/partition/attempt/map tuple.
struct MapKey {
  int shuffleId;
  int partitionId;
  int attemptId;
  int mapId;

  bool operator<(const MapKey& other) const {
    if (shuffleId != other.shuffleId) {
      return shuffleId < other.shuffleId;
    }
    if (partitionId != other.partitionId) {
      return partitionId < other.partitionId;
    }
    if (attemptId != other.attemptId) {
      return attemptId < other.attemptId;
    }
    return mapId < other.mapId;
  }
};

void readStreamAndExpect(
    const std::shared_ptr<arrow::io::InputStream>& stream,
    const std::string& payload) {
  auto readResult = stream->Read(payload.size());
  ASSERT_TRUE(readResult.ok());
  auto buffer = readResult.ValueOrDie();
  ASSERT_NE(buffer, nullptr);
  ASSERT_EQ(buffer->size(), static_cast<int64_t>(payload.size()));
  std::string readBack(
      reinterpret_cast<const char*>(buffer->data()), buffer->size());
  EXPECT_EQ(readBack, payload);
}

class FakeTransportClient final : public celeborn::network::TransportClient {
 public:
  explicit FakeTransportClient(std::vector<uint8_t> payload)
      : celeborn::network::TransportClient(
            nullptr,
            nullptr,
            std::chrono::milliseconds(0)),
        payload_(std::move(payload)) {}

  celeborn::network::RpcResponse sendRpcRequestSync(
      const celeborn::network::RpcRequest& request,
      celeborn::Timeout) override {
    PbStreamHandler pb;
    pb.set_streamid(1);
    pb.set_numchunks(1);
    pb.add_chunkoffsets(0);
    pb.set_fullpath("test");
    auto transportMessage = celeborn::protocol::TransportMessage(
        ::STREAM_HANDLER, pb.SerializeAsString());
    return celeborn::network::RpcResponse(
        request.requestId(), transportMessage.toReadOnlyByteBuffer());
  }

  void sendRpcRequestWithoutResponse(
      const celeborn::network::RpcRequest&) override {}

  void fetchChunkAsync(
      const celeborn::protocol::StreamChunkSlice& streamChunkSlice,
      const celeborn::network::RpcRequest&,
      celeborn::network::FetchChunkSuccessCallback onSuccess,
      celeborn::network::FetchChunkFailureCallback) override {
    onSuccess(streamChunkSlice, buildChunkPayload(payload_));
  }

 private:
  std::vector<uint8_t> payload_;
};

class FakeTransportClientFactory final
    : public celeborn::network::TransportClientFactory {
 public:
  explicit FakeTransportClientFactory(
      const std::shared_ptr<const celeborn::conf::CelebornConf>& conf)
      : celeborn::network::TransportClientFactory(conf) {}

  void setPayload(std::vector<uint8_t> payload) {
    payload_ = std::move(payload);
  }

  std::shared_ptr<celeborn::network::TransportClient> createClient(
      const std::string&,
      uint16_t) override {
    return std::make_shared<FakeTransportClient>(payload_);
  }

 private:
  std::vector<uint8_t> payload_;
};

class FakeShuffleClient final : public celeborn::client::ShuffleClient {
 public:
  FakeShuffleClient()
      : conf_(std::make_shared<celeborn::conf::CelebornConf>()) {}

  void setupLifecycleManagerRef(std::string&, int) override {}

  void setupLifecycleManagerRef(
      std::shared_ptr<celeborn::network::NettyRpcEndpointRef>&) override {}

  // Store each push as its own chunk to preserve multi-push semantics.
  int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions) override {
    if (!hasKnownCounts_) {
      knownNumMappers_ = numMappers;
      knownNumPartitions_ = numPartitions;
      hasKnownCounts_ = true;
    } else {
      BOLT_CHECK(
          knownNumMappers_ == numMappers &&
              knownNumPartitions_ == numPartitions,
          "Inconsistent mapper/partition counts for pushData");
    }
    if (data && length > 0) {
      const MapKey mapKey{shuffleId, partitionId, attemptId, mapId};
      auto& chunks = payloadByMapKey_[mapKey];
      chunks.emplace_back(data + offset, data + offset + length);
    }
    return 0;
  }

  void mapperEnd(int, int, int, int) override {}

  void cleanup(int, int, int) override {}

  void updateReducerFileGroup(int) override {}

  // Read a partition by stitching map outputs for the requested map range.
  std::unique_ptr<celeborn::client::CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) override {
    return readPartitionImpl(
        shuffleId,
        partitionId,
        attemptNumber,
        startMapIndex,
        endMapIndex,
        false);
  }

  // Overload that carries the compression flag through to the stream.
  std::unique_ptr<celeborn::client::CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      bool needCompression) override {
    return readPartitionImpl(
        shuffleId,
        partitionId,
        attemptNumber,
        startMapIndex,
        endMapIndex,
        needCompression);
  }

  bool cleanupShuffle(int) override {
    return true;
  }

  void shutdown() override {}

 private:
  // Build a stream by concatenating chunks for all maps in [start, end).
  std::unique_ptr<celeborn::client::CelebornInputStream> readPartitionImpl(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      bool needCompression) {
    if (endMapIndex <= startMapIndex) {
      return nullptr;
    }
    std::vector<uint8_t> combinedPayload;
    // Concatenate map outputs for the requested map index range.
    for (int mapId = startMapIndex; mapId < endMapIndex; ++mapId) {
      const MapKey mapKey{shuffleId, partitionId, attemptNumber, mapId};
      auto payloadIt = payloadByMapKey_.find(mapKey);
      if (payloadIt == payloadByMapKey_.end()) {
        continue;
      }
      for (const auto& chunk : payloadIt->second) {
        combinedPayload.insert(
            combinedPayload.end(), chunk.begin(), chunk.end());
      }
    }
    if (combinedPayload.empty()) {
      return nullptr;
    }
    auto clientFactory = std::make_shared<FakeTransportClientFactory>(conf_);
    clientFactory->setPayload(combinedPayload);
    std::vector<std::shared_ptr<const celeborn::protocol::PartitionLocation>>
        locations;
    std::vector<int> attempts{0};
    locations.emplace_back(makeLocation(partitionId));
    return std::make_unique<celeborn::client::CelebornInputStream>(
        "shuffle_key",
        std::static_pointer_cast<const celeborn::conf::CelebornConf>(conf_),
        clientFactory,
        std::move(locations),
        attempts,
        attemptNumber,
        startMapIndex,
        endMapIndex,
        needCompression);
  }

  std::shared_ptr<celeborn::conf::CelebornConf> conf_;
  bool hasKnownCounts_{false};
  int knownNumMappers_{0};
  int knownNumPartitions_{0};
  std::map<MapKey, std::vector<std::vector<uint8_t>>> payloadByMapKey_;
};
// Verifies iterator returns nullptr when no partitions or after close.
TEST(CelebornReaderStreamIteratorTest, ReturnsNullWhenEmptyOrClosed) {
  auto client = std::make_shared<FakeShuffleClient>();
  CelebornReaderStreamIterator iterator(client, 1, {}, 0, 0, 0, false);
  EXPECT_EQ(iterator.nextStream(arrow::default_memory_pool()), nullptr);
  iterator.close();
  EXPECT_EQ(iterator.nextStream(arrow::default_memory_pool()), nullptr);
}

// Reads two partitions and confirms data matches per-map concatenation order.
TEST(CelebornReaderStreamIteratorTest, iteratesPartitionsAndReads) {
  auto client = std::make_shared<FakeShuffleClient>();
  constexpr int kShuffleId = 42;
  constexpr int kMapId0 = 0;
  constexpr int kMapId1 = 1;
  constexpr int kAttemptId = 3;
  constexpr int kNumMappers = 2;
  constexpr int kNumPartitions = 2;
  constexpr int kAttemptNumber = kAttemptId;
  constexpr int kStartMapIndex = 0;
  constexpr int kEndMapIndex = 2;
  constexpr int32_t kPartitionA = 0;
  constexpr int32_t kPartitionB = 1;
  const std::string payloadA0 = "p0m0";
  const std::string payloadA1 = "p0m1";
  const std::string payloadB0 = "p1m0";
  const std::string payloadB1 = "p1m1";
  const std::string expectedA = payloadA0 + payloadA1;
  const std::string expectedB = payloadB0 + payloadB1;
  NativeCelebornClient nativeClient0(
      client, kShuffleId, kMapId0, kAttemptId, kNumMappers, kNumPartitions);
  NativeCelebornClient nativeClient1(
      client, kShuffleId, kMapId1, kAttemptId, kNumMappers, kNumPartitions);
  nativeClient0.pushPartitionData(
      kPartitionA, const_cast<char*>(payloadA0.data()), payloadA0.size());
  nativeClient1.pushPartitionData(
      kPartitionA, const_cast<char*>(payloadA1.data()), payloadA1.size());
  nativeClient0.pushPartitionData(
      kPartitionB, const_cast<char*>(payloadB0.data()), payloadB0.size());
  nativeClient1.pushPartitionData(
      kPartitionB, const_cast<char*>(payloadB1.data()), payloadB1.size());
  std::vector<int32_t> partitions{kPartitionA, kPartitionB};
  CelebornReaderStreamIterator iterator(
      client,
      kShuffleId,
      partitions,
      kAttemptNumber,
      kStartMapIndex,
      kEndMapIndex,
      false);
  auto stream = iterator.nextStream(arrow::default_memory_pool());
  ASSERT_NE(stream, nullptr);
  readStreamAndExpect(stream, expectedA);

  auto tellResult = stream->Tell();
  ASSERT_TRUE(tellResult.ok());
  EXPECT_EQ(tellResult.ValueOrDie(), static_cast<int64_t>(expectedA.size()));

  auto stream2 = iterator.nextStream(arrow::default_memory_pool());
  ASSERT_NE(stream2, nullptr);
  readStreamAndExpect(stream2, expectedB);
  EXPECT_EQ(iterator.nextStream(arrow::default_memory_pool()), nullptr);
}

// Ensures multiple pushes for the same map are read in push order.
TEST(CelebornReaderStreamIteratorTest, appendsMultiplePushesPerMap) {
  auto client = std::make_shared<FakeShuffleClient>();
  constexpr int kShuffleId = 9;
  constexpr int kMapId = 0;
  constexpr int kAttemptId = 2;
  constexpr int kNumMappers = 1;
  constexpr int kNumPartitions = 1;
  constexpr int kAttemptNumber = kAttemptId;
  constexpr int kStartMapIndex = 0;
  constexpr int kEndMapIndex = 1;
  constexpr int32_t kPartitionId = 0;
  const std::string payload0 = "hello";
  const std::string payload1 = "world";
  const std::string expected = payload0 + payload1;
  NativeCelebornClient nativeClient(
      client, kShuffleId, kMapId, kAttemptId, kNumMappers, kNumPartitions);
  nativeClient.pushPartitionData(
      kPartitionId, const_cast<char*>(payload0.data()), payload0.size());
  nativeClient.pushPartitionData(
      kPartitionId, const_cast<char*>(payload1.data()), payload1.size());
  CelebornReaderStreamIterator iterator(
      client,
      kShuffleId,
      {kPartitionId},
      kAttemptNumber,
      kStartMapIndex,
      kEndMapIndex,
      false);
  auto stream = iterator.nextStream(arrow::default_memory_pool());
  ASSERT_NE(stream, nullptr);
  readStreamAndExpect(stream, expected);
}

// Confirms updateMetrics is a no-op for the iterator.
TEST(CelebornReaderStreamIteratorTest, updateMetricsNoop) {
  auto client = std::make_shared<FakeShuffleClient>();
  CelebornReaderStreamIterator iterator(client, 1, {1}, 0, 0, 0, false);
  EXPECT_NO_THROW(iterator.updateMetrics(0, 0, 0, 0, 0));
}

TEST(CelebornE2ESmokeTest, nativeClientPushAndReadBack) {
  if (!readBoolEnv(kRunE2EEnv)) {
    GTEST_SKIP()
        << "Set BOLT_CELEBORN_E2E=1 to run real Celeborn E2E smoke test";
  }

  auto appId = getEnvOrDefault(kCelebornLmAppIdEnv, "bolt-shuffle-test-app");
  auto shuffleClient = createRealCelebornClientForTests(appId);
  RealCelebornClientCleanupGuard realCelebornClientCleanupGuard(&shuffleClient);

  // Let the cleanup guard allocate a process-scoped shuffle id and own
  // cleanup/shutdown responsibilities on all exit paths.
  const int kShuffleId = realCelebornClientCleanupGuard.shuffleId();
  constexpr int kAttemptId = 0;
  constexpr int kNumMappers = 2;
  constexpr int kNumPartitions = 2;

  NativeCelebornClient mapper0(
      shuffleClient, kShuffleId, 0, kAttemptId, kNumMappers, kNumPartitions);
  NativeCelebornClient mapper1(
      shuffleClient, kShuffleId, 1, kAttemptId, kNumMappers, kNumPartitions);

  const std::string p0m0 = "partition0-map0|";
  const std::string p0m1 = "partition0-map1";
  const std::string p1m0 = "partition1-map0|";
  const std::string p1m1 = "partition1-map1";

  mapper0.pushPartitionData(0, const_cast<char*>(p0m0.data()), p0m0.size());
  mapper1.pushPartitionData(0, const_cast<char*>(p0m1.data()), p0m1.size());
  mapper0.pushPartitionData(1, const_cast<char*>(p1m0.data()), p1m0.size());
  mapper1.pushPartitionData(1, const_cast<char*>(p1m1.data()), p1m1.size());

  mapper0.stop();
  mapper1.stop();

  shuffleClient->updateReducerFileGroup(kShuffleId);

  std::vector<int32_t> partitions = {0, 1};
  CelebornReaderStreamIterator iterator(
      shuffleClient, kShuffleId, partitions, kAttemptId, 0, kNumMappers, false);

  auto stream0 = iterator.nextStream(arrow::default_memory_pool());
  ASSERT_NE(stream0, nullptr);
  readStreamAndExpect(stream0, p0m0 + p0m1);

  auto stream1 = iterator.nextStream(arrow::default_memory_pool());
  ASSERT_NE(stream1, nullptr);
  readStreamAndExpect(stream1, p1m0 + p1m1);

  EXPECT_EQ(iterator.nextStream(arrow::default_memory_pool()), nullptr);
  realCelebornClientCleanupGuard.cleanupNow();
}

// Verifies read fails before any push and succeeds after push.
TEST(CelebornReaderStreamIteratorTest, requiresPushBeforeRead) {
  auto client = std::make_shared<FakeShuffleClient>();
  constexpr int kShuffleId = 7;
  constexpr int32_t kPartitionId = 0;
  constexpr int kMapId0 = 0;
  constexpr int kMapId1 = 1;
  constexpr int kAttemptId = 1;
  constexpr int kAttemptNumber = kAttemptId;
  constexpr int kNumMappers = 2;
  constexpr int kNumPartitions = 1;
  constexpr int kStartMapIndex = 0;
  constexpr int kEndMapIndex = 2;
  CelebornReaderStreamIterator iterator(
      client,
      kShuffleId,
      {kPartitionId},
      kAttemptNumber,
      kStartMapIndex,
      kEndMapIndex,
      false);
  EXPECT_THROW(
      iterator.nextStream(arrow::default_memory_pool()),
      bytedance::bolt::BoltRuntimeError);

  NativeCelebornClient nativeClient(
      client, kShuffleId, kMapId0, kAttemptId, kNumMappers, kNumPartitions);
  NativeCelebornClient nativeClient2(
      client, kShuffleId, kMapId1, kAttemptId, kNumMappers, kNumPartitions);
  const std::string payload0 = "ping0";
  const std::string payload1 = "ping1";
  const std::string expected = payload0 + payload1;
  nativeClient.pushPartitionData(
      kPartitionId, const_cast<char*>(payload0.data()), payload0.size());
  nativeClient2.pushPartitionData(
      kPartitionId, const_cast<char*>(payload1.data()), payload1.size());

  CelebornReaderStreamIterator iterator2(
      client,
      kShuffleId,
      {kPartitionId},
      kAttemptNumber,
      kStartMapIndex,
      kEndMapIndex,
      false);
  auto stream = iterator2.nextStream(arrow::default_memory_pool());
  ASSERT_NE(stream, nullptr);
  readStreamAndExpect(stream, expected);
}

TEST(NativeCelebornClientTest, testStopTriggeredTwice) {
  auto client = std::make_shared<FakeShuffleClient>();
  constexpr int kShuffleId = 15;
  constexpr int32_t kPartitionId = 0;
  constexpr int kMapId = 0;
  constexpr int kAttemptId = 5;
  constexpr int kNumMappers = 1;
  constexpr int kNumPartitions = 1;

  NativeCelebornClient nativeClient(
      client, kShuffleId, kMapId, kAttemptId, kNumMappers, kNumPartitions);
  nativeClient.pushPartitionData(kPartitionId, nullptr, 0);

  nativeClient.stop();

  // Calling stop again for the same map attempt should trigger an
  // BoltRuntimeError.
  EXPECT_THROW(nativeClient.stop(), bytedance::bolt::BoltRuntimeError);

  // Further pushPartitionData calls after stop should also fail.
  EXPECT_THROW(
      nativeClient.pushPartitionData(kPartitionId, nullptr, 0),
      bytedance::bolt::BoltRuntimeError);
}

} // namespace bytedance::bolt::shuffle::sparksql::test
