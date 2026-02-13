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

#include <gtest/gtest.h>

#include <folly/init/Init.h>

#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <celeborn/client/ShuffleClient.h>
#include <celeborn/conf/CelebornConf.h>

#include <unistd.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/CelebornReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/NativeCelebornClient.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

constexpr const char* kRunEnv = "BOLT_CELEBORN_E2E";
constexpr const char* kEndpointFileEnv = "BOLT_CELEBORN_LM_ENDPOINT_FILE";
constexpr const char* kLmAppIdEnv = "BOLT_CELEBORN_LM_APP_ID";

bool envEnabled(const char* name) {
  const char* v = std::getenv(name);
  if (v == nullptr) {
    return false;
  }
  return std::string(v) == "1";
}

std::string envOrDefault(const char* name, std::string value) {
  const char* v = std::getenv(name);
  if (v == nullptr || std::strlen(v) == 0) {
    return value;
  }
  return std::string(v);
}

std::pair<std::string, int> loadLifecycleManagerEndpoint() {
  std::string endpointFile = envOrDefault(
      kEndpointFileEnv,
      "bolt/shuffle/sparksql/tests/celeborn/runtime/state/lifecycle_manager.endpoint");
  std::ifstream in(endpointFile);
  if (!in.good()) {
    BOLT_FAIL(
        "Failed to open lifecycle manager endpoint file: " + endpointFile);
  }
  std::string endpoint;
  std::getline(in, endpoint);
  auto pos = endpoint.rfind(':');
  BOLT_CHECK(
      pos != std::string::npos,
      "Invalid lifecycle manager endpoint format in file " + endpointFile +
          ": " + endpoint);
  return {endpoint.substr(0, pos), std::stoi(endpoint.substr(pos + 1))};
}

std::string readAll(
    const std::shared_ptr<arrow::io::InputStream>& stream,
    size_t expectedBytes) {
  auto result = stream->Read(static_cast<int64_t>(expectedBytes));
  BOLT_CHECK(result.ok(), "Failed reading bytes from Celeborn stream");
  auto buf = result.ValueOrDie();
  return std::string(reinterpret_cast<const char*>(buf->data()), buf->size());
}

std::vector<std::shared_ptr<celeborn::client::ShuffleClient>>&
leakedShuffleClients() {
  static auto* clients =
      new std::vector<std::shared_ptr<celeborn::client::ShuffleClient>>();
  return *clients;
}

void ensureFollyInitialized() {
  static std::once_flag once;
  std::call_once(once, [] {
    int argc = 1;
    char arg0[] = "bolt-celeborn-e2e";
    char* argv[] = {arg0, nullptr};
    char** argvPtr = argv;
    folly::init(&argc, &argvPtr, false);
  });
}

} // namespace

TEST(CelebornE2ESmokeTest, nativeClientPushAndReadBack) {
  if (!envEnabled(kRunEnv)) {
    GTEST_SKIP()
        << "Set BOLT_CELEBORN_E2E=1 to run real Celeborn E2E smoke test";
  }

  auto [lmHost, lmPort] = loadLifecycleManagerEndpoint();
  ensureFollyInitialized();
  auto conf = std::make_shared<celeborn::conf::CelebornConf>();
  auto endpoint = celeborn::client::ShuffleClientEndpoint(conf);
  auto appId = envOrDefault(kLmAppIdEnv, "bolt-shuffle-test-app");
  auto shuffleClient =
      celeborn::client::ShuffleClientImpl::create(appId, conf, endpoint);
  shuffleClient->setupLifecycleManagerRef(lmHost, lmPort);

  constexpr int kShuffleId = 10001;
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
  EXPECT_EQ(readAll(stream0, p0m0.size() + p0m1.size()), p0m0 + p0m1);

  auto stream1 = iterator.nextStream(arrow::default_memory_pool());
  ASSERT_NE(stream1, nullptr);
  EXPECT_EQ(readAll(stream1, p1m0.size() + p1m1.size()), p1m0 + p1m1);

  EXPECT_EQ(iterator.nextStream(arrow::default_memory_pool()), nullptr);
  shuffleClient->cleanupShuffle(kShuffleId);
  leakedShuffleClients().push_back(std::move(shuffleClient));
}

} // namespace bytedance::bolt::shuffle::sparksql::test
