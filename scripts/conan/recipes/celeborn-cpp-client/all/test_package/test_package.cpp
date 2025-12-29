/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <folly/init/Init.h>

#include "celeborn/client/ShuffleClient.h"
#include "celeborn/client/reader/CelebornInputStream.h"
#include "celeborn/client/writer/PushState.h"
#include "celeborn/conf/CelebornConf.h"
#include "celeborn/network/TransportClient.h"

namespace {

int Fail(const char* message) {
  std::cerr << "test_package failure: " << message << std::endl;
  return 1;
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  auto conf = std::make_shared<celeborn::conf::CelebornConf>();
  auto confConst = std::static_pointer_cast<const celeborn::conf::CelebornConf>(
      conf);
  celeborn::client::ShuffleClientEndpoint endpoint(confConst);
  auto client = celeborn::client::ShuffleClientImpl::create(
      "test_app", confConst, endpoint);

  if (!client) {
    return Fail("ShuffleClientImpl construction failed");
  }

  celeborn::client::PushState pushState(*conf);
  const std::string hostAndPort = "127.0.0.1:1234";
  int batchId = pushState.nextBatchId();
  pushState.addBatch(batchId, hostAndPort);
  pushState.onSuccess(hostAndPort);
  pushState.onCongestControl(hostAndPort);
  pushState.removeBatch(batchId, hostAndPort);
  pushState.cleanup();

  auto clientFactory =
      std::make_shared<celeborn::network::TransportClientFactory>(confConst);
  std::vector<std::shared_ptr<const celeborn::protocol::PartitionLocation>>
      locations;
  std::vector<int> attempts;
  celeborn::client::CelebornInputStream stream(
      "shuffle_key",
      confConst,
      clientFactory,
      std::move(locations),
      attempts,
      0,
      0,
      0,
      false);
  uint8_t buffer[1] = {0};
  if (stream.read(buffer, 0, 1) != -1) {
    return Fail("CelebornInputStream should return -1 for empty locations");
  }

  client->shutdown();
  return 0;
}
